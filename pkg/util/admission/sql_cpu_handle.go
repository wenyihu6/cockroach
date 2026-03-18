// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

// SQLCPUBasedResponseAdmissionEnabled controls whether SQL response admission
// is handled through CPU time token (CTT) based admission instead of the
// legacy slot-based response admission queues. When enabled,
// MeasureAndAdmitResponse only performs CPU measurement and skips the call to
// WorkQueue.Admit.
var SQLCPUBasedResponseAdmissionEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.sql_cpu_based_response_admission.enabled",
	"if true, SQL response admission is handled through CPU time measurement "+
		"instead of the legacy response admission queues",
	false,
)

// SQLWorkInfo captures identifying information about SQL work for CPU
// accounting and admission.
//
// Note that the TenantID here is set via Codec.TenantID (see MakeCPUHandle
// in flow.go), whereas the response admission call sites previously hardcoded
// roachpb.SystemTenantID. The Codec.TenantID value is correct for
// shared-process multi-tenancy and is equivalent to SystemTenantID in
// single-tenant clusters.
type SQLWorkInfo struct {
	// AtGateway is true if the work is being executed at a gateway node.
	AtGateway bool
	// TenantID is the id of the tenant. For single-tenant clusters, this will
	// always be the SystemTenantID.
	TenantID roachpb.TenantID
	// Priority is utilized within a tenant.
	Priority admissionpb.WorkPriority
	// CreateTime is equivalent to Time.UnixNano() at the creation time of this
	// work or a parent work (e.g. could be the start time of the transaction,
	// if this work was created as part of a transaction). It is used to order
	// work within a (WorkloadID, Priority) pair -- earlier CreateTime is given
	// preference.
	CreateTime int64
	// WorkloadID is the statement fingerprint ID, used for ASH sampling.
	WorkloadID uint64
}

// SQLCPUProvider is used to get a SQLCPUHandle that is used for CPU
// accounting and admission.
type SQLCPUProvider interface {
	// GetHandle returns a SQLCPUHandle for the supplied work info.
	GetHandle(work SQLWorkInfo) *SQLCPUHandle
	GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64)
}

var sqlCPUAdmissionHandleContextKey = ctxutil.RegisterFastValueKey()

// ContextWithSQLCPUHandle returns a Context wrapping the supplied handle, if
// any.
func ContextWithSQLCPUHandle(ctx context.Context, h *SQLCPUHandle) context.Context {
	if h == nil {
		return ctx
	}
	return ctxutil.WithFastValue(ctx, sqlCPUAdmissionHandleContextKey, h)
}

// SQLCPUHandleFromContext returns the handle contained in the Context, if
// any.
func SQLCPUHandleFromContext(ctx context.Context) *SQLCPUHandle {
	val := ctxutil.FastValue(ctx, sqlCPUAdmissionHandleContextKey)
	h, ok := val.(*SQLCPUHandle)
	if !ok {
		return nil
	}
	return h
}

var goroutineCPUHandlePool = sync.Pool{
	New: func() interface{} {
		return &GoroutineCPUHandle{}
	},
}

// SQLCPUHandle manages CPU accounting and admission for SQL work across
// multiple goroutines. When CTT-based SQL admission is enabled, the handle
// reserves a chunk of CPU tokens from the CTT WorkQueue. Goroutines deduct
// from the reservation atomically via reportCPU. When the reservation is
// exhausted, settleAndAdmit settles the previous interval and requests a
// new chunk, blocking if tokens are unavailable. Reservation size adapts
// to the handle's CPU consumption rate via EWMA (mirroring KV's per-tenant
// cpuTimeTokenEstimator pattern). On Close, the EWMA is saved to the
// provider so future handles of the same type (gateway vs flow) start
// with a warm estimate instead of the cold initialReservationNanos.
type SQLCPUHandle struct {
	workInfo SQLWorkInfo
	p        *sqlCPUProviderImpl
	// q is the CTT WorkQueue. Protected by settleMu; set on first
	// settleAndAdmit call.
	q *WorkQueue
	// reservedTokenNanos is the remaining token reservation in nanos.
	// Decremented atomically by reportCPU. When <= 0, settleAndAdmit
	// replenishes it from the WorkQueue.
	reservedTokenNanos atomic.Int64
	// totalReserved is the size of the current reservation chunk in nanos.
	// Protected by settleMu.
	totalReserved int64
	// ewmaCPUNanos is the exponentially weighted moving average of CPU
	// consumed per reservation interval. Used to size the next reservation.
	// Protected by settleMu.
	ewmaCPUNanos float64

	// settleMu serializes the token settlement and refill path. Separate
	// from mu to avoid blocking RegisterGoroutine during a potentially
	// blocking Admit call.
	settleMu syncutil.Mutex

	mu struct {
		syncutil.Mutex
		closed   bool
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
		// lastAdmitResp is the AdmitResponse from the most recent Admit call
		// on the CTT queue. Used to settle at the next checkpoint or Close.
		lastAdmitResp AdmitResponse
	}
}

func newSQLCPUAdmissionHandle(workInfo SQLWorkInfo, p *sqlCPUProviderImpl) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo: workInfo,
		p:        p,
	}
	h.mu.gHandles = h.mu.handlesBacking[:0]
	return h
}

// reportCPU atomically adds the CPU time difference to the appropriate
// cumulative counter and deducts from the token reservation.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	nanos := diff.Nanoseconds()
	if h.workInfo.AtGateway {
		h.p.cumulativeGatewayCPUNanos.Add(nanos)
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(nanos)
	}
	h.reservedTokenNanos.Add(-nanos)
}

// TODO(sumeer): see the comment
// https://github.com/cockroachdb/cockroach/pull/161952#pullrequestreview-3741525716
// on additional integrations that may need to call RegisterGoroutine.

// RegisterGoroutine returns a GoroutineCPUHandle to use for reporting and
// admission. If the goroutine was already registered, the existing handle
// will be returned. CPU time is accounted for at this goroutine from the
// instant the handle was first created for this goroutine. The cpu accounting
// will end for this goroutine when it is closed by calling
// GoroutineCPUHandle.Close, or if never closed, until the last call to
// GoroutineCPUHandle.MeasureAndAdmit.
func (h *SQLCPUHandle) RegisterGoroutine() *GoroutineCPUHandle {
	gid := goid.Get()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, gh := range h.mu.gHandles {
		if gh.gid == gid {
			// Already registered.
			return gh
		}
	}
	// Not registered, create a new handle.
	gh := newGoroutineCPUHandle(gid, h)
	h.mu.gHandles = append(h.mu.gHandles, gh)

	return gh
}

// MeasureAndAdmitResponse measures CPU time for the calling goroutine and
// performs response admission. The calling goroutine must already be
// registered via RegisterGoroutine (called at goroutine start); this method
// retrieves the existing handle.
//
// When SQLCPUBasedResponseAdmissionEnabled is true, admission is handled
// through the CTT WorkQueue: each call settles the previous interval's
// estimate and admits for the next interval, blocking if tokens are
// exhausted. The passed queue q is ignored in this path.
//
// When the setting is false, the legacy slot-based response admission queue
// (q) is used.
func (h *SQLCPUHandle) MeasureAndAdmitResponse(ctx context.Context, q *WorkQueue) error {
	if trace.IsEnabled() {
		defer trace.StartRegion(ctx, "admission.SQLCPUHandle.responseAdmit").End()
	}
	gh := h.RegisterGoroutine()
	if err := gh.MeasureAndAdmit(ctx); err != nil {
		return err
	}
	if h.cttBasedSQLEnabled() {
		cttQueue := h.p.getCTTQueue(h.workInfo.TenantID)
		if cttQueue != nil {
			return h.settleAndAdmit(ctx, cttQueue)
		}
	}
	if q != nil {
		workInfo := WorkInfo{
			TenantID:   h.workInfo.TenantID,
			Priority:   h.workInfo.Priority,
			CreateTime: h.workInfo.CreateTime,
			WorkloadID: h.workInfo.WorkloadID,
		}
		if _, err := q.Admit(ctx, workInfo); err != nil {
			return err
		}
	}
	return nil
}

const (
	// minReservationNanos is the minimum reservation chunk size.
	minReservationNanos = int64(10 * time.Millisecond)
	// maxReservationNanos is the maximum reservation chunk size.
	maxReservationNanos = int64(1 * time.Second)
	// initialReservationNanos is the default reservation before the EWMA
	// has converged.
	initialReservationNanos = int64(100 * time.Millisecond)
	// ewmaAlpha is the smoothing factor for the CPU EWMA. Matches the
	// α=0.5 used by KV's per-tenant cpuTimeTokenEstimator.
	ewmaAlpha = 0.5
	// reservationMultiplier provides headroom above the EWMA to absorb
	// variance in CPU consumption.
	reservationMultiplier = 2.0
)

// nextReservationSize computes the next reservation size based on the
// EWMA of CPU consumed per reservation interval. Must be called with
// settleMu held.
func (h *SQLCPUHandle) nextReservationSize() int64 {
	if h.ewmaCPUNanos <= 0 {
		return initialReservationNanos
	}
	size := int64(h.ewmaCPUNanos * reservationMultiplier)
	if size < minReservationNanos {
		return minReservationNanos
	}
	if size > maxReservationNanos {
		return maxReservationNanos
	}
	return size
}

// settleAndAdmit checks the token reservation. If tokens remain, it
// returns immediately. Otherwise, it settles the previous reservation
// with the WorkQueue and requests a new one, blocking if tokens are
// exhausted.
func (h *SQLCPUHandle) settleAndAdmit(ctx context.Context, q *WorkQueue) error {
	remaining := h.reservedTokenNanos.Load()
	if remaining > 0 {
		return nil
	}

	// Tokens exhausted. Serialize settlement.
	if trace.IsEnabled() {
		defer trace.StartRegion(ctx, "admission.SQLCPUHandle.settle").End()
	}
	h.settleMu.Lock()
	defer h.settleMu.Unlock()

	// Re-check: another goroutine may have refilled while we waited.
	remaining = h.reservedTokenNanos.Load()
	if remaining > 0 {
		return nil
	}

	// Set q under settleMu (first call sets it, subsequent are no-ops).
	h.q = q

	// Settle the previous reservation. Read and clear lastAdmitResp in
	// a single lock acquisition to reduce mu contention.
	actualUsed := time.Duration(h.totalReserved - remaining)
	h.mu.Lock()
	prevResp := h.mu.lastAdmitResp
	h.mu.lastAdmitResp = AdmitResponse{}
	h.mu.Unlock()
	if prevResp.Enabled {
		q.AdmittedWorkDone(prevResp, actualUsed)
		h.ewmaCPUNanos = ewmaAlpha*float64(actualUsed) +
			(1-ewmaAlpha)*h.ewmaCPUNanos
		h.totalReserved = 0
	}

	// Request new reservation. Blocks if tokens are exhausted.
	nextSize := h.nextReservationSize()
	workInfo := WorkInfo{
		TenantID:       h.workInfo.TenantID,
		Priority:       h.workInfo.Priority,
		CreateTime:     h.workInfo.CreateTime,
		WorkloadID:     h.workInfo.WorkloadID,
		RequestedCount: nextSize,
	}
	resp, err := q.Admit(ctx, workInfo)
	if err != nil {
		return err
	}

	h.totalReserved = nextSize
	h.mu.Lock()
	h.mu.lastAdmitResp = resp
	h.mu.Unlock()
	// Store last so other goroutines see the new tokens after
	// lastAdmitResp is updated.
	h.reservedTokenNanos.Store(nextSize)
	return nil
}

// maybeSettleAndAdmit checks whether CTT-based admission is enabled and, if
// so, triggers settlement when the token reservation is exhausted. Called
// from GoroutineCPUHandle.measureAndAdmit to enforce admission at cancel
// checker checkpoints, not just at response admission boundaries.
func (h *SQLCPUHandle) maybeSettleAndAdmit(ctx context.Context) error {
	if !h.cttBasedSQLEnabled() {
		return nil
	}
	cttQueue := h.p.getCTTQueue(h.workInfo.TenantID)
	if cttQueue == nil {
		return nil
	}
	return h.settleAndAdmit(ctx, cttQueue)
}

// cttBasedSQLEnabled returns true if CTT-based SQL response admission is
// enabled. Returns false if settings are unavailable (e.g. external tenants).
func (h *SQLCPUHandle) cttBasedSQLEnabled() bool {
	return h.p.sv != nil && SQLCPUBasedResponseAdmissionEnabled.Get(h.p.sv)
}

// Close is called when no more reporting is needed. It performs final
// settlement of any outstanding CTT admission, saves the handle's
// EWMA to the provider for cross-handle learning, and pools
// GoroutineCPUHandles that have been closed.
func (h *SQLCPUHandle) Close() {
	h.mu.Lock()
	h.mu.closed = true
	resp := h.mu.lastAdmitResp
	h.mu.lastAdmitResp = AdmitResponse{}
	for i, gh := range h.mu.gHandles {
		if gh.closed.Load() {
			gh.reset()
			goroutineCPUHandlePool.Put(gh)
		}
		h.mu.gHandles[i] = nil
	}
	h.mu.gHandles = nil
	h.mu.Unlock()

	// Final settlement: settle with actual CPU used since last
	// reservation, and update the EWMA so the provider gets an accurate
	// estimate including this final interval.
	if resp.Enabled && h.q != nil {
		remaining := h.reservedTokenNanos.Swap(0)
		actualUsed := time.Duration(h.totalReserved - remaining)
		h.q.AdmittedWorkDone(resp, actualUsed)
		h.ewmaCPUNanos = ewmaAlpha*float64(actualUsed) +
			(1-ewmaAlpha)*h.ewmaCPUNanos
	}

	// Save the EWMA to the provider so future handles of the same type
	// start with a warm estimate.
	if h.ewmaCPUNanos > 0 {
		if h.workInfo.AtGateway {
			h.p.gatewayEWMANanos.Store(int64(h.ewmaCPUNanos))
		} else {
			h.p.flowEWMANanos.Store(int64(h.ewmaCPUNanos))
		}
	}
}

// GoroutineCPUHandle is used for CPU accounting on a single goroutine. It
// should be closed by calling Close when the goroutine's flow-related work is
// done. The Close call must be at the goroutine boundary, to structurally
// guarantee that MeasureAndAdmit is never called after Close. This structural
// guarantee is essential for safe pooling - the handle may be reused for a
// different goroutine after being pooled. It is safe to never Close a
// GoroutineCPUHandle -- it will be garbage collected.
type GoroutineCPUHandle struct {
	gid int64
	h   *SQLCPUHandle

	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// cpuAccounted is the total CPU time accounted for on this goroutine.
	// Monotonically increasing.
	cpuAccounted time.Duration

	pauseDur   time.Duration
	paused     int
	pauseStart time.Duration

	// closed is set to true when Close() is called. This is primarily for
	// debugging - the structural guarantee (Close at goroutine boundary)
	// is the primary safety mechanism, not this field.
	closed atomic.Bool
}

func newGoroutineCPUHandle(gid int64, h *SQLCPUHandle) *GoroutineCPUHandle {
	gh := goroutineCPUHandlePool.Get().(*GoroutineCPUHandle)
	*gh = GoroutineCPUHandle{
		gid:      gid,
		h:        h,
		cpuStart: grunning.Time(),
	}
	return gh
}

// reset clears all fields in preparation for returning to the pool.
func (h *GoroutineCPUHandle) reset() {
	*h = GoroutineCPUHandle{}
}

// Close marks this handle as closed. It must be called at the goroutine
// boundary when the goroutine's SQL work is done. After Close, the handle
// will be pooled when SQLCPUHandle.Close is called, so MeasureAndAdmit must
// never be called after Close.
func (h *GoroutineCPUHandle) Close(ctx context.Context) {
	_ = h.measureAndAdmit(ctx, true /* noWait */)
	h.closed.Store(true)
}

// MeasureAndAdmit should be called frequently. The callee will measure the
// CPU time spent in the goroutine and decide whether more CPU needs to be
// allocated. If more CPU is needed, it can block in acquiring CPU tokens.
// Returns a non-nil error iff the context is canceled while waiting.
func (h *GoroutineCPUHandle) MeasureAndAdmit(ctx context.Context) error {
	return h.measureAndAdmit(ctx, false /* noWait */)
}

// measureAndAdmit is the internal implementation of MeasureAndAdmit. The
// noWait parameter should only be set to true when the work is finished, so
// only measurement is desired (blocking is no longer productive). When noWait
// is true, this function never returns an error.
func (h *GoroutineCPUHandle) measureAndAdmit(ctx context.Context, noWait bool) error {
	if h.paused > 0 {
		return nil
	}
	cpuUsed := grunning.Elapsed(h.cpuStart, grunning.Time()) - h.pauseDur
	diff := cpuUsed - h.cpuAccounted
	if diff <= 0 {
		return nil
	}
	// TODO(sumeer): adding this diff to an atomic in SQLCPUHandle may be too
	// much overhead. An alternative would be implement an atomic here, and
	// only update the SQLCPUHandle when enough has accumulated. The reason
	// we would need an atomic here is that when SQLCPUHandle is closed, it
	// needs to reach in and grab whatever CPU has not yet been reported.
	h.cpuAccounted += diff
	h.h.reportCPU(diff)
	// When tokens are exhausted and we're not in the noWait (Close) path,
	// trigger settlement to enforce admission control. Without this, the
	// cancel-checker path would overdraft unboundedly between response
	// admission points.
	if !noWait && h.h.reservedTokenNanos.Load() <= 0 {
		return h.h.maybeSettleAndAdmit(ctx)
	}
	return nil
}

// PauseMeasuring is used to pause the CPU accounting for this goroutine. It
// must be paired with UnpauseMeasuring. Used when the goroutine is being used
// for KV work. If PauseMeasuring is called multiple times, an equal number of
// UnpauseMeasuring calls are needed to resume measuring.
func (h *GoroutineCPUHandle) PauseMeasuring() {
	h.paused++
	if h.paused == 1 {
		h.pauseStart = grunning.Time()
	}
}

// UnpauseMeasuring resumes CPU accounting for this goroutine.
func (h *GoroutineCPUHandle) UnpauseMeasuring() {
	h.paused--
	if h.paused == 0 {
		h.pauseDur += grunning.Elapsed(h.pauseStart, grunning.Time())
	}
}

type sqlCPUProviderImpl struct {
	// sv provides access to cluster settings. May be nil for external tenants
	// that do not have KV-level admission infrastructure.
	sv *settings.Values
	// systemTenantCTTQueue and appTenantCTTQueue are the CTT WorkQueues for
	// system and app tenant SQL work respectively. May be nil for external
	// tenants.
	systemTenantCTTQueue *WorkQueue
	appTenantCTTQueue    *WorkQueue
	// cumulativeGatewayCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for SQL work executed at gateway nodes. This value is
	// monotonically increasing and is updated atomically as CPU time is
	// reported via SQLCPUHandle.reportCPU.
	cumulativeGatewayCPUNanos atomic.Int64
	// cumulativeDistSQLCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for distributed SQL work. This value is monotonically
	// increasing and is updated atomically as CPU time is reported via
	// SQLCPUHandle.reportCPU.
	cumulativeDistSQLCPUNanos atomic.Int64
	// gatewayEWMANanos and flowEWMANanos store the last observed EWMA (in
	// nanos) from gateway and remote-flow handles respectively. New handles
	// are seeded from these values so they start with a reasonable
	// reservation size instead of the fixed initialReservationNanos.
	// Updated atomically on SQLCPUHandle.Close.
	gatewayEWMANanos atomic.Int64
	flowEWMANanos    atomic.Int64
}

// getCTTQueue returns the CTT WorkQueue for the given tenant. Returns nil
// if CTT queues are not available (e.g. external tenants).
func (p *sqlCPUProviderImpl) getCTTQueue(tenantID roachpb.TenantID) *WorkQueue {
	if tenantID.IsSystem() {
		return p.systemTenantCTTQueue
	}
	return p.appTenantCTTQueue
}

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo SQLWorkInfo) *SQLCPUHandle {
	h := newSQLCPUAdmissionHandle(workInfo, p)
	// Seed the handle's EWMA from the provider's cross-handle history so
	// it starts with a warm reservation size instead of the cold default.
	if workInfo.AtGateway {
		h.ewmaCPUNanos = float64(p.gatewayEWMANanos.Load())
	} else {
		h.ewmaCPUNanos = float64(p.flowEWMANanos.Load())
	}
	return h
}

// NewSQLCPUProvider creates a new SQLCPUProvider. The sv parameter provides
// access to cluster settings and may be nil for external tenants that do not
// have KV-level admission infrastructure. The CTT queue parameters may be
// nil for external tenants.
func NewSQLCPUProvider(
	sv *settings.Values, systemTenantCTTQueue, appTenantCTTQueue *WorkQueue,
) SQLCPUProvider {
	return &sqlCPUProviderImpl{
		sv:                   sv,
		systemTenantCTTQueue: systemTenantCTTQueue,
		appTenantCTTQueue:    appTenantCTTQueue,
	}
}
