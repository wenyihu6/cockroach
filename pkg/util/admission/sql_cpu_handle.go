// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
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
// integrates with the CTT WorkQueue: each MeasureAndAdmitResponse call
// settles the previous interval's estimate and admits for the next interval.
// Close performs final settlement.
type SQLCPUHandle struct {
	workInfo SQLWorkInfo
	p        *sqlCPUProviderImpl
	// q is the CTT WorkQueue, set on first settleAndAdmit call.
	q *WorkQueue
	// cpuSinceLastAdmit tracks CPU nanos accumulated since the last Admit
	// call. Updated atomically by reportCPU from multiple goroutines.
	cpuSinceLastAdmit atomic.Int64

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
// cumulative counter and tracks CPU since the last Admit call.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	nanos := diff.Nanoseconds()
	if h.workInfo.AtGateway {
		h.p.cumulativeGatewayCPUNanos.Add(nanos)
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(nanos)
	}
	h.cpuSinceLastAdmit.Add(nanos)
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

// settleAndAdmit settles the previous interval's CPU estimate with the
// WorkQueue and admits for the next interval. It blocks if tokens are
// exhausted, providing backpressure on SQL query processing.
func (h *SQLCPUHandle) settleAndAdmit(ctx context.Context, q *WorkQueue) error {
	h.q = q
	actualCPU := time.Duration(h.cpuSinceLastAdmit.Swap(0))

	h.mu.Lock()
	prevResp := h.mu.lastAdmitResp
	h.mu.Unlock()

	// Settle previous interval.
	if prevResp.Enabled {
		q.AdmittedWorkDone(prevResp, actualCPU)
	}

	// Admit for next interval. Blocks if tokens are exhausted.
	workInfo := WorkInfo{
		TenantID:   h.workInfo.TenantID,
		Priority:   h.workInfo.Priority,
		CreateTime: h.workInfo.CreateTime,
		WorkloadID: h.workInfo.WorkloadID,
	}
	resp, err := q.Admit(ctx, workInfo)
	if err != nil {
		return err
	}

	h.mu.Lock()
	h.mu.lastAdmitResp = resp
	h.mu.Unlock()
	return nil
}

// cttBasedSQLEnabled returns true if CTT-based SQL response admission is
// enabled. Returns false if settings are unavailable (e.g. external tenants).
func (h *SQLCPUHandle) cttBasedSQLEnabled() bool {
	return h.p.sv != nil && SQLCPUBasedResponseAdmissionEnabled.Get(h.p.sv)
}

// Close is called when no more reporting is needed. It performs final
// settlement of any outstanding CTT admission and pools GoroutineCPUHandles
// that have been closed. GoroutineCPUHandles that are not yet closed are
// left for GC.
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

	// Final settlement: return unused tokens.
	if resp.Enabled && h.q != nil {
		actualCPU := time.Duration(h.cpuSinceLastAdmit.Swap(0))
		h.q.AdmittedWorkDone(resp, actualCPU)
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
//
// TODO(sumeer): implement the measurement and admission logic.
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
	// TODO(sumeer): implement.
	return newSQLCPUAdmissionHandle(workInfo, p)
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
