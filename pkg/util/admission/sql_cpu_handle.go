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
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/petermattis/goid"
)

// SQLCPUProvider is used to get a SQLCPUHandle that is used for CPU
// accounting and admission.
type SQLCPUProvider interface {
	// GetHandle returns a SQLCPUHandle for the supplied work info.
	// atGateway indicates whether the work is being executed at the
	// gateway node, used for CPU accounting to distinguish gateway
	// vs DistSQL CPU usage.
	GetHandle(work WorkInfo, atGateway bool) *SQLCPUHandle
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
// multiple goroutines.
//
// SQL CPU admission differs from KV admission in several important ways:
//
//   - Admission model: KV uses an estimate-then-correct model. WorkQueue.Admit
//     is called before execution with an estimated RequestedCount (via
//     cpuTimeTokenEstimator), and AdmittedWorkDone is called after execution to
//     correct the estimate using the actual CPU time from grunning. SQL CPU
//     admission uses a measure-then-admit model: measureAndAdmit is called
//     periodically during execution (every ~1024 rows or every vectorized batch
//     via the CancelChecker), and the exact CPU consumed since the last call is
//     known from grunning. There is no estimation and no correction step.
//
//   - Lifetime: A KV request typically consumes microseconds to low
//     milliseconds of on-CPU time (as measured by grunning), with a
//     single Admit/AdmittedWorkDone pair. A SQLCPUHandle is created
//     per-statement (in MakeCPUHandle) and lives until the statement
//     completes. It spans the entire operator tree: each goroutine
//     in the DistSQL flow registers via RegisterGoroutine and gets a
//     GoroutineCPUHandle. For a simple query this may be a single goroutine
//     lasting milliseconds; for a long-running OLAP query, IMPORT, or BACKUP,
//     the handle can live for minutes or hours across many goroutines, with
//     measureAndAdmit called thousands or millions of times.
//
//   - Why admit-after-consume is acceptable: The goroutine runs freely between
//     measureAndAdmit calls — CPU is consumed without permission and then
//     retroactively deducted from the shared token bucket. If the bucket is
//     depleted, the goroutine blocks, preventing it from consuming more CPU.
//     Each uncontrolled burst is bounded by the work between two CancelChecker
//     calls (~1024 rows), so the amount of unpermitted CPU per check is limited.
//     The throttling does not prevent past usage but gates future usage.
type SQLCPUHandle struct {
	workInfo  WorkInfo
	atGateway bool
	p         *sqlCPUProviderImpl
	wq        *WorkQueue

	// admitMu serializes Admit calls. This is separate from mu so that
	// goroutines deducting from the reservation (fast path under mu) are
	// not blocked while another goroutine waits on Admit.
	admitMu struct {
		syncutil.Mutex
		// lastAdmitNanos is the UnixNano timestamp of the last Admit
		// call, used to compute the interval between calls for adaptive
		// sizing.
		lastAdmitNanos int64
		// nextReserveSize is the adaptive reservation size (in
		// nanoseconds) to request on the next Admit call, beyond the
		// immediate deficit. It grows when Admit is called frequently
		// (goroutine is CPU-hot) and shrinks when calls are infrequent.
		nextReserveSize int64
	}

	mu struct {
		syncutil.Mutex
		closed   bool
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle

		// reservedTokens is the number of pre-paid CPU token nanoseconds
		// remaining. Goroutines deduct from this before calling Admit,
		// reducing contention on the WorkQueue mutex. Non-negative.
		reservedTokens int64
	}
}

func newSQLCPUAdmissionHandle(
	workInfo WorkInfo, atGateway bool, p *sqlCPUProviderImpl, wq *WorkQueue,
) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo:  workInfo,
		atGateway: atGateway,
		p:         p,
		wq:        wq,
	}
	h.mu.gHandles = h.mu.handlesBacking[:0]
	return h
}

// minReserveSize and maxReserveSize bound the adaptive reservation to avoid
// reserving too little (negating the optimization) or too much (starving
// other work by holding tokens that may not be used).
const (
	minReserveSize = int64(100 * time.Microsecond)
	maxReserveSize = int64(10 * time.Millisecond)
)

// admitIntervalThresholds control the adaptive sizing. When the interval
// between Admit calls is shorter than shortAdmitInterval, the reservation
// grows (the goroutine is CPU-hot and will be back soon). When longer than
// longAdmitInterval, it shrinks (the goroutine is cooling down).
const (
	shortAdmitInterval = int64(1 * time.Millisecond)
	longAdmitInterval  = int64(10 * time.Millisecond)
)

// reportAndAcquireConsumedCPU updates cumulative CPU counters and, if a CTT
// WorkQueue is attached, deducts the consumed CPU from the shared token
// budget. To reduce contention on the WorkQueue mutex, this method maintains
// a local token reservation: goroutines deduct from the reservation first
// and only call Admit when it is exhausted. The reservation size adapts
// based on the interval between Admit calls — it grows when calls are
// frequent (CPU-hot goroutine) and shrinks when infrequent.
//
// RequestedCount is set to the exact amount needed (deficit + reservation),
// so the WorkQueue's CPU time token estimator is skipped (see Admit).
// Because the exact amount is deducted at Admit time, there is no estimate
// to correct, so AdmittedWorkDone is not called. This also avoids training
// the KV estimator with SQL CPU data, which would corrupt its estimates.
func (h *SQLCPUHandle) reportAndAcquireConsumedCPU(
	ctx context.Context, diff time.Duration, noWait bool,
) error {
	if h.atGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}

	if h.wq == nil {
		return nil
	}

	diffNanos := diff.Nanoseconds()

	h.mu.Lock()
	// If the handle is already closed, skip admission. This can happen
	// when a GoroutineCPUHandle outlives the SQLCPUHandle (the goroutine
	// hasn't called GoroutineCPUHandle.Close yet). Counter updates above
	// still apply; we just don't acquire new tokens that nobody would
	// return.
	if h.mu.closed {
		h.mu.Unlock()
		return nil
	}
	// Fast path: deduct from local reservation without calling Admit.
	if h.mu.reservedTokens >= diffNanos {
		h.mu.reservedTokens -= diffNanos
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	// Slow path: not enough tokens in the reservation. Release mu so
	// other goroutines can still deduct from whatever reservation remains
	// (a goroutine needing fewer tokens than what's left can proceed).

	if noWait {
		// noWait (handle closing): BypassAdmission means Admit never
		// blocks, and reserveExtra is 0 so the reservation is not
		// modified. Skip admitMu to avoid blocking behind a goroutine
		// waiting on a real Admit call.
		workInfo := h.workInfo
		workInfo.RequestedCount = diffNanos
		workInfo.BypassAdmission = true
		_, err := h.wq.Admit(ctx, workInfo)
		return err
	}

	// Serialize Admit calls under admitMu to prevent multiple goroutines
	// from refilling simultaneously.
	h.admitMu.Lock()

	// Re-check under mu: the reservation may have been refilled by the
	// goroutine that held admitMu before us.
	h.mu.Lock()
	if h.mu.closed {
		h.mu.Unlock()
		h.admitMu.Unlock()
		return nil
	}
	if h.mu.reservedTokens >= diffNanos {
		h.mu.reservedTokens -= diffNanos
		h.mu.Unlock()
		h.admitMu.Unlock()
		return nil
	}
	h.mu.Unlock()

	// Adapt the reservation size based on how frequently Admit is called.
	// Short intervals mean the goroutine is CPU-hot and will benefit from
	// a larger reservation; long intervals mean it's cooling down and a
	// smaller reservation avoids holding unused tokens. These fields are
	// protected by admitMu, not mu.
	now := timeutil.Now().UnixNano()
	if h.admitMu.lastAdmitNanos > 0 {
		interval := now - h.admitMu.lastAdmitNanos
		if interval < shortAdmitInterval {
			h.admitMu.nextReserveSize = min(h.admitMu.nextReserveSize*2, maxReserveSize)
		} else if interval > longAdmitInterval {
			h.admitMu.nextReserveSize = max(h.admitMu.nextReserveSize/2, minReserveSize)
		}
	} else {
		h.admitMu.nextReserveSize = minReserveSize
	}

	reserveExtra := h.admitMu.nextReserveSize

	// Request the full diff (not just the deficit) from Admit. We
	// intentionally leave whatever remains in reservedTokens for other
	// goroutines that may need fewer tokens — they can proceed on the
	// fast path while this goroutine waits on Admit.
	requestSize := diffNanos + reserveExtra

	workInfo := h.workInfo
	workInfo.RequestedCount = requestSize
	workInfo.BypassAdmission = false
	// AdmitResponse is intentionally discarded: its fields (Enabled,
	// requestedCount) are only needed by AdmittedWorkDone, which is not
	// called here (see comment on SQLCPUHandle).
	_, err := h.wq.Admit(ctx, workInfo)
	if err != nil {
		h.admitMu.Unlock()
		return err
	}

	// Add only the extra reservation (not the deficit) — the deficit
	// covers this goroutine's own consumption. If the handle was closed
	// concurrently, return the extra tokens to the granter immediately
	// to avoid leaking them.
	h.mu.Lock()
	if h.mu.closed {
		h.mu.Unlock()
		h.admitMu.Unlock()
		h.wq.ReturnTokens(reserveExtra)
		return nil
	}
	h.mu.reservedTokens += reserveExtra
	h.admitMu.lastAdmitNanos = now
	h.mu.Unlock()
	h.admitMu.Unlock()
	return nil
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

// Close is called when no more reporting is needed. It returns any unused
// reserved tokens to the WorkQueue and pools GoroutineCPUHandles that have
// been closed. GoroutineCPUHandles that are not yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.closed = true
	// Return unused reserved tokens to the WorkQueue so they are
	// available to other goroutines immediately.
	if h.wq != nil && h.mu.reservedTokens > 0 {
		h.wq.ReturnTokens(h.mu.reservedTokens)
		h.mu.reservedTokens = 0
	}
	for i, gh := range h.mu.gHandles {
		if gh.closed.Load() {
			gh.reset()
			goroutineCPUHandlePool.Put(gh)
		}
		h.mu.gHandles[i] = nil
	}
	h.mu.gHandles = nil
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
//
// See SQLCPUHandle for how SQL CPU admission differs from KV admission.
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
	return h.h.reportAndAcquireConsumedCPU(ctx, diff, noWait)
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
	// cumulativeGatewayCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for SQL work executed at gateway nodes. This value is
	// monotonically increasing and is updated atomically as CPU time is
	// reported via SQLCPUHandle.reportAndAcquireConsumedCPU.
	cumulativeGatewayCPUNanos atomic.Int64
	// cumulativeDistSQLCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for distributed SQL work. This value is monotonically
	// increasing and is updated atomically as CPU time is reported via
	// SQLCPUHandle.reportAndAcquireConsumedCPU.
	cumulativeDistSQLCPUNanos atomic.Int64
	// sv is the settings values used to check if CTT AC is enabled.
	sv *settings.Values
	// getWorkQueue returns the CTT WorkQueue for the given tenant. This
	// allows SQL to share the KV CTT WorkQueue, using the appropriate
	// tier (system vs app) based on tenant ID. Can be nil in testing.
	getWorkQueue func(roachpb.TenantID) *WorkQueue
}

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo WorkInfo, atGateway bool) *SQLCPUHandle {
	var wq *WorkQueue
	if p.getWorkQueue != nil && sqlCPUTimeTokenACIsEnabled(p.sv) {
		wq = p.getWorkQueue(workInfo.TenantID)
	}
	return newSQLCPUAdmissionHandle(workInfo, atGateway, p, wq)
}

// NewSQLCPUProvider creates a new SQLCPUProvider. The sv parameter is required
// and provides access to cluster settings for checking if SQL CPU time token
// AC is enabled. The getWorkQueue function returns the CTT WorkQueue for a
// given tenant, allowing SQL to share the KV CTT WorkQueue; it may be nil when
// CTT AC is not available (e.g. separate-process tenants).
func NewSQLCPUProvider(
	sv *settings.Values, getWorkQueue func(roachpb.TenantID) *WorkQueue,
) SQLCPUProvider {
	return &sqlCPUProviderImpl{
		sv:           sv,
		getWorkQueue: getWorkQueue,
	}
}
