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

	// closed is set when Close() begins. Checked atomically on the fast
	// path (to skip admission for goroutines that outlive the handle) and
	// in the slow path after Admit returns (to prevent token leaks when
	// Close races with a concurrent refill).
	closed atomic.Bool

	// reservation holds the remaining CPU time tokens (nanoseconds).
	// Accessed atomically via CAS on the fast path: goroutines deduct
	// only when the reservation has sufficient tokens, ensuring it never
	// goes negative. This allows multiple goroutines to consume tokens
	// concurrently without any locking.
	reservation atomic.Int64

	// refillMu serializes Admit calls. This is separate from mu so that
	// RegisterGoroutine and Close are not blocked while a goroutine waits
	// on Admit. The fast path (CAS on reservation) does not acquire any
	// lock.
	//
	// A mutex (rather than a channel) is used here because Admit itself
	// is context-aware: it selects on ctx.Done() internally. When a
	// context is cancelled, the goroutine holding refillMu returns from
	// Admit promptly, and subsequent goroutines acquiring refillMu will
	// see ctx.Err() and bail out immediately. The serial drain through
	// the mutex after cancellation is on the order of microseconds.
	refillMu struct {
		syncutil.Mutex
		// lastRefillTime is the wall-clock time of the last Admit call,
		// used to compute the interval for adaptive sizing.
		lastRefillTime time.Time
		// lastHeuristic is the adaptive reservation size (in nanoseconds)
		// to request on the next Admit call, beyond the immediate deficit.
		lastHeuristic int64
	}

	mu struct {
		syncutil.Mutex
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
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

const (
	// refillGrowThreshold is the wall-clock duration below which we
	// consider refills too frequent and double the heuristic. Below this
	// threshold, contention on the WorkQueue mutex is the primary concern.
	refillGrowThreshold = time.Millisecond
	// refillDecayThreshold is the wall-clock duration above which we
	// consider the heuristic too large and halve it. Above this threshold,
	// overcounting in tenant.used is the primary concern. Between
	// refillGrowThreshold and refillDecayThreshold is the acceptable
	// deadband where the heuristic is stable.
	refillDecayThreshold = 5 * time.Millisecond
	// maxRefillHeuristic caps the heuristic to bound overcounting.
	// 10ms of CPU tokens per handle.
	maxRefillHeuristic = int64(10 * time.Millisecond)
)

// tryDeductReservation attempts to deduct diffNanos from the reservation
// via CAS. Returns true if successful (reservation had enough tokens).
// Never drives the reservation negative.
func (h *SQLCPUHandle) tryDeductReservation(diffNanos int64) bool {
	for {
		current := h.reservation.Load()
		if current < diffNanos {
			return false
		}
		if h.reservation.CompareAndSwap(current, current-diffNanos) {
			return true
		}
	}
}

// refillHeuristic returns the number of extra tokens to request beyond
// covering the current checkpoint's consumption. It uses exponential
// backoff based on wall time between refills:
//
//   - elapsed < 1ms: heuristic doubles (refills too frequent, reduce
//     WorkQueue contention)
//   - 1ms <= elapsed <= 5ms: heuristic unchanged (acceptable range)
//   - elapsed > 5ms: heuristic halves (over-requested, reduce
//     overcounting in tenant.used)
//
// This deadband eliminates steady-state oscillation: the heuristic
// grows until it reaches the acceptable range, then stabilizes. It
// only decays when the workload genuinely becomes lighter.
//
// Must be called while holding refillMu.
func (h *SQLCPUHandle) refillHeuristic(consumed int64) int64 {
	now := timeutil.Now()
	if h.refillMu.lastRefillTime.IsZero() {
		// Bootstrap: start with consumed (same as current 2x behavior).
		h.refillMu.lastHeuristic = consumed
	} else {
		elapsed := now.Sub(h.refillMu.lastRefillTime)
		if elapsed < refillGrowThreshold {
			// Came back too soon — double to reduce call frequency.
			h.refillMu.lastHeuristic = min(
				h.refillMu.lastHeuristic*2, maxRefillHeuristic,
			)
		} else if elapsed > refillDecayThreshold {
			// Buffer lasted too long — halve to reduce overcounting.
			h.refillMu.lastHeuristic = max(
				consumed, h.refillMu.lastHeuristic/2,
			)
		}
		// else: in [1ms, 5ms] deadband — no change.
	}
	h.refillMu.lastRefillTime = now
	return h.refillMu.lastHeuristic
}

// reportAndAcquireConsumedCPU updates cumulative CPU counters and, if a CTT
// WorkQueue is attached, deducts the consumed CPU from the shared token
// budget. To reduce contention on the WorkQueue mutex, this method maintains
// a local token reservation: goroutines deduct from the reservation first
// (via CAS, lock-free) and only call Admit when it is exhausted.
//
// The reservation size adapts based on the interval between Admit calls — it
// grows when calls are frequent (CPU-hot goroutine) and shrinks when
// infrequent. RequestedCount is set to the exact amount needed (deficit +
// heuristic), and IsSQLCPU is set so the WorkQueue's CPU time token estimator
// is skipped (see Admit). Because the exact amount is deducted at Admit time,
// there is no estimate to correct, so AdmittedWorkDone is not called. This
// also avoids training the KV estimator with SQL CPU data, which would
// corrupt its estimates.
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

	// If the handle is already closed, skip admission. This can happen
	// when a GoroutineCPUHandle outlives the SQLCPUHandle (the goroutine
	// hasn't called GoroutineCPUHandle.Close yet). Counter updates above
	// still apply; we just don't acquire new tokens that nobody would
	// return.
	if h.closed.Load() {
		return nil
	}

	// Fast path: CAS deducts only if the reservation has enough tokens.
	// No lock or channel interaction needed. Multiple goroutines can
	// deduct concurrently.
	if h.tryDeductReservation(diffNanos) {
		return nil
	}

	if noWait {
		// Closing: account the CPU via BypassAdmission (non-blocking).
		// Do NOT deduct from reservation — driving it negative would
		// poison CAS for other goroutines. No turn needed since
		// BypassAdmission just updates accounting without waiting.
		workInfo := h.workInfo
		workInfo.RequestedCount = diffNanos
		workInfo.BypassAdmission = true
		workInfo.IsSQLCPU = true
		_, _ = h.wq.Admit(ctx, workInfo)
		return nil
	}

	// Slow path: serialize Admit calls under refillMu to prevent
	// multiple goroutines from refilling simultaneously.
	h.refillMu.Lock()

	// Re-check: another goroutine may have refilled while we waited.
	if h.tryDeductReservation(diffNanos) {
		h.refillMu.Unlock()
		return nil
	}

	heuristic := h.refillHeuristic(diffNanos)
	requestSize := diffNanos + heuristic

	workInfo := h.workInfo
	workInfo.RequestedCount = requestSize
	workInfo.BypassAdmission = false
	workInfo.IsSQLCPU = true
	resp, err := h.wq.Admit(ctx, workInfo)
	if err != nil {
		h.refillMu.Unlock()
		return err
	}

	if resp.Enabled {
		// Add the heuristic portion to reservation. We consume diffNanos
		// ourselves, so only the extra (heuristic) becomes buffer for
		// other goroutines. If the handle was closed concurrently, return
		// the heuristic tokens to the granter immediately to avoid
		// leaking them (Close already returned the reservation).
		if h.closed.Load() {
			h.refillMu.Unlock()
			h.wq.AdmittedSQLWorkDone(h.workInfo.TenantID, heuristic)
			return nil
		}
		h.reservation.Add(heuristic)
	}
	// If !resp.Enabled, AC is disabled — Admit took no tokens from the
	// granter. We must NOT add to reservation (would create phantom
	// tokens that corrupt the granter when returned at Close). The
	// goroutine proceeds untracked. Next checkpoint will try again.
	h.refillMu.Unlock()
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
// reserved tokens to the WorkQueue (adjusting tenant.used and the granter)
// and pools GoroutineCPUHandles that have been closed. GoroutineCPUHandles
// that are not yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	// Set closed first so that any in-flight slow-path goroutine that
	// completes Admit after this point will see it and return its
	// heuristic tokens directly instead of adding to reservation.
	h.closed.Store(true)

	// Return unused reservation tokens. Acquire refillMu to ensure no
	// concurrent refill is in progress — otherwise a concurrent refill
	// could add tokens after our swap.
	if h.wq != nil {
		h.refillMu.Lock()
		remaining := h.reservation.Swap(0)
		if remaining > 0 {
			h.wq.AdmittedSQLWorkDone(h.workInfo.TenantID, remaining)
		}
		h.refillMu.Unlock()
	}

	h.mu.Lock()
	defer h.mu.Unlock()
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
