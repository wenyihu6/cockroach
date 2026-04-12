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

	// Synchronization:
	//
	// admitTurn serializes slow-path Admit calls. It is a capacity-1
	// channel, initially empty. A goroutine writes to it to acquire
	// the turn (blocks if another goroutine holds the turn) and reads
	// from it to release. A channel is used instead of a mutex so the
	// slow path can select on both admitTurn and ctx.Done().
	//
	// mu protects closed and gHandles. It is held briefly (never
	// during Admit). The post-Admit commit step (check closed + Add
	// to reservation) and setClosed (set closed + Swap reservation)
	// both acquire mu, serializing them and preventing the race where
	// setClosed's Swap(0) misses a concurrent Add.
	//
	// Invariants:
	//
	// reservation is always >= 0. It is the only atomic field. Writes
	// that decrease reservation use CAS to ensure non-negativity.
	// Writes that increase reservation (Add) happen while holding the
	// turn or under mu. When closed is true, reservation is 0 (set by
	// setClosed's Swap(0), or cleaned up by the post-Admit commit
	// step under mu).
	//
	// closed transitions from false to true exactly once (in
	// setClosed) and never back. Once closed is true, no new tokens
	// are added to reservation.
	//
	// Three paths for deducting consumed CPU:
	//
	//  1. Fast path: CAS deduction from reservation. Lock-free, no
	//     Admit call. Grabs min(available, requested).
	//  2. noWait path: BypassAdmission Admit. Non-blocking accounting
	//     only, used by GoroutineCPUHandle.Close.
	//  3. Slow path: acquire the turn, call Admit to replenish
	//     reservation. May block until tokens are available.
	admitTurn chan struct{}

	// reservation is the local token cache, funded by Admit calls.
	// Accessed via CAS on the fast path (lock-free) and via Add/Swap
	// while holding the turn or under mu.
	reservation atomic.Int64

	// bufferNanos is the adaptive buffer (in nanoseconds) to request
	// beyond the consumed CPU on the next slow-path Admit call. The
	// buffer portion goes into reservation for future fast-path CAS
	// deductions. Starts at 0 (first call requests exactly what's
	// needed), then grows exponentially up to maxBufferNanos.
	// Accessed only while holding the turn.
	bufferNanos int64

	mu struct {
		syncutil.Mutex
		// closed is set to true exactly once by setClosed, never
		// reverted. Checked under mu in the post-Admit commit step.
		closed   bool
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations
		// in gHandles when there are 2 or fewer goroutines.
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
		admitTurn: make(chan struct{}, 1),
	}
	h.mu.gHandles = h.mu.handlesBacking[:0]
	return h
}

// reportCPU atomically adds the CPU time difference to the appropriate
// cumulative counter.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	if h.atGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}
}

// tryDeductReservation attempts to deduct up to diffNanos from the
// reservation via CAS. Returns the amount actually grabbed, which may
// be less than diffNanos if the reservation didn't have enough tokens
// (partial deduction). Never drives the reservation negative.
func (h *SQLCPUHandle) tryDeductReservation(diffNanos int64) int64 {
	for {
		current := h.reservation.Load()
		if current <= 0 {
			return 0
		}
		grab := min(current, diffNanos)
		if h.reservation.CompareAndSwap(current, current-grab) {
			return grab
		}
	}
}

const (
	// maxBufferNanos caps the buffer portion of the heuristic to bound
	// overcounting in tenant.used. 1ms of CPU buffer per handle is
	// sufficient — even kv95 requests consume well under 1ms of CPU,
	// so calling into the WorkQueue every 1ms of CPU time is fine.
	maxBufferNanos = int64(time.Millisecond)
)

// refillHeuristic returns the total RequestedCount to pass to Admit:
// the consumed CPU (diffNanos) plus an adaptive buffer for future
// fast-path CAS deductions. The buffer grows exponentially from 0 to
// maxBufferNanos based on CPU consumed (not wall-time intervals).
//
// On the first call, exactly diffNanos is requested (no buffer). Each
// subsequent call requests diffNanos + bufferNanos, then doubles the
// buffer for next time. This amortizes admission overhead over larger
// chunks of CPU work. The return value is always >= diffNanos.
//
// Must be called while holding the turn (after writing to admitTurn).
func (h *SQLCPUHandle) refillHeuristic(diffNanos int64) int64 {
	if h.bufferNanos == 0 {
		// First call: request exactly what's needed. Seed the buffer
		// for future calls so the next slow-path entry gets a buffer.
		h.bufferNanos = min(diffNanos, maxBufferNanos)
		return diffNanos
	}
	total := diffNanos + h.bufferNanos
	// Grow exponentially for next time, capped at maxBufferNanos.
	h.bufferNanos = min(h.bufferNanos*2, maxBufferNanos)
	return total
}

// constructWorkInfo returns a copy of the handle's WorkInfo with the given
// RequestedCount and BypassAdmission values set.
func (h *SQLCPUHandle) constructWorkInfo(reqCount int64, noWait bool) WorkInfo {
	workInfo := h.workInfo
	workInfo.RequestedCount = reqCount
	workInfo.BypassAdmission = noWait
	return workInfo
}

// reportAndAcquireConsumedCPU updates cumulative CPU counters and, if a CTT
// WorkQueue is attached, deducts the consumed CPU from the token bucket.
//
// See the SQLCPUHandle struct comment for the three paths (fast, noWait,
// slow) and the synchronization invariants.
func (h *SQLCPUHandle) reportAndAcquireConsumedCPU(
	ctx context.Context, diff time.Duration, noWait bool,
) error {
	h.reportCPU(diff)

	if h.wq == nil {
		return nil
	}

	diffNanos := diff.Nanoseconds()

	// Fast path: deduct from reservation via CAS. No lock needed. Grabs
	// as much as available, up to diffNanos (partial deduction). After
	// Close, reservation is 0 (Swap'd), so this returns 0 immediately.
	grabbed := h.tryDeductReservation(diffNanos)
	if grabbed == diffNanos {
		return nil
	}
	remaining := diffNanos - grabbed

	if noWait {
		// Account the remaining CPU via BypassAdmission (non-blocking).
		// This updates tenant.used and tells the granter tokens were
		// taken, but never blocks. The grabbed portion was already
		// accounted for by a prior Admit that filled the reservation.
		// This path is safe after Close because BypassAdmission never
		// blocks and keeps tenant.used accurate for CPU consumed by
		// goroutines that haven't closed yet.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true))
		return nil
	}

	// Slow path: take a turn to serialize Admit calls. Respects context
	// cancellation while waiting for the turn.
	select {
	case h.admitTurn <- struct{}{}:
		// Got the turn. Release it when we're done.
		defer func() { <-h.admitTurn }()
	case <-ctx.Done():
		// Return grabbed tokens to reservation so setClosed can return
		// them to the granter.
		h.reservation.Add(grabbed)
		return ctx.Err()
	}

	// Re-check after taking the turn: Close may have run, or another
	// goroutine's refill may have replenished the reservation.
	h.mu.Lock()
	closed := h.mu.closed
	h.mu.Unlock()
	if closed {
		// After close, account remaining via BypassAdmission to keep
		// tenant.used accurate.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true))
		return nil
	}
	grabbed2 := h.tryDeductReservation(remaining)
	if grabbed2 == remaining {
		return nil
	}
	remaining -= grabbed2

	// Request the consumed CPU plus a buffer (see refillHeuristic). Setting
	// RequestedCount > 0 skips the WorkQueue's CPU time token estimator
	// (see callerSetRequestedCount in Admit), which is designed for KV
	// requests and should not be trained with SQL CPU data. The buffer
	// portion goes into reservation for future fast-path CAS deductions.
	// Because the exact amount is deducted at Admit time, there is no
	// estimate to correct, so AdmittedWorkDone is not called.
	resp, err := h.wq.Admit(ctx, h.constructWorkInfo(h.refillHeuristic(remaining), false))
	if err != nil {
		// Return grabbed tokens to reservation so setClosed can return
		// them to the granter.
		h.reservation.Add(grabbed + grabbed2)
		return err
	}

	if resp.Enabled {
		buffer := resp.requestedCount - remaining
		// The commit step is atomic under mu: checking closed and adding
		// to reservation cannot interleave with setClosed setting
		// closed=true. If setClosed already ran, we return the buffer.
		// If it hasn't, reservation.Add is visible to setClosed's later
		// Swap(0).
		h.mu.Lock()
		if h.mu.closed {
			h.mu.Unlock()
			if buffer > 0 {
				h.wq.AdmittedSQLWorkDone(h.workInfo.TenantID, buffer)
			}
		} else {
			h.reservation.Add(buffer)
			h.mu.Unlock()
		}
	}
	return nil
}

// TODO(sumeer): see the comment
// https://github.com/cockroachdb/cockroach/pull/161952#pullrequestreview-3741525716
// on additional integrations that may need to call RegisterGoroutine.

// AtGateway returns true if this handle is for work executing at the gateway
// node, as opposed to DistSQL work on a remote node.
func (h *SQLCPUHandle) AtGateway() bool {
	return h.atGateway
}

// IsGoroutineRegistered returns true if the calling goroutine already has a
// registered handle. Unlike RegisterGoroutine, this does not create a new
// handle as a side effect.
func (h *SQLCPUHandle) IsGoroutineRegistered() bool {
	gid := goid.Get()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, gh := range h.mu.gHandles {
		if gh.gid == gid {
			return true
		}
	}
	return false
}

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

// setClosed marks the handle as closed and returns any remaining reservation
// tokens to the granter. It sets mu.closed=true under mu.Lock() to serialize
// with the slow path's post-Admit commit step, then atomically swaps the
// reservation to 0 and returns any tokens. This is non-blocking: if a
// goroutine is in the slow path (blocked in Admit), it will see mu.closed
// when Admit returns and return its buffer tokens via AdmittedSQLWorkDone.
func (h *SQLCPUHandle) setClosed() {
	h.mu.Lock()
	h.mu.closed = true
	h.mu.Unlock()
	if h.wq != nil {
		remaining := h.reservation.Swap(0)
		if remaining > 0 {
			h.wq.AdmittedSQLWorkDone(h.workInfo.TenantID, remaining)
		}
	}
}

// Close is called when no more reporting is needed. It pools
// GoroutineCPUHandles that have been closed. GoroutineCPUHandles that are not
// yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.setClosed()
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
