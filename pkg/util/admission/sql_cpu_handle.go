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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

// SQLWorkInfo captures identifying information about SQL work for CPU
// accounting and admission.
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

// tokenBudgetChunkNanos is the amount of CPU time tokens (in nanoseconds)
// requested from the granter when the local budget is depleted. Set to 1ms,
// matching the filler goroutine's tick rate. This balances between
// responsiveness (not over-allocating) and efficiency (not hitting the granter
// on every MeasureAndAdmit call).
const tokenBudgetChunkNanos = int64(time.Millisecond)

// SQLCPUHandle manages CPU accounting and admission for SQL work across
// multiple goroutines. It uses a "lease model" for CPU time tokens: an
// initial budget of tokens is obtained from the granter, goroutines deduct
// measured CPU from this shared budget, and when the budget is depleted,
// more tokens are requested (blocking if the system is overloaded). At
// Close, any unused budget is returned to the granter.
type SQLCPUHandle struct {
	workInfo SQLWorkInfo
	p        *sqlCPUProviderImpl

	// granter is the CTT granter for requesting CPU time tokens. Nil when
	// CTT is disabled or unavailable (e.g., shared-process tenants).
	granter *cpuTimeTokenGranter
	// tier is the resource tier for this handle's token bucket lookups.
	tier resourceTier
	// budget tracks the remaining pre-allocated CPU time tokens in
	// nanoseconds. Multiple goroutines deduct from this atomically. When
	// negative, a goroutine must call requestMoreTokens to refill.
	budget atomic.Int64
	// refillMu serializes token refill requests so only one goroutine at
	// a time calls into the granter.
	refillMu syncutil.Mutex

	mu struct {
		syncutil.Mutex
		closed   bool
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
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
// cumulative counter.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	if h.workInfo.AtGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}
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

// requestMoreTokens blocks until more CPU tokens are available from the
// granter. It serializes refill requests via refillMu so only one goroutine
// at a time interacts with the granter. After acquiring refillMu, the budget
// is rechecked in case another goroutine already refilled it.
func (h *SQLCPUHandle) requestMoreTokens(ctx context.Context) error {
	h.refillMu.Lock()
	defer h.refillMu.Unlock()

	// Another goroutine may have already refilled the budget.
	if h.budget.Load() >= 0 {
		return nil
	}

	// Fast path: try to get tokens without waiting.
	if h.granter.tryGet(h.tier, noBurst, tokenBudgetChunkNanos) {
		h.budget.Add(tokenBudgetChunkNanos)
		return nil
	}

	// Slow path: wait for the filler to replenish the token buckets.
	if h.granter.waitForTokens(ctx, h.tier, noBurst, tokenBudgetChunkNanos) {
		h.budget.Add(tokenBudgetChunkNanos)
		return nil
	}
	return ctx.Err()
}

// Close is called when no more reporting is needed. It returns any unused
// token budget to the granter and pools GoroutineCPUHandles that have been
// closed. GoroutineCPUHandles that are not yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.closed = true

	if h.granter != nil {
		remaining := h.budget.Load()
		if remaining > 0 {
			h.granter.returnGrant(remaining)
		} else if remaining < 0 {
			// We used more CPU than we had tokens for (the last
			// MeasureAndAdmit with noWait=true may have gone over budget).
			// Report the overage so the granter's buckets stay accurate.
			h.granter.tookWithoutPermission(-remaining)
		}
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
	h.cpuAccounted += diff
	h.h.reportCPU(diff)

	if h.h.granter != nil {
		h.h.budget.Add(-diff.Nanoseconds())
		if !noWait && h.h.budget.Load() < 0 {
			return h.h.requestMoreTokens(ctx)
		}
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
	// cpuCoords provides access to the CTT granter and settings. Nil when
	// CTT is unavailable (e.g., shared-process tenants or tests).
	cpuCoords *CPUGrantCoordinators
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

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo SQLWorkInfo) *SQLCPUHandle {
	h := newSQLCPUAdmissionHandle(workInfo, p)
	if p.cpuCoords != nil && cpuTimeTokenACIsEnabled(&p.cpuCoords.st.SV) {
		granter := p.cpuCoords.cpuTimeCoord.granter
		tier := appTenant
		if workInfo.TenantID.IsSystem() {
			tier = systemTenant
		}
		h.granter = granter
		h.tier = tier
		// Allocate an initial token budget. If the bucket is exhausted, the
		// handle starts with zero budget and the first MeasureAndAdmit call
		// will block to acquire tokens.
		if granter.tryGet(tier, noBurst, tokenBudgetChunkNanos) {
			h.budget.Store(tokenBudgetChunkNanos)
		}
	}
	return h
}

// NewSQLCPUProvider creates a new SQLCPUProvider. If cpuCoords is non-nil,
// SQLCPUHandles will use the CTT granter for CPU admission control.
func NewSQLCPUProvider(cpuCoords *CPUGrantCoordinators) SQLCPUProvider {
	return &sqlCPUProviderImpl{cpuCoords: cpuCoords}
}
