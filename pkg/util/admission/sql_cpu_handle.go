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
// TODO(sumeer): fill in more details.
type SQLCPUHandle struct {
	workInfo SQLWorkInfo
	p        *sqlCPUProviderImpl

	mu struct {
		syncutil.Mutex
		closed   bool
		gHandles []*GoroutineCPUHandle
	}
}

func newSQLCPUAdmissionHandle(workInfo SQLWorkInfo, p *sqlCPUProviderImpl) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo: workInfo,
		p:        p,
	}
	return h
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

// reportCPU reports CPU time consumed by this handle to the provider's
// aggregate counters.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	nanos := int64(diff)
	if h.workInfo.AtGateway {
		h.p.gatewayCPUNanos.Add(nanos)
	} else {
		h.p.distCPUNanos.Add(nanos)
	}
}

// Close is called when no more reporting is needed. It pools
// GoroutineCPUHandles that have been closed. GoroutineCPUHandles that are not
// yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.closed = true
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
	_ = h.measureAndAdmit(ctx, true)
	h.closed.Store(true)
}

// MeasureAndAdmit should be called frequently. The callee will measure the
// CPU time spent in the goroutine and decide whether more CPU needs to be
// allocated. If more CPU is needed, it can block in acquiring CPU tokens.
// Returns a non-nil error iff the context is canceled while waiting.
//
// TODO(sumeer): implement the measurement and admission logic.
func (h *GoroutineCPUHandle) MeasureAndAdmit(ctx context.Context) error {
	return h.measureAndAdmit(ctx, false)
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
	// Report the CPU diff to the provider for aggregate tracking. We report
	// directly to the provider's atomics, categorized by whether this is
	// gateway or DistSQL work. This is used by the capacity model to separate
	// SQL_G (gateway) from SQL_DIST (distributed) CPU.
	h.h.reportCPU(diff)
	h.cpuAccounted += diff
	return nil
}

// PauseMeasuring is used to pause the CPU accounting for this goroutine. It
// must be paired with UnpauseMeasuring. Used when the goroutine is being used
// for KV work. If pause is called multiple times, an equal number of unpause
// calls are needed to resume measuring.
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

// SQLCPUStatsProvider provides cumulative SQL CPU usage statistics, split into
// gateway and distributed (DistSQL) components. Used by NodeCapacityProvider
// to compute SQL CPU rates for the capacity model.
type SQLCPUStatsProvider interface {
	// GetCumulativeSQLCPUNanos returns cumulative CPU time in nanoseconds for
	// gateway SQL work and distributed SQL work respectively.
	GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64)
}

type sqlCPUProviderImpl struct {
	// gatewayCPUNanos accumulates cumulative CPU nanoseconds for SQL work
	// executed at the gateway node (parsing, planning, result serialization).
	gatewayCPUNanos atomic.Int64
	// distCPUNanos accumulates cumulative CPU nanoseconds for distributed SQL
	// work (TableReaders, joins, aggregations, sorts placed at replica nodes).
	distCPUNanos atomic.Int64
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo SQLWorkInfo) *SQLCPUHandle {
	return newSQLCPUAdmissionHandle(workInfo, p)
}

// GetCumulativeSQLCPUNanos implements SQLCPUStatsProvider.
func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.gatewayCPUNanos.Load(), p.distCPUNanos.Load()
}

// NewSQLCPUProvider creates a new SQLCPUProvider and SQLCPUStatsProvider.
// The returned SQLCPUProvider is used for creating per-query CPU handles,
// while the SQLCPUStatsProvider is used for reading aggregate SQL CPU stats
// for the capacity model.
func NewSQLCPUProvider() (SQLCPUProvider, SQLCPUStatsProvider) {
	p := &sqlCPUProviderImpl{}
	return p, p
}
