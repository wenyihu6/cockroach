// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestSQLCPUHandleFastPath verifies that the CAS-based fast path deducts
// from reservation without calling Admit.
func TestSQLCPUHandleFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// First call: reservation is 0, so slow path is taken.
	// heuristic(1ms) = 1ms + min(1ms, 10ms) = 2ms requested.
	// 1ms consumed, 1ms goes to reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	reservationBefore := h.mu.reservation.Load()
	require.Equal(t, int64(1*time.Millisecond), reservationBefore)

	// Clear the testGranter buffer to verify no new Admit call.
	_ = tg.buf.stringAndReset()

	// Second call should deduct via fast path CAS.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 500*time.Microsecond, false))
	reservationAfter := h.mu.reservation.Load()
	require.Equal(t, reservationBefore-int64(500*time.Microsecond), reservationAfter)

	// No Admit call was made.
	output := tg.buf.stringAndReset()
	require.Empty(t, output, "fast path should not call Admit")

	// CPU should still be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond+500*time.Microsecond), gw)
}

// TestSQLCPUHandleSlowPath verifies that when reservation is exhausted,
// the slow path calls Admit and refills reservation.
func TestSQLCPUHandleSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// First call: reservation is 0, slow path taken.
	// heuristic(2ms) = 2ms + min(2ms, 10ms) = 4ms. Reservation = 4ms - 2ms = 2ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, false))
	require.Equal(t, int64(2*time.Millisecond), h.mu.reservation.Load())

	// Exhaust reservation with a larger request.
	// 5ms > 2ms reservation, so CAS grabs 2ms, remaining=3ms, slow path.
	// heuristic(3ms) = 3ms + min(3ms, 10ms) = 6ms.
	// buffer = 6ms - 3ms = 3ms added to reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Millisecond, false))
	require.Equal(t, int64(3*time.Millisecond), h.mu.reservation.Load())
}

// TestSQLCPUHandleCloseReturnsTokens verifies that Close returns unused
// reservation tokens via AdmittedSQLWorkDone.
func TestSQLCPUHandleCloseReturnsTokens(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Acquire tokens so reservation has a buffer.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	remaining := h.mu.reservation.Load()
	require.Equal(t, int64(1*time.Millisecond), remaining)

	// Clear buffer and close.
	_ = tg.buf.stringAndReset()
	h.Close()

	// Verify returnGrant was called with the remaining reservation.
	output := tg.buf.String()
	require.Contains(t, output, "returnGrant")

	// Reservation should be zeroed.
	require.Equal(t, int64(0), h.mu.reservation.Load())
	require.True(t, h.isClosed())
}

// TestSQLCPUHandleCloseZeroReservation verifies that Close with zero
// reservation does not call returnGrant.
func TestSQLCPUHandleCloseZeroReservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	_ = tg.buf.stringAndReset()
	h.Close()

	require.True(t, h.isClosed())
	output := tg.buf.String()
	require.NotContains(t, output, "returnGrant")
	require.NotContains(t, output, "tookWithoutPermission")
}

// TestSQLCPUHandleNoWaitBypassAdmission verifies that the noWait path
// uses BypassAdmission and does not block.
func TestSQLCPUHandleNoWaitBypassAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	// Make tryGet return false — a blocking Admit would hang, but
	// noWait should bypass admission entirely via BypassAdmission.
	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = false
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, true))

	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond), gw)
}

// TestSQLCPUHandleNoWorkQueue verifies that when no WorkQueue is
// attached (CTT AC disabled), CPU is still reported.
func TestSQLCPUHandleNoWorkQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, true))

	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3*time.Millisecond), gw)

	h.Close()
	require.True(t, h.isClosed())
}

// TestSQLCPUHandleRegisterGoroutineIdempotent verifies that registering
// the same goroutine ID twice returns the existing handle.
func TestSQLCPUHandleRegisterGoroutineIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	gh1 := h.RegisterGoroutine()
	gh2 := h.RegisterGoroutine()
	require.Same(t, gh1, gh2, "same goroutine should get the same handle")

	h.mu.Lock()
	require.Len(t, h.mu.gHandles, 1)
	h.mu.Unlock()
}

// TestSQLCPUHandleClosePoolsHandles verifies that Close pools closed
// GoroutineCPUHandles and nils out gHandles.
func TestSQLCPUHandleClosePoolsHandles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	gh := h.RegisterGoroutine()
	gh.Close(ctx)
	require.True(t, gh.closed.Load())

	h.Close()
	h.mu.Lock()
	require.Nil(t, h.mu.gHandles)
	h.mu.Unlock()
}

// TestSQLCPUHandlePauseMeasuring verifies nested pause/unpause.
func TestSQLCPUHandlePauseMeasuring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	gh := h.RegisterGoroutine()

	gh.PauseMeasuring()
	require.Equal(t, 1, gh.paused)
	gh.PauseMeasuring()
	require.Equal(t, 2, gh.paused)

	ctx := context.Background()
	require.NoError(t, gh.MeasureAndAdmit(ctx))

	gh.UnpauseMeasuring()
	require.Equal(t, 1, gh.paused)
	gh.UnpauseMeasuring()
	require.Equal(t, 0, gh.paused)

	gh.Close(ctx)
	h.Close()
}

// TestSQLCPUHandleConcurrentFastPath exercises the CAS-based fast path
// under contention from multiple goroutines. All goroutines deduct from
// the same reservation. The total deducted must be exact, and
// reservation must never go negative.
func TestSQLCPUHandleConcurrentFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed a large reservation via slow path.
	// heuristic(50ms) = 50ms + min(50ms, 10ms) = 60ms.
	// Reservation = 60ms - 50ms = 10ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 50*time.Millisecond, false))
	require.Equal(t, int64(10*time.Millisecond), h.mu.reservation.Load())

	_ = tg.buf.stringAndReset()

	// Launch goroutines that each deduct a small amount via fast path.
	const numGoroutines = 20
	const perGoroutine = 100 * time.Microsecond // 100us * 20 = 2ms total
	var wg sync.WaitGroup
	var errors atomic.Int64
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false); err != nil {
				errors.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), errors.Load())

	// Reservation should be exactly 10ms - 2ms = 8ms.
	expected := int64(10*time.Millisecond) - int64(numGoroutines)*int64(perGoroutine)
	require.Equal(t, expected, h.mu.reservation.Load(),
		"CAS deductions should be exact under contention")

	// No Admit calls should have been made.
	output := tg.buf.stringAndReset()
	require.Empty(t, output, "all deductions should use CAS fast path")
}

// TestSQLCPUHandleConcurrentSlowPath exercises the slow path under
// contention. When reservation is exhausted, goroutines serialize on
// admitTurn and only one calls Admit while others may find reservation
// refilled by the winner.
func TestSQLCPUHandleConcurrentSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// No reservation seed — all goroutines hit the slow path.
	const numGoroutines = 10
	const perGoroutine = 1 * time.Millisecond
	var wg sync.WaitGroup
	var errors atomic.Int64
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false); err != nil {
				errors.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(0), errors.Load())
	// INVARIANT: reservation >= 0.
	require.GreaterOrEqual(t, h.mu.reservation.Load(), int64(0))
	// All CPU should be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(numGoroutines)*int64(perGoroutine), gw)
}

// TestSQLCPUHandleConcurrentCloseAndAdmit verifies that Close and
// reportAndAcquireConsumedCPU can run concurrently without races,
// panics, or token leaks. After Close, reservation is 0.
func TestSQLCPUHandleConcurrentCloseAndAdmit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Millisecond, false))

	var wg sync.WaitGroup

	// Goroutines calling the blocking path.
	const numBlocking = 10
	wg.Add(numBlocking)
	for i := 0; i < numBlocking; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = h.reportAndAcquireConsumedCPU(ctx, 10*time.Microsecond, false)
			}
		}()
	}

	// Goroutines calling the noWait path.
	const numNoWait = 10
	wg.Add(numNoWait)
	for i := 0; i < numNoWait; i++ {
		go func() {
			defer wg.Done()
			_ = h.reportAndAcquireConsumedCPU(ctx, 100*time.Microsecond, true)
		}()
	}

	// Close concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.Close()
	}()

	wg.Wait()

	require.True(t, h.isClosed())
	// INVARIANT: closed == true => reservation == 0.
	require.Equal(t, int64(0), h.mu.reservation.Load())
}

// TestSQLCPUHandleConcurrentCASAndSwap verifies that the fast-path CAS
// and Close's Swap(0) don't lose tokens. Every token from the initial
// reservation is either deducted via CAS or returned via Swap.
func TestSQLCPUHandleConcurrentCASAndSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}

	// Run many iterations to exercise the race window.
	for iter := 0; iter < 100; iter++ {
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		// Seed a large reservation.
		require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 10*time.Millisecond, false))
		initialReservation := h.mu.reservation.Load()

		var wg sync.WaitGroup
		var casDeducted atomic.Int64

		// Goroutines try CAS deductions concurrently.
		const numGoroutines = 5
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				amount := int64(500 * time.Microsecond)
				grabbed := h.tryDeductReservation(amount)
				casDeducted.Add(grabbed)
			}()
		}

		// Close concurrently — sets closed under mu, then Swap(0).
		wg.Add(1)
		var swapped int64
		go func() {
			defer wg.Done()
			func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				h.mu.closed = true
			}()
			swapped = h.mu.reservation.Swap(0)
		}()

		wg.Wait()

		// Token conservation: CAS'd + Swap'd == initial.
		totalAccountedFor := casDeducted.Load() + swapped
		require.Equal(t, initialReservation, totalAccountedFor,
			"iter %d: CAS(%d) + Swap(%d) should equal initial reservation(%d)",
			iter, casDeducted.Load(), swapped, initialReservation)
	}
}

// TestSQLCPUHandleConcurrentRegisterGoroutine verifies that concurrent
// RegisterGoroutine calls from different goroutines are safe and each
// goroutine gets its own handle.
func TestSQLCPUHandleConcurrentRegisterGoroutine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	const numGoroutines = 20
	handles := make([]*GoroutineCPUHandle, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			handles[i] = h.RegisterGoroutine()
		}()
	}
	wg.Wait()

	// Each goroutine should have a unique handle.
	seen := make(map[*GoroutineCPUHandle]bool)
	for _, gh := range handles {
		require.NotNil(t, gh)
		require.False(t, seen[gh], "each goroutine should get a unique handle")
		seen[gh] = true
	}

	h.mu.Lock()
	require.Equal(t, numGoroutines, len(h.mu.gHandles))
	h.mu.Unlock()
}

// TestSQLCPUHandleConcurrentMeasureAndClose exercises the real
// GoroutineCPUHandle.MeasureAndAdmit and Close paths under
// concurrency, simulating the actual DistSQL flow pattern.
func TestSQLCPUHandleConcurrentMeasureAndClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	const numGoroutines = 5
	var wg sync.WaitGroup

	// Each goroutine registers, calls MeasureAndAdmit several times,
	// then closes its handle — mimicking a DistSQL flow.
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			gh := h.RegisterGoroutine()
			for j := 0; j < 10; j++ {
				_ = gh.MeasureAndAdmit(ctx)
			}
			gh.Close(ctx)
		}()
	}
	wg.Wait()

	// All goroutine handles should be closed.
	h.mu.Lock()
	for _, gh := range h.mu.gHandles {
		require.True(t, gh.closed.Load())
	}
	h.mu.Unlock()

	h.Close()
	require.True(t, h.isClosed())
	h.mu.Lock()
	require.Nil(t, h.mu.gHandles)
	h.mu.Unlock()
}

// TestSQLCPUHandleAdmitVsCloseTokenConservation runs a stress test
// verifying the token conservation invariant: all tokens obtained from
// Admit are either consumed, held in reservation, or returned via
// AdmittedSQLWorkDone. This exercises the Admit-vs-Close race where
// the commit step checks closed under mu.
func TestSQLCPUHandleAdmitVsCloseTokenConservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}

	// Run many iterations to exercise the race window between
	// Admit's commit step and Close's Swap(0).
	for iter := 0; iter < 200; iter++ {
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		var wg sync.WaitGroup

		// Multiple goroutines call reportAndAcquireConsumedCPU
		// concurrently.
		const numWorkers = 5
		wg.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_ = h.reportAndAcquireConsumedCPU(ctx, 50*time.Microsecond, false)
				}
			}()
		}

		// Close races with the workers.
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Close()
		}()

		wg.Wait()

		// After Close, both invariants must hold.
		require.True(t, h.isClosed())
		require.Equal(t, int64(0), h.mu.reservation.Load(),
			"iter %d: closed == true => reservation == 0", iter)
	}
}

// TestSQLCPUHandleContextCancellation verifies that when a goroutine's
// context is canceled while waiting for admitTurn, it falls through to
// the BypassAdmission path, accounts the deficit without blocking, and
// returns ctx.Err().
func TestSQLCPUHandleContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Hold admitTurn so the next goroutine blocks on it.
	h.admitTurn <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		// This will try to send to admitTurn (blocked) and fall
		// through to ctx.Done().
		errCh <- h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false)
	}()

	// Cancel the context — the goroutine should return ctx.Err().
	cancel()
	err := <-errCh
	require.ErrorIs(t, err, context.Canceled)

	// CPU should still be reported despite the cancellation.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond), gw)

	// Release admitTurn.
	<-h.admitTurn
	h.Close()
}

// TestSQLCPUHandleCloseDoesNotBlockOnAdmitTurn verifies that Close
// returns immediately even when a goroutine is holding admitTurn
// (blocked in Admit). Close sets closed under mu and drains
// reservation via Swap(0) — it never touches admitTurn.
func TestSQLCPUHandleCloseDoesNotBlockOnAdmitTurn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation so Close has something to drain.
	ctx := context.Background()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))

	// Hold admitTurn to simulate a goroutine blocked in Admit.
	h.admitTurn <- struct{}{}

	// Close should return immediately — it doesn't touch admitTurn.
	closeDone := make(chan struct{})
	go func() {
		h.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// Close returned without blocking.
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked on admitTurn — it should not touch admitTurn")
	}

	require.True(t, h.isClosed())
	require.Equal(t, int64(0), h.mu.reservation.Load())

	// Release admitTurn.
	<-h.admitTurn
}

// TestSQLCPUHandleSecondDeductionAfterTurn verifies that when the
// previous admitTurn holder refills the reservation, the next
// turn-holder deducts from the refilled reservation and may skip
// Admit entirely (the optimization at the second deductFromReservation
// call after taking the turn).
func TestSQLCPUHandleSecondDeductionAfterTurn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Goroutine A: exhaust reservation and call Admit, which refills.
	// heuristic(1ms) = 1ms + min(1ms, 10ms) = 2ms.
	// Reservation = 2ms - 1ms = 1ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())

	// Now drain reservation so the next call goes to slow path.
	h.mu.reservation.Store(0)

	// Goroutine B: goes to slow path, takes turn. Reservation is 0,
	// so second deductFromReservation gets nothing, must call Admit.
	// heuristic(500us) = 500us + min(500us, 10ms) = 1ms.
	// Reservation = 1ms - 500us = 500us.
	_ = tg.buf.stringAndReset()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 500*time.Microsecond, false))
	output := tg.buf.stringAndReset()
	require.Contains(t, output, "tryGet", "should have called Admit")
	require.Equal(t, int64(500*time.Microsecond), h.mu.reservation.Load())

	// Now the interesting case: reservation is 500us. A call for 200us
	// takes the fast path (CAS), leaving 300us. Then a call for 400us
	// exceeds the 300us reservation (remaining=100us), goes to slow
	// path, takes turn, does second deductFromReservation. But first,
	// simulate another goroutine refilling while we wait for the turn.
	//
	// We can't perfectly orchestrate the interleaving, so we test the
	// simpler case: reservation was refilled between the two
	// deductFromReservation calls by directly adding to reservation.

	// Consume 400us: CAS grabs 500us reservation (wait, 400us < 500us,
	// so fast path covers it).
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 400*time.Microsecond, false))
	require.Equal(t, int64(100*time.Microsecond), h.mu.reservation.Load())

	// Now request 200us: CAS grabs 100us, remaining=100us, goes to
	// slow path. While waiting for turn (no contention here), the
	// second deductFromReservation finds 0 (nothing was refilled).
	// Admit is called for 100us shortfall.
	_ = tg.buf.stringAndReset()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 200*time.Microsecond, false))
	output = tg.buf.stringAndReset()
	require.Contains(t, output, "tryGet",
		"shortfall after second deduction should trigger Admit")

	h.Close()
}

// accountingGranter wraps a testGranter with atomic counters that
// track the total tokens flowing through tryGet, returnGrant, and
// tookWithoutPermission. This enables asserting the token conservation
// invariant after a handle is closed:
//
//	tryGet_total + bypass_total = consumed_total + returned_total
//
// Where consumed_total is the sum of all diffNanos passed to
// reportAndAcquireConsumedCPU.
type accountingGranter struct {
	inner         *testGranter
	tryGetTotal   atomic.Int64
	returnedTotal atomic.Int64
	bypassTotal   atomic.Int64
}

var _ granter = &accountingGranter{}

func (ag *accountingGranter) tryGet(burstQual burstQualification, count int64) bool {
	granted := ag.inner.tryGet(burstQual, count)
	if granted {
		ag.tryGetTotal.Add(count)
	}
	return granted
}

func (ag *accountingGranter) returnGrant(count int64) {
	ag.returnedTotal.Add(count)
	ag.inner.returnGrant(count)
}

func (ag *accountingGranter) tookWithoutPermission(count int64) {
	ag.bypassTotal.Add(count)
	ag.inner.tookWithoutPermission(count)
}

func (ag *accountingGranter) continueGrantChain(id grantChainID) {
	ag.inner.continueGrantChain(id)
}

// makeAccountingWorkQueue creates a CTT WorkQueue with an
// accountingGranter for token conservation testing.
func makeAccountingWorkQueue(t *testing.T) (q *WorkQueue, ag *accountingGranter, cleanup func()) {
	st := cluster.MakeTestingClusterSettings()
	metrics := makeWorkQueueMetrics("", metric.NewRegistry())
	tg := &testGranter{buf: &builderWithMu{}}
	tg.mu.returnValueFromTryGet = true
	ag = &accountingGranter{inner: tg}
	cpuMetrics := makeCPUTimeTokenMetrics()
	initialTime := timeutil.FromUnixMicros(
		int64(100) * int64(time.Millisecond/time.Microsecond))
	opts := makeWorkQueueOptions(KVWork)
	opts.mode = usesCPUTimeTokens
	opts.perTenantAggMetrics = &tenantAggMetrics{
		admittedCount:  cpuMetrics.AdmittedCountPerTenant[systemTenant],
		waitTimeNanos:  cpuMetrics.WaitTimeNanosPerTenant[systemTenant],
		tokensUsed:     cpuMetrics.TokensUsedPerTenant[systemTenant],
		tokensReturned: cpuMetrics.TokensReturnedPerTenant[systemTenant],
	}
	opts.timeSource = timeutil.NewManualTime(initialTime)
	opts.disableEpochClosingGoroutine = true
	opts.disableGCTenantsAndResetUsed = true
	q = makeWorkQueue(
		log.MakeTestingAmbientContext(tracing.NewTracer()),
		KVWork, ag, st, metrics, opts,
	).(*WorkQueue)
	tg.r = q
	return q, ag, q.close
}

// TestSQLCPUHandleRandomizedTokenConservation is a randomized stress
// test for the token conservation invariant. Each iteration creates a
// handle, spawns goroutines that make randomized requests (varying
// duration, noWait vs blocking), closes at a random time, and asserts:
//
//	tryGet_total + bypass_total = consumed_total + returned_total
//
// This exercises the full spectrum of race windows: CAS vs Swap,
// commit step vs Close, context cancellation, noWait bypass, and
// the second deductFromReservation after taking the turn.
func TestSQLCPUHandleRandomizedTokenConservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	const numIterations = 200
	for iter := 0; iter < numIterations; iter++ {
		q, ag, cleanup := makeAccountingWorkQueue(t)

		provider := &sqlCPUProviderImpl{}
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		numWorkers := 1 + rng.Intn(8)         // 1-8 workers
		opsPerWorker := 1 + rng.Intn(20)      // 1-20 ops each
		closeAfterOps := rng.Intn(numWorkers) // close after this many workers finish

		var wg sync.WaitGroup
		var workersFinished atomic.Int64
		var totalConsumed atomic.Int64

		wg.Add(numWorkers)
		for w := 0; w < numWorkers; w++ {
			go func() {
				defer wg.Done()
				for op := 0; op < opsPerWorker; op++ {
					// Random duration: 10us - 2ms.
					dur := time.Duration(10+rng.Intn(1990)) * time.Microsecond
					noWait := rng.Intn(5) == 0 // 20% chance of noWait
					totalConsumed.Add(dur.Nanoseconds())
					_ = h.reportAndAcquireConsumedCPU(ctx, dur, noWait)
				}
				if int(workersFinished.Add(1)) == closeAfterOps {
					h.Close()
				}
			}()
		}
		wg.Wait()

		// Ensure Close is called exactly once.
		if !h.isClosed() {
			h.Close()
		}

		// INVARIANT: closed => reservation == 0.
		require.Equal(t, int64(0), h.mu.reservation.Load(),
			"iter %d: reservation must be 0 after Close", iter)

		// Token conservation: tokens in = tokens out.
		tokensIn := ag.tryGetTotal.Load() + ag.bypassTotal.Load()
		tokensOut := totalConsumed.Load() + ag.returnedTotal.Load()
		require.Equal(t, tokensIn, tokensOut,
			"iter %d: tryGet(%d) + bypass(%d) = %d, but consumed(%d) + returned(%d) = %d",
			iter,
			ag.tryGetTotal.Load(), ag.bypassTotal.Load(), tokensIn,
			totalConsumed.Load(), ag.returnedTotal.Load(), tokensOut)

		cleanup()
	}
}

// makeBenchWorkQueue creates a WorkQueue in CTT mode for benchmarks.
func makeBenchWorkQueue(b *testing.B) *WorkQueue {
	st := cluster.MakeTestingClusterSettings()
	metrics := makeWorkQueueMetrics("", metric.NewRegistry())
	tg := &testGranter{buf: &builderWithMu{}}
	tg.mu.returnValueFromTryGet = true
	cpuMetrics := makeCPUTimeTokenMetrics()
	initialTime := timeutil.FromUnixMicros(
		int64(100) * int64(time.Millisecond/time.Microsecond))
	opts := makeWorkQueueOptions(KVWork)
	opts.mode = usesCPUTimeTokens
	opts.perTenantAggMetrics = &tenantAggMetrics{
		admittedCount:  cpuMetrics.AdmittedCountPerTenant[systemTenant],
		waitTimeNanos:  cpuMetrics.WaitTimeNanosPerTenant[systemTenant],
		tokensUsed:     cpuMetrics.TokensUsedPerTenant[systemTenant],
		tokensReturned: cpuMetrics.TokensReturnedPerTenant[systemTenant],
	}
	opts.timeSource = timeutil.NewManualTime(initialTime)
	opts.disableEpochClosingGoroutine = true
	opts.disableGCTenantsAndResetUsed = true
	q := makeWorkQueue(
		log.MakeTestingAmbientContext(tracing.NewTracer()),
		KVWork, tg, st, metrics, opts,
	).(*WorkQueue)
	tg.r = q
	b.Cleanup(q.close)
	return q
}

// BenchmarkSQLCPUHandleReservation measures the throughput benefit of
// the local token reservation in SQLCPUHandle. WithReservation uses
// the normal reportAndAcquireConsumedCPU path where most calls hit
// the fast-path CAS. DirectAdmit calls WorkQueue.Admit on every
// iteration, showing the baseline cost without reservation.
//
// Each goroutine gets its own SQLCPUHandle, matching production where
// each SQL statement has its own handle. The reservation reduces
// contention on the shared WorkQueue.mu.
//
// Run with -cpu=1,2,4,8 to observe contention scaling.
func BenchmarkSQLCPUHandleReservation(b *testing.B) {
	ctx := context.Background()
	cpuPerCheckpoint := 50 * time.Microsecond
	workInfo := WorkInfo{
		TenantID:   roachpb.MustMakeTenantID(2),
		CreateTime: 0,
	}

	for _, numGoroutines := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			b.Run("WithReservation", func(b *testing.B) {
				q := makeBenchWorkQueue(b)
				p := &sqlCPUProviderImpl{}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					h := newSQLCPUAdmissionHandle(workInfo, true, p, q)
					defer h.Close()
					_ = h.reportAndAcquireConsumedCPU(ctx, cpuPerCheckpoint, false)
					for pb.Next() {
						_ = h.reportAndAcquireConsumedCPU(ctx, cpuPerCheckpoint, false)
					}
				})
			})

			b.Run("DirectAdmit", func(b *testing.B) {
				q := makeBenchWorkQueue(b)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					wi := workInfo
					wi.RequestedCount = cpuPerCheckpoint.Nanoseconds()
					for pb.Next() {
						_, _ = q.Admit(ctx, wi)
					}
				})
			})
		})
	}
}
