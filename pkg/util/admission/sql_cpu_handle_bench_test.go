// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// benchGranter is a minimal granter that always grants. No logging, no
// mutex — pure fast path for benchmarking.
type benchGranter struct{}

var _ granterWithStoreReplicatedWorkAdmitted = &benchGranter{}

func (g *benchGranter) tryGet(_ burstQualification, _ int64) bool { return true }
func (g *benchGranter) returnGrant(_ int64)                       {}
func (g *benchGranter) tookWithoutPermission(_ int64)             {}
func (g *benchGranter) continueGrantChain(_ grantChainID)         {}

func (g *benchGranter) storeWriteDone(_ int64, _ StoreWorkDoneInfo) int64 {
	return 0
}

func (g *benchGranter) storeReplicatedWorkAdmittedLocked(
	_ int64, _ storeReplicatedWorkAdmittedInfo,
) int64 {
	return 0
}

// benchSetup creates a WorkQueue in CTT mode and a SQLCPUProvider for
// benchmarking. The WorkQueue uses a benchGranter that always grants.
func benchSetup(b *testing.B) (*WorkQueue, SQLCPUProvider) {
	b.Helper()
	st := cluster.MakeTestingClusterSettings()
	SQLCPUBasedResponseAdmissionEnabled.Override(
		context.Background(), &st.SV, true)

	registry := metric.NewRegistry()
	metrics := makeWorkQueueMetrics("bench", registry)
	opts := makeWorkQueueOptions(KVWork)
	opts.mode = usesCPUTimeTokens
	opts.disableEpochClosingGoroutine = true
	opts.disableGCTenantsAndResetUsed = true
	initialTime := timeutil.FromUnixMicros(
		int64(100) * int64(time.Millisecond/time.Microsecond))
	opts.timeSource = timeutil.NewManualTime(initialTime)
	cpuMetrics := makeCPUTimeTokenMetrics()
	opts.perTenantAggMetrics = &tenantAggMetrics{
		admittedCount:  cpuMetrics.AdmittedCountPerTenant[systemTenant],
		waitTimeNanos:  cpuMetrics.WaitTimeNanosPerTenant[systemTenant],
		tokensUsed:     cpuMetrics.TokensUsedPerTenant[systemTenant],
		tokensReturned: cpuMetrics.TokensReturnedPerTenant[systemTenant],
	}
	g := &benchGranter{}
	q := makeWorkQueue(
		log.MakeTestingAmbientContext(tracing.NewTracer()),
		KVWork, g, st, metrics, opts).(*WorkQueue)
	q.knobs.DisableCPUTimeTokenEstimation = true

	provider := NewSQLCPUProvider(&st.SV, q, q)
	return q, provider
}

// doWork simulates realistic SQL work between admission checkpoints.
// It allocates memory, does random access, and computes a hash to
// defeat CPU cache effects and prevent the compiler from optimizing
// away the work. Returns a value to prevent dead-code elimination.
//
//go:noinline
func doWork(rng *rand.Rand, arena []byte) uint64 {
	// Random allocation to stress the allocator.
	size := 64 + rng.Intn(4096)
	buf := make([]byte, size)

	// Random writes into a large arena to defeat L1/L2 cache.
	for i := 0; i < 16; i++ {
		pos := rng.Intn(len(arena))
		arena[pos] = byte(i)
		buf[rng.Intn(len(buf))] = arena[pos]
	}

	// Hash computation to consume CPU.
	h := fnv.New64a()
	h.Write(buf)
	return h.Sum64()
}

// BenchmarkSettleAndAdmit isolates the settlement path. Each iteration
// drains the token reservation and calls settleAndAdmit, measuring the
// cost of settleMu + mu lock acquisitions and WorkQueue Admit +
// AdmittedWorkDone.
func BenchmarkSettleAndAdmit(b *testing.B) {
	for _, numGoroutines := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			q, provider := benchSetup(b)
			defer q.close()
			b.ReportAllocs()

			handle := provider.GetHandle(SQLWorkInfo{
				TenantID:   roachpb.SystemTenantID,
				Priority:   admissionpb.NormalPri,
				CreateTime: time.Now().UnixNano(),
				AtGateway:  true,
			})
			defer handle.Close()

			// Do one initial settlement to set up q on the handle.
			handle.reservedTokenNanos.Store(0)
			if err := handle.settleAndAdmit(
				context.Background(), q); err != nil {
				b.Fatal(err)
			}

			var settlements atomic.Int64
			itersPerGoroutine := b.N / numGoroutines
			b.ResetTimer()

			var wg sync.WaitGroup
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < itersPerGoroutine; i++ {
						// Drain the reservation to force settlement.
						handle.reservedTokenNanos.Store(0)
						if err := handle.settleAndAdmit(
							context.Background(), q); err != nil {
							return
						}
						settlements.Add(1)
					}
				}()
			}
			wg.Wait()
			b.StopTimer()

			b.ReportMetric(
				float64(settlements.Load())/float64(b.N),
				"settlements/op")
		})
	}
}

// BenchmarkMeasureAndAdmit benchmarks the cancel-checker hot path:
// grunning.Time() measurement, atomic reportCPU deduction, and
// occasional settlement when the reservation is exhausted. Uses real
// CPU time via grunning, with simulated work between measurements.
func BenchmarkMeasureAndAdmit(b *testing.B) {
	q, provider := benchSetup(b)
	defer q.close()
	b.ReportAllocs()

	handle := provider.GetHandle(SQLWorkInfo{
		TenantID:   roachpb.SystemTenantID,
		Priority:   admissionpb.NormalPri,
		CreateTime: time.Now().UnixNano(),
		AtGateway:  true,
	})
	defer handle.Close()

	gh := handle.RegisterGoroutine()
	defer gh.Close(context.Background())

	rng := rand.New(rand.NewSource(1))
	arena := make([]byte, 64*1024) // 64KB to bust L1/L2 cache
	var sink uint64
	var settlements atomic.Int64

	// Record initial reservation for settlement counting.
	prevRemaining := handle.reservedTokenNanos.Load()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink += doWork(rng, arena)
		if err := gh.MeasureAndAdmit(context.Background()); err != nil {
			b.Fatal(err)
		}
		cur := handle.reservedTokenNanos.Load()
		if cur > prevRemaining {
			// Reservation was refilled — a settlement happened.
			settlements.Add(1)
		}
		prevRemaining = cur
	}
	b.StopTimer()

	b.ReportMetric(
		float64(settlements.Load())/float64(b.N),
		"settlements/op")
	// Prevent dead-code elimination.
	if sink == 0 {
		b.Log("sink", sink)
	}
}

// BenchmarkSQLCPUHandleConcurrent simulates multiple concurrent SQL
// queries (handles) sharing a single per-node WorkQueue, each with
// multiple goroutines. This measures WorkQueue mutex contention under
// realistic concurrent access patterns.
func BenchmarkSQLCPUHandleConcurrent(b *testing.B) {
	for _, numHandles := range []int{1, 4, 16} {
		for _, goroutinesPerHandle := range []int{1, 2, 4} {
			name := fmt.Sprintf(
				"handles=%d/goroutines=%d",
				numHandles, goroutinesPerHandle)
			b.Run(name, func(b *testing.B) {
				q, provider := benchSetup(b)
				defer q.close()
				b.ReportAllocs()

				totalGoroutines := numHandles * goroutinesPerHandle
				itersPerGoroutine := b.N / totalGoroutines
				if itersPerGoroutine < 1 {
					itersPerGoroutine = 1
				}

				var settlements atomic.Int64
				b.ResetTimer()

				var wg sync.WaitGroup
				for h := 0; h < numHandles; h++ {
					handle := provider.GetHandle(SQLWorkInfo{
						TenantID:   roachpb.SystemTenantID,
						Priority:   admissionpb.NormalPri,
						CreateTime: time.Now().UnixNano(),
						AtGateway:  h%2 == 0,
					})

					for g := 0; g < goroutinesPerHandle; g++ {
						wg.Add(1)
						go func(handle *SQLCPUHandle, seed int64) {
							defer wg.Done()
							gh := handle.RegisterGoroutine()
							defer gh.Close(context.Background())

							rng := rand.New(rand.NewSource(seed))
							arena := make([]byte, 64*1024)
							var sink uint64
							prevRemaining := handle.reservedTokenNanos.Load()

							for i := 0; i < itersPerGoroutine; i++ {
								sink += doWork(rng, arena)
								if err := gh.MeasureAndAdmit(
									context.Background()); err != nil {
									return
								}
								cur := handle.reservedTokenNanos.Load()
								if cur > prevRemaining {
									settlements.Add(1)
								}
								prevRemaining = cur
							}
							_ = sink
						}(handle, int64(h*goroutinesPerHandle+g))
					}
				}
				wg.Wait()
				b.StopTimer()

				// Close handles after timer stops.
				// (Handles are GC'd; Close is for final settlement.)

				b.ReportMetric(
					float64(settlements.Load())/float64(b.N),
					"settlements/op")
			})
		}
	}
}
