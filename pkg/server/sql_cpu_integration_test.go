// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSQLCPUIntegration verifies that the SQL CPU accounting and
// admission pipeline is properly wired end-to-end. It starts a test
// server, enables CTT-based SQL admission, runs concurrent SQL
// queries, and verifies that:
//  1. CPU handles are created and goroutines registered (cumulative
//     CPU nanos > 0 when grunning is supported).
//  2. Admission events occur on the CTT WorkQueue (via the
//     WorkQueueAdmitInterceptor testing knob).
//
// This test must run under Bazel (./dev test) for full verification
// because grunning.Time() requires the patched Go runtime.
func TestSQLCPUIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Count admission events via the WorkQueueAdmitInterceptor.
	var admitCount atomic.Int64

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			AdmissionControl: &admission.TestingKnobs{
				WorkQueueAdmitInterceptor: func(info admission.WorkInfo) {
					// Count token-based admissions. CTT-based SQL
					// admission uses RequestedCount > 0 (CPU nanos),
					// distinguishing it from slot-based KV admission.
					if info.RequestedCount > 0 {
						admitCount.Add(1)
					}
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	// Enable CTT-based SQL response admission.
	admission.SQLCPUBasedResponseAdmissionEnabled.Override(
		ctx, &srv.ClusterSettings().SV, true)

	// Retrieve the SQLCPUProvider to check cumulative CPU later.
	cpuProvider := srv.ApplicationLayer().DB().SQLCPUProvider

	// Run concurrent SQL queries that produce many rows, exercising
	// the cancel checker path (MeasureAndAdmit every 1024 rows) and
	// the response admission path (MeasureAndAdmitResponse on KV
	// responses).
	const numClients = 8
	const numQueriesPerClient = 5
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runner := sqlutils.MakeSQLRunner(sqlDB)
			for q := 0; q < numQueriesPerClient; q++ {
				// generate_series produces many rows, triggering the
				// cancel checker's MeasureAndAdmit on each 1024th row.
				runner.Exec(t, fmt.Sprintf(
					"SELECT count(*) FROM generate_series(1, %d)",
					10000+clientID*100))
			}
		}(i)
	}
	wg.Wait()

	// Verify CPU accounting. When grunning is supported (Bazel builds),
	// cumulative CPU nanos should be > 0, proving that handles were
	// created and goroutines measured CPU.
	gatewayCPU, _ := cpuProvider.GetCumulativeSQLCPUNanos()
	if grunning.Supported {
		require.Positive(t, gatewayCPU,
			"expected cumulative gateway CPU nanos > 0 "+
				"when grunning is supported")
	}
	t.Logf("cumulative gateway CPU nanos: %d (grunning.Supported=%v)",
		gatewayCPU, grunning.Supported)

	// Verify admission events occurred on the CTT WorkQueue.
	// The WorkQueueAdmitInterceptor counts token-based admissions
	// (RequestedCount > 0), which are the CTT settlement path.
	count := admitCount.Load()
	if grunning.Supported {
		require.Positive(t, count,
			"expected CTT admission events when grunning is supported")
	}
	t.Logf("CTT admission events: %d", count)
}
