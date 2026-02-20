// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestMultiStoreStartupAdmissionRace starts a 3-node cluster with 3 stores per
// node, creates data to populate replicas across stores, then restarts the
// cluster. On restart, stores have existing replicas with raft logs to catch up
// on, which may trigger the "unable to find queue for store" error during the
// window between Store.Start() and SetPebbleMetricsProvider().
func TestMultiStoreStartupAdmissionRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		numNodes         = 3
		numStoresPerNode = 3
	)

	ser := fs.NewStickyRegistry()

	mkClusterArgs := func() base.TestClusterArgs {
		tcArgs := base.TestClusterArgs{
			ReplicationMode:   base.ReplicationAuto,
			ServerArgsPerNode: map[int]base.TestServerArgs{},
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			},
		}
		for nodeIdx := 0; nodeIdx < numNodes; nodeIdx++ {
			args := base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			}
			args.Knobs.Server = &server.TestingKnobs{StickyVFSRegistry: ser}
			for storeIdx := 0; storeIdx < numStoresPerNode; storeIdx++ {
				args.StoreSpecs = append(args.StoreSpecs, base.StoreSpec{
					InMemory:    true,
					StickyVFSID: fmt.Sprintf("s%d.%d", nodeIdx+1, storeIdx+1),
				})
			}
			tcArgs.ServerArgsPerNode[nodeIdx] = args
		}
		return tcArgs
	}

	// Phase 1: Start cluster, create data to spread replicas across stores.
	t.Log("phase 1: starting cluster and creating data")
	tc := testcluster.StartTestCluster(t, numNodes, mkClusterArgs())

	db := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test.kv (k INT PRIMARY KEY, v STRING)")
	for i := 0; i < 500; i++ {
		db.Exec(t, fmt.Sprintf("INSERT INTO test.kv VALUES (%d, repeat('x', 1024))", i))
	}
	db.Exec(t, "ALTER TABLE test.kv SCATTER")

	t.Log("phase 1: data created, stopping cluster")
	tc.Stopper().Stop(ctx)

	// Phase 2: Restart the cluster. Now stores have existing replicas and raft
	// logs. Check logs for "unable to find queue for store" errors.
	t.Log("phase 2: restarting cluster — check logs for 'unable to find queue for store'")
	tcArgs := mkClusterArgs()
	tcArgs.ParallelStart = true
	tc = testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	t.Log("phase 2: cluster restarted successfully")
	t.FailNow()
}
