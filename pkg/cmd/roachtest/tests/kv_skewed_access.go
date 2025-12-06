// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// registerKVSkewedAccess registers a test that runs a KV workload with
// skewed (zipfian) access pattern - 95% reads, 5% writes.
// Modeled after allocbench/nodes=7/cpu=8/kv/r=95/access=skew but
// simplified for local reproduction (~10 minutes).
//
// To run locally:
//
//	./dev build roachtest && ./dev build cockroach
//	./bin/roachtest run kv-skewed-access --local
func registerKVSkewedAccess(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "kv-skewed-access",
		Owner:            registry.OwnerKV,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Cluster:          r.MakeClusterSpec(4, spec.CPU(4), spec.WorkloadNode()),
		Timeout:          15 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runKVSkewedAccess(ctx, t, c)
		},
	})
}

func runKVSkewedAccess(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodes := c.Spec().NodeCount - 1 // exclude workload node

	// Start the cluster with verbose logging for rebalancing.
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
		"--vmodule=store_rebalancer=2,allocator=2,replicate_queue=2")

	settings := install.MakeClusterSettings()
	// Enable MMA (multi-metric allocator) if desired.
	settings.Env = append(settings.Env, "COCKROACH_ALLOW_MMA=true")

	t.Status("starting cluster")
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	// Set load-based rebalancing to use CPU as the objective.
	t.Status("configuring cluster settings")
	if _, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.allocator.load_based_rebalancing = 'leases and replicas'`); err != nil {
		t.Fatalf("failed to set load_based_rebalancing: %v", err)
	}

	// Run the kv workload with a skewed (zipfian) access pattern.
	// This models the allocbench "access=skew" pattern with hot keys.
	t.Status("running kv workload with skewed access pattern")

	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())

	// Parameters modeled after allocbench smallReads.skewed():
	// - readPercent: 95 (mostly reads, some writes)
	// - zipfian: true (skewed access pattern - hot keys)
	// - splits: scaled down from 210 to 50 for faster init
	// - insertCount: scaled down from 10M to 1M for faster init
	// - blockSize: 1 byte (small values)
	// - concurrency: 256 (moderate load)

	m.Go(func(ctx context.Context) error {
		// Duration: 10 minutes for cloud, 2 minutes for local.
		duration := roachtestutil.IfLocal(c, "2m", "10m")

		// Concurrency: lower for local runs.
		concurrency := roachtestutil.IfLocal(c, "64", "256")

		// Insert count: lower for local runs for faster init.
		insertCount := roachtestutil.IfLocal(c, "100000", "1000000")

		// Splits: fewer for local runs.
		splits := roachtestutil.IfLocal(c, "10", "50")

		// Initialize the workload with pre-populated data.
		initCmd := fmt.Sprintf(
			"./cockroach workload init kv "+
				"--insert-count=%s "+
				"--min-block-bytes=1 --max-block-bytes=1 "+
				"--zipfian "+
				"--splits=%s "+
				"{pgurl:1}",
			insertCount, splits,
		)
		t.Status("initializing kv workload")
		if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), initCmd); err != nil {
			return err
		}

		// Run the workload with 95% reads and skewed access.
		runCmd := fmt.Sprintf(
			"./cockroach workload run kv "+
				"--read-percent=95 "+
				"--min-block-bytes=1 --max-block-bytes=1 "+
				"--zipfian "+
				"--concurrency=%s "+
				"--duration=%s "+
				"--tolerate-errors "+
				"{pgurl:1-%d}",
			concurrency, duration, nodes,
		)
		t.Status(fmt.Sprintf("running kv workload for %s", duration))
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runCmd)
	})

	m.Wait()
	t.Status("workload completed successfully")
}
