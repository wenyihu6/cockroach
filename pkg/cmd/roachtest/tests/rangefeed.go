// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type rangefeedOpts struct {
	ranges            int
	nodes             int
	duration          time.Duration
	resolvedTarget    time.Duration
	catchUpInterval   string
	maxRate           int
	changefeedMaxRate int
	insertCount       int
}

func registerRangefeed(r registry.Registry) {
	numNodes := 3
	duration := 15 * time.Minute

	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("rangefeed/nodes=%d/duration=%s", numNodes, duration),
		Owner:            registry.OwnerKV,
		Cluster:          r.MakeClusterSpec(numNodes+1, spec.WorkloadNode()), // +1 for workload node
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runRangefeed(ctx, t, c, rangefeedOpts{
				ranges:            1000,
				nodes:             numNodes,
				duration:          duration,
				catchUpInterval:   "3m",
				resolvedTarget:    5 * time.Second,
				maxRate:           500,
				changefeedMaxRate: 300,
				insertCount:       1000,
			})
		},
	})
}

func runRangefeed(ctx context.Context, t test.Test, c cluster.Cluster, opt rangefeedOpts) {
	t.Status("starting cluster")
	c.Start(ctx, t.L(), withRangefeedVMod(option.DefaultStartOpts()), install.MakeClusterSettings(), c.Range(1, opt.nodes))

	t.Status("initializing workload")
	t.L().Printf("creating table with ranges: %d, insert count: %d", opt.ranges, opt.insertCount)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()),
		fmt.Sprintf(`./cockroach workload init kv --splits=%d --insert-count %d {pgurl:1-%d}`, opt.ranges, opt.insertCount, opt.nodes))

	m := c.NewDeprecatedMonitor(ctx, c.Range(1, opt.nodes))
	m.Go(func(ctx context.Context) error {
		t.Status("running workload with changefeed")

		opts := []string{
			"--tolerate-errors",
			fmt.Sprintf("--max-rate=%d", opt.maxRate),
			fmt.Sprintf("--duration=%s", opt.duration),
			"--changefeed",
			fmt.Sprintf("--changefeed-resolved-target=%s", opt.resolvedTarget),
			fmt.Sprintf("--changefeed-start-delay=%s", opt.catchUpInterval),
			fmt.Sprintf("--changefeed-max-rate=%d", opt.changefeedMaxRate),
		}

		cmd := fmt.Sprintf("./cockroach workload run kv %s {pgurl:1-%d}",
			strings.Join(opts, " "),
			opt.nodes,
		)
		t.L().Printf("Running workload: %s", cmd)
		return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
	})
	m.Wait()

	t.L().Printf("Rangefeed test completed successfully")
	t.Fatal("Rangefeed test completed successfully but fail for logs")
}

func withRangefeedVMod(startOpts option.StartOpts) option.StartOpts {
	startOpts.RoachprodOpts.ExtraArgs = append(
		startOpts.RoachprodOpts.ExtraArgs,
		`--vmodule=replica_rangefeed=5,unbuffered_registration=5,buffered_registration=5,buffered_sender=5,unbuffered_sender=5,stream_manager=5,dist_sender_mux_rangefeed=5,scheduled_processor=5,dist_sender_rangefeed=5,catchup_scan=5`,
	)
	return startOpts
}
