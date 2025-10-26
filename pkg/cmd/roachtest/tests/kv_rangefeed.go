// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package tests

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// kvRangefeedTest is a performance test for rangefeeds. They run a workload
// with a changefeed and ensure that (1) the changefeed enters a steady state
// and (2) the foreground SQL workload isn't impacted.
type kvRangefeedTest struct {
	// writeMaxRate is the req/s of the write workload.
	//
	// TODO(ssd): An issue to keep in mind is that we want read load as well so
	// that we can push CPU on the
	writeMaxRate int64

	// duration is how long the test will run.
	duration time.Duration

	// insertCount is the number of rows to insert into the KV table.
	insertCount int64
	// sinkProvisioning is the throughput of the sink expressed in a percentage of
	// the writeMaxRate.
	sinkProvisioning float64
	// splits is the number of splits to initialize the KV table with.
	splits int

	// expectChangefeedCatchesUp is whether or not we expect the changefeed to
	// catch up in the given configuration. We don't expect under-provisioned
	// changefeeds to catch up.
	expectChangefeedCatchesUp bool
}

func (t kvRangefeedTest) changefeedMaxRate() int64 {
	return int64(float64(t.writeMaxRate) * t.sinkProvisioning)
}

func (t kvRangefeedTest) expectedCatchupDuration() (time.Duration, error) {
	writesToCatchUp := t.insertCount
	catchUpRate := t.changefeedMaxRate() - t.writeMaxRate
	if catchUpRate < 0 {
		return 0, errors.AssertionFailedf("catch-up rate (%d) is negative, catch up will not complete", catchUpRate)
	}
	catchUpTime := time.Duration((writesToCatchUp / catchUpRate)) * time.Second
	return catchUpTime, nil
}

// Rought math: 
// To hit the slow consumer err, 1. overflow catch up buffer 2. overflow buffered sender queue size. 
// Given per-changefeed memory limit is 512 KiB, each event is 1024B (512 KiB / 1024B = 512 events). 
// 25 ranges, 4096 events per range.
// To buffer there full, (rate of incoming - outgoing) * time.Duration(sec) = 512 events.
// During non-catch up, 512/(500-500*0.9) = 10.24s. After that, we will start buffering in buffered sender queue. 
// To overflow buffered sender queue (4096*25), 512/(500-500*0.9) = 10.24s. 
// 
// If we want to overflow with catchUpBuf: 
// During catchup, real live events will be buffered in catch up buffer (4096*25) / 500 = 204.8s.
// After catch up buffer, all events are loaded to buffered sender queue (4096*25) = 102400 events.
// If range overflowed during catch up, they were all disconnected. Otherwise, they are still connected. 
// Then, if events flow in at a rate > 500, buffered sender queue will overflow in 10s. 
// Need to lift the catch up iterator to 64 ranges a time as well. 
func makeKVRangefeedOptions(c cluster.Cluster) (option.StartOpts, install.ClusterSettings) {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	settings := install.MakeClusterSettings()
	// changefeed machinery and instead force the buffered sender to queue.
	settings.ClusterSettings["kv.rangefeed.enabled"] = "true"
	settings.ClusterSettings["kv.rangefeed.concurrent_catchup_iterators"] = "64"
	settings.ClusterSettings["changefeed.memory.per_changefeed_limit"] = "512KiB"
	settings.Env = append(settings.Env, "COCKROACH_RANGEFEED_SEND_TIMEOUT=0")
	startOpts.RoachprodOpts.ExtraArgs = append(
		startOpts.RoachprodOpts.ExtraArgs,
		`--vmodule=replica_rangefeed=5,unbuffered_registration=5,buffered_registration=5,buffered_sender=5,unbuffered_sender=5,stream_manager=5,dist_sender_mux_rangefeed=5,scheduled_processor=5,dist_sender_rangefeed=5,catchup_scan=5`,
	)
	return startOpts, settings
}

func runKVRangefeed(ctx context.Context, t test.Test, c cluster.Cluster, opts kvRangefeedTest) {
	// Check this early to avoid test misconfigurations.
	var catchUpDur time.Duration
	if opts.expectChangefeedCatchesUp {
		var err error
		catchUpDur, err = opts.expectedCatchupDuration()
		if err != nil {
			t.Fatal(err)
		}
		if opts.duration > 0 && opts.duration < catchUpDur {
			t.Fatalf("duration (%s) is insufficient for catch up to complete (%s)", opts.duration, catchUpDur)
		}
	}

	nodes := c.Spec().NodeCount - 1
	startOpts, settings := makeKVRangefeedOptions(c)
	c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.Status("initializing workload")
	// 64 bytes per block 
	// 64 * 1024 = 65536 bytes
	initCmd := fmt.Sprintf("./cockroach workload init kv --splits=%d --read-percent 0 --min-block-bytes=1024 --max-block-bytes=1024 {pgurl:1-%d}",
		opts.splits, nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	var cursorStr string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&cursorStr); err != nil {
		t.Fatal(err)
	}
	t.L().Printf("using cursor %s", cursorStr)

	t.Status("inserting rows")
	initCmd = fmt.Sprintf("./cockroach workload init kv --insert-count %d --read-percent 0 --data-loader=insert --min-block-bytes=1024 --max-block-bytes=1024 {pgurl:1-%d}",
		opts.insertCount, nodes)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)

	t.Status("running workload with changefeed")
	t.L().Printf("inserting %d rows", opts.insertCount)
	if opts.expectChangefeedCatchesUp {
		t.L().Printf("catch-up expected to take %s", catchUpDur)
	}

	const resolvedTarget = 5 * time.Second

	m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		opts := []string{
			"--tolerate-errors",
			roachtestutil.GetWorkloadHistogramArgs(t, c, nil),
			" --changefeed",
			fmt.Sprintf("--changefeed-resolved-target=%s", resolvedTarget),
			fmt.Sprintf("--duration=%s", opts.duration),
			fmt.Sprintf("--changefeed-max-rate=%d", opts.changefeedMaxRate()),
			fmt.Sprintf("--max-rate=%d", opts.writeMaxRate),
			fmt.Sprintf("--changefeed-cursor=%s", cursorStr),
		}

		cmd := fmt.Sprintf("./cockroach workload run kv --read-percent 0 %s {pgurl:1-%d}",
			strings.Join(opts, " "),
			nodes,
		)
		t.L().Printf("Running workload: %s", cmd)
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
		return nil
	})
	m.Wait()

	metrics, err := fetchAndParseMetrics(ctx, t, c)
	if err != nil {
		t.Fatal(err)
	}
	if opts.expectChangefeedCatchesUp {
		catchUpDur, err := opts.expectedCatchupDuration()
		if err != nil {
			t.Fatal(err)
		}

		allowedCatchUpDuration := time.Duration(int64(float64(catchUpDur) * float64(1.1)))
		actualCatchUpDuration := findP99Below(metrics["changefeed-resolved"], resolvedTarget*2)
		if actualCatchUpDuration == 0 {
			t.Fatal("changefeed never caught up")
		} else if actualCatchUpDuration > allowedCatchUpDuration {
			t.Fatalf("changefeed caught up too slowly: %s > %s (%s+10%%)", actualCatchUpDuration, allowedCatchUpDuration, catchUpDur)
		} else {
			t.L().Printf("changefeed caught up quickly enough %s < %s", actualCatchUpDuration, allowedCatchUpDuration)
		}
	}
}

func findP99Below(ticks []exporter.SnapshotTick, target time.Duration) time.Duration {
	startTime := ticks[0].Now
	for _, tick := range ticks {
		if tick.Hist == nil {
			continue
		}

		h := hdrhistogram.Import(tick.Hist)
		if h == nil {
			continue
		}

		p99 := time.Duration(h.ValueAtQuantile(99))
		if p99 > 0 && p99 < target {
			return tick.Now.Sub(startTime)
		}
	}
	return 0
}

func metricsFileName(t test.Test) string {
	return path.Join(t.PerfArtifactsDir(), "stats.json")
}

func fetchAndParseMetrics(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (map[string][]exporter.SnapshotTick, error) {
	localMetricsFile := path.Join(t.ArtifactsDir(), "stats.json")

	if err := c.Get(ctx, t.L(), metricsFileName(t), localMetricsFile, c.WorkloadNode()); err != nil {
		return nil, err
	}
	return parseMetrics(localMetricsFile)
}

func parseMetrics(metricsFile string) (map[string][]exporter.SnapshotTick, error) {
	byType, err := histogram.DecodeSnapshots(metricsFile)
	if err != nil {
		return nil, err
	}
	if _, ok := byType["changefeed-resolved"]; !ok {
		return nil, errors.AssertionFailedf("expected changefeed-resolved series: %v", byType)
	}
	if _, ok := byType["write"]; !ok {
		return nil, errors.AssertionFailedf("expected write series")
	}
	return byType, nil
}

func registerKVRangefeed(r registry.Registry) {
	testConfigs := []struct {
		writeMaxRate              int64
		duration                  time.Duration
		sinkProvisioning          float64
		splits                    int64
		expectChangefeedCatchesUp  bool
		catchUpInterval            time.Duration
	}{
		{
			writeMaxRate:             500,
			duration:                 25 * time.Minute,
			sinkProvisioning:         0.9, // Under provisioned.
			splits:                   25,
			expectChangefeedCatchesUp: false,
			catchUpInterval:          7 * time.Minute,
		},
		// {
		// 	writeMaxRate:             500,
		// 	duration:                 10 * time.Minute,
		// 	sinkProvisioning:         1.2, // Correctly provisioned.
		// 	splits:                   1000,
		// 	expectChangefeedCatchesUp: true,
		// 	catchUpInterval:          1 * time.Minute,
		// },
		// {
		// 	writeMaxRate:             1000,
		// 	sinkProvisioning:         0.9, // Under-provisioned.
		// 	splits:                   1000,
		// 	expectChangefeedCatchesUp: false,
		// },
	}

	for _, opts := range testConfigs {
		testName := fmt.Sprintf("kv-rangefeed/write-rate=%d/sink-rate=%d/catchup=%s/splits=%d",
			opts.writeMaxRate,
			int64(float64(opts.writeMaxRate) * opts.sinkProvisioning),
			opts.catchUpInterval,
			opts.splits,
		)
		r.Add(registry.TestSpec{
			Name:      testName,
			Owner:     registry.OwnerKV,
			Benchmark: true,
			Cluster:   r.MakeClusterSpec(4, spec.CPU(8), spec.WorkloadNode(), spec.WorkloadNodeCPU(4)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runKVRangefeed(ctx, t, c, kvRangefeedTest{
					writeMaxRate: opts.writeMaxRate,
					duration: opts.duration,
					insertCount: int64((opts.catchUpInterval.Seconds()) * float64(opts.writeMaxRate)),
					sinkProvisioning: opts.sinkProvisioning,
					splits: int(opts.splits),
					expectChangefeedCatchesUp: opts.expectChangefeedCatchesUp,
				})
				t.Fatalf("passed but fail for logs")
			},
			CompatibleClouds: registry.AllClouds,
			Suites:           registry.Suites(registry.Nightly),
		})
	}
}
