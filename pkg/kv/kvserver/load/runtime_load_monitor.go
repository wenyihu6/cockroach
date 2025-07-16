// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// defaultMovingAverageAge defines the effective time window size. With a value
// of 20 and 1-second sampling intervals, newer CPU usage measurements are
// weighted more heavily, and data from roughly the past 20 seconds contributes
// to the average.
const defaultMovingAverageAge = 20

// RunTimeLoadStats is the stats of the runtime load.
type RunTimeLoadStats struct {
	// CPUUsageNanoPerSec is the CPU usage in nano-seconds per second.
	CPUUsageNanoPerSec int64
	// CPUCapacityNanoPerSec is the CPU capacity in logicalCPU nano-seconds per
	// second.
	CPUCapacityNanoPerSec int64
}

// RuntimeLoadMonitor is the monitor for the runtime load.
type RuntimeLoadMonitor struct {
	cpuUsageRefresherInterval    time.Duration
	cpuCapacityRefresherInterval time.Duration
	stopper                      *stop.Stopper
	mu                           struct {
		syncutil.Mutex
		// Last cumulative CPU usage in nano-sec using
		// status.GetProcCPUTime.
		lastCumulativeCPUUsageNano float64
		// Moving average of CPU usage in nano-sec measured using
		// status.GetProcCPUTime.
		cpuUsageNano ewma.MovingAverage
		// logicalCPU-sec per sec capacity of the node measured using
		// status.GetCPUCapacity.
		cpuCapacityPerSec int64
	}
}

func testingNewRuntimeLoadMonitor(
	stopper *stop.Stopper,
	cpuUsageRefresherInterval time.Duration,
	cpuCapacityRefresherInterval time.Duration,
) *RuntimeLoadMonitor {
	if stopper == nil {
		panic("programming error: stopper cannot be nil")
	}
	rlm := &RuntimeLoadMonitor{}
	rlm.stopper = stopper
	rlm.cpuUsageRefresherInterval = cpuUsageRefresherInterval
	rlm.cpuCapacityRefresherInterval = cpuCapacityRefresherInterval
	rlm.mu.cpuUsageNano = ewma.NewMovingAverage(defaultMovingAverageAge)
	return rlm
}

func newRuntimeLoadMonitor(
	stopper *stop.Stopper,
	cpuUsageRefresherInterval time.Duration,
	cpuCapacityRefresherInterval time.Duration,
) *RuntimeLoadMonitor {
	if stopper == nil {
		panic("programming error: stopper cannot be nil")
	}
	rlm := &RuntimeLoadMonitor{}
	rlm.stopper = stopper
	rlm.mu.cpuUsageNano = ewma.NewMovingAverage(defaultMovingAverageAge)
	rlm.cpuUsageRefresherInterval = cpuUsageRefresherInterval
	rlm.cpuCapacityRefresherInterval = cpuCapacityRefresherInterval
	return rlm
}

func (rlm *RuntimeLoadMonitor) GetCPUStats() *RunTimeLoadStats {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	return &RunTimeLoadStats{
		CPUUsageNanoPerSec:    int64(rlm.mu.cpuUsageNano.Value() / float64(rlm.cpuUsageRefresherInterval.Seconds())),
		CPUCapacityNanoPerSec: rlm.mu.cpuCapacityPerSec * (time.Second.Nanoseconds()),
	}
}

func (rlm *RuntimeLoadMonitor) recordCPUUsage(ctx context.Context) error {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	userTime, sysTime, err := status.GetProcCPUTime(ctx)
	if buildutil.CrdbTestBuild && err != nil {
		panic(err)
	}
	// Convert milliseconds to nanoseconds.
	cumulativeCPUUsage := float64(userTime*1e6 + sysTime*1e6)
	if buildutil.CrdbTestBuild && rlm.mu.lastCumulativeCPUUsageNano > cumulativeCPUUsage {
		panic("programming error: last cpu usage is larger than cumulative")
	}
	rlm.mu.cpuUsageNano.Add(cumulativeCPUUsage - rlm.mu.lastCumulativeCPUUsageNano)
	rlm.mu.lastCumulativeCPUUsageNano = cumulativeCPUUsage
	return nil
}

func (rlm *RuntimeLoadMonitor) recordCPUCapacity() {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()
	rlm.mu.cpuCapacityPerSec = int64(status.GetCPUCapacity())
}

func (rlm *RuntimeLoadMonitor) run(ctx context.Context) {
	cpuUsageRefresherTimer := time.NewTicker(rlm.cpuUsageRefresherInterval)
	defer cpuUsageRefresherTimer.Stop()
	cpuCapacityRefresherTimer := time.NewTicker(rlm.cpuCapacityRefresherInterval)
	defer cpuCapacityRefresherTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rlm.stopper.ShouldQuiesce():
			return
		case <-cpuUsageRefresherTimer.C:
			cpuUsageRefresherTimer.Reset(rlm.cpuUsageRefresherInterval)
			if err := rlm.recordCPUUsage(ctx); err != nil {
				log.Errorf(ctx, "error recording CPU usage: %v", err)
				continue
			}
		case <-cpuCapacityRefresherTimer.C:
			cpuCapacityRefresherTimer.Reset(rlm.cpuCapacityRefresherInterval)
			rlm.recordCPUCapacity()
		}
	}
}

func (rlm *RuntimeLoadMonitor) Run(ctx context.Context) {
	_ = rlm.stopper.RunAsyncTask(ctx, "kvserver-runtime-load-monitor", func(ctx context.Context) {
		rlm.run(ctx)
	})
}
