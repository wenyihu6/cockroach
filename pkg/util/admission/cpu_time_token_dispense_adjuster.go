// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// dispenseFracStep is how much the per-tick dispensing fraction moves
// in either direction on each CPULoad sample. With timePerTick = 1ms
// and CPULoad samples arriving at ~1ms, the fraction can move from
// 1.0 to 0.0 (or back) in ~20 ticks (~20ms). Mirrors the slot
// adjuster's additive ±1 step on overload signals; see
// kvSlotAdjuster.CPULoad.
const dispenseFracStep = 0.05

// cttDispenseAdjuster maintains a per-tick "dispensing fraction" in
// [0, 1] that the cpuTimeTokenAllocator scales every per-bucket
// allocation by before pushing to the granter.
//
// The fraction is driven by a CPULoad feedback loop: when the Go
// scheduler is overloaded (runnable goroutines >= threshold * procs),
// the fraction decays by dispenseFracStep per CPULoad sample,
// withholding tokens so the scheduler backlog can drain. When the
// scheduler is underloaded (runnable goroutines <= (threshold *
// procs) / 2), the fraction recovers by the same step. The threshold
// is the existing KVSlotAdjusterOverloadThreshold cluster setting,
// so the CTT and slot-based admission systems back off and recover
// on the same signal.
//
// This addresses the high-CPU edge case for CTT (issue #168386):
// when a resource group's utilization target is at or near 100% (e.g.
// burstable-limit=100% in resource manager mode), there is no slack
// in the model's refill rate to absorb a Go scheduler backlog. The
// linear model in cpuTimeTokenLinearModel reacts at a 1s timescale,
// which is too slow; this adjuster reacts at the CPULoad cadence
// (~1ms).
//
// The fraction lives on the adjuster, not on the allocator, so the
// CPULoad goroutine (registered via
// goschedstats.RegisterRunnableCountCallback) and the filler
// goroutine (cpuTimeTokenFiller) can update and read it without
// taking a lock. fracBits stores math.Float64bits(curFrac); the
// allocator reads the latest value once per tick.
type cttDispenseAdjuster struct {
	settings *cluster.Settings
	// fracBits stores math.Float64bits(curFrac). Written by the
	// CPULoad goroutine and read by the filler/allocator goroutine.
	fracBits atomic.Uint64
}

var _ CPULoadListener = (*cttDispenseAdjuster)(nil)

func newCTTDispenseAdjuster(settings *cluster.Settings) *cttDispenseAdjuster {
	a := &cttDispenseAdjuster{settings: settings}
	a.fracBits.Store(math.Float64bits(1.0))
	return a
}

// CPULoad implements CPULoadListener. It moves the dispensing
// fraction by ±dispenseFracStep on overload/underload, clamped to
// [0, 1]. The hysteresis band (between threshold/2 and threshold) is
// intentional: it matches the kvSlotAdjuster behavior and avoids
// thrashing on small fluctuations around the threshold.
func (a *cttDispenseAdjuster) CPULoad(runnable int, procs int, _ time.Duration) {
	threshold := int(KVSlotAdjusterOverloadThreshold.Get(&a.settings.SV))
	frac := a.getFrac()
	switch {
	case runnable >= threshold*procs:
		frac = max(0, frac-dispenseFracStep)
	case runnable <= (threshold*procs)/2:
		frac = min(1, frac+dispenseFracStep)
	default:
		return
	}
	a.fracBits.Store(math.Float64bits(frac))
}

// getFrac returns the current dispensing fraction in [0, 1].
func (a *cttDispenseAdjuster) getFrac() float64 {
	return math.Float64frombits(a.fracBits.Load())
}

// setFracForTest overrides the current dispensing fraction. Test-only.
func (a *cttDispenseAdjuster) setFracForTest(frac float64) {
	a.fracBits.Store(math.Float64bits(frac))
}
