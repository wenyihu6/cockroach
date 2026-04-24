// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCTTDispenseAdjuster exercises the CPULoad-driven feedback loop
// that owns the per-tick dispensing fraction. The adjuster mirrors
// kvSlotAdjuster.CPULoad: it backs off when runnable >= threshold *
// procs and recovers when runnable <= (threshold * procs) / 2,
// leaving the fraction unchanged in the hysteresis band between.
func TestCTTDispenseAdjuster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		procs    = 4
		overload = 32 * procs       // matches default KVSlotAdjusterOverloadThreshold
		under    = (32 * procs) / 2 // recovery threshold
		mid      = under + 1        // inside the hysteresis band
	)

	tests := []struct {
		name         string
		startFrac    float64
		runnable     int
		expectedFrac float64
	}{
		{
			name:         "decay when overloaded",
			startFrac:    1.0,
			runnable:     overload,
			expectedFrac: 1.0 - dispenseFracStep,
		},
		{
			name:         "decay clamped at zero",
			startFrac:    0.0,
			runnable:     overload,
			expectedFrac: 0.0,
		},
		{
			name:         "recover when underloaded",
			startFrac:    0.5,
			runnable:     under,
			expectedFrac: 0.5 + dispenseFracStep,
		},
		{
			name:         "recover clamped at one",
			startFrac:    1.0,
			runnable:     under,
			expectedFrac: 1.0,
		},
		{
			name:         "stay put inside hysteresis band",
			startFrac:    0.5,
			runnable:     mid,
			expectedFrac: 0.5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := cluster.MakeClusterSettings()
			a := newCTTDispenseAdjuster(st)
			a.setFracForTest(tc.startFrac)
			a.CPULoad(tc.runnable, procs, time.Millisecond)
			require.InDelta(t, tc.expectedFrac, a.getFrac(), 1e-9)
		})
	}
}

// TestCTTDispenseAdjusterDecayConverges verifies that repeated
// overload samples drive the fraction to 0 in roughly 1/dispenseFracStep
// ticks (~20), and repeated underload samples drive it back to 1.
func TestCTTDispenseAdjusterDecayConverges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		procs    = 4
		overload = 32 * procs
		under    = (32 * procs) / 4 // safely below threshold/2
	)

	st := cluster.MakeClusterSettings()
	a := newCTTDispenseAdjuster(st)
	require.Equal(t, 1.0, a.getFrac())

	// Sustained overload drives the fraction to 0.
	for i := 0; i < 100; i++ {
		a.CPULoad(overload, procs, time.Millisecond)
	}
	require.Equal(t, 0.0, a.getFrac())

	// Sustained underload drives it back to 1.
	for i := 0; i < 100; i++ {
		a.CPULoad(under, procs, time.Millisecond)
	}
	require.Equal(t, 1.0, a.getFrac())
}
