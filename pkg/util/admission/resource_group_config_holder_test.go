// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceGroupConfigHolderConstructorSeed(t *testing.T) {
	h := newResourceGroupConfigHolder()
	snap := h.Snapshot()

	require.Len(t, snap, len(defaultRMResourceGroupConfig),
		"constructor seed should populate the default high/low groups")
	require.Contains(t, snap, highResourceGroupID)
	require.Contains(t, snap, lowResourceGroupID)

	high := snap[highResourceGroupID]
	require.True(t, high.MaxCPU, "highResourceGroupID seed has MaxCPU=true")
	require.Equal(t, 1.0, high.BurstFrac, "MaxCPU=true sets BurstFrac=1.0")

	low := snap[lowResourceGroupID]
	require.False(t, low.MaxCPU, "lowResourceGroupID seed has MaxCPU=false")
	require.Equal(t, 0.5, low.BurstFrac,
		"two configured groups with equal weight share BurstFrac equally when MaxCPU=false")
}

func TestResourceGroupConfigHolderSetReplacesPreviousState(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(map[uint64]ResourceGroupConfig{
		42: {Weight: 5, MaxCPU: false},
	})

	snap := h.Snapshot()
	require.Len(t, snap, 1, "Set replaces wholesale; default seed should be gone")
	require.Contains(t, snap, uint64(42))
	require.NotContains(t, snap, highResourceGroupID)
}

func TestResourceGroupConfigHolderSnapshotIsIndependent(t *testing.T) {
	h := newResourceGroupConfigHolder()
	snap1 := h.Snapshot()

	// Mutate the snapshot; subsequent snapshot should be unaffected.
	snap1[999] = DerivedGroupConfig{Weight: 1, MaxCPU: true, BurstFrac: 1.0}
	delete(snap1, highResourceGroupID)

	snap2 := h.Snapshot()
	require.NotContains(t, snap2, uint64(999),
		"caller mutations to snap1 must not affect holder state")
	require.Contains(t, snap2, highResourceGroupID,
		"caller deletions from snap1 must not affect holder state")
}

func TestResourceGroupConfigHolderInputAliasingSafety(t *testing.T) {
	h := newResourceGroupConfigHolder()
	input := map[uint64]ResourceGroupConfig{
		7: {Weight: 3, MaxCPU: false},
	}
	h.Set(input)

	// Mutating the input map after Set must not affect the holder.
	input[7] = ResourceGroupConfig{Weight: 999, MaxCPU: true}
	input[8] = ResourceGroupConfig{Weight: 1, MaxCPU: false}

	snap := h.Snapshot()
	require.Len(t, snap, 1, "input mutation post-Set must not add the new id=8 entry")
	got := snap[7]
	require.Equal(t, uint32(3), got.Weight,
		"input mutation post-Set must not raise Weight from 3 to 999")
	require.False(t, got.MaxCPU, "input mutation post-Set must not flip MaxCPU")
	require.Equal(t, 1.0, got.BurstFrac,
		"single configured group with non-MaxCPU gets full BurstFrac (Weight/totalWeight = 3/3)")
}

func TestResourceGroupConfigHolderGetDerivedOrDefault(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(map[uint64]ResourceGroupConfig{
		highResourceGroupID: {Weight: 2, MaxCPU: true},
	})

	got := h.GetDerivedOrDefault(highResourceGroupID)
	require.True(t, got.MaxCPU)
	require.Equal(t, 1.0, got.BurstFrac)

	// Unknown ID returns the safety default, NOT the seed for that ID.
	unknown := h.GetDerivedOrDefault(9999)
	require.Equal(t, defaultDerivedGroupConfig, unknown,
		"unknown ID falls back to defaultDerivedGroupConfig (weight=1, maxCPU=false, burstFrac=0)")
	require.Equal(t, uint32(defaultGroupWeight), unknown.Weight)
	require.False(t, unknown.MaxCPU)
	require.Equal(t, 0.0, unknown.BurstFrac)
}

func TestComputeDerivedBurstFrac(t *testing.T) {
	tests := []struct {
		name              string
		input             map[uint64]ResourceGroupConfig
		expectedBurstFrac map[uint64]float64
	}{
		{
			name: "MaxCPU group always gets BurstFrac=1.0",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 1, MaxCPU: true},
				2: {Weight: 9, MaxCPU: false},
			},
			expectedBurstFrac: map[uint64]float64{
				1: 1.0,
				2: float64(9) / float64(10),
			},
		},
		{
			name: "all-zero weights yield BurstFrac=0",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 0, MaxCPU: false},
				2: {Weight: 0, MaxCPU: false},
			},
			expectedBurstFrac: map[uint64]float64{
				1: 0,
				2: 0,
			},
		},
		{
			name: "MaxCPU=true overrides zero weight",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 0, MaxCPU: true},
			},
			expectedBurstFrac: map[uint64]float64{
				1: 1.0,
			},
		},
		{
			name:              "empty config produces empty derived",
			input:             map[uint64]ResourceGroupConfig{},
			expectedBurstFrac: map[uint64]float64{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := computeDerived(tc.input)
			require.Len(t, got, len(tc.expectedBurstFrac))
			for id, want := range tc.expectedBurstFrac {
				require.Contains(t, got, id)
				require.Equal(t, want, got[id].BurstFrac, "id=%d", id)
			}
		})
	}
}

func TestComputeDerivedWeightScaling(t *testing.T) {
	tests := []struct {
		name           string
		input          map[uint64]ResourceGroupConfig
		expectedWeight map[uint64]uint32
	}{
		{
			name: "weights below cap pass through unchanged",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 5, MaxCPU: false},
				2: {Weight: 10, MaxCPU: false},
			},
			expectedWeight: map[uint64]uint32{
				1: 5,
				2: 10,
			},
		},
		{
			name: "max weight above cap scales all entries down",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 200, MaxCPU: false},
				2: {Weight: 100, MaxCPU: false},
			},
			// maxWeight=200, scaling = 20/200 = 0.1
			// id=1: ceil(200 * 0.1) = 20
			// id=2: ceil(100 * 0.1) = 10
			expectedWeight: map[uint64]uint32{
				1: 20,
				2: 10,
			},
		},
		{
			name: "weights below floor are raised to defaultGroupWeight",
			input: map[uint64]ResourceGroupConfig{
				1: {Weight: 0, MaxCPU: false},
			},
			expectedWeight: map[uint64]uint32{
				1: defaultGroupWeight,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := computeDerived(tc.input)
			require.Len(t, got, len(tc.expectedWeight))
			for id, want := range tc.expectedWeight {
				require.Contains(t, got, id)
				require.Equal(t, want, got[id].Weight, "id=%d", id)
			}
		})
	}
}

func TestDefaultDerivedGroupConfigShape(t *testing.T) {
	// Lock down the safety floor so unrelated changes don't silently weaken
	// the unknown-ID fallback. If this test fails, the change was likely
	// unintentional - revisit the holder field comment before updating.
	require.Equal(t, uint32(defaultGroupWeight), defaultDerivedGroupConfig.Weight)
	require.False(t, defaultDerivedGroupConfig.MaxCPU)
	require.Equal(t, 0.0, defaultDerivedGroupConfig.BurstFrac)
}
