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
	require.Equal(t, uint32(50), high.Weight)
	require.True(t, high.MaxCPU, "highResourceGroupID seed has MaxCPU=true")

	low := snap[lowResourceGroupID]
	require.Equal(t, uint32(50), low.Weight)
	require.False(t, low.MaxCPU, "lowResourceGroupID seed has MaxCPU=false")
}

func TestResourceGroupConfigHolderSetReplacesPreviousState(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(map[uint64]ResourceGroupConfig{
		42: {Weight: 100, MaxCPU: false},
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
	snap1[999] = ResourceGroupConfig{Weight: 50, MaxCPU: true}
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
		7: {Weight: 100, MaxCPU: false},
	}
	h.Set(input)

	// Mutating the input map after Set must not affect the holder.
	input[7] = ResourceGroupConfig{Weight: 50, MaxCPU: true}
	input[8] = ResourceGroupConfig{Weight: 50, MaxCPU: false}

	snap := h.Snapshot()
	require.Len(t, snap, 1, "input mutation post-Set must not add the new id=8 entry")
	got := snap[7]
	require.Equal(t, uint32(100), got.Weight,
		"input mutation post-Set must not change Weight")
	require.False(t, got.MaxCPU, "input mutation post-Set must not flip MaxCPU")
}

func TestResourceGroupConfigHolderGetOrDefault(t *testing.T) {
	h := newResourceGroupConfigHolder()
	h.Set(map[uint64]ResourceGroupConfig{
		highResourceGroupID: {Weight: 50, MaxCPU: true},
	})

	got := h.GetOrDefault(highResourceGroupID)
	require.Equal(t, uint32(50), got.Weight)
	require.True(t, got.MaxCPU)

	// Unknown ID returns the safety default, NOT the seed for that ID.
	unknown := h.GetOrDefault(9999)
	require.Equal(t, defaultGroupConfig, unknown,
		"unknown ID falls back to defaultGroupConfig")
	require.Equal(t, uint32(defaultGroupWeight), unknown.Weight)
	require.False(t, unknown.MaxCPU)
}

func TestDefaultGroupConfigShape(t *testing.T) {
	// Lock down the safety floor so unrelated changes don't silently weaken
	// the unknown-ID fallback. If this test fails, the change was likely
	// unintentional - revisit the holder field comment before updating.
	require.Equal(t, uint32(defaultGroupWeight), defaultGroupConfig.Weight)
	require.False(t, defaultGroupConfig.MaxCPU)
}
