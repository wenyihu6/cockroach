// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceGroupRegistry(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		r := NewResourceGroupRegistry()
		require.Equal(t, 1, r.NumGroups())
		require.Equal(t, int32(100), r.TotalWeight())

		g := r.GetGroup(DefaultResourceGroupID)
		require.Equal(t, "default", g.Name)
		require.Equal(t, int32(100), g.WeightCPU)
		require.True(t, g.MaxCPU)
	})

	t.Run("serverless", func(t *testing.T) {
		r := NewServerlessResourceGroupRegistry()
		require.Equal(t, 2, r.NumGroups())
		require.Equal(t, int32(175), r.TotalWeight())

		sys := r.GetGroup(ResourceGroupID(0))
		require.Equal(t, "system", sys.Name)
		require.True(t, sys.MaxCPU)

		app := r.GetGroup(ResourceGroupID(1))
		require.Equal(t, "app", app.Name)
	})

	t.Run("add_groups", func(t *testing.T) {
		r := NewResourceGroupRegistry()
		id1 := r.AddGroup(ResourceGroupConfig{Name: "analytics", WeightCPU: 50, MaxCPU: false})
		require.Equal(t, ResourceGroupID(1), id1)
		require.Equal(t, 2, r.NumGroups())
		require.Equal(t, int32(150), r.TotalWeight())

		id2 := r.AddGroup(ResourceGroupConfig{Name: "batch", WeightCPU: 25, MaxCPU: false})
		require.Equal(t, ResourceGroupID(2), id2)
		require.Equal(t, 3, r.NumGroups())
		require.Equal(t, int32(175), r.TotalWeight())
	})

	t.Run("update_group", func(t *testing.T) {
		r := NewResourceGroupRegistry()
		r.AddGroup(ResourceGroupConfig{Name: "analytics", WeightCPU: 50, MaxCPU: false})
		require.Equal(t, int32(150), r.TotalWeight())

		r.UpdateGroup(ResourceGroupID(1), ResourceGroupConfig{Name: "analytics", WeightCPU: 80, MaxCPU: true})
		require.Equal(t, int32(180), r.TotalWeight())
		g := r.GetGroup(ResourceGroupID(1))
		require.Equal(t, int32(80), g.WeightCPU)
		require.True(t, g.MaxCPU)
	})

	t.Run("snapshot", func(t *testing.T) {
		r := NewResourceGroupRegistry()
		r.AddGroup(ResourceGroupConfig{Name: "analytics", WeightCPU: 50, MaxCPU: false})
		groups, total := r.Snapshot()
		require.Equal(t, 2, len(groups))
		require.Equal(t, int32(150), total)
		require.Equal(t, "default", groups[0].Name)
		require.Equal(t, "analytics", groups[1].Name)
	})

	t.Run("compute_target_utilizations", func(t *testing.T) {
		r := NewResourceGroupRegistry()
		// default group: weight=100, MaxCPU=true
		// Add analytics: weight=100, MaxCPU=false
		r.AddGroup(ResourceGroupConfig{Name: "analytics", WeightCPU: 100, MaxCPU: false})
		// Total weight = 200, so each group gets 50% weight fraction.

		targets := r.ComputeTargetUtilizations(0.8, 0.05)
		require.Equal(t, 2, len(targets))

		// Group 0 (default, MaxCPU=true): noBurst = 0.8 * 0.5 = 0.4
		require.InDelta(t, 0.4, targets[0].noBurst, 0.001)
		// canBurst = 0.4 + 0.05*0.5 = 0.425
		require.InDelta(t, 0.425, targets[0].canBurst, 0.001)

		// Group 1 (analytics, MaxCPU=false): noBurst = min(0.8*0.5, 0.75*0.5) = min(0.4, 0.375) = 0.375
		require.InDelta(t, 0.375, targets[1].noBurst, 0.001)
		// canBurst = min(0.4+0.025, 0.75*0.5) = min(0.425, 0.375) = 0.375
		require.InDelta(t, 0.375, targets[1].canBurst, 0.001)
	})
}

func TestCPUTimeTokenGranterDynamic(t *testing.T) {
	t.Run("3_tiers", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3)
		require.Equal(t, 3, granter.numTiers)
		require.Equal(t, 3, len(granter.requester))
		require.Equal(t, 3, len(granter.mu.buckets))
	})

	t.Run("refill_dynamic", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3)
		// No requesters, so no granting will happen.

		toAdd := makeTokenCounts(3)
		toAdd[0] = [numBurstQualifications]int64{1000, 800}
		toAdd[1] = [numBurstQualifications]int64{500, 400}
		toAdd[2] = [numBurstQualifications]int64{250, 200}

		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps)

		granter.mu.Lock()
		require.Equal(t, int64(1000), granter.mu.buckets[0][canBurst].tokens)
		require.Equal(t, int64(800), granter.mu.buckets[0][noBurst].tokens)
		require.Equal(t, int64(500), granter.mu.buckets[1][canBurst].tokens)
		require.Equal(t, int64(400), granter.mu.buckets[1][noBurst].tokens)
		require.Equal(t, int64(250), granter.mu.buckets[2][canBurst].tokens)
		require.Equal(t, int64(200), granter.mu.buckets[2][noBurst].tokens)
		granter.mu.Unlock()
	})

	t.Run("tryGet_deducts_all_tiers", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3)

		// Seed buckets.
		toAdd := makeTokenCounts(3)
		for i := range toAdd {
			toAdd[i] = [numBurstQualifications]int64{1000, 1000}
		}
		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps)

		// Tier 0 gets 100 tokens.
		ok := granter.tryGet(0, canBurst, 100)
		require.True(t, ok)

		// All tiers should have been deducted.
		granter.mu.Lock()
		require.Equal(t, int64(900), granter.mu.buckets[0][canBurst].tokens)
		require.Equal(t, int64(900), granter.mu.buckets[1][canBurst].tokens)
		require.Equal(t, int64(900), granter.mu.buckets[2][canBurst].tokens)
		granter.mu.Unlock()
	})

	t.Run("tryGet_denied_when_tier_exhausted", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3)

		// Seed: tier 2 starts with 0 tokens.
		toAdd := makeTokenCounts(3)
		toAdd[0] = [numBurstQualifications]int64{1000, 1000}
		toAdd[1] = [numBurstQualifications]int64{500, 500}
		toAdd[2] = [numBurstQualifications]int64{0, 0}
		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps)

		// Tier 0: should succeed.
		ok := granter.tryGet(0, canBurst, 50)
		require.True(t, ok)

		// Tier 2: should fail (tokens <= 0).
		ok = granter.tryGet(2, canBurst, 50)
		require.False(t, ok)
	})
}
