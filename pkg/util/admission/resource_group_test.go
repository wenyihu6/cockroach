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
		// canBurst = 0.8 + 0.05 = 0.85 (full node target, since MaxCPU=true)
		require.InDelta(t, 0.85, targets[0].canBurst, 0.001)

		// Group 1 (analytics, MaxCPU=false): noBurst = 0.8 * 0.5 = 0.4
		require.InDelta(t, 0.4, targets[1].noBurst, 0.001)
		// canBurst = same as noBurst (MaxCPU=false, no bursting)
		require.InDelta(t, 0.4, targets[1].canBurst, 0.001)
	})

	t.Run("compute_target_utilizations_requirements_example", func(t *testing.T) {
		// From requirements doc: online=160, batch=20, support=20
		r := &ResourceGroupRegistry{}
		r.mu.groups = []ResourceGroupConfig{
			{Name: "online_rg", WeightCPU: 160, MaxCPU: true},
			{Name: "batch_rg", WeightCPU: 20, MaxCPU: false},
			{Name: "support_rg", WeightCPU: 20, MaxCPU: false},
		}
		r.mu.totalWeight = 200

		targets := r.ComputeTargetUtilizations(0.8, 0.05)
		require.Equal(t, 3, len(targets))

		// online_rg: weight=160/200=80%, noBurst = 0.8*0.8 = 0.64
		require.InDelta(t, 0.64, targets[0].noBurst, 0.001)
		// canBurst = 0.85 (full node, MaxCPU=true)
		require.InDelta(t, 0.85, targets[0].canBurst, 0.001)

		// batch_rg: weight=20/200=10%, noBurst = 0.8*0.1 = 0.08
		require.InDelta(t, 0.08, targets[1].noBurst, 0.001)
		// canBurst = 0.08 (same as noBurst, MaxCPU=false)
		require.InDelta(t, 0.08, targets[1].canBurst, 0.001)

		// support_rg: same as batch_rg
		require.InDelta(t, 0.08, targets[2].noBurst, 0.001)
		require.InDelta(t, 0.08, targets[2].canBurst, 0.001)
	})
}

func TestParseResourceGroupsJSON(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		r, err := ParseResourceGroupsJSON("")
		require.NoError(t, err)
		require.Nil(t, r)
	})

	t.Run("single_group", func(t *testing.T) {
		r, err := ParseResourceGroupsJSON(`[{"name":"default","weight_cpu":100,"max_cpu":true}]`)
		require.NoError(t, err)
		require.Equal(t, 1, r.NumGroups())
		g := r.GetGroup(0)
		require.Equal(t, "default", g.Name)
		require.Equal(t, int32(100), g.WeightCPU)
		require.True(t, g.MaxCPU)
	})

	t.Run("three_groups", func(t *testing.T) {
		json := `[
			{"name":"default","weight_cpu":100,"max_cpu":true},
			{"name":"analytics","weight_cpu":50,"max_cpu":false},
			{"name":"batch","weight_cpu":25,"max_cpu":false}
		]`
		r, err := ParseResourceGroupsJSON(json)
		require.NoError(t, err)
		require.Equal(t, 3, r.NumGroups())
		require.Equal(t, int32(175), r.TotalWeight())
	})

	t.Run("invalid_weight", func(t *testing.T) {
		_, err := ParseResourceGroupsJSON(`[{"name":"bad","weight_cpu":0,"max_cpu":true}]`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid weight_cpu")
	})

	t.Run("invalid_json", func(t *testing.T) {
		_, err := ParseResourceGroupsJSON(`not json`)
		require.Error(t, err)
	})
}

func TestCPUTimeTokenGranterDynamic(t *testing.T) {
	t.Run("3_tiers", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3, true)
		require.Equal(t, 3, granter.numTiers)
		require.Equal(t, 3, len(granter.requester))
		require.Equal(t, 3, len(granter.mu.buckets))
	})

	t.Run("refill_dynamic", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3, true)
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

	t.Run("tryGet_independent_budgets", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3, true)

		// Seed per-group buckets and global bucket.
		toAdd := makeTokenCounts(3)
		for i := range toAdd {
			toAdd[i] = [numBurstQualifications]int64{1000, 1000}
		}
		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps, refillGlobalParams{toAdd: 3000, capacity: 3000})

		// Tier 0 gets 100 tokens.
		ok := granter.tryGet(0, canBurst, 100)
		require.True(t, ok)

		// Only tier 0 should have been deducted; tiers 1 and 2 are independent.
		// Global bucket is deducted by all.
		granter.mu.Lock()
		require.Equal(t, int64(900), granter.mu.buckets[0][canBurst].tokens)
		require.Equal(t, int64(1000), granter.mu.buckets[1][canBurst].tokens)
		require.Equal(t, int64(1000), granter.mu.buckets[2][canBurst].tokens)
		require.Equal(t, int64(2900), granter.mu.globalBucket.tokens)
		granter.mu.Unlock()
	})

	t.Run("tryGet_denied_when_tier_exhausted", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3, true)

		// Seed: tier 2 starts with 0 tokens. Global has plenty.
		toAdd := makeTokenCounts(3)
		toAdd[0] = [numBurstQualifications]int64{1000, 1000}
		toAdd[1] = [numBurstQualifications]int64{500, 500}
		toAdd[2] = [numBurstQualifications]int64{0, 0}
		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps, refillGlobalParams{toAdd: 3000, capacity: 3000})

		// Tier 0: should succeed.
		ok := granter.tryGet(0, canBurst, 50)
		require.True(t, ok)

		// Tier 2: should fail (per-group tokens <= 0, even though global
		// has capacity — group is at its cap).
		ok = granter.tryGet(2, canBurst, 50)
		require.False(t, ok)
	})

	t.Run("work_conserving_via_global_bucket", func(t *testing.T) {
		granter := newCPUTimeTokenGranter(3, true)

		// Tier 0 (online_rg): large budget, Tier 1/2: small budget.
		// Global = sum of all.
		toAdd := makeTokenCounts(3)
		toAdd[0] = [numBurstQualifications]int64{800, 640}
		toAdd[1] = [numBurstQualifications]int64{80, 80}
		toAdd[2] = [numBurstQualifications]int64{80, 80}
		caps := ratesToCapacities(rates(toAdd))
		granter.refill(toAdd, caps, refillGlobalParams{toAdd: 800, capacity: 800})

		// Tier 0 (online) uses only 50 tokens (low load).
		ok := granter.tryGet(0, noBurst, 50)
		require.True(t, ok)

		// Tier 1 (batch) wants 80 tokens — exactly its per-group budget.
		ok = granter.tryGet(1, noBurst, 80)
		require.True(t, ok)

		// Tier 2 (support) wants 80 tokens — exactly its budget.
		ok = granter.tryGet(2, noBurst, 80)
		require.True(t, ok)

		// Global: 800 - 50 - 80 - 80 = 590 remaining. Plenty of headroom
		// because online didn't use its full share.
		granter.mu.Lock()
		require.Equal(t, int64(590), granter.mu.globalBucket.tokens)
		granter.mu.Unlock()

		// Now batch wants MORE than its per-group budget (which is
		// exhausted at 0). This is denied even though global has
		// capacity — the per-group cap enforces the minimum guarantee.
		ok = granter.tryGet(1, noBurst, 10)
		require.False(t, ok)
	})
}
