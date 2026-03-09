// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestResourceGroupEndToEnd validates the full resource group flow:
// registry → granter → allocator → coordinator → per-group WorkQueues.
func TestResourceGroupEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()

	// Configure 3 resource groups via JSON (simulating cluster setting).
	rgRegistry, err := ParseResourceGroupsJSON(
		`[{"name":"default","weight_cpu":100,"max_cpu":true},
		  {"name":"analytics","weight_cpu":50,"max_cpu":false},
		  {"name":"batch","weight_cpu":25,"max_cpu":false}]`)
	require.NoError(t, err)
	require.Equal(t, 3, rgRegistry.NumGroups())

	opts := Options{ResourceGroupRegistry: rgRegistry}
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	defer func(prev bool) {
		cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, prev)
	}(cpuTimeTokenACEnabled.Get(&settings.SV))

	// Enable CPU time token AC.
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, true)

	// Verify 3 separate WorkQueues exist.
	q0 := cpuCoords.GetKVWorkQueueForGroup(0)
	q1 := cpuCoords.GetKVWorkQueueForGroup(1)
	q2 := cpuCoords.GetKVWorkQueueForGroup(2)
	require.NotEqual(t, q0, q1)
	require.NotEqual(t, q1, q2)

	// Verify status shows all groups.
	status := cpuCoords.GetResourceGroupStatus()
	require.Equal(t, 3, len(status))
	require.Equal(t, "default", status[0].Name)
	require.Equal(t, int32(100), status[0].WeightCPU)
	require.True(t, status[0].MaxCPU)
	require.Equal(t, "analytics", status[1].Name)
	require.Equal(t, int32(50), status[1].WeightCPU)
	require.False(t, status[1].MaxCPU)
	require.Equal(t, "batch", status[2].Name)
	require.Equal(t, int32(25), status[2].WeightCPU)
	require.False(t, status[2].MaxCPU)

	// Verify weight updates propagate.
	rgRegistry.UpdateGroup(1, ResourceGroupConfig{Name: "analytics", WeightCPU: 75, MaxCPU: true})
	status = cpuCoords.GetResourceGroupStatus()
	require.Equal(t, int32(75), status[1].WeightCPU)
	require.True(t, status[1].MaxCPU)

	// Verify target utilizations are proportional to weights.
	// Total weight: 100 + 75 + 25 = 200
	// Group 0: 100/200 = 50%
	// Group 1: 75/200 = 37.5%
	// Group 2: 25/200 = 12.5%
	targets := rgRegistry.ComputeTargetUtilizations(0.8, 0.05)
	require.Equal(t, 3, len(targets))
	// Group 0 (MaxCPU=true): noBurst = 0.8 * 0.5 = 0.4
	require.InDelta(t, 0.4, targets[0].noBurst, 0.001)
	// Group 1 (MaxCPU=true, weight=75/200): noBurst = 0.8 * 0.375 = 0.3
	require.InDelta(t, 0.3, targets[1].noBurst, 0.001)
	// Group 2 (MaxCPU=false, weight=25/200=0.125):
	// noBurst = min(0.8*0.125, 0.75*0.125) = min(0.1, 0.09375) = 0.09375
	require.InDelta(t, 0.09375, targets[2].noBurst, 0.001)
}

// TestResourceGroupFallbackToSlots verifies that when CPU time token AC
// is disabled, all groups fall back to the same slots-based WorkQueue.
func TestResourceGroupFallbackToSlots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()

	rgRegistry := NewResourceGroupRegistry()
	rgRegistry.AddGroup(ResourceGroupConfig{Name: "batch", WeightCPU: 25, MaxCPU: false})

	opts := Options{ResourceGroupRegistry: rgRegistry}
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	// With CPU time token AC disabled, all groups use the same slots queue.
	q0 := cpuCoords.GetKVWorkQueueForGroup(0)
	q1 := cpuCoords.GetKVWorkQueueForGroup(1)
	require.Equal(t, q0, q1)
	require.Equal(t, usesSlots, q0.mode)
}

// TestResourceGroupDynamicWeightUpdate verifies that changing the cluster
// setting updates group weights without a restart.
func TestResourceGroupDynamicWeightUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()

	// Start with 2 groups.
	rgRegistry, err := ParseResourceGroupsJSON(
		`[{"name":"default","weight_cpu":100,"max_cpu":true},
		  {"name":"batch","weight_cpu":25,"max_cpu":false}]`)
	require.NoError(t, err)

	opts := Options{ResourceGroupRegistry: rgRegistry}
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	defer func(prev bool) {
		cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, prev)
	}(cpuTimeTokenACEnabled.Get(&settings.SV))
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, true)

	// Verify initial weights.
	status := cpuCoords.GetResourceGroupStatus()
	require.Equal(t, int32(25), status[1].WeightCPU)
	require.False(t, status[1].MaxCPU)

	// Update weights via cluster setting.
	ResourceGroupsConfig.Override(context.Background(), &settings.SV,
		`[{"name":"default","weight_cpu":100,"max_cpu":true},
		  {"name":"batch","weight_cpu":75,"max_cpu":true}]`)

	// Verify updated weights.
	status = cpuCoords.GetResourceGroupStatus()
	require.Equal(t, int32(75), status[1].WeightCPU)
	require.True(t, status[1].MaxCPU)
}
