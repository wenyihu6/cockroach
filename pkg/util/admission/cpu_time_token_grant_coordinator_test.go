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

func TestCPUTimeTokenACWithResourceGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()

	// Configure 3 resource groups.
	rgRegistry := NewResourceGroupRegistry()
	rgRegistry.AddGroup(ResourceGroupConfig{Name: "analytics", WeightCPU: 50, MaxCPU: false})
	rgRegistry.AddGroup(ResourceGroupConfig{Name: "batch", WeightCPU: 25, MaxCPU: false})

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

	// Verify each resource group gets its own WorkQueue.
	q0 := cpuCoords.GetKVWorkQueueForGroup(0)
	q1 := cpuCoords.GetKVWorkQueueForGroup(1)
	q2 := cpuCoords.GetKVWorkQueueForGroup(2)
	require.NotNil(t, q0)
	require.NotNil(t, q1)
	require.NotNil(t, q2)
	require.NotEqual(t, q0, q1)
	require.NotEqual(t, q1, q2)
	require.NotEqual(t, q0, q2)

	// All should use CPU time tokens mode.
	require.Equal(t, usesCPUTimeTokens, q0.mode)
	require.Equal(t, usesCPUTimeTokens, q1.mode)
	require.Equal(t, usesCPUTimeTokens, q2.mode)

	// When disabled, all groups fall back to the same slots queue.
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, false)
	q0Slots := cpuCoords.GetKVWorkQueueForGroup(0)
	q1Slots := cpuCoords.GetKVWorkQueueForGroup(1)
	q2Slots := cpuCoords.GetKVWorkQueueForGroup(2)
	require.Equal(t, usesSlots, q0Slots.mode)
	require.Equal(t, q0Slots, q1Slots)
	require.Equal(t, q1Slots, q2Slots)
}

func TestCPUTimeTokenACEnableAndDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	var opts Options
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	defer func(prev bool) {
		cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, prev)
	}(cpuTimeTokenACEnabled.Get(&settings.SV))

	// Test that if setting is disabled, WorkQueues uses slots, else they
	// use CPU time tokens.
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, false)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// If CPU time token AC is disabled, we use one WorkQueue for both
	// system & app tenant work.
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// If CPU time token AC is enabled, we use one WorkQueue for system
	// tenant work & a second WorkQueue for app tenant work.
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Test that the env var kill switch overrides the cluster setting.
	// Even with the setting enabled, the kill switch forces slot-based AC.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Disabling the kill switch restores CPU time token AC (setting is
	// still enabled).
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))
}
