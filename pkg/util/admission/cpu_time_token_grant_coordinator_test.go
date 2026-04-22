// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestObsoleteCode contains nudges for cleanups that may be possible in the
// future. When this test fails (which is necessarily a result of bumping the
// MinSupportedVersion), please carry out the cleanups that are now possible or
// file issues asking for them to be done.
func TestObsoleteCode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	msv := clusterversion.RemoveDevOffset(clusterversion.MinSupported.Version())
	t.Logf("MinSupported: %v", msv)

	// When MinSupported is bumped above V26_3, the legacy
	// cpuTimeTokenACEnabled bool (admission.cpu_time_tokens.enabled) can
	// be removed along with the fallback logic in cpuTimeTokenACIsEnabled.
	// All clusters will have the mode setting by then.
	v26dot3 := clusterversion.RemoveDevOffset(clusterversion.V26_3.Version())
	if !msv.LessEq(v26dot3) {
		_ = cpuTimeTokenACEnabled
		t.Fatalf("cpuTimeTokenACEnabled (admission.cpu_time_tokens.enabled) and " +
			"its fallback in cpuTimeTokenACIsEnabled can be removed")
	}
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
	ctx := context.Background()

	defer func(prev bool) {
		cpuTimeTokenACEnabled.Override(ctx, &settings.SV, prev)
	}(cpuTimeTokenACEnabled.Get(&settings.SV))

	// Test that if setting is disabled, WorkQueues uses slots, else they
	// use CPU time tokens.
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, false)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// If CPU time token AC is disabled, we use the slots-based WorkQueue
	// for both system & app tenant work.
	require.Equal(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Default mode is Serverless - 2 separate queues.
	cpuTimeTokenACEnabled.Override(ctx, &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// In Serverless mode, system and app tenant work use different queues.
	require.NotEqual(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Switch to RM mode dynamically - single queue for all work.
	// In production, mode changes take effect when the filler goroutine
	// calls resetInterval and publishes the new mode. Since the filler
	// goroutine is disabled in this test, we update the atomic directly.
	cpuCoords.cpuTimeCoord.filler.activeMode.Store(
		int64(resourceManagerMode))
	require.Equal(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Switch back to Serverless - 2 separate queues again.
	cpuCoords.cpuTimeCoord.filler.activeMode.Store(
		int64(serverlessMode))
	require.NotEqual(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Test that the env var kill switch overrides the cluster setting.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.Equal(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	// Disabling the kill switch restores CPU time token AC (setting is
	// still enabled, mode is still Serverless from above).
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	require.NotEqual(t,
		cpuCoords.GetKVWorkQueue(false /* isSystemTenant */),
		cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))
}
