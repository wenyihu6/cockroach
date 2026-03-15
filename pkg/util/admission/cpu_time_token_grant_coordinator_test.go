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

	// Test that if setting is disabled, WorkQueue uses slots, else it
	// uses CPU time tokens.
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, false)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true).mode)
	// With slots, both calls return the same slot-based WorkQueue.
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	// With CPU time tokens, there is a single WorkQueue for all tenants.
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Test that the env var kill switch overrides the cluster setting.
	// Even with the setting enabled, the kill switch forces slot-based AC.
	defer func(prev bool) {
		cpuTimeTokenACKillSwitch = prev
	}(cpuTimeTokenACKillSwitch)
	cpuTimeTokenACKillSwitch = true
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))

	// Disabling the kill switch restores CPU time token AC (setting is
	// still enabled).
	cpuTimeTokenACKillSwitch = false
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true).mode)
	require.Equal(t, cpuCoords.GetKVWorkQueue(false), cpuCoords.GetKVWorkQueue(true))
}
