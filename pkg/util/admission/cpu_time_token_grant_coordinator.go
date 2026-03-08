// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ResourceGroupsConfig is a cluster setting that configures resource groups
// for CPU isolation via a JSON array. Example:
//
//	SET CLUSTER SETTING admission.resource_groups.config =
//	  '[{"name":"default","weight_cpu":100,"max_cpu":true},
//	    {"name":"batch","weight_cpu":25,"max_cpu":false}]';
//
// An empty string (the default) disables resource groups, falling back to
// the 2-tier system/app tenant behavior.
var ResourceGroupsConfig = settings.RegisterStringSetting(
	settings.SystemOnly,
	"admission.resource_groups.config",
	"JSON array configuring resource groups for CPU isolation; "+
		"empty string disables resource groups",
	"",
	settings.WithPublic,
)

var cpuTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.enabled",
	"if true, CPU time token AC will be used for foreground KVWork, instead of slots-based AC -- "+
		"note that this is not supported in production except on multi-tenant Serverless clusters",
	false)

// cpuTimeTokenACKillSwitch is an env var kill switch that disables CPU time
// token AC regardless of the cluster setting. This is useful when SQL is
// unavailable, preventing the cluster setting from being changed.
var cpuTimeTokenACKillSwitch = envutil.EnvOrDefaultBool(
	"COCKROACH_DISABLE_CPU_TIME_TOKEN_AC", false)

// cpuTimeTokenACIsEnabled returns true if CPU time token AC is enabled. It
// checks both the cluster setting and the env var kill switch. The kill switch
// takes precedence over the cluster setting.
func cpuTimeTokenACIsEnabled(sv *settings.Values) bool {
	return !cpuTimeTokenACKillSwitch && cpuTimeTokenACEnabled.Get(sv)
}

// CPUGrantCoordinators's main purpose is to act as a shim. Depending on
// whether admission.cpu_time_tokens.enabled is true or false, a WorkQueue
// that does slot-based or CPU time token AC is returned from
// GetKVWorkQueue. This way, we support both, without requiring a process
// restart.
//
// With resource groups, CPUGrantCoordinators routes work to per-group
// WorkQueues based on ResourceGroupID instead of the binary
// isSystemTenant flag.
type CPUGrantCoordinators struct {
	st           *cluster.Settings
	slotsCoord   *GrantCoordinator
	cpuTimeCoord *cpuTimeTokenGrantCoordinator
}

// GetKVWorkQueue returns a WorkQueue to use for KVWork. If
// admission.cpu_time_tokens.enabled is true, it returns a WorkQueue that
// implements CPU time token AC. Else it returns a WorkQueue that does
// slots-based AC. If CPU time token AC, there is one WorkQueue per
// resource tier/group.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	if isSystemTenant {
		return coord.cpuTimeCoord.getWorkQueue(systemTenant)
	}
	return coord.cpuTimeCoord.getWorkQueue(appTenant)
}

// GetKVWorkQueueForGroup returns the WorkQueue for a specific resource group.
// If CPU time token AC is not enabled, falls back to the slots-based queue.
func (coord *CPUGrantCoordinators) GetKVWorkQueueForGroup(groupID ResourceGroupID) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	return coord.cpuTimeCoord.getWorkQueue(resourceTier(groupID))
}

// GetSQLWorkQueue returns a WorkQueue for SQLKVResponseWork or
// SQLSQLResponseWork. If any other queue is requested from this function,
// it panics.
func (coord *CPUGrantCoordinators) GetSQLWorkQueue(workKind WorkKind) *WorkQueue {
	if workKind != SQLKVResponseWork && workKind != SQLSQLResponseWork {
		panic(fmt.Sprintf("workKind %q not supported by GetSQLWorkQueue", workKind))
	}
	return coord.slotsCoord.queues[workKind].(*WorkQueue)
}

// SetTenantWeights sets the weight of tenants, using the provided tenant ID
// => weight map. A nil map will result in all tenants having the same weight.
// SetTenantWeights adjusts the weights on all WorkQueues that
// CPUGrantCoordinators manages.
func (coord *CPUGrantCoordinators) SetTenantWeights(weights map[uint64]uint32) {
	coord.slotsCoord.GetWorkQueue(KVWork).SetTenantWeights(weights)
	coord.cpuTimeCoord.setTenantWeights(weights)
}

// GetRunnableCountCallback returns a callback of type
// goschedstats.RunnableCountCallback.
func (coord *CPUGrantCoordinators) GetRunnableCountCallback() goschedstats.RunnableCountCallback {
	return coord.slotsCoord.CPULoad
}

// Close implements the stop.Closer interface.
func (cg *CPUGrantCoordinators) Close() {
	cg.slotsCoord.Close()
	cg.cpuTimeCoord.close()
}

type cpuTimeTokenGrantCoordinator struct {
	filler   *cpuTimeTokenFiller
	numTiers int
	queues   []requesterClose
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	numTiers := numDefaultResourceTiers
	var rgRegistry *ResourceGroupRegistry
	if opts.ResourceGroupRegistry != nil {
		numTiers = opts.ResourceGroupRegistry.NumGroups()
		rgRegistry = opts.ResourceGroupRegistry
	}

	granter := newCPUTimeTokenGranter(numTiers)
	childGranters := make([]cpuTimeTokenChildGranter, numTiers)
	for tier := 0; tier < numTiers; tier++ {
		childGranters[tier] = cpuTimeTokenChildGranter{
			tier:   resourceTier(tier),
			parent: granter,
		}
	}
	timeSource := timeutil.DefaultTimeSource{}
	filler := &cpuTimeTokenFiller{
		timeSource: timeSource,
		closeCh:    make(chan struct{}),
	}
	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		numTiers: numTiers,
		queues:   make([]workQueueIForAllocator, numTiers),
		settings: settings,
		registry: rgRegistry,
	}
	model := &cpuTimeTokenLinearModel{
		granter:            granter,
		cpuMetricsProvider: opts.CPUMetricsProvider,
		timeSource:         timeSource,
	}
	allocator.model = model
	filler.allocator = allocator

	requesters := make([]requester, numTiers)
	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	for tier := 0; tier < numTiers; tier++ {
		wqOpts := makeWorkQueueOptions(KVWork)
		wqOpts.mode = usesCPUTimeTokens
		requesters[tier] = makeWorkQueue(
			ambientCtx, KVWork, &childGranters[tier], settings, wqMetrics, wqOpts)
		granter.requester[tier] = requesters[tier]
		allocator.queues[tier] = requesters[tier].(*WorkQueue)
	}

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler:   filler,
		numTiers: numTiers,
		queues:   make([]requesterClose, numTiers),
	}
	for tier := 0; tier < numTiers; tier++ {
		coordinator.queues[tier] = requesters[tier]
	}

	// The filler ticking appears to have a slight negative impact on perf.
	// For now, we accept this, since CPU time token AC will be off by
	// default, and only enabled in Serverless. To track fixing the perf
	// issue, we have the following ticket:
	// https://github.com/cockroachdb/cockroach/issues/161945
	if !knobs.DisableCPUTimeTokenFillerGoroutine {
		var once sync.Once
		if cpuTimeTokenACIsEnabled(&settings.SV) {
			once.Do(func() {
				filler.start(ambientCtx.AnnotateCtx(context.Background()))
			})
		}
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV, func(ctx context.Context) {
			if cpuTimeTokenACIsEnabled(&settings.SV) {
				once.Do(func() {
					filler.start(ambientCtx.AnnotateCtx(context.Background()))
				})
			}
		})
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue(tier resourceTier) *WorkQueue {
	if int(tier) >= len(coord.queues) {
		// Fall back to the last tier if the requested tier doesn't exist.
		tier = resourceTier(len(coord.queues) - 1)
	}
	return coord.queues[tier].(*WorkQueue)
}

func (coord *cpuTimeTokenGrantCoordinator) setTenantWeights(weights map[uint64]uint32) {
	for tier := range coord.queues {
		coord.queues[tier].(*WorkQueue).SetTenantWeights(weights)
	}
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	for tier := range coord.queues {
		coord.queues[tier].close()
	}
	coord.filler.close()
}
