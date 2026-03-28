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

var cpuTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.enabled",
	"if true, CPU time token AC will be used for foreground KVWork, instead "+
		"of slots-based AC -- note that this is not supported in production "+
		"except on multi-tenant Serverless clusters",
	false)

// cpuTimeTokenACKillSwitch is an env var kill switch that disables CPU time
// token AC regardless of the cluster setting.
var cpuTimeTokenACKillSwitch = envutil.EnvOrDefaultBool(
	"COCKROACH_DISABLE_CPU_TIME_TOKEN_AC", false)

func cpuTimeTokenACIsEnabled(sv *settings.Values) bool {
	return !cpuTimeTokenACKillSwitch && cpuTimeTokenACEnabled.Get(sv)
}

// cpuTimeTokenMode selects between Serverless (2 WorkQueues, per-tier
// settings) and Resource Manager (1 WorkQueue, resource groups) modes.
// The mode is read from the admission.cpu_time_tokens.mode cluster
// setting at startup and cannot be changed without a restart.
type cpuTimeTokenMode int64

const (
	// serverlessMode uses 2 WorkQueues (systemTenant, appTenant), per-tier
	// utilization targets, and 4 buckets (2 tiers x 2 burst quals).
	serverlessMode cpuTimeTokenMode = iota
	// resourceManagerMode uses 1 WorkQueue with N resource groups,
	// a single utilization target, and 2 buckets (1 tier x 2 burst quals).
	// In RM mode, only queue[0] receives work; queue[1] sits idle.
	resourceManagerMode
)

// KVCPUTimeTokenACMode selects between Serverless and Resource Manager
// modes for CPU time token admission control. Read once at startup;
// changing the setting requires a restart to take effect.
var KVCPUTimeTokenACMode = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"admission.cpu_time_tokens.mode",
	"selects between serverless (2 queues, per-tier targets) and "+
		"resource_manager (1 queue, single target) CPU time token modes",
	"serverless",
	map[int64]string{
		int64(serverlessMode):      "serverless",
		int64(resourceManagerMode): "resource_manager",
	},
)

// CPUGrantCoordinators's main purpose is to act as a shim. Depending on
// whether admission.cpu_time_tokens.enabled is true or false, a WorkQueue
// that does slot-based or CPU time token AC is returned from
// GetKVWorkQueue. This way, we support both, without requiring a process
// restart.
type CPUGrantCoordinators struct {
	st           *cluster.Settings
	slotsCoord   *GrantCoordinator
	cpuTimeCoord *cpuTimeTokenGrantCoordinator
}

// GetKVWorkQueue returns a WorkQueue to use for KVWork. If
// admission.cpu_time_tokens.enabled is true, it returns a WorkQueue that
// implements CPU time token AC. In Serverless mode, there is one
// WorkQueue for system tenant work and another for app tenant work. In
// Resource Manager mode, there is a single WorkQueue for all work.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	if coord.cpuTimeCoord.mode == serverlessMode {
		if isSystemTenant {
			return coord.cpuTimeCoord.getWorkQueue(systemTenant)
		}
		return coord.cpuTimeCoord.getWorkQueue(appTenant)
	}
	// Resource Manager mode: single queue for all work.
	return coord.cpuTimeCoord.getWorkQueue(0)
}

// GetSQLWorkQueue returns a WorkQueue for SQLKVResponseWork or
// SQLSQLResponseWork. If any other queue is requested from this function,
// it panics.
func (coord *CPUGrantCoordinators) GetSQLWorkQueue(workKind WorkKind) *WorkQueue {
	if workKind != SQLKVResponseWork && workKind != SQLSQLResponseWork {
		panic(fmt.Sprintf(
			"workKind %q not supported by GetSQLWorkQueue", workKind))
	}
	return coord.slotsCoord.queues[workKind].(*WorkQueue)
}

// SetTenantWeights sets the weight of tenants, using the provided tenant
// ID => weight map. A nil map will result in all tenants having the same
// weight. SetTenantWeights adjusts the weights on all WorkQueues that
// CPUGrantCoordinators manages.
func (coord *CPUGrantCoordinators) SetTenantWeights(weights map[uint64]uint32) {
	coord.slotsCoord.GetWorkQueue(KVWork).SetTenantWeights(weights)
	coord.cpuTimeCoord.setTenantWeights(weights)
}

// ResourceGroupConfig holds per-resource-group configuration.
type ResourceGroupConfig struct {
	Weight         uint32
	BurstLimitFrac float64
}

// SetResourceGroupConfig sets per-resource-group weights and burst limits.
// Only meaningful in Resource Manager mode.
func (coord *CPUGrantCoordinators) SetResourceGroupConfig(config map[uint64]ResourceGroupConfig) {
	weights := make(map[uint64]uint32, len(config))
	burstLimits := make(map[uint64]float64, len(config))
	for id, cfg := range config {
		weights[id] = cfg.Weight
		burstLimits[id] = cfg.BurstLimitFrac
	}
	coord.SetTenantWeights(weights)
	// In RM mode, there's only one queue (tier 0).
	coord.cpuTimeCoord.queues[0].(*WorkQueue).SetBurstLimits(burstLimits)
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
	mode   cpuTimeTokenMode
	filler *cpuTimeTokenFiller
	queues [numResourceTiers]requesterClose
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	// Always create 2 tiers. In RM mode, tier-1 sits idle (no work
	// routed, zero refill rates).
	initialMode := cpuTimeTokenMode(KVCPUTimeTokenACMode.Get(&settings.SV))

	metrics := makeCPUTimeTokenMetrics()
	registry.AddMetricStruct(metrics)
	timeSource := timeutil.DefaultTimeSource{}
	granter := newCPUTimeTokenGranter(metrics, timeSource)

	filler := &cpuTimeTokenFiller{
		timeSource: timeSource,
		closeCh:    make(chan struct{}),
	}

	initialActiveTiers := int(numResourceTiers)
	if initialMode == resourceManagerMode {
		initialActiveTiers = 1
	}
	allocator := &cpuTimeTokenAllocator{
		granter:        granter,
		numActiveTiers: initialActiveTiers,
		mode:           initialMode,
		settings:       settings,
		metrics:        metrics,
	}
	model := &cpuTimeTokenLinearModel{
		granter:            granter,
		cpuMetricsProvider: opts.CPUMetricsProvider,
		timeSource:         timeSource,
		metrics:            metrics,
	}
	allocator.model = model
	filler.allocator = allocator

	var childGranters [numResourceTiers]cpuTimeTokenChildGranter
	for tier := 0; tier < int(numResourceTiers); tier++ {
		childGranters[tier] = cpuTimeTokenChildGranter{
			tier:   resourceTier(tier),
			parent: granter,
		}
	}

	// In Serverless mode, defaultBurstLimitFrac=0.0 (preserves the
	// 90%-fullness check for burst qualification). In RM mode,
	// defaultBurstLimitFrac=1.0 (unconfigured groups always canBurst).
	var defaultBurstFrac float64
	if initialMode == resourceManagerMode {
		defaultBurstFrac = 1.0
	}
	var requesters [numResourceTiers]requester
	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	for tier := 0; tier < int(numResourceTiers); tier++ {
		wqOpts := makeWorkQueueOptions(KVWork)
		wqOpts.mode = usesCPUTimeTokens
		wqOpts.defaultBurstLimitFrac = defaultBurstFrac
		wqOpts.perTenantAggMetrics = &tenantAggMetrics{
			admittedCount:  metrics.AdmittedCountPerTenant[tier],
			waitTimeNanos:  metrics.WaitTimeNanosPerTenant[tier],
			tokensUsed:     metrics.TokensUsedPerTenant[tier],
			tokensReturned: metrics.TokensReturnedPerTenant[tier],
		}
		requesters[tier] = makeWorkQueue(
			ambientCtx, KVWork, &childGranters[tier],
			settings, wqMetrics, wqOpts)
		granter.requester[tier] = requesters[tier]
		allocator.queues[tier] = requesters[tier].(*WorkQueue)
	}

	coordinator := &cpuTimeTokenGrantCoordinator{
		mode:   initialMode,
		filler: filler,
	}
	for tier := 0; tier < int(numResourceTiers); tier++ {
		coordinator.queues[tier] = requesters[tier]
	}

	if !knobs.DisableCPUTimeTokenFillerGoroutine {
		var once sync.Once
		if cpuTimeTokenACIsEnabled(&settings.SV) {
			once.Do(func() {
				filler.start(
					ambientCtx.AnnotateCtx(context.Background()))
			})
		}
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV,
			func(ctx context.Context) {
				if cpuTimeTokenACIsEnabled(&settings.SV) {
					once.Do(func() {
						filler.start(
							ambientCtx.AnnotateCtx(context.Background()))
					})
				}
			})
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue(tier resourceTier) *WorkQueue {
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
