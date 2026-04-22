// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/goschedstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// cpuTimeTokenACEnabled is the legacy bool setting for enabling CPU time
// token AC. Deprecated in favor of cpuTimeTokenACMode. Kept registered
// so that clusters upgrading from older versions (where this was set to
// true) continue to function. The cpuTimeTokenACIsEnabled helper checks
// cpuTimeTokenACMode first and falls back to this setting. This setting
// will be retired in 26.4 once all clusters have migrated to the new
// mode setting.
var cpuTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.enabled",
	"if true, CPU time token AC will be used for foreground KVWork, instead of slots-based AC -- "+
		"deprecated in favor of admission.cpu_time_tokens.mode",
	false)

// cpuTimeTokenMode selects between off (slot-based AC), Serverless
// (per-tier settings), and Resource Manager (resource groups) modes.
// Both Serverless and Resource Manager modes use a single WorkQueue;
// the mode controls which cluster settings are read and how burst
// bucket refill is computed.
type cpuTimeTokenMode int64

const (
	// offMode disables CPU time token AC; slot-based AC is used instead.
	// When the mode is off, the legacy bool setting is checked as a
	// fallback.
	offMode cpuTimeTokenMode = iota
	// serverlessMode uses the app tenant utilization settings and a
	// single WorkQueue. System tenant work gets priority through the
	// burst qualification mechanism (maxCPU resource groups always
	// qualify for canBurst).
	serverlessMode
	// resourceManagerMode uses the resource manager utilization
	// settings and a single WorkQueue with N resource groups.
	resourceManagerMode
)

// cpuTimeTokenACMode selects the CPU time token admission control
// mode. Can be changed at runtime without a restart. When set to off,
// the legacy admission.cpu_time_tokens.enabled bool is checked as a
// fallback for backward compatibility.
//
// This is ApplicationLevel to match the legacy cpuTimeTokenACEnabled
// bool that it replaces. The class should be revisited when the
// legacy bool is retired.
var cpuTimeTokenACMode = settings.RegisterEnumSetting[cpuTimeTokenMode](
	settings.ApplicationLevel,
	"admission.cpu_time_tokens.mode",
	"selects the CPU time token admission control mode: off uses "+
		"slot-based AC (or falls back to the legacy enabled bool), "+
		"serverless uses app tenant utilization settings, "+
		"resource_manager uses resource group settings",
	"off",
	map[cpuTimeTokenMode]string{
		offMode:             "off",
		serverlessMode:      "serverless",
		resourceManagerMode: "resource_manager",
	},
	settings.WithValidateEnum(func(val string) error {
		if val == "resource_manager" {
			return errors.New("resource_manager mode is not yet implemented")
		}
		return nil
	}),
)

// cpuTimeTokenACKillSwitch is an env var kill switch that disables CPU time
// token AC regardless of the cluster setting. This is useful when SQL is
// unavailable, preventing the cluster setting from being changed.
var cpuTimeTokenACKillSwitch = envutil.EnvOrDefaultBool(
	"COCKROACH_DISABLE_CPU_TIME_TOKEN_AC", false)

// cpuTimeTokenACIsEnabled returns true if CPU time token AC is enabled.
// It checks cpuTimeTokenACMode first; if that is off, it falls back
// to the legacy cpuTimeTokenACEnabled bool for backward compatibility.
// The env var kill switch takes precedence over both settings.
func cpuTimeTokenACIsEnabled(sv *settings.Values) bool {
	if cpuTimeTokenACKillSwitch {
		return false
	}
	if cpuTimeTokenACMode.Get(sv) != offMode {
		return true
	}
	return cpuTimeTokenACEnabled.Get(sv)
}

var sqlCPUTimeTokenACEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"admission.sql_cpu_time_tokens.enabled",
	"when true, SQL CPU usage is admitted through the same CPU time token "+
		"budget as KV work; has no effect unless CPU time token AC is enabled "+
		"via admission.cpu_time_tokens.mode or the legacy enabled setting",
	false,
)

// sqlCPUTimeTokenACIsEnabled returns true if SQL CPU usage is admitted
// through the same CPU time token AC as KV work. It has no effect unless
// CPU time token AC is enabled.
func sqlCPUTimeTokenACIsEnabled(sv *settings.Values) bool {
	return cpuTimeTokenACIsEnabled(sv) && sqlCPUTimeTokenACEnabled.Get(sv)
}

// CPUGrantCoordinators acts as a shim. Depending on
// admission.cpu_time_tokens.mode (off, serverless, resource_manager),
// a WorkQueue that does slot-based or CPU time token AC is returned
// from GetKVWorkQueue. This way, we support both, without requiring a
// process restart.
type CPUGrantCoordinators struct {
	st           *cluster.Settings
	slotsCoord   *GrantCoordinator
	cpuTimeCoord *cpuTimeTokenGrantCoordinator
}

// GetKVWorkQueue returns a WorkQueue to use for KVWork. If CPU time
// token AC is enabled (via admission.cpu_time_tokens.mode or the legacy
// enabled bool), it returns the single CPU time token WorkQueue.
// Else it returns a WorkQueue that does slots-based AC.
//
// The isSystemTenant parameter is preserved for backward compatibility
// but is ignored when CPU time token AC is enabled - all work goes
// through the same queue. Differentiation between system and app
// tenant work happens via resource groups within the single queue.
func (coord *CPUGrantCoordinators) GetKVWorkQueue(isSystemTenant bool) *WorkQueue {
	if !cpuTimeTokenACIsEnabled(&coord.st.SV) {
		return coord.slotsCoord.GetWorkQueue(KVWork)
	}
	return coord.cpuTimeCoord.getWorkQueue()
}

// GetCTTWorkQueue returns the CPU time token WorkQueue unconditionally,
// without checking whether CPU time token AC is enabled. The caller is
// responsible for gating on the setting. This avoids a race in GetKVWorkQueue
// where the setting can flip between the caller's check and the internal
// re-check, returning a slot-based queue to a caller that expects a CTT queue.
func (coord *CPUGrantCoordinators) GetCTTWorkQueue(isSystemTenant bool) *WorkQueue {
	return coord.cpuTimeCoord.getWorkQueue()
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
	coord.cpuTimeCoord.setGroupWeights(weights)
}

// ResourceGroupConfig holds per-resource-group configuration.
type ResourceGroupConfig struct {
	Weight uint32
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the default resource group
// configuration used in Resource Manager mode when no external
// config has been set via SetResourceGroupConfig.
var defaultRMResourceGroupConfig = map[uint64]ResourceGroupConfig{
	highResourceGroupID: {Weight: 1, MaxCPU: true},
	lowResourceGroupID:  {Weight: 1, MaxCPU: false},
}

// SetResourceGroupConfig sets per-resource-group weights and maxCPU
// flags. Only meaningful in Resource Manager mode. Weights are applied
// immediately; maxCPU and burst fractions are picked up by the filler
// goroutine in the next resetInterval (within 1s).
func (coord *CPUGrantCoordinators) SetResourceGroupConfig(config map[uint64]ResourceGroupConfig) {
	weights := make(map[uint64]uint32, len(config))
	for id, cfg := range config {
		weights[id] = cfg.Weight
	}
	coord.SetTenantWeights(weights)
	configCopy := make(map[uint64]ResourceGroupConfig, len(config))
	for id, cfg := range config {
		configCopy[id] = cfg
	}
	coord.cpuTimeCoord.resourceGroupConfig.Store(&configCopy)
	coord.cpuTimeCoord.configDirty.Store(true)
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
	filler *cpuTimeTokenFiller
	queue  requesterClose
	// resourceGroupConfig points to the allocator's atomic config
	// pointer. Written by SetResourceGroupConfig (external callers),
	// read by the filler goroutine in resetInterval to recompute
	// burst fractions from weights.
	resourceGroupConfig *atomic.Pointer[map[uint64]ResourceGroupConfig]
	// configDirty points to the allocator's dirty flag. Set by
	// SetResourceGroupConfig, checked by the filler goroutine.
	configDirty *atomic.Bool
}

func makeCPUTimeTokenGrantCoordinator(
	ambientCtx log.AmbientContext,
	opts Options,
	settings *cluster.Settings,
	registry *metric.Registry,
	knobs *TestingKnobs,
) *cpuTimeTokenGrantCoordinator {
	// Default to serverless when mode is off (legacy bool path or CTT
	// not yet enabled). The strategy only matters when the filler runs,
	// and the filler only starts when CTT is enabled. Using serverless
	// as the default preserves the legacy behavior.
	initialMode := cpuTimeTokenACMode.Get(&settings.SV)
	if initialMode == offMode {
		initialMode = serverlessMode
	}
	metrics := makeCPUTimeTokenMetrics()
	registry.AddMetricStruct(metrics)
	timeSource := timeutil.DefaultTimeSource{}
	granter := newCPUTimeTokenGranter(metrics, timeSource)
	filler := &cpuTimeTokenFiller{
		timeSource: timeSource,
		closeCh:    make(chan struct{}),
	}
	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: settings,
		metrics:  metrics,
	}
	model := &cpuTimeTokenLinearModel{
		granter:            granter,
		cpuMetricsProvider: opts.CPUMetricsProvider,
		timeSource:         timeSource,
		metrics:            metrics,
	}

	wqOpts := makeWorkQueueOptions(KVWork)
	wqOpts.mode = usesCPUTimeTokens
	wqOpts.perGroupAggMetrics = &groupAggMetrics{
		admittedCount:  metrics.AdmittedCountPerTenant,
		waitTimeNanos:  metrics.WaitTimeNanosPerTenant,
		tokensUsed:     metrics.TokensUsedPerTenant,
		tokensReturned: metrics.TokensReturnedPerTenant,
	}
	wqMetrics := makeWorkQueueMetrics("cpu", registry)
	queue := makeWorkQueue(
		ambientCtx, KVWork, granter, settings, wqMetrics, wqOpts)
	granter.requester = queue
	allocator.queue = queue.(*WorkQueue)

	allocator.strategy = allocator.newStrategy(initialMode)
	allocator.model = model
	filler.allocator = allocator

	coordinator := &cpuTimeTokenGrantCoordinator{
		filler:              filler,
		queue:               queue,
		resourceGroupConfig: &allocator.resourceGroupConfig,
		configDirty:         &allocator.configDirty,
	}

	// Initialize the filler's activeMode so GetKVWorkQueue returns the
	// correct queue before the filler goroutine starts.
	filler.activeMode.Store(int64(initialMode))

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
		startIfEnabled := func(ctx context.Context) {
			if cpuTimeTokenACIsEnabled(&settings.SV) {
				once.Do(func() {
					filler.start(ambientCtx.AnnotateCtx(context.Background()))
				})
			}
		}
		cpuTimeTokenACMode.SetOnChange(&settings.SV, startIfEnabled)
		cpuTimeTokenACEnabled.SetOnChange(&settings.SV, startIfEnabled)
	}

	return coordinator
}

func (coord *cpuTimeTokenGrantCoordinator) getWorkQueue() *WorkQueue {
	return coord.queue.(*WorkQueue)
}

func (coord *cpuTimeTokenGrantCoordinator) setGroupWeights(weights map[uint64]uint32) {
	coord.queue.(*WorkQueue).SetTenantWeights(weights)
}

func (coord *cpuTimeTokenGrantCoordinator) close() {
	coord.queue.close()
	coord.filler.close()
}
