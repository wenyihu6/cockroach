// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"math"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// SQLCPUStatsProvider provides cumulative SQL CPU usage statistics, split into
// gateway and distributed (DistSQL) components. This interface matches the one
// defined in the admission package to avoid a direct import cycle.
type SQLCPUStatsProvider interface {
	// GetCumulativeSQLCPUNanos returns cumulative CPU time in nanoseconds for
	// gateway SQL work and distributed SQL work respectively.
	GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64)
}

// StoresStatsAggregator provides aggregated cpu usage stats across all stores.
type StoresStatsAggregator interface {
	// GetAggregatedStoreStats returns the total cpu usage across all stores and
	// the count of stores. If useCached is true, it uses the cached store
	// descriptor instead of computing new ones. Implemented by Stores.
	GetAggregatedStoreStats(useCached bool) (aggregatedCPUUsage int64, totalStoreCount int32, err error)
}

// NodeCapacityProvider reports node-level cpu usage and capacity by sampling
// runtime stats and aggregating store-level cpu capacity across all stores. It
// is used by Store to populate the NodeCapacity field in the StoreDescriptor.
type NodeCapacityProvider struct {
	stores             StoresStatsAggregator
	runtimeLoadMonitor *runtimeLoadMonitor
	sqlCPUStats        SQLCPUStatsProvider
}

// NodeCapacityProviderConfig holds the configuration for creating a
// NodeCapacityProvider.
type NodeCapacityProviderConfig struct {
	// CPUUsageRefreshInterval controls how often cpu usage measurements are
	// sampled.
	CPUUsageRefreshInterval time.Duration
	// CPUCapacityRefreshInterval controls how often the total CPU capacity is
	// polled.
	CPUCapacityRefreshInterval time.Duration
	// CPUUsageMovingAverageAge defines the effective time window size for the
	// moving average when sampling cpu usage.
	CPUUsageMovingAverageAge float64
}

// NewNodeCapacityProvider creates a new NodeCapacityProvider that monitors CPU
// metrics using the provided stores aggregator and configuration. The
// sqlCPUStats parameter may be nil if SQL CPU tracking is not available (e.g.,
// in tests or SQL-only tenants).
func NewNodeCapacityProvider(
	stopper *stop.Stopper,
	stores StoresStatsAggregator,
	config NodeCapacityProviderConfig,
	sqlCPUStats SQLCPUStatsProvider,
) *NodeCapacityProvider {
	if stopper == nil || stores == nil {
		panic("programming error: stopper or stores aggregator cannot be nil")
	}

	monitor := &runtimeLoadMonitor{
		stopper:                 stopper,
		usageRefreshInterval:    config.CPUUsageRefreshInterval,
		capacityRefreshInterval: config.CPUCapacityRefreshInterval,
		sqlCPUStats:             sqlCPUStats,
	}
	monitor.mu.usageEWMA = ewma.NewMovingAverage(config.CPUUsageMovingAverageAge)
	monitor.mu.sqlGatewayEWMA = ewma.NewMovingAverage(config.CPUUsageMovingAverageAge)
	monitor.mu.sqlDistEWMA = ewma.NewMovingAverage(config.CPUUsageMovingAverageAge)
	monitor.recordCPUCapacity(context.Background())
	return &NodeCapacityProvider{
		stores:             stores,
		runtimeLoadMonitor: monitor,
		sqlCPUStats:        sqlCPUStats,
	}
}

// Run starts the background monitoring of cpu metrics.
func (n *NodeCapacityProvider) Run(ctx context.Context) {
	// Record CPU usage and capacity prior to starting the async job to verify
	// that we're able to read CPU utilization metrics at all.
	err := n.runtimeLoadMonitor.recordCPUUsage(ctx)
	if err != nil {
		log.KvDistribution.Fatalf(ctx, "failed to record cpu usage: %v", err)
		return
	}

	_ = n.runtimeLoadMonitor.stopper.RunAsyncTask(ctx, "runtime-load-monitor", func(ctx context.Context) {
		n.runtimeLoadMonitor.run(ctx)
	})
}

// GetNodeCapacity returns the NodeCapacity with node-level CPU usage,
// capacity, and a pre-computed per-store CPU capacity. The store CPU capacity
// is computed here at the sender node using a model that accounts for SQL
// gateway and DistSQL CPU overhead, with K1 and K2 dynamically fitted from
// the current measurements at each gossip interval. This pre-computed value
// is sent in gossip so receivers can use it directly without recomputing.
//
// If useCached is true, it will use cached store descriptors to aggregate the
// sum of store-level CPU capacity.
func (n *NodeCapacityProvider) GetNodeCapacity(useCached bool) (roachpb.NodeCapacity, error) {
	storesCPURate, numStores, err := n.stores.GetAggregatedStoreStats(useCached)
	if err != nil {
		return roachpb.NodeCapacity{}, err
	}
	// TODO(wenyihu6): may be unexpected to caller that useCached only applies to
	// the stores stats but not runtime load monitor. We can change
	// runtimeLoadMonitor to also fetch updated stats.
	// TODO(wenyihu6): NodeCPURateCapacity <= NodeCPURateUsage fails on CI and
	// requires more investigation.
	cpuUsageNanoPerSec, cpuCapacityNanoPerSec, sqlGatewayCPURate, sqlDistCPURate :=
		n.runtimeLoadMonitor.GetCPUStats()

	// Compute the per-store CPU capacity at the sender. K1 and K2 are
	// dynamically fitted from the current measurements at each gossip interval.
	// The raw SQL CPU inputs (sqlGatewayCPURate, sqlDistCPURate) are only used
	// locally for this computation and logging — they are NOT sent in gossip.
	SSCR := float64(storesCPURate)
	NCR := float64(cpuUsageNanoPerSec)
	NCRC := float64(cpuCapacityNanoPerSec)
	SQL_G := float64(sqlGatewayCPURate)
	SQL_DIST := float64(sqlDistCPURate)
	N := float64(numStores)
	computedCapacity := computeStoreCPUCapacity(SSCR, NCR, NCRC, SQL_G, SQL_DIST, N)

	// Log the raw inputs and computed capacity at the sender for debuggability.
	// Receivers don't see these raw values — they only get the pre-computed
	// capacity in the gossip message.
	if SQL_G > 0 || SQL_DIST > 0 {
		var K1, K2 float64
		totalMeasured := SSCR + SQL_DIST + SQL_G
		if totalMeasured > 0 && SSCR > 0 {
			K1 = NCR / totalMeasured
			K2 = SQL_DIST / SSCR
		}
		log.KvDistribution.VInfof(context.Background(), 2,
			"store CPU capacity model: SSCR=%.0f NCR=%.0f NCRC=%.0f SQL_G=%.0f SQL_DIST=%.0f "+
				"N=%.0f K1=%.4f K2=%.4f => capacity=%d",
			SSCR, NCR, NCRC, SQL_G, SQL_DIST, N, K1, K2, computedCapacity)
	}

	return roachpb.NodeCapacity{
		StoresCPURate:            storesCPURate,
		NumStores:                numStores,
		NodeCPURateCapacity:      cpuCapacityNanoPerSec,
		NodeCPURateUsage:         cpuUsageNanoPerSec,
		ComputedStoreCPUCapacity: computedCapacity,
	}, nil
}

// computeStoreCPUCapacity computes the per-store CPU capacity using a model
// that accounts for SQL CPU (both gateway and DistSQL) when available.
//
// # Model
//
// We fit two equations to the observed CPU:
//
//	(SSCR + SQL_DIST + SQL_G) * K1 = NCR
//	SSCR * K2 = SQL_DIST
//
// Where:
//   - SSCR = StoresCPURate (aggregate per-replica CPU tracked at KV layer)
//   - SQL_G = gateway SQL work (NOT proportional to replicas)
//   - SQL_DIST = DistSQL work (proportional to replica load, simplifying assumption)
//   - NCR = NodeCPURateUsage (actual process CPU)
//   - NCRC = NodeCPURateCapacity (CPU capacity)
//   - K1 = scale factor from measured CPU to actual process CPU
//   - K2 = ratio of DistSQL CPU to KV replica CPU
//
// K1 and K2 are dynamically adjusted at each gossip interval from the current
// measurements — they are NOT pre-determined constants.
//
// Solving:
//
//	K2 = SQL_DIST / SSCR
//	K1 = NCR / (SSCR + SQL_DIST + SQL_G)
//
// The effective node capacity (in SSCR units) is:
//
//	NodeCapacity = (NCRC - SQL_G*K1) / ((K2+1)*K1)
//
// This subtracts gateway CPU from the capacity budget (since gateway CPU is
// NOT proportional to replica load and cannot be shed by moving replicas),
// then converts the remaining capacity to SSCR units. The per-store capacity
// is NodeCapacity / NumStores.
//
// When SQL_G=0 and SQL_DIST=0, this reduces to the original scheme:
//
//	NodeCapacity = NCRC / (NCR/SSCR) = SSCR * NCRC/NCR = SSCR/cpuUtil
func computeStoreCPUCapacity(
	SSCR, NCR, NCRC, SQL_G, SQL_DIST, N float64,
) int64 {
	cpuUtil := NCR / NCRC
	almostZeroUtil := cpuUtil < 0.01

	if SSCR == 0 || almostZeroUtil || N == 0 {
		// Fallback: StoresCPURate is zero, utilization is near-zero, or no
		// stores. We assume that only 50% of the usage can be accounted for
		// in StoresCPURate, so we divide 50% of the capacity among all stores.
		if N == 0 {
			N = 1
		}
		return int64(NCRC / 2 / N)
	}

	// Check if we have SQL CPU measurements. If both are zero, fall back to the
	// original scheme for backwards compatibility and because the formula
	// produces the same result.
	hasSQLCPU := SQL_G > 0 || SQL_DIST > 0

	if !hasSQLCPU {
		// Original scheme: NodeCapacity = SSCR / cpuUtil.
		// This implicitly fits the model SSCR * K1 = NCR (all CPU is
		// proportional to store load).
		nodeCapacity := SSCR / cpuUtil
		storeCapacity := nodeCapacity / N
		return int64(storeCapacity)
	}

	// Alternative scheme with SQL CPU accounting.
	totalMeasured := SSCR + SQL_DIST + SQL_G

	// K1 = NCR / totalMeasured (dynamically adjusted each interval)
	// K2 = SQL_DIST / SSCR     (dynamically adjusted each interval)
	K1 := NCR / totalMeasured
	K2 := SQL_DIST / SSCR

	// NodeCapacity = (NCRC - SQL_G * K1) / ((K2 + 1) * K1)
	numerator := NCRC - SQL_G*K1
	denominator := (K2 + 1) * K1

	if denominator <= 0 || math.IsInf(numerator/denominator, 0) || math.IsNaN(numerator/denominator) {
		// Safety: if the formula produces non-finite results, fall back.
		nodeCapacity := SSCR / cpuUtil
		return int64(nodeCapacity / N)
	}

	nodeCapacity := numerator / denominator
	// Clamp to avoid negative capacity (can happen if gateway CPU is very
	// large relative to total capacity).
	if nodeCapacity < 0 {
		nodeCapacity = SSCR / cpuUtil
	}

	storeCapacity := nodeCapacity / N
	return int64(storeCapacity)
}

// runtimeLoadMonitor polls cpu usage and capacity stats of the node
// periodically and maintaining a moving average.
type runtimeLoadMonitor struct {
	usageRefreshInterval    time.Duration
	capacityRefreshInterval time.Duration
	stopper                 *stop.Stopper
	// sqlCPUStats provides cumulative SQL CPU measurements. May be nil.
	sqlCPUStats SQLCPUStatsProvider

	mu struct {
		syncutil.Mutex
		// lastTotalUsageNanos tracks cumulative cpu usage in nanoseconds using
		// status.GetProcCPUTime.
		lastTotalUsageNanos float64
		// usageEWMA maintains a moving average of delta cpu usage between two
		// subsequent polls in nanoseconds. The cpu usage is obtained by polling
		// stats from status.GetProcCPUTime which is cumulative.
		usageEWMA ewma.MovingAverage
		// logicalCPUsPerSec represents the node's cpu capacity in logical
		// CPU-seconds per second, obtained from status.GetCPUCapacity.
		logicalCPUsPerSec int64

		// SQL CPU tracking: cumulative snapshots and EWMA rates.
		lastGatewayCPUNanos float64
		lastDistCPUNanos    float64
		sqlGatewayEWMA      ewma.MovingAverage
		sqlDistEWMA         ewma.MovingAverage
	}
}

// GetCPUStats returns the current cpu usage, capacity, and SQL CPU rate stats
// for the node.
func (m *runtimeLoadMonitor) GetCPUStats() (
	cpuUsageNanoPerSec, cpuCapacityNanoPerSec, sqlGatewayCPURate, sqlDistCPURate int64,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// usageEWMA is usage in nanoseconds. Divide by refresh interval to get the
	// per-second nano-sec rate.
	cpuUsageNanoPerSec = int64(m.mu.usageEWMA.Value() / m.usageRefreshInterval.Seconds())
	// logicalCPUsPerSec is in logical cpu-seconds per second. Convert the unit
	// from cpu-seconds to cpu-nanoseconds.
	cpuCapacityNanoPerSec = m.mu.logicalCPUsPerSec * time.Second.Nanoseconds()
	// SQL CPU rates are also computed as nanos-per-interval / interval_seconds.
	if m.mu.sqlGatewayEWMA != nil {
		sqlGatewayCPURate = int64(m.mu.sqlGatewayEWMA.Value() / m.usageRefreshInterval.Seconds())
	}
	if m.mu.sqlDistEWMA != nil {
		sqlDistCPURate = int64(m.mu.sqlDistEWMA.Value() / m.usageRefreshInterval.Seconds())
	}
	return
}

// recordCPUUsage samples and records the current cpu usage of the node,
// including SQL CPU (gateway and DistSQL) if available.
func (m *runtimeLoadMonitor) recordCPUUsage(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	userTimeMillis, sysTimeMillis, err := status.GetProcCPUTime(ctx)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "failed to get cpu usage")
	}
	// Convert milliseconds to nanoseconds.
	totalUsageNanos := float64(userTimeMillis*1e6 + sysTimeMillis*1e6)
	if totalUsageNanos < m.mu.lastTotalUsageNanos {
		log.KvDistribution.Warningf(ctx, "last cpu usage is larger than current: %v > %v",
			m.mu.lastTotalUsageNanos, totalUsageNanos)
		totalUsageNanos = m.mu.lastTotalUsageNanos
	}
	m.mu.usageEWMA.Add(totalUsageNanos - m.mu.lastTotalUsageNanos)
	m.mu.lastTotalUsageNanos = totalUsageNanos

	// Sample SQL CPU cumulative counters and compute deltas for EWMA.
	if m.sqlCPUStats != nil {
		gatewayCPUNanos, distCPUNanos := m.sqlCPUStats.GetCumulativeSQLCPUNanos()
		gatewayDelta := float64(gatewayCPUNanos) - m.mu.lastGatewayCPUNanos
		distDelta := float64(distCPUNanos) - m.mu.lastDistCPUNanos
		if gatewayDelta < 0 {
			gatewayDelta = 0
		}
		if distDelta < 0 {
			distDelta = 0
		}
		m.mu.sqlGatewayEWMA.Add(gatewayDelta)
		m.mu.sqlDistEWMA.Add(distDelta)
		m.mu.lastGatewayCPUNanos = float64(gatewayCPUNanos)
		m.mu.lastDistCPUNanos = float64(distCPUNanos)
	}
	return nil
}

// recordCPUCapacity samples and records the current cpu capacity of the node.
func (m *runtimeLoadMonitor) recordCPUCapacity(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.logicalCPUsPerSec = int64(status.GetCPUCapacity())
	if m.mu.logicalCPUsPerSec == 0 {
		if buildutil.CrdbTestBuild {
			panic("programming error: cpu capacity is 0")
		}
		// TODO(wenyihu6): we should pass in an actual context here.
		log.KvDistribution.Warningf(ctx, "failed to get cpu capacity")
	}
}

// run is the main loop of the RuntimeLoadMonitor and periodically polls the cpu
// usage and capacity. It continues to run until the context is done or the
// stopper is quiesced.
func (m *runtimeLoadMonitor) run(ctx context.Context) {
	usageTimer := time.NewTicker(m.usageRefreshInterval)
	defer usageTimer.Stop()
	capacityTimer := time.NewTicker(m.capacityRefreshInterval)
	defer capacityTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopper.ShouldQuiesce():
			return
		case <-usageTimer.C:
			usageTimer.Reset(m.usageRefreshInterval)
			err := m.recordCPUUsage(ctx)
			if err != nil {
				log.KvDistribution.Warningf(ctx, "failed to record cpu usage: %v", err)
			}
		case <-capacityTimer.C:
			capacityTimer.Reset(m.capacityRefreshInterval)
			m.recordCPUCapacity(ctx)
		}
	}
}
