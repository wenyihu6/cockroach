// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type StoresStatsAggregator interface {
	GetAggregatedStoreStats(useCached bool) (aggregatedCPUUsage int64, totalStoreCount int32)
}

type NodeCapacityProvider struct {
	stores             StoresStatsAggregator
	runtimeLoadMonitor *RuntimeLoadMonitor
}

// defaultCPUUsageRefreshInterval is the interval at which the CPU usage is
// refreshed.
const defaultCPUUsageRefreshInterval = 1 * time.Second

// defaultCPUCapacityRefreshInterval is the interval at which the CPU capacity
// is refreshed.
const defaultCPUCapacityRefreshInterval = 10 * time.Second

func NewNodeCapacityProvider(
	stopper *stop.Stopper, stores StoresStatsAggregator, knobs *NodeCapacityProviderTestingKnobs,
) *NodeCapacityProvider {
	cpuUsageRefreshInterval := defaultCPUUsageRefreshInterval
	cpuCapacityRefreshInterval := defaultCPUCapacityRefreshInterval
	if knobs != nil {
		cpuUsageRefreshInterval = knobs.CpuUsageRefreshInterval
		cpuCapacityRefreshInterval = knobs.CpuCapacityRefreshInterval
	}
	return &NodeCapacityProvider{
		stores:             stores,
		runtimeLoadMonitor: newRuntimeLoadMonitor(stopper, cpuUsageRefreshInterval, cpuCapacityRefreshInterval),
	}
}

func (n *NodeCapacityProvider) Run(ctx context.Context) {
	n.runtimeLoadMonitor.Run(ctx)
}

func (n *NodeCapacityProvider) GetNodeCapacity(useCached bool) roachpb.NodeCapacity {
	storesCPURate, numStores := n.stores.GetAggregatedStoreStats(useCached)
	stats := n.runtimeLoadMonitor.GetCPUStats()
	return roachpb.NodeCapacity{
		StoresCPURate:       storesCPURate,
		NumStores:           numStores,
		NodeCPURateCapacity: stats.CPUCapacityNanoPerSec,
		NodeCPURateUsage:    stats.CPUUsageNanoPerSec,
	}
}
