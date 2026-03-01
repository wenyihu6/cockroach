// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MakeStoreLoadMsg constructs a StoreLoadMsg with load and capacity expressed
// in physical units. MMA receives only this message for aggregate store state;
// amplification factors for per-range loads are obtained separately via
// ComputeAmplificationFactors.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector

	if desc.NodeCapacity.NodeCPURateCapacity > 0 && desc.NodeCapacity.NumStores > 0 {
		cpuResult := computePhysicalCPU(storeCPURateCapacityInput{
			storesCPURate:       float64(desc.NodeCapacity.StoresCPURate),
			nodeCPURateUsage:    float64(desc.NodeCapacity.NodeCPURateUsage),
			nodeCPURateCapacity: float64(desc.NodeCapacity.NodeCPURateCapacity),
			numStores:           desc.NodeCapacity.NumStores,
		})
		load[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuResult.load)
		capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuResult.capacity)
	} else {
		// NodeCapacity not yet populated (e.g. early in node startup before the
		// first capacity sample). Fall back to assuming 50% CPU utilization.
		load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
		capacity[mmaprototype.CPURate] = load[mmaprototype.CPURate] * 2
	}

	load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mmaprototype.WriteBandwidth] = mmaprototype.UnknownCapacity

	diskResult := computePhysicalDisk(
		desc.Capacity.LogicalBytes,
		desc.Capacity.Used,
		desc.Capacity.Available,
	)
	load[mmaprototype.ByteSize] = mmaprototype.LoadValue(diskResult.load)
	capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(diskResult.capacity)

	var secondaryLoad mmaprototype.SecondaryLoadVector
	secondaryLoad[mmaprototype.LeaseCount] = mmaprototype.LoadValue(desc.Capacity.LeaseCount)
	secondaryLoad[mmaprototype.ReplicaCount] = mmaprototype.LoadValue(desc.Capacity.RangeCount)

	return mmaprototype.StoreLoadMsg{
		NodeID:        desc.Node.NodeID,
		StoreID:       desc.StoreID,
		Load:          load,
		Capacity:      capacity,
		SecondaryLoad: secondaryLoad,
		LoadTime:      timeutil.FromUnixNanos(origTimestampNanos),
	}
}
