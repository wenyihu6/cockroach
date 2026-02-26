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

// MakeStoreLoadMsg makes a store load message. Load and Capacity are expressed
// in physical units (CPU ns/s, physical disk bytes). The AmplificationFactor
// converts logical per-range deltas to physical units inside MMA.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector
	var ampFactor [mmaprototype.NumLoadDimensions]float64

	// CPU: use the physical model when node-level metrics are available.
	if desc.NodeCapacity.NodeCPURateCapacity > 0 && desc.NodeCapacity.NumStores > 0 {
		cpuResult := computePhysicalCPU(storeCPURateCapacityInput{
			storesCPURate:           float64(desc.NodeCapacity.StoresCPURate),
			nodeCPURateUsage:        float64(desc.NodeCapacity.NodeCPURateUsage),
			nodeCPURateCapacity:     float64(desc.NodeCapacity.NodeCPURateCapacity),
			sqlGatewayCPUNanoPerSec: float64(desc.NodeCapacity.SQLGatewayCPUNanoPerSec),
			sqlDistCPUNanoPerSec:    float64(desc.NodeCapacity.SQLDistCPUNanoPerSec),
			numStores:               desc.NodeCapacity.NumStores,
		})
		load[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuResult.load)
		capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuResult.capacity)
		ampFactor[mmaprototype.CPURate] = cpuResult.amplificationFactor
	} else {
		// NodeCapacity not yet populated (e.g. early in node startup before the
		// first capacity sample). Fall back to assuming 50% CPU utilization.
		load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
		capacity[mmaprototype.CPURate] = load[mmaprototype.CPURate] * 2
		ampFactor[mmaprototype.CPURate] = 1.0
	}

	// Write bandwidth: no capacity model, amplification factor is 1.
	load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mmaprototype.WriteBandwidth] = mmaprototype.UnknownCapacity
	ampFactor[mmaprototype.WriteBandwidth] = 1.0

	// Disk: use the physical model.
	diskResult := computePhysicalDisk(
		desc.Capacity.LogicalBytes,
		desc.Capacity.Used,
		desc.Capacity.Available,
	)
	load[mmaprototype.ByteSize] = mmaprototype.LoadValue(diskResult.load)
	capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(diskResult.capacity)
	ampFactor[mmaprototype.ByteSize] = diskResult.amplificationFactor

	var secondaryLoad mmaprototype.SecondaryLoadVector
	secondaryLoad[mmaprototype.LeaseCount] = mmaprototype.LoadValue(desc.Capacity.LeaseCount)
	secondaryLoad[mmaprototype.ReplicaCount] = mmaprototype.LoadValue(desc.Capacity.RangeCount)

	return mmaprototype.StoreLoadMsg{
		NodeID:              desc.Node.NodeID,
		StoreID:             desc.StoreID,
		Load:                load,
		Capacity:            capacity,
		AmplificationFactor: ampFactor,
		SecondaryLoad:       secondaryLoad,
		LoadTime:            timeutil.FromUnixNanos(origTimestampNanos),
	}
}
