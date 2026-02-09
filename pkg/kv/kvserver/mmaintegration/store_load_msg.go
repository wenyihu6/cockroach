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

// MakeStoreLoadMsg makes a store load message.
//
// The CPU capacity is pre-computed at the sender node (in
// NodeCapacityProvider.GetNodeCapacity) and sent in the gossip message.
// Receivers use the pre-computed value directly rather than recomputing
// from raw inputs. K1 and K2 are dynamically fitted at each gossip interval.
//
// TODO(wenyihu6): Add more tests for this function.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector
	load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
	if desc.NodeCapacity.ComputedNodeCPURateCapacity > 0 {
		// Use the pre-computed per-store CPU capacity from the sender.
		capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.NodeCapacity.ComputedNodeCPURateCapacity)
	} else if desc.NodeCapacity.NodeCPURateCapacity > 0 {
		// Fallback for older gossip messages that don't have pre-computed
		// capacity: use the original scheme (SSCR / cpuUtil / N).
		SSCR := float64(desc.NodeCapacity.StoresCPURate)
		NCR := float64(desc.NodeCapacity.NodeCPURateUsage)
		NCRC := float64(desc.NodeCapacity.NodeCPURateCapacity)
		N := float64(desc.NodeCapacity.NumStores)
		cpuUtil := NCR / NCRC
		if SSCR == 0 || cpuUtil < 0.01 || N == 0 {
			if N == 0 {
				N = 1
			}
			capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(NCRC / 2 / N)
		} else {
			capacity[mmaprototype.CPURate] = mmaprototype.LoadValue(SSCR / cpuUtil / N)
		}
	} else {
		// TODO(sumeer): remove this hack of defaulting to 50% utilization, since
		// NodeCPURateCapacity should never be 0.
		// TODO(tbg): when do we expect to hit this branch? Mixed version cluster?
		capacity[mmaprototype.CPURate] = load[mmaprototype.CPURate] * 2
	}
	load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(desc.Capacity.WriteBytesPerSecond)
	capacity[mmaprototype.WriteBandwidth] = mmaprototype.UnknownCapacity
	// ByteSize is based on LogicalBytes since that is how we measure the size
	// of each range
	load[mmaprototype.ByteSize] = mmaprototype.LoadValue(desc.Capacity.LogicalBytes)
	// Available does not compensate for the ballast, so utilization will look
	// higher than actual. This is fine since the ballast is small (default is
	// 1% of capacity) and is for use in an emergency.
	byteSizeUtil :=
		float64(desc.Capacity.Capacity-desc.Capacity.Available) / float64(desc.Capacity.Capacity)
	almostZeroUtil := byteSizeUtil < 0.01
	if load[mmaprototype.ByteSize] != 0 && !almostZeroUtil {
		// Normal case. The store has some ranges, and is not almost empty.
		capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(float64(load[mmaprototype.ByteSize]) / byteSizeUtil)
	} else {
		// Has no ranges or is almost empty. This is likely a new store. Since
		// LogicalBytes are uncompressed, we start with the compressed available,
		// which is desirably pessimistic.
		capacity[mmaprototype.ByteSize] = mmaprototype.LoadValue(desc.Capacity.Available)
	}
	var secondaryLoad mmaprototype.SecondaryLoadVector
	secondaryLoad[mmaprototype.LeaseCount] = mmaprototype.LoadValue(desc.Capacity.LeaseCount)
	secondaryLoad[mmaprototype.ReplicaCount] = mmaprototype.LoadValue(desc.Capacity.RangeCount)
	// TODO(tbg): this triggers early in tests, probably we're making load messages
	// before having received the first capacity. Still, this is bad, should fix.
	// or handle properly by communicating an unknown capacity.
	// if capacity[mmaprototype.CPURate] == 0 {
	// 	panic("ouch")
	// }
	return mmaprototype.StoreLoadMsg{
		NodeID:        desc.Node.NodeID,
		StoreID:       desc.StoreID,
		Load:          load,
		Capacity:      capacity,
		SecondaryLoad: secondaryLoad,
		LoadTime:      timeutil.FromUnixNanos(origTimestampNanos),
	}
}
