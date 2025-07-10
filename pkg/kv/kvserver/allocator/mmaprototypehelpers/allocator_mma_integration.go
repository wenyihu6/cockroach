// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototypehelpers

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// UsageInfoToMMALoad converts a RangeUsageInfo to a mmaprototype.RangeLoad.
func UsageInfoToMMALoad(usage allocator.RangeUsageInfo) mmaprototype.RangeLoad {
	lv := mmaprototype.LoadVector{}
	lv[mmaprototype.CPURate] = mmaprototype.LoadValue(usage.RequestCPUNanosPerSecond) + mmaprototype.LoadValue(usage.RaftCPUNanosPerSecond)
	lv[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(usage.WriteBytesPerSecond)
	lv[mmaprototype.ByteSize] = mmaprototype.LoadValue(usage.LogicalBytes)
	return mmaprototype.RangeLoad{
		Load:    lv,
		RaftCPU: mmaprototype.LoadValue(usage.RaftCPUNanosPerSecond),
	}
}

// ReplicaDescriptorToReplicaIDAndType converts a ReplicaDescriptor to a
// StoreIDAndReplicaState. The leaseholder store is passed in as lh.
func ReplicaDescriptorToReplicaIDAndType(
	desc roachpb.ReplicaDescriptor, lh roachpb.StoreID,
) mmaprototype.StoreIDAndReplicaState {
	return mmaprototype.StoreIDAndReplicaState{
		StoreID: desc.StoreID,
		ReplicaState: mmaprototype.ReplicaState{
			ReplicaIDAndType: mmaprototype.ReplicaIDAndType{
				ReplicaID: desc.ReplicaID,
				ReplicaType: mmaprototype.ReplicaType{
					ReplicaType:   desc.Type,
					IsLeaseholder: desc.StoreID == lh,
				},
			},
		},
	}
}
