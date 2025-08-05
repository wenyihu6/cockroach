// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// changeState represents the state of a registered change.
type changeState int

const (
	// changeRegistered indicates the change has been registered but not enacted.
	changeRegistered changeState = iota
	// changeSucceeded indicates the change was successfully enacted.
	changeSucceeded
	// changeFailed indicates the change failed to enact.
	changeFailed
)

// mockMMAAllocator implements the mmaAllocator interface for testing.
type mockMMAAllocator struct {
	// tracks registered changes and their state
	changes map[mmaprototype.ChangeID]changeState
}

// mockStorePool implements the storePool interface for testing.
type mockStorePool struct {
	// Track which store has the lease.
	leaseholder roachpb.StoreID
	// Track store loads.
	storeCPULoads map[roachpb.StoreID]float64
	// Track rebalance operations.
	rebalances []roachpb.StoreID
}

// UpdateLocalStoresAfterLeaseTransfer updates the mock store pool after a lease
// transfer.
func (m *mockStorePool) UpdateLocalStoresAfterLeaseTransfer(
	removeFrom, addTo roachpb.StoreID, usage allocator.RangeUsageInfo,
) {
	m.leaseholder = addTo
	// Update CPU loads after lease transfer.
	m.storeCPULoads[addTo] += float64(usage.RequestCPUNanosPerSecond)
	m.storeCPULoads[removeFrom] -= float64(usage.RequestCPUNanosPerSecond)
}

// UpdateLocalStoreAfterRebalance updates the mock store pool after a rebalance.
func (m *mockStorePool) UpdateLocalStoreAfterRebalance(
	storeID roachpb.StoreID, usage allocator.RangeUsageInfo, changeType roachpb.ReplicaChangeType,
) {
	switch changeType {
	case roachpb.ADD_VOTER, roachpb.ADD_NON_VOTER:
		m.storeCPULoads[storeID] += float64(usage.RequestCPUNanosPerSecond + usage.RaftCPUNanosPerSecond)
	case roachpb.REMOVE_VOTER, roachpb.REMOVE_NON_VOTER:
		m.storeCPULoads[storeID] -= float64(usage.RequestCPUNanosPerSecond + usage.RaftCPUNanosPerSecond)
	}
	// For testing purposes, we'll just track that this method was called
	// The actual implementation would handle the rebalance logic
	m.rebalances = append(m.rebalances, storeID) // Simplified for testing
}

// AdjustPendingChangesDisposition informs mockMMAAllocator that the pending
// changes have been applied.
func (m *mockMMAAllocator) AdjustPendingChangesDisposition(
	changes []mmaprototype.ChangeID, success bool,
) {
	for _, id := range changes {
		if success {
			m.changes[id] = changeSucceeded
		} else {
			m.changes[id] = changeFailed
		}
	}
}

// RegisterExternalChanges informs mockMMAAllocator that the external changes
// have been registered.
func (m *mockMMAAllocator) RegisterExternalChanges(
	changes []mmaprototype.ReplicaChange,
) []mmaprototype.ChangeID {
	changeIDs := make([]mmaprototype.ChangeID, len(changes))
	for i := range changeIDs {
		id := mmaprototype.ChangeID(i + 1)
		changeIDs[i] = id
		m.changes[id] = changeRegistered // Register change as pending
	}
	return changeIDs
}

// createTestAllocatorSync creates a test allocator sync with mock dependencies.
func createTestAllocatorSync() (*AllocatorSync, *mockMMAAllocator, *mockStorePool) {
	st := cluster.MakeTestingClusterSettings()
	mma := &mockMMAAllocator{
		changes: make(map[mmaprototype.ChangeID]changeState),
	}
	sp := &mockStorePool{
		storeCPULoads: make(map[roachpb.StoreID]float64),
	}
	as := NewAllocatorSync(sp, mma, st)
	return as, mma, sp
}

// getTracked is a helper function to get a tracked change from the allocator sync
// without deleting the change.
func getTracked(as *AllocatorSync, id SyncChangeID) (trackedAllocatorChange, bool) {
	as.mu.Lock()
	defer as.mu.Unlock()
	change, ok := as.mu.trackedChanges[id]
	return change, ok
}

// TestSyncChangeIDIsValid tests that the SyncChangeID type correctly identifies
// valid and invalid IDs.
func TestSyncChangeIDIsValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	invalidID := SyncChangeID(0)
	validID := SyncChangeID(1)

	require.False(t, invalidID.IsValid())
	require.True(t, validID.IsValid())
	require.False(t, InvalidSyncChangeID.IsValid())
}

// TestAllocatorSyncAddAndGetTrackedChange tests that the allocator sync can
// add and get a tracked change.
func TestAllocatorSyncAddAndGetTrackedChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	as, _, sp := createTestAllocatorSync()

	// Create a test change.
	change := trackedAllocatorChange{
		usage: allocator.RangeUsageInfo{
			RequestCPUNanosPerSecond: 1000,
			RaftCPUNanosPerSecond:    500,
			WriteBytesPerSecond:      1024,
			LogicalBytes:             2048,
		},
		leaseTransferOp: &leaseTransferOp{
			transferFrom: 1,
			transferTo:   2,
		},
	}

	// Add the change.
	syncChangeID := as.addTrackedChange(change)
	require.True(t, syncChangeID.IsValid())
	require.Equal(t, SyncChangeID(1), syncChangeID)

	// Get the change back.
	retrievedChange, ok := getTracked(as, syncChangeID)
	require.True(t, ok)
	require.Equal(t, change.usage, retrievedChange.usage)
	require.Equal(t, change.leaseTransferOp, retrievedChange.leaseTransferOp)

	// Post apply should remove the change.
	as.PostApply(syncChangeID, true)
	emptyChange, ok := getTracked(as, syncChangeID)
	require.False(t, ok)
	require.Equal(t, trackedAllocatorChange{}, emptyChange)
	// Verify store pool was updated.
	require.Equal(t, float64(1000), sp.storeCPULoads[2])
	require.Equal(t, float64(-1000), sp.storeCPULoads[1])
}

// TestAllocatorSyncNonMMAPreTransferLease tests that allocator sync on a lease
// transfer operation.
func TestAllocatorSyncNonMMAPreTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	as, _, sp := createTestAllocatorSync()

	desc := &roachpb.RangeDescriptor{
		RangeID: 1,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{StoreID: 1, ReplicaID: 1, Type: roachpb.VOTER_FULL},
			{StoreID: 2, ReplicaID: 2, Type: roachpb.VOTER_FULL},
		},
	}

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	transferFrom := roachpb.ReplicationTarget{StoreID: 1, NodeID: 1}
	transferTo := roachpb.ReplicationTarget{StoreID: 2, NodeID: 2}

	syncChangeID := as.NonMMAPreTransferLease(desc, usage, transferFrom, transferTo)

	require.True(t, syncChangeID.IsValid())
	require.Equal(t, SyncChangeID(1), syncChangeID)

	// Verify the change was tracked properly.
	change, ok := getTracked(as, syncChangeID)
	require.True(t, ok)
	require.Equal(t, usage, change.usage)
	require.NotNil(t, change.leaseTransferOp)
	require.Equal(t, roachpb.StoreID(1), change.leaseTransferOp.transferFrom)
	require.Equal(t, roachpb.StoreID(2), change.leaseTransferOp.transferTo)

	// Post apply should remove the change.
	as.PostApply(syncChangeID, true)
	change, ok = getTracked(as, syncChangeID)
	require.False(t, ok)
	require.Equal(t, trackedAllocatorChange{}, change)

	// Verify store pool was updated.
	require.Equal(t, roachpb.StoreID(2), sp.leaseholder)
	require.Equal(t, float64(1000), sp.storeCPULoads[2])
	require.Equal(t, float64(-1000), sp.storeCPULoads[1])
}

// TestAllocatorSyncNonMMAPreChangeReplicas tests that allocator sync on a
// replica change operation.

func TestAllocatorSyncNonMMAPreChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	as, _, sp := createTestAllocatorSync()

	desc := &roachpb.RangeDescriptor{
		RangeID: 1,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{StoreID: 1, ReplicaID: 1, Type: roachpb.VOTER_FULL},
			{StoreID: 2, ReplicaID: 2, Type: roachpb.VOTER_FULL},
			{StoreID: 3, ReplicaID: 3, Type: roachpb.NON_VOTER},
		},
	}

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	// Create a test change which removes a voter and adds a non-voter to store
	// 2, and adds a voter to store 4.
	changes := kvpb.ReplicationChanges{
		kvpb.ReplicationChange{
			ChangeType: roachpb.REMOVE_VOTER,
			Target:     roachpb.ReplicationTarget{StoreID: 2, NodeID: 2},
		},
		kvpb.ReplicationChange{
			ChangeType: roachpb.ADD_NON_VOTER,
			Target:     roachpb.ReplicationTarget{StoreID: 2, NodeID: 2},
		},
		kvpb.ReplicationChange{
			ChangeType: roachpb.ADD_VOTER,
			Target:     roachpb.ReplicationTarget{StoreID: 4, NodeID: 4},
		},
	}

	syncChangeID := as.NonMMAPreChangeReplicas(desc, usage, changes, 1)

	require.True(t, syncChangeID.IsValid())
	require.Equal(t, SyncChangeID(1), syncChangeID)

	// Verify the change was tracked.
	change, ok := getTracked(as, syncChangeID)
	require.True(t, ok)
	require.Equal(t, usage, change.usage)
	require.NotNil(t, change.changeReplicasOp)
	require.Equal(t, changes, change.changeReplicasOp.chgs)

	as.PostApply(syncChangeID, true)
	change, ok = getTracked(as, syncChangeID)
	require.False(t, ok)
	require.Equal(t, trackedAllocatorChange{}, change)

	// Verify store pool was updated.
	require.Len(t, sp.rebalances, 3)
	require.Equal(t, roachpb.StoreID(2), sp.rebalances[0])
	require.Equal(t, roachpb.StoreID(2), sp.rebalances[1])
	require.Equal(t, roachpb.StoreID(4), sp.rebalances[2])
	// Remove a non-voter and add a voter to store 2, so store 2 should have 0
	// CPU load delta. Store 4 should have 1500 CPU load delta (1000 + 500).
	require.Equal(t, float64(0), sp.storeCPULoads[2])
	require.Equal(t, float64(1500), sp.storeCPULoads[4])
}

// TestAllocatorSyncMMAPreApply tests that allocator sync on a mma pending range
// change.
func TestAllocatorSyncMMAPreApply(t *testing.T) {
	defer leaktest.AfterTest(t)()

	as, _, _ := createTestAllocatorSync()

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	// Create a proper PendingRangeChange.
	pendingChange := mmaprototype.PendingRangeChange{
		RangeID: 1,
		// Note: We can't easily create a ReplicaChange here since it has
		// unexported fields.
	}

	syncChangeID := as.MMAPreApply(usage, pendingChange)

	require.True(t, syncChangeID.IsValid())
	require.Equal(t, SyncChangeID(1), syncChangeID)

	// Verify the change was tracked.
	change, ok := getTracked(as, syncChangeID)
	require.True(t, ok)
	require.Equal(t, usage, change.usage)
	require.NotNil(t, change.changeIDs)

	as.PostApply(syncChangeID, true)
	change, ok = getTracked(as, syncChangeID)
	require.False(t, ok)

	// We can't verify the store pool easily since pendingChange is not
	// populated with the actual changes due to the unexported fields.
}

// TestAllocatorSyncMarkChangesAsFailed tests that MarkChangesAsFailed informs
// mma allocator that the changes have failed without changing the store pool.
func TestAllocatorSyncMarkChangesAsFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	as, mma, _ := createTestAllocatorSync()
	mma.changes[1] = changeRegistered
	mma.changes[2] = changeRegistered
	mma.changes[3] = changeRegistered
	mma.changes[4] = changeRegistered

	changeIDs := []mmaprototype.ChangeID{1, 2, 3}
	as.MarkChangesAsFailed(changeIDs)

	// Verify that the changes were marked as failed and change 4 is still
	// registered.
	require.Equal(t, changeFailed, mma.changes[1])
	require.Equal(t, changeFailed, mma.changes[2])
	require.Equal(t, changeFailed, mma.changes[3])
	require.Equal(t, changeRegistered, mma.changes[4])
}

// TestConvertLeaseTransferToMMA tests that convertLeaseTransferToMMA converts
// a lease transfer operation to a ReplicaChange.
func TestConvertLeaseTransferToMMA(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &roachpb.RangeDescriptor{
		RangeID: 1,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{StoreID: 1, ReplicaID: 1, Type: roachpb.VOTER_FULL},
			{StoreID: 2, ReplicaID: 2, Type: roachpb.VOTER_FULL},
		},
	}

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	transferFrom := roachpb.ReplicationTarget{StoreID: 1, NodeID: 1}
	transferTo := roachpb.ReplicationTarget{StoreID: 2, NodeID: 2}

	changes := convertLeaseTransferToMMA(desc, usage, transferFrom, transferTo)
	require.Len(t, changes, 2)

	// Note that we can't check other fields of the ReplicaChange since they
	// are unexported.
}

// TestConvertReplicaChangeToMMA tests that convertReplicaChangeToMMA converts
// a replica change operation to a ReplicaChange.
func TestConvertReplicaChangeToMMA(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &roachpb.RangeDescriptor{
		RangeID: 1,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{StoreID: 1, ReplicaID: 1, Type: roachpb.VOTER_FULL},
			{StoreID: 2, ReplicaID: 2, Type: roachpb.VOTER_FULL},
		},
	}

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	changes := kvpb.ReplicationChanges{
		kvpb.ReplicationChange{
			ChangeType: roachpb.ADD_VOTER,
			Target:     roachpb.ReplicationTarget{StoreID: 3, NodeID: 3},
		},
	}

	replicaChanges := convertReplicaChangeToMMA(desc, usage, changes, 1)
	require.Len(t, replicaChanges, 1)
	// Note that we can't check other fields of the ReplicaChange since they
	// are unexported.
}

// TestMakeStoreLoadMsg tests that MakeStoreLoadMsg creates a StoreLoadMsg
// with the correct fields.
func TestMakeStoreLoadMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := roachpb.StoreDescriptor{
		StoreID: 1,
		Node: roachpb.NodeDescriptor{
			NodeID: 1,
		},
		Capacity: roachpb.StoreCapacity{
			CPUPerSecond:        1000,
			WriteBytesPerSecond: 1024,
			LogicalBytes:        2048,
			Capacity:            1000,
			Available:           200,
			LeaseCount:          500,
			RangeCount:          1000,
		},
		NodeCapacity: roachpb.NodeCapacity{
			NodeCPURateCapacity: 2000,
			NodeCPURateUsage:    1000,
			StoresCPURate:       1500,
			NumStores:           2,
		},
	}
	// For basic case:
	// load[cpu] = CPUPerSecond(1000)
	// load[write] = WriteBytesPerSecond(1024)
	// load[byte] = LogicalBytes(2048)

	// cpuUtil = NodeCPURateUsage(1000)/NodeCPURateCapacity(2000) (50%)
	// nodeCapacity = StoresCPURate(1500)/cpuUtil(0.5) (3000)
	// capacity[cpu] = nodeCapacity(3000)/NumStores(2) (1500)

	// capacity[write] = UnknownCapacity

	// byteSizeUtil = (Capacity(1000)-Available(200))/Capacity(1000) = 0.8
	// capacity[byteSize] = LogicalBytes(2048)/byteSizeUtil(0.8) (2560)

	// secondary load
	// lease count = 500, range count = 1000

	const expectedCPURate, expectedWriteBandwidth, expectedByteSize = 1000, 1024, 2048
	const expectedCapacityCPURate, expectedCapacityWriteBandwidth, expectedCapacityByteSize = 1500, mmaprototype.UnknownCapacity, 2560
	const expectedSecondaryLoadLeaseCount, expectedSecondaryLoadRangeCount = 500, 1000
	t.Run("basic", func(t *testing.T) {
		origTimestampNanos := int64(1234567890)
		msg := MakeStoreLoadMsg(desc, origTimestampNanos)

		require.Equal(t, roachpb.StoreID(1), msg.StoreID)
		require.Equal(t, origTimestampNanos, msg.LoadTime.UnixNano())

		// Test all LoadDimension values for Load.
		require.Equal(t, mmaprototype.LoadValue(expectedCPURate), msg.Load[mmaprototype.CPURate])
		require.Equal(t, mmaprototype.LoadValue(expectedWriteBandwidth), msg.Load[mmaprototype.WriteBandwidth])
		require.Equal(t, mmaprototype.LoadValue(expectedByteSize), msg.Load[mmaprototype.ByteSize])

		// Test all LoadDimension values for Capacity.
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityCPURate), msg.Capacity[mmaprototype.CPURate])
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityWriteBandwidth), msg.Capacity[mmaprototype.WriteBandwidth])
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityByteSize), msg.Capacity[mmaprototype.ByteSize])

		// Test secondary load.
		require.Equal(t, mmaprototype.LoadValue(expectedSecondaryLoadLeaseCount), msg.SecondaryLoad[mmaprototype.LeaseCount])
		require.Equal(t, mmaprototype.LoadValue(expectedSecondaryLoadRangeCount), msg.SecondaryLoad[mmaprototype.ReplicaCount])
	})
	t.Run("almost zero util", func(t *testing.T) {
		// If StoresCPURate is 0, store_capacity = ((NodeCPURateCapacity(200000000000)/2)/NumStores(2)) = 50000000000.
		desc.NodeCapacity.NodeCPURateUsage = 1
		desc.NodeCapacity.NodeCPURateCapacity = 200000000000
		// If capacity == available, byte size capacity is Available(200).
		desc.Capacity.Capacity = desc.Capacity.Available
		const expectedCapacityCPURate, expectedCapacityWriteBandwidth, expectedCapacityByteSize = 50000000000, mmaprototype.UnknownCapacity, 200

		origTimestampNanos := int64(1234567890)
		msg := MakeStoreLoadMsg(desc, origTimestampNanos)

		require.Equal(t, roachpb.StoreID(1), msg.StoreID)
		require.Equal(t, origTimestampNanos, msg.LoadTime.UnixNano())

		// Test all LoadDimension values for Load.
		require.Equal(t, mmaprototype.LoadValue(expectedCPURate), msg.Load[mmaprototype.CPURate])
		require.Equal(t, mmaprototype.LoadValue(expectedWriteBandwidth), msg.Load[mmaprototype.WriteBandwidth])
		require.Equal(t, mmaprototype.LoadValue(expectedByteSize), msg.Load[mmaprototype.ByteSize])

		// Test all LoadDimension values for Capacity.
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityCPURate), msg.Capacity[mmaprototype.CPURate])
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityWriteBandwidth), msg.Capacity[mmaprototype.WriteBandwidth])
		require.Equal(t, mmaprototype.LoadValue(expectedCapacityByteSize), msg.Capacity[mmaprototype.ByteSize])

		// Test secondary load.
		require.Equal(t, mmaprototype.LoadValue(expectedSecondaryLoadLeaseCount), msg.SecondaryLoad[mmaprototype.LeaseCount])
		require.Equal(t, mmaprototype.LoadValue(expectedSecondaryLoadRangeCount), msg.SecondaryLoad[mmaprototype.ReplicaCount])
	})
}

// TestMMARangeLoad tests that MMARangeLoad creates a RangeLoad with the
// correct fields.
func TestMMARangeLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	usage := allocator.RangeUsageInfo{
		RequestCPUNanosPerSecond: 1000,
		RaftCPUNanosPerSecond:    500,
		WriteBytesPerSecond:      1024,
		LogicalBytes:             2048,
	}

	rl := mmaRangeLoad(usage)

	expectedCPURate := mmaprototype.LoadValue(1000 + 500) // RequestCPU + RaftCPU.
	expectedRaftCPU := mmaprototype.LoadValue(500)
	expectedWriteBandwidth := mmaprototype.LoadValue(1024)
	expectedByteSize := mmaprototype.LoadValue(2048)

	require.Equal(t, expectedCPURate, rl.Load[mmaprototype.CPURate])
	require.Equal(t, expectedRaftCPU, rl.RaftCPU)
	require.Equal(t, expectedWriteBandwidth, rl.Load[mmaprototype.WriteBandwidth])
	require.Equal(t, expectedByteSize, rl.Load[mmaprototype.ByteSize])
}
