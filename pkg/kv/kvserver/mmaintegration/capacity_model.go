// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Unit conversion boundary
//
// Per-range loads are reported in replica-CPU (ns/s) and MVCC-bytes — these
// only account for work directly tracked at each replica (request evaluation,
// raft processing, logical data size). Store and node-level metrics, however,
// are measured in node-CPU (ns/s) and disk-bytes — these include all overhead
// (RPC handling, compactions, WAL, tombstones, etc.).
//
// MMA operates exclusively in node-CPU and disk-bytes so that its utilization
// numbers (load/capacity) match what is observable at the OS level. All
// conversion from replica-CPU to node-CPU, and from MVCC-bytes to disk-bytes,
// happens in this package via amplification factors that act as unit-conversion
// rates:
//
//   node-CPU   = replica-CPU  × cpuAmpFactor
//   disk-bytes = MVCC-bytes   × diskAmpFactor
//
// Three functions implement this boundary:
//
//   - MakeStoreLoadMsg converts a StoreDescriptor into a StoreLoadMsg whose
//     Load and Capacity fields are in node-CPU (ns/s) and disk-bytes.
//   - MakePhysicalRangeLoad converts per-range loads from replica-CPU and
//     MVCC-bytes into node-CPU and disk-bytes by applying AmplificationFactors.
//   - ComputeAmplificationFactors derives the conversion rates from a
//     StoreDescriptor.
//
// AmplificationFactors are never passed into MMA itself. MMA sees only
// node-CPU and disk-bytes LoadVectors.

// storeCPURateCapacityInput holds the inputs needed to compute per-store
// node-CPU load, capacity, and the replica-CPU → node-CPU conversion factor.
// Using a struct avoids accidentally swapping the order of parameters.
type storeCPURateCapacityInput struct {
	// storesCPURate is the aggregate replica-CPU across all stores on the node
	// (sum of directly-tracked per-store CPU) in ns/s.
	storesCPURate float64
	// nodeCPURateUsage is the total node-CPU usage (from OS-level metrics) in
	// ns/s. This includes indirect overhead not tracked per-replica.
	nodeCPURateUsage float64
	// nodeCPURateCapacity is the total node-CPU capacity (from OS-level
	// metrics) in ns/s — e.g. 8 cores = 8e9.
	nodeCPURateCapacity float64
	// numStores is the number of stores on the node.
	numStores int32
}

// cpuIndirectOverheadMultiplier caps the replica-CPU → node-CPU conversion
// factor. A value of 3 means each nanosecond of replica-CPU can account for up
// to 3 nanoseconds of node-CPU (the original work plus up to 2× indirect
// overhead from RPC handling, compactions, etc.). Any node-CPU usage beyond
// storesCPURate × multiplier is treated as background work unrelated to
// replica activity.
const cpuIndirectOverheadMultiplier = 3.0

// physicalCPUResult holds the outputs of the CPU unit-conversion model.
type physicalCPUResult struct {
	// load is the per-store CPU in node-CPU ns/s. The sum across all stores on
	// a node equals nodeCPURateUsage.
	load float64
	// capacity is the per-store CPU capacity in node-CPU ns/s. Equal to
	// nodeCPURateCapacity / numStores.
	capacity float64
	// amplificationFactor is the replica-CPU → node-CPU conversion rate
	// (>= 1, <= cpuIndirectOverheadMultiplier). Applied at the integration
	// boundary to convert per-range replica-CPU deltas into node-CPU before
	// passing them into MMA.
	amplificationFactor float64
}

// computePhysicalCPU computes per-store node-CPU load, capacity, and the
// replica-CPU → node-CPU conversion factor.
//
// All outputs are in node-CPU (ns/s), so MMA's utilization (load/capacity)
// directly matches the OS-observable CPU utilization:
//
//	sum(store.load)     = nodeCPURateUsage
//	sum(store.capacity) = nodeCPURateCapacity
//	mean utilization    = nodeCPURateUsage / nodeCPURateCapacity
//
// The amplification factor is the clamped ratio of node-CPU to replica-CPU
// (i.e. nodeCPURateUsage / storesCPURate). It is used outside MMA to convert
// per-range loads from replica-CPU to node-CPU.
//
// Downstream consumers — all in node-CPU (ns/s):
//
//  1. MakeStoreLoadMsg sets StoreLoadMsg.Load[CPURate] = load and
//     StoreLoadMsg.Capacity[CPURate] = capacity. Inside MMA,
//     processStoreLoadMsg stores these as storeState.reportedLoad[CPURate]
//     and storeState.capacity[CPURate].
//
//  2. computeMeansForStoreSet sums load and capacity across stores:
//     meanUtil = sum(load) / sum(capacity). Because both sides are in
//     node-CPU the ratio equals real cluster CPU utilization.
//
//  3. loadSummaryForDimension compares a store's load and capacity to the
//     cluster mean:
//     - fractionAbove = load/meanLoad - 1   (drives mean-based rebalancing)
//     - fractionUsed  = load/capacity        (drives overload detection)
//
//  4. canShedAndAddLoad temporarily adds/subtracts per-range deltas (in
//     node-CPU, after conversion from replica-CPU via ampFactor) to a
//     store's adjusted load and recomputes the load summary to decide
//     whether a transfer is safe.
//
// Unit-consistency proof (single-store node):
//
//	sum(per-range replica-CPU) ≈ storesCPURate
//	ampFactor                  = nodeCPURateUsage / storesCPURate  (clamped)
//	sum(per-range node-CPU)    = sum(per-range replica-CPU) × ampFactor
//	                           ≈ storesCPURate × (nodeCPURateUsage / storesCPURate)
//	                           = nodeCPURateUsage
//	                           = store.load
//
// So when canShedAndAddLoad (item 4) adds a per-range node-CPU delta to a
// store's adjusted load (also node-CPU), the arithmetic is in consistent
// units, and load/capacity continues to reflect actual CPU utilization.
//
// Multi-store distribution: we split the node's total CPU usage evenly across
// stores (matching how capacity is split). A proportional split weighted by
// each store's replica-CPU would be more precise but requires per-store info
// not available here.
func computePhysicalCPU(in storeCPURateCapacityInput) physicalCPUResult {
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(
			context.Background(), "numStores and nodeCPURateCapacity must be > 0",
		)
	}

	numStores := float64(in.numStores)
	capacity := in.nodeCPURateCapacity / numStores

	// Compute the replica-CPU → node-CPU conversion rate. When no replica-CPU
	// is reported yet (storesCPURate <= 0), assume the maximum overhead.
	var ampFactor float64
	if in.storesCPURate <= 0 {
		ampFactor = cpuIndirectOverheadMultiplier
	} else {
		implicitMult := in.nodeCPURateUsage / in.storesCPURate
		ampFactor = max(1, min(implicitMult, cpuIndirectOverheadMultiplier))
	}

	// Physical load per store: spread node CPU usage evenly across stores.
	// With a single store this is just nodeCPURateUsage. With multiple stores
	// each gets an equal share. A proportional distribution (weighted by each
	// store's CPUPerSecond) would be more precise for multi-store but requires
	// per-store info not available here; even splitting is the simplest
	// starting point and matches how we split capacity.
	load := in.nodeCPURateUsage / numStores

	return physicalCPUResult{
		load:                load,
		capacity:            capacity,
		amplificationFactor: ampFactor,
	}
}

// maxDiskSpaceAmplification caps the MVCC-bytes → disk-bytes conversion
// factor. Ratios above this are treated as if the extra disk usage is
// independent of range data (e.g. WAL, auxiliary files).
const maxDiskSpaceAmplification = 5.0

// physicalDiskResult holds the outputs of the disk unit-conversion model.
type physicalDiskResult struct {
	// load is the store's disk usage in disk-bytes.
	load float64
	// capacity is the total usable disk space (Used + Available) in disk-bytes.
	capacity float64
	// amplificationFactor is the MVCC-bytes → disk-bytes conversion rate
	// (>= 1, <= maxDiskSpaceAmplification). Applied at the integration
	// boundary to convert per-range MVCC-byte sizes into disk-bytes before
	// passing them into MMA.
	amplificationFactor float64
}

// computePhysicalDisk computes per-store disk-byte load, capacity, and the
// MVCC-bytes → disk-bytes conversion factor. Both load and capacity are in
// disk-bytes so that load/capacity = Used/(Used+Available) = actual disk
// utilization.
//
// For empty/new stores (logicalBytes == 0 or used == 0), the conversion
// factor defaults to 1.0 (MVCC-bytes ≈ disk-bytes).
func computePhysicalDisk(logicalBytes int64, used int64, available int64) physicalDiskResult {
	capacity := float64(used + available)
	var ampFactor float64
	if logicalBytes > 0 && used > 0 {
		ampFactor = float64(used) / float64(logicalBytes)
		ampFactor = max(1.0, min(ampFactor, maxDiskSpaceAmplification))
	} else {
		ampFactor = 1.0
	}
	return physicalDiskResult{
		load:                float64(used),
		capacity:            capacity,
		amplificationFactor: ampFactor,
	}
}

// AmplificationFactors holds the unit-conversion rates for CPU and disk.
// CPU converts replica-CPU → node-CPU; Disk converts MVCC-bytes → disk-bytes.
// These are computed from store metrics and applied at the integration boundary
// so that MMA operates exclusively in node-CPU and disk-bytes.
type AmplificationFactors struct {
	CPU  float64 // replica-CPU → node-CPU
	Disk float64 // MVCC-bytes → disk-bytes
}

// ComputeAmplificationFactors returns the replica-CPU → node-CPU and
// MVCC-bytes → disk-bytes conversion rates for a store, derived from its
// descriptor.
//
// Design note: the same computePhysicalCPU / computePhysicalDisk functions
// are also called by MakeStoreLoadMsg to derive the store-level load and
// capacity in node-CPU / disk-bytes. Ideally both paths would use factors from
// the exact same snapshot of store metrics, guaranteeing that the converted
// per-range loads are perfectly consistent with the store-level load MMA
// received. In practice, the factors are computed from cached metrics that may
// be from a slightly different point in time than the StoreDescriptor used by
// MakeStoreLoadMsg. This is acceptable because:
//  1. The underlying inputs (node CPU EWMA, space amplification) are
//     slow-moving; the drift between two successive reads is negligible.
//  2. MMA already tolerates mismatch between store-level load and the sum of
//     per-range loads (not all ranges report, follower replicas contribute to
//     store load but not to range loads, etc.).
//
// If tighter consistency is ever needed, the factors can be returned as a
// byproduct of MakeStoreLoadMsg and cached alongside the StoreLoadMsg.
func ComputeAmplificationFactors(desc roachpb.StoreDescriptor) AmplificationFactors {
	amp := AmplificationFactors{CPU: 1.0, Disk: 1.0}

	if desc.NodeCapacity.NodeCPURateCapacity > 0 && desc.NodeCapacity.NumStores > 0 {
		cpuResult := computePhysicalCPU(storeCPURateCapacityInput{
			storesCPURate:       float64(desc.NodeCapacity.StoresCPURate),
			nodeCPURateUsage:    float64(desc.NodeCapacity.NodeCPURateUsage),
			nodeCPURateCapacity: float64(desc.NodeCapacity.NodeCPURateCapacity),
			numStores:           desc.NodeCapacity.NumStores,
		})
		amp.CPU = cpuResult.amplificationFactor
	}

	diskResult := computePhysicalDisk(
		desc.Capacity.LogicalBytes,
		desc.Capacity.Used,
		desc.Capacity.Available,
	)
	amp.Disk = diskResult.amplificationFactor
	return amp
}

// MakePhysicalRangeLoad converts per-range loads from replica-CPU and
// MVCC-bytes into node-CPU and disk-bytes by applying the amplification
// factors. This is the single entry point for all per-range unit conversion
// and should be called at the integration boundary before passing range loads
// to MMA. WriteBandwidth is already in a common unit and passes through
// without conversion.
func MakePhysicalRangeLoad(
	requestCPUNanos, raftCPUNanos, writeBytesPerSec float64,
	logicalBytes int64,
	amp AmplificationFactors,
) mmaprototype.RangeLoad {
	var rl mmaprototype.RangeLoad
	cpuNanos := requestCPUNanos + raftCPUNanos
	rl.Load[mmaprototype.CPURate] = mmaprototype.LoadValue(cpuNanos * amp.CPU)
	rl.RaftCPU = mmaprototype.LoadValue(raftCPUNanos * amp.CPU)
	rl.Load[mmaprototype.WriteBandwidth] = mmaprototype.LoadValue(writeBytesPerSec)
	rl.Load[mmaprototype.ByteSize] = mmaprototype.LoadValue(
		float64(logicalBytes) * amp.Disk)
	return rl
}

// computeStoreByteSizeCapacity is the legacy disk capacity model retained for
// comparison in tests. Unlike the current model which converts everything to
// disk-bytes, this kept load in MVCC-bytes and baked the MVCC→disk conversion
// into a virtual capacity so that load/capacity still recovered actual disk
// utilization.
func computeStoreByteSizeCapacity(
	logicalBytes int64, diskFractionUsed float64, availableBytes int64,
) int64 {
	almostZeroUtil := diskFractionUsed < 0.01
	if logicalBytes == 0 || almostZeroUtil {
		return availableBytes
	}
	return int64(float64(logicalBytes) / diskFractionUsed)
}

// computeCPUCapacityWithCap is the legacy CPU capacity model retained for
// comparison in tests. Unlike the current model which converts everything to
// node-CPU, this kept load in replica-CPU and baked the replica→node
// conversion into a virtual capacity.
func computeCPUCapacityWithCap(in storeCPURateCapacityInput) (capacity float64) {
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(
			context.Background(), "numStores and nodeCPURateCapacity must be > 0",
		)
	}

	var mult float64
	if in.storesCPURate <= 0 {
		mult = cpuIndirectOverheadMultiplier
	} else {
		implicitMult := in.nodeCPURateUsage / in.storesCPURate
		mult = max(1, min(implicitMult, cpuIndirectOverheadMultiplier))
	}

	mmaAttributedLoad := in.storesCPURate * mult
	backgroundLoad := max(0.0, in.nodeCPURateUsage-mmaAttributedLoad)
	mmaShareOfCapacity := max(0.0, in.nodeCPURateCapacity-backgroundLoad)
	mmaDirectCapacity := mmaShareOfCapacity / mult
	capacity = mmaDirectCapacity / float64(in.numStores)
	return capacity
}
