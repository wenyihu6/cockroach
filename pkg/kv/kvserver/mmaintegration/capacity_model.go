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

// Boundary contract: MMA operates exclusively on physical units (CPU ns/s,
// disk bytes). All conversion from logical per-range loads (direct replica CPU,
// MVCC bytes) to physical quantities happens in this package:
//
//   - MakeStoreLoadMsg converts a StoreDescriptor into a StoreLoadMsg whose
//     Load and Capacity fields are physical (CPU ns/s, disk bytes).
//   - MakePhysicalRangeLoad converts logical per-range loads into a physical
//     RangeLoad by applying AmplificationFactors.
//   - ComputeAmplificationFactors derives the factors from a StoreDescriptor.
//
// AmplificationFactors are never passed into MMA itself. MMA sees only physical
// LoadVectors, so its utilization metrics match observable node-level metrics.

// storeCPURateCapacityInput holds the inputs needed to compute per-store
// physical CPU load, capacity, and amplification factor. Using a struct avoids
// accidentally swapping the order of parameters.
type storeCPURateCapacityInput struct {
	// storesCPURate is the aggregated CPU usage across all stores on the node
	// (sum of per-store CPU load usage) in ns/sec.
	storesCPURate float64
	// nodeCPURateUsage is the total CPU usage of the node (from OS-level
	// metrics) in ns/sec.
	nodeCPURateUsage float64
	// nodeCPURateCapacity is the total CPU capacity of the node (from OS-level
	// metrics) in ns/sec.
	nodeCPURateCapacity float64
	// numStores is the number of stores on the node.
	numStores int32
}

// computeStoreByteSizeCapacity computes the byte size (disk) capacity for a
// store. The load is LogicalBytes (uncompressed) and the capacity is expressed
// in the same LogicalBytes-space, so that load/capacity recovers the actual
// disk utilization.
//
// The two cases are:
//
//  1. Normal: logicalBytes > 0 and diskFractionUsed >= 0.01.
//     capacity = logicalBytes / diskFractionUsed.
//     This ensures that logicalBytes/capacity equals the observed physical
//     disk utilization, encoding the space amplification factor
//     (physical/logical) into the capacity.
//
//  2. Empty/new store: logicalBytes is 0 or diskFractionUsed < 0.01.
//     We fall back to availableBytes (compressed/physical), which is
//     pessimistic since LogicalBytes are uncompressed.
//
// diskFractionUsed should be computed via StoreCapacity.FractionUsed(), which
// prefers Used/(Available+Used) to scope utilization to the store's own data.
// Available does not compensate for the ballast, so utilization will look
// slightly higher than actual. This is acceptable since the ballast is small
// (default 1% of capacity) and is for emergency use.
func computeStoreByteSizeCapacity(
	logicalBytes mmaprototype.LoadValue, diskFractionUsed float64, availableBytes int64,
) mmaprototype.LoadValue {
	almostZeroUtil := diskFractionUsed < 0.01
	if logicalBytes == 0 || almostZeroUtil {
		// Has no ranges or is almost empty. This is likely a new store. Since
		// LogicalBytes are uncompressed, we start with the compressed available,
		// which is desirably pessimistic.
		return mmaprototype.LoadValue(availableBytes)
	}
	// Normal case. The store has some ranges, and is not almost empty.
	return mmaprototype.LoadValue(float64(logicalBytes) / diskFractionUsed)
}

// cpuIndirectOverheadMultiplier is the maximum ratio of total CPU caused by
// store work to the directly-tracked store CPU. For example, a value of 3
// means we assume each unit of direct MMA load (replica CPU) can cause up to 2
// additional units of indirect CPU (RPC handling, compactions, etc.), for a
// total of 3 units. Any node CPU usage beyond storesCPURate * multiplier is
// treated as background load unrelated to MMA.
const cpuIndirectOverheadMultiplier = 3.0

// computeCPUCapacityWithCap computes per-store CPU capacity using a clamped
// multiplier. Unlike the naive model (computeStoreCPURateCapacityNaive in
// legacy_model_test.go, which assumes all node CPU usage is caused by
// MMA-tracked store work), this model distinguishes between MMA-attributed
// load and background load.
//
// The formula:
//
//  1. clampedMult = clamp(nodeCPURateUsage / storesCPURate, 1, cpuIndirectOverheadMultiplier)
//     When the implicit multiplier is low (<= cap), we assume all non-store
//     CPU is indirectly caused by store work (e.g. RPC overhead, compactions)
//     and will scale linearly with MMA's direct load.
//     When the implicit multiplier exceeds the cap, we stop attributing
//     unrelated CPU consumption (SQL gateway work, GC, background jobs) to
//     the stores.
//
//  2. backgroundLoad = nodeCPURateUsage - storesCPURate * clampedMult
//     The portion of node CPU usage that we assume is independent of
//     MMA-tracked work. This is zero when the implicit multiplier is below
//     the cap, and grows as auxiliary CPU usage increases.
//
//  3. mmaShareOfCapacity = nodeCPURateCapacity - backgroundLoad
//     The number of cores available for MMA-related work (both direct store
//     load and its indirect overhead).
//
//  4. mmaDirectCapacity = mmaShareOfCapacity / clampedMult
//     The capacity in terms of direct MMA load. Dividing by the multiplier
//     accounts for the indirect overhead: each unit of direct load consumes
//     clampedMult units of actual CPU.
//
//  5. storeCapacity = mmaDirectCapacity / numStores
//     Split evenly across stores on the node.
//
// When storesCPURate is 0 (no replicas reporting CPU yet), we use the limiting
// behavior: MMA attributes none of the node usage to itself (all is
// background), and divides the idle capacity by the multiplier across stores.
func computeCPUCapacityWithCap(in storeCPURateCapacityInput) (capacity float64) {
	mult := 0.0
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(context.Background(), "numStores and nodeCPURateCapacity must be > 0")
	}

	if in.storesCPURate <= 0 {
		// When MMA has zero load, the implicit multiplier is infinite, so we
		// use the cap. MMA attributes 0 * cap = 0 of the node usage to itself,
		// meaning all node usage is "background". MMA gets the remaining idle
		// capacity, scaled down by the multiplier.
		mult = cpuIndirectOverheadMultiplier
	} else {
		// Compute the implicit multiplier and clamp it to [1, cap].
		// - Clamping from above prevents pathological behavior when MMA tracks
		//   little load but the node has high CPU usage from other sources.
		// - Clamping from below (at 1) handles the case where MMA load exceeds
		//   node usage (shouldn't happen, but can due to measurement lag).
		//   Without this, MMA would get unreasonably high capacity.
		implicitMult := in.nodeCPURateUsage / in.storesCPURate
		mult = max(1, min(implicitMult, cpuIndirectOverheadMultiplier))
	}

	// Background load is the portion of node usage NOT attributed to MMA.
	// This is clamped to be non-negative.
	mmaAttributedLoad := in.storesCPURate * mult
	backgroundLoad := max(0.0, in.nodeCPURateUsage-mmaAttributedLoad)

	// MMA's share of capacity is what remains after background load.
	// Clamp to non-negative to handle overloaded nodes where backgroundLoad
	// exceeds nodeCPURateCapacity.
	mmaShareOfCapacity := max(0.0, in.nodeCPURateCapacity-backgroundLoad)

	// MMA's direct capacity is scaled down by the multiplier to account
	// for indirect overhead.
	mmaDirectCapacity := mmaShareOfCapacity / mult

	// Divide evenly across stores.
	capacity = mmaDirectCapacity / float64(in.numStores)
	return capacity
}

// physicalCPUResult holds the outputs of the physical CPU model.
type physicalCPUResult struct {
	// load is the per-store physical CPU usage in ns/s. The sum of load across
	// all stores on a node equals the node's total CPU usage (nodeCPURateUsage).
	load float64
	// capacity is the per-store physical CPU capacity in ns/s. Equal to
	// nodeCPURateCapacity / numStores, i.e. actual physical cores.
	capacity float64
	// amplificationFactor converts direct replica CPU (logical) to total
	// physical CPU footprint (>= 1). Used at the integration boundary to
	// amplify per-range load deltas before passing them into MMA.
	amplificationFactor float64
}

// computePhysicalCPU computes per-store physical CPU load, capacity, and
// amplification factor.
//
// All outputs are in physical CPU units (ns/s). MMA's utilization
// (load/capacity) directly matches the node-level CPU utilization observable
// via OS metrics:
//
//	sum(store.load) = nodeCPURateUsage
//	sum(store.capacity) = nodeCPURateCapacity
//	mean utilization = nodeCPURateUsage / nodeCPURateCapacity
//
// The amplification factor is the clamped ratio of total node CPU to
// directly-tracked store CPU. It is used outside MMA to convert per-range
// logical CPU deltas to physical units.
//
// For load distribution across stores: we spread the node's total CPU usage
// proportionally to each store's share of storesCPURate. With a single store
// this equals nodeCPURateUsage; with multiple stores each gets its proportional
// share. When storesCPURate is 0 (no replicas reporting CPU yet), we split
// evenly.
func computePhysicalCPU(in storeCPURateCapacityInput) physicalCPUResult {
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(
			context.Background(), "numStores and nodeCPURateCapacity must be > 0",
		)
	}

	numStores := float64(in.numStores)
	capacity := in.nodeCPURateCapacity / numStores

	// Compute amplification factor.
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

// maxDiskSpaceAmplification caps the ratio of physical disk bytes used to
// logical (MVCC) bytes. Values above this are treated as if the extra physical
// usage is independent of range data (e.g. WAL, auxiliary files).
const maxDiskSpaceAmplification = 5.0

// physicalDiskResult holds the outputs of the physical disk model.
type physicalDiskResult struct {
	// load is the physical disk bytes used by the store.
	load float64
	// capacity is the total usable disk space (Used + Available).
	capacity float64
	// amplificationFactor converts logical bytes (MVCC) to physical bytes
	// (>= 1, capped at maxDiskSpaceAmplification). Used at the integration
	// boundary to amplify per-range byte-size deltas.
	amplificationFactor float64
}

// computePhysicalDisk computes physical disk load, capacity, and space
// amplification factor. Both load and capacity are in physical bytes so that
// load/capacity = Used/(Used+Available) = actual disk utilization.
//
// For empty/new stores (logicalBytes == 0 or used == 0), the amplification
// factor defaults to 1.0.
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

// AmplificationFactors holds CPU and disk amplification factors that convert
// logical per-range loads (direct replica CPU, MVCC bytes) into physical units.
// These are computed from store metrics and applied at the integration boundary
// so that MMA operates exclusively on physical quantities.
type AmplificationFactors struct {
	CPU  float64
	Disk float64
}

// ComputeAmplificationFactors returns the CPU and disk amplification factors
// for a store, given its descriptor. These factors convert logical per-range
// loads (direct replica CPU, MVCC bytes) into physical units for use at the
// MMA integration boundary.
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

// MakePhysicalRangeLoad converts logical per-range load measurements into a
// physical RangeLoad by applying the amplification factors. This is the single
// entry point for all logical-to-physical range load conversion and should be
// called at the integration boundary before passing range loads to MMA.
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
