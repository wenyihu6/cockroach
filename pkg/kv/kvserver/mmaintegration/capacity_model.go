// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This file contains the capacity model functions used by MMA to derive
// per-store capacity from node-level and disk-level metrics. These are
// extracted here so they can be unit tested directly.

// storeCPURateCapacityInput holds the inputs needed to compute per-store CPU
// capacity. Using a struct avoids accidentally swapping the order of parameters
// when passing them to functions.
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
	// sqlGatewayCPUNanoPerSec is the SQL gateway CPU usage in ns/sec, measured
	// by the runtime load monitor tracking SQL work executed at gateway nodes.
	sqlGatewayCPUNanoPerSec float64
	// sqlDistCPUNanoPerSec is the distributed SQL CPU usage in ns/sec, measured
	// by the runtime load monitor tracking distributed SQL work.
	sqlDistCPUNanoPerSec float64
	// numStores is the number of stores on the node.
	numStores int32
}

// assertValid asserts that the input is valid.
func (in *storeCPURateCapacityInput) assertValid() {
	// TODO(wenyihu6): should we always assert instead?
	if !buildutil.CrdbTestBuild {
		return
	}
	if in.numStores <= 0 || in.nodeCPURateCapacity <= 0 {
		log.KvDistribution.Fatalf(context.Background(), "numStores and nodeCPURateCapacity must be > 0")
	}
	if in.storesCPURate < 0 {
		log.KvDistribution.Fatalf(context.Background(), "storesCPURate must be > 0")
	}
	if in.nodeCPURateUsage < 0 {
		log.KvDistribution.Fatalf(context.Background(), "nodeCPURateUsage must be > 0")
	}
	if in.sqlGatewayCPUNanoPerSec < 0 {
		log.KvDistribution.Fatalf(context.Background(), "sqlGatewayCPUNanoPerSec must be >= 0")
	}
	if in.sqlDistCPUNanoPerSec < 0 {
		log.KvDistribution.Fatalf(context.Background(), "sqlDistCPUNanoPerSec must be >= 0")
	}
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

// maxDiskSpaceAmplification caps the ratio of physical disk bytes used to
// logical (MVCC) bytes. Values above this are treated as if the extra physical
// usage is independent of range data (e.g. WAL, auxiliary files).
const maxDiskSpaceAmplification = 5.0

// physicalCPUResult holds the outputs of the physical CPU model.
type physicalCPUResult struct {
	// load is the per-store direct replica CPU in ns/s (unchanged from input).
	load float64
	// capacity is the physical CPU capacity available to this store, in ns/s.
	capacity float64
	// amplificationFactor converts direct replica CPU to total physical CPU
	// footprint (>= 1). MMA multiplies per-range CPU deltas by this factor
	// when adjusting store loads.
	amplificationFactor float64
}

// computePhysicalCPU computes per-store physical CPU load, capacity, and
// amplification factor using a capped-multiplier model.
//
// The key idea: load and capacity are both in physical CPU units.
//   - load = per-store direct replica CPU (storesCPURate / numStores)
//   - capacity = physical CPU capacity for MMA work on this store
//     (node capacity minus background, divided by numStores)
//   - amplificationFactor = clamped multiplier, so that
//     load * amplificationFactor = physical CPU consumed by this store's
//     range work (direct + indirect overhead)
//
// utilization = (load * amplificationFactor + fixedUsageShare) / capacity
// matches the node-level CPU utilization observable via OS metrics.
func computePhysicalCPU(in storeCPURateCapacityInput) physicalCPUResult {
	in.assertValid()

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

	perStoreLoad := in.storesCPURate / float64(in.numStores)
	perStoreCapacity := mmaShareOfCapacity / float64(in.numStores)

	return physicalCPUResult{
		load:                perStoreLoad,
		capacity:            perStoreCapacity,
		amplificationFactor: mult,
	}
}

// physicalDiskResult holds the outputs of the physical disk model.
type physicalDiskResult struct {
	// load is the physical disk bytes used by the store.
	load float64
	// capacity is the total usable disk space (Used + Available).
	capacity float64
	// amplificationFactor converts logical bytes (MVCC) to physical bytes
	// (>= 1, capped at maxDiskSpaceAmplification).
	amplificationFactor float64
}

// computePhysicalDisk computes physical disk load, capacity, and space
// amplification factor. Unlike the legacy computeStoreByteSizeCapacity which
// operates in logical-byte space, this function works entirely in physical
// units.
//
//   - load = Used (physical bytes consumed by the store)
//   - capacity = Used + Available (total usable disk space)
//   - amplificationFactor = clamp(Used / LogicalBytes, 1, maxDiskSpaceAmplification)
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

// computeStoreCPURateCapacityWithSQL computes per-store CPU capacity using the
// alternative model that accounts for SQL gateway and distributed SQL CPU
// separately. This model fits:
//
//	(SSCR + SQL_DIST + SQL_G) * K1 = NCR
//	SSCR * K2 = SQL_DIST
//
// Where:
//   - SSCR = storesCPURate (aggregate KV CPU across all stores)
//   - SQL_DIST = sqlDistCPUNanoPerSec (distributed SQL CPU)
//   - SQL_G = sqlGatewayCPUNanoPerSec (gateway SQL CPU)
//   - NCR = nodeCPURateUsage (total node CPU usage)
//   - NCRC = nodeCPURateCapacity (total node CPU capacity)
//   - K1 = multiplier for total CPU overhead
//   - K2 = multiplier for SQL_DIST relative to SSCR
//
// Solving for K1 and K2:
//
//	K2 = SQL_DIST / SSCR (if SSCR > 0)
//	K1 = NCR / (SSCR + SQL_DIST + SQL_G) (if denominator > 0)
//
// To find the maximum SSCR the node can sustain (NodeCapacity), we set total
// CPU = NCRC. Gateway SQL is fixed background, so SQL_G stays constant while
// SSCR grows. DistSQL scales with KV: SQL_DIST = K2 * SSCR. So at capacity:
//
//	(SSCR_max + K2*SSCR_max + SQL_G) * K1 = NCRC
//	SSCR_max*(K2+1)*K1 + SQL_G*K1 = NCRC
//	SSCR_max*(K2+1)*K1 = NCRC - SQL_G*K1
//	SSCR_max = (NCRC - SQL_G*K1) / ((K2+1)*K1)
//
// In other words: subtract gateway SQL's share (SQL_G*K1) from total capacity,
// then divide by the per-unit cost of KV work ((K2+1)*K1).
//
//	NodeCapacity = (NCRC - SQL_G*K1) / ((K2+1)*K1)
//	StoreCapacity = NodeCapacity / numStores
//
// This model correctly attributes gateway SQL CPU (SQL_G) as background load
// that doesn't scale with store work, while distributed SQL CPU (SQL_DIST) is
// assumed to scale proportionally with KV work.
func computeStoreCPURateCapacityWithSQL(in storeCPURateCapacityInput) (capacity float64) {
	in.assertValid()

	// Handle edge cases where we can't compute the model.
	if in.storesCPURate <= 0 {
		// No store CPU usage. Use a fallback: assume SQL_G is background and
		// distribute remaining capacity evenly, scaled by a conservative multiplier.
		const fallbackMultiplier = 3.0
		backgroundLoad := in.sqlGatewayCPUNanoPerSec
		availableCapacity := max(0.0, in.nodeCPURateCapacity-backgroundLoad)
		nodeCapacity := availableCapacity / fallbackMultiplier
		return nodeCapacity / float64(in.numStores)
	}

	// Compute K2: SQL_DIST scales proportionally with SSCR.
	k2 := in.sqlDistCPUNanoPerSec / in.storesCPURate

	// Compute K1: total CPU overhead multiplier.
	// (SSCR + SQL_DIST + SQL_G) * K1 = NCR
	totalAttributedLoad := in.storesCPURate + in.sqlDistCPUNanoPerSec + in.sqlGatewayCPUNanoPerSec
	if totalAttributedLoad <= 0 {
		// Fallback: assume no overhead.
		nodeCapacity := in.nodeCPURateCapacity / (k2 + 1)
		return nodeCapacity / float64(in.numStores)
	}
	// K1 represents unobserved overhead (OS-level CPU beyond attributed
	// SQL+KV). Clamp to 1.0: if nodeCPURateUsage < totalAttributedLoad.
	k1 := max(1.0, in.nodeCPURateUsage/totalAttributedLoad)

	// effectiveMult = (K2+1)*K1: total node CPU cost per core of SSCR.
	// NodeCapacity = (NCRC - SQL_G*K1) / effectiveMult
	sqlGatewayAttributedLoad := in.sqlGatewayCPUNanoPerSec * k1
	availableCapacity := max(0.0, in.nodeCPURateCapacity-sqlGatewayAttributedLoad)
	effectiveMult := (k2 + 1) * k1
	nodeCapacity := availableCapacity / effectiveMult
	return nodeCapacity / float64(in.numStores)
}
