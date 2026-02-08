// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MakeStoreLoadMsg makes a store load message.
//
// TODO(wenyihu6): Add more tests for this function.
func MakeStoreLoadMsg(
	desc roachpb.StoreDescriptor, origTimestampNanos int64,
) mmaprototype.StoreLoadMsg {
	var load, capacity mmaprototype.LoadVector
	load[mmaprototype.CPURate] = mmaprototype.LoadValue(desc.Capacity.CPUPerSecond)
	if desc.NodeCapacity.NodeCPURateCapacity > 0 {
		capacity[mmaprototype.CPURate] = computeStoreCPUCapacity(desc.NodeCapacity)
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

// computeStoreCPUCapacity computes the per-store CPU capacity from the
// NodeCapacity. It uses a model that accounts for SQL CPU (both gateway
// and DistSQL) when available, falling back to the original scheme when
// SQL CPU measurements are not available.
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
//   - SQL_G = SQLGatewayCPURate (gateway SQL work, NOT proportional to replicas)
//   - SQL_DIST = SQLDistCPURate (DistSQL work, proportional to replica load)
//   - NCR = NodeCPURateUsage (actual process CPU)
//   - NCRC = NodeCPURateCapacity (CPU capacity)
//   - K1 = scale factor from measured CPU to actual process CPU
//   - K2 = ratio of DistSQL CPU to KV replica CPU
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
func computeStoreCPUCapacity(nc roachpb.NodeCapacity) mmaprototype.LoadValue {
	SSCR := float64(nc.StoresCPURate)
	NCR := float64(nc.NodeCPURateUsage)
	NCRC := float64(nc.NodeCPURateCapacity)
	SQL_G := float64(nc.SQLGatewayCPURate)
	SQL_DIST := float64(nc.SQLDistCPURate)
	N := float64(nc.NumStores)

	cpuUtil := NCR / NCRC
	almostZeroUtil := cpuUtil < 0.01

	if SSCR == 0 || almostZeroUtil || N == 0 {
		// Fallback: StoresCPURate is zero, utilization is near-zero, or no
		// stores. We assume that only 50% of the usage can be accounted for
		// in StoresCPURate, so we divide 50% of the capacity among all stores.
		if N == 0 {
			N = 1
		}
		return mmaprototype.LoadValue(NCRC / 2 / N)
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
		return mmaprototype.LoadValue(storeCapacity)
	}

	// Alternative scheme with SQL CPU accounting.
	totalMeasured := SSCR + SQL_DIST + SQL_G

	// K1 = NCR / totalMeasured
	// K2 = SQL_DIST / SSCR
	K1 := NCR / totalMeasured
	K2 := SQL_DIST / SSCR

	// NodeCapacity = (NCRC - SQL_G * K1) / ((K2 + 1) * K1)
	numerator := NCRC - SQL_G*K1
	denominator := (K2 + 1) * K1

	if denominator <= 0 || !math.IsFinite(numerator/denominator) {
		// Safety: if the formula produces non-finite results, fall back.
		nodeCapacity := SSCR / cpuUtil
		return mmaprototype.LoadValue(nodeCapacity / N)
	}

	nodeCapacity := numerator / denominator
	// Clamp to avoid negative capacity (can happen if gateway CPU is very
	// large relative to total capacity).
	if nodeCapacity < 0 {
		nodeCapacity = SSCR / cpuUtil
	}

	storeCapacity := nodeCapacity / N
	return mmaprototype.LoadValue(storeCapacity)
}
