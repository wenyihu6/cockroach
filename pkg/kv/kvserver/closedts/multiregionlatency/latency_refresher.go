// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionlatency

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const numLocalityComparisonTypes = 4
const defaultMaxNetworkRoundTrip = 150 * time.Millisecond

// NetworkLatency tracks network latency between nodes based on their locality.
type LatencyRefresher struct {
	//nodeID         roachpb.NodeID
	nodeLocality   roachpb.Locality
	getNodeDesc    func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	getLatency     func(roachpb.NodeID) (time.Duration, bool)
	roundTripTimes [numLocalityComparisonTypes]atomic.Int64
}

func NewLatencyRefresher(
	nodeLocality roachpb.Locality,
	getLatency func(roachpb.NodeID) (time.Duration, bool),
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error),
) *LatencyRefresher {
	l := &LatencyRefresher{nodeLocality: nodeLocality, getLatency: getLatency, getNodeDesc: getNodeDesc}
	for i := 0; i < numLocalityComparisonTypes; i++ {
		l.updateLatencyForLocalityProximity(roachpb.LocalityComparisonType(i), defaultMaxNetworkRoundTrip)
	}
	return l
}

// GetLatencyByLocalityProximity returns the latency for the given locality comparison type.
func (l *LatencyRefresher) GetLatencyByLocalityProximity(lct roachpb.LocalityComparisonType) time.Duration {
	return time.Duration(l.roundTripTimes[lct].Load())
}

// UpdateLatencyForLocalityProximity updates the latency for the given locality comparison type.
func (l *LatencyRefresher) updateLatencyForLocalityProximity(lct roachpb.LocalityComparisonType, updatedLatency time.Duration) {
	l.roundTripTimes[lct].Store(int64(updatedLatency))
}

// RefreshLatency updates latencies for all locality comparison types based on the given node IDs.
func (l *LatencyRefresher) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	fromLocality := l.nodeLocality
	maxLatencies := [numLocalityComparisonTypes]time.Duration{}

	for _, nodeID := range nodeIDs {
		// May be refreshing on the same node as the l but it is okay since we are taking the max.
		toNodeDesc, err := l.getNodeDesc(nodeID)
		if err != nil {
			continue
		}
		comparisonResult, _, _ := fromLocality.CompareWithLocality(toNodeDesc.Locality)
		if latency, ok := l.getLatency(nodeID); ok {
			maxLatencies[comparisonResult] = max(maxLatencies[comparisonResult], latency)
		}
	}

	for i := 0; i < numLocalityComparisonTypes; i++ {
		if maxLatencies[i] == 0 {
			maxLatencies[i] = defaultMaxNetworkRoundTrip
		}
		l.updateLatencyForLocalityProximity(roachpb.LocalityComparisonType(i), maxLatencies[i])
	}
}
