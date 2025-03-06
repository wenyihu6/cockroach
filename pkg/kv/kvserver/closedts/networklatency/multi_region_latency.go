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
	nodeID          roachpb.NodeID
	getNodeLocality func(roachpb.NodeID) roachpb.Locality
	getLatency      func(roachpb.NodeID) (time.Duration, bool)
	roundTripTimes  [numLocalityComparisonTypes]atomic.Int64
}

// NewLatencyRefresher creates a new NetworkLatency instance with the given latency lookup function.
func NewLatencyRefresher(
	nodeID roachpb.NodeID,
	getNodeLocality func(roachpb.NodeID) roachpb.Locality,
	latencyFunc func(roachpb.NodeID) (time.Duration, bool),
) *LatencyRefresher {
	l := &LatencyRefresher{
		nodeID:          nodeID,
		getNodeLocality: getNodeLocality,
		getLatency:      latencyFunc,
	}
	// Initialize all locality comparison types with default latency
	for i := 0; i < numLocalityComparisonTypes; i++ {
		l.UpdateLatencyForLocalityProximity(roachpb.LocalityComparisonType(i), defaultMaxNetworkRoundTrip)
	}
	return l
}

// GetLatencyByLocalityProximity returns the latency for the given locality comparison type.
func (l *LatencyRefresher) GetLatencyByLocalityProximity(lct roachpb.LocalityComparisonType) time.Duration {
	return time.Duration(l.roundTripTimes[lct].Load())
}

// UpdateLatencyForLocalityProximity updates the latency for the given locality comparison type.
func (l *LatencyRefresher) UpdateLatencyForLocalityProximity(lct roachpb.LocalityComparisonType, updatedLatency time.Duration) {
	l.roundTripTimes[lct].Store(int64(updatedLatency))
}

// RefreshLatency updates latencies for all locality comparison types based on the given node IDs.
func (l *LatencyRefresher) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	fromLocality := l.getNodeLocality(l.nodeID)
	maxLatencies := [numLocalityComparisonTypes]time.Duration{}

	for _, nodeID := range nodeIDs {
		// Skip self comparison
		if nodeID == l.nodeID {
			continue
		}

		toLocality := l.getNodeLocality(nodeID)
		comparisonResult, _, _ := fromLocality.CompareWithLocality(toLocality)
		if latency, ok := l.getLatency(nodeID); ok {
			maxLatencies[comparisonResult] = max(maxLatencies[comparisonResult], latency)
		}
	}

	for i := 0; i < numLocalityComparisonTypes; i++ {
		if maxLatencies[i] == 0 {
			maxLatencies[i] = defaultMaxNetworkRoundTrip
		}
		l.UpdateLatencyForLocalityProximity(roachpb.LocalityComparisonType(i), maxLatencies[i])
	}
}
