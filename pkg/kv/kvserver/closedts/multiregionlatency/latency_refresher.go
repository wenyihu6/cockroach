// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionlatency

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// LatencyRefresher tracks and updates network latency between nodes based on
// their locality. It is used by side transport and raft to estimate the amount
// of time it takes for closed timestamp updates to propagate and determine lead
// time for global reads.
type LatencyRefresher struct {
	nodeLocality roachpb.Locality
	getNodeDesc  func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	getLatency   func(roachpb.NodeID) (time.Duration, bool)
	rtt          [roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE]atomic.Int64
}

// NewLatencyRefresher creates a new LatencyRefresher instance.
func NewLatencyRefresher(
	nodeLocality roachpb.Locality,
	getLatency func(roachpb.NodeID) (time.Duration, bool),
	getNodeDesc func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error),
) *LatencyRefresher {
	if getLatency == nil || getNodeDesc == nil {
		log.Errorf(context.Background(), "nodeLocality, getLatency and getNodeDesc must be provided")
	}

	l := &LatencyRefresher{
		nodeLocality: nodeLocality,
		getLatency:   getLatency,
		getNodeDesc:  getNodeDesc,
	}

	// Initialize all locality comparison types with default latency.
	// LocalityComparisonType_UNDEFINED should be left with default latency.
	for i := 0; i < len(l.rtt); i++ {
		l.rtt[roachpb.LocalityComparisonType(i)].Store(int64(closedts.DefaultMaxNetworkRTT))
	}
	return l
}

// GetLatencyByLocalityProximity returns the latency for the given locality comparison type.
func (l *LatencyRefresher) GetLatencyByLocalityProximity(lct roachpb.LocalityComparisonType) time.Duration {
	return time.Duration(l.rtt[lct].Load())
}

// updateLatencyForLocalityProximity updates the latency for the given locality comparison type.
func (l *LatencyRefresher) updateLatencyForLocalityProximity(lct roachpb.LocalityComparisonType, updatedLatency time.Duration) {
	if lct == roachpb.LocalityComparisonType_UNDEFINED {
		return
	}
	l.rtt[lct].Store(int64(updatedLatency))
}

// RefreshLatency updates latencies for all locality comparison types based on
// latency info gathered for the given node IDs.
func (l *LatencyRefresher) RefreshLatency(nodeIDs roachpb.NodeIDSlice) {
	maxLatencies := map[roachpb.LocalityComparisonType]time.Duration{}
	for _, nodeID := range nodeIDs {
		// Note that it is possible that the nodeID is the same as the nodeID of the
		// current node. It is fine since we are taking the max latency.
		toNodeDesc, err := l.getNodeDesc(nodeID)
		if err != nil {
			continue
		}
		comparisonResult, _, _ := l.nodeLocality.CompareWithLocality(toNodeDesc.Locality)
		if latency, ok := l.getLatency(nodeID); ok {
			maxLatencies[comparisonResult] = max(maxLatencies[comparisonResult], latency)
		}
	}
	for i := roachpb.LocalityComparisonType_CROSS_REGION; i < roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE; i++ {
		// Skip if the latency is not updated.
		if maxLatency, ok := maxLatencies[i]; ok {
			l.updateLatencyForLocalityProximity(i, maxLatency)
		}
	}
}
