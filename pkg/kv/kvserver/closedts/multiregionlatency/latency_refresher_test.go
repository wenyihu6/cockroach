// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionlatency

// func TestLatencyRefresher(t *testing.T) {
// 	defer leaktest.AfterTest(t)()

// 	baseLocality := roachpb.Locality{
// 		Tiers: []roachpb.Tier{
// 			{Key: "region", Value: "us-east"},
// 			{Key: "zone", Value: "us-east-1a"},
// 		},
// 	}

// 	// Mock node descriptors for different localities
// 	nodeDescs := map[roachpb.NodeID]*roachpb.NodeDescriptor{
// 		1: { // Same zone.
// 			NodeID: 1,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "us-east"},
// 					{Key: "zone", Value: "us-east-1a"},
// 				},
// 			},
// 		},
// 		2: { // Same region, cross zone.
// 			NodeID: 2,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "us-east"},
// 					{Key: "zone", Value: "us-east-1b"},
// 				},
// 			},
// 		},
// 		3: { // Cross region.
// 			NodeID: 3,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "us-west"},
// 					{Key: "zone", Value: "us-west-1a"},
// 				},
// 			},
// 		},
// 		4: { // Invalid locality.
// 			NodeID: 4,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{},
// 			},
// 		},
// 		5: { // Another same zone node
// 			NodeID: 5,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "us-east"},
// 					{Key: "zone", Value: "us-east-1a"},
// 				},
// 			},
// 		},
// 		6: { // Another cross zone node
// 			NodeID: 6,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "us-east"},
// 					{Key: "zone", Value: "us-east-1c"},
// 				},
// 			},
// 		},
// 		7: { // Another cross region node
// 			NodeID: 7,
// 			Locality: roachpb.Locality{
// 				Tiers: []roachpb.Tier{
// 					{Key: "region", Value: "eu-west"},
// 					{Key: "zone", Value: "eu-west-1a"},
// 				},
// 			},
// 		},
// 	}

// 	testCases := []struct {
// 		name string
// 		// Input
// 		nodeIDs    []roachpb.NodeID
// 		latencyMap map[roachpb.NodeID]time.Duration
// 		// Expected results for each locality comparison type.
// 		expectedLatencies map[roachpb.LocalityComparisonType]time.Duration
// 	}{
// 		{
// 			name:    "basic test with all locality types",
// 			nodeIDs: []roachpb.NodeID{1, 2, 3, 4},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 1 * time.Millisecond,  // same zone
// 				2: 5 * time.Millisecond,  // cross zone
// 				3: 25 * time.Millisecond, // cross region
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  1 * time.Millisecond,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 5 * time.Millisecond,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           25 * time.Millisecond,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "missing latencies fall back to default",
// 			nodeIDs: []roachpb.NodeID{1, 2, 3},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 1 * time.Millisecond,
// 				// Missing latency for node 2
// 				3: 25 * time.Millisecond,
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  1 * time.Millisecond,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           25 * time.Millisecond,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "latencies below minimum are clamped",
// 			nodeIDs: []roachpb.NodeID{1, 2, 3},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 0 * time.Millisecond,
// 				2: 5 * time.Millisecond,
// 				3: 25 * time.Millisecond,
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  minAcceptableNetworkRTT,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 5 * time.Millisecond,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           25 * time.Millisecond,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:       "empty node list maintains defaults",
// 			nodeIDs:    []roachpb.NodeID{},
// 			latencyMap: map[roachpb.NodeID]time.Duration{},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "takes maximum latency within each group",
// 			nodeIDs: []roachpb.NodeID{1, 2, 3, 5, 6, 7},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 2 * time.Millisecond,  // same zone
// 				5: 5 * time.Millisecond,  // same zone (higher)
// 				2: 10 * time.Millisecond, // cross zone
// 				6: 15 * time.Millisecond, // cross zone (higher)
// 				3: 50 * time.Millisecond, // cross region
// 				7: 80 * time.Millisecond, // cross region (higher)
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  5 * time.Millisecond,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 15 * time.Millisecond,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           80 * time.Millisecond,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "mixed valid and invalid latencies",
// 			nodeIDs: []roachpb.NodeID{1, 2, 3, 5, 6, 7},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 2 * time.Millisecond,  // valid
// 				5: 0,                     // invalid - too low
// 				2: 10 * time.Millisecond, // valid
// 				6: maxAcceptableNetworkRTT + time.Second, // invalid - too high
// 				3: -1 * time.Millisecond, // invalid - negative
// 				7: 80 * time.Millisecond, // valid
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  2 * time.Millisecond,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: 10 * time.Millisecond,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           80 * time.Millisecond,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "all nodes in same locality",
// 			nodeIDs: []roachpb.NodeID{1, 5},
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				1: 2 * time.Millisecond,
// 				5: 5 * time.Millisecond,
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  5 * time.Millisecond,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 		{
// 			name:    "invalid node IDs",
// 			nodeIDs: []roachpb.NodeID{99, 100}, // non-existent nodes
// 			latencyMap: map[roachpb.NodeID]time.Duration{
// 				99:  5 * time.Millisecond,
// 				100: 10 * time.Millisecond,
// 			},
// 			expectedLatencies: map[roachpb.LocalityComparisonType]time.Duration{
// 				roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:  defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE: defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_CROSS_REGION:           defaultMaxNetworkRTT,
// 				roachpb.LocalityComparisonType_UNDEFINED:              defaultMaxNetworkRTT,
// 			},
// 		},
// 	}

// 	for _, tc := range testCases {
// 		t.Run(tc.name, func(t *testing.T) {
// 			getNodeDesc := func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
// 				if desc, ok := nodeDescs[nodeID]; ok {
// 					return desc, nil
// 				}
// 				return nil, errors.New("node not found")
// 			}

// 			getLatency := func(nodeID roachpb.NodeID) (time.Duration, bool) {
// 				if latency, ok := tc.latencyMap[nodeID]; ok {
// 					return latency, true
// 				}
// 				return 0, false
// 			}

// 			lr := NewLatencyRefresher(baseLocality, getLatency, getNodeDesc)
// 			lr.RefreshLatency(tc.nodeIDs)

// 			for lct, expectedLatency := range tc.expectedLatencies {
// 				actualLatency := lr.GetLatencyByLocalityProximity(lct)
// 				require.Equal(t, expectedLatency, actualLatency,
// 					"incorrect latency for locality comparison type %v", lct)
// 			}
// 		})
// 	}
// }
