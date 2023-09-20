// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/stretchr/testify/require"
)

func TestValidator(t *testing.T) {
	zone, region, zoneToRegion := processClusterInfo(state.ComplexConfig.Regions)
	testCases := []struct {
		constraint          string
		expectedSuccess     bool
		expectedErrorMsgStr string
	}{
		{
			constraint:          "num_replicas=5 num_voters=5 voter_constraints={'+region=US_West':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=5 num_voters=5 " +
				"constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1} " +
				"voter_constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=2 num_voters=2 " +
				"constraints={'+zone=US_West_1':2} voter_constraints={'+region=US_West':2}",
			expectedSuccess: true,
			// replica constraints force replicas in regions
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10}",
			expectedSuccess: true,
			// replica constraints force replicas in regions
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
				"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2,'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4}" +
				"voter_constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
				"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2,'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=6 num_voters=5 " +
				"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':2} " +
				"voter_constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':1}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=6 num_voters=5 " +
				"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':1,'+zone=US_East_3':1} " +
				"voter_constraints={'+zone=US_West_1':2,'+zone=US_East_2':2}",
			expectedSuccess:     true,
			expectedErrorMsgStr: "",
		},
		{
			constraint: "num_replicas=28 num_voters=28 " +
				"constraints={'+region=US_East':17,'+region=US_West':2,'+region=EU':10}",
			expectedSuccess: false,
			// constraints require more replicas than provided.
			expectedErrorMsgStr: "failed to satisfy constraints for region US_East",
		},
		{
			constraint: "num_replicas=5 num_voters=3 " +
				"constraints={'+region=US_East':5} voter_constraints={'+region=US_West':3}",
			expectedSuccess: false,
			// replica constraints force replicas in regions
			expectedErrorMsgStr: "failed to satisfy constraints for region US_West",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.constraint, func(t *testing.T) {
			ma := newMockAllocator(zone, region, zoneToRegion)
			config := spanconfigtestutils.ParseZoneConfig(t, tc.constraint).AsSpanConfig()
			success, actualError := ma.isSatisfiable(config)
			require.Equal(t, tc.expectedSuccess, success)
			if tc.expectedErrorMsgStr == "" {
				require.Nil(t, actualError)
			} else {
				require.EqualError(t, actualError, tc.expectedErrorMsgStr)
			}
		})
	}
}
