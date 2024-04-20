package tests

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
)

var InterestingCases = []struct {
	description         string
	constraint          string
	expectedSuccess     bool
	expectedErrorMsgStr string
}{
	{
		description:         "straightforward valid configuration",
		constraint:          "num_replicas=2 num_voters=1",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "straightforward valid configuration",
		constraint: "num_replicas=5 num_voters=5 " +
			"constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1} " +
			"voter_constraints={'+region=US_East':3,'+region=US_West':1,'+region=EU':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promotion to satisfy region voter constraint",
		constraint: "num_replicas=2 num_voters=2 " +
			"constraints={'+zone=US_West_1':2} voter_constraints={'+region=US_West':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "promotion to satisfy cluster constraint",
		constraint:          "num_replicas=2 num_voters=2 constraints={'+zone=US_West_1':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promoting some nonvoters to voters",
		constraint: "num_replicas=6 num_voters=3 constraints={'+zone=US_East_3':3} " +
			"voter_constraints={'+region=US_East':3,'+zone=US_East_2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "promoting some nonvoters + add voters + add nonvoters",
		constraint: "num_replicas=15 num_voters=6 " +
			"constraints={'+zone=US_East_4':10,'+region=EU':3,'+region=US_East':11} " +
			"voter_constraints={'+region=US_East':3,'+zone=US_East_3':1,'+zone=US_West_1':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "satisfying zone constraint can help satisfy region constraint",
		constraint:          "num_replicas=2 constraints={'+zone=US_West_1':2,'+region=US_West':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "cluster is fully assigned by region constraints",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "cluster is fully assigned by region and zone constraints",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
			"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2," +
			"'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4} " +
			"voter_constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10," +
			"'+zone=US_East_1':1,'+zone=US_East_2':2,'+zone=US_East_3':3,'+zone=US_East_4':10,'+zone=US_West_1':2," +
			"'+zone=EU_1':3,'+zone=EU_2':3,'+zone=EU_3':4}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "having unconstrained replicas + unconstrained voters",
		constraint: "num_replicas=28 num_voters=25 " +
			"constraints={'+region=US_East':2} voter_constraints={'+region=US_East':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "having unconstrained replicas + fully constrained voters",
		constraint:          "num_replicas=27 num_voters=16 voter_constraints={'+region=US_East':16}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "having fully constrained replicas + unconstrained voters",
		constraint: "num_replicas=16 num_voters=3 " +
			"constraints={'+region=US_East':16,'+zone=US_East_1':1,'+zone=US_East_2':2} " +
			"voter_constraints={'+zone=US_East_4':3}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "can promote any replicas to voters at cluster level",
		constraint: "num_replicas=28 num_voters=3 " +
			"constraints={'+region=US_East':16,'+region=US_West':2,'+region=EU':10} " +
			"voter_constraints={'+region=EU':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #106559",
		constraint: "num_replicas=6 num_voters=5 " +
			"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':2} " +
			"voter_constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':2,'+zone=US_East_3':1}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #106559",
		constraint: "num_replicas=6 num_voters=5 " +
			"constraints={'+zone=US_West_1':1,'+zone=EU_1':1,'+zone=US_East_2':1,'+zone=US_East_3':1} " +
			"voter_constraints={'+zone=US_West_1':2,'+zone=US_East_2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "configuration for issue #122292",
		constraint: "num_replicas=4 num_voters=3 " +
			"constraints={'+region=US_East':1,'+zone=US_East_1':1,'+zone=US_East_2':1} " +
			"voter_constraints={'+zone=US_East_2':2}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description:         "no voters or replicas needed to add for constraints",
		constraint:          "num_replicas=0 constraints={'+zone=US_East_1':0}",
		expectedSuccess:     true,
		expectedErrorMsgStr: "",
	},
	{
		description: "insufficient replicas for region constraint",
		constraint: "num_replicas=28 num_voters=28 " +
			"constraints={'+region=US_East':17,'+region=US_West':2,'+region=EU':10}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for region US_East",
	},
	{
		description: "insufficient replicas for cluster constraints",
		constraint: "num_replicas=16 num_voters=3 " +
			"constraints={'+region=US_East':16} voter_constraints={'+region=EU':2}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "more voters than replicas",
		constraint:          "num_replicas=1 num_voters=2",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "too many replicas for cluster constraint",
		constraint:          "num_replicas=6 num_voters=2 constraints={'+region=US_East':16}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description:         "too many voters for cluster constraint",
		constraint:          "num_replicas=20 num_voters=2 voter_constraints={'+region=US_East':16}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for cluster",
	},
	{
		description: "zero NumReplicas should use total num_replicas, num_voters for constraints",
		constraint: "num_replicas=5 num_voters=3 " +
			"constraints={'+region=US_East'} voter_constraints={'+region=US_West'}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "failed to satisfy constraints for region US_West",
	},
	{
		description:         "unsupported constraint key",
		constraint:          "num_replicas=5 constraints={'+az=US_East'}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "only zone and region constraint keys are supported",
	},
	{
		description:         "unsupported constraint value",
		constraint:          "num_replicas=5 num_voters=1 voter_constraints={'+region=CA':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "region constraint value CA is not found in the cluster set up",
	},
	{
		description:         "unsupported constraint value",
		constraint:          "num_replicas=5 constraints={'+zone=CA':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "zone constraint value CA is not found in the cluster set up",
	},
	{
		description:         "unsupported constraint type",
		constraint:          "num_replicas=5 constraints={'-region=US_West':1}",
		expectedSuccess:     false,
		expectedErrorMsgStr: "constraints marked as Constraint_PROHIBITED are unsupported",
	},
}

func GetInterestingSpanConfigs() []zonepb.ZoneConfig {
	spanConfigs := make([]zonepb.ZoneConfig, 0)
	for _, c := range InterestingCases {
		spanConfigs = append(spanConfigs, spanconfigtestutils.ParseZoneConfig(&testing.T{}, c.constraint))
	}
	return spanConfigs
}
