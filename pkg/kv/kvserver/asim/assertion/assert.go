// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package assertion

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/validator"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/history"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/montanaflynn/stats"
)

type ThresholdType int

const (
	ExactBound ThresholdType = iota
	UpperBound
	LowerBound
)

// String returns the string representation of ThresholdType.
func (tht ThresholdType) String() string {
	switch tht {
	case ExactBound:
		return "="
	case UpperBound:
		return "<"
	case LowerBound:
		return ">"
	default:
		panic("unknown threshold type")
	}
}

// Threshold is created by parsing CmdArgs array and is used for assertion to
// validate user-defined Threshold constraints.
type Threshold struct {
	// Value indicates the predefined Threshold value specified by arguments.
	Value float64
	// ThresholdType indicates the predefined Threshold bound type specified by
	// arguments.
	ThresholdType ThresholdType
}

// String returns the string representation of Threshold.
func (th Threshold) String() string {
	return fmt.Sprintf("(%v%.2f)", th.ThresholdType, th.Value)
}

// isViolated returns true if the Threshold constraint is violated and false
// otherwise. Note that if the provided actual value is NaN, the function
// returns false.
func (th Threshold) isViolated(actual float64) bool {
	switch th.ThresholdType {
	case UpperBound:
		return actual > th.Value
	case LowerBound:
		return actual < th.Value
	case ExactBound:
		return actual != th.Value
	default:
		panic("unknown threshold type")
	}
}

// SimulationAssertion provides methods to assert on properties of a cluster
// simulation over time.
type SimulationAssertion interface {
	// Assert looks at a simulation run history and returns true if the
	// assertion holds and false if not. When the assertion does not hold, the
	// reason is also returned.
	Assert(context.Context, history.History) (holds bool, reason string)
	// String returns the string representation of the assertion.
	String() string
}

// SteadyStateAssertion implements the SimulationAssertion interface. The
// SteadyStateAssertion declares an assertion. A common use case is to specify
// an upper_bound for the type=steady threshold. With this configuration, the
// given Stat for each store must be no greater than Threshold % of the mean
// over the assertion Ticks. This assertion is useful for when a cluster should
// stop activity and converge after a period of initial activity. A natural
// example is asserting that rebalancing activity reaches a steady state, so
// there is not thrashing.
type SteadyStateAssertion struct {
	Ticks     int
	Stat      string
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the declared
// Stat's minimum/mean and maximum/mean meets the Threshold constraint at each
// assertion tick. If violated, holds is returned as false along with the
// reason.
func (sa SteadyStateAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if sa.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, sa.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.Stat]

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for i, storeStats := range statTs {
		trimmedStoreStats := storeStats[ticks-sa.Ticks-1:]
		mean, _ := stats.Mean(trimmedStoreStats)
		max, _ := stats.Max(trimmedStoreStats)
		min, _ := stats.Min(trimmedStoreStats)

		maxMean := math.Abs(max/mean - 1)
		minMean := math.Abs(min/mean - 1)

		if sa.Threshold.isViolated(maxMean) || sa.Threshold.isViolated(minMean) {
			if holds {
				fmt.Fprintf(&buf, "  %s\n", sa)
				holds = false
			}
			fmt.Fprintf(&buf,
				"\tstore=%d min/mean=%.2f max/mean=%.2f\n",
				i+1, minMean, maxMean)
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (sa SteadyStateAssertion) String() string {
	return fmt.Sprintf("steady state stat=%s threshold=%v ticks=%d",
		sa.Stat, sa.Threshold, sa.Ticks)
}

// BalanceAssertion implements the SimulationAssertion interface. The
// BalanceAssertion declares an assertion. A common use case is to specify an
// upper_bound for the type=balance threshold. With this configuration, the
// given Stat across all Stores must be no greater than the Threshold for all
// assertion Ticks. This assertion is useful when a Stat is being controlled,
// such as QPS and a correct rebalancing algorithm should balance the stat.
//
// TODO(kvoli): Rationalize this assertion for multi-locality clusters with
// zone configurations. This balance assertion uses the mean and maximum across
// all stores in the cluster. In multi-locality clusters, it is possible for
// balance to be a property that only holds within regions or not at all with
// targeted zone configs. e.g.
//
//	zone config (all ranges)
//	  num_replicas      = 3
//	  constraints       = [{+zone=a1: 1}, {+zone=a2: 1},
//	                       {+zone=a3: 1}]
//	  lease_preferences = [[+zone=a3]]
//
//	localities
//	  s1-s2 zone = a1
//	  s3-s4 zone = a2
//	  s5-s6 zone = a3
//
// Then in this configuration, the lease for each range should be on either
// s5 or s6 and there should be a replica in each zone. Asserting on the
// balance of the cluster doesn't make sense logically, the configuration
// requires leaseholders are on s5,s6 so naturally they should have greater
// load.
type BalanceAssertion struct {
	Ticks     int
	Stat      string
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the declared
// Stat's maximum/mean (over all stores) in the cluster meets the Threshold
// constraint at each assertion tick. If violated, holds is returned as false
// along with the reason.
func (ba BalanceAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if ba.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, ba.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := metrics.Transpose(ts[ba.Stat])

	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	// Check that the assertion holds for the last ba.Ticks; from the most
	// recent tick to recent tick - ba.Ticks.
	for tick := 0; tick < ba.Ticks && tick < ticks; tick++ {
		tickStats := statTs[ticks-tick-1]
		mean, _ := stats.Mean(tickStats)
		max, _ := stats.Max(tickStats)
		maxMeanRatio := max / mean

		log.VInfof(ctx, 2,
			"Balance assertion: stat=%s, max/mean=%.2f, threshold=%+v raw=%v",
			ba.Stat, maxMeanRatio, ba.Threshold, tickStats)
		if ba.Threshold.isViolated(maxMeanRatio) {
			if holds {
				fmt.Fprintf(&buf, "  %s\n", ba)
				holds = false
			}
			fmt.Fprintf(&buf, "\tmax/mean=%.2f tick=%d\n", maxMeanRatio, tick)
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (ba BalanceAssertion) String() string {
	return fmt.Sprintf(
		"balance stat=%s threshold=%v ticks=%d",
		ba.Stat, ba.Threshold, ba.Ticks)
}

// StoreStatAssertion implements the SimulationAssertion interface. The
// StoreStatAssertion declares an assertion. A common use case is to specify an
// exact_bound for the type=stat threshold. With this configuration, the given
// Stat for each store in stores must be == Threshold over the assertion Ticks.
type StoreStatAssertion struct {
	Ticks     int
	Stat      string
	Stores    []int
	Threshold Threshold
}

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (sa StoreStatAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	m := h.Recorded
	ticks := len(m)
	if sa.Ticks > ticks {
		log.VInfof(ctx, 2,
			"The history to run assertions against (%d) is shorter than "+
				"the assertion duration (%d)", ticks, sa.Ticks)
		return true, ""
	}

	ts := metrics.MakeTS(m)
	statTs := ts[sa.Stat]
	holds = true
	// Set holds to be true initially, holds is set to false if the steady
	// state assertion doesn't hold on any store.
	holds = true
	buf := strings.Builder{}

	for _, store := range sa.Stores {
		trimmedStoreStats := statTs[store-1][ticks-sa.Ticks-1:]
		for _, stat := range trimmedStoreStats {
			if sa.Threshold.isViolated(stat) {
				if holds {
					holds = false
					fmt.Fprintf(&buf, "  %s\n", sa)
				}
				fmt.Fprintf(&buf,
					"\tstore=%d stat=%.2f\n",
					store, stat)
			}
		}
	}
	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (sa StoreStatAssertion) String() string {
	return fmt.Sprintf("stat=%s value=%v ticks=%d",
		sa.Stat, sa.Threshold, sa.Ticks)
}

type ConformanceAssertion struct {
	WithExpensiveValidator    bool
	WithPrettyFormat          bool
	Underreplicated           int
	Overreplicated            int
	ViolatingConstraints      int
	Unavailable               int
	ViolatingLeasePreferences int
	LessPreferredLeases       int
}

// ConformanceAssertionSentinel declares a sentinel value which when any of the
// ConformanceAssertion parameters are set to, we ignore the conformance
// reports value for that type of conformance.
const ConformanceAssertionSentinel = -1

func leasePreferenceReport(
	ctx context.Context, h history.History,
) (violating, lessPreferred []roachpb.ConformanceReportedRange) {
	ranges := h.S.Ranges()
	for _, r := range ranges {
		if lhStore, ok := h.S.LeaseholderStore(r.RangeID()); ok {
			storeDescriptor := lhStore.Descriptor()
			spanConf := r.SpanConfig()
			status := kvserver.CheckStoreAgainstLeasePreferences(
				storeDescriptor.StoreID,
				storeDescriptor.Attrs,
				storeDescriptor.Node.Attrs,
				storeDescriptor.Node.Locality,
				spanConf.LeasePreferences,
			)
			switch status {
			case kvserver.LeasePreferencesOK:
			case kvserver.LeasePreferencesLessPreferred:
				lessPreferred = append(lessPreferred, roachpb.ConformanceReportedRange{
					RangeDescriptor: *r.Descriptor(),
					Config:          *spanConf,
				})
			case kvserver.LeasePreferencesViolating:
				violating = append(violating, roachpb.ConformanceReportedRange{
					RangeDescriptor: *r.Descriptor(),
					Config:          *spanConf,
				})
			default:
				panic("unknown lease preference status type")
			}
		}
	}

	return
}

// Assert looks at a simulation run history and returns true if the
// assertion holds and false if not. When the assertion does not hold, the
// reason is also returned.
func (ca ConformanceAssertion) Assert(
	ctx context.Context, h history.History,
) (holds bool, reason string) {
	replicaReport := h.S.Report()
	leaseViolatingPrefs, leaseLessPrefs := leasePreferenceReport(ctx, h)
	buf := strings.Builder{}
	holds = true

	unavailable, under, over, violatingConstraints := len(replicaReport.Unavailable), len(replicaReport.UnderReplicated), len(replicaReport.OverReplicated), len(replicaReport.ViolatingConstraints)
	violatingLeases, lessPrefLeases := len(leaseViolatingPrefs), len(leaseLessPrefs)

	maybeInitHolds := func() {
		if holds {
			holds = false
			fmt.Fprintf(&buf, "  %s\n", ca)
			fmt.Fprintf(&buf, "  actual unavailable=%d under=%d, over=%d violating=%d lease-violating=%d lease-less-preferred=%d\n",
				unavailable, under, over, violatingConstraints, violatingLeases, lessPrefLeases,
			)
		}
	}

	if ca.Unavailable != ConformanceAssertionSentinel &&
		ca.Unavailable != unavailable {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"unavailable", replicaReport.Unavailable))
	}
	if ca.Underreplicated != ConformanceAssertionSentinel &&
		ca.Underreplicated != under {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"under replicated", replicaReport.UnderReplicated))
	}
	if ca.Overreplicated != ConformanceAssertionSentinel &&
		ca.Overreplicated != over {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"over replicated", replicaReport.OverReplicated))
	}

	// Check for the first constraint violation.
	if ca.ViolatingConstraints != ConformanceAssertionSentinel &&
		ca.ViolatingConstraints != violatingConstraints {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"violating constraints", replicaReport.ViolatingConstraints,
			withValidator(ca.WithExpensiveValidator, h.S), withPrettyFormat(ca.WithPrettyFormat, h.S)))
	}
	if ca.ViolatingLeasePreferences != ConformanceAssertionSentinel &&
		ca.ViolatingLeasePreferences != violatingLeases {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"violating lease preferences", leaseViolatingPrefs))
	}
	if ca.LessPreferredLeases != ConformanceAssertionSentinel &&
		ca.LessPreferredLeases != lessPrefLeases {
		maybeInitHolds()
		buf.WriteString(PrintSpanConfigConformanceList(
			"less preferred preferences", leaseLessPrefs))
	}

	return holds, buf.String()
}

// String returns the string representation of the assertion.
func (ca ConformanceAssertion) String() string {
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "conformance ")
	if ca.Unavailable != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "unavailable=%d ", ca.Unavailable)
	}
	if ca.Underreplicated != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "under=%d ", ca.Underreplicated)
	}
	if ca.Overreplicated != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "over=%d ", ca.Overreplicated)
	}
	if ca.ViolatingConstraints != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "violating=%d ", ca.ViolatingConstraints)
	}
	if ca.ViolatingLeasePreferences != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "lease-violating=%d ", ca.ViolatingLeasePreferences)
	}
	if ca.LessPreferredLeases != ConformanceAssertionSentinel {
		fmt.Fprintf(&buf, "lease-less-preferred=%d ", ca.LessPreferredLeases)
	}
	return buf.String()
}

func formatReplicaType(replicaT roachpb.ReplicaType, replicaNum int) string {
	plural := ""
	if replicaNum > 1 {
		plural = "s"
	}

	var role string
	switch replicaT {
	case roachpb.VOTER_FULL:
		role = "voter"
	default:
		role = strings.ToLower(replicaT.String())
	}
	return fmt.Sprintf("%d%s%s", replicaNum, role, plural)
}

// Summarize replicas info across all nodes. Count number of replica types for
// each locality tier.
func summarizeNodeInfo(nodes map[state.NodeID]roachpb.Locality, replicas []roachpb.ReplicaDescriptor) string {
	if len(replicas) <= 0 {
		return "<no replicas>"
	}

	tiers := make(map[string][]int)

	// O(replicas)*O(tiers)
	for _, rep := range replicas {
		locality, ok := nodes[state.NodeID(rep.NodeID)]
		if !ok {
			panic(fmt.Sprintf("node %d not found", rep.NodeID))
		}
		for _, tier := range locality.Tiers {
			if _, ok := tiers[tier.Value]; !ok {
				tiers[tier.Value] = make([]int, len(roachpb.ReplicaType_name))
			}
			tiers[tier.Value][roachpb.ReplicaType_value[rep.Type.String()]]++
		}
	}

	// O(tiers*log(tiers))
	tiersValues := make([]string, 0, len(tiers))
	for tv := range tiers {
		tiersValues = append(tiersValues, tv)
	}
	sort.Strings(tiersValues)

	var buf strings.Builder
	// O(tiers*len(roachpb.ReplicaType_name))
	for _, tv := range tiersValues {
		v := tiers[tv]
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(fmt.Sprintf("(%s:", tv))
		for repType, repTypeNum := range v {
			if repTypeNum == 0 {
				continue
			}
			if repType != 0 {
				buf.WriteString(" ")
			}
			buf.WriteString(formatReplicaType(roachpb.ReplicaType(repType), repTypeNum))
		}
		buf.WriteString(")")
	}
	return buf.String()
}

func prettyPrintRangeDesc(nodes map[state.NodeID]roachpb.Locality, r roachpb.RangeDescriptor) string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
	buf.WriteString(" [")
	buf.WriteString(summarizeNodeInfo(nodes, r.Replicas().Descriptors()))
	buf.WriteString("]")
	return buf.String()
}

func printRangeDesc(r roachpb.RangeDescriptor) string {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
	buf.WriteString(r.RSpan().String())
	buf.WriteString(" [")
	if allReplicas := r.Replicas().Descriptors(); len(allReplicas) > 0 {
		for i, rep := range allReplicas {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(rep.String())
		}
	} else {
		buf.WriteString("<no replicas>")
	}
	buf.WriteString("]")
	return buf.String()
}

type printConfigs struct {
	withPrettyFormat bool
	withValidator    bool
	withState        state.State
}

type option func(t *printConfigs)

func withValidator(b bool, s state.State) option {
	return func(t *printConfigs) {
		t.withValidator = b
		t.withState = s
	}
}

func withPrettyFormat(b bool, s state.State) option {
	return func(t *printConfigs) {
		t.withPrettyFormat = b
		t.withState = s
	}
}

func printSpanConfigConformanceList(tag string, ranges []roachpb.ConformanceReportedRange) string {
	var buf strings.Builder
	for i, r := range ranges {
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%s:\n", tag))
		}
		buf.WriteString(fmt.Sprintf("  %s applying %s", printRangeDesc(r.RangeDescriptor),
			spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(r.Config)))
		if i != len(ranges)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func prettyPrintSpanConfigConformanceList(tag string, ranges []roachpb.ConformanceReportedRange,
	nodes map[state.NodeID]roachpb.Locality) string {
	var buf strings.Builder
	const rangesCount = 6
	for i, r := range ranges {
		if i == 0 {
			buf.WriteString(fmt.Sprintf("%s:\n", tag))
		}
		if i == rangesCount {
			return buf.String() + fmt.Sprintf("... and %d more", len(ranges)-rangesCount)
		}

		buf.WriteString(fmt.Sprintf("  %s", prettyPrintRangeDesc(nodes, r.RangeDescriptor)))
		if i != len(ranges)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func nodeLocalityMap(s state.State) map[state.NodeID]roachpb.Locality {
	nodeLocality := make(map[state.NodeID]roachpb.Locality, len(s.Nodes()))
	for _, n := range s.Nodes() {
		nodeLocality[n.NodeID()] = n.Descriptor().Locality
	}
	return nodeLocality
}

func PrintSpanConfigConformanceList(tag string, ranges []roachpb.ConformanceReportedRange, opts ...option) string {
	cfg := printConfigs{}
	for _, o := range opts {
		o(&cfg)
	}

	buf := strings.Builder{}

	if cfg.withPrettyFormat {
		m := nodeLocalityMap(cfg.withState)
		// Okay to skip check for state == nil. Programming error otherwise.
		buf.WriteString(prettyPrintSpanConfigConformanceList(tag, ranges, m))
		buf.WriteString("\n")
		if cfg.withValidator {
			buf.WriteString(ValidateResult(m, ranges[0], cfg.withState))
		}
	} else {
		buf.WriteString(printSpanConfigConformanceList(tag, ranges))
		buf.WriteString("\n")
	}

	return buf.String()
}

func ValidateResult(nodes map[state.NodeID]roachpb.Locality, actualRange roachpb.ConformanceReportedRange, s state.State) string {
	buf := strings.Builder{}
	v := validator.NewValidator(s.ClusterInfo().Regions)
	res := v.ValidateConfig(actualRange.Config)
	buf.WriteString(res.String())
	if !res.Satisfiable {
		return buf.String()
	}
	expectedConfig := res.Configurations
	actualReplicas := actualRange.RangeDescriptor.Replicas().Descriptors()
	type role struct {
		voters    int
		nonvoters int
	}

	tiers := make(map[string]role)
	k := make([]string, 0)
	totalNonVoters := 0
	totalVoters := 0
	for _, rep := range actualReplicas {
		locality, ok := nodes[state.NodeID(rep.NodeID)]
		if !ok {
			panic(fmt.Sprintf("node %d not found", rep.NodeID))
		}
		for _, tier := range locality.Tiers {
			if _, ok := tiers[tier.Value]; !ok {
				k = append(k, tier.Value)
			}
			r := tiers[tier.Value]
			if rep.Type == roachpb.VOTER_FULL {
				r.voters += 1
				totalVoters += 1
			} else {
				r.nonvoters += 1
				totalNonVoters += 1
			}
			tiers[tier.Value] = r
		}
	}

	helper := func(expected int, actual int, role string) string {
		if actual < expected {
			return fmt.Sprintf("(%d missing %s)", expected-actual, role)
		} else if actual > expected {
			return fmt.Sprintf("(%d extra %s)", actual-expected, role)
		}
		return ""
	}

	sort.Strings(k)
	buf.WriteString("diff:\n")
	for _, tier := range k {
		localityTierValue := tier
		role := tiers[tier]
		if v, ok := expectedConfig.Zone[localityTierValue]; ok {
			buf.WriteString(fmt.Sprintf("\tzone %s: ", localityTierValue))
			buf.WriteString(helper(v.AssignedVoters, role.voters, "voters"))
			buf.WriteString(helper(v.AssignedNonVoters, role.nonvoters, "non_voters"))
		} else if v, ok := expectedConfig.Region[localityTierValue]; ok {
			buf.WriteString(fmt.Sprintf("\tregion %s: ", localityTierValue))
			buf.WriteString(helper(v.AssignedVoters, role.voters, "voters"))
			buf.WriteString(helper(v.AssignedNonVoters, role.nonvoters, "non_voters"))
		}
		buf.WriteString("\n")
	}
	buf.WriteString("\tcluster: ")
	buf.WriteString(helper(expectedConfig.Cluster.AssignedVoters, totalVoters, "voters"))
	buf.WriteString(helper(expectedConfig.Cluster.AssignedNonVoters, totalNonVoters, "non_voters"))
	buf.WriteString("\n")
	return buf.String()
}
