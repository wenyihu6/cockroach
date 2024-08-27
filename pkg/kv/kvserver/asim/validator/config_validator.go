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
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// processClusterInfo handles Region data and returns: 1. A map of Zone names to
// their respective Region names 2. A map of Zone names to the number of
// available nodes in the zones 3. A map of Region names to the number of
// available nodes in the regions
func processClusterInfo(
	regions []state.Region,
) (map[string]string, map[string]int, map[string]int, int) {
	zone := map[string]int{}
	region := map[string]int{}
	total := 0
	zoneToRegion := map[string]string{}

	for _, r := range regions {
		for _, z := range r.Zones {
			zoneToRegion[z.Name] = r.Name
			zone[z.Name] += z.NodeCount
			region[r.Name] += z.NodeCount
			total += z.NodeCount
		}
	}
	return zoneToRegion, zone, region, total
}

type AllocationDetailsAtEachLevel struct {
	unassigned        int
	AssignedVoters    int
	AssignedNonVoters int
}

func (a AllocationDetailsAtEachLevel) String() string {
	buf := strings.Builder{}
	if a.AssignedVoters != 0 {
		if a.AssignedVoters > 1 {
			buf.WriteString(fmt.Sprintf("%d voters", a.AssignedVoters))
		} else {
			buf.WriteString(fmt.Sprintf("%d voter", a.AssignedVoters))
		}
	}
	if a.AssignedNonVoters != 0 {
		if buf.Len() != 0 {
			buf.WriteString(",")
		}
		if a.AssignedNonVoters > 1 {
			buf.WriteString(fmt.Sprintf("%d non_voters", a.AssignedNonVoters))
		} else {
			buf.WriteString(fmt.Sprintf("%d non_voter", a.AssignedNonVoters))
		}
	}
	return buf.String()
}

func (ma *MockAllocator) String() string {
	buf := strings.Builder{}
	buf.WriteString("expected configuration:\n")

	helper := func(m map[string]AllocationDetailsAtEachLevel) {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if m[k].AssignedVoters != 0 || m[k].AssignedNonVoters != 0 {
				buf.WriteString(fmt.Sprintf("\t\t%s: %v\n", k, m[k]))
			}
		}
	}

	buf.WriteString("\t1.zone:\n")
	helper(ma.Zone)
	buf.WriteString("\t2.region:\n")
	helper(ma.Region)
	buf.WriteString(fmt.Sprintf("\t3.cluster: %v", ma.Cluster))
	return buf.String()
}

// tryToAddVoters attempts to assign numOfVoters from the available nodes as
// voters. It returns true if there are sufficient available nodes to be
// assigned as voters, and false otherwise.
func (a *AllocationDetailsAtEachLevel) tryToAddVoters(numOfVoters int) (success bool) {
	if a.unassigned < numOfVoters {
		return false
	}
	a.unassigned -= numOfVoters
	a.AssignedVoters += numOfVoters
	return true
}

// tryToAddNonVoters attempts to assign numOfNonVoters from the available nodes
// as nonvoters. It returns true if there are sufficient available nodes to be
// assigned as voters, and false otherwise.
func (a *AllocationDetailsAtEachLevel) tryToAddNonVoters(numOfNonVoters int) (success bool) {
	if a.unassigned < numOfNonVoters {
		return false
	}
	a.unassigned -= numOfNonVoters
	a.AssignedNonVoters += numOfNonVoters
	return true
}

// promoteNonVoters promotes numOfNonVotersToPromote of nonvoters to voters.
func (a *AllocationDetailsAtEachLevel) promoteNonVoters(numOfNonVotersToPromote int) {
	if a.AssignedNonVoters < numOfNonVotersToPromote {
		panic("insufficient non-voters for promotion. This is unexpected as computeNecessaryChanges " +
			"should calculate number of non-voters for promotion correctly.")
	}
	a.AssignedNonVoters -= numOfNonVotersToPromote
	a.AssignedVoters += numOfNonVotersToPromote
}

type MockAllocator struct {
	zoneToRegion map[string]string
	Zone         map[string]AllocationDetailsAtEachLevel
	Region       map[string]AllocationDetailsAtEachLevel
	Cluster      AllocationDetailsAtEachLevel
}

func DeepCopyMap(original interface{}) interface{} {
	switch orig := original.(type) {
	case map[string]string:
		newMap := make(map[string]string)
		for key, value := range orig {
			newMap[key] = value
		}
		return newMap
	case map[string]int:
		newMap := make(map[string]int)
		for key, value := range orig {
			newMap[key] = value
		}
		return newMap
	default:
		panic("unsupported type for DeepCopyMap")
	}
}

// newMockAllocator creates a mock allocator based on the provided Cluster
// setup. MockAllocator is designed to determine if a config can be satisfied by
// trying to assign replicas in a way that meet the constraints. Note that since
// isSatisfiable directly alters mockAlloactor fields, a new mock allocator
// should be initialized for each isSatisfiable call.
func (v Validator) newMockAllocator() MockAllocator {
	zoneToRegion, zone, region, total := DeepCopyMap(v.zoneToRegion).(map[string]string),
		DeepCopyMap(v.zone).(map[string]int), DeepCopyMap(v.region).(map[string]int), v.total
	m := MockAllocator{
		zoneToRegion: zoneToRegion,
		Zone:         map[string]AllocationDetailsAtEachLevel{},
		Region:       map[string]AllocationDetailsAtEachLevel{},
		Cluster: AllocationDetailsAtEachLevel{
			unassigned: total,
		},
	}

	for k, v := range zone {
		m.Zone[k] = AllocationDetailsAtEachLevel{
			unassigned: v,
		}
	}

	for k, v := range region {
		m.Region[k] = AllocationDetailsAtEachLevel{
			unassigned: v,
		}
	}
	return m
}

type constraint struct {
	requiredReplicas int
	requiredVoters   int
}

// validateConstraint returns nil if the constraint is feasible and error
// (not `nil`) otherwise.
func (ma *MockAllocator) validateConstraint(c roachpb.Constraint) string {
	if c.Type == roachpb.Constraint_PROHIBITED {
		return "constraints marked as Constraint_PROHIBITED are unsupported"
	}
	switch c.Key {
	case "zone":
		_, ok := ma.Zone[c.Value]
		if !ok {
			return fmt.Sprintf("zone constraint value %s is not found in the cluster set up", c.Value)
		}
	case "region":
		_, ok := ma.Region[c.Value]
		if !ok {
			return fmt.Sprintf("region constraint value %s is not found in the cluster set up", c.Value)
		}
	default:
		return "only zone and region constraint keys are supported"
	}
	return ""
}

// processConstraintsHelper is a helper function for processConstraint to handle
// the processing logic for both replica and voter constraints. It centralizes
// the validation and updating of the given zoneConstraints and
// regionConstraints. If all constraints are feasible, it returns nil.
// Otherwise, it returns error (not `nil`).
func (ma *MockAllocator) processConstraintsHelper(
	constraintsConjunction []roachpb.ConstraintsConjunction,
	isVoterConstraint bool,
	totalNumOfVotersOrReplicas int,
	zoneConstraints map[string]constraint,
	regionConstraints map[string]constraint,
) string {
	for _, cc := range constraintsConjunction {
		required := int(cc.NumReplicas)
		if cc.NumReplicas == 0 {
			// If NumReplicas is zero, the constraints will be applied to all voters /
			// replicas.
			required = totalNumOfVotersOrReplicas
		}
		for _, c := range cc.Constraints {
			if reason := ma.validateConstraint(c); reason != "" {
				return reason
			}
			if c.Key == "zone" {
				zc := zoneConstraints[c.Value]
				if isVoterConstraint {
					zc.requiredVoters = required
				} else {
					zc.requiredReplicas = required
				}
				zoneConstraints[c.Value] = zc
			} else if c.Key == "region" {
				rc := regionConstraints[c.Value]
				if isVoterConstraint {
					rc.requiredVoters = required
				} else {
					rc.requiredReplicas = required
				}
				regionConstraints[c.Value] = rc
			}
		}
	}
	return ""
}

// processConstraints validates and extracts Region and Zone-specific replica
// and voter constraints, storing them in two separate maps. If certain
// constraints fail the validation, they are considered as infeasible. In such
// cases, error(not `nil`) will be returned.
func (ma *MockAllocator) processConstraints(
	config roachpb.SpanConfig,
) (map[string]constraint, map[string]constraint, string) {
	zoneConstraints := map[string]constraint{}
	regionConstraints := map[string]constraint{}
	totalVoters := int(config.GetNumVoters())
	totalReplicas := int(config.NumReplicas)
	if reason := ma.processConstraintsHelper(
		config.VoterConstraints, true /*isVoterConstraint*/, totalVoters, /*totalNumOfVotersOrReplicas*/
		zoneConstraints, regionConstraints); reason != "" {
		return map[string]constraint{}, map[string]constraint{}, reason
	}
	if reason := ma.processConstraintsHelper(
		config.Constraints, false /*isVoterConstraint*/, totalReplicas, /*totalNumOfVotersOrReplicas*/
		zoneConstraints, regionConstraints); reason != "" {
		return map[string]constraint{}, map[string]constraint{}, reason
	}
	return zoneConstraints, regionConstraints, ""
}

// computeNecessaryChanges computes the necessary minimal changes needed for a
// level to satisfy the constraints, considering the existing number of voters
// and non-voters, as well as the required number of voters and replicas.
func computeNecessaryChanges(
	existingVoters int, existingNonVoters int, requiredVoters int, requiredReplicas int,
) (int, int, int) {
	// Note that having more than required (having unconstrained
	// replicas/voters) is fine and simply means no more additional voters or
	// replicas need to be added.

	// numOfVotersNeeded will be satisfied by promoting non-voters or adding
	// voters. Try to promote as many existing nonvoters to voters as possible
	// first to satisfy voter constraints (so that we require minimal voters or
	// replicas to be added).
	numOfVotersNeeded := int(math.Max(0, float64(requiredVoters-existingVoters)))

	// Step 1: find out number of nonvoters needed to be promoted
	nonVotersToPromote := int(math.Min(float64(existingNonVoters), float64(numOfVotersNeeded)))
	existingVotersAfterPromotion := existingVoters + nonVotersToPromote
	existingNonVotersAfterPromotion := existingNonVoters - nonVotersToPromote

	// Step 2: find out number of voters needed to be added
	votersToAdd := int(math.Max(0, float64(requiredVoters-existingVotersAfterPromotion)))
	existingVotersAfterPromotionAndVoterAddition := existingVotersAfterPromotion + votersToAdd
	existingNonVotersAfterPromotionAndVoterAddition := existingNonVotersAfterPromotion // no changes

	// Step 3: find out number of nonvoters needed to be added
	nonVotersToAdd := int(math.Max(0, float64(requiredReplicas-existingVotersAfterPromotionAndVoterAddition-existingNonVotersAfterPromotionAndVoterAddition)))
	return nonVotersToPromote, votersToAdd, nonVotersToAdd
}

// applyAtRegionLevel attempts to apply the desired changes (nonVotersToPromote,
// votersToAdd, nonVotersToAdd) at the provided Region (specified by
// regionName). If enough nodes are available, it makes the changes and returns
// true. Otherwise, it returns false.
func (ma *MockAllocator) applyAtRegionLevel(
	regionName string, nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := ma.Region[regionName]
	if !ok {
		panic("unknown region name in the region constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}

	existing.promoteNonVoters(nonVotersToPromote)
	success := existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd)
	ma.Region[regionName] = existing
	return success
}

// applyAtClusterLevel attempts to apply the desired changes
// (nonVotersToPromote, votersToAdd, nonVotersToAdd) at the Cluster level. If
// enough nodes are available, it makes the changes and returns true. Otherwise,
// it returns false.
func (ma *MockAllocator) applyAtClusterLevel(
	nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	ma.Cluster.promoteNonVoters(nonVotersToPromote)
	return ma.Cluster.tryToAddVoters(votersToAdd) && ma.Cluster.tryToAddNonVoters(nonVotersToAdd)
}

// applyAtZoneLevel attempts to apply the desired changes (nonVotersToPromote,
// votersToAdd, nonVotersToAdd) at the provided Zone (specified by zoneName). If
// enough nodes are available, it makes the changes and returns true. Otherwise,
// it returns false.
func (ma *MockAllocator) applyAtZoneLevel(
	zoneName string, nonVotersToPromote int, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := ma.Zone[zoneName]
	if !ok {
		panic("unknown zone name in the zone constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	existing.promoteNonVoters(nonVotersToPromote)
	success := existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd)
	ma.Zone[zoneName] = existing
	return success
}

// tryToSatisfyRegionConstraint checks whether the allocator can assign voters
// and replicas in a manner that meets the specified required voters and
// replicas for the Region. If possible, it makes the necessary assignment,
// updates the allocator, and returns true. Otherwise, it returns false.
func (ma *MockAllocator) tryToSatisfyRegionConstraint(
	regionName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := ma.Region[regionName]
	if !ok {
		panic("unknown region name in the region constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.AssignedVoters, existing.AssignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to Region and Cluster.
	return ma.applyAtRegionLevel(regionName, nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		ma.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
}

// tryToSatisfyZoneConstraint checks whether the allocator can assign voters and
// replicas in a manner that meets the specified required voters and replicas
// for the Zone. If possible, it makes the necessary assignment, updates the
// allocator, and returns true. Otherwise, it returns false.
func (ma *MockAllocator) tryToSatisfyZoneConstraint(
	zoneName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := ma.Zone[zoneName]
	if !ok {
		panic("unknown zone name in the zone constraint. " +
			"This is unexpected as validateConstraint should have validated it beforehand.")
	}
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.AssignedVoters, existing.AssignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to Zone, Region and Cluster.
	return ma.applyAtZoneLevel(zoneName, nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		ma.applyAtRegionLevel(ma.zoneToRegion[zoneName], nonVotersToPromote, votersToAdd, nonVotersToAdd) &&
		ma.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
}

// tryToSatisfyClusterConstraint checks whether the allocator can assign voters
// and replicas in a manner that meets the specified required voters and
// replicas for the Cluster. If possible, it makes the necessary assignment,
// updates the allocator, and returns true. Otherwise, it returns false.
func (ma *MockAllocator) tryToSatisfyClusterConstraint(
	requiredVoters int, requiredReplicas int,
) bool {
	existing := ma.Cluster
	nonVotersToPromote, votersToAdd, nonVotersToAdd := computeNecessaryChanges(
		existing.AssignedVoters, existing.AssignedNonVoters, requiredVoters, requiredReplicas)
	if nonVotersToPromote == 0 && votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// Propagate the changes to Cluster.
	success := ma.applyAtClusterLevel(nonVotersToPromote, votersToAdd, nonVotersToAdd)
	if ma.Cluster.AssignedVoters != requiredVoters || ma.Cluster.AssignedNonVoters+ma.Cluster.AssignedVoters != requiredReplicas {
		// Since having unconstrained replicas or voters do not lead to error in
		// earlier process, we check for exact bound Cluster constraint here.
		return false
	}
	return success
}

// isSatisfiable is a method that assesses whether a given configuration is
// satisfiable within the Cluster used to initialize the MockAllocator. It
// returns (true, nil) for satisfiable configurations and (false, reason) for
// unsatisfiable configurations. MockAllocator tries to allocate voters and
// nonvoters across nodes in a manner that satisfies the constraints. If no such
// allocation can be found, the constraint is considered unsatisfiable. The
// allocation is found through the following process:
// 1. Preprocess the config constraints to store replica and voter constraints
// specific to the Zone and Region in two maps.
// 2. Try to satisfy Zone constraints first, Region constraints next, and
// Cluster constraints in the end. As we allocate replicas for Zone constraints,
// some Region constraints are also satisfied.
// 3. While trying to satisfy constraints at each hierarchical level, we
// allocate voters or replicas specific to the Zone or Region only when
// necessary. It first promotes non-voters to voters when possible as voters are
// also replicas and can satisfy both constraints. Additional voters and
// non-voters are then assigned as needed. If any zones or regions lack
// available nodes for assignment, the constraint is considered as
// unsatisfiable.
//
// Limitation:
// - leaseholder preference are not checked and treated as satisfiable. -
// constraints with a key other than Zone and Region are unsatisfiable. -
// constraints with a value that does not correspond to a known Zone or Region
// in the Cluster setup are unsatisfiable.
// - constraints labeled as Constraint_PROHIBITED are considered unsatisfiable.
func (ma *MockAllocator) isSatisfiable(config roachpb.SpanConfig) (success bool, reason string) {
	zoneConstraints, regionConstraints, reason := ma.processConstraints(config)
	if reason != "" {
		return false, reason
	}

	for zoneName, zc := range zoneConstraints {
		if !ma.tryToSatisfyZoneConstraint(zoneName, zc.requiredVoters, zc.requiredReplicas) {
			return false, fmt.Sprintf("failed to satisfy constraints for zone %s", zoneName)
		}
	}

	for regionName, rc := range regionConstraints {
		if !ma.tryToSatisfyRegionConstraint(regionName, rc.requiredVoters, rc.requiredReplicas) {
			return false, fmt.Sprintf("failed to satisfy constraints for region %s", regionName)
		}
	}

	if !ma.tryToSatisfyClusterConstraint(int(config.GetNumVoters()), int(config.NumReplicas)) {
		return false, "failed to satisfy constraints for Cluster"
	}
	return true, ""
}
