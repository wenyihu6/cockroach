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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// this information stays the same for all constraints satisfy check
func processClusterInfo(
	regions []state.Region,
) (map[string]int, map[string]int, map[string]string) {
	zone := map[string]int{}
	region := map[string]int{}
	zoneToRegion := map[string]string{}

	for _, r := range regions {
		for _, z := range r.Zones {
			zoneToRegion[z.Name] = r.Name
			zone[z.Name] += z.NodeCount
			region[r.Name] += z.NodeCount
		}
	}
	return zone, region, zoneToRegion
}

type allocationDetailsAtEachLevel struct {
	unassigned        int
	assignedVoters    int
	assignedNonVoters int
}

// nonvoter restriction but no voter restriction -> request from remaining non voter is 0
func (a *allocationDetailsAtEachLevel) tryToAddVoters(numOfVoters int) (success bool) {
	if a.unassigned < numOfVoters {
		return false
	}
	a.unassigned -= numOfVoters
	a.assignedVoters += numOfVoters
	return true
}

func (a *allocationDetailsAtEachLevel) tryToAddNonVoters(numOfNonVoters int) (success bool) {
	if a.unassigned < numOfNonVoters {
		return false
	}
	a.unassigned -= numOfNonVoters
	a.assignedNonVoters += numOfNonVoters
	return true
}

// validate if the config is satisfiable only
type mockAllocator struct {
	zone              map[string]allocationDetailsAtEachLevel
	region            map[string]allocationDetailsAtEachLevel
	zoneToRegion      map[string]string
	remainingVoters   int
	remainingReplicas int
}

// newMockAllocator creates a mock allocator based on the provided cluster
// setup, designed to determine if a config can be satisfied using
// isSatisfiable(roachpb.SpanConfig). Note that since isSatisfiable directly
// alters mockAlloactor fields, a new mock allocator should be initialized for
// each isSatisfiable call.
func newMockAllocator(
	zone map[string]int, region map[string]int, zoneToRegion map[string]string,
) mockAllocator {
	m := mockAllocator{
		zone:         map[string]allocationDetailsAtEachLevel{},
		region:       map[string]allocationDetailsAtEachLevel{},
		zoneToRegion: zoneToRegion,
	}
	// validator needs to handle request, act as an allocator to see if something is satisfiable
	// make a deep copy of everything
	for k, v := range zone {
		m.zone[k] = allocationDetailsAtEachLevel{
			unassigned: v,
		}
	}

	for k, v := range region {
		m.region[k] = allocationDetailsAtEachLevel{
			unassigned: v,
		}
	}
	return m
}

type constraint struct {
	requiredReplicas int
	requiredVoters   int
}

func (m *mockAllocator) validateConstraint(c roachpb.Constraint) error {
	if c.Type == roachpb.Constraint_PROHIBITED {
		return errors.New("BETTER MSG")
	}
	switch c.Key {
	case "zone":
		_, ok := m.zone[c.Value]
		if !ok {
			return errors.New("BETTER MSG")
		}
	case "region":
		_, ok := m.region[c.Value]
		if !ok {
			return errors.New("BETTER MSG")
		}
	default:
		return errors.New("BETTER MSG")
	}
	return nil
}

func (m *mockAllocator) processConstraints(
	config roachpb.SpanConfig,
) (zoneConstraints map[string]constraint, regionConstraints map[string]constraint, _ error) {
	zoneConstraints = map[string]constraint{}
	regionConstraints = map[string]constraint{}
	totalVoters := int(config.GetNumVoters())
	for _, voterConstraint := range config.VoterConstraints {
		requiredVoters := int(voterConstraint.NumReplicas)
		if voterConstraint.NumReplicas == 0 {
			requiredVoters = totalVoters
		}
		for _, vc := range voterConstraint.Constraints {
			if err := m.validateConstraint(vc); err != nil {
				return map[string]constraint{}, map[string]constraint{}, err
			}
			if vc.Key == "zone" {
				zc := zoneConstraints[vc.Value]
				zc.requiredVoters = requiredVoters
				zoneConstraints[vc.Value] = zc
			} else if vc.Key == "region" {
				rc := regionConstraints[vc.Value]
				rc.requiredVoters = requiredVoters
				regionConstraints[vc.Value] = rc
			}
		}
	}

	totalReplicas := int(config.NumReplicas)
	for _, replicaConstraint := range config.Constraints {
		requiredReplicas := int(replicaConstraint.NumReplicas)
		if replicaConstraint.NumReplicas == 0 {
			requiredReplicas = totalReplicas
		}
		for _, vc := range replicaConstraint.Constraints {
			if err := m.validateConstraint(vc); err != nil {
				return map[string]constraint{}, map[string]constraint{}, err
			}
			if vc.Key == "zone" {
				// update zone constraint
				zc := zoneConstraints[vc.Value]
				zc.requiredReplicas = requiredReplicas
				zoneConstraints[vc.Value] = zc
			} else if vc.Key == "region" {
				rc := regionConstraints[vc.Value]
				rc.requiredReplicas = requiredReplicas
				regionConstraints[vc.Value] = rc
			}
		}
	}
	return zoneConstraints, regionConstraints, nil
}

func computeNonVotersToAdd(existingNonVoters int, requiredVoters int, requiredReplicas int) int {
	return int(math.Max(0, float64(requiredReplicas-requiredVoters-existingNonVoters)))
}
func computeVotersToAdd(existingVoters int, requiredVoters int) int {
	return int(math.Max(0, float64(requiredVoters-existingVoters)))
}

func (m *mockAllocator) applyAtRegionLevel(
	regionName string, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := m.region[regionName]
	if !ok {
		panic("BETTER MSG")
	}

	if existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd) {
		m.region[regionName] = existing
		return true
	} else {
		return false
	}
}

func (m *mockAllocator) applyAtClusterLevel(votersToAdd int, nonVotersToAdd int) bool {
	if m.remainingVoters < votersToAdd {
		return false
	}
	if m.remainingReplicas < nonVotersToAdd {
		return false
	}
	m.remainingVoters -= votersToAdd
	m.remainingReplicas -= votersToAdd + nonVotersToAdd
	return true
}

func (m *mockAllocator) applyAtZoneLevel(
	zoneName string, votersToAdd int, nonVotersToAdd int,
) bool {
	existing, ok := m.zone[zoneName]
	if !ok {
		panic("BETTER MSG")
	}
	if existing.tryToAddVoters(votersToAdd) && existing.tryToAddNonVoters(nonVotersToAdd) {
		m.zone[zoneName] = existing
		return true
	} else {
		return false
	}
}

func (m *mockAllocator) tryToSatisfyRegionConstraint(
	regionName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := m.region[regionName]
	if !ok {
		panic("BETTER MSG")
	}
	votersToAdd := computeVotersToAdd(existing.assignedVoters, requiredVoters)
	nonVotersToAdd := computeNonVotersToAdd(existing.assignedNonVoters, requiredVoters, requiredReplicas)
	fmt.Println(nonVotersToAdd)
	if votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// propagate the changes to the cluster and see if the changes can be made to all cluster successfully
	return m.applyAtRegionLevel(regionName, votersToAdd, nonVotersToAdd) &&
		m.applyAtClusterLevel(votersToAdd, nonVotersToAdd)
}

func (m *mockAllocator) tryToSatisfyZoneConstraint(
	zoneName string, requiredVoters int, requiredReplicas int,
) bool {
	existing, ok := m.zone[zoneName]
	if !ok {
		panic("BETTER MSG")
	}
	votersToAdd := computeVotersToAdd(existing.assignedVoters, requiredVoters)
	nonVotersToAdd := computeNonVotersToAdd(existing.assignedNonVoters, requiredVoters, requiredReplicas)
	if votersToAdd == 0 && nonVotersToAdd == 0 {
		return true
	}
	// propagate the changes to the cluster and see if the changes can be made to all cluster successfully
	return m.applyAtZoneLevel(zoneName, votersToAdd, nonVotersToAdd) &&
		m.applyAtRegionLevel(m.zoneToRegion[zoneName], votersToAdd, nonVotersToAdd) &&
		m.applyAtClusterLevel(votersToAdd, nonVotersToAdd)
}

// isSatisfiable only supports configurations with zone and region constraints
// only.
// mockAllocator, initialized with the specific cluster setup. This is how
// isSatisfiable checks for satisfiability:
// - It allocates voters or replicas to a specific zone or region only when we
// know that it is necessary and would fail otherwise.
// - It starts by processing zone and region constraints. Each zone and region correspond to a specific constraint with replica and voter constraint.
// - We then process zone constraints, then region constraints. While satisfying some zone constraint, we know that some of the region constraint would be
// satisfied as well.
// - For zone constraint, we compute voters and nonvoters to ba added

// given the region and zone constriant, we can give arise the same constraints
func (m *mockAllocator) isSatisfiable(config roachpb.SpanConfig) (success bool, reason error) {
	m.remainingVoters = int(config.GetNumVoters())
	m.remainingReplicas = int(config.NumReplicas)
	zoneConstraints, regionConstraints, err := m.processConstraints(config)
	if err != nil {
		return false, err
	}

	for zoneName, zc := range zoneConstraints {
		if ok := m.tryToSatisfyZoneConstraint(zoneName, zc.requiredVoters, zc.requiredReplicas); !ok {
			return false, errors.Newf("failed to satisfy constraints for zone %s", zoneName)
		}
	}

	for regionName, rc := range regionConstraints {
		if ok := m.tryToSatisfyRegionConstraint(regionName, rc.requiredVoters, rc.requiredReplicas); !ok {
			return false, errors.Newf("failed to satisfy constraints for region %s", regionName)
		}
	}

	return true, nil
}
