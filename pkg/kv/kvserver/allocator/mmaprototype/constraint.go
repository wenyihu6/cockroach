// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// This file contains helper classes and functions for the allocator related
// to constraint satisfaction, where "constraints" include both replica counts
// and constraints conjunctions. The primary ones are normalizedSpanConfig and
// rangeAnalyzedConstraints.
//
// Other misc pieces: storeSet represents a set of stores, and is
// used here and will be used elsewhere for set operations.
// localityTierInterner is used for interning the tiers to avoid string
// comparisons, used for diversity computation. It will also be used
// elsewhere.
//
// The goal here is to decompose the allocator functionality into modules: the
// constraints "module" does not know about the full set of stores in a
// cluster, and only has a glimpse into those stores via knowing about the
// current set of replica stores for a range and what constraints they
// satisfy. rangeAnalyzedConstraints has various methods to aid in allocator
// decision-making, and these return two kinds of candidates: candidates
// consisting of existing replicas (which can be explicitly enumerated), and
// candidates representing an unknown set that needs to satisfy some
// constraint expression.

// Subset of roachpb.SpanConfig with some normalization.
type normalizedSpanConfig struct {
	numVoters   int32
	numReplicas int32

	// If the corresponding []roachpb.ConstraintsConjunction in the SpanConfig
	// is nil, the one here is also nil. Note, we allow for either or both
	// constraints and voterConstraints to be nil.
	//
	// If non-nil, the []roachpb.ConstraintsConjunction is normalized:
	// - If SpanConfig has one or more ConstraintsConjunction with
	//   NumReplicas=0, they have all been added to the same
	//   ContraintsConjunction slice (since all need to be satisfied). We know
	//   that SpanConfig must only have ConstraintsConjunction with
	//   NumReplicas=0 in this case. The NumReplicas is set to the required
	//   number. So there is no special case of
	//   ConstraintsConjunction.NumReplicas=0
	//
	// - If SpanConfig has ConstraintsConjunctions with NumReplicas != 0, they
	//   are unchanged, but if the sum does not add up to the required number of
	//   replicas, then another ConstraintsConjunction is added with an empty
	//   constraints slice, and the remaining count. This ConstraintsConjunction
	//   with the empty slice is always the last conjunction in this slice.
	//
	// If both voterConstraints and constraints are non-nil, we require that
	// voterConstraints is stricter than constraints. That is, if we satisfy the
	// voterConstraints, any replicas chosen there will satisfy some constraint
	// in constraints, and if no constraint in voterConstraints is
	// oversatisfied, then no constraint in constraints is oversatisfied (when
	// we don't consider any non-voter replicas).
	//
	// It is also highly-encouraged that when there are multiple
	// ConstraintsConjunctions (except for the one we synthesize above with the
	// empty slice) they are structured such that the same store cannot satisfy
	// multiple conjunctions.
	//
	// TODO(sumeer):
	// - For existing clusters this strictness requirement may not be met, so we
	//   will do a structural-normalization to meet this requirement, and if
	//   this structural-normalization is not possible, we will log an error and
	//   not switch the cluster to the new allocator until the operator fixes
	//   their SpanConfigs and retries.

	// constraints applies to all replicas.
	constraints []internedConstraintsConjunction
	// voterConstraints applies to voter replicas.
	voterConstraints []internedConstraintsConjunction
	// Best-effort. Conjunctions are in order of preference, and it is ok if
	// none are satisfied.
	leasePreferences []internedLeasePreference

	// For pretty-printing.
	interner *stringInterner
}

type internedConstraint struct {
	// type captures the kind of constraint this is: required or prohibited.
	typ roachpb.Constraint_Type
	// key captures the locality tag key we're constraining against.
	key stringCode
	// value is the locality tag value we're constraining against.
	value stringCode
}

func (ic internedConstraint) unintern(interner *stringInterner) roachpb.Constraint {
	return roachpb.Constraint{
		Type:  ic.typ,
		Key:   interner.toString(ic.key),
		Value: interner.toString(ic.value),
	}
}

func (ic internedConstraint) less(b internedConstraint) bool {
	if ic.typ != b.typ {
		return ic.typ < b.typ
	}
	if ic.key != b.key {
		return ic.key < b.key
	}
	if ic.value != b.value {
		return ic.value < b.value
	}
	return false
}

// constraints are in increasing order using internedConstraint.less.
type constraintsConj []internedConstraint

type conjunctionRelationship int

// Relationship between conjunctions used for structural normalization. This
// relationship is solely defined based on the conjunctions, and not based on
// what stores actually match. It simply assumes that if two conjuncts are not
// equal their sets are non-overlapping. This simplifying assumption is made
// since we are only trying to do a best-effort structural normalization.
const (
	conjIntersecting conjunctionRelationship = iota
	conjEqualSet
	conjStrictSubset
	conjStrictSuperset
	conjNonIntersecting
)

func (cc constraintsConj) relationship(b constraintsConj) conjunctionRelationship {
	n := len(cc)
	m := len(b)
	extraInCC := 0
	extraInB := 0
	inBoth := 0
	for i, j := 0, 0; i < n || j < m; {
		if i >= n {
			extraInB++
			j++
			continue
		}
		if j >= m {
			extraInCC++
			i++
			continue
		}
		if cc[i] == b[j] {
			inBoth++
			i++
			j++
			continue
		}
		if cc[i].less(b[j]) {
			// Found a conjunct that is not in b.
			extraInCC++
			i++
			continue
		} else {
			extraInB++
			j++
			continue
		}
	}
	if extraInCC > 0 && extraInB == 0 {
		return conjStrictSubset
	}
	if extraInB > 0 && extraInCC == 0 {
		return conjStrictSuperset
	}
	// (extraInCC == 0 || extraInB > 0) && (extraInB == 0 || extraInCC > 0)
	// =>
	// (extraInCC == 0 && extraInB == 0) || (extraInB > 0 && extraInCC > 0)
	if extraInCC == 0 && extraInB == 0 {
		return conjEqualSet
	}
	// (extraInB > 0 && extraInCC > 0)
	if inBoth > 0 {
		return conjIntersecting
	}
	return conjNonIntersecting
}

type internedConstraintsConjunction struct {
	numReplicas int32
	constraints constraintsConj
}

func (icc internedConstraintsConjunction) unintern(
	interner *stringInterner,
) roachpb.ConstraintsConjunction {
	var cc roachpb.ConstraintsConjunction
	cc.NumReplicas = icc.numReplicas
	for _, c := range icc.constraints {
		cc.Constraints = append(cc.Constraints, c.unintern(interner))
	}
	return cc
}

type internedLeasePreference struct {
	constraints constraintsConj
}

// makeNormalizedSpanConfig is called infrequently, when there is a new
// SpanConfig for which we don't have a normalized value. The rest of the
// allocator code works with normalizedSpanConfig. Due to the infrequent
// nature of this, we don't attempt to reduce memory allocations.
func makeNormalizedSpanConfig(
	conf *roachpb.SpanConfig, interner *stringInterner,
) (*normalizedSpanConfig, error) {
	numVoters := conf.GetNumVoters()
	var normalizedConstraints, normalizedVoterConstraints []internedConstraintsConjunction
	var err error
	if conf.VoterConstraints != nil {
		normalizedVoterConstraints, err = normalizeConstraints(
			conf.VoterConstraints, numVoters, interner)
		if err != nil {
			return nil, err
		}
	}
	if conf.Constraints != nil {
		normalizedConstraints, err = normalizeConstraints(conf.Constraints, conf.NumReplicas, interner)
		if err != nil {
			return nil, err
		}
	} else if (conf.NumReplicas-numVoters > 0) || len(normalizedVoterConstraints) == 0 {
		// - No constraints, but have some non-voters.
		// - No voter constraints either.
		// Need an empty constraints conjunction so that non-voters or voters have
		// some constraint they can satisfy.
		normalizedConstraints = []internedConstraintsConjunction{
			{
				numReplicas: conf.NumReplicas,
				constraints: nil,
			},
		}
	}
	var lps []internedLeasePreference
	for i := range conf.LeasePreferences {
		lps = append(lps, internedLeasePreference{
			constraints: interner.internConstraintsConj(conf.LeasePreferences[i].Constraints)})
	}
	nConf := &normalizedSpanConfig{
		numVoters:        numVoters,
		numReplicas:      conf.NumReplicas,
		constraints:      normalizedConstraints,
		voterConstraints: normalizedVoterConstraints,
		leasePreferences: lps,
		interner:         interner,
	}
	return doStructuralNormalization(nConf)
}

// normalizeConstraints processes a slice of ConstraintsConjunctions for a given
// number of replicas and returns a normalized slice of internedConstraintsConjunction.
// This operation ensures the following:
//   - Combines constraints with NumReplicas == 0 into a single conjunction containing
//     the union of their constraints, which must be satisfied by all replicas.
//   - Ensures that the total number of replicas required by all constraints does not
//     exceed numReplicas.
//   - If constraints under-specify the number of replicas, adds a "catch-all"
//     constraints conjunction that will match all stores for the remaining replicas.
//   - Returns errors for illegal constraint mixes (like both zero and nonzero NumReplicas,
//     or if constraints require more replicas than allowed).
func normalizeConstraints(
	constraints []roachpb.ConstraintsConjunction, numReplicas int32, interner *stringInterner,
) ([]internedConstraintsConjunction, error) {
	var nc []roachpb.ConstraintsConjunction // Will hold normalized constraints
	haveZero := false                       // Tracks if any conjunction has NumReplicas==0
	sumReplicas := int32(0)                 // Total number of replicas required by explicit constraints

	// Iterate over input constraints, separating those with NumReplicas==0 and summing explicit replicas.
	for i := range constraints {
		if constraints[i].NumReplicas == 0 {
			haveZero = true
			if len(nc) == 0 {
				// First time we see NumReplicas==0: create a slot
				nc = append(nc, roachpb.ConstraintsConjunction{})
			}
			// Add these constraints to the single conjunction (all must be satisfied)
			// This effectively merges all zero-replica constraints into one.
			nc[0].Constraints = append(nc[0].Constraints, constraints[i].Constraints...)
		} else {
			sumReplicas += constraints[i].NumReplicas
		}
	}

	// Error if both explicit constraints and zero-replica constraints are specified.
	if haveZero && sumReplicas > 0 {
		return nil, errors.Errorf("invalid mix of constraints")
	}

	// Error if sum of explicit constraints exceeds allowed replicas.
	if sumReplicas > numReplicas {
		return nil, errors.Errorf("constraint replicas add up to more than configured replicas")
	}

	// Compose the normalized list depending on input shape
	if haveZero {
		// For zero-replica constraints, all replicas must satisfy the merged list.
		nc[0].NumReplicas = numReplicas
	} else {
		// Keep all input constraints as-is.
		nc = append(nc, constraints...)
		// If we have additional unspecified replicas, add a catch-all constraint (nil constraints).
		if sumReplicas < numReplicas {
			cc := roachpb.ConstraintsConjunction{
				NumReplicas: numReplicas - sumReplicas,
				Constraints: nil,
			}
			nc = append(nc, cc)
		}
	}

	// Intern the constraints and create final output slice.
	var rv []internedConstraintsConjunction
	for i := range nc {
		icc := internedConstraintsConjunction{
			numReplicas: nc[i].NumReplicas,
			constraints: interner.internConstraintsConj(nc[i].Constraints),
		}
		rv = append(rv, icc)
	}
	return rv, nil
}

// Structural normalization establishes relationships between every pair of
// ConstraintsConjunctions in constraints and voterConstraints, and then tries
// to map conjunctions in voterConstraints to narrower conjunctions in
// constraints. This is done to handle configs which under-specify
// conjunctions in voterConstraints under the assumption that one does not
// need to repeat information provided in constraints (see the new
// "strictness" comment in roachpb.SpanConfig which now requires users to
// repeat the information).
//
// This function does some structural normalization even when returning an
// error. See the under-specified voter constraint examples in the datadriven
// test -- we sometimes see these in production settings, and we want to fix
// ones that we can, and raise an error for users to fix their configs.
func doStructuralNormalization(conf *normalizedSpanConfig) (*normalizedSpanConfig, error) {
	// If there are no constraints or voterConstraints defined, nothing to do.
	if len(conf.constraints) == 0 || len(conf.voterConstraints) == 0 {
		return conf, nil
	}

	// We want to examine the relationship between every pair:
	//   - voter constraint conjunction (from voterConstraints list)
	//   - all-replica constraint conjunction (from constraints list)
	// This relationship can be: equal, strict subset, strict superset, non-intersecting, or intersecting.
	type relationshipVoterAndAll struct {
		voterIndex     int                     // Index of the voter constraint in conf.voterConstraints
		allIndex       int                     // Index of the all-replica constraint in conf.constraints
		voterAndAllRel conjunctionRelationship // Relationship between these two constraint conjunctions
	}

	emptyConstraintIndex := -1      // Will hold index of the empty (catch-all) constraint, if it exists
	emptyVoterConstraintIndex := -1 // Will hold index of the empty voter constraint, if it exists

	var rels []relationshipVoterAndAll // List of all pairwise relationships
	for i := range conf.voterConstraints {
		// Check if this voter constraint is the empty constraint (no conjunctions)
		if len(conf.voterConstraints[i].constraints) == 0 {
			emptyVoterConstraintIndex = i
		}
		for j := range conf.constraints {
			// Check if this all-replica constraint is the empty constraint
			if len(conf.constraints[j].constraints) == 0 {
				emptyConstraintIndex = j
			}
			rels = append(rels, relationshipVoterAndAll{
				voterIndex:     i,
				allIndex:       j,
				voterAndAllRel: conf.voterConstraints[i].constraints.relationship(conf.constraints[j].constraints),
			})
		}
	}

	// Sort relationships so those requiring attention come first (by conj relationship order)
	slices.SortFunc(rels, func(a, b relationshipVoterAndAll) int {
		return cmp.Compare(a.voterAndAllRel, b.voterAndAllRel)
	})

	// Check for intersecting constraints - which is always an error.
	index := 0
	for rels[index].voterAndAllRel == conjIntersecting {
		index++
	}
	var err error
	if index > 0 {
		// At least one intersecting pair was found - record an error.
		err = errors.Errorf("intersecting conjunctions in constraints and voter constraints")
	}
	// Even if we saw an error, normalization proceeds in an attempt to produce a best-effort result.

	// Track, for each all-replica constraint:
	//   - how many replicas have not been claimed by voter constraints,
	//   - and, if we've built a new voter constraint based on narrowing this constraint, its index.
	type allReplicaConstraintsInfo struct {
		remainingReplicas int32 // Replicas of this constraint still available for voter constraints
		newVoterIndex     int   // Index of a narrower voter constraint created from this constraint, else -1
	}
	var allReplicaConstraints []allReplicaConstraintsInfo
	for i := range conf.constraints {
		allReplicaConstraints = append(allReplicaConstraints,
			allReplicaConstraintsInfo{
				remainingReplicas: conf.constraints[i].numReplicas, // initially, all are unassigned
				newVoterIndex:     -1,
			})
	}

	// For each voter constraint, we track its interned form (with zeroed numReplicas),
	// and how many "additional" replicas should be created by narrowing.
	type voterConstraintsAndAdditionalInfo struct {
		internedConstraintsConjunction
		additionalReplicas int32
	}
	var voterConstraints []voterConstraintsAndAdditionalInfo
	for _, constraint := range conf.voterConstraints {
		constraint.numReplicas = 0
		voterConstraints = append(voterConstraints, voterConstraintsAndAdditionalInfo{
			internedConstraintsConjunction: constraint,
		})
	}

	// 1. Assign as many voter replicas as possible via equal and strict subset relationships.
	for ; index < len(rels) && rels[index].voterAndAllRel <= conjStrictSubset; index++ {
		rel := rels[index]
		if rel.voterIndex == emptyVoterConstraintIndex {
			// Don't claim from the empty all-replica constraint for the empty voter constraint;
			// might need it later to satisfy voter constraints that have NO matching all-replica constraint.
			continue
		}
		remainingAll := allReplicaConstraints[rel.allIndex].remainingReplicas
		// Compute the number of voter replicas needed for this voter constraint.
		// Note that additionalReplicas is always zero in this part of the code.
		neededVoterReplicas := conf.voterConstraints[rel.voterIndex].numReplicas -
			voterConstraints[rel.voterIndex].numReplicas
		if neededVoterReplicas > 0 && remainingAll > 0 {
			// Satisfy as many replicas as we can for this voter constraint with this all-replica constraint.
			toAdd := remainingAll
			if toAdd > neededVoterReplicas {
				toAdd = neededVoterReplicas
			}
			voterConstraints[rel.voterIndex].numReplicas += toAdd
			allReplicaConstraints[rel.allIndex].remainingReplicas -= toAdd
		}
	}

	// 2. At this point, only strict superset (or non-intersecting, which is ignored) relationships remain.
	//    Use strict superset relationships to create *narrower* voter constraints ("load balancing" among supersets).
	//    The idea is to favor spreading replicas when both c1∧c2 and c1∧c3 could satisfy a voter constraint c1.

	// Before "narrowing", check if (empty voter constraint, empty all-replica constraint) exist.
	// If so, try to satisfy empty voter constraint with any remaining replicas from the empty all-replica constraint,
	// to avoid unnecessary creation of narrowed constraints.
	if emptyVoterConstraintIndex > 0 && emptyConstraintIndex > 0 {
		neededReplicas := conf.voterConstraints[emptyVoterConstraintIndex].numReplicas
		actualReplicas := voterConstraints[emptyVoterConstraintIndex].numReplicas
		remaining := neededReplicas - actualReplicas
		if remaining > 0 {
			remainingSatisfiable := allReplicaConstraints[emptyConstraintIndex].remainingReplicas
			if remainingSatisfiable > 0 {
				count := remainingSatisfiable
				if count > remaining {
					count = remaining
				}
				// Satisfy as many as we can for the empty voter constraint
				voterConstraints[emptyVoterConstraintIndex].numReplicas += count
				allReplicaConstraints[emptyConstraintIndex].remainingReplicas -= count
			}
		}
	}

	// Now, loop over all remaining strict superset relationships and try to
	// "narrow" constraints, spreading the needed voter replicas evenly.
	for {
		added := false
		for i := index; i < len(rels) && rels[i].voterAndAllRel == conjStrictSuperset; i++ {
			rel := rels[i]
			remainingAll := allReplicaConstraints[rel.allIndex].remainingReplicas
			// Compute how many more replicas are needed from this voter constraint
			neededVoterReplicas := conf.voterConstraints[rel.voterIndex].numReplicas -
				voterConstraints[rel.voterIndex].numReplicas -
				voterConstraints[rel.voterIndex].additionalReplicas
			if neededVoterReplicas > 0 && remainingAll > 0 {
				// Take one replica to satisfy a narrowed constraint (to enable load balancing)
				voterConstraints[rel.voterIndex].additionalReplicas++
				allReplicaConstraints[rel.allIndex].remainingReplicas--
				newVoterIndex := allReplicaConstraints[rel.allIndex].newVoterIndex
				if newVoterIndex == -1 {
					// If the all-replica constraint hasn't already spawned a narrowed voter constraint, do so now.
					newVoterIndex = len(voterConstraints)
					allReplicaConstraints[rel.allIndex].newVoterIndex = newVoterIndex
					voterConstraints = append(voterConstraints, voterConstraintsAndAdditionalInfo{
						internedConstraintsConjunction: internedConstraintsConjunction{
							numReplicas: 0,
							constraints: conf.constraints[rel.allIndex].constraints,
						},
						additionalReplicas: 0,
					})
				}
				voterConstraints[newVoterIndex].numReplicas++
				added = true
			}
		}
		if !added {
			break // No more work to do: all possible narrowing has been attempted
		}
	}

	// 3. Final accounting: for each original voter constraint, make sure we satisfied its needs (or fill anyway)
	for i := range conf.voterConstraints {
		neededReplicas := conf.voterConstraints[i].numReplicas
		actualReplicas := voterConstraints[i].numReplicas + voterConstraints[i].additionalReplicas
		if actualReplicas > neededReplicas {
			panic("code bug") // Invariant: should never over-assign voter replicas!
		}
		if actualReplicas < neededReplicas {
			// Not all voter constraints could be satisfied (e.g., non-intersecting config).
			err = errors.Errorf("could not satisfy all voter constraints due to " +
				"non-intersecting conjunctions in voter and all replica constraints")
			// Just force satisfaction by adding remaining needed.
			voterConstraints[i].numReplicas += neededReplicas - actualReplicas
		}
	}

	// 4. Move the empty voter constraint (i.e., the catch-all) to the end, so it's considered last for placement logic.
	n := len(voterConstraints) - 1
	if emptyVoterConstraintIndex >= 0 && emptyVoterConstraintIndex < n {
		voterConstraints[emptyVoterConstraintIndex], voterConstraints[n] =
			voterConstraints[n], voterConstraints[emptyVoterConstraintIndex]
	}

	// 5. Collect back only voter constraints with > 0 assigned replicas (ignore others)
	var vc []internedConstraintsConjunction
	for i := range voterConstraints {
		if voterConstraints[i].numReplicas > 0 {
			vc = append(vc, voterConstraints[i].internedConstraintsConjunction)
		}
	}
	conf.voterConstraints = vc

	// --
	// Now, normalize constraints (not just voterConstraints), especially when
	// there is an empty constraint and it's possible to "tighten" its allocation
	// based on actual voter needs. This reduces risk of later misplacing non-voter replicas.
	// --

	if emptyConstraintIndex >= 0 {
		// The structure of voterConstraints has changed, so recompute relationships for this normalization pass.
		emptyVoterConstraintIndex = -1
		rels = rels[:0]
		for i := range conf.voterConstraints {
			if len(conf.voterConstraints[i].constraints) == 0 {
				// Again, just record if we see one.
				emptyVoterConstraintIndex = i
			}
			for j := range conf.constraints {
				rels = append(rels, relationshipVoterAndAll{
					voterIndex: i,
					allIndex:   j,
					voterAndAllRel: conf.voterConstraints[i].constraints.relationship(
						conf.constraints[j].constraints),
				})
			}
		}
		// Sort again for relationship examination
		slices.SortFunc(rels, func(a, b relationshipVoterAndAll) int {
			return cmp.Compare(a.voterAndAllRel, b.voterAndAllRel)
		})
		// Again, skip contradicting pairs (conjIntersecting).
		index = 0
		for rels[index].voterAndAllRel == conjIntersecting {
			index++
		}
		voterConstraintHasEqualityWithConstraint := make([]bool, len(conf.voterConstraints))
		// For conjEqualSet: try to align all-replica constraint's replica count with voterConstraint needs,
		// using the empty constraint's allocation if necessary.
		for ; index < len(rels) && rels[index].voterAndAllRel <= conjEqualSet; index++ {
			rel := rels[index]
			voterConstraintHasEqualityWithConstraint[rel.voterIndex] = true
			if rel.allIndex == emptyConstraintIndex {
				// This is the (empty,empty) case; nothing to transfer
				continue
			}
			// If the matching constraint doesn't have enough replicas, "steal" from the empty constraint
			if conf.constraints[rel.allIndex].numReplicas < conf.voterConstraints[rel.voterIndex].numReplicas {
				toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas -
					conf.constraints[rel.allIndex].numReplicas
				availableCount := conf.constraints[emptyConstraintIndex].numReplicas
				if availableCount < toAddCount {
					toAddCount = availableCount
				}
				// Reassign some replicas from the empty constraint to this specific constraint
				conf.constraints[emptyConstraintIndex].numReplicas -= toAddCount
				conf.constraints[rel.allIndex].numReplicas += toAddCount
			}
		}
		// Now, for conjStrictSubset: If there's a subset voter constraint, but no exact match, allocate from empty constraint
		for ; index < len(rels) && rels[index].voterAndAllRel <= conjStrictSubset; index++ {
			rel := rels[index]
			if rel.allIndex != emptyConstraintIndex {
				// Only care about ones that map to the empty constraint
				continue
			}
			if voterConstraintHasEqualityWithConstraint[rel.voterIndex] {
				// Already handled by exact match, skip
				continue
			}
			availableCount := conf.constraints[emptyConstraintIndex].numReplicas
			if availableCount > 0 {
				toAddCount := conf.voterConstraints[rel.voterIndex].numReplicas
				if toAddCount > availableCount {
					toAddCount = availableCount
				}
				// Reassign from the empty constraint to a new specific one.
				conf.constraints[emptyConstraintIndex].numReplicas -= toAddCount
				conf.constraints = append(conf.constraints, internedConstraintsConjunction{
					numReplicas: toAddCount,
					constraints: conf.voterConstraints[rel.voterIndex].constraints,
				})
			}
		}
		// To keep ordering (more-specific-to-less), keep the empty constraint last,
		// swapping if we've just appended.
		n := len(conf.constraints) - 1
		if n != emptyConstraintIndex {
			conf.constraints[n], conf.constraints[emptyConstraintIndex] =
				conf.constraints[emptyConstraintIndex], conf.constraints[n]
		}
		// If, as a result, the empty constraint has no remaining replicas, drop it entirely.
		if conf.constraints[n].numReplicas == 0 {
			conf.constraints = conf.constraints[:n]
		}
	}
	return conf, err
}

type replicaKindIndex int32

const (
	voterIndex replicaKindIndex = iota
	nonVoterIndex
	numReplicaKinds
)

// NB: To optimize allocations, try to avoid maps in rangeAnalyzedConstraints,
// analyzedConstraints, and analyzeConstraintsBuf. Slices are easy to reuse.

// analyzedConstraints is the result of processing a normalized
// []roachpb.ConstraintsConjunction from the spanConfig against the current
// set of replicas.
type analyzedConstraints struct {
	// If len(constraints) == 0, there are no constraints, and the satisfiedBy*
	// slices are also empty.
	constraints []internedConstraintsConjunction

	// Overlapping conjunctions: There is nothing preventing overlapping
	// ConstraintsConjunctions such that the same store can satisfy multiple,
	// though we expect this to be uncommon. This is algorithmically painful
	// since:
	// - Duplicate stores in satisfiedBy* slices make it hard to decide what
	//   still needs to be satisfied.
	// - When we need to move a replica from s1 (for rebalancing) we want to
	//   cheaply compute which constraint the new store needs to satisfy.
	//   Consider the case where existing replicas s1 and s2 both could satisfy
	//   constraint conjunction cc1 and cc2 each of which needed 1 replica to be
	//   satisfied. Now when trying to find a new store to take the place of s1,
	//   ideally we can consider either cc1 or cc2, since s2 can take the place
	//   of s1 in either constraint conjunction. But considering the
	//   combinations of existing replicas is complicated, so we avoid it.
	//
	// For simplicity, we assume that if the same store can satisfy multiple
	// conjunctions, users have ordered the conjunctions in spanConfig from most
	// strict to least strict, such that once a store satisfies one conjunction
	// we omit considering it for a later conjunction. That is, we satisfy in a
	// greedy manner instead of considering all possibilities. So all the
	// satisfiedBy slices represent sets that are non-intersecting.

	satisfiedByReplica [numReplicaKinds][][]roachpb.StoreID

	// These are stores that satisfy no constraint. Even though we are strict
	// about constraint satisfaction, this can happen if the SpanConfig changed
	// or the attributes of a store changed. Additionally, if these
	// analyzedConstraints correspond to voterConstraints, there can be
	// non-voters here (which is harmless).
	satisfiedNoConstraintReplica [numReplicaKinds][]roachpb.StoreID
}

func (ac *analyzedConstraints) clear() {
	ac.constraints = ac.constraints[:0]
	for i := range ac.satisfiedByReplica {
		ac.satisfiedByReplica[i] = clear2DSlice(ac.satisfiedByReplica[i])
		ac.satisfiedNoConstraintReplica[i] = ac.satisfiedNoConstraintReplica[i][:0]
	}
}

func (ac *analyzedConstraints) isEmpty() bool {
	return len(ac.constraints) == 0
}

// extend2DSlice extends a 2D slice by adding an empty inner slice.
// On the slow path (not enough capacity), append(v, nil) adds a zero-valued
// (empty) inner slice at the end.
func extend2DSlice[T any](v [][]T) [][]T {
	n := len(v)
	if cap(v) > n {
		v = v[:n+1]
		v[n] = v[n][:0]
	} else {
		// Slow path: append nil gives a new empty inner slice.
		v = append(v, nil)
	}
	return v
}

// clear2DSlice resets all inner slices to empty and truncates the outer slice.
func clear2DSlice[T any](v [][]T) [][]T {
	for i := range v {
		v[i] = v[i][:0]
	}
	return v[:0]
}

// rangeAnalyzedConstraints is a function of the spanConfig and the current
// stores that have replicas for that range (including the ReplicaType).
//
// LEARNER and VOTER_DEMOTING_LEARNER replicas are ignored.
type rangeAnalyzedConstraints struct {
	numNeededReplicas [numReplicaKinds]int32
	replicas          [numReplicaKinds][]storeAndLocality
	constraints       analyzedConstraints
	voterConstraints  analyzedConstraints
	// TODO(sumeer): add unit test for these.
	replicaLocalityTiers replicasLocalityTiers
	voterLocalityTiers   replicasLocalityTiers

	// leasePreferenceIndices[i] is the index of the earliest entry in
	// normalizedSpanConfig.leasePreferences, matched by the replica at
	// replicas[voterIndex][i]. If the replica does not match any lease
	// preference, this is set to math.MaxInt32.
	leasePreferenceIndices     []int32
	leaseholderID              roachpb.StoreID
	leaseholderPreferenceIndex int32

	votersDiversityScore   float64
	replicasDiversityScore float64

	spanConfig *normalizedSpanConfig
	buf        analyzeConstraintsBuf
}

func (rac *rangeAnalyzedConstraints) String() string {
	return redact.StringWithoutMarkers(rac)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rac *rangeAnalyzedConstraints) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("leaseholder=%v", rac.leaseholderID)
	w.Print(" voters=[")
	for i := range rac.replicas[voterIndex] {
		if i > 0 {
			w.Print(", ")
		}
		w.Printf("s%v", rac.replicas[voterIndex][i].StoreID)
	}
	w.Print("]")
	w.Print(" non-voters=[")
	for i := range rac.replicas[nonVoterIndex] {
		if i > 0 {
			w.Print(", ")
		}
		w.Printf("s%v", rac.replicas[nonVoterIndex][i].StoreID)
	}
	w.Print("]")
	w.Printf(" req_num_voters=%v req_num_non_voters=%v",
		rac.numNeededReplicas[voterIndex], rac.numNeededReplicas[nonVoterIndex])
}

var rangeAnalyzedConstraintsPool = sync.Pool{
	New: func() interface{} {
		return &rangeAnalyzedConstraints{}
	},
}

func newRangeAnalyzedConstraints() *rangeAnalyzedConstraints {
	return rangeAnalyzedConstraintsPool.Get().(*rangeAnalyzedConstraints)
}

func releaseRangeAnalyzedConstraints(rac *rangeAnalyzedConstraints) {
	rac.constraints.clear()
	rac.voterConstraints.clear()
	rac.buf.clear()
	for i := range rac.replicas {
		rac.replicas[i] = rac.replicas[i][:0]
	}
	rac.leasePreferenceIndices = rac.leasePreferenceIndices[:0]
	*rac = rangeAnalyzedConstraints{
		replicas:         rac.replicas,
		constraints:      rac.constraints,
		voterConstraints: rac.voterConstraints,
		buf:              rac.buf,
	}
	rangeAnalyzedConstraintsPool.Put(rac)
}

// Initialization usage:
//
// Initialization of rangeAnalyzedConstraints is done by first fetching (using
// stateForInit) and initializing the analyzeConstraintsBuf via
// analyzeConstraintsBuf.tryAddingStore, followed by calling finishInit. The
// rangeAnalyzedConstraints should be retrieved from
// rangeAnalyzedConstraintsPool and when no longer needed returned using
// releaseRangeAnalyzedConstraints.
func (rac *rangeAnalyzedConstraints) stateForInit() *analyzeConstraintsBuf {
	return &rac.buf
}

type storeMatchesConstraintInterface interface {
	storeMatches(storeID roachpb.StoreID, constraintConj constraintsConj) bool
}

// finishInit performs final setup for rangeAnalyzedConstraints after store
// information has been gathered. It processes the provided normalizedSpanConfig
// and evaluates constraint satisfaction for all stores, assigning stores to
// constraint buckets for both voters and non-voters. It also assigns lease
// preference indices, verifies leaseholder presence, and computes diversity
// scores for both voters and all replicas.
func (rac *rangeAnalyzedConstraints) finishInit(
	spanConfig *normalizedSpanConfig,
	constraintMatcher storeMatchesConstraintInterface,
	leaseholder roachpb.StoreID,
) {
	rac.spanConfig = spanConfig
	rac.numNeededReplicas[voterIndex] = spanConfig.numVoters
	rac.numNeededReplicas[nonVoterIndex] = spanConfig.numReplicas - spanConfig.numVoters
	rac.replicas = rac.buf.replicas

	// analyzeFunc matches stores to constraints, populating assignment
	// structures for later decision-making.
	// analyzeFunc maps stores to constraints based on the given analyzedConstraints struct.
	// It fills various slices indicating which replica/store satisfies which constraint.
	analyzeFunc := func(ac *analyzedConstraints) {
		if len(ac.constraints) == 0 {
			// No constraints to analyze; nothing to do.
			return
		}
		// Expand/extend internal 2D slices (for both replica kinds) to match the number of constraints.
		for i := 0; i < len(ac.constraints); i++ {
			ac.satisfiedByReplica[voterIndex] = extend2DSlice(ac.satisfiedByReplica[voterIndex])
			ac.satisfiedByReplica[nonVoterIndex] = extend2DSlice(ac.satisfiedByReplica[nonVoterIndex])
		}

		// For each replica kind (voter/non-voter), iterate over known stores.
		// For each store, determine which constraints it satisfies.
		for kind := voterIndex; kind < numReplicaKinds; kind++ {
			for i, store := range rac.buf.replicas[kind] {
				// Ensure the constraint indices list for this kind is sufficiently large.
				rac.buf.replicaConstraintIndices[kind] =
					extend2DSlice(rac.buf.replicaConstraintIndices[kind])
				// For each constraint, check if store satisfies it (or the unconstrained catch-all).
				for j, c := range ac.constraints {
					if len(c.constraints) == 0 || constraintMatcher.storeMatches(store.StoreID, c.constraints) {
						// Store satisfies this constraint, record constraint index.
						rac.buf.replicaConstraintIndices[kind][i] =
							append(rac.buf.replicaConstraintIndices[kind][i], int32(j))
					}
				}
				n := len(rac.buf.replicaConstraintIndices[kind][i])
				if n == 0 {
					// Store doesn't satisfy any constraint: add to "no constraint matched" list.
					ac.satisfiedNoConstraintReplica[kind] =
						append(ac.satisfiedNoConstraintReplica[kind], store.StoreID)
				} else if n == 1 {
					// Store satisfies exactly one constraint: assign directly to that constraint.
					constraintIndex := rac.buf.replicaConstraintIndices[kind][i][0]
					ac.satisfiedByReplica[kind][constraintIndex] =
						append(ac.satisfiedByReplica[kind][constraintIndex], store.StoreID)
					// Clear constraint indices as assignment is done.
					rac.buf.replicaConstraintIndices[kind][i] = rac.buf.replicaConstraintIndices[kind][i][:0]
				}
				// If store satisfies multiple constraints, assignment is deferred for next stage.
			}
		}

		// For stores matching multiple constraints, assign stores in a fixed order,
		// stopping when each constraint's replica target is satisfied and avoiding over-assignment.
		for j := range ac.constraints {
			// Helper function to check if the sum of assigned voter and non-voter replicas
			// meets (or exceeds) the number of replicas needed for this constraint.
			doneFunc := func() bool {
				return len(ac.satisfiedByReplica[voterIndex][j])+
					len(ac.satisfiedByReplica[nonVoterIndex][j]) >= int(ac.constraints[j].numReplicas)
			}
			done := doneFunc()
			if done {
				// Constraint already satisfied; no stores need to be assigned to it.
				continue
			}
			// Iterate over replicas (voters then non-voters) and try to assign deferred stores to this constraint.
			for kind := voterIndex; kind < numReplicaKinds; kind++ {
				for i := range rac.buf.replicaConstraintIndices[kind] {
					constraintIndices := rac.buf.replicaConstraintIndices[kind][i]
					for _, index := range constraintIndices {
						if index == int32(j) {
							// Assign this store to constraint j for this kind.
							ac.satisfiedByReplica[kind][j] =
								append(ac.satisfiedByReplica[kind][j], rac.replicas[kind][i].StoreID)
							// Clear out indices as assignment is handled.
							rac.buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
							// Recompute done status after assignment.
							done = doneFunc()
							// Store assigned, stop looking at other constraint indices for this store.
							break
						}
					}
					// If constraint is now satisfied, don't assign more stores to it.
					if done {
						break
					}
				}
				if done {
					break
				}
			}
		}

		// Any remaining stores that satisfied multiple constraints but weren't assigned above
		// are greedily assigned to constraints (should not oversatisfy due to earlier logic).
		for kind := voterIndex; kind < numReplicaKinds; kind++ {
			for i := range rac.buf.replicaConstraintIndices[kind] {
				constraintIndices := rac.buf.replicaConstraintIndices[kind][i]
				for _, index := range constraintIndices {
					// Assign to the first matching constraint remaining; break after assignment.
					ac.satisfiedByReplica[kind][index] =
						append(ac.satisfiedByReplica[kind][index], rac.replicas[kind][i].StoreID)
					rac.buf.replicaConstraintIndices[kind][i] = constraintIndices[:0]
					break
				}
			}
		}
	}

	// Analyze constraints for general and voter-specific constraints.
	if spanConfig.constraints != nil {
		rac.constraints.constraints = spanConfig.constraints
		analyzeFunc(&rac.constraints)
	}
	if spanConfig.voterConstraints != nil {
		rac.voterConstraints.constraints = spanConfig.voterConstraints
		analyzeFunc(&rac.voterConstraints)
	}

	// Leaseholder must exist; compute lease preference indices and leaseholder preference.
	rac.leaseholderID = leaseholder
	rac.leaseholderPreferenceIndex = -1
	for i := range rac.replicas[voterIndex] {
		storeID := rac.replicas[voterIndex][i].StoreID
		leasePreferenceIndex := matchedLeasePreferenceIndex(
			storeID, spanConfig.leasePreferences, constraintMatcher)
		rac.leasePreferenceIndices = append(rac.leasePreferenceIndices, leasePreferenceIndex)
		if storeID == leaseholder {
			rac.leaseholderPreferenceIndex = leasePreferenceIndex
		}
	}
	if rac.leaseholderPreferenceIndex == -1 {
		// Leaseholder might be a demoting non-voter.
		for i := range rac.replicas[nonVoterIndex] {
			storeID := rac.replicas[nonVoterIndex][i].StoreID
			if storeID == leaseholder {
				rac.leaseholderPreferenceIndex = matchedLeasePreferenceIndex(
					storeID, spanConfig.leasePreferences, constraintMatcher)
				break
			}
		}
		if rac.leaseholderPreferenceIndex == -1 {
			panic("leaseholder not found in replicas")
		}
	}

	// Compute voter and overall replica diversity scores for range.
	var replicaLocalityTiers, voterLocalityTiers []localityTiers
	for i := range rac.replicas[voterIndex] {
		replicaLocalityTiers = append(replicaLocalityTiers, rac.replicas[voterIndex][i].localityTiers)
		voterLocalityTiers = append(voterLocalityTiers, rac.replicas[voterIndex][i].localityTiers)
	}
	for i := range rac.replicas[nonVoterIndex] {
		replicaLocalityTiers = append(replicaLocalityTiers, rac.replicas[nonVoterIndex][i].localityTiers)
	}
	rac.replicaLocalityTiers = makeReplicasLocalityTiers(replicaLocalityTiers)
	rac.voterLocalityTiers = makeReplicasLocalityTiers(voterLocalityTiers)

	// Helper to compute the sum of pairwise diversity scores and sample count.
	diversityFunc := func(
		stores1 []storeAndLocality, stores2 []storeAndLocality, sameStores bool,
	) (sumScore float64, numSamples int) {
		for i := range stores1 {
			s1 := stores1[i]
			for j := range stores2 {
				s2 := stores2[j]
				// Avoid duplicate scoring for the same pair if stores1 == stores2.
				if sameStores && s2.StoreID <= s1.StoreID {
					continue
				}
				sumScore += s1.localityTiers.diversityScore(s2.localityTiers)
				numSamples++
			}
		}
		return sumScore, numSamples
	}
	// Compute normalized average diversity from pairwise sums.
	scoreFromSumAndSamples := func(sumScore float64, numSamples int) float64 {
		if numSamples == 0 {
			return roachpb.MaxDiversityScore
		}
		return sumScore / float64(numSamples)
	}
	// Voter diversity.
	sumVoterScore, numVoterSamples := diversityFunc(
		rac.replicas[voterIndex], rac.replicas[voterIndex], true)
	rac.votersDiversityScore = scoreFromSumAndSamples(sumVoterScore, numVoterSamples)

	// All replica diversity, considering both voter and non-voter contributions.
	sumReplicaScore, numReplicaSamples := sumVoterScore, numVoterSamples
	srs, nrs := diversityFunc(rac.replicas[nonVoterIndex], rac.replicas[nonVoterIndex], true)
	sumReplicaScore += srs
	numReplicaSamples += nrs
	srs, nrs = diversityFunc(rac.replicas[voterIndex], rac.replicas[nonVoterIndex], false)
	sumReplicaScore += srs
	numReplicaSamples += nrs
	rac.replicasDiversityScore = scoreFromSumAndSamples(sumReplicaScore, numReplicaSamples)
}

// Disjunction of conjunctions.
type constraintsDisj []constraintsConj

// FNV-1a hash algorithm.
func (cd constraintsDisj) hash() uint64 {
	h := uint64(offset64)
	for i := range cd {
		for _, c := range cd[i] {
			h ^= uint64(c.typ)
			h *= prime64
			h ^= uint64(c.key)
			h *= prime64
			h ^= uint64(c.value)
			h *= prime64
		}
		// Separator between conjunctions.
		h *= prime64
	}
	return h
}

func (cd constraintsDisj) isEqual(b mapKey) bool {
	other := b.(constraintsDisj)
	if len(cd) != len(other) {
		return false
	}
	for i := range cd {
		c1 := cd[i]
		c2 := other[i]
		if len(c1) != len(c2) {
			return false
		}
		for j := range c1 {
			if c1[j] != c2[j] {
				return false
			}
		}
	}
	return true
}

var _ mapKey = constraintsDisj{}

func (rac *rangeAnalyzedConstraints) replicaRole(
	storeID roachpb.StoreID,
) (isVoter bool, isNonVoter bool) {
	for _, s := range rac.replicas[voterIndex] {
		if s.StoreID == storeID {
			return true, false
		}
	}
	for _, s := range rac.replicas[nonVoterIndex] {
		if s.StoreID == storeID {
			return false, true
		}
	}
	return false, false
}

// Usage for a range that may need attention:
//
//				if notEnoughVoters() {
//				  stores := candidatesToConvertFromNonVoterToVoter()
//				  if len(stores) > 0 {
//				    // Pick the candidate that is best for voter diversity
//				    ...
//				  } else {
//			      conjOfDisj, err := constraintsForAddingVoter()
//			      // Use conjOfDisj to prune stores and then add store
//			      // that can handle the load and is best for voter diversity.
//			      ...
//			    }
//				} else if notEnoughNonVoters() {
//		      stores := candidatesToConvertFromVoterToNonVoter()
//		      if len(stores) > 0 {
//		        // Pick the candidate that is best for diversity.
//		      } else {
//		        disj := constraintsForAddingNonVoter()
//		        // Use disj to prune stores and then add store
//		        // that can handle the load and is best for voter diversity.
//		      }
//		    } else {
//          // Have enough replicas of each kind, but not necessarily in the right places.
//          swapCands := candidatesForRoleSwapForConstraints()
//          if len(swapCands[voterIndex]) > 0 {
//            ...
//          }
//          toRemove := candidatesToRemove()
//          if len(toRemove) > 0 {
//            // Have extra voters or non-voters. Remove one.
//            ...
//          } else {
//	          // Have right number of voters and non-voters, but constraints may
//	          // not be satisfied.
//	          storesToRemove, conjOfDisjToAdd := candidatesVoterConstraintsUnsatisfied()
//	          if ...{
//
//		        }
//		        storesToRemove, conjOfDisjToAdd := candidatesNonVoterConstraintsUnsatisfied() {
//	          if ...{
//
//		        }
//          }
//        }
//
// Rebalance:
//   Only if !notEnoughVoters() && !notEnoughNonVoters() && no constraints unsatisfied.
//   cc := candidatesToReplaceVoterForRebalance(storeID)
//   cc := candidatesToReplaceNonVoterForRebalance(storeID)

func (rac *rangeAnalyzedConstraints) notEnoughVoters() bool {
	return len(rac.replicas[voterIndex]) < int(rac.numNeededReplicas[voterIndex])
}

func (rac *rangeAnalyzedConstraints) notEnoughNonVoters() bool {
	return len(rac.replicas[nonVoterIndex]) < int(rac.numNeededReplicas[nonVoterIndex])
}

func (rac *rangeAnalyzedConstraints) expectEnoughNonVoters(enough bool) error {
	if rac.notEnoughNonVoters() == enough {
		return errors.AssertionFailedf(
			"expected enough=%v non-voters but have %d/%d",
			enough, len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVoters(enough bool) error {
	if rac.notEnoughVoters() == enough {
		return errors.AssertionFailedf(
			"expected enough=%v voters but have %d/%d",
			enough, len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectEnoughVotersAndNonVoters() error {
	if rac.notEnoughVoters() || rac.notEnoughNonVoters() {
		return errors.AssertionFailedf(
			"expected enough voters and non-voters but have %d/%d voters and %d/%d non-voters",
			len(rac.replicas[voterIndex]), int(rac.numNeededReplicas[voterIndex]),
			len(rac.replicas[nonVoterIndex]), int(rac.numNeededReplicas[nonVoterIndex]),
		)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) voterConstraintCount() (under, match, over int) {
	for i, c := range rac.voterConstraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualReplicas := len(rac.voterConstraints.satisfiedByReplica[voterIndex][i])
		if neededReplicas > actualReplicas {
			under++
		} else if neededReplicas == actualReplicas {
			match++
		} else {
			over++
		}
	}
	return under, match, over
}

func (rac *rangeAnalyzedConstraints) constraintCount() (under, match, over int) {
	for i, c := range rac.constraints.constraints {
		neededReplicas := int(c.numReplicas)
		actualReplicas := len(rac.constraints.satisfiedByReplica[voterIndex][i]) +
			len(rac.constraints.satisfiedByReplica[nonVoterIndex][i])
		if neededReplicas > actualReplicas {
			under++
		} else if neededReplicas == actualReplicas {
			match++
		} else {
			over++
		}
	}
	return under, match, over
}

func (rac *rangeAnalyzedConstraints) expectMatchedConstraints() error {
	if under, match, over := rac.constraintCount(); under > 0 || over > 0 {
		return errors.AssertionFailedf(
			"expected only matched constraints but found "+
				"under=%d match=%d over=%d", under, match, over)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectMatchedVoterConstraints() error {
	if under, match, over := rac.voterConstraintCount(); under > 0 || over > 0 {
		return errors.AssertionFailedf(
			"expected only matched voter constraints but found "+
				"under=%d match=%d over=%d", under, match, over)
	}
	return nil
}

func (rac *rangeAnalyzedConstraints) expectNoUnsatisfied() error {
	if err := rac.expectMatchedConstraints(); err != nil {
		return err
	}
	return rac.expectMatchedVoterConstraints()
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters() and no unsatisfied
// constraint.
func (rac *rangeAnalyzedConstraints) candidatesToReplaceVoterForRebalance(
	storeID roachpb.StoreID,
) (toReplace constraintsConj, err error) {
	if err = rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}
	if err = rac.expectNoUnsatisfied(); err != nil {
		// Need to address constraint satisfaction first.
		return nil, err
	}

	for i, c := range rac.voterConstraints.constraints {
		// Find the voter constraint which the store being replaced satisfies.
		for _, checkStoreID := range rac.voterConstraints.satisfiedByReplica[voterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	for i, c := range rac.constraints.constraints {
		// Find the all-replica constraint which the store being replaced
		// satisfies.
		for _, checkStoreID := range rac.constraints.satisfiedByReplica[voterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	return nil,
		errors.Errorf("expected replaced store %d to match a constraint", toReplace)
}

// REQUIRES: !notEnoughVoters() and !notEnoughNonVoters() and no unsatisfied
// constraint.
func (rac *rangeAnalyzedConstraints) candidatesToReplaceNonVoterForRebalance(
	storeID roachpb.StoreID,
) (toReplace constraintsConj, err error) {
	if err = rac.expectEnoughVotersAndNonVoters(); err != nil {
		// Need to add necessary voters and non-voters first.
		return nil, err
	}
	if err = rac.expectNoUnsatisfied(); err != nil {
		// Need to address constraint satisfaction first.
		return nil, err
	}

	for i, c := range rac.constraints.constraints {
		// Find the all-replica constraint which the store being replaced
		// satisfies.
		for _, checkStoreID := range rac.constraints.satisfiedByReplica[nonVoterIndex][i] {
			if checkStoreID == storeID {
				return c.constraints, nil
			}
		}
	}
	return nil,
		errors.Errorf("expected replaced store %d to match a constraint", toReplace)
}

type storeAndLeasePreference struct {
	// Smaller is better.
	leasePreferenceIndex int32
	storeID              roachpb.StoreID
}

// candidatesToMoveLease will return candidates equal to or better than the
// current leaseholder wrt satisfaction of lease preferences. The candidates
// exclude the current leaseholder, and are sorted in non-increasing order of
// lease preference satisfaction.
//
// TODO(sumeer): the computation in this method can be performed once, when
// constructing rangeAnalyzedConstraints.
//
// Should this return the set of candidates which satisfy the first lease
// preference. If none do, the second lease preference, and so on. This
// implies a tighter condition than the current one for candidate selection.
// See existing allocator candidate selection:
// https://github.com/sumeerbhola/cockroach/blob/c4c1dcdeda2c0f38c38270e28535f2139a077ec7/pkg/kv/kvserver/allocator/allocatorimpl/allocator.go#L2980-L2980
// This may be unnecessarily strict and not essential, since it seems many
// users only set one lease preference.
func (rac *rangeAnalyzedConstraints) candidatesToMoveLease() (
	cands []storeAndLeasePreference,
	curLeasePreferenceIndex int32,
) {
	curLeasePreferenceIndex = rac.leaseholderPreferenceIndex
	for i := range rac.leasePreferenceIndices {
		if rac.leasePreferenceIndices[i] <= curLeasePreferenceIndex &&
			rac.replicas[voterIndex][i].StoreID != rac.leaseholderID {
			cands = append(cands, storeAndLeasePreference{
				leasePreferenceIndex: rac.leasePreferenceIndices[i],
				storeID:              rac.replicas[voterIndex][i].StoreID,
			})
		}
	}
	slices.SortFunc(cands, func(a, b storeAndLeasePreference) int {
		return cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex)
	})
	return cands, curLeasePreferenceIndex
}

func isVoter(typ roachpb.ReplicaType) bool {
	return typ == roachpb.VOTER_FULL || typ == roachpb.VOTER_INCOMING
}

func isNonVoter(typ roachpb.ReplicaType) bool {
	return typ == roachpb.NON_VOTER || typ == roachpb.VOTER_DEMOTING_NON_VOTER
}

// Helper for constructing rangeAnalyzedConstraints. Contains initial state
// and intermediate scratch space needed for computing
// rangeAnalyzedConstraints.
//
// All ReplicaTypes are translated into VOTER_FULL or NON_VOTER. We ignore the
// LEARNER state below, since a badly behaved replica can stay in LEARNER for
// a prolonged period of time, even if the raft group has quorum. This is
// usually fine since the LEARNER state must be happening because of a pending
// change, so typically we will not be analyzing this range. If the pending
// change expires, this LEARNER state has probably gone on too long and we can
// no longer depend on this LEARNER being useful, so we should up-replicate
// (which is what is likely to happen as a side-effect of ignoring the
// LEARNER).
//
// We also ignore VOTER_DEMOTING_LEARNER, since it is going away.
//
// TODO(sumeer): the read-only methods should also use this buf to reduce
// allocations, if there is no concurrency.
type analyzeConstraintsBuf struct {
	replicas [numReplicaKinds][]storeAndLocality

	// Scratch space. replicaConstraintIndices[k][i] is the constraint matching
	// state for replicas[k][i].
	replicaConstraintIndices [numReplicaKinds][][]int32
}

type storeAndLocality struct {
	roachpb.StoreID
	localityTiers
}

func (acb *analyzeConstraintsBuf) clear() {
	for i := range acb.replicas {
		acb.replicas[i] = acb.replicas[i][:0]
		acb.replicaConstraintIndices[i] = clear2DSlice(acb.replicaConstraintIndices[i])
	}
}

func (acb *analyzeConstraintsBuf) tryAddingStore(
	storeID roachpb.StoreID, replicaType roachpb.ReplicaType, locality localityTiers,
) {
	// We ae ignoring LEARNER and VOTER_DEMOTING_LEARNER, since these are
	// in the process of going away.
	//
	// If we are in a joint config with VOTER_DEMOTING_LEARNER, and ignore it
	// here, we may propose another change. This is harmless since
	// Replica.AdminRelocateRange calls
	// Replica.maybeLeaveAtomicChangeReplicasAndRemoveLearners which will remove
	// the VOTER_DEMOTING_LEARNER and exit the joint config first.
	if isVoter(replicaType) {
		acb.replicas[voterIndex] = append(
			acb.replicas[voterIndex], storeAndLocality{storeID, locality})
	} else if isNonVoter(replicaType) {
		acb.replicas[nonVoterIndex] = append(
			acb.replicas[nonVoterIndex], storeAndLocality{storeID, locality})
	}
}

// stringInterner maps locality tiers and constraint strings to unique ints
// (code), so that we don't need to do expensive string equality comparisons.
// There is no removal from this map. It is very unlikely that new localities
// or constraints will be created fast enough for removal to be needed to
// lower memory consumption or to prevent overflow. The empty string is
// assigned code 0.
type stringInterner struct {
	stringToCode map[string]stringCode
	codeToString []string
}

type stringCode uint32

const emptyStringCode stringCode = 0

func newStringInterner() *stringInterner {
	si := &stringInterner{stringToCode: map[string]stringCode{}}
	si.stringToCode[""] = emptyStringCode
	si.codeToString = append(si.codeToString, "")
	return si
}

func (si *stringInterner) toCode(s string) stringCode {
	code, ok := si.stringToCode[s]
	if !ok {
		n := len(si.stringToCode)
		if n == math.MaxUint32 {
			panic("overflowed stringInterner")
		}
		code = stringCode(n)
		si.stringToCode[s] = code
		si.codeToString = append(si.codeToString, s)
	}
	return code
}

func (si *stringInterner) toString(code stringCode) string {
	return si.codeToString[code]
}

func (si *stringInterner) internConstraintsConj(constraints []roachpb.Constraint) constraintsConj {
	if len(constraints) == 0 {
		return nil
	}
	var rv []internedConstraint
	for i := range constraints {
		rv = append(rv, internedConstraint{
			typ:   constraints[i].Type,
			key:   si.toCode(constraints[i].Key),
			value: si.toCode(constraints[i].Value),
		})
	}
	sort.Slice(rv, func(j, k int) bool {
		return rv[j].less(rv[k])
	})
	j := 0
	// De-dup conjuncts in the conjunction.
	for k := 1; k < len(rv); k++ {
		if !(rv[j] == rv[k]) {
			j++
			rv[j] = rv[k]
		}
	}
	rv = rv[:j+1]
	return rv
}

// localityTierInterner maps Tier.Value strings to unique ints, so that we
// don't need to do expensive string equality comparisons.
type localityTierInterner struct {
	si *stringInterner
}

func newLocalityTierInterner(interner *stringInterner) *localityTierInterner {
	return &localityTierInterner{si: interner}
}

// intern is called occasionally, when we have a new store, or the locality of
// a store changes.
func (lti *localityTierInterner) intern(locality roachpb.Locality) localityTiers {
	var lt localityTiers
	var buf strings.Builder
	for i := range locality.Tiers {
		code := lti.si.toCode(locality.Tiers[i].Value)
		lt.tiers = append(lt.tiers, code)
		fmt.Fprintf(&buf, "%d:", code)
	}
	lt.str = buf.String()
	return lt
}

func (lti *localityTierInterner) unintern(lt localityTiers) roachpb.Locality {
	var locality roachpb.Locality
	for _, t := range lt.tiers {
		locality.Tiers = append(locality.Tiers, roachpb.Tier{Value: lti.si.toString(t)})
	}
	return locality
}

// localityTiers encodes a locality value hierarchy, represented by codes
// from an associated stringInterner.
//
// Note that CockroachDB operators must use matching *keys*[1] across all nodes
// in each deployment, so we only need to deal with a slice of locality
// *values*.
//
// [1]: https://www.cockroachlabs.com/docs/stable/cockroach-start#locality
type localityTiers struct {
	tiers []stringCode
	// str is useful as a map key for caching computations.
	str string
}

func (l localityTiers) diversityScore(other localityTiers) float64 {
	length := len(l.tiers)
	lengthOther := len(other.tiers)
	if lengthOther < length {
		length = lengthOther
	}
	for i := 0; i < length; i++ {
		if l.tiers[i] != other.tiers[i] {
			return float64(length-i) / float64(length)
		}
	}
	if length != lengthOther {
		return roachpb.MaxDiversityScore / float64(length+1)
	}
	return 0
}

const notMatchedLeasePreferenceIndex = math.MaxInt32

// matchedLeasePreferenceIndex returns the index of the lease preference that
// matches, else notMatchedLeasePreferenceIndex
func matchedLeasePreferenceIndex(
	storeID roachpb.StoreID,
	leasePreferences []internedLeasePreference,
	constraintMatcher storeMatchesConstraintInterface,
) int32 {
	if len(leasePreferences) == 0 {
		return 0
	}
	for j := range leasePreferences {
		if constraintMatcher.storeMatches(storeID, leasePreferences[j].constraints) {
			return int32(j)
		}
	}
	return notMatchedLeasePreferenceIndex
}

// Avoid unused lint errors.

var _ = normalizedSpanConfig{}
var _ = makeNormalizedSpanConfig
var _ = normalizeConstraints
var _ = analyzedConstraints{}
var _ = rangeAnalyzedConstraints{}
var _ = releaseRangeAnalyzedConstraints
var _ = constraintsDisj{}
var _ = analyzeConstraintsBuf{}
var _ = storeAndLocality{}
var _ = localityTierInterner{}
var _ = localityTiers{}
var _ = storeSet{}

var _ = constraintsDisj{}.hash
var _ = constraintsDisj{}.isEqual
var _ = (&stringInterner{}).toString

func init() {
	var ac analyzedConstraints
	var _ = ac.clear
	var _ = ac.isEmpty

	var rac rangeAnalyzedConstraints
	var _ = rac.stateForInit
	var _ = rac.finishInit
	var _ = rac.notEnoughVoters
	var _ = rac.candidatesToConvertFromNonVoterToVoter
	var _ = rac.constraintsForAddingVoter
	var _ = rac.notEnoughNonVoters
	var _ = rac.candidatesToConvertFromVoterToNonVoter
	var _ = rac.constraintsForAddingNonVoter
	var _ = rac.candidatesForRoleSwapForConstraints
	var _ = rac.candidatesToRemove
	var _ = rac.candidatesVoterConstraintsUnsatisfied
	var _ = rac.candidatesNonVoterConstraintsUnsatisfied
	var _ = rac.candidatesToReplaceVoterForRebalance
	var _ = rac.candidatesToReplaceNonVoterForRebalance

	var acb analyzeConstraintsBuf
	var _ = acb.tryAddingStore
	var _ = acb.clear

	var ltt localityTierInterner
	var _ = ltt.intern
	var lt localityTiers
	var _ = lt.diversityScore

	var pl storeSet
	var _ = pl.union
	var _ = pl.intersect
	var _ = pl.isEqual
	var _ = pl.remove
	var _ = pl.insert
	var _ = pl.contains
	var _ = pl.hash

	var _ = storeAndLocality{StoreID: 0, localityTiers: localityTiers{}}
}
