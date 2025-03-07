package multiregionlatency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// PolicyLocalityKey uniquely identifies a closed timestamp entry by its policy and locality.
// The locality can be UNDEFINED or a specific locality comparison type.
type PolicyLocalityKey struct {
	Policy   roachpb.RangeClosedTimestampPolicy
	Locality roachpb.LocalityComparisonType
}

// ToIndex maps a PolicyLocalityKey to a unique index in the closed timestamp array.
// The mapping works as follows:
// - First numOfPolicies indices are reserved for (Policy, UNDEFINED) pairs
// - Remaining indices are for (LEAD_FOR_GLOBAL_READS, Locality) pairs
// Examples:
// (LAG, UNDEFINED) -> 0
// (GLOBAL, UNDEFINED) -> 1
// (GLOBAL, CROSS_REGION) -> 2
// (GLOBAL, CROSS_ZONE) -> 3
// (GLOBAL, SAME_ZONE) -> 4
func (k PolicyLocalityKey) ToIndex() int {
	if k.Locality == roachpb.LocalityComparisonType_UNDEFINED {
		return int(k.Policy)
	}
	// For non-UNDEFINED localities, index starts after policy indices
	// and maps directly to locality enum value (skipping UNDEFINED)
	return int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY) + int(k.Locality) - 1
}

// Constants for array sizing and indexing
const (
	// numOfPolicies is the number of possible closed timestamp policies
	numOfPolicies = int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY)

	// numOfLocalities is the number of locality comparison types, excluding UNDEFINED
	numOfLocalities = int(roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE - 1)

	// numOfEntries is the total size needed for the closed timestamp array
	numOfEntries = numOfPolicies + numOfLocalities
)

// EntryIdx represents an index into the closed timestamp array
type EntryIdx int

// valid returns true if the index is within bounds
func (idx EntryIdx) valid() bool {
	return idx >= 0 && idx < EntryIdx(numOfEntries)
}

// ToPolicyLocalityKey converts an array index back to a PolicyLocalityKey
func (idx EntryIdx) ToPolicyLocalityKey() PolicyLocalityKey {
	if !idx.valid() {
		log.Errorf(context.Background(), "programming error: invalid index %d", idx)
	}

	if idx < EntryIdx(numOfPolicies) {
		// First numOfPolicies indices map to (Policy, UNDEFINED)
		return PolicyLocalityKey{
			Policy:   roachpb.RangeClosedTimestampPolicy(idx),
			Locality: roachpb.LocalityComparisonType_UNDEFINED,
		}
	}

	// Remaining indices map to (LEAD_FOR_GLOBAL_READS, Locality)
	// Add 1 to skip over UNDEFINED when mapping back to LocalityComparisonType
	localityVal := roachpb.LocalityComparisonType(idx - EntryIdx(numOfPolicies) + 1)
	return PolicyLocalityKey{
		Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
		Locality: localityVal,
	}
}

// PolicyLocalityToTimestampMap maps policy+locality combinations to timestamps.
// It maintains an array of timestamps indexed by policy and locality combinations.
// For example:
type PolicyLocalityToTimestampMap struct {
	// timestamps maps policy+locality combinations to timestamps.
	// The mapping is done by converting PolicyLocalityKey to an array index.
	Timestamps [numOfEntries]hlc.Timestamp
}

// Get returns the timestamp for a given policy and locality combination.
func (m *PolicyLocalityToTimestampMap) Get(
	policy roachpb.RangeClosedTimestampPolicy, locality roachpb.LocalityComparisonType,
) hlc.Timestamp {
	key := PolicyLocalityKey{Policy: policy, Locality: locality}.ToIndex()
	return m.Timestamps[key]
}

// Len returns the number of possible policy+locality combinations.
func (m *PolicyLocalityToTimestampMap) Len() int {
	return numOfEntries
}

// Reset clears all timestamps in the map.
func (m *PolicyLocalityToTimestampMap) Reset() {
	m.Timestamps = [numOfEntries]hlc.Timestamp{}
}

// Set stores a timestamp for a given policy and locality combination.
func (m *PolicyLocalityToTimestampMap) Set(
	policy roachpb.RangeClosedTimestampPolicy, locality roachpb.LocalityComparisonType, ts hlc.Timestamp,
) {
	key := PolicyLocalityKey{Policy: policy, Locality: locality}.ToIndex()
	m.Timestamps[key] = ts
}
