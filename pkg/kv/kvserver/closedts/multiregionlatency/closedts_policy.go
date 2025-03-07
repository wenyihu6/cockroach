package multiregionlatency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// PolicyLocalityKey uniquely identifies a closed timestamp entry by its policy and locality
type PolicyLocalityKey struct {
	Policy   roachpb.RangeClosedTimestampPolicy
	Locality roachpb.LocalityComparisonType
}

func (k PolicyLocalityKey) ToIndex() int {
	if k.Locality == roachpb.LocalityComparisonType_UNDEFINED {
		return int(k.Policy)
	}
	return int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY) + int(k.Locality)
}

// ClosedTimestampEntries is the maximum number of entries in the closed timestamp map.
// It's the sum of the number of possible policies and locality comparison types.
const numOfEntries = int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY) + int(roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE)

type EntryIdx int

func (key EntryIdx) valid() bool {
	return key >= 0 && key < EntryIdx(numOfEntries)
}

func (key EntryIdx) ToPolicyLocalityKey() PolicyLocalityKey {
	if !key.valid() {
		log.Fatalf(context.Background(), "invalid key %d", key)
	}
	if key < EntryIdx(roachpb.MAX_CLOSED_TIMESTAMP_POLICY) {
		return PolicyLocalityKey{Policy: roachpb.RangeClosedTimestampPolicy(key), Locality: roachpb.LocalityComparisonType_UNDEFINED}
	}
	return PolicyLocalityKey{Policy: roachpb.LEAD_FOR_GLOBAL_READS, Locality: roachpb.LocalityComparisonType(key - EntryIdx(roachpb.MAX_CLOSED_TIMESTAMP_POLICY))}
}

// ClosedTimestamps tracks closed timestamps for different policy and locality combinations.
type ClosedTimestamps struct {
	Entries [numOfEntries]hlc.Timestamp
}

// GetClosedTsForPolicyAndLocality returns the closed timestamp for a given policy and locality.
func (ct *ClosedTimestamps) GetClosedTsForPolicyAndLocality(
	policy roachpb.RangeClosedTimestampPolicy, locality roachpb.LocalityComparisonType,
) hlc.Timestamp {
	key := PolicyLocalityKey{Policy: policy, Locality: locality}.ToIndex()
	return ct.Entries[key]
}

// Len returns the number of closed timestamp entries.
func (ct *ClosedTimestamps) Len() int {
	return numOfEntries
}

// Reset clears all closed timestamp entries.
func (ct *ClosedTimestamps) Reset() {
	ct.Entries = [numOfEntries]hlc.Timestamp{}
}

// SetLastClosedTsForPolicyAndLocality sets the closed timestamp for a given policy and locality.
func (ct *ClosedTimestamps) SetLastClosedTsForPolicyAndLocality(
	policy roachpb.RangeClosedTimestampPolicy, locality roachpb.LocalityComparisonType, ts hlc.Timestamp,
) {
	// Make sure that the contract here is good
	key := PolicyLocalityKey{Policy: policy, Locality: locality}.ToIndex()
	ct.Entries[key] = ts
}
