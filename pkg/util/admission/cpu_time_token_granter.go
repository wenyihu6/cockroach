// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/olekukonko/tablewriter"
)

// resourceTier specifies the tier of a resource group, in descending levels
// of importance. That is, tier 0 is the most important. The token bucket
// sizes must be such that the non-burstable token bucket size of tier-i must
// be greater than the burstable token bucket size of tier-(i+1); see
// cpuTimeTokenGranter for details on this.
//
// The tier determination for a request happens at a layer outside the
// admission package.
//
// NB: Inter-tenant fair sharing only works within a tier.
//
// With resource groups, each resource group maps to a resourceTier. The
// number of tiers is no longer fixed at 2 but determined by the number of
// configured resource groups.
type resourceTier uint8

const (
	// systemTenant is the tier associated with all system tenant work.
	// Retained for backward compatibility with the 2-tier serverless setup.
	systemTenant resourceTier = iota
	// appTenant is the tier associated with all app tenant work.
	// Retained for backward compatibility with the 2-tier serverless setup.
	appTenant
	// numDefaultResourceTiers is the default number of resource tiers when
	// no resource group registry is configured.
	numDefaultResourceTiers = 2
)

// cpuTimeTokenChildGranter implements granter. It stores resourceTier and
// proxies to cpuTimeTokenGranter. See the declaration comment for
// cpuTimeTokenGranter for more details.
//
// Each "child" granter is paired with a requester, since the requester (in
// practice, a WorkQueue for a certain resourceTier) does not need to know
// about the others. An alternative would be to make resourceTier an argument
// to the various granter methods, but this approach seems cleaner.
type cpuTimeTokenChildGranter struct {
	tier   resourceTier
	parent *cpuTimeTokenGranter
}

var _ granter = &cpuTimeTokenChildGranter{}

// tryGet implements granter.
func (cg *cpuTimeTokenChildGranter) tryGet(qual burstQualification, count int64) bool {
	return cg.parent.tryGet(cg.tier, qual, count)
}

// returnGrant implements granter.
func (cg *cpuTimeTokenChildGranter) returnGrant(count int64) {
	cg.parent.returnGrant(count)
}

// tookWithoutPermission implements granter.
func (cg *cpuTimeTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermission(count)
}

// continueGrantChain implements granter.
func (cg *cpuTimeTokenChildGranter) continueGrantChain(grantChainID grantChainID) {
	// Ignore since grant chains are not used.
}

// cpuTimeTokenGranter uses token buckets to limit CPU usage. There is one
// token bucket per (resourceTier, burstQualification) pair. Requests are only
// admitted (tryGet returns true) if the bucket for the type of request has
// positive tokens. Before a request is admitted, tokens are deducted from all
// buckets, not just the bucket that was checked initially. This enables
// setting up a hierarchy of types of requests, where some types can use more
// CPU than others.
//
// For example, on an 8 vCPU machine with 2 resource groups, it might be set
// up like this:
//
// - Burstable group-0 work -> 6 seconds of CPU time per second
// - Non-burstable group-0 work -> 5 seconds of CPU time per second
// - Burstable group-1 work -> 2 seconds of CPU time per second
// - Non-burstable group-1 work -> 1 seconds of CPU time per second
//
// With N resource groups, each group gets its own row of token buckets with
// rates proportional to its weight. Higher-weighted groups get larger token
// buckets and higher refill rates.
//
// Note that cpuTimeTokenGranter does not handle replenishing the buckets.
type cpuTimeTokenGranter struct {
	numTiers  int
	requester []requester
	mu        struct {
		syncutil.Mutex
		// Invariant #1: For any two buckets A & B, if A has a lower ordinal
		// resourceTier, then A must have more tokens than B.
		// Invariant #2: For any two buckets A & B, if A & B have the same
		// resourceTier, and if A has a lower ordinal burstQualification,
		// then A must have more tokens than B.
		//
		// Since admission deducts from all buckets, these invariants are true,
		// so long as token bucket replenishing respects it also.
		buckets    [][numBurstQualifications]tokenBucket
		tokensUsed int64
	}
}

// newCPUTimeTokenGranter creates a cpuTimeTokenGranter with the specified
// number of resource tiers.
func newCPUTimeTokenGranter(numTiers int) *cpuTimeTokenGranter {
	stg := &cpuTimeTokenGranter{
		numTiers:  numTiers,
		requester: make([]requester, numTiers),
	}
	stg.mu.buckets = make([][numBurstQualifications]tokenBucket, numTiers)
	return stg
}

type tokenBucket struct {
	tokens int64
}

func (stg *cpuTimeTokenGranter) String() string {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	var buf strings.Builder
	tw := tablewriter.NewWriter(&buf)
	hdrs := [numBurstQualifications + 1]string{}
	hdrs[0] = "cpuTTG"
	for gk := canBurst; gk < numBurstQualifications; gk++ {
		hdrs[1+gk] = gk.String()
	}
	tw.SetAlignment(tablewriter.ALIGN_LEFT)
	tw.SetAutoFormatHeaders(false)
	tw.SetBorder(false)
	tw.SetColumnSeparator("")
	tw.SetHeader(hdrs[:])
	tw.SetHeaderLine(false)
	tw.SetNoWhiteSpace(true)
	tw.SetTablePadding(" ")
	tw.SetTrimWhiteSpaceAtEOL(true)

	for tier := 0; tier < stg.numTiers; tier++ {
		row := [1 + numBurstQualifications]string{}
		row[0] = "tier" + strconv.Itoa(tier)
		for gk := canBurst; gk < numBurstQualifications; gk++ {
			row[gk+1] = fmt.Sprint(stg.mu.buckets[tier][gk].tokens)
		}
		tw.Append(row[:])
	}
	tw.Render()
	return buf.String()
}

// tryGet is the helper for implementing granter.tryGet.
func (stg *cpuTimeTokenGranter) tryGet(
	tier resourceTier, qual burstQualification, count int64,
) bool {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if stg.mu.buckets[tier][qual].tokens <= 0 {
		return false
	}
	stg.tookWithoutPermissionLocked(count)
	return true
}

// returnGrant is the helper for implementing granter.returnGrant.
func (stg *cpuTimeTokenGranter) returnGrant(count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	stg.tookWithoutPermissionLocked(-count)
	// count must be positive. Thus above always adds tokens to the buckets.
	// Thus returnGrant should always attempt to grant admission to waiting
	// requests.
	stg.grantUntilNoWaitingRequestsLocked()
}

// tookWithoutPermission is the helper for implementing
// granter.tookWithoutPermission.
func (stg *cpuTimeTokenGranter) tookWithoutPermission(count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	stg.tookWithoutPermissionLocked(count)
}

func (stg *cpuTimeTokenGranter) tookWithoutPermissionLocked(count int64) {
	stg.mu.tokensUsed += count
	for tier := range stg.mu.buckets {
		for qual := range stg.mu.buckets[tier] {
			stg.mu.buckets[tier][qual].tokens -= count
		}
	}
}

// grantUntilNoWaitingRequestsLocked grants admission to all queued requests
// that can be granted, given the current state of the token buckets, etc.
// It prioritizes requesters from higher class work in the sense of
// resourceTier. That is, multiple waiting tier-0 requests will be granted
// before a single tier-1 request.
func (stg *cpuTimeTokenGranter) grantUntilNoWaitingRequestsLocked() {
	for stg.tryGrantLocked() {
	}
}

// tryGrantLocked attempts to grant admission to a single queued request.
// It prioritizes requesters from higher class work, in the sense of
// resourceTier.
func (stg *cpuTimeTokenGranter) tryGrantLocked() bool {
	for tier := range stg.requester {
		if stg.requester[tier] == nil {
			continue
		}
		hasWaitingRequests, qual := stg.requester[tier].hasWaitingRequests()
		if !hasWaitingRequests {
			continue
		}
		if stg.mu.buckets[tier][qual].tokens <= 0 {
			// tryGrantLocked does not need to continue here, since there
			// are no more requests to grant. The detailed reason for this
			// is:
			//
			// - stg.requester is ordered by resourceTier.
			// - Given two buckets A & B, if A is for a lower ordinal
			//   resourceTier, more tokens will be in bucket A than bucket
			//   B (see cpuTimeTokenGranter for more on this invariant).
			// - Thus, if no tokens in A, there are no tokens in B.
			return false
		}
		tokens := stg.requester[tier].granted(noGrantChain)
		if tokens == 0 {
			// Did not accept grant.
			continue
		}
		stg.tookWithoutPermissionLocked(tokens)
		return true
	}
	return false
}

// resetTokensUsedInInterval resets the tracked used tokens to zero. The
// previous value is returned.
func (stg *cpuTimeTokenGranter) resetTokensUsedInInterval() int64 {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	tokensUsed := stg.mu.tokensUsed
	stg.mu.tokensUsed = 0
	return tokensUsed
}

// refill adds toAdd tokens to the corresponding buckets, while respecting
// the capacity info stored in bucketCapacities. That is, tokens that would
// bring the bucket above capacity will be discarded instead. refill attempts
// to grant admission to waiting requests in case where tokens are added to
// some bucket.
func (stg *cpuTimeTokenGranter) refill(toAdd tokenCounts, bucketCapacities capacities) {
	stg.mu.Lock()
	defer stg.mu.Unlock()

	var shouldGrant bool
	for wc := range stg.mu.buckets {
		for kind := range stg.mu.buckets[wc] {
			if toAdd[wc][kind] > 0 {
				shouldGrant = true
			}
			newTokenCount := stg.mu.buckets[wc][kind].tokens + toAdd[wc][kind]
			if newTokenCount > bucketCapacities[wc][kind] {
				newTokenCount = bucketCapacities[wc][kind]
			}
			stg.mu.buckets[wc][kind].tokens = newTokenCount
		}
	}

	// Grant if tokens are added to any of the buckets.
	if shouldGrant {
		stg.grantUntilNoWaitingRequestsLocked()
	}
}
