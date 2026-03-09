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
	cg.parent.returnGrantForTier(cg.tier, count)
}

// tookWithoutPermission implements granter.
func (cg *cpuTimeTokenChildGranter) tookWithoutPermission(count int64) {
	cg.parent.tookWithoutPermissionForTier(cg.tier, count)
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
	numTiers int
	// useIndependentBudgets controls the token deduction strategy:
	//   false (legacy): every request deducts from ALL tiers' buckets,
	//     creating a priority hierarchy where higher tiers eat into lower
	//     tiers' budgets.
	//   true (resource groups): each request only deducts from its own
	//     tier's buckets, giving each group an independent CPU budget.
	//     Work-conserving behavior is achieved through the canBurst bucket
	//     which has higher capacity for MaxCPU=true groups.
	useIndependentBudgets bool
	requester             []requester
	mu                    struct {
		syncutil.Mutex
		// When useIndependentBudgets is false:
		//   Invariant #1: For any two buckets A & B, if A has a lower ordinal
		//   resourceTier, then A must have more tokens than B.
		//   Invariant #2: For any two buckets A & B with same resourceTier,
		//   if A has lower ordinal burstQualification, A has more tokens.
		// When useIndependentBudgets is true:
		//   Each tier's buckets are independent. No cross-tier invariants.
		buckets    [][numBurstQualifications]tokenBucket
		tokensUsed int64
		// perTierTokensUsed tracks token consumption per resource tier
		// within the current interval. Reset by resetTokensUsedInInterval.
		perTierTokensUsed []int64
	}
}

// newCPUTimeTokenGranter creates a cpuTimeTokenGranter with the specified
// number of resource tiers.
func newCPUTimeTokenGranter(numTiers int, useIndependentBudgets bool) *cpuTimeTokenGranter {
	stg := &cpuTimeTokenGranter{
		numTiers:              numTiers,
		useIndependentBudgets: useIndependentBudgets,
		requester:             make([]requester, numTiers),
	}
	stg.mu.buckets = make([][numBurstQualifications]tokenBucket, numTiers)
	stg.mu.perTierTokensUsed = make([]int64, numTiers)
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
	if stg.useIndependentBudgets {
		stg.tookFromTierLocked(tier, count)
	} else {
		stg.tookFromAllLocked(count)
	}
	if int(tier) < len(stg.mu.perTierTokensUsed) {
		stg.mu.perTierTokensUsed[tier] += count
	}
	return true
}

// returnGrantForTier is the tier-aware version of returnGrant.
func (stg *cpuTimeTokenGranter) returnGrantForTier(tier resourceTier, count int64) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if stg.useIndependentBudgets {
		stg.tookFromTierLocked(tier, -count)
	} else {
		stg.tookFromAllLocked(-count)
	}
	// count must be positive. Thus above always adds tokens to the buckets.
	stg.grantUntilNoWaitingRequestsLocked()
}

// tookWithoutPermissionForTier is the tier-aware version of
// tookWithoutPermission.
func (stg *cpuTimeTokenGranter) tookWithoutPermissionForTier(
	tier resourceTier, count int64,
) {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	if stg.useIndependentBudgets {
		stg.tookFromTierLocked(tier, count)
	} else {
		stg.tookFromAllLocked(count)
	}
}

func (stg *cpuTimeTokenGranter) tookWithoutPermissionLocked(count int64) {
	if stg.useIndependentBudgets {
		// In independent mode, tookWithoutPermission is called without
		// knowing which tier. We can't deduct properly, so this is only
		// used for the legacy path.
		stg.tookFromAllLocked(count)
	} else {
		stg.tookFromAllLocked(count)
	}
}

// tookFromAllLocked deducts count from ALL tiers' buckets (legacy behavior).
func (stg *cpuTimeTokenGranter) tookFromAllLocked(count int64) {
	stg.mu.tokensUsed += count
	for tier := range stg.mu.buckets {
		for qual := range stg.mu.buckets[tier] {
			stg.mu.buckets[tier][qual].tokens -= count
		}
	}
}

// tookFromTierLocked deducts count only from the specified tier's buckets
// (resource group mode). This gives each group an independent CPU budget.
func (stg *cpuTimeTokenGranter) tookFromTierLocked(tier resourceTier, count int64) {
	stg.mu.tokensUsed += count
	for qual := range stg.mu.buckets[tier] {
		stg.mu.buckets[tier][qual].tokens -= count
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
			if stg.useIndependentBudgets {
				// In independent mode, each tier has its own budget.
				// This tier is exhausted, but others may still have
				// tokens. Continue checking.
				continue
			}
			// In legacy mode, tiers share a priority hierarchy.
			// If a higher-priority tier is exhausted, all lower
			// tiers are too.
			return false
		}
		tokens := stg.requester[tier].granted(noGrantChain)
		if tokens == 0 {
			continue
		}
		if stg.useIndependentBudgets {
			stg.tookFromTierLocked(resourceTier(tier), tokens)
		} else {
			stg.tookFromAllLocked(tokens)
		}
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
	for i := range stg.mu.perTierTokensUsed {
		stg.mu.perTierTokensUsed[i] = 0
	}
	return tokensUsed
}

// getPerTierTokensUsed returns a snapshot of per-tier token usage since
// the last reset. Useful for observability and metrics.
func (stg *cpuTimeTokenGranter) getPerTierTokensUsed() []int64 {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	result := make([]int64, len(stg.mu.perTierTokensUsed))
	copy(result, stg.mu.perTierTokensUsed)
	return result
}

// getPerTierTokenBalances returns a snapshot of per-tier token balances
// for the noBurst bucket. Useful for observability.
func (stg *cpuTimeTokenGranter) getPerTierTokenBalances() []int64 {
	stg.mu.Lock()
	defer stg.mu.Unlock()
	result := make([]int64, len(stg.mu.buckets))
	for i := range stg.mu.buckets {
		result[i] = stg.mu.buckets[i][noBurst].tokens
	}
	return result
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
