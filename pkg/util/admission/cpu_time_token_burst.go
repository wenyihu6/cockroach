// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "github.com/cockroachdb/redact"

// cpuTimeBurstBucket is a per-tenant token bucket that determines whether a
// tenant qualifies for burst priority. If a tenant qualifies, two things
// happen:
//
//  1. In the WorkQueue, its work sorts above the work of tenants that don't
//     qualify for burst priority.
//  2. It has access to more CPU time tokens than tenants that don't qualify
//     (see cpu_time_token_granter.go for more).
//
// These two things are related. Since canBurst tenants have access to more
// CPU time than noBurst tenants, it is important that work from a
// canBurst tenant always sorts before work from a noBurst tenant -- else
// available capacity is left on the table.
//
// The bucket works as follows:
//   - Tokens are added periodically via refill(), called by
//     cpuTimeTokenAllocator.
//   - Tokens are deducted when work is admitted, etc. via adjust().
//   - A tenant qualifies for burst (canBurst) when bucket is > 90% full.
//   - The bucket can go negative (down to -capacity/4) to allow recovery.
//
// The bucket capacity and refill rate are derived from the canBurst
// (100% CPU) rate, scaled per-tenant by burstLimitFrac (= CPU_MIN
// fraction) in refillBurstBuckets. A tenant with CPU_MIN=10% gets
// 10% of the canBurst rate, so its burst bucket breaks even at
// exactly 10% CPU usage.
type cpuTimeBurstBucket struct {
	tokens   int64
	capacity int64
	// disabled is true when mode != usesCPUTimeTokens, causing
	// burstQualification to always return noBurst. This effectively
	// disables the burstQualification functionality.
	disabled bool
	// burstLimitFrac controls per-tenant burst qualification behavior.
	// A value >= 1.0 means the tenant always qualifies for burst
	// (FULLY_UTILIZE resource groups). A value in (0, 1) means the
	// tenant's burst bucket capacity and refill rate are scaled by
	// this fraction in refillBurstBuckets, making it harder to qualify.
	// The default (from defaultBurstLimitFrac) is 0.25.
	burstLimitFrac float64
}

func (m *cpuTimeBurstBucket) init(capacity int64, disabled bool, burstLimitFrac float64) {
	// The bucket of a new tenant is inited full. This implies that
	// a tenant can burst when its work first appears on a KV node.
	// After <= 1s, the bucket state should track the usage of the
	// tenant accurately.
	//
	// The caller is responsible for scaling capacity by burstLimitFrac
	// before calling init. See newTenantInfo.
	*m = cpuTimeBurstBucket{
		tokens:         capacity,
		capacity:       capacity,
		disabled:       disabled,
		burstLimitFrac: burstLimitFrac,
	}
}

// burstQualification returns whether this tenant qualifies for burst
// priority. See the comments above cpuTimeBurstBucket for more.
func (m *cpuTimeBurstBucket) burstQualification() burstQualification {
	if m.disabled {
		return noBurst
	}
	// FULLY_UTILIZE resource groups (burstLimitFrac >= 1.0) always
	// qualify for burst. This is the primary mechanism by which
	// FULLY_UTILIZE groups get priority — they always check the
	// canBurst granter bucket.
	if m.burstLimitFrac >= 1.0 {
		return canBurst
	}
	// Note that at CRDB startup time, the capacity that is passed into
	// cpuTimeBurstBucket.init will be zero, until 1ms passes, and the
	// first call to refillBurstBuckets is made by cpuTimeTokenAllocator.
	// So it is important that this code does not assume that m.capacity
	// is non-zero. (There is a test for this case in
	// TestCPUTimeTokenBurst.)
	if m.tokens > (m.capacity*9)/10 {
		return canBurst
	}
	return noBurst
}

// adjust modifies the token count by delta. A positive delta adds tokens
// (e.g., when returning unused resources), while a negative delta removes
// tokens (e.g., when work is admitted). The token count is capped at
// capacity but has no floor here. The floor is enforced in refill, which
// is called every 1ms. There is no need to enforce the floor more
// frequently than that.
func (m *cpuTimeBurstBucket) adjust(delta int64) {
	m.tokens += delta
	m.tokens = min(m.tokens, m.capacity)
}

// refill adds tokens to the bucket and updates capacity. This is called
// periodically by cpuTimeTokenAllocator (every 1ms). The token count is capped
// at capacity and floored at -capacity/4. The negative floor allows tenants
// that have gone into debt (consumed more than their share) to recover over
// time rather than being disqualified from bursting for arbitrarily long periods
// of time.
func (m *cpuTimeBurstBucket) refill(toAdd int64, capacity int64) {
	// The caller is responsible for scaling toAdd and capacity by
	// burstLimitFrac before calling refill. See refillBurstBuckets.
	m.capacity = capacity
	m.adjust(toAdd)
	m.tokens = max(m.tokens, -m.capacity/4)
}

func (m *cpuTimeBurstBucket) String() string {
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m *cpuTimeBurstBucket) SafeFormat(s redact.SafePrinter, _ rune) {
	var fullness float64
	if m.capacity > 0 {
		fullness = float64(m.tokens) / float64(m.capacity) * 100
	}
	s.Printf("fullness=%.1f%% tokens=%d capacity=%d qual=%s burstLimitFrac=%.2f",
		fullness, m.tokens, m.capacity, m.burstQualification(), m.burstLimitFrac)
}
