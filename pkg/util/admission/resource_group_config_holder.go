// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ResourceGroupConfig holds per-resource-group configuration in
// Resource Manager mode.
type ResourceGroupConfig struct {
	// Weight controls the group's share of fair-shared resources.
	// computeDerived caps and scales it via groupWeightCap into
	// DerivedGroupConfig.Weight; that scaled value is what
	// WorkQueue.ApplyResourceGroupConfig pushes onto
	// groupWeights.active and onto each existing groupInfo.weight.
	Weight uint32
	// MaxCPU=true means the group always qualifies for burst
	// regardless of its burst bucket level. Used for groups that
	// should be allowed to consume up to full node CPU on demand.
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the configuration used until an
// explicit SetResourceGroupConfig call replaces it. The two
// hardcoded groups (high/low) match the two outputs of
// priorityToResourceGroup.
var defaultRMResourceGroupConfig = map[uint64]ResourceGroupConfig{
	highResourceGroupID: {Weight: 1, MaxCPU: true},
	lowResourceGroupID:  {Weight: 1, MaxCPU: false},
}

// DerivedGroupConfig is the per-resource-group state derived from
// ResourceGroupConfig. It is what WorkQueue consumes when applying
// config: a scaled Weight (already capped per groupWeightCap, with
// the same rules as SetTenantWeights), MaxCPU, and BurstFrac.
//
// BurstFrac is the group's share of burst bucket refill: 1.0 if
// MaxCPU is true (the group can burst to full node CPU), else
// Weight / sum-of-weights across all configured groups, or 0 when
// the configured set has zero total weight (no burst budget).
type DerivedGroupConfig struct {
	Weight    uint32
	MaxCPU    bool
	BurstFrac float64
}

// defaultDerivedGroupConfig is the safety fallback returned by
// ResourceGroupConfigHolder.GetDerivedOrDefault for IDs that aren't
// explicitly configured. It deliberately mirrors the lowResourceGroupID
// seed in shape (Weight=1, MaxCPU=false) but pins BurstFrac=0 so an
// unknown-ID group gets enough weight to compete in fair sharing yet
// receives no per-group burst budget. In practice RM admits route via
// priorityToResourceGroup, which always returns a configured ID, so
// this fallback only fires under truly unexpected conditions
// (test setups that bypass the configured map, or future code paths
// that hand WorkQueue a novel ID).
var defaultDerivedGroupConfig = DerivedGroupConfig{
	Weight:    defaultGroupWeight,
	MaxCPU:    false,
	BurstFrac: 0,
}

// ResourceGroupConfigHolder owns the source-of-truth resource group
// configuration for Resource Manager mode and the derived state that
// WorkQueue consumes when applying config (scaled weights, burstFracs).
//
// Lifecycle: constructed once at cpuTimeTokenGrantCoordinator startup,
// seeded with defaultRMResourceGroupConfig, then injected into
// WorkQueue via workQueueOptions.configHolder. Mutated by
// SetResourceGroupConfig (admin DDL); read by WorkQueue's lazy group
// creation in RM mode (one read per cache miss in Admit) and by
// mode-swap synchronization (one snapshot per false→true transition
// or in-RM-mode Set).
//
// === Lock ordering ===
//
// The holder owns its own mutex. Callers may invoke Set, Snapshot,
// or GetDerivedOrDefault while holding WorkQueue.mu - the holder
// never reaches back into WorkQueue. WorkQueue.mu must NEVER be
// acquired while holding the holder's mutex; that ordering invariant
// is enforced implicitly because no code path inside the holder
// reaches outside it.
//
// === Why a dedicated holder owns this storage ===
//
// Earlier designs put the source config on WorkQueue alongside the
// derived state it produces (groupInfo.weight, cpuTimeBurstBucket.maxCPU,
// groupWeights.active). Separating it here trades a small amount of
// indirection for three concrete properties:
//
//   - Source-of-truth lives in one place. WorkQueue stores only what
//     it actively reads on hot paths (groupInfo.burstFrac for refill,
//     groupInfo.weight for fair-share ordering). The Weight + MaxCPU
//     pair from ResourceGroupConfig is no longer copied onto
//     WorkQueue's mutex-guarded state; it lives once on the holder.
//   - GC of configured-but-idle groups is safe. WorkQueue's GC sweep
//     no longer needs a "skip configured" exception because the
//     holder is consulted by lazy-create on the next admit, which
//     correctly recovers Weight/MaxCPU/BurstFrac without any pre-
//     creation invariant.
//   - Lock isolation is real. Admin DDL via SetResourceGroupConfig
//     touches the holder under its own lock; WorkQueue.mu is taken
//     separately only when applying derived state. Concurrent admits
//     contend with apply only during the (microsecond-scale) apply
//     critical section, not during the precompute pass.
//
// === Alternative owners considered and rejected ===
//
// (1) cpuTimeTokenAllocator: an earlier design (see git history for
// baf469d24cf) put the config on the allocator as an
// atomic.Pointer[map] plus an atomic.Bool dirty flag. The coord
// then had to pierce in via raw pointers to those atomics from
// SetResourceGroupConfig. The data lived in the wrong place
// (allocator's job is token allocation, not config storage), so
// the API entry point had to reach across components.
//
// (2) rmStrategy: tempting because rmStrategy is the RM-mode
// component that consumes the config. Rejected because rmStrategy
// is short-lived (destroyed on every mode swap).
// SetResourceGroupConfig calls that arrive before RM mode
// activates - or between strategy rebuilds - would have nowhere
// to land. The storage needs to outlive any specific strategy.
//
// (3) cpuTimeTokenGrantCoordinator: the API entry point and
// long-lived. Tempting because adding storage there avoids any
// q.mu interaction. Rejected because (a) coord is otherwise a
// thin wrapper around objects (filler + queues), not a data
// manager - adding state changes its character; (b) coord-owns
// separates source-of-truth from derived state, requiring
// rmStrategy and WorkQueue to read from a third object, when the
// holder gives WorkQueue a single pointer to consult.
//
// === Single map, no pending/active separation ===
//
// We considered a pending+active design where pending is the
// just-stored config and active is the last-applied config, with
// lazy creation in Admit reading active to stay consistent with
// existing groupInfos. Rejected: GetDerivedOrDefault is the only
// read path, and the snapshot returned by Snapshot() is an
// independent map. With no other external reader, pending+active
// separation only buys naming clarity, not correctness.
//
// === No hard "must be configured before Admit" invariant ===
//
// We considered enforcing that any group ID seen by Admit must
// already be in the holder (i.e., reject or panic on unknown IDs).
// Rejected because (a) it doesn't fit serverless mode where
// TenantIDs are arbitrary and unbounded; (b) it's brittle in the
// face of ordering races (e.g., Admit arriving in the startup
// window before setUseResourceGroup(true) materializes the
// config). Lazy creation via GetDerivedOrDefault returns
// defaultDerivedGroupConfig for unknown IDs - a safe minimum
// (weight=defaultGroupWeight, maxCPU=false, burstFrac=0) that
// lets the request proceed without granting unexpected burst
// budget.
type ResourceGroupConfigHolder struct {
	mu struct {
		syncutil.Mutex
		// derived is the most recently computed derived state. Replaced
		// wholesale on every Set; never mutated in place after
		// publication so that a Snapshot caller observing the map under
		// mu can release mu and read map entries safely (entries are
		// immutable values, not pointers).
		derived map[uint64]DerivedGroupConfig
	}
}

// newResourceGroupConfigHolder constructs a holder seeded with
// defaultRMResourceGroupConfig. The seed ensures that an immediate
// Snapshot on a freshly-constructed holder returns the high/low
// hardcoded resource groups, which is what WorkQueue applies on
// first activation of RM mode (no separate "wait for first Set"
// path).
func newResourceGroupConfigHolder() *ResourceGroupConfigHolder {
	h := &ResourceGroupConfigHolder{}
	h.Set(defaultRMResourceGroupConfig)
	return h
}

// Set replaces the source config and recomputes derived state in
// one atomic swap. The argument map is read-only with respect to the
// holder; the holder does not retain a reference to it. Callers may
// safely mutate the input map after Set returns.
func (h *ResourceGroupConfigHolder) Set(config map[uint64]ResourceGroupConfig) {
	derived := computeDerived(config)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.derived = derived
}

// GetDerivedOrDefault returns the derived config for id if it is
// configured, otherwise defaultDerivedGroupConfig. Used by
// WorkQueue's lazy group creation in RM mode: an admit for an ID
// without a corresponding groupInfo consults the holder to
// populate weight/maxCPU/burstFrac for the new groupInfo.
func (h *ResourceGroupConfigHolder) GetDerivedOrDefault(id uint64) DerivedGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cfg, ok := h.mu.derived[id]; ok {
		return cfg
	}
	return defaultDerivedGroupConfig
}

// Snapshot returns a copy of the current derived state, suitable for
// passing to WorkQueue.ApplyResourceGroupConfig. The returned map is
// owned by the caller; subsequent Set calls on the holder do not
// affect previously-returned snapshots.
func (h *ResourceGroupConfigHolder) Snapshot() map[uint64]DerivedGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := make(map[uint64]DerivedGroupConfig, len(h.mu.derived))
	for id, cfg := range h.mu.derived {
		snap[id] = cfg
	}
	return snap
}

// computeDerived produces the derived per-group state from a raw
// config map. It applies the same cap+scaling rules as
// SetTenantWeights (groupWeightCap, defaultGroupWeight floor) so RM
// and serverless treat weights identically, then computes BurstFrac
// per group: 1.0 if MaxCPU, else Weight/totalWeight, else 0 when
// totalWeight is 0.
func computeDerived(config map[uint64]ResourceGroupConfig) map[uint64]DerivedGroupConfig {
	var totalWeight uint32
	maxWeight := uint32(1)
	for _, c := range config {
		totalWeight += c.Weight
		if c.Weight > maxWeight {
			maxWeight = c.Weight
		}
	}
	scaling := float64(1)
	if maxWeight > groupWeightCap {
		scaling = groupWeightCap / float64(maxWeight)
	}

	derived := make(map[uint64]DerivedGroupConfig, len(config))
	for id, c := range config {
		scaledWeight := uint32(math.Ceil(float64(c.Weight) * scaling))
		if scaledWeight < defaultGroupWeight {
			scaledWeight = defaultGroupWeight
		}
		var burstFrac float64
		switch {
		case c.MaxCPU:
			burstFrac = 1.0
		case totalWeight > 0:
			burstFrac = float64(c.Weight) / float64(totalWeight)
		}
		derived[id] = DerivedGroupConfig{
			Weight:    scaledWeight,
			MaxCPU:    c.MaxCPU,
			BurstFrac: burstFrac,
		}
	}
	return derived
}
