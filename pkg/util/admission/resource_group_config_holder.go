// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ResourceGroupConfig is the per-resource-group state WorkQueue
// applies in Resource Manager mode. The caller is responsible for
// pre-normalizing the values: the holder stores the map as-is and
// does no derivation.
type ResourceGroupConfig struct {
	// Weight is the group's percentage share of node CPU, in [0, 100].
	// Weights across the configured set MUST sum to 100. Used directly
	// as the group's heap weight and as its burst-bucket refill share
	// (Weight/100). The caller is also responsible for keeping the
	// highest-to-lowest ratio within whatever heap-fairness bound is
	// acceptable - extreme ratios (e.g. {99, 1}) let the heavy group
	// starve the light one in the queue.
	Weight uint32
	// MaxCPU=true means the group always qualifies for burst regardless
	// of its burst bucket level. Used for groups that should be allowed
	// to consume up to full node CPU on demand. MaxCPU does not affect
	// the bucket's size or refill rate (those still scale by Weight/100);
	// it only short-circuits burst qualification, so the bucket level is
	// never consulted.
	MaxCPU bool
}

// defaultRMResourceGroupConfig is the configuration used until an
// explicit SetResourceGroupConfig call replaces it. The two hardcoded
// groups (high/low) match the two outputs of priorityToResourceGroup
// and split CPU evenly with high additionally allowed to burst.
var defaultRMResourceGroupConfig = map[uint64]ResourceGroupConfig{
	highResourceGroupID: {Weight: 50, MaxCPU: true},
	lowResourceGroupID:  {Weight: 50, MaxCPU: false},
}

// defaultGroupConfig is the safety fallback returned by
// ResourceGroupConfigHolder.GetOrDefault for IDs that aren't
// explicitly configured. Weight=defaultGroupWeight lets the request
// compete in fair sharing while granting effectively no burst budget
// (Weight/100 ≈ 0.01 for defaultGroupWeight=1). In practice RM admits
// route via priorityToResourceGroup, which always returns a configured
// ID, so this fallback only fires under unexpected conditions (test
// setups that bypass the configured map, or future code paths that
// hand WorkQueue a novel ID).
var defaultGroupConfig = ResourceGroupConfig{
	Weight: defaultGroupWeight,
	MaxCPU: false,
}

// ResourceGroupConfigHolder owns the source-of-truth resource group
// configuration for Resource Manager mode. The configuration is
// pre-normalized by the caller (see ResourceGroupConfig); the holder
// is pure storage behind a mutex.
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
// or GetOrDefault while holding WorkQueue.mu - the holder never
// reaches back into WorkQueue. WorkQueue.mu must NEVER be acquired
// while holding the holder's mutex; that ordering invariant is
// enforced implicitly because no code path inside the holder reaches
// outside it.
//
// === Why a dedicated holder owns this storage ===
//
// Earlier designs put the source config on WorkQueue alongside the
// state it produces (groupInfo.weight, cpuTimeBurstBucket.maxCPU).
// Separating it here trades a small amount of indirection for three
// concrete properties:
//
//   - Source-of-truth lives in one place. WorkQueue stores only what
//     it actively reads on hot paths (groupInfo.burstFrac for refill,
//     groupInfo.weight for fair-share ordering). The config is no
//     longer copied onto WorkQueue's mutex-guarded state; it lives
//     once on the holder.
//   - GC of configured-but-idle groups is safe. WorkQueue's GC sweep
//     no longer needs a "skip configured" exception because the
//     holder is consulted by lazy-create on the next admit, which
//     correctly recovers Weight and MaxCPU without any pre-creation
//     invariant.
//   - Lock isolation is real. Admin DDL via SetResourceGroupConfig
//     touches the holder under its own lock; WorkQueue.mu is taken
//     separately only when applying config. Concurrent admits contend
//     with apply only during the (microsecond-scale) apply critical
//     section, not during the holder Set.
type ResourceGroupConfigHolder struct {
	mu struct {
		syncutil.Mutex
		// config is the most recently Set configuration. Replaced
		// wholesale on every Set; never mutated in place after
		// publication so that a Snapshot caller observing the map
		// under mu can release mu and read map entries safely
		// (entries are immutable values, not pointers).
		config map[uint64]ResourceGroupConfig
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

// Set replaces the stored config. The holder defensive-copies the
// argument map; callers may safely mutate the input after Set returns.
func (h *ResourceGroupConfigHolder) Set(config map[uint64]ResourceGroupConfig) {
	cp := make(map[uint64]ResourceGroupConfig, len(config))
	for id, cfg := range config {
		cp[id] = cfg
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.config = cp
}

// GetOrDefault returns the config for id if it is configured,
// otherwise defaultGroupConfig. Used by WorkQueue's lazy group
// creation in RM mode: an admit for an ID without a corresponding
// groupInfo consults the holder to populate weight and maxCPU for
// the new groupInfo (burstFrac is computed inline as Weight/100).
func (h *ResourceGroupConfigHolder) GetOrDefault(id uint64) ResourceGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cfg, ok := h.mu.config[id]; ok {
		return cfg
	}
	return defaultGroupConfig
}

// Snapshot returns a copy of the current config, suitable for passing
// to WorkQueue's apply path. The returned map is owned by the caller;
// subsequent Set calls on the holder do not affect previously-returned
// snapshots.
func (h *ResourceGroupConfigHolder) Snapshot() map[uint64]ResourceGroupConfig {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := make(map[uint64]ResourceGroupConfig, len(h.mu.config))
	for id, cfg := range h.mu.config {
		snap[id] = cfg
	}
	return snap
}
