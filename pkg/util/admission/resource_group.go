// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ResourceGroupID identifies a resource group for CPU isolation.
// Resource groups generalize the fixed 2-tier resourceTier system
// (systemTenant/appTenant) to support N user-defined groups with weighted
// CPU sharing.
//
// Each resource group maps to a resourceTier, and gets its own:
//   - WorkQueue in the cpuTimeTokenGrantCoordinator
//   - Row of token buckets in cpuTimeTokenGranter
//   - Per-group refill rate in cpuTimeTokenAllocator
//
// Resource groups are configured via ResourceGroupRegistry and propagated
// through WorkInfo.ResourceGroup to the admission control layer.
type ResourceGroupID uint8

const (
	// DefaultResourceGroupID is the default resource group that all work is
	// assigned to when no explicit resource group is configured.
	DefaultResourceGroupID ResourceGroupID = 0

	// MaxResourceGroups is the maximum number of resource groups supported.
	MaxResourceGroups = 254
)

// ResourceGroupConfig defines the CPU isolation parameters for a resource group.
type ResourceGroupConfig struct {
	// Name is the human-readable name of the resource group.
	Name string
	// WeightCPU is the relative weight for CPU sharing. Higher weight means
	// a larger share of CPU. The actual share for a group is:
	//   group_weight / sum(all_group_weights) * node_cpu_capacity
	WeightCPU int32
	// MaxCPU controls burst behavior:
	//   true  = can burst up to 100% of node CPU when capacity is available
	//   false = capped at 75% of node CPU even when capacity is available
	MaxCPU bool
}

// maxCPUBurstFraction is the maximum fraction of CPU that a group with
// MaxCPU=false can burst to, even when there is spare capacity from
// idle groups.
const maxCPUBurstFraction = 0.75

// ResourceGroupRegistry manages the set of configured resource groups.
// It is safe for concurrent use.
//
// When resource groups are not configured (the default), a single "default"
// group exists with weight 100 and MaxCPU=true, which is equivalent to the
// existing systemTenant tier behavior.
//
// To replicate the existing 2-tier serverless behavior, configure two groups:
//   - Group 0: "system" (weight=95, MaxCPU=true)  → maps to systemTenant tier
//   - Group 1: "app"    (weight=80, MaxCPU=false) → maps to appTenant tier
//
// For non-serverless clusters (the Fidelity use case), configure N groups:
//   - Group 0: "default"  (weight=100, MaxCPU=true)
//   - Group 1: "analytics" (weight=50, MaxCPU=false)
//   - Group 2: "batch"     (weight=25, MaxCPU=false)
type ResourceGroupRegistry struct {
	mu struct {
		syncutil.Mutex
		groups      []ResourceGroupConfig
		totalWeight int32
	}
}

// NewResourceGroupRegistry creates a new registry with a single default group.
func NewResourceGroupRegistry() *ResourceGroupRegistry {
	r := &ResourceGroupRegistry{}
	r.mu.groups = []ResourceGroupConfig{
		{Name: "default", WeightCPU: 100, MaxCPU: true},
	}
	r.mu.totalWeight = 100
	return r
}

// NewServerlessResourceGroupRegistry creates a registry that mirrors the
// existing 2-tier serverless behavior (system tenant + app tenant).
func NewServerlessResourceGroupRegistry() *ResourceGroupRegistry {
	r := &ResourceGroupRegistry{}
	r.mu.groups = []ResourceGroupConfig{
		{Name: "system", WeightCPU: 95, MaxCPU: true},
		{Name: "app", WeightCPU: 80, MaxCPU: true},
	}
	r.mu.totalWeight = 175
	return r
}

// AddGroup adds a new resource group and returns its ID.
func (r *ResourceGroupRegistry) AddGroup(cfg ResourceGroupConfig) ResourceGroupID {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.mu.groups) >= int(MaxResourceGroups) {
		panic("too many resource groups")
	}
	id := ResourceGroupID(len(r.mu.groups))
	r.mu.groups = append(r.mu.groups, cfg)
	r.mu.totalWeight += cfg.WeightCPU
	return id
}

// UpdateGroup updates an existing resource group's configuration.
func (r *ResourceGroupRegistry) UpdateGroup(id ResourceGroupID, cfg ResourceGroupConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.mu.groups[id]
	r.mu.totalWeight += cfg.WeightCPU - old.WeightCPU
	r.mu.groups[id] = cfg
}

// NumGroups returns the number of configured groups.
func (r *ResourceGroupRegistry) NumGroups() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.groups)
}

// GetGroup returns the config for a group.
func (r *ResourceGroupRegistry) GetGroup(id ResourceGroupID) ResourceGroupConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.groups[id]
}

// TotalWeight returns the sum of all group weights.
func (r *ResourceGroupRegistry) TotalWeight() int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.totalWeight
}

// Snapshot returns a consistent snapshot of all groups and total weight.
func (r *ResourceGroupRegistry) Snapshot() (groups []ResourceGroupConfig, totalWeight int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]ResourceGroupConfig, len(r.mu.groups))
	copy(result, r.mu.groups)
	return result, r.mu.totalWeight
}

// ComputeTargetUtilizations computes per-group target CPU utilizations for
// each burst qualification.
//
// For a group with weight W and total weight T:
//   - noBurst target = baseNoBurstTarget * (W / T)
//     This is the group's minimum guaranteed CPU share.
//   - canBurst target depends on MaxCPU:
//   - MaxCPU=true:  baseNoBurstTarget + burstDelta (full node target)
//     — allows bursting to use idle capacity from other groups.
//   - MaxCPU=false: same as noBurst (no bursting beyond minimum share)
//
// Example: 3 groups (online=160, batch=20, support=20), totalWeight=200,
// baseNoBurstTarget=0.8:
//
//	online (MaxCPU=true):  noBurst=0.64, canBurst=0.85
//	batch  (MaxCPU=false): noBurst=0.08, canBurst=0.08
//	support(MaxCPU=false): noBurst=0.08, canBurst=0.08
//
// This ensures online_rg can burst to use idle CPU, while batch_rg and
// support_rg are capped at their proportional share.
func (r *ResourceGroupRegistry) ComputeTargetUtilizations(
	baseNoBurstTarget float64, burstDelta float64,
) []targetUtilizationPair {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]targetUtilizationPair, len(r.mu.groups))
	totalWeight := float64(r.mu.totalWeight)
	if totalWeight == 0 {
		totalWeight = 1
	}
	for i, g := range r.mu.groups {
		weightFrac := float64(g.WeightCPU) / totalWeight
		noBurstTarget := baseNoBurstTarget * weightFrac
		var canBurstTarget float64
		if g.MaxCPU {
			// MaxCPU=true: can burst up to 100% of node CPU. This enables
			// full work-conserving behavior — the group can use all idle
			// capacity from other groups.
			canBurstTarget = 1.0
		} else {
			// MaxCPU=false: can burst up to 75% of node CPU. This allows
			// the group to use idle capacity from other groups, but caps
			// it below 100% to prevent non-MaxCPU groups from monopolizing
			// the node.
			canBurstTarget = maxCPUBurstFraction
		}
		result[i] = targetUtilizationPair{
			noBurst:  noBurstTarget,
			canBurst: canBurstTarget,
		}
	}
	return result
}

// targetUtilizationPair holds the noBurst and canBurst target CPU utilizations
// for a single resource group.
type targetUtilizationPair struct {
	noBurst  float64
	canBurst float64
}

// resourceGroupJSON is the JSON representation of a resource group for
// configuration via cluster settings or test knobs.
type resourceGroupJSON struct {
	Name      string `json:"name"`
	WeightCPU int32  `json:"weight_cpu"`
	MaxCPU    bool   `json:"max_cpu"`
}

// ParseResourceGroupsJSON parses a JSON array of resource group configs.
// Example: [{"name":"default","weight_cpu":100,"max_cpu":true},{"name":"batch","weight_cpu":25,"max_cpu":false}]
func ParseResourceGroupsJSON(jsonStr string) (*ResourceGroupRegistry, error) {
	if jsonStr == "" {
		return nil, nil
	}
	var groups []resourceGroupJSON
	if err := json.Unmarshal([]byte(jsonStr), &groups); err != nil {
		return nil, errors.Wrap(err, "parsing resource groups JSON")
	}
	if len(groups) == 0 {
		return nil, nil
	}
	r := &ResourceGroupRegistry{}
	r.mu.groups = make([]ResourceGroupConfig, len(groups))
	for i, g := range groups {
		if g.WeightCPU <= 0 {
			return nil, errors.Newf("resource group %q has invalid weight_cpu: %d", g.Name, g.WeightCPU)
		}
		r.mu.groups[i] = ResourceGroupConfig{
			Name:      g.Name,
			WeightCPU: g.WeightCPU,
			MaxCPU:    g.MaxCPU,
		}
		r.mu.totalWeight += g.WeightCPU
	}
	return r, nil
}
