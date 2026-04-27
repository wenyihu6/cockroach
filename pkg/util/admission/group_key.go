// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "strconv"

// groupKind distinguishes the semantic origin of a groupInfo's
// container in q.mu.groups. The same numeric ID can represent
// either a tenant (in serverless mode) or a resource group (in RM
// mode), and tenant IDs and RM resource group IDs are drawn from
// the same uint64 space (e.g., system tenant ID = 1 collides with
// highResourceGroupID = 1). Pairing the ID with a kind in the map
// key prevents these two semantic meanings from ever sharing a
// container.
type groupKind uint8

const (
	// tenantKind identifies a container created for a tenant ID
	// (serverless mode routing via TenantID).
	tenantKind groupKind = iota
	// rgKind identifies a container created for a resource group
	// ID (RM mode routing via priorityToResourceGroup).
	rgKind
)

// groupKey is the composite map key for q.mu.groups. It pairs the
// uint64 group ID with its semantic kind so that, e.g., tenant 1
// and high-priority RG 1 occupy distinct map entries.
type groupKey struct {
	id   uint64
	kind groupKind
}

// isTenant reports whether k identifies a tenant container.
func (k groupKey) isTenant() bool { return k.kind == tenantKind }

// isRG reports whether k identifies a resource group container.
func (k groupKey) isRG() bool { return k.kind == rgKind }

// tenantGroupKey returns the groupKey for a tenant container.
func tenantGroupKey(id uint64) groupKey {
	return groupKey{id: id, kind: tenantKind}
}

// rgGroupKey returns the groupKey for a resource group container.
func rgGroupKey(id uint64) groupKey {
	return groupKey{id: id, kind: rgKind}
}

// metricLabel returns the per-group metric label for k. Tenant-keyed
// and rg-keyed containers get distinct labels ("tenant:N" vs "rg:N")
// so dashboard time-series naturally separate the two semantic
// classes (e.g. system tenant id 1 vs high-pri RG id 1).
func (k groupKey) metricLabel() string {
	prefix := "tenant:"
	if k.isRG() {
		prefix = "rg:"
	}
	return prefix + strconv.FormatUint(k.id, 10)
}
