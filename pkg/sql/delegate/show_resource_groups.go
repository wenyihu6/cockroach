// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// delegateShowResourceGroups implements SHOW RESOURCE GROUPS by querying the
// system.resource_groups table.
func (d *delegator) delegateShowResourceGroups(_ *tree.ShowResourceGroups) (tree.Statement, error) {
	return d.parse(`SELECT id, name, weight_cpu, max_cpu FROM system.resource_groups ORDER BY id`)
}
