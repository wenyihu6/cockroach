// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
)

// delegateShowResourceGroups implements SHOW RESOURCE GROUPS by reading the
// admission.resource_groups.config cluster setting.
func (d *delegator) delegateShowResourceGroups(_ *tree.ShowResourceGroups) (tree.Statement, error) {
	configStr := admission.ResourceGroupsConfig.Get(
		&d.evalCtx.Settings.SV,
	)
	if configStr == "" {
		// No resource groups configured; return empty result with schema.
		return d.parse(
			`SELECT id::INT8, name::STRING, weight_cpu::INT8, max_cpu::BOOL
			 FROM (VALUES (NULL, NULL, NULL, NULL)) AS t(id, name, weight_cpu, max_cpu)
			 WHERE false`)
	}
	registry, err := admission.ParseResourceGroupsJSON(configStr)
	if err != nil {
		return nil, err
	}
	groups, _ := registry.Snapshot()
	var values []string
	for i, g := range groups {
		values = append(values, fmt.Sprintf(
			"(%d, '%s', %d, %t)", i, g.Name, g.WeightCPU, g.MaxCPU))
	}
	query := fmt.Sprintf(
		`SELECT id::INT8, name::STRING, weight_cpu::INT8, max_cpu::BOOL
		 FROM (VALUES %s) AS t(id, name, weight_cpu, max_cpu)
		 ORDER BY id`,
		strings.Join(values, ", "))
	return d.parse(query)
}
