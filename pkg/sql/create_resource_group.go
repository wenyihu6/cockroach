// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/errors"
)

// resourceGroupJSON matches the JSON format used by
// admission.ParseResourceGroupsJSON.
type resourceGroupJSON struct {
	Name      string `json:"name"`
	WeightCPU int32  `json:"weight_cpu"`
	MaxCPU    bool   `json:"max_cpu"`
}

func readResourceGroups(p *planner) ([]resourceGroupJSON, error) {
	configStr := admission.ResourceGroupsConfig.Get(&p.ExecCfg().Settings.SV)
	if configStr == "" {
		return nil, nil
	}
	var groups []resourceGroupJSON
	if err := json.Unmarshal([]byte(configStr), &groups); err != nil {
		return nil, errors.Wrap(err, "parsing resource groups config")
	}
	return groups, nil
}

func writeResourceGroups(ctx context.Context, p *planner, groups []resourceGroupJSON) error {
	data, err := json.Marshal(groups)
	if err != nil {
		return err
	}
	_, err = p.InternalSQLTxn().ExecEx(
		ctx,
		"update-resource-groups",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			"SET CLUSTER SETTING %s = $1",
			admission.ResourceGroupsConfig.Name()),
		string(data),
	)
	return err
}

type createResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.CreateResourceGroup
}

func (p *planner) CreateResourceGroup(
	ctx context.Context, n *tree.CreateResourceGroup,
) (planNode, error) {
	return &createResourceGroupNode{n: n}, nil
}

func evalWeightCPU(params runParams, expr tree.Expr) (int32, error) {
	typedExpr, err := tree.TypeCheckAndRequire(
		params.ctx, expr, params.p.SemaCtx(), types.Int, "WEIGHT_CPU")
	if err != nil {
		return 0, err
	}
	d, err := eval.Expr(params.ctx, params.p.EvalContext(), typedExpr)
	if err != nil {
		return 0, err
	}
	weightCPU, ok := d.(*tree.DInt)
	if !ok {
		return 0, errors.Newf("WEIGHT_CPU must be an integer, got %T", d)
	}
	if *weightCPU <= 0 {
		return 0, errors.New("WEIGHT_CPU must be positive")
	}
	return int32(*weightCPU), nil
}

func (n *createResourceGroupNode) startExec(params runParams) error {
	weightCPU, err := evalWeightCPU(params, n.n.WeightCPU)
	if err != nil {
		return err
	}

	groups, err := readResourceGroups(params.p)
	if err != nil {
		return err
	}
	name := string(n.n.Name)
	for _, g := range groups {
		if g.Name == name {
			if n.n.IfNotExists {
				return nil
			}
			return errors.Newf("resource group %q already exists", name)
		}
	}
	groups = append(groups, resourceGroupJSON{
		Name:      name,
		WeightCPU: weightCPU,
		MaxCPU:    n.n.MaxCPU,
	})
	return writeResourceGroups(params.ctx, params.p, groups)
}

func (n *createResourceGroupNode) Next(runParams) (bool, error) { return false, nil }
func (n *createResourceGroupNode) Values() tree.Datums          { return nil }
func (n *createResourceGroupNode) Close(context.Context)        {}

type alterResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.AlterResourceGroup
}

func (p *planner) AlterResourceGroup(
	ctx context.Context, n *tree.AlterResourceGroup,
) (planNode, error) {
	return &alterResourceGroupNode{n: n}, nil
}

func (n *alterResourceGroupNode) startExec(params runParams) error {
	groups, err := readResourceGroups(params.p)
	if err != nil {
		return err
	}
	name := string(n.n.Name)
	found := false
	for i, g := range groups {
		if g.Name == name {
			found = true
			if n.n.WeightCPU != nil {
				w, err := evalWeightCPU(params, n.n.WeightCPU)
				if err != nil {
					return err
				}
				groups[i].WeightCPU = w
			}
			if n.n.MaxCPU != nil {
				groups[i].MaxCPU = *n.n.MaxCPU
			}
			break
		}
	}
	if !found {
		return errors.Newf("resource group %q does not exist", name)
	}
	return writeResourceGroups(params.ctx, params.p, groups)
}

func (n *alterResourceGroupNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterResourceGroupNode) Values() tree.Datums          { return nil }
func (n *alterResourceGroupNode) Close(context.Context)        {}

type dropResourceGroupNode struct {
	zeroInputPlanNode
	n *tree.DropResourceGroup
}

func (p *planner) DropResourceGroup(
	ctx context.Context, n *tree.DropResourceGroup,
) (planNode, error) {
	return &dropResourceGroupNode{n: n}, nil
}

func (n *dropResourceGroupNode) startExec(params runParams) error {
	groups, err := readResourceGroups(params.p)
	if err != nil {
		return err
	}
	name := string(n.n.Name)
	found := false
	newGroups := make([]resourceGroupJSON, 0, len(groups))
	for _, g := range groups {
		if g.Name == name {
			found = true
			continue
		}
		newGroups = append(newGroups, g)
	}
	if !found {
		if n.n.IfExists {
			return nil
		}
		return errors.Newf("resource group %q does not exist", name)
	}
	return writeResourceGroups(params.ctx, params.p, newGroups)
}

func (n *dropResourceGroupNode) Next(runParams) (bool, error) { return false, nil }
func (n *dropResourceGroupNode) Values() tree.Datums          { return nil }
func (n *dropResourceGroupNode) Close(context.Context)        {}
