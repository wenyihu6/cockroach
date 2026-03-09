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

// readResourceGroupByName looks up a resource group by name from the
// system.resource_groups table. Returns (id, weight, maxCPU, found, err).
func readResourceGroupByName(
	ctx context.Context, p *planner, name string,
) (int64, int32, bool, bool, error) {
	row, err := p.InternalSQLTxn().QueryRowEx(
		ctx,
		"read-resource-group",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT id, weight_cpu, max_cpu FROM system.resource_groups WHERE name = $1",
		name,
	)
	if err != nil {
		return 0, 0, false, false, err
	}
	if row == nil {
		return 0, 0, false, false, nil
	}
	id := int64(tree.MustBeDInt(row[0]))
	weight := int32(tree.MustBeDInt(row[1]))
	maxCPU := bool(tree.MustBeDBool(row[2]))
	return id, weight, maxCPU, true, nil
}

// nextResourceGroupID returns the next available resource group ID.
func nextResourceGroupID(ctx context.Context, p *planner) (int64, error) {
	row, err := p.InternalSQLTxn().QueryRowEx(
		ctx,
		"next-resource-group-id",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT COALESCE(MAX(id), -1) + 1 FROM system.resource_groups",
	)
	if err != nil {
		return 0, err
	}
	return int64(tree.MustBeDInt(row[0])), nil
}

// syncResourceGroupsToSetting reads all resource groups from the system
// table and writes them to the cluster setting so the admission engine
// picks up the change.
func syncResourceGroupsToSetting(ctx context.Context, p *planner) error {
	rows, err := p.InternalSQLTxn().QueryBufferedEx(
		ctx,
		"sync-resource-groups",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT name, weight_cpu, max_cpu FROM system.resource_groups ORDER BY id",
	)
	if err != nil {
		return err
	}
	type rgJSON struct {
		Name      string `json:"name"`
		WeightCPU int32  `json:"weight_cpu"`
		MaxCPU    bool   `json:"max_cpu"`
	}
	groups := make([]rgJSON, len(rows))
	for i, row := range rows {
		groups[i] = rgJSON{
			Name:      string(tree.MustBeDString(row[0])),
			WeightCPU: int32(tree.MustBeDInt(row[1])),
			MaxCPU:    bool(tree.MustBeDBool(row[2])),
		}
	}
	data, err := json.Marshal(groups)
	if err != nil {
		return err
	}
	_, err = p.InternalSQLTxn().ExecEx(
		ctx,
		"sync-resource-groups-setting",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("SET CLUSTER SETTING %s = $1", admission.ResourceGroupsConfig.Name()),
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

	name := string(n.n.Name)
	_, _, _, found, err := readResourceGroupByName(params.ctx, params.p, name)
	if err != nil {
		return err
	}
	if found {
		if n.n.IfNotExists {
			return nil
		}
		return errors.Newf("resource group %q already exists", name)
	}

	id, err := nextResourceGroupID(params.ctx, params.p)
	if err != nil {
		return err
	}

	_, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		"create-resource-group",
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"INSERT INTO system.resource_groups (id, name, weight_cpu, max_cpu) VALUES ($1, $2, $3, $4)",
		id, name, weightCPU, n.n.MaxCPU,
	)
	if err != nil {
		return err
	}
	return syncResourceGroupsToSetting(params.ctx, params.p)
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
	name := string(n.n.Name)
	id, currentWeight, currentMaxCPU, found, err := readResourceGroupByName(
		params.ctx, params.p, name)
	if err != nil {
		return err
	}
	if !found {
		return errors.Newf("resource group %q does not exist", name)
	}

	newWeight := currentWeight
	if n.n.WeightCPU != nil {
		w, err := evalWeightCPU(params, n.n.WeightCPU)
		if err != nil {
			return err
		}
		newWeight = w
	}
	newMaxCPU := currentMaxCPU
	if n.n.MaxCPU != nil {
		newMaxCPU = *n.n.MaxCPU
	}

	_, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		"alter-resource-group",
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"UPDATE system.resource_groups SET weight_cpu = $1, max_cpu = $2 WHERE id = $3",
		newWeight, newMaxCPU, id,
	)
	if err != nil {
		return err
	}
	return syncResourceGroupsToSetting(params.ctx, params.p)
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
	name := string(n.n.Name)
	_, _, _, found, err := readResourceGroupByName(params.ctx, params.p, name)
	if err != nil {
		return err
	}
	if !found {
		if n.n.IfExists {
			return nil
		}
		return errors.Newf("resource group %q does not exist", name)
	}

	_, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx,
		"drop-resource-group",
		params.p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		"DELETE FROM system.resource_groups WHERE name = $1",
		name,
	)
	if err != nil {
		return err
	}
	return syncResourceGroupsToSetting(params.ctx, params.p)
}

func (n *dropResourceGroupNode) Next(runParams) (bool, error) { return false, nil }
func (n *dropResourceGroupNode) Values() tree.Datums          { return nil }
func (n *dropResourceGroupNode) Close(context.Context)        {}
