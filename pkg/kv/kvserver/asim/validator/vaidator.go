// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type Validator struct {
	zoneToRegion map[string]string
	zone         map[string]int
	region       map[string]int
	total        int
}

type ValidationResult struct {
	Config         roachpb.SpanConfig
	Satisfiable    bool
	Configurations MockAllocator
	Reason         string
}

func PrettyFormat(config roachpb.SpanConfig) string {
	return fmt.Sprintf("expected constraints: replicas=%v voters=%v constraints=%v voter_constraints=%v\n",
		config.NumReplicas, config.NumVoters, config.Constraints, config.VoterConstraints)
}

func (v ValidationResult) String() string {
	buf := strings.Builder{}
	buf.WriteString(PrettyFormat(v.Config))
	if v.Satisfiable {
		buf.WriteString(fmt.Sprintf("satisfiable: %v\n", v.Configurations.String()))
	} else {
		buf.WriteString(fmt.Sprintf("unsatisfiable: %v\n", v.Reason))
	}
	return buf.String()
}

func NewValidator(regions []state.Region) Validator {
	// Since all constraint checks utilize the same Cluster info, we process the
	// Cluster info once and reuse it.
	zoneToRegion, zone, region, total := processClusterInfo(regions)
	return Validator{zoneToRegion, zone, region, total}
}

func (v Validator) ValidateConfig(config roachpb.SpanConfig) ValidationResult {
	// Create a new MockAllocator for every constraint satisfiability check as
	// isSatisfiable directly modifies MockAllocator fields.
	ma := v.newMockAllocator()
	satisfiable, err := ma.isSatisfiable(config)
	return ValidationResult{
		Config:         config,
		Satisfiable:    satisfiable,
		Configurations: ma,
		Reason:         err,
	}
}
