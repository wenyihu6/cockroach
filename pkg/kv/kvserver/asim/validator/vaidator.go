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
	Configurations string
	Reason         string
}

func PrettyFormat(config roachpb.SpanConfig) string {
	var buf strings.Builder
	buf.WriteString("constraint:\n")
	buf.WriteString(fmt.Sprintf("replicas=%v\n", config.NumReplicas))
	buf.WriteString(fmt.Sprintf("voters=%v\n", config.NumVoters))
	buf.WriteString(fmt.Sprintf("constraints=%v\n", config.Constraints))
	buf.WriteString(fmt.Sprintf("voter_constraints=%v\n", config.VoterConstraints))
	return buf.String()
}

func (v ValidationResult) String() string {
	buf := strings.Builder{}
	if v.Satisfiable {
		buf.WriteString(fmt.Sprintf("satisfiable:\n%v\n%v",
			v.Configurations, PrettyFormat(v.Config)))
	} else {
		buf.WriteString(fmt.Sprintf("unsatisfiable:\n%v\n%v\n%v",
			v.Configurations, v.Reason, PrettyFormat(v.Config)))
	}
	return buf.String()
}

func NewValidator(regions []state.Region) Validator {
	// Since all constraint checks utilize the same cluster info, we process the
	// cluster info once and reuse it.
	zoneToRegion, zone, region, total := processClusterInfo(regions)
	return Validator{zoneToRegion, zone, region, total}
}

func (v Validator) ValidateConfig(config roachpb.SpanConfig) ValidationResult {
	// Create a new mockAllocator for every constraint satisfiability check as
	// isSatisfiable directly modifies mockAllocator fields.
	ma := v.newMockAllocator()
	satisfiable, err := ma.isSatisfiable(config)
	return ValidationResult{
		Config:         config,
		Satisfiable:    satisfiable,
		Configurations: fmt.Sprint(ma.String()),
		Reason:         err,
	}
}
