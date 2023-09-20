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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

func Validate(initialState state.State, events scheduled.EventExecutor) string {
	buf := strings.Builder{}
	buf.WriteString("validation result:\n")
	failed := false
	zone, region, zoneToRegion := processClusterInfo(initialState.ClusterInfo().Regions)
	for _, se := range events.ScheduledEvents() {
		if e, ok := se.TargetEvent.(event.SetSpanConfigEvent); ok {
			ma := newMockAllocator(zone, region, zoneToRegion)
			if success, reason := ma.isSatisfiable(e.Config); !success {
				failed = true
				buf.WriteString(fmt.Sprintf("\tinvalid: event scheduled at %s is expected to lead to failure\n", se.At.Format("2006-01-02 15:04:05")))
				buf.WriteString(fmt.Sprintf("\tunsatisfiable: %s\n", reason))
			}
		}
	}
	if !failed {
		buf.WriteString("\tvalid\n")
	}
	return buf.String()
}
