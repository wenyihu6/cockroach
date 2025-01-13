// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package checkpoint contains code responsible for handling changefeed
// checkpoints.
package checkpoint

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// SpanIter is an iterator over a collection of spans.
type SpanIter func(forEachSpan span.Operation)

// Make creates a checkpoint with as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans. A SpanGroup is used to merge adjacent
// spans above the high-water mark.
func Make(
	frontier hlc.Timestamp, forEachSpan SpanIter, maxBytes int64,
) jobspb.ChangefeedProgress_Checkpoint {
	// Collect leading spans into a SpanGroup to merge adjacent spans and store
	// the lowest timestamp found
	var checkpointSpanGroup roachpb.SpanGroup
	currCheckpointFrontier := hlc.Timestamp{}
	prevCheckpointFrontier := hlc.Timestamp{}
	var prevCheckpointSpans []roachpb.Span
	var currCheckpointSpans []roachpb.Span
	forEachSpan(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if frontier.Less(ts) {
			checkpointSpanGroup.Add(s)
			// check if exceeded yet
			var used int64
			for _, sp := range checkpointSpanGroup.Slice() {
				used += int64(len(sp.Key)) + int64(len(sp.EndKey))
				if used > maxBytes {
					return span.StopMatch
				}
				currCheckpointSpans = append(currCheckpointSpans, sp)
				if currCheckpointFrontier.IsEmpty() || ts.Less(currCheckpointFrontier) {
					currCheckpointFrontier = ts
				}
			}
			prevCheckpointSpans = currCheckpointSpans
			currCheckpointSpans = []roachpb.Span{}
			prevCheckpointFrontier = currCheckpointFrontier
			currCheckpointFrontier = hlc.Timestamp{}
		}
		return span.ContinueMatch
	})

	return jobspb.ChangefeedProgress_Checkpoint{
		Spans:     prevCheckpointSpans,
		Timestamp: prevCheckpointFrontier,
	}
}
