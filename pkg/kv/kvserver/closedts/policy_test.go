// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTargetForPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cast := func(i int, unit time.Duration) time.Duration { return time.Duration(i) * unit }
	secs := func(i int) time.Duration { return cast(i, time.Second) }
	millis := func(i int) time.Duration { return cast(i, time.Millisecond) }

	now := hlc.Timestamp{WallTime: millis(100).Nanoseconds()}
	maxClockOffset := millis(500)

	for _, tc := range []struct {
		name                         string
		lagTargetNanos               time.Duration
		leadTargetOverride           time.Duration
		leadTargetAutoTune           bool
		sideTransportCloseInterval   time.Duration
		observedRaftPropLatency      time.Duration
		observedSideTransportLatency time.Duration
		rangePolicy                  roachpb.RangeClosedTimestampPolicy
		expClosedTSTarget            hlc.Timestamp
	}{
		{
			name:              "configured to lag by 3s kv.closed_timestamp.target_duration",
			lagTargetNanos:    secs(3),
			rangePolicy:       roachpb.LAG_BY_CLUSTER_SETTING,
			expClosedTSTarget: now.Add(-secs(3).Nanoseconds(), 0),
		},
		{
			name:              "configured to lag by 1s kv.closed_timestamp.target_duration",
			lagTargetNanos:    secs(1),
			rangePolicy:       roachpb.LAG_BY_CLUSTER_SETTING,
			expClosedTSTarget: now.Add(-secs(1).Nanoseconds(), 0),
		},
		{
			name:                       "configured for global reads - dominated by side transport closed ts propagation",
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(275) /* sideTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
		},
		{
			name:                       "configured for global reads - dominated by raft transport closed ts propagation",
			sideTransportCloseInterval: millis(50),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(245) /* raftTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_override",
			leadTargetOverride:         millis(1234),
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget:          now.Add(millis(1234).Nanoseconds(), 0),
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_override with auto-tuning",
			leadTargetOverride:         millis(1234),
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			leadTargetAutoTune:         true,
			// auto-tuning is disabled when an override is set.
			expClosedTSTarget: now.Add(millis(1234).Nanoseconds(), 0),
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_auto_tune with no observation",
			sideTransportCloseInterval: millis(50),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(245) /* raftTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
		},
		{
			name:                       "kv.closed_timestamp.lead_for_global_reads_auto_tune with raft",
			sideTransportCloseInterval: millis(200),
			rangePolicy:                roachpb.LEAD_FOR_GLOBAL_READS,
			observedRaftPropLatency:    millis(700),
			leadTargetAutoTune:         true,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(720) /* raftTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
		},
		{
			name:                         "kv.closed_timestamp.lead_for_global_reads_auto_tune with side transport",
			sideTransportCloseInterval:   millis(200),
			rangePolicy:                  roachpb.LEAD_FOR_GLOBAL_READS,
			observedSideTransportLatency: millis(600),
			leadTargetAutoTune:           true,
			expClosedTSTarget: now.
				Add((maxClockOffset +
					millis(800) /* sideTransportPropTime */ +
					millis(25) /* bufferTime */).Nanoseconds(), 0),
		},
	} {
		t.Run("", func(t *testing.T) {
			target := TargetForPolicy(
				now.UnsafeToClockTimestamp(),    /*now*/
				maxClockOffset,                  /*maxClockOffset*/
				tc.lagTargetNanos,               /*lagTargetDuration*/
				tc.leadTargetOverride,           /*leadTargetOverride*/
				tc.leadTargetAutoTune,           /*leadTargetAutoTune*/
				tc.sideTransportCloseInterval,   /*sideTransportCloseInterval*/
				tc.observedRaftPropLatency,      /*observedRaftPropLatency*/
				tc.observedSideTransportLatency, /*observedSideTransportLatency*/
				tc.rangePolicy,                  /*policy*/
			)
			require.Equal(t, tc.expClosedTSTarget, target)
		})
	}
}
