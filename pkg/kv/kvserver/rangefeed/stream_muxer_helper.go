// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type testRangefeedCounter struct {
	count atomic.Int32
}

func newTestRangefeedCounter() *testRangefeedCounter {
	return &testRangefeedCounter{}
}

func (c *testRangefeedCounter) IncrementRangefeedCounter() {
	c.count.Add(1)
}

func (c *testRangefeedCounter) DecrementRangefeedCounter() {
	c.count.Add(-1)
}

func (c *testRangefeedCounter) get() int32 {
	return c.count.Load()
}

// noopStream is a stream that does nothing, except count events.
type testServerStream struct {
	syncutil.Mutex
	totalEvents int
	events      map[int64][]*kvpb.MuxRangeFeedEvent
	streamsDone map[int64]chan error
	sendErr     error
}

func (s *testServerStream) registerDone(streamID int64, c chan error) {
	s.Lock()
	defer s.Unlock()
	s.streamsDone[streamID] = c
}

func (s *testServerStream) eventSentCount() int {
	s.Lock()
	defer s.Unlock()
	return s.totalEvents
}

func (s *testServerStream) hasEvent(e *kvpb.MuxRangeFeedEvent) bool {
	s.Lock()
	defer s.Unlock()
	for _, streamEvent := range s.events[e.StreamID] {
		if reflect.DeepEqual(e, streamEvent) {
			return true
		}
	}
	return false
}

type muxEvents map[int64][]*kvpb.MuxRangeFeedEvent

func newTestServerStream() *testServerStream {
	return &testServerStream{
		events:      make(muxEvents),
		streamsDone: make(map[int64]chan error),
	}
}
func (s *testServerStream) String() string {
	str := strings.Builder{}
	for streamID, eventList := range s.events {
		str.WriteString(fmt.Sprintf("Stream ID: %d, Len: %d, Events: %v\n", streamID, len(eventList), eventList))
	}
	return str.String()
}

func (s *testServerStream) filterEventsSentById(
	streamID int64, condition func(*kvpb.MuxRangeFeedEvent) bool,
) (res []*kvpb.MuxRangeFeedEvent) {
	s.Lock()
	defer s.Unlock()
	sent := s.events[streamID]
	s.events[streamID] = nil
	for _, event := range sent {
		if condition(event) {
			s.events[streamID] = append(s.events[streamID], event)
		}
	}
	return
}

func (s *testServerStream) rangefeedEventsSentById(
	streamID int64,
) (rangefeedEvents []*kvpb.RangeFeedEvent) {
	s.Lock()
	defer s.Unlock()
	sent := s.events[streamID]
	s.events[streamID] = nil
	for _, e := range sent {
		rangefeedEvents = append(rangefeedEvents, &e.RangeFeedEvent)
	}
	return
}

func (s *testServerStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	s.Lock()
	defer s.Unlock()
	s.totalEvents++
	if s.sendErr != nil {
		return s.sendErr
	}
	s.events[e.StreamID] = append(s.events[e.StreamID], e)
	if e.Error != nil && s.streamsDone[e.StreamID] != nil {
		s.streamsDone[e.StreamID] <- e.Error.Error.GoError()
	}
	return nil
}

func (s *testServerStream) SetSendErr(err error) {
	s.Lock()
	defer s.Unlock()
	s.sendErr = err
}

func makeRangefeedErrorEvent(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) *kvpb.MuxRangeFeedEvent {
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.SetValue(&kvpb.RangeFeedError{
		Error: *err,
	})
	return ev
}
