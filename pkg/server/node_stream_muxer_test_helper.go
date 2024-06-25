// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
)

type TestSingleStreamMuxer struct {
	streamID int64
	*TestMultiStreamMuxer
}

func NewTestStreamMuxer() *TestSingleStreamMuxer {
	return &TestSingleStreamMuxer{
		TestMultiStreamMuxer: NewTestMultiStreamMuxer(),
		streamID:             1,
	}
}

func (m *TestSingleStreamMuxer) NewStream(cancel context.CancelFunc) {
	m.TestMultiStreamMuxer.newStream(m.streamID, cancel)
}

func (m *TestSingleStreamMuxer) RegisterCleanUp(f func()) {
	m.TestMultiStreamMuxer.registerRangefeedCleanUp(m.streamID, f)
}

func (m *TestSingleStreamMuxer) DisconnectRangefeedWithError(err *kvpb.Error) {
	m.TestMultiStreamMuxer.DisconnectRangefeedWithError(m.streamID, 1, err)
}

func (m *TestSingleStreamMuxer) EventsSent() (rangefeedEvents []*kvpb.RangeFeedEvent) {
	muxRangefeedEvents := m.TestMultiStreamMuxer.EventsSentById(m.streamID)
	for _, e := range muxRangefeedEvents {
		rangefeedEvents = append(rangefeedEvents, &e.RangeFeedEvent)
	}
	return
}

func (m *TestSingleStreamMuxer) Send(event *kvpb.RangeFeedEvent) error {
	return m.TestMultiStreamMuxer.Send(m.streamID, 1, event)
}

func (m *TestSingleStreamMuxer) Error() error {
	return m.TestMultiStreamMuxer.Error(m.streamID)
}

func (m *TestSingleStreamMuxer) TryError() error {
	return m.TestMultiStreamMuxer.TryError(m.streamID)
}

type testRangefeedCounter struct {
	count atomic.Int32
}

func newTestRangefeedCounter() *testRangefeedCounter {
	return &testRangefeedCounter{}
}

func (c *testRangefeedCounter) incrementRangefeedCounter() {
	c.count.Add(1)
}

func (c *testRangefeedCounter) decrementRangefeedCounter() {
	c.count.Add(-1)
}

func (c *testRangefeedCounter) activeRangefeedCount() int32 {
	return c.count.Load()
}

type TestMultiStreamMuxer struct {
	*streamMuxer
	serverStream     *testStream
	rangefeedCounter *testRangefeedCounter
	registrations    sync.Map
}

func NewTestMultiStreamMuxer() *TestMultiStreamMuxer {
	m := &TestMultiStreamMuxer{
		serverStream:     newTestStream(),
		rangefeedCounter: newTestRangefeedCounter(),
	}
	m.streamMuxer = newStreamMuxer(m.serverStream, m.rangefeedCounter)
	return m
}

func (m *TestMultiStreamMuxer) SetSendError(err error) {
	m.serverStream.setSendError(err)
}

func (m *TestMultiStreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) {
	m.streamMuxer.run(ctx, stopper)
}

func (m *TestMultiStreamMuxer) NewStream(streamID int64, cancel context.CancelFunc) {
	m.streamMuxer.newStream(streamID, cancel)
	m.registrations.Store(streamID, make(chan *kvpb.Error, 1))
}

func (m *TestMultiStreamMuxer) RegisterCleanUp(streamID int64, cleanUp func()) {
	m.streamMuxer.registerRangefeedCleanUp(streamID, cleanUp)
}

func (m *TestMultiStreamMuxer) Send(
	streamID int64, rangeID roachpb.RangeID, event *kvpb.RangeFeedEvent,
) error {
	return m.streamMuxer.send(streamID, rangeID, event)
}

func (m *TestMultiStreamMuxer) DisconnectRangefeedWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	v, loaded := m.registrations.Load(streamID)
	if !loaded {
		panic("unexpected registration not found")
	}
	if c, ok := v.(chan *kvpb.Error); ok {
		c <- err
	}
	m.streamMuxer.disconnectRangefeedWithError(streamID, rangeID, err)
}

func (m *TestMultiStreamMuxer) EventsSentById(streamID int64) []*kvpb.MuxRangeFeedEvent {
	return m.serverStream.eventsSentById(streamID)
}

func (m *TestMultiStreamMuxer) Error(streamID int64) error {
	v, loaded := m.registrations.Load(streamID)
	if !loaded {
		return nil
	}
	if c, ok := v.(chan *kvpb.Error); ok {
		select {
		case err := <-c:
			return err.GoError()
		case <-time.After(30 * time.Second):
			return errors.New("time out waiting for rangefeed completion")
		}
	}
	return nil
}

func (m *TestMultiStreamMuxer) TryError(streamID int64) error {
	v, loaded := m.registrations.Load(streamID)
	if !loaded {
		return errors.New("unexpected registration not found")
	}
	if c, ok := v.(chan *kvpb.Error); ok {
		select {
		case err := <-c:
			return err.GoError()
		default:
			return nil
		}
	}
	return errors.New("unexpected channel type")
}

func (m *TestMultiStreamMuxer) sentEventCount() int {
	return m.serverStream.sentEventCount()
}

func (m *TestMultiStreamMuxer) Contains(e *kvpb.MuxRangeFeedEvent) bool {
	return m.serverStream.contains(e)
}

func (m *TestMultiStreamMuxer) ActiveRangefeedCount() int32 {
	return m.rangefeedCounter.activeRangefeedCount()
}

func (m *TestMultiStreamMuxer) BlockSend() func() {
	return m.serverStream.blockSend()
}

// noopStream is a stream that does nothing, except count events.
type testStream struct {
	syncutil.Mutex
	totalEvents int
	// streamId -> event
	events  map[int64][]*kvpb.MuxRangeFeedEvent
	sendErr error
}

func (s *testStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	s.Lock()
	defer s.Unlock()
	s.totalEvents++
	if s.sendErr != nil {
		return s.sendErr
	}
	s.events[e.StreamID] = append(s.events[e.StreamID], e)
	return nil
}

func (s *testStream) blockSend() func() {
	s.Lock()
	var once sync.Once
	return func() {
		once.Do(s.Unlock) // safe to call multiple times, e.g. defer and explicit //nolint:deferunlockcheck
	}
}

func newTestStream() *testStream {
	return &testStream{
		events: make(map[int64][]*kvpb.MuxRangeFeedEvent),
	}
}

func (s *testStream) setSendError(err error) {
	s.Lock()
	defer s.Unlock()
	s.sendErr = err
}

func (s *testStream) eventsSentById(streamID int64) []*kvpb.MuxRangeFeedEvent {
	s.Lock()
	defer s.Unlock()
	events := s.events[streamID]
	s.events[streamID] = nil
	return events
}

func (s *testStream) sentEventCount() int {
	s.Lock()
	defer s.Unlock()
	return s.totalEvents
}

func (s *testStream) contains(e *kvpb.MuxRangeFeedEvent) bool {
	s.Lock()
	defer s.Unlock()
	for _, streamEvent := range s.events[e.StreamID] {
		if reflect.DeepEqual(e, streamEvent) {
			return true
		}
	}
	return false
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
