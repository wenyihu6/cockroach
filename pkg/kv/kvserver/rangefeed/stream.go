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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Stream is an object capable of transmitting RangeFeedEvents from a server
// rangefeed to a client.
type Stream interface {
	kvpb.RangeFeedEventSink
	// Disconnect disconnects the stream with the provided error. Note that this
	// function can be called by the processor worker while holding raftMu, so it
	// is important that this function doesn't block IO or try acquiring locks
	// that could lead to deadlocks.
	Disconnect(err *kvpb.Error)
}

// PerRangeEventSink is an implementation of Stream which annotates each
// response with rangeID and streamID. It is used by MuxRangeFeed.
type PerRangeEventSink struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *UnbufferedSender
}

func NewPerRangeEventSink(
	ctx context.Context, rangeID roachpb.RangeID, streamID int64, wrapped *UnbufferedSender,
) *PerRangeEventSink {
	return &PerRangeEventSink{
		ctx:      ctx,
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ kvpb.RangeFeedEventSink = (*PerRangeEventSink)(nil)
var _ Stream = (*PerRangeEventSink)(nil)

func (s *PerRangeEventSink) Context() context.Context {
	return s.ctx
}

// SendIsThreadSafe is a no-op declaration method. It is a contract that the
// Send method is thread-safe. Note that UnbufferedSender.SendUnbuffered is
// thread-safe.
func (s *PerRangeEventSink) SendIsThreadSafe() {}

func (s *PerRangeEventSink) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.SendUnbuffered(response)
}

// Disconnect implements the Stream interface. It requests the UnbufferedSender
// to detach the stream. The UnbufferedSender is then responsible for handling
// the actual disconnection and additional cleanup. Note that Caller should not
// rely on immediate disconnection as cleanup takes place async.
func (s *PerRangeEventSink) Disconnect(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		RangeID:  s.rangeID,
		StreamID: s.streamID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	s.wrapped.SendBufferedError(ev)
}

// transformRangefeedErrToClientError converts a rangefeed error to a client
// error to be sent back to client. This also handles nil values, preventing nil
// pointer dereference.
//
// NB: when processor.Stop() is called (stopped when it no longer has any
// registrations, it would attempt to close all feeds again with a nil error).
// Theoretically, this should never happen as processor would always stop with a
// reason if feeds are active.
func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		return kvpb.NewError(
			kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

// BufferedStream is a Stream that can buffer events before sending them to the
// underlying Stream. Note that the caller may still choose to bypass the buffer
// and send to the underlying Stream directly by calling Send directly. Doing so
// can cause event re-ordering. Caller is responsible for ensuring that events
// are sent in order.
type BufferedStream interface {
	Stream
	// SendBuffered buffers the event before sending it to the underlying Stream.
	SendBuffered(*kvpb.RangeFeedEvent, *SharedBudgetAllocation) error
}

// BufferedPerRangeEventSink is an implementation of BufferedStream which is
// similar to PerRangeEventSink but buffers events in BufferedSender before
// forwarding events to the underlying grpc stream.
type BufferedPerRangeEventSink struct {
	ctx      context.Context
	rangeID  roachpb.RangeID
	streamID int64
	wrapped  *BufferedSender
}

func NewBufferedPerRangeEventSink(
	ctx context.Context, rangeID roachpb.RangeID, streamID int64, wrapped *BufferedSender,
) *BufferedPerRangeEventSink {
	return &BufferedPerRangeEventSink{
		ctx:      ctx,
		rangeID:  rangeID,
		streamID: streamID,
		wrapped:  wrapped,
	}
}

var _ kvpb.RangeFeedEventSink = (*BufferedPerRangeEventSink)(nil)
var _ Stream = (*BufferedPerRangeEventSink)(nil)
var _ BufferedStream = (*BufferedPerRangeEventSink)(nil)

func (s *BufferedPerRangeEventSink) Context() context.Context {
	return s.ctx
}

// SendIsThreadSafe is a no-op declaration method. It is a contract that the
// Send method is thread-safe. Note that BufferedSender.SendBuffered is
// thread-safe.
func (s *BufferedPerRangeEventSink) SendIsThreadSafe() {}

// SendBuffered buffers the event in BufferedSender and transfers the ownership
// of SharedBudgetAllocation to BufferedSender. BufferedSender is responsible
// for properly using and releasing it when an error occurs or when the event is
// sent. The event is guaranteed to be sent unless BufferedSender terminates
// before sending (such as due to broken grpc stream).
//
// If the function returns an error, it is safe to disconnect the stream and
// assume that all future SendBuffered on this stream will return an error.
func (s *BufferedPerRangeEventSink) SendBuffered(
	event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.SendBuffered(response, alloc)
}

// Send bypass the buffer and sends the event to the underlying grpc stream
// directly. It blocks until the event is sent or an error occurs.
func (s *BufferedPerRangeEventSink) Send(event *kvpb.RangeFeedEvent) error {
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.SendUnbuffered(response, nil)
}

// Disconnect implements the Stream interface. BufferedSender is then
// responsible for canceling the context of the stream. The actual rangefeed
// disconnection from processor happens late when the error event popped from
// the queue and about to be sent to the grpc stream. So caller should not rely
// on immediate disconnection as cleanup takes place async.
func (s *BufferedPerRangeEventSink) Disconnect(err *kvpb.Error) {
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: s.streamID,
		RangeID:  s.rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	s.wrapped.SendBufferedError(ev)
}
