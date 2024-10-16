// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

//	┌─────────────────────────────────────────┐                                      MuxRangefeedEvent
//	│            Node.MuxRangeFeed            │◄──────────────────────────────────────────────────┐
//	└─────────────────┬───▲───────────────────┘                  ▲                                │
//	 Sender.AddStream │   │LockedMuxStream.Send                  │                                │
//			 ┌────────────▼───┴──────────┐                           │                                │
//			 │ Buffered/Unbuffered Sender├───────────┐               │                                │
//			 └────────────┬──────────────┘           │               │                                │
//			 	   				  │                          │               │                                │
//			 	   ┌────────▼─────────┐                │               │                                │
//			 	   │ Stores.Rangefeed │                │               │                                │
//			 	   └────────┬─────────┘                │               │                                │
//			 	   				  │                          │               │                                │
//			 	    ┌───────▼─────────┐         BufferedSender      BufferedSender                      │
//			 	    │ Store.Rangefeed │ SendUnbuffered/SendBuffered SendBufferedError ─────► BufferedSender.run
//			 	    └───────┬─────────┘ (catch-up scan)(live raft)     ▲
//			 	   				  │                        ▲                 │
//			 	   ┌────────▼──────────┐             │                 │
//			 	   │ Replica.Rangefeed │             │                 │
//			 	   └────────┬──────────┘             │                 │
//			 	   				  │                        │                 │
//			 	    ┌───────▼──────┐                 │                 │
//			 	    │ Registration │                 │                 │
//			 	    └──────┬───────┘                 │                 │
//			 	    			 │      								   │					  		 │
//			 	    			 │                         │                 │
//			 	    			 └─────────────────────────┘─────────────────┘
//			 	    		BufferedPerRangeEventSink.Send    BufferedPerRangeEventSink.Disconnect
//
// BufferedSender is embedded in every rangefeed.BufferedPerRangeEventSink,
// serving as a helper which buffers events before forwarding events to the
// underlying gRPC stream.
//
// Refer to the comments above UnbufferedSender for more details on the role of
// senders in the entire rangefeed architecture.
type BufferedSender struct {
	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to recod rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	return &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
}

func (bs *BufferedSender) NewStream(
	ctx context.Context, rangeID roachpb.RangeID, streamID int64,
) Stream {
	return &BufferedPerRangeEventSink{
		PerRangeEventSink: NewPerRangeEventSink(ctx, rangeID, streamID, bs.sendUnbuffered),
		sendBuffered:      bs.sendBuffered,
	}
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
func (bs *BufferedSender) sendBuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// SendUnbuffered bypasses the buffer and sends the event to the underlying
// ServerStreamSender directly. Note that this can cause event re-ordering.
// Caller is responsible for ensuring that events are sent in order.
func (bs *BufferedSender) sendUnbuffered(
	event *kvpb.MuxRangeFeedEvent,
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// Left here to implement but to be removed and replaced with a Disconnect
// interface.
func (bs *BufferedSender) Disconnect(streamID int64, rangeID roachpb.RangeID, err *kvpb.Error) {
	// Disconnect stream and cancel context. Then call sendBuffered with the error
	// event.
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) sendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	// Disconnect stream and cancel context. Then call sendBuffered with the error
	// event.
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) AddStream(streamID int64, cancel context.CancelFunc) {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Stop() {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) Error() chan error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}
