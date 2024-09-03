// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

//            ┌─────────────────┐
//            │Node.MuxRangefeed│
//            └──────┬───┬──────┘
//  Sender.AddStream │   │LockedMuxStream.Send ───────────────────┐────────────────────────────────┐
//        ┌──────────┴─▼─┴────────────┐                           │                                │
//        │ Buffered/Unbuffered Sender├───────────┐               │                                │
//        └────────────┬──────────────┘           │               │                                │
//                     │                          │               │                                │
//            ┌────────▼─────────┐                │               │                                │
//            │ Stores.Rangefeed │                │               │                                │
//            └────────┬─────────┘                │               │                                │
//                     │                          │               │                                │
//             ┌───────▼─────────┐         BufferedSender      BufferedSender                      │
//             │ Store.Rangefeed │ SendUnbuffered/SendBuffered SendBufferedError ─────► BufferedSender.run
//             └───────┬─────────┘ (catch-up scan)(live raft)     ▲
//                     │                          ▲               │
//            ┌────────▼──────────┐               │               │
//            │ Replica.Rangefeed │               │               │
//            └────────┬──────────┘               │               │
//                     │                          │               │
//             ┌───────▼──────┐                   │               │
//             │ Registration │                   │               │
//             └──────┬───────┘                   │               │
//                    │                           │               │
//                    │                           │               │
//                    └───────────────────────────┘───────────────┘
//               BufferedPerRangeEventSink.Send    BufferedPerRangeEventSink.SendError
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

	queueMu struct {
		syncutil.Mutex
		stopped bool
		buffer  *eventQueue
	}

	// Unblocking channel to notify the BufferedSender.run goroutine that there
	// are events to send.
	notifyDataC chan struct{}
}

func NewBufferedSender(sender ServerStreamSender) *BufferedSender {
	bs := &BufferedSender{
		sender: sender,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.notifyDataC = make(chan struct{}, 1)
	return bs
}

// sendBuffered buffers the event before sending it to the underlying grpc
// stream. It should not block since errors are sent directly here.
//
// sendBuffered will take the ownership of the alloc and release it if the
// return error is non-nil. Note that it is safe to send error events without
// being blocked for too long.
func (bs *BufferedSender) sendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	if bs.queueMu.stopped {
		log.Errorf(context.Background(), "stream sender is stopped")
		return errors.New("stream sender is stopped")
	}
	alloc.Use(context.Background())
	bs.queueMu.buffer.pushBack(sharedMuxEvent{ev, alloc})
	select {
	case bs.notifyDataC <- struct{}{}:
	default:
	}
	return nil
}

// sendUnbuffered sends the event directly to the underlying ServerStreamSender.
// It bypasses the buffer.
func (bs *BufferedSender) sendUnbuffered(ev *kvpb.MuxRangeFeedEvent) error {
	return bs.sender.Send(ev)
}

// run forwards buffered events back to the client. run is expected to be called
// in a goroutine and will block until the context is done or the stopper is
// quiesced. BufferedSender will stop forwarding events after run completes. It
// may still buffer more events in the buffer, but they will be cleaned up soon
// during bs.Stop(), and there should be no new events buffered after that.
func (bs *BufferedSender) run(
	ctx context.Context, stopper *stop.Stopper, onError func(streamID int64),
) error {
	for {
		select {
		case <-ctx.Done():
			// Top level goroutine will receive the context cancellation and handle
			// ctx.Err().
			return nil
		case <-stopper.ShouldQuiesce():
			// Top level goroutine will receive the stopper quiesce signal and handle
			// error.
			return nil
		case <-bs.notifyDataC:
			for {
				e, success := bs.popFront()
				if success {
					err := bs.sender.Send(e.ev)
					e.alloc.Release(ctx)
					if e.ev.Error != nil {
						onError(e.ev.StreamID)
					}
					if err != nil {
						return err
					}
				} else {
					break
				}
			}
		}
	}
}

// popFront pops the front event from the buffer queue. It returns the event and
// a boolean indicating if the event was successfully popped.
func (bs *BufferedSender) popFront() (e sharedMuxEvent, success bool) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.popFront()
	return event, ok
}

// cleanup is called when the sender is stopped. It is expected to free up
// buffer queue and no new events should be buffered after this.
func (bs *BufferedSender) cleanup() {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.free()
}
