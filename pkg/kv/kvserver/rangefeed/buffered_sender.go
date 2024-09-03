// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type sharedMuxEvent struct {
	event *kvpb.MuxRangeFeedEvent
	alloc *SharedBudgetAllocation
}

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
	// taskCancel is a function to cancel BufferedSender.run spawned in the
	// background. It is called by BufferedSender.Stop. It is expected to be
	// called after BufferedSender.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by BufferedSender. Currently,
	// there is only one task spawned by BufferedSender.Start
	// (BufferedSender.run).
	wg sync.WaitGroup

	// errCh is used to signal errors from BufferedSender.run back to the caller.
	// If non-empty, the BufferedSender.run is finished and error should be
	// handled. Note that it is possible for BufferedSender.run to be finished
	// without sending an error to errCh. Other goroutines are expected to receive
	// the same shutdown signal in this case and handle error appropriately.
	errCh chan error

	// streamID -> context cancellation
	streams syncutil.Map[int64, context.CancelFunc]

	// streamID -> cleanup callback
	rangefeedCleanup syncutil.Map[int64, func()]

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	queueMu struct {
		syncutil.Mutex
		stopped bool
		buffer  *eventQueue
	}

	// Unblocking channel to notify the BufferedSender.run goroutine that there
	// are events to send.
	notifyDataC chan struct{}
}

func NewBufferedSender(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder,
) *BufferedSender {
	bs := &BufferedSender{
		sender:  sender,
		metrics: metrics,
	}
	bs.queueMu.buffer = newEventQueue()
	bs.notifyDataC = make(chan struct{}, 1)
	return bs
}

// SendBuffered buffers the event before sending them to the underlying
// ServerStreamSender.
//
// alloc.Release is nil-safe. SendBuffered will take the ownership of the alloc
// and release it if the return error is non-nil. Note that it is safe to send
// error events without being blocked for too long.
func (bs *BufferedSender) SendBuffered(
	ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) (err error) {
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

// SendUnbuffered bypasses the buffer and sends the event to the underlying
// ServerStreamSender directly. Note that this can cause event re-ordering.
// Caller is responsible for ensuring that events are sent in order.
func (bs *BufferedSender) SendUnbuffered(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

func (bs *BufferedSender) waitForEmptyBuffer(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		bs.queueMu.Lock()
		caughtUp := bs.queueMu.buffer.Len() == 0 // nolint:deferunlockcheck
		bs.queueMu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("buffered sender failed to send in time")
}

func (bs *BufferedSender) SendBufferedError(ev *kvpb.MuxRangeFeedEvent) {
	if ev.Error == nil {
		log.Fatalf(context.Background(), "unexpected: SendWithoutBlocking called with non-error event")
	}
	if cancel, ok := bs.streams.LoadAndDelete(ev.StreamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		(*cancel)()
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		if err := bs.SendBuffered(ev, nil); err != nil {
			// Ignore error since the stream is already disconnecting. There is nothing
			// else that could be done. When SendBuffered is returning an error, a node
			// level shutdown from node.MuxRangefeed is happening soon to let clients
			// know that the rangefeed is shutting down.
			log.Infof(context.Background(),
				"failed to buffer rangefeed complete event for stream %d due to %s, "+
					"but a node level shutdown should be happening", ev.StreamID, ev.Error)
		}
	}
}

func (bs *BufferedSender) Disconnect(ev *kvpb.MuxRangeFeedEvent) bool {
	panic("unimplemented: buffered sender for rangefeed #126560")
}

// disconnectAll disconnects all active streams and invokes all rangefeed clean
// up callbacks. It is expected to be called during BufferedSender.Stop.
func (bs *BufferedSender) disconnectAll() {
	bs.streams.Range(func(streamID int64, cancel *context.CancelFunc) bool {
		(*cancel)()
		// Remove the stream from the activeStreams map.
		bs.streams.Delete(streamID)
		bs.metrics.UpdateMetricsOnRangefeedDisconnect()
		return true
	})

	bs.rangefeedCleanup.Range(func(streamID int64, cleanUp *func()) bool {
		(*cleanUp)()
		bs.rangefeedCleanup.Delete(streamID)
		return true
	})
}

func (bs *BufferedSender) AddStream(streamID int64, r Disconnector) {
	if _, loaded := bs.streams.LoadOrStore(streamID, &cancel); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	bs.metrics.UpdateMetricsOnRangefeedConnect()
}

// run forwards buffered events back to the client. run is expected to be called
// in a goroutine and will block until the context is done or the stopper is
// quiesced. BufferedSender will stop forwarding events after run completes. It
// may still buffer more events in the buffer, but they will be cleaned up soon
// during bs.Stop(), and there should be no new events buffered after that.
func (bs *BufferedSender) run(ctx context.Context, stopper *stop.Stopper) error {
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
					err := bs.sender.Send(e.event)
					e.alloc.Release(ctx)
					if e.event.Error != nil {
						// Add metrics here
						if cleanUp, ok := bs.rangefeedCleanup.LoadAndDelete(e.event.StreamID); ok {
							// TODO(wenyihu6): add more observability metrics into how long the
							// clean up call is taking
							(*cleanUp)()
						}
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

func (bs *BufferedSender) popFront() (e sharedMuxEvent, success bool) {
	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	event, ok := bs.queueMu.buffer.popFront()
	return event, ok
}

func (bs *BufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	bs.errCh = make(chan error, 1)
	bs.wg.Add(1)
	ctx, bs.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "buffered stream output", func(ctx context.Context) {
		defer bs.wg.Done()
		if err := bs.run(ctx, stopper); err != nil {
			bs.errCh <- err
		}
	}); err != nil {
		bs.taskCancel()
		bs.wg.Done()
		return err
	}
	return nil
}

// Stop cancels the BufferedSender.run task, waits for it to complete, and
// handles any cleanups for active streams. It is expected to be called after
// BufferedSender.Start. After this function returns, BufferedSend will return
// an error to avoid more being buffered afterwards. The caller is also not
// expected to call RegisterRangefeedCleanUp after this function ends and assume
// that the registered cleanup callback will be executed after calling
// SendBufferedError, or assume that SendBufferedError would send an error back
// to the client.
// TODO(wenyihu6): add observability into when this goes wrong
// TODO(wenyihu6): add tests to make sure client treats node out of budget
// errors as retryable and will restart all rangefeeds
func (bs *BufferedSender) Stop() {
	bs.taskCancel()
	bs.wg.Wait()
	bs.disconnectAll()

	bs.queueMu.Lock()
	defer bs.queueMu.Unlock()
	bs.queueMu.stopped = true
	bs.queueMu.buffer.removeAll()
}

func (bs *BufferedSender) Error() chan error {
	if bs.errCh == nil {
		log.Fatalf(context.Background(), "BufferedSender.Error called before BufferedSender.Start")
	}
	return bs.errCh
}
