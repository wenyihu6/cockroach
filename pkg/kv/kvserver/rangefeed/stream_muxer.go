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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RangefeedMetricsRecorder is an interface for recording rangefeed metrics.
type RangefeedMetricsRecorder interface {
	UpdateMetricsOnRangefeedConnect()
	UpdateMetricsOnRangefeedDisconnect()
}

// ServerStreamSender forwards MuxRangefeedEvents from StreamMuxer to the
// underlying stream.
type ServerStreamSender interface {
	// SendUnbuffered must be thread-safe to be called concurrently. We use
	// SendUnbuffered to distinguish from BufferedStreamSender.SendBuffered.
	SendUnbuffered(*kvpb.MuxRangeFeedEvent) error
	// SendIsThreadSafe is a no-op declaration method. It is a contract that the
	// interface has a thread-safe Send method.
	SendIsThreadSafe()
}

// StreamMuxer is responsible for managing a set of active rangefeed streams and
// forwarding rangefeed completion errors to the client.
//
//			                            ┌───────────────────────────┐
//			                            │ DistSender.RangefeedSpans │  rangefeedMuxer
//			                            └───────────────────────────┘
//			                                         │ divideAllSpansOnRangeBoundaries
//			             ┌───────────────────────────┬───────────────────────────┐
//			             ▼                           ▼                           ▼
//			   ┌────────────────────┐     ┌────────────────────┐      ┌────────────────────┐
//			   │   rangefeedMuxer   │     │   rangefeedMuxer   │      │   rangefeedMuxer   │
//			   │startSingleRangefeed│     │startSingleRangefeed│      │startSingleRangefeed│
//			   └─────────┬──────────┘     └──────────┬─────────┘      └──────────┬─────────┘
//			             ▼                           ▼                           ▼
//			        new streamID               new streamID                 new streamID
//			     ┌────────────────┐         ┌────────────────┐           ┌────────────────┐
//			     │RangefeedRequest│         │RangefeedRequest│           │RangefeedRequest│
//			     └────────────────┘         └────────────────┘           └────────────────┘
//			       rangefeedMuxer             rangefeedMuxer               rangefeedMuxer
//			   establishMuxConnection     establishMuxConnection       establishMuxConnection
//			             │                           │                            │
//			             ▼                           ▼                            ▼
//			           rangefeedMuxer.startNodeMuxRangefeed           rangefeedMuxer.startNodeMuxRangefeed
//	               rangefeedMuxer.receiveEventsFromNode           rangefeedMuxer.receiveEventsFromNode
//			        ┌─────────────────────────────────────────┐    ┌─────────────────────────────────────────┐
//			        │rpc.RestrictedInternalClient.MuxRangeFeed│    │rpc.RestrictedInternalClient.MuxRangeFeed│
//			        └─────────────┬────────────▲──────────────┘    └─────────────────────────────────────────┘
//			kvpb.RangefeedRequest │            │ kvpb.MuxRangefeedEvent
//		          ┌─────────────▼────────────┴──────────────┐
//		          │            Node.MuxRangeFeed            │◄───────────────── MuxRangefeedEvent with kvpb.RangeFeedError
//		          └─────────────────┬───▲───────────────────┘                   (client: rangefeedMuxer.restartActiveRangeFeed)
//		      StreamMuxer.AddStream │   │LockedMuxStream.Send(*kvpb.MuxRangefeedEvent)                          │
//		                       ┌────▼───┴────┐                                                                  │
//		                       │ StreamMuxer ├────────────────────────────────────┬─────────────────────────────┐
//		                       └──────┬──────┘                                    │                             │
//		                              │                                           │                             │
//		                     ┌────────▼─────────┐                                 │                             │
//		                     │ Stores.Rangefeed │                                 │                             │
//		                     └────────┬─────────┘                                 │                             │
//		                              │                                           │                             │
//		                      ┌───────▼─────────┐                           StreamMuxer                   StreamMuxer
//		                      │ Store.Rangefeed │              RegisterRangefeedCleanUp     DisconnectStreamWithError
//		                      └───────┬─────────┘                                 ▲                             ▲
//		                              │                                           │                             │
//		                     ┌────────▼──────────┐                                │                             │
//		                     │ Replica.Rangefeed │                                │                             │
//		                     └────────┬──────────┘                                │                             │
//		                              │                                           │                             │
//		                       ┌──────▼───────┐                                   │                             │
//		                       │ Registration ├───────────────────────────────────┘                             │
//		                       └──────┬───────┘       ScheduledProcessor.Register                               │
//		                              │                                                                         │
//		                              └─────────────────────────────────────────────────────────────────────────┘
//		                                              registration.disconnect
type StreamMuxer struct {
	// taskCancel is a function to cancel StreamMuxer.run spawned in the
	// background. It is called by StreamMuxer.Stop. It is expected to be called
	// after StreamMuxer.Start.
	taskCancel context.CancelFunc

	// wg is used to coordinate async tasks spawned by StreamMuxer. Currently,
	// there is only one task spawned by StreamMuxer.Start (StreamMuxer.run).
	wg sync.WaitGroup

	// errCh is used to signal errors from StreamMuxer.run back to the caller. If
	// non-empty, the StreamMuxer.run is finished and error should be handled.
	// Note that it is possible for StreamMuxer.run to be finished without sending
	// an error to errCh. Other goroutines are expected to receive the same
	// shutdown signal in this case and handle error appropriately.
	errCh chan error

	// Note that lockedMuxStream wraps the underlying grpc server stream, ensuring
	// thread safety.
	sender ServerStreamSender

	// metrics is used to record rangefeed metrics for the node.
	metrics RangefeedMetricsRecorder

	// streamID -> streamInfo for active rangefeeds
	activeStreams syncutil.Map[int64, streamInfo]

	// streamID -> cleanup callback
	rangefeedCleanup syncutil.Map[int64, func()]

	// notifyMuxError is a buffered channel of size 1 used to signal the presence
	// of muxErrors. Additional signals are dropped if the channel is already full
	// so that it's non-blocking. StreamMuxer would forward muxErrors back to the
	// client.
	notifyMuxError chan struct{}

	// notifyRangefeedCleanUp is a buffered channel of size 1 used to signal the
	// presence of clean up ids. Additional signals are dropped if the channel is
	// already full so that it's non-blocking. StreamMuxer would invoke
	// corresponding clean up callback registered during run.
	notifyRangefeedCleanUp chan struct{}

	mu struct {
		syncutil.Mutex
		// muxErrors is a slice of mux rangefeed completion errors to be sent back
		// to the client. Upon receiving the error, the client restart rangefeed
		// when possible.
		muxErrors []*kvpb.MuxRangeFeedEvent
		// cleanupIDs is a slice of stream IDs that have been disconnected and
		// require invocation on rangefeed cleanup callback registered on
		// rangefeedCleanup. Note that it is possible to have duplicate stream ids
		// in the slice. We rely on the rangefeedCleanup map to perform dedupe.
		cleanupIDs []int64
	}
}

// NewStreamMuxer creates a new StreamMuxer. There should only one for each
// incoming node.MuxRangefeed RPC stream.
func NewStreamMuxer(sender ServerStreamSender, metrics RangefeedMetricsRecorder) *StreamMuxer {
	return &StreamMuxer{
		sender:                 sender,
		metrics:                metrics,
		notifyMuxError:         make(chan struct{}, 1),
		notifyRangefeedCleanUp: make(chan struct{}, 1),
	}
}

// streamInfo contains the rangeID and cancel function for an active rangefeed.
// It should be treated as immutable.
type streamInfo struct {
	rangeID roachpb.RangeID
	cancel  context.CancelFunc
}

// AddStream registers a server rangefeed stream with the StreamMuxer. It
// remains active until DisconnectStreamWithError is called with the same
// streamID. Caller must ensure no duplicate stream IDs are added without
// disconnecting the old one first.
func (sm *StreamMuxer) AddStream(
	streamID int64, rangeID roachpb.RangeID, cancel context.CancelFunc,
) {
	if _, loaded := sm.activeStreams.LoadOrStore(streamID, &streamInfo{
		rangeID: rangeID,
		cancel:  cancel,
	}); loaded {
		log.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	sm.metrics.UpdateMetricsOnRangefeedConnect()
}

// SendIsThreadSafe is a no-op declaration method. It is a contract that the
// Send method is thread-safe. Note that Send wraps ServerStreamSender which
// also declares its Send method to be thread-safe.
func (sm *StreamMuxer) SendIsThreadSafe() {}

func (sm *StreamMuxer) SendUnbuffered(e *kvpb.MuxRangeFeedEvent) error {
	return sm.sender.SendUnbuffered(e)
}

// SendBuffered sends a buffered MuxRangeFeedEvent to the client. Note that this
// method can only be called if the wrapped sender is a BufferedStreamSender. It
// is caller's responsibility to check if stream is active. SendBuffered only
// sends events.
func (sm *StreamMuxer) SendBuffered(
	e *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation,
) error {
	// Panics if sender is not a BufferedStreamSender. This is a programming
	// error. Check memory accounting for sharedmux vs sharedevent.
	return sm.sender.(*BufferedStreamSender).SendBuffered(e, alloc)
}

func (sm *StreamMuxer) appendCleanUp(streamID int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.cleanupIDs = append(sm.mu.cleanupIDs, streamID)
	// Note that notifyCleanUp is non-blocking.
	select {
	case sm.notifyRangefeedCleanUp <- struct{}{}:
	default:
	}
}

// RegisterRangefeedCleanUp registers a cleanup callback for a rangefeed stream.
// It will be scheduled to run in the background during
// DisconnectStreamWithError. Note that the cleanup callback is not called
// immediately after DisconnectStreamWithError and will not be called if
// StreamMuxer.Stop has been called (which happens when node.MuxRangefeed
// returns).
func (sm *StreamMuxer) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
	sm.rangefeedCleanup.Store(streamID, &cleanUp)
}

// transformRangefeedErrToClientError converts a rangefeed error to a client
// error to be sent back to client. This also handles nil values, preventing nil
// pointer dereference.
func transformRangefeedErrToClientError(err *kvpb.Error) *kvpb.Error {
	if err == nil {
		// When processor is stopped when it no longer has any registrations, it
		// would attempt to close all feeds again with a nil error. Theoretically,
		// this should never happen as processor would always stop with a reason if
		// feeds are active.
		return kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	}
	return err
}

// appendMuxError appends a mux rangefeed completion error to be sent back to
// the client. Note that this method cannot block on IO. If the underlying
// stream is broken, the error will be dropped.
func (sm *StreamMuxer) appendMuxError(e *kvpb.MuxRangeFeedEvent) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.mu.muxErrors = append(sm.mu.muxErrors, e)
	// Note that notifyMuxError is non-blocking.
	select {
	case sm.notifyMuxError <- struct{}{}:
	default:
	}
}

// DisconnectStreamWithError disconnects a stream with an error. Safe to call
// repeatedly for the same stream, but subsequent errors are ignored. It ensures
// 1. the stream context is cancelled 2. exactly one error is sent back to the
// client on behalf of the stream.
//
// Note that this function can be called by the processor worker while holding
// raftMu, so it is important that this function doesn't block IO. It does so by
// delegating the responsibility of sending mux error to StreamMuxer.run.
func (sm *StreamMuxer) DisconnectStreamWithError(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) {
	if stream, ok := sm.activeStreams.LoadAndDelete(streamID); ok {
		// Fine to skip nil checking here since that would be a programming error.
		stream.cancel()
		clientErrorEvent := transformRangefeedErrToClientError(err)
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		ev.MustSetValue(&kvpb.RangeFeedError{
			Error: *clientErrorEvent,
		})
		sm.appendMuxError(ev)
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
	}
	// Note that we may repeatedly append cleanup signal for the same id. We will
	// rely on the map rangefeedCleanup to dedupe during Run. Note that it will
	// not be called if the StreamMuxer is stopped.
	if _, ok := sm.rangefeedCleanup.Load(streamID); ok {
		sm.appendCleanUp(streamID)
	}
}

func (sm *StreamMuxer) detachCleanUpIDs() []int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toCleanUp := sm.mu.cleanupIDs
	sm.mu.cleanupIDs = nil
	return toCleanUp
}

// detachMuxErrors returns muxErrors and clears the slice. Caller must ensure
// the returned errors are sent back to the client.
func (sm *StreamMuxer) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	toSend := sm.mu.muxErrors
	sm.mu.muxErrors = nil
	return toSend
}

// run forwards rangefeed completion errors back to the client. run is expected
// to be called in a goroutine and will block until the context is done or the
// stopper is quiesced. StreamMuxer will stop forward rangefeed completion
// errors after run completes, and caller is responsible for handling shutdown.
func (sm *StreamMuxer) run(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-sm.notifyMuxError:
			toSend := sm.detachMuxErrors()
			for _, clientErr := range toSend {
				if bs, ok := sm.sender.(*BufferedStreamSender); ok {
					// For BufferedStreamSender, use SendBuffered to ensure that errors
					// are sent only after all other queued events have been sent to the
					// client.
					// TODO(wenyihu6): add tests for this
					if err := bs.SendBuffered(clientErr, nil); err != nil {
						log.Errorf(ctx,
							"failed to send rangefeed completion error back to client due to broken stream: %v", err)
						return err
					}
				} else {
					if err := sm.sender.SendUnbuffered(clientErr); err != nil {
						log.Errorf(ctx,
							"failed to send rangefeed completion error back to client due to broken stream: %v", err)
						return err
					}
				}
			}
		case <-sm.notifyRangefeedCleanUp:
			toCleanUp := sm.detachCleanUpIDs()
			for _, streamID := range toCleanUp {
				if cleanUp, ok := sm.rangefeedCleanup.LoadAndDelete(streamID); ok {
					// TODO(wenyihu6): add more observability metrics into how long
					// queuing and the clean up call is taking
					(*cleanUp)()
				}
			}
		case <-ctx.Done():
			// Top level goroutine will receive the context cancellation and handle
			// ctx.Err().
			return nil
		case <-stopper.ShouldQuiesce():
			// Top level goroutine will receive the stopper quiesce signal and handle
			// error.
			return nil
		}
	}
}

// Error returns a channel that can be used to receive errors from
// StreamMuxer.run. Only non-nil errors are sent on this channel. If non-empty,
// streamMuxer.run is finished, and the caller is responsible for handling the
// error.
func (sm *StreamMuxer) Error() chan error {
	if sm.errCh == nil {
		log.Fatalf(context.Background(), "StreamMuxer.Error called before StreamMuxer.Start")
	}
	return sm.errCh
}

// Stop cancels the StreamMuxer.run task, waits for it to complete, and handles
// any cleanups for active streams. It is expected to be called after
// StreamMuxer.Start. After this function returns, the caller is not expected to
// call RegisterRangefeedCleanUp, assume that the registered cleanup callback
// will be executed after calling DisconnectStreamWithError, or assume that
// DisconnectStreamWithError would send an error back to the client.
// TODO(wenyihu6): add observability into when this goes wrong
//
// Note that it does not send any errors back to the clients since the grpc
// stream is being torn down, and the client will decide whether to restart all
// rangefeeds again based on the returned error from stream.
//
// TODO(wenyihu6): add tests to make sure client treats node out of budget
// errors as retryable and will restart all rangefeeds
func (sm *StreamMuxer) Stop() {
	sm.taskCancel()
	sm.wg.Wait()
	sm.disconnectAll()
}

// disconnectAll disconnects all active streams and invokes all rangefeed clean
// up callbacks. It is expected to be called during StreamMuxer.Stop.
func (sm *StreamMuxer) disconnectAll() {
	sm.activeStreams.Range(func(streamID int64, info *streamInfo) bool {
		info.cancel()
		// Remove the stream from the activeStreams map.
		sm.activeStreams.Delete(streamID)
		sm.metrics.UpdateMetricsOnRangefeedDisconnect()
		return true
	})

	sm.rangefeedCleanup.Range(func(streamID int64, cleanUp *func()) bool {
		(*cleanUp)()
		sm.rangefeedCleanup.Delete(streamID)
		return true
	})
}

// Start launches StreamMuxer.run in the background if no error is returned.
// StreamMuxer.run continues running until it errors or StreamMuxer.Stop is
// called. The caller is responsible for calling StreamMuxer.Stop and handle any
// cleanups for any active streams. Note that it is not valid to call Start
// multiple times or restart after Stop. Example usage:
//
//	if err := streamMuxer.Start(ctx, stopper); err != nil {
//	 return err
//	}
//
// defer streamMuxer.Stop()
func (sm *StreamMuxer) Start(ctx context.Context, stopper *stop.Stopper) error {
	if sm.errCh != nil {
		log.Fatalf(ctx, "StreamMuxer.Start called multiple times")
	}
	sm.errCh = make(chan error, 1)
	ctx, sm.taskCancel = context.WithCancel(ctx)
	sm.wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "test-stream-muxer", func(ctx context.Context) {
		defer sm.wg.Done()
		if err := sm.run(ctx, stopper); err != nil {
			sm.errCh <- err
		}
	}); err != nil {
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	return nil
}
