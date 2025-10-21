// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// +-----------------+               +-----------------+                  +-----------------+
// |                 | Disconnect    |                 |  r.disconnect    |                 |
// |  MuxRangeFeed   +-------------->|  StreamManager  +----------------->|   Registration  |
// |                 |               |                 |  Send/SendError  |                 |
// |                 |               |                 |<-----------------+                 |
// +-----------------+               +-----------------+                  +-------------+---+
//                                                                            ^         |
//                                                                r.disconnect|         | p.asyncUnregisterReq
//                                                                            |         v
//                                  +-----------------+                 +---+-------------+
//                                  |                 | UnregFromReplica|                 |
//                                  |    Replica      |<----------------+   Processor     |
//                                  |                 |                 |                 |
//                                  +-----------------+                 +-----------------+

// StreamManager manages one or more streams. It is responsible for starting and
// stopping the underlying sender, as well as managing the streams themselves.
type StreamManager struct {
	// taskCancel cancels the context used by the sender.runspawn goroutine,
	// causing it to exit. It is called in Stop().
	taskCancel context.CancelFunc

	// wg is used to coordinate goroutines spawned by StreamManager.
	wg sync.WaitGroup

	// errCh delivers errors from sender.run back to the caller. If non-empty, the
	// sender.run is finished and error should be handled. Note that it is
	// possible for sender.run to be finished without sending an error to errCh.
	errCh chan error

	// Implemented by UnbufferedSender and BufferedSender. Implementations should
	// ensure that sendUnbuffered and sendBuffered are thread-safe.
	sender sender

	// streamID -> Disconnector
	streams struct {
		syncutil.Mutex
		m map[int64]Disconnector
	}

	// metrics is used to record rangefeed metrics for the node. It tracks number
	// of active rangefeeds.
	metrics *StreamManagerMetrics
}

// sender is an interface that is implemented by BufferedSender and
// UnbufferedSender. It is wrapped under StreamManager, Stream, and
// BufferedStream.
type sender interface {
	// sendUnbuffered sends a RangeFeedEvent to the underlying grpc stream
	// directly. This call may block and must be thread-safe.
	sendUnbuffered(ev *kvpb.MuxRangeFeedEvent) error
	// sendBuffered buffers a RangeFeedEvent before sending to the underlying grpc
	// stream. This call must be non-blocking and thread-safe.
	sendBuffered(ev *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation) error
	// run is the main loop for the sender. It is expected to run in the
	// background until a node level error is encountered which would shut down
	// all streams in StreamManager.
	run(ctx context.Context, stopper *stop.Stopper, onError func(int64)) error

	// TODO(ssd): These two methods call into question whether StreamManager and
	// sender can really be separate. We might consider combining the two for
	// simplicity.
	//
	// addStream is called when an individual stream is being added.
	addStream(streamID int64)
	// removeStream is called when an individual stream is being removed.
	removeStream(streamID int64)

	// cleanup is called when the sender is stopped. It is expected to clean up
	// any resources used by the sender.
	cleanup(ctx context.Context)
}

func NewStreamManager(sender sender, metrics *StreamManagerMetrics) *StreamManager {
	sm := &StreamManager{
		sender:  sender,
		metrics: metrics,
	}
	sm.streams.m = make(map[int64]Disconnector)
	return sm
}

func (sm *StreamManager) NewStream(streamID int64, rangeID roachpb.RangeID) (sink Stream) {
	if log.V(5) {
		senderType := "unknown"
		switch sm.sender.(type) {
		case *BufferedSender:
			senderType = "buffered"
		case *UnbufferedSender:
			senderType = "unbuffered"
		}
		log.KvExec.Infof(context.Background(), "stream_manager: creating new %s stream for streamID=%d rangeID=%d",
			senderType, streamID, rangeID)
	}
	switch sender := sm.sender.(type) {
	case *BufferedSender:
		return &BufferedPerRangeEventSink{
			PerRangeEventSink: NewPerRangeEventSink(rangeID, streamID, sender),
		}
	case *UnbufferedSender:
		return NewPerRangeEventSink(rangeID, streamID, sender)
	default:
		log.KvDistribution.Fatalf(context.Background(), "unexpected sender type %T", sm)
		return nil
	}
}

// OnError is a callback that is called when a sender sends a rangefeed
// completion error back to the client. Note that we check for the existence of
// streamID to avoid metrics inaccuracy when the error is sent before the stream
// is added to the StreamManager.
func (sm *StreamManager) OnError(streamID int64) {
	func() {
		sm.streams.Lock()
		defer sm.streams.Unlock()
		if _, ok := sm.streams.m[streamID]; ok {
			// TODO(ssd): We should be able to assert we are disconnected here.
			delete(sm.streams.m, streamID)
			sm.metrics.ActiveMuxRangeFeed.Dec(1)
			if log.V(5) {
				log.KvExec.Infof(context.Background(), "stream_manager: OnError for stream %d, removed from manager, active streams: %d",
					streamID, len(sm.streams.m))
			}
		} else {
			if log.V(5) {
				log.KvExec.Infof(context.Background(), "stream_manager: OnError for stream %d, but stream not found in manager (may have been removed already)",
					streamID)
			}
		}
	}()
	sm.sender.removeStream(streamID)
}

// DisconnectStream disconnects the stream with the given streamID.
func (sm *StreamManager) DisconnectStream(streamID int64, err *kvpb.Error) {
	if err == nil {
		log.KvDistribution.Fatalf(context.Background(),
			"unexpected: DisconnectStream called with nil error")
		return
	}
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if disconnector, ok := sm.streams.m[streamID]; ok {
		// Fine to skip nil checking here since that would be a programming error.
		if log.V(5) {
			log.KvExec.Infof(context.Background(), "stream_manager: disconnecting stream %d with error: %v",
				streamID, err)
		}
		disconnector.Disconnect(err)
	} else {
		if log.V(5) {
			log.KvExec.Infof(context.Background(), "stream_manager: attempted to disconnect stream %d, but stream not found",
				streamID)
		}
	}
}

// RegisteringStream is called once a stream will be registered. After this
// point, the stream may start to see event.
func (sm *StreamManager) RegisteringStream(streamID int64) {
	if log.V(5) {
		log.KvExec.Infof(context.Background(), "stream_manager: registering stream %d, preparing to receive events",
			streamID)
	}
	sm.sender.addStream(streamID)
}

// AddStream adds a streamID with its disconnector to the StreamManager.
// StreamManager can use the disconnector to shut down the rangefeed stream
// later on.
func (sm *StreamManager) AddStream(streamID int64, d Disconnector) {
	// At this point, the stream had been registered with the processor and
	// started receiving events. We need to lock here to avoid race conditions
	// with a disconnect error passing through before the stream is added.
	sm.streams.Lock()
	defer sm.streams.Unlock()
	if d.IsDisconnected() {
		// If the stream is already disconnected, we don't add it to streams. The
		// registration will have already sent an error to the client.
		if log.V(5) {
			log.KvExec.Infof(context.Background(), "stream_manager: stream %d already disconnected, not adding to manager",
				streamID)
		}
		return
	}
	if _, ok := sm.streams.m[streamID]; ok {
		log.KvDistribution.Fatalf(context.Background(), "stream %d already exists", streamID)
	}
	sm.streams.m[streamID] = d
	sm.metrics.ActiveMuxRangeFeed.Inc(1)
	sm.metrics.NumMuxRangeFeed.Inc(1)
	if log.V(5) {
		log.KvExec.Infof(context.Background(), "stream_manager: added stream %d to manager, total active streams: %d, cumulative streams: %d",
			streamID, len(sm.streams.m), sm.metrics.NumMuxRangeFeed.Count())
	}
}

// Start launches sender.run in the background if no error is returned.
// sender.run continues running until it errors or StreamManager.Stop is called.
// Note that it is not valid to call Start multiple times or restart after Stop.
// Example usage:
//
//	if err := StreamManager.Start(ctx, stopper); err != nil {
//	 return err
//	}
//
// defer StreamManager.Stop()
func (sm *StreamManager) Start(ctx context.Context, stopper *stop.Stopper) error {
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: starting sender run loop")
	}
	sm.errCh = make(chan error, 1)
	sm.wg.Add(1)
	ctx, sm.taskCancel = context.WithCancel(ctx)
	if err := stopper.RunAsyncTask(ctx, "stream-manager-sender", func(ctx context.Context) {
		defer sm.wg.Done()
		if err := sm.sender.run(ctx, stopper, sm.OnError); err != nil {
			if log.V(5) {
				log.KvExec.Infof(ctx, "stream_manager: sender run loop returned error: %v", err)
			}
			sm.errCh <- err
		} else {
			if log.V(5) {
				log.KvExec.Infof(ctx, "stream_manager: sender run loop completed without error")
			}
		}
	}); err != nil {
		if log.V(5) {
			log.KvExec.Infof(ctx, "stream_manager: failed to start async task: %v", err)
		}
		sm.taskCancel()
		sm.wg.Done()
		return err
	}
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: started successfully")
	}
	return nil
}

// Stop cancels the sender.run task and waits for it to complete. It does
// nothing if sender.run is already finished. It is expected to be called after
// StreamManager.Start.
func (sm *StreamManager) Stop(ctx context.Context) {
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: stopping, canceling sender task")
	}
	sm.taskCancel()
	sm.wg.Wait()
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: sender task completed, cleaning up")
	}
	sm.sender.cleanup(ctx)
	sm.streams.Lock()
	defer sm.streams.Unlock()
	numStreams := len(sm.streams.m)
	log.KvDistribution.VInfof(ctx, 2, "stopping stream manager: disconnecting %d streams", numStreams)
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: disconnecting %d active streams", numStreams)
		// Log individual streams being disconnected
		if numStreams > 0 && numStreams <= 10 {
			// Only log individual stream IDs if there aren't too many
			streamIDs := make([]int64, 0, numStreams)
			for streamID := range sm.streams.m {
				streamIDs = append(streamIDs, streamID)
			}
			log.KvExec.Infof(ctx, "stream_manager: disconnecting streams: %v", streamIDs)
		}
	}
	rangefeedClosedErr := kvpb.NewError(
		kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
	sm.metrics.ActiveMuxRangeFeed.Dec(int64(len(sm.streams.m)))
	for streamID, disconnector := range sm.streams.m {
		// Disconnect all streams with a retry error. No rangefeed errors will be
		// sent to the client after shutdown, but the gRPC stream will still
		// terminate.
		disconnector.Disconnect(rangefeedClosedErr)
		if log.V(5) && numStreams <= 10 {
			log.KvExec.Infof(ctx, "stream_manager: disconnected stream %d", streamID)
		}
	}
	sm.streams.m = nil
	if log.V(5) {
		log.KvExec.Infof(ctx, "stream_manager: stopped successfully")
	}
}

// Error returns a channel for receiving errors from sender.run. Only non-nil
// errors are sent, and at most one error is delivered. If the channel is
// non-empty, sender.run has finished, and the error should be handled.
// sender.run may also finish without sending anything to the channel.
func (sm *StreamManager) Error() <-chan error {
	if sm.errCh == nil {
		log.KvDistribution.Fatalf(context.Background(), "StreamManager.Error called before StreamManager.Start")
	}
	return sm.errCh
}

// For testing only.
func (sm *StreamManager) activeStreamCount() int {
	sm.streams.Lock()
	defer sm.streams.Unlock()
	return len(sm.streams.m)
}
