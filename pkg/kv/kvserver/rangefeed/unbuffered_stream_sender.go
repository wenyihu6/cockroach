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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// type MuxStream struct {
// }
//
// var _ Stream = (*MuxStream)(nil)
//
// // BufferedStreamSender is a StreamSender that buffers events before sending
// // them to the underlying rpc ServerStreamSender stream.

type UnbufferedSender struct {
	sm             *StreamMuxer
	taskCancel     context.CancelFunc
	wg             sync.WaitGroup
	notifyMuxError chan struct{}
	errCh          chan error

	mu struct {
		syncutil.Mutex
		// muxErrors is a slice of mux rangefeed completion errors to be sent back
		// to the client. Upon receiving the error, the client restart rangefeed
		// when possible.
		muxErrors []*kvpb.MuxRangeFeedEvent
	}
}

func NewUnbufferedSender(sm *StreamMuxer) *UnbufferedSender {
	ubs := &UnbufferedSender{
		sm: sm,
	}
	ubs.errCh = make(chan error, 1)
	return ubs
}

func (ubr *UnbufferedSender) Error() chan error {
	if ubr.errCh == nil {
		log.Fatalf(context.Background(), "StreamMuxer.Error called before StreamMuxer.Start")
	}
	return ubr.errCh
}

func (ubr *UnbufferedSender) detachMuxErrors() []*kvpb.MuxRangeFeedEvent {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	toSend := ubr.mu.muxErrors
	ubr.mu.muxErrors = nil
	return toSend
}

// After shutting down, no more and current mux errors are sent to the client.
// But we expect a node level shutdown. Clients should start the all rangefeeds
// after that. TODO(wenyihu6): add tests on client side to confirm this
func (ubr *UnbufferedSender) Stop() {
	ubr.taskCancel()
	ubr.wg.Wait()
}

func (ubr *UnbufferedSender) appendMuxError(e *kvpb.MuxRangeFeedEvent) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.mu.muxErrors = append(ubr.mu.muxErrors, e)
	// Note that notifyMuxError is non-blocking.
	select {
	case ubr.notifyMuxError <- struct{}{}:
	default:
	}
}

// No more events after this shutdown. But we expect node level shutdown soon.
func (ubr *UnbufferedSender) run(ctx context.Context, stopper *stop.Stopper) error {
	for {
		select {
		case <-ubr.notifyMuxError:
			toSend := ubr.detachMuxErrors()
			for _, clientErr := range toSend {
				if err := ubr.sm.Send(clientErr); err != nil {
					log.Errorf(ctx,
						"failed to send rangefeed completion error back to client due to broken stream: %v", err)
					return err
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

func (ubr *UnbufferedSender) Start(ctx context.Context, stopper *stop.Stopper) error {
	if ubr.errCh != nil {
		log.Fatalf(ctx, "UnbufferedSender.Start called multiple times")
	}
	ubr.errCh = make(chan error, 1)
	ctx, ubr.taskCancel = context.WithCancel(ctx)
	ubr.wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "test-stream-muxer", func(ctx context.Context) {
		defer ubr.wg.Done()
		if err := ubr.run(ctx, stopper); err != nil {
			ubr.errCh <- err
		}
	}); err != nil {
		ubr.taskCancel()
		ubr.wg.Done()
		return err
	}
	return nil
}

// For non-error event, we promise to block until the event is sent. For error
// event, this call is un-blocking and the error will be sent later on. It is
// possible that the error is not sent successfully if a node level shutdown is
// encountered. Important to shutdown and no more events are sent after that.
func (ubr *UnbufferedSender) SendUnbuffered(event *kvpb.MuxRangeFeedEvent) error {
	if event.Error != nil {
		// If error out later a node level shutdown will be triggered.
		ubr.appendMuxError(event)
		return nil
	}
	return ubr.sm.Send(event)
}
