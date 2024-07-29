// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// registration is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys.
// Updates are delivered to its stream until one of the following
// conditions is met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration is unregistered its error
// channel is sent an error to inform it that the registration
// has finished.
type nonBufferedRegistration struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	// TODO(wenyihu6): refactor here and embed another base registration struct
	// which handles these and id and keys.
	withDiff       bool
	withFiltering  bool
	withOmitRemote bool
	metrics        *Metrics

	// Output.
	stream Stream
	unreg  func()
	// Internal.
	id            int64
	keys          interval.Range
	blockWhenFull bool // if true, block when buf is full (for tests)

	mu struct {
		sync.Locker
		catchUpBuf          chan *sharedEvent
		catchUpOverflowed   bool
		caughtUp            bool
		catchUpScanCancelFn func()
		disconnected        bool
		catchUpIter         *CatchUpIterator
	}
}

func (nbr *nonBufferedRegistration) getWithDiff() bool {
	return nbr.withDiff
}

func (nbr *nonBufferedRegistration) getWithOmitRemote() bool {
	return nbr.withOmitRemote
}

func (nbr *nonBufferedRegistration) setID(id int64) {
	nbr.id = id
}

func (nbr *nonBufferedRegistration) setSpanAsKeys() {
	nbr.keys = nbr.span.AsRange()
}

func (nbr *nonBufferedRegistration) getSpan() roachpb.Span {
	return nbr.span
}

func (nbr *nonBufferedRegistration) getCatchUpTimestamp() hlc.Timestamp {
	return nbr.catchUpTimestamp
}

func (nbr *nonBufferedRegistration) getWithFiltering() bool {
	return nbr.withFiltering
}

func (nbr *nonBufferedRegistration) getUnreg() func() {
	return nbr.unreg
}

func newNonBufferedRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bufferSz int,
	blockWhenFull bool,
	metrics *Metrics,
	stream Stream,
	unregisterFn func(),
) *bufferedRegistration {
	br := &bufferedRegistration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		withOmitRemote:   withOmitRemote,
		metrics:          metrics,
		stream:           stream,
		unreg:            unregisterFn,
		buf:              make(chan *sharedEvent, bufferSz),
		blockWhenFull:    blockWhenFull,
	}
	br.mu.Locker = &syncutil.Mutex{}
	br.mu.caughtUp = true
	br.mu.catchUpIter = catchUpIter
	return br
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (nbr *nonBufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: nbr.maybeStripEvent(ctx, event), alloc: alloc})

	shouldSendToUnderlyingStream := func() bool {
		nbr.mu.Lock()
		defer nbr.mu.Unlock()
		if nbr.mu.catchUpOverflowed || nbr.mu.disconnected {
			// Dropping events. The registration is either disconnected or will be
			// disconnected soon after catch up scan is done and we will disconnect. It
			// will need a catch up scan when it reconnects. Treat these as true to
			// avoid sending to stream.
			return false
		}
		if nbr.mu.catchUpBuf == nil {
			// Catch up buf has been drained and not due to disconnected. Safe to send
			// to underlying stream. Important to check disconnected first since it is
			// nil after draining as well.
			return true
		}

		// TODO(wenyihu6): add memory accounting here
		select {
		case nbr.mu.catchUpBuf <- e:
		default:
			// Dropping events.
			nbr.mu.catchUpOverflowed = true
			putPooledSharedEvent(e)
		}
		return false
	}()

	if shouldSendToUnderlyingStream {
		// not disconnected yet -> should send to underlying stream
		if err := nbr.stream.Send(e.event); err != nil {
			nbr.disconnect(kvpb.NewError(err))
		}
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
// TODO(wenyihu6): change this to be part of base registration
func (nbr *nonBufferedRegistration) maybeStripEvent(
	ctx context.Context, event *kvpb.RangeFeedEvent,
) *kvpb.RangeFeedEvent {
	ret := event
	copyOnWrite := func() interface{} {
		if ret == event {
			ret = event.ShallowCopy()
		}
		return ret.GetValue()
	}

	switch t := ret.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.PrevValue.IsPresent() && !nbr.withDiff {
			// If no registrations for the current Range are requesting previous
			// values, then we won't even retrieve them on the Raft goroutine.
			// However, if any are and they overlap with an update then the
			// previous value on the corresponding events will be populated.
			// If we're in this case and any other registrations don't want
			// previous values then we'll need to strip them.
			t = copyOnWrite().(*kvpb.RangeFeedValue)
			t.PrevValue = roachpb.Value{}
		}
	case *kvpb.RangeFeedCheckpoint:
		if !t.Span.EqualValue(nbr.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(nbr.span) {
				log.Fatalf(ctx, "registration span %v larger than checkpoint span %v", nbr.span, t.Span)
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = nbr.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(nbr.span); !i.Equal(t.Span) {
			t = copyOnWrite().(*kvpb.RangeFeedDeleteRange)
			t.Span = i.Clone()
		}
	case *kvpb.RangeFeedSSTable:
		// SSTs are always sent in their entirety, it is up to the caller to
		// filter out irrelevant entries.
	default:
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
	return ret
}
func (nbr *nonBufferedRegistration) setDisconnectedIfNot() {
	nbr.mu.Lock()
	defer nbr.mu.Unlock()
	nbr.setDisconnectedIfNotWithRMu()
}

func (nbr *nonBufferedRegistration) setDisconnectedIfNotWithRMu() (alreadyDisconnected bool) {
	// TODO(wenyihu6): think about if you should just drain catchUpBuf here we
	// never publish anything in catch up buf if disconnected. But this might take
	// a long time and you are on a hot path.
	if nbr.mu.disconnected {
		return true
	}
	if nbr.mu.catchUpIter != nil {
		// Catch up scan hasn't started yet.
		nbr.mu.catchUpIter.Close()
		nbr.mu.catchUpIter = nil
	}
	if nbr.mu.catchUpScanCancelFn != nil {
		nbr.mu.catchUpScanCancelFn()
	}
	nbr.mu.disconnected = true
	return false
}

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration. Safe to run multiple
// times, but subsequent errors would be discarded. Catch up goroutine is
// responsible for draining catch up buffer.
func (nbr *nonBufferedRegistration) disconnect(pErr *kvpb.Error) {
	nbr.mu.Lock()
	defer nbr.mu.Unlock()
	if alreadyDisconnected := nbr.setDisconnectedIfNotWithRMu(); !alreadyDisconnected {
		// It is fine to not hold the lock here as the registration has been set as
		// disconnected.
		nbr.stream.Disconnect(pErr)
	}
}

func (nbr *nonBufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	nbr.mu.Lock()
	defer nbr.mu.Unlock()

	// TODO(wenyihu6): check if you can drain first without holding locks
	drainAndPublish := func() error {
		for {
			select {
			case e := <-nbr.mu.catchUpBuf:
				if err := nbr.stream.Send(e.event); err != nil {
					return err
				}
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		}
	}

	if err := drainAndPublish(); err != nil {
		return err
	}

	// Must disconnect first before setting catchUpBuf to nil.
	if nbr.mu.catchUpOverflowed {
		return newErrBufferCapacityExceeded().GoError()
	}

	// success
	nbr.mu.catchUpBuf = nil
	nbr.mu.catchUpScanCancelFn = nil
	return nil
}

func (nbr *nonBufferedRegistration) discardCatchUpBufferWithRMu() {
	func() {
		for {
			select {
			case e := <-nbr.mu.catchUpBuf:
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()

	nbr.mu.catchUpBuf = nil
	nbr.mu.catchUpScanCancelFn = nil
}

func (nbr *nonBufferedRegistration) disconnectAndDiscardCatchUpBuffer(pErr *kvpb.Error) {
	nbr.disconnect(pErr)
	nbr.mu.Lock()
	defer nbr.mu.Unlock()
	nbr.discardCatchUpBufferWithRMu()
}

func (nbr *nonBufferedRegistration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	nbr.mu.Lock()

	if nbr.mu.disconnected {
		// The registration has already been disconnected.
		nbr.discardCatchUpBufferWithRMu()
		nbr.mu.Unlock()
		return
	}

	ctx, nbr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	nbr.mu.Unlock()

	if err := nbr.maybeRunCatchUpScan(ctx); err != nil {
		nbr.disconnectAndDiscardCatchUpBuffer(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		return
	}

	if err := nbr.publishCatchUpBuffer(ctx); err != nil {
		nbr.disconnectAndDiscardCatchUpBuffer(kvpb.NewError(err))
		return
	}
	// publishCatchUpBuffer will drain the catch up buffer.
}

// no allocations other than catch up buf.
func (nbr *nonBufferedRegistration) drainAllocations(ctx context.Context) { return }

// maybeRunCatchUpScan starts a catch-up scan which will output entries for all
// recorded changes in the replica that are newer than the catchUpTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (nbr *nonBufferedRegistration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := nbr.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		nbr.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	return catchUpIter.CatchUpScan(ctx, nbr.stream.Send, nbr.withDiff, nbr.withFiltering, nbr.withOmitRemote)
}

// ID implements interval.Interface.
func (nbr *nonBufferedRegistration) ID() uintptr {
	return uintptr(nbr.id)
}

// Range implements interval.Interface.
func (nbr *nonBufferedRegistration) Range() interval.Range {
	return nbr.keys
}

func (nbr *nonBufferedRegistration) String() string {
	return fmt.Sprintf("[%s @ %s+]", nbr.span, nbr.catchUpTimestamp)
}

// Wait for this registration to completely process its internal buffer.
func (nbr *nonBufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	return nil
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (nbr *nonBufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	nbr.mu.Lock()
	defer nbr.mu.Unlock()
	catchUpIter := nbr.mu.catchUpIter
	nbr.mu.catchUpIter = nil
	return catchUpIter
}
