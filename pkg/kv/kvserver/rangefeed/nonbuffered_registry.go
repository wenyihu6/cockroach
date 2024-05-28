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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type nonBufferedRegistration struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	metrics          *Metrics

	// TODO(wenyihu6): change it to use something like unreg instead of regDrained later on
	// unreg func()
	done *future.ErrorFuture

	// Internal.
	id   int64
	keys interval.Range
	// TODO(wenyihu6): implement BufferedStream
	stream BufferedStream

	// TODO(wenyihu6): check why catchUpDrained is needed and why it's not under mutex
	catchUpDrained chan struct{}
	mu             struct {
		// TODO(wenyihu6): check why locker not mutex
		sync.Locker
		// cancel catch up scan goroutine
		catchUpScanCancelFn func()
		disconnected        bool

		// store events while catch up is in progress
		catchUpBuf chan *sharedEvent

		// similar to prev overflowed (prev we have r.buf)
		catchUpOverflow bool

		// TODO(wenyihu6): check if catch up iterator needs mutex protection
		catchUpIter *CatchUpIterator

		// catchUpDrained chan struct{}
	}
}

func newNonBufferedRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	catchUpBufferSize int,
	metrics *Metrics,
	stream BufferedStream,
	done *future.ErrorFuture,
) nonBufferedRegistration {
	r := nonBufferedRegistration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		metrics:          metrics,
		stream:           stream,
		done:             done,
		catchUpDrained:   make(chan struct{}),
	}
	r.mu.Locker = &syncutil.Mutex{}

	// TODO(wenyihuy6): check if doing connect catch up iter and stream make sense
	// here (oleg did it in a snew connect function)
	if catchUpIter == nil {
		close(r.catchUpDrained)
	} else {
		r.mu.catchUpBuf = make(chan *sharedEvent, catchUpBufferSize)
		r.mu.catchUpIter = catchUpIter
	}
	r.stream = stream
	return r
}

// validateEvent checks that the event contains enough information for the
// registation.
func validateEvent(event *kvpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Key: %v", t))
		}
		if t.Value.RawBytes == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.RawBytes: %v", t))
		}
		if t.Value.Timestamp.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.Timestamp: %v", t))
		}
	case *kvpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedCheckpoint.Span.Key: %v", t))
		}
	case *kvpb.RangeFeedSSTable:
		if len(t.Data) == 0 {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Data: %v", t))
		}
		if len(t.Span.Key) == 0 {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Span: %v", t))
		}
		if t.WriteTS.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedSSTable.Timestamp: %v", t))
		}
	case *kvpb.RangeFeedDeleteRange:
		if len(t.Span.Key) == 0 || len(t.Span.EndKey) == 0 {
			panic(fmt.Sprintf("unexpected empty key in RangeFeedDeleteRange.Span: %v", t))
		}
		if t.Timestamp.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedDeleteRange.Timestamp: %v", t))
		}
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
}

func (r *nonBufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	validateEvent(event)

	if r.maybePutInCatchupBuffer(ctx, event, alloc) {
		return
	}

	r.stream.Send(r.maybeStripEvent(event), alloc)
}

// assertEvent asserts that the event contains the necessary data.
func (r *nonBufferedRegistration) assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Key: %v", t)
		}
		if t.Value.RawBytes == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.RawBytes: %v", t)
		}
		if t.Value.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedCheckpoint.Span.Key: %v", t)
		}
	case *kvpb.RangeFeedSSTable:
		if len(t.Data) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Data: %v", t)
		}
		if len(t.Span.Key) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Span: %v", t)
		}
		if t.WriteTS.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedDeleteRange:
		if len(t.Span.Key) == 0 || len(t.Span.EndKey) == 0 {
			log.Fatalf(ctx, "unexpected empty key in RangeFeedDeleteRange.Span: %v", t)
		}
		if t.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedDeleteRange.Timestamp: %v", t)
		}
	default:
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
}

// func (r *nonBufferedRegistration) abortAndDisconnectNonStarted(pErr *kvpb.Error) {
//
//		r.disconnect(pErr)
//	}
func (r *nonBufferedRegistration) disconnect(pErr *kvpb.Error) {
	r.cancelCatchUp()
	r.stream.SendError(pErr)
}

func (r *nonBufferedRegistration) cancelCatchUp() {
	defer func() {
		<-r.catchUpDrained
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected {
		return
	}
	r.mu.disconnected = true
	// Case where catch up is in progress and we need to terminate and drain.
	if r.mu.catchUpScanCancelFn != nil {
		r.mu.catchUpScanCancelFn()
	}

	if r.mu.catchUpIter != nil {
		r.mu.catchUpIter.Close()
		r.mu.catchUpIter = nil
		close(r.catchUpDrained)
	}
}

func (r *nonBufferedRegistration) discardCatchUpBuffer(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	func() {
		for {
			select {
			case e := <-r.mu.catchUpBuf:
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()
	if r.mu.catchUpBuf != nil {
		r.mu.catchUpBuf = nil
	}
}

func (r *nonBufferedRegistration) maybeStripEvent(event *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
	ret := event
	copyOnWrite := func() interface{} {
		if ret == event {
			ret = event.ShallowCopy()
		}
		return ret.GetValue()
	}

	switch t := ret.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.PrevValue.IsPresent() && !r.withDiff {
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
		if !t.Span.EqualValue(r.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(r.span) {
				panic(fmt.Sprintf("registration span %v larger than checkpoint span %v", r.span, t.Span))
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = r.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(r.span); !i.Equal(t.Span) {
			t = copyOnWrite().(*kvpb.RangeFeedDeleteRange)
			t.Span = i.Clone()
		}
	case *kvpb.RangeFeedSSTable:
		// SSTs are always sent in their entirety, it is up to the caller to
		// filter out irrelevant entries.
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
	return ret
}

func (r *nonBufferedRegistration) maybePutInCatchupBuffer(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	// If any of those is not nil that means we still need to process catch up
	// scan or we are actively processing catch up scan.
	if r.mu.catchUpBuf == nil {
		return false
	}
	if !r.mu.catchUpOverflow {
		e := getPooledSharedEvent(sharedEvent{
			event: event,
			alloc: alloc,
		})
		alloc.Use(ctx)
		select {
		case r.mu.catchUpBuf <- e:
		default:
			alloc.Release(ctx)
			putPooledSharedEvent(e)
			r.mu.catchUpOverflow = true
		}
	}
	return true
}

func (r *nonBufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	drainCurrent := func() error {
		for {
			select {
			case e := <-r.mu.catchUpBuf:
				r.stream.Send(e.event, e.alloc)
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		}
	}

	// First we drain without lock to avoid unnecessary blocking of publish.
	if err := drainCurrent(); err != nil {
		return err
	}
	// Now drain again under lock to ensure we don't leave behind events written
	// while we were draining.
	r.mu.Lock()
	defer r.mu.Unlock()
	err := drainCurrent()
	r.mu.catchUpScanCancelFn = nil
	if err != nil {
		return err
	}
	if r.mu.catchUpOverflow {
		err = newErrBufferCapacityExceeded().GoError()
	}
	if err == nil {
		// If we successfully wrote buffer, and no data was lost because of overflow
		// enable normal writes.
		r.mu.catchUpBuf = nil
	}
	return err
}

func (r *nonBufferedRegistration) runCatchupScanLoop(
	ctx context.Context, _forStacks roachpb.RangeID,
) {
	// TODO(wenyihu6): check if logic here makes sense (check deadlock)
	r.mu.Lock()

	if r.mu.catchUpIter == nil || r.mu.disconnected {
		// TODO(wenyihu6): check what should be done for disconnect The registration
		// has already been disconnected. we will rely on the disconnect to drain
		// catch up buffer?
		// TODO(wenyihu6): recheck shutdown logic
		r.mu.Unlock()
		return
	}

	ctx, r.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()

	start := timeutil.Now()
	defer func() {
		r.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	// TODO(wenyihu6) does this catch up scan get cancelled if ctx is cancelled
	err := r.mu.catchUpIter.CatchUpScan(ctx, r.stream.SendUnbuffered, r.withDiff, r.withFiltering)
	// If catchup is successful, publish pending event accumulated from processor.
	if err == nil {
		err = r.publishCatchUpBuffer(ctx)
	}

	// TODO(wenyihu6): check again here and see if we can make clean up logic
	// nicer
	if err != nil {
		r.discardCatchUpBuffer(ctx)
		// Send error so that downstream knows stream failed just in case.
		r.stream.SendError(kvpb.NewError(err))
	}
}

// ID implements interval.Interface.
func (r *nonBufferedRegistration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *nonBufferedRegistration) Range() interval.Range {
	return r.keys
}

func (r nonBufferedRegistration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchUpTimestamp)
}

func (r *nonBufferedRegistration) needsPrev() bool {
	return r.withDiff
}

// registry holds a set of registrations and manages their lifecycle.
type nonBufferedRegistry struct {
	metrics *Metrics
	tree    interval.Tree // *registration items
	idAlloc int64
}

func makeNonBufferedRegistry(metrics *Metrics) nonBufferedRegistry {
	return nonBufferedRegistry{
		metrics: metrics,
		tree:    interval.NewTree(interval.ExclusiveOverlapper),
	}
}

func (reg *nonBufferedRegistry) Len() int {
	return reg.tree.Len()
}

func (reg *nonBufferedRegistry) NewFilter() *Filter {
	return newFilterFromFilterTree(reg.tree)
}

func (reg *nonBufferedRegistry) Register(ctx context.Context, r *nonBufferedRegistration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		// TODO(erikgrinaker): these errors should arguably be returned.
		log.Fatalf(ctx, "%v", err)
	}
}

func (reg *nonBufferedRegistry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

func (reg *nonBufferedRegistry) PublishToOverlapping(
	ctx context.Context,
	span roachpb.Span,
	event *kvpb.RangeFeedEvent,
	omitInRangefeeds bool,
	alloc *SharedBudgetAllocation,
) {

	var minTS hlc.Timestamp
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		minTS = t.Value.Timestamp
	case *kvpb.RangeFeedSSTable:
		minTS = t.WriteTS
	case *kvpb.RangeFeedDeleteRange:
		minTS = t.Timestamp
	case *kvpb.RangeFeedCheckpoint:
		// Always publish checkpoint notifications, regardless of a registration's
		// starting timestamp.
		//
		// TODO(dan): It's unclear if this is the right contract, it's certainly
		// surprising. Revisit this once RangeFeed has more users.
		minTS = hlc.MaxTimestamp
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}

	reg.forOverlappingRegs(ctx, span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
		if r.catchUpTimestamp.Less(minTS) && !(r.withFiltering && omitInRangefeeds) {
			r.publish(ctx, event, alloc)
		}
		return false, nil
	})
}

func (reg *nonBufferedRegistry) Unregister(ctx context.Context, r *nonBufferedRegistration) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		panic(err)
	}
	// TODO(wenyihu6): check if we should drain allocations from catch up scans here
	r.drainAllocations(ctx)
}

func (reg *nonBufferedRegistry) DisconnectAllOnShutdown(ctx context.Context, pErr *kvpb.Error) {
	reg.metrics.RangeFeedRegistrations.Dec(int64(reg.tree.Len()))
	reg.DisconnectWithErr(ctx, all, pErr)
}

func (reg *nonBufferedRegistry) Disconnect(ctx context.Context, span roachpb.Span) {
	reg.DisconnectWithErr(ctx, span, nil /* pErr */)
}

func (reg *nonBufferedRegistry) DisconnectWithErr(
	ctx context.Context, span roachpb.Span, pErr *kvpb.Error,
) {
	reg.forOverlappingRegs(ctx, span, func(r *nonBufferedRegistration) (bool, *kvpb.Error) {
		return true /* disconned */, pErr
	})
}

func (reg *nonBufferedRegistry) forOverlappingRegs(
	ctx context.Context,
	span roachpb.Span,
	fn func(*nonBufferedRegistration) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(*nonBufferedRegistration)
		dis, pErr := fn(r)
		if dis {
			r.disconnect(pErr)
			toDelete = append(toDelete, i)
		}
		return false
	}
	if span.EqualValue(all) {
		reg.tree.Do(matchFn)
	} else {
		reg.tree.DoMatching(matchFn, span.AsRange())
	}

	if len(toDelete) == reg.tree.Len() {
		reg.tree.Clear()
	} else if len(toDelete) == 1 {
		if err := reg.tree.Delete(toDelete[0], false /* fast */); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
	} else if len(toDelete) > 1 {
		for _, i := range toDelete {
			if err := reg.tree.Delete(i, true /* fast */); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}
		reg.tree.AdjustRanges()
	}
}

func (r *nonBufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchUpIter := r.mu.catchUpIter
	r.mu.catchUpIter = nil
	return catchUpIter
}
