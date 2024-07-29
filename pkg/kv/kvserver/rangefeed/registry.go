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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	kvpb.RangeFeedEventSink
	// Disconnect disconnects the stream with the provided error. Note that this
	// function can be called by the processor worker while holding raftMu, so it
	// is important that this function doesn't block IO or try acquiring locks
	// that could lead to deadlocks.
	Disconnect(err *kvpb.Error)
	RegisterRangefeedCleanUp(func())
}

// Shared event is an entry stored in registration channel. Each entry is
// specific to registration but allocation is shared between all registrations
// to track memory budgets. event itself could either be shared or not in case
// we optimized unused fields in it based on registration options.
type sharedEvent struct {
	event *kvpb.RangeFeedEvent
	alloc *SharedBudgetAllocation
}

var sharedEventSyncPool = sync.Pool{
	New: func() interface{} {
		return new(sharedEvent)
	},
}

func getPooledSharedEvent(e sharedEvent) *sharedEvent {
	ev := sharedEventSyncPool.Get().(*sharedEvent)
	*ev = e
	return ev
}

func putPooledSharedEvent(e *sharedEvent) {
	*e = sharedEvent{}
	sharedEventSyncPool.Put(e)
}

// registry holds a set of registrations and manages their lifecycle.
type registry struct {
	metrics *Metrics
	tree    interval.Tree // registration items
	idAlloc int64
}

func makeRegistry(metrics *Metrics) registry {
	return registry{
		metrics: metrics,
		tree:    interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// Len returns the number of registrations in the registry.
func (reg *registry) Len() int {
	return reg.tree.Len()
}

// NewFilter returns a operation filter reflecting the registrations
// in the registry.
func (reg *registry) NewFilter() *Filter {
	return newFilterFromRegistry(reg)
}

// Register adds the provided registration to the registry.
func (reg *registry) Register(ctx context.Context, r registration) {
	reg.metrics.RangeFeedRegistrations.Inc(1)
	r.setID(reg.nextID())
	r.setSpanAsKeys()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		// TODO(erikgrinaker): these errors should arguably be returned.
		log.Fatalf(ctx, "%v", err)
	}
}

func (reg *registry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry) PublishToOverlapping(
	ctx context.Context,
	span roachpb.Span,
	event *kvpb.RangeFeedEvent,
	valueMetadata logicalOpMetadata,
	alloc *SharedBudgetAllocation,
) {
	// Determine the earliest starting timestamp that a registration
	// can have while still needing to hear about this event.
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
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}

	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
		// Don't publish events if they:
		// 1. are equal to or less than the registration's starting timestamp, or
		// 2. have OmitInRangefeeds = true and this registration has opted into filtering, or
		// 3. have OmitRemote = true and this value is from a remote cluster.
		if r.getCatchUpTimestamp().Less(minTS) && !(r.getWithFiltering() && valueMetadata.omitInRangefeeds) && (!r.getWithOmitRemote() || valueMetadata.originID == 0) {
			r.publish(ctx, event, alloc)
		}
		return false, nil
	})
}

// Unregister removes a registration from the registry. It is assumed that the
// registration has already been disconnected, this is intended only to clean
// up the registry.
// We also drain all pending events for the sake of memory accounting. To do
// that we rely on a fact that caller is not going to post any more events
// concurrently or after this function is called.
func (reg *registry) Unregister(ctx context.Context, r registration) {
	reg.metrics.RangeFeedRegistrations.Dec(1)
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	r.drainAllocations(ctx)
}

// DisconnectAllOnShutdown disconnectes all registrations on processor shutdown.
// This is different from normal disconnect as registrations won't be able to
// perform Unregister when processor's work loop is already terminated.
// This method will cleanup metrics controlled by registry itself beside posting
// errors to registrations.
// TODO: this should be revisited as part of
// https://github.com/cockroachdb/cockroach/issues/110634
func (reg *registry) DisconnectAllOnShutdown(ctx context.Context, pErr *kvpb.Error) {
	reg.metrics.RangeFeedRegistrations.Dec(int64(reg.tree.Len()))
	reg.DisconnectWithErr(ctx, all, pErr)
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry) Disconnect(ctx context.Context, span roachpb.Span) {
	reg.DisconnectWithErr(ctx, span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(ctx context.Context, span roachpb.Span, pErr *kvpb.Error) {
	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
		return true /* disconned */, pErr
	})
}

// all is a span that overlaps with all registrations.
var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *registry) forOverlappingRegs(
	ctx context.Context, span roachpb.Span, fn func(registration) (disconnect bool, pErr *kvpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(registration)
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

// waitForCaughtUp waits for all registrations overlapping the given span to
// completely process their internal buffers.
func (reg *registry) waitForCaughtUp(ctx context.Context, span roachpb.Span) error {
	var outerErr error
	reg.forOverlappingRegs(ctx, span, func(r registration) (bool, *kvpb.Error) {
		if outerErr == nil {
			outerErr = r.waitForCaughtUp(ctx)
		}
		return false, nil
	})
	return outerErr
}

// assertEvent asserts that the event contains the necessary data.
func assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
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
