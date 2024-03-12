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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

const (
	mvccLogicalOp      = int64(unsafe.Sizeof(enginepb.MVCCLogicalOp{}))
	mvccWriteValueOp   = int64(unsafe.Sizeof(enginepb.MVCCWriteValueOp{}))
	mvccDeleteRangeOp  = int64(unsafe.Sizeof(enginepb.MVCCDeleteRangeOp{}))
	mvccWriteIntentOp  = int64(unsafe.Sizeof(enginepb.MVCCWriteIntentOp{}))
	mvccUpdateIntentOp = int64(unsafe.Sizeof(enginepb.MVCCUpdateIntentOp{}))
	mvccCommitIntentOp = int64(unsafe.Sizeof(enginepb.MVCCCommitIntentOp{}))
	mvccAbortIntentOp  = int64(unsafe.Sizeof(enginepb.MVCCAbortIntentOp{}))
	mvccAbortTxnOp     = int64(unsafe.Sizeof(enginepb.MVCCAbortTxnOp{}))
)

const (
	eventOverhead = int64(unsafe.Sizeof(&event{})) + int64(unsafe.Sizeof(event{}))
)

const (
	sharedEventPtrOverhead  = int64(unsafe.Sizeof(&sharedEvent{}))
	sharedEventOverhead     = int64(unsafe.Sizeof(sharedEvent{}))
	rangeFeedEventOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedEvent{}))
	allocEventOverhead      = int64(unsafe.Sizeof(SharedBudgetAllocation{}))
	feedBudgetOverhead      = int64(unsafe.Sizeof(FeedBudget{}))
	futureEventBaseOverhead = sharedEventPtrOverhead + sharedEventOverhead + rangeFeedEventOverhead + allocEventOverhead + feedBudgetOverhead
)

const (
	rangefeedValueOverhead       = int64(unsafe.Sizeof(kvpb.RangeFeedValue{}))
	rangefeedDeleteRangeOverhead = int64(unsafe.Sizeof(kvpb.RangeFeedDeleteRange{}))
	rangefeedCheckpointOverhead  = int64(unsafe.Sizeof(kvpb.RangeFeedCheckpoint{}))
	rangefeedSSTTableOverhead    = int64(unsafe.Sizeof(kvpb.RangeFeedSSTable{}))
)

const (
	sstEventOverhead  = int64(unsafe.Sizeof(sstEvent{}))
	syncEventOverhead = int64(unsafe.Sizeof(syncEvent{}))
)

func newDeleteRangeFeedEvent(
	startKey, endKey roachpb.Key, timestamp hlc.Timestamp,
) kvpb.RangeFeedEvent {
	span := roachpb.Span{Key: startKey, EndKey: endKey}
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedDeleteRange{
		Span:      span,
		Timestamp: timestamp,
	})
	return event
}

func newValueRangeFeedEvent(
	key roachpb.Key, timestamp hlc.Timestamp, value, prevValue []byte,
) kvpb.RangeFeedEvent {
	var prevVal roachpb.Value
	if prevValue != nil {
		prevVal.RawBytes = prevValue
	}
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
		PrevValue: prevVal,
	})
	return event
}

func newCheckpointRangeFeedEvent(span roachpb.RSpan, ts resolvedTimestamp) kvpb.RangeFeedEvent {
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       span.AsRawSpanWithNoLocals(),
		ResolvedTS: ts.Get(),
	})
	return event
}

func newSSTRangeFeedEvent(
	sst []byte, sstSpan roachpb.Span, sstWTS hlc.Timestamp,
) kvpb.RangeFeedEvent {
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedSSTable{
		Data:    sst,
		Span:    sstSpan,
		WriteTS: sstWTS,
	})
	return event
}

func (ct ctEvent) futureMemUsage(span roachpb.RSpan, rts resolvedTimestamp) int64 {
	e := newCheckpointRangeFeedEvent(span, rts)
	return RangefeedEventMemUsage(&e)
}

func (ct ctEvent) currMemUsage() int64 {
	return 0
}

func (initRTS initRTSEvent) currMemUsage() int64 {
	return 0
}

func (initRTS initRTSEvent) futureMemUsage(span roachpb.RSpan, rts resolvedTimestamp) int64 {
	e := newCheckpointRangeFeedEvent(span, rts)
	return RangefeedEventMemUsage(&e)
}

func (sst sstEvent) currMemUsage() int64 {
	return sstEventOverhead + int64(cap(sst.data)+cap(sst.span.Key)+cap(sst.span.EndKey))
}

func (sst sstEvent) futureMemUsage() int64 {
	e := newSSTRangeFeedEvent(sst.data, sst.span, sst.ts)
	return RangefeedEventMemUsage(&e)
}

func (sync syncEvent) currMemUsage() int64 {
	return syncEventOverhead
}

func (sync syncEvent) futureMemUsage() int64 {
	return 0
}

func (ops opsEvent) futureMemUsage(span roachpb.RSpan, rts resolvedTimestamp) int64 {
	futureMemUsage := int64(0)
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			e := newValueRangeFeedEvent(t.Key, t.Timestamp, t.Value, t.PrevValue)
			futureMemUsage += RangefeedEventMemUsage(&e)
		case *enginepb.MVCCDeleteRangeOp:
			e := newDeleteRangeFeedEvent(t.StartKey, t.EndKey, t.Timestamp)
			futureMemUsage += RangefeedEventMemUsage(&e)
		case *enginepb.MVCCCommitIntentOp:
			e := newValueRangeFeedEvent(t.Key, t.Timestamp, t.Value, t.PrevValue)
			futureMemUsage += RangefeedEventMemUsage(&e)
		}
		e := newCheckpointRangeFeedEvent(span, rts)
		futureMemUsage += RangefeedEventMemUsage(&e)
	}
	return futureMemUsage
}

func (ops opsEvent) currMemUsage() int64 {
	currMemUsage := mvccLogicalOp * int64(cap(ops))
	for _, op := range ops {
		currMemUsage += int64(op.Size())
		switch op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += mvccWriteValueOp
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += mvccDeleteRangeOp
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += mvccWriteIntentOp
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += mvccUpdateIntentOp
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += mvccCommitIntentOp
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += mvccAbortIntentOp
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += mvccAbortTxnOp
		}
	}
	return currMemUsage
}

func RangefeedEventMemUsage(re *kvpb.RangeFeedEvent) int64 {
	currMemUsage := futureEventBaseOverhead
	switch re.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		currMemUsage += rangefeedValueOverhead + int64(re.Size())
	case *kvpb.RangeFeedDeleteRange:
		currMemUsage += rangefeedDeleteRangeOverhead + int64(re.Size())
	case *kvpb.RangeFeedSSTable:
		currMemUsage += rangefeedSSTTableOverhead + int64(re.Size())
	case *kvpb.RangeFeedCheckpoint:
		currMemUsage += rangefeedCheckpointOverhead + int64(re.Size())
	}
	return currMemUsage
}

// currMemUsage returns the current memory usage of event.
func (e event) currMemUsage() int64 {
	currMemUsage := eventOverhead
	switch {
	case e.ops != nil:
		currMemUsage += e.ops.currMemUsage()
	case !e.ct.IsEmpty():
		currMemUsage += e.ct.currMemUsage()
	case bool(e.initRTS):
		currMemUsage += e.initRTS.currMemUsage()
	case e.sst != nil:
		currMemUsage += e.sst.currMemUsage()
	case e.sync != nil:
		currMemUsage += e.sync.currMemUsage()
	}
	return currMemUsage
}

// futureMemUsage returns the predicted memory usage created in the future by
// event. Note that we are not sure if the checkpoint events will be published.
// Our strategy is to overaccount since it will be released soon after
// consumeEvent returns in processEvents and no other registrations have a
// reference to alloc. We determine the future memory usage by following the
// same code path and creating mock rangefeed events.
func (e event) futureMemUsage(span roachpb.RSpan, rts resolvedTimestamp) int64 {
	switch {
	case e.ops != nil:
		return e.ops.futureMemUsage(span, rts)
	case !e.ct.IsEmpty():
		return e.ct.futureMemUsage(span, rts)
	case bool(e.initRTS):
		// no current extra memory usage
		// may publish checkpoint but we overaccount for now and release later on right after we know we dont need it.
		return e.initRTS.futureMemUsage(span, rts)
	case e.sst != nil:
		return e.sst.futureMemUsage()
	case e.sync != nil:
		return e.sync.futureMemUsage()
	}
	return 0
}

func EventMemUsage(e event, span roachpb.RSpan, rts resolvedTimestamp) int64 {
	return max(e.futureMemUsage(span, rts), e.currMemUsage())
}
