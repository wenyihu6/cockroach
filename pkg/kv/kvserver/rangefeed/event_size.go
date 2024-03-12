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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

func rangefeedCheckpointOpMemUsage() int64 {
	// Timestamp is already accounted in rangefeedCheckpointOpMemUsage. Ignore
	// bytes under checkpoint.span here since it comes from p.Span which always
	// points at the same underlying data.
	return futureEventBaseOverhead + rangefeedCheckpointOverhead
}

func (ct ctEvent) futureMemUsage() int64 {
	return rangefeedCheckpointOpMemUsage()
}

func (ct ctEvent) currMemUsage() int64 {
	return 0
}

func (initRTS initRTSEvent) currMemUsage() int64 {
	return 0
}

func (initRTS initRTSEvent) futureMemUsage() int64 {
	return rangefeedCheckpointOpMemUsage()
}

func (sst sstEvent) currMemUsage() int64 {
	return sstEventOverhead + int64(cap(sst.data)+cap(sst.span.Key)+cap(sst.span.EndKey))
}

func (sst sstEvent) futureMemUsage() int64 {
	return futureEventBaseOverhead + rangefeedSSTTableOverhead + int64(cap(sst.data)+cap(sst.span.Key)+cap(sst.span.EndKey))
}

func (sync syncEvent) currMemUsage() int64 {
	return syncEventOverhead
}

func (sync syncEvent) futureMemUsage() int64 {
	return 0
}

func rangefeedWriteValueOpMemUsage(key roachpb.Key, value, prevValue []byte) int64 {
	// Key, Timestamp are already accounted in rangefeedValueOverhead.
	memUsage := futureEventBaseOverhead + rangefeedValueOverhead
	memUsage += int64(cap(key))
	memUsage += int64(cap(value))
	memUsage += int64(cap(prevValue))
	return memUsage
}

func rangefeedDeleteRangeOpMemUsage(startKey, endKey roachpb.Key) int64 {
	memUsage := futureEventBaseOverhead + rangefeedDeleteRangeOverhead
	// Timestamp is already accounted in rangefeedDeleteRangeOpMemUsage.
	memUsage += int64(cap(startKey))
	memUsage += int64(cap(endKey))
	return memUsage
}

func (ops opsEvent) futureMemUsage() int64 {
	futureMemUsage := int64(0)
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCDeleteRangeOp:
			futureMemUsage += rangefeedDeleteRangeOpMemUsage(t.StartKey, t.EndKey)
		case *enginepb.MVCCCommitIntentOp:
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		}
		futureMemUsage += rangefeedCheckpointOpMemUsage()
	}
	return futureMemUsage
}

func writeValueOpMemUsage(key roachpb.Key, value, prevValue []byte) int64 {
	// Pointer to the MVCCWriteValueOp is already accounted in mvccLogicalOp.
	currMemUsage := mvccWriteValueOp
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	// Base structs for Timestamp and OmitInRangefeeds, are already accounted in
	// mvccWriteValueOp.
	return currMemUsage
}

func deleteRangeOpMemUsage(startKey, endKey roachpb.Key) int64 {
	// Pointer to the MVCCDeleteRangeOp is already accounted in mvccLogicalOp.
	currMemUsage := mvccDeleteRangeOp
	currMemUsage += int64(cap(startKey))
	currMemUsage += int64(cap(endKey))
	// Base struct for Timestamp is already accounted in mvccDeleteRangeOp.
	return currMemUsage
}

func writeIntentOpMemUsage(txnID uuid.UUID, txnKey []byte) int64 {
	currMemUsage := mvccWriteIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(txnKey))
	// TxnIsoLevel, TxnMinTimestamp, Timestamp are already accounted in
	// mvccWriteIntentOp.
	return currMemUsage
}

func updateIntentOpMemUsage(txnID uuid.UUID) int64 {
	currMemUsage := mvccUpdateIntentOp
	currMemUsage += int64(cap(txnID))
	// Timestamp is already accounted in mvccUpdateIntentOp.
	return currMemUsage
}

func commitIntentOpMemUsage(txnID uuid.UUID, key []byte, value []byte, prevValue []byte) int64 {
	currMemUsage := mvccCommitIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	// Base structs for Timestamp and OmitInRangefeeds, are already accounted in
	// mvccCommitIntentOp.
	return currMemUsage
}

func abortIntentOpMemUsage(txnID uuid.UUID) int64 {
	currMemUsage := mvccAbortIntentOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

func abortTxnOpMemUsage(txnID uuid.UUID) int64 {
	currMemUsage := mvccAbortTxnOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

func (ops opsEvent) currMemUsage() int64 {
	currMemUsage := mvccLogicalOp * int64(cap(ops))
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += writeValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += deleteRangeOpMemUsage(t.StartKey, t.EndKey)
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += writeIntentOpMemUsage(t.TxnID, t.TxnKey)
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += updateIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += commitIntentOpMemUsage(t.TxnID, t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += abortIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += abortTxnOpMemUsage(t.TxnID)
		}
	}
	return currMemUsage
}

func (ops opsEvent) currAndFutureMemUsage() (currMemUsage int64, futureMemUsage int64) {
	currMemUsage = mvccLogicalOp * int64(cap(ops))
	futureMemUsage = int64(0)
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			currMemUsage += writeValueOpMemUsage(t.Key, t.Value, t.PrevValue)
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCDeleteRangeOp:
			currMemUsage += deleteRangeOpMemUsage(t.StartKey, t.EndKey)
			futureMemUsage += rangefeedDeleteRangeOpMemUsage(t.StartKey, t.EndKey)
		case *enginepb.MVCCWriteIntentOp:
			currMemUsage += writeIntentOpMemUsage(t.TxnID, t.TxnKey)
		case *enginepb.MVCCUpdateIntentOp:
			currMemUsage += updateIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCCommitIntentOp:
			currMemUsage += commitIntentOpMemUsage(t.TxnID, t.Key, t.Value, t.PrevValue)
			futureMemUsage += rangefeedWriteValueOpMemUsage(t.Key, t.Value, t.PrevValue)
		case *enginepb.MVCCAbortIntentOp:
			currMemUsage += abortIntentOpMemUsage(t.TxnID)
		case *enginepb.MVCCAbortTxnOp:
			currMemUsage += abortTxnOpMemUsage(t.TxnID)
		}
		futureMemUsage += rangefeedCheckpointOpMemUsage()
	}
	return currMemUsage, futureMemUsage
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
func (e event) futureMemUsage() int64 {
	switch {
	case e.ops != nil:
		return e.ops.futureMemUsage()
	case !e.ct.IsEmpty():
		return e.ct.futureMemUsage()
	case bool(e.initRTS):
		// no current extra memory usage
		// may publish checkpoint but we overaccount for now and release later on right after we know we dont need it.
		return e.initRTS.futureMemUsage()
	case e.sst != nil:
		return e.sst.futureMemUsage()
	case e.sync != nil:
		return e.sync.futureMemUsage()
	}
	return 0
}

func EventMemUsage(e event) int64 {
	if e.ops != nil {
		curr, future := e.ops.currAndFutureMemUsage()
		return max(curr, future)
	}
	return max(e.futureMemUsage(), e.currMemUsage())
}
