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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	mvccLogicalOp      = int64(1)
	mvccWriteValueOp   = int64(1)
	mvccDeleteRangeOp  = int64(1)
	mvccWriteIntentOp  = int64(1)
	mvccUpdateIntentOp = int64(1)
	mvccCommitIntentOp = int64(1)
	mvccAbortIntentOp  = int64(1)
	mvccAbortTxnOp     = int64(1)

	eventPtrOverhead = int64(1)
	eventOverhead    = int64(1)

	sstEventOverhead  = int64(1)
	syncEventOverhead = int64(1)

	// futureEventBaseOverhead accounts for the base struct overhead of
	// sharedEvent{} and its pointer. Each sharedEvent contains a
	// *kvpb.RangeFeedEvent and *SharedBudgetAllocation. futureEventBaseOverhead
	// also accounts for the underlying base struct memory of RangeFeedEvent and
	// SharedBudgetAllocation. Underlying data for SharedBudgetAllocation includes
	// a pointer to the FeedBudget, but that points to the same data structure
	// across all rangefeeds, so we opted out in the calculation.
	sharedEventPtrOverhead  = int64(1)
	sharedEventOverhead     = int64(1)
	rangeFeedEventOverhead  = int64(1)
	allocEventOverhead      = int64(1)
	futureEventBaseOverhead = sharedEventPtrOverhead + sharedEventOverhead + rangeFeedEventOverhead + allocEventOverhead

	rangefeedValueOverhead      = int64(1)
	rangefeedCheckpointOverhead = int64(1)
	rangefeedSSTTableOverhead   = int64(1)
)

// No future memory usages have been accounted so far.
// rangefeedSSTTableOpMemUsage accounts for the entire memory usage of a new
// RangeFeedSSTable event.
func rangefeedSSTTableOpMemUsage(data []byte, startKey, endKey roachpb.Key) int64 {
	// Pointer to RangeFeedSSTable has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedValueOverhead includes the memory usage of the
	// underlying RangeFeedSSTable base struct.
	memUsage := futureEventBaseOverhead + rangefeedSSTTableOverhead

	// RangeFeedSSTable has Data, Span{startKey,endKey}, and WriteTS. Only Data,
	// startKey, and endKey has underlying memory usage in []byte. Timestamp and
	// other base structs of Value have no underlying data and are already
	// accounted in rangefeedValueOverhead.
	memUsage += int64(cap(data))
	memUsage += int64(cap(startKey))
	memUsage += int64(cap(endKey))
	return memUsage
}

// No future memory usages have been accounted so far.
// rangefeedCheckpointOpMemUsage accounts for the entire memory usage of a new
// RangeFeedCheckpoint event.
func rangefeedCheckpointOpMemUsage() int64 {
	// Pointer to RangeFeedCheckpoint has already been accounted in
	// futureEventBaseOverhead as part of the base struct overhead of
	// RangeFeedEvent. rangefeedCheckpointOverhead includes the memory usage of
	// the underlying RangeFeedCheckpoint base struct.

	// RangeFeedCheckpoint has Span{p.Span} and Timestamp{rts.Get()}. Timestamp is
	// already accounted in rangefeedCheckpointOverhead. Ignore bytes under
	// checkpoint.span here since it comes from p.Span which always points at the
	// same underlying data.
	return futureEventBaseOverhead + rangefeedCheckpointOverhead
}

// Pointer to the MVCCWriteValueOp was already accounted in mvccLogicalOp in the
// caller. writeValueOpMemUsage accounts for the memory usage of
// MVCCWriteValueOp.
func writeValueOpMemUsage(key roachpb.Key, value, prevValue []byte) int64 {
	// MVCCWriteValueOp has Key, Timestamp, Value, PrevValue, OmitInRangefeeds.
	// Only key, value, and prevValue has underlying memory usage in []byte.
	// Timestamp and OmitInRangefeeds have no underlying data and are already
	// accounted in MVCCWriteValueOp.
	currMemUsage := mvccWriteValueOp
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	return currMemUsage
}

// Pointer to the MVCCDeleteRangeOp was already accounted in mvccLogicalOp in
// the caller. deleteRangeOpMemUsage accounts for the memory usage of
// MVCCDeleteRangeOp.
func deleteRangeOpMemUsage(startKey, endKey roachpb.Key) int64 {
	// MVCCDeleteRangeOp has StartKey, EndKey, and Timestamp. Only StartKey and
	// EndKey has underlying memory usage in []byte. Timestamp has no underlying
	// data and was already accounted in MVCCDeleteRangeOp.
	currMemUsage := mvccDeleteRangeOp
	currMemUsage += int64(cap(startKey))
	currMemUsage += int64(cap(endKey))
	return currMemUsage
}

// Pointer to the MVCCWriteIntentOp was already accounted in mvccLogicalOp in
// the caller. writeIntentOpMemUsage accounts for the memory usage of
// MVCCWriteIntentOp.
func writeIntentOpMemUsage(txnID uuid.UUID, txnKey []byte) int64 {
	// MVCCWriteIntentOp has TxnID, TxnKey, TxnIsoLevel, TxnMinTimestamp, and
	// Timestamp. Only TxnID and TxnKey has underlying memory usage in []byte.
	// TxnIsoLevel, TxnMinTimestamp, and Timestamp have no underlying data and was
	// already accounted in MVCCWriteIntentOp.
	currMemUsage := mvccWriteIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(txnKey))
	return currMemUsage
}

// Pointer to the MVCCUpdateIntentOp was already accounted in mvccLogicalOp in
// the caller. updateIntentOpMemUsage accounts for the memory usage of
// MVCCUpdateIntentOp.
func updateIntentOpMemUsage(txnID uuid.UUID) int64 {
	// MVCCUpdateIntentOp has TxnID and Timestamp. Only TxnID has underlying
	// memory usage in []byte. Timestamp has no underlying data and was already
	// accounted in MVCCUpdateIntentOp.
	currMemUsage := mvccUpdateIntentOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// Pointer to the MVCCCommitIntentOp was already accounted in mvccLogicalOp in
// the caller. commitIntentOpMemUsage accounts for the memory usage of
// MVCCCommitIntentOp.
func commitIntentOpMemUsage(txnID uuid.UUID, key []byte, value []byte, prevValue []byte) int64 {
	// MVCCCommitIntentOp has TxnID, Key, Timestamp, Value, PrevValue, and
	// OmintInRangefeeds. Only TxnID, Key, Value, and PrevValue has underlying
	// memory usage in []byte. Timestamp and OmintInRangefeeds have no underlying
	// data and was already accounted in MVCCCommitIntentOp.
	currMemUsage := mvccCommitIntentOp
	currMemUsage += int64(cap(txnID))
	currMemUsage += int64(cap(key))
	currMemUsage += int64(cap(value))
	currMemUsage += int64(cap(prevValue))
	return currMemUsage
}

// Pointer to the MVCCAbortIntentOp was already accounted in mvccLogicalOp in
// the caller. abortIntentOpMemUsage accounts for the memory usage of
// MVCCAbortIntentOp.
func abortIntentOpMemUsage(txnID uuid.UUID) int64 {
	// MVCCAbortIntentOp has TxnID which has underlying memory usage in []byte.
	currMemUsage := mvccAbortIntentOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// Pointer to the MVCCAbortTxnOp was already accounted in mvccLogicalOp in the
// caller. abortTxnOpMemUsage accounts for the memory usage of MVCCAbortTxnOp.
func abortTxnOpMemUsage(txnID uuid.UUID) int64 {

	// MVCCAbortTxnOp has TxnID which has underlying memory usage in []byte.
	currMemUsage := mvccAbortTxnOp
	currMemUsage += int64(cap(txnID))
	return currMemUsage
}

// eventOverhead accounts for the base struct of event{} which only included
// pointer to syncEvent. currMemUsage accounts the base struct memory of
// syncEvent{} and its underlying memory.
func (sync syncEvent) currMemUsage() int64 {
	// syncEvent has a channel and a pointer to testRegCatchupSpan.
	// testRegCatchupSpan is never set in production. Channel sent is always
	// struct{}, so the memory has already been accounted in syncEventOverhead.
	return syncEventOverhead
}

// currMemUsage returns the current memory usage of the opsEvent including base
// structs overhead and underlying memory usage.
func (ops opsEvent) currMemUsage() int64 {
	// currMemUsage: eventOverhead already accounts for slice overhead in
	// opsEvent, []enginepb.MVCCLogicalOp. For each cap(ops), the underlying
	// memory include a MVCCLogicalOp overhead.
	currMemUsage := mvccLogicalOp * int64(cap(ops))
	for _, op := range ops {
		switch t := op.GetValue().(type) {
		// currMemUsage: for each op, the pointer to the op is already accounted in
		// mvccLogicalOp. We now account for the underlying memory usage inside the
		// op below.
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
		default:
			log.Fatalf(context.Background(), "unknown logical op %T", t)
		}
	}
	// For each op, a checkpoint may or may not be published depending on whether
	// the operation caused resolved timestamp to update. Since these are very
	// rare, we disregard them to avoid the complexity.
	return currMemUsage
}

func (e *event) String() string {
	if e == nil {
		return ""
	}
	switch {
	case e.ops != nil:
		str := strings.Builder{}
		str.WriteString("event: logicalops\n")
		for _, op := range e.ops {
			switch t := op.GetValue().(type) {
			case *enginepb.MVCCWriteValueOp, *enginepb.MVCCDeleteRangeOp, *enginepb.MVCCWriteIntentOp,
				*enginepb.MVCCUpdateIntentOp, *enginepb.MVCCCommitIntentOp, *enginepb.MVCCAbortIntentOp, *enginepb.MVCCAbortTxnOp:
				str.WriteString(fmt.Sprintf("op: %T\n", t))
			default:
				str.WriteString("unknown logical op")
			}
		}
		return str.String()
	case !e.ct.IsEmpty():
		return "event: checkpoint"
	case bool(e.initRTS):
		return "event: initrts"
	case e.sst != nil:
		return "event: sst"
	case e.sync != nil:
		return "event: sync"
	default:
		return "missing event variant"
	}
}

// MemUsage estimates the total memory usage of the event, including its
// underlying data. The memory usage is estimated in bytes.
func (e *event) MemUsage() int64 {
	panic("not expected")
}
