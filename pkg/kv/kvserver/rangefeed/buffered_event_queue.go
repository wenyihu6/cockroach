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
)

const eventQueueChunkSize = 4000

var sharedEventQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(eventQueueChunk)
	},
}

func newPooledEventQueueChunk() *eventQueueChunk {
	return sharedEventQueueChunkSyncPool.Get().(*eventQueueChunk)
}

func releasePooledEventQueueChunk(e *eventQueueChunk) {
	*e = eventQueueChunk{}
	sharedEventQueueChunkSyncPool.Put(e)
}

type sharedMuxEvent struct {
	event *kvpb.MuxRangeFeedEvent
	alloc *SharedBudgetAllocation
}

type eventQueueChunk struct {
	data      [eventQueueChunkSize]*sharedMuxEvent
	nextChunk *eventQueueChunk
}

type muxEventQueue struct {
	head, tail *eventQueueChunk
	// What we will read and write next.
	read, write int
	eventLen    int
}

func newMuxEventQueue() *muxEventQueue {
	// q.head or q.tail should never be nil
	dummyChunk := new(eventQueueChunk)
	return &muxEventQueue{
		head: dummyChunk,
		tail: dummyChunk,
	}
}

func (q *muxEventQueue) pushback(data *sharedMuxEvent) {
	if q.write == eventQueueChunkSize {
		// Insert a new chunk in the back.
		newChunk := newPooledEventQueueChunk()
		q.tail.nextChunk = newChunk
		q.tail = newChunk
		q.write = 0
	}

	// we can write at q.write now -> assert that we can now write at q.write [0,eventQueueChunkSize)
	q.tail.data[q.write] = data
	q.write += 1
	q.eventLen += 1
}

func (q *muxEventQueue) popfront() (*sharedMuxEvent, bool) {
	if q.eventLen == 0 {
		return nil, false
	}

	if q.read == eventQueueChunkSize {
		removed := q.head
		q.head = q.head.nextChunk
		q.read = 0
		// q.head has to be non-nil since q.eventCount != 0
		// prev q.head should be garbage collected at this point
		releasePooledEventQueueChunk(removed)
	}
	res := q.tail.data[q.read]
	q.tail.data[q.read] = nil // Free data in sharedmux
	q.read++
	q.eventLen--
	return res, true
}

// q.head could be nil now -> cannot do pushback again
// make sure q.head is not empty after removeAll
// q.head should be nil after this
func (q *muxEventQueue) removeAll(ctx context.Context) {
	for {
		e, success := q.popfront()
		if !success {
			return
		}
		e.alloc.Release(ctx)
	}
}
