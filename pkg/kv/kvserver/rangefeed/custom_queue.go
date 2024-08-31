package rangefeed

import (
	"context"
	"sync"
)

const eventQueueChunkSize = 8000

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type queueChunk struct {
	data      [eventQueueChunkSize]sharedMuxEvent
	nextChunk *queueChunk
}

var sharedQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(queueChunk)
	},
}

func getPooledQueueChunk() *queueChunk {
	return sharedQueueChunkSyncPool.Get().(*queueChunk)
}

func putPooledQueueChunk(e *queueChunk) {
	// Don't need to cleanup chunk as it is an array of values.
	e.nextChunk = nil
	sharedQueueChunkSyncPool.Put(e)
}

// idQueue stores pending processor ID's. Internally data is stored in
// idQueueChunkSize sized arrays that are added as needed and discarded once
// reader and writers finish working with it. Since we only have a single
// scheduler per store, we don't use a pool as only reuse could happen within
// the same queue and in that case we can just increase chunk size.
type eventQueue struct {
	first, last *queueChunk
	read, write int
	size        int
}

func newEventQueue() *eventQueue {
	chunk := getPooledQueueChunk()
	return &eventQueue{
		first: chunk,
		last:  chunk,
	}
}

func (q *eventQueue) pushBack(e sharedMuxEvent) {
	if q.write == eventQueueChunkSize {
		nexChunk := getPooledQueueChunk()
		q.last.nextChunk = nexChunk
		q.last = nexChunk
		q.write = 0
	}
	q.last.data[q.write] = e
	q.write++
	q.size++
}

func (q *eventQueue) popFront() (sharedMuxEvent, bool) {
	if q.size == 0 {
		return sharedMuxEvent{}, false
	}
	if q.read == eventQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.read++
	q.size--
	return res, true
}

func (q *eventQueue) removeAll() {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i].alloc.Release(context.Background())
			chunk.data[i] = sharedMuxEvent{}
		}
		next := chunk.nextChunk
		putPooledQueueChunk(chunk)
		chunk = next
		start = 0
	}
	q.first = q.last
	q.read = 0
	q.write = 0
	q.size = 0
}

func (q *eventQueue) Len() int64 {
	return int64(q.size)
}
