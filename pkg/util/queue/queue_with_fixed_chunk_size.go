// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import "sync"

const fixedChunkSize = 4000

func sharedEventQueueChunkSyncPool[T any]() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(queueChunkWithFixedSize[T])
		},
	}
}

func getPooledEventQueueChunk[T any](p *sync.Pool) *queueChunkWithFixedSize[T] {
	return p.Get().(*queueChunkWithFixedSize[T])
}

func putPooledEventQueueChunk[T any](p *sync.Pool, e *queueChunkWithFixedSize[T]) {
	*e = queueChunkWithFixedSize[T]{}
	p.Put(e)
}

// QueueWithFixedChunkSize is a FIFO queue implemented as a chunked linked list.
// Chunk size is fixed to help pre-allocate memory and amortize costs of
// allocations.
type QueueWithFixedChunkSize[T any] struct {
	pool       *sync.Pool
	head, tail *queueChunkWithFixedSize[T]
	eventCount int64
}

// NewQueueWithFixedChunkSize returns a QueueWithFixedChunkSize of T.
func NewQueueWithFixedChunkSize[T any]() *QueueWithFixedChunkSize[T] {
	return &QueueWithFixedChunkSize[T]{
		pool: sharedEventQueueChunkSyncPool[T](),
	}
}

func (q *QueueWithFixedChunkSize[T]) Len() int64 {
	return q.eventCount
}

// Enqueue adds an element to the back of the queue.
func (q *QueueWithFixedChunkSize[T]) Enqueue(e T) {
	defer func() {
		q.eventCount++
	}()
	if q.tail == nil {
		chunk := getPooledEventQueueChunk[T](q.pool)
		q.head, q.tail = chunk, chunk
		q.head.push(e) // guaranteed to insert into new chunk
		return
	}

	if !q.tail.push(e) {
		chunk := getPooledEventQueueChunk[T](q.pool)
		q.tail.next = chunk
		q.tail = chunk
		q.tail.push(e) // guaranteed to insert into new chunk
	}
}

// Empty returns true IFF the Queue is empty.
func (q *QueueWithFixedChunkSize[T]) Empty() bool {
	return q.eventCount == 0
}

// Dequeue removes an element from the front of the queue and returns it.
func (q *QueueWithFixedChunkSize[T]) Dequeue() (e T, ok bool) {
	if q.head == nil {
		return e, false
	}

	e, ok = q.head.pop()
	if !ok {
		return e, false
	}

	// If everything has been consumed from the chunk, remove it.
	if q.head.finished() {
		if q.tail == q.head {
			q.tail = q.head.next
		}
		removed := q.head
		q.head = q.head.next
		// The previous value of q.head will be garbage collected.
		putPooledEventQueueChunk[T](q.pool, removed)
	}
	q.eventCount--
	return e, true
}

func (q *QueueWithFixedChunkSize[T]) removeAll(callback func(e T)) {
	for q.head != nil {
		q.head.clearAll(func(e T) {
			callback(e)
			q.eventCount--
		})
		remove := q.head
		q.head = q.head.next
		// The previous value of q.head will be garbage collected.
		putPooledEventQueueChunk[T](q.pool, remove)
	}
	q.tail = q.head
}

func (q *QueueWithFixedChunkSize[T]) purge() {
	q.removeAll(func(e T) {})
}

type queueChunkWithFixedSize[T any] struct {
	events     [fixedChunkSize]T
	head, tail int
	next       *queueChunkWithFixedSize[T] // linked-list element
}

func (c *queueChunkWithFixedSize[T]) clearAll(callback func(e T)) {
	for i := c.head; i < c.tail; i++ {
		callback(c.events[i])
		var zeroValue T
		c.events[i] = zeroValue
	}
}

func (c *queueChunkWithFixedSize[T]) push(e T) (inserted bool) {
	if c.tail == len(c.events) {
		return false
	}

	c.events[c.tail] = e
	c.tail++
	return true
}

func (c *queueChunkWithFixedSize[T]) pop() (e T, ok bool) {
	if c.head == c.tail {
		return e, false
	}

	e = c.events[c.head]
	var zeroValue T
	// Clear the value to make the data garbage collectable.
	c.events[c.head] = zeroValue
	c.head++
	return e, true
}

// finished returns true if all events have been inserted and consumed from the chunk.
func (c *queueChunkWithFixedSize[T]) finished() bool {
	return c.head == len(c.events)
}

func (c *queueChunkWithFixedSize[T]) empty() bool {
	return c.tail == c.head
}
