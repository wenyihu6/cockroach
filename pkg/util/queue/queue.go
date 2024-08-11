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

// An Option is a configurable parameter of the queue.
type Option[T any] func(q *Queue[T]) error

func (o Option[T]) apply(q *Queue[T]) error {
	return o(q)
}

func sharedEventQueueChunkSyncPool[T any]() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(queueChunk[T])
		},
	}
}

func getPooledEventQueueChunk[T any](p *sync.Pool) *queueChunk[T] {
	return p.Get().(*queueChunk[T])
}

func putPooledEventQueueChunk[T any](p *sync.Pool, e *queueChunk[T]) {
	*e = queueChunk[T]{}
	p.Put(e)
}

// Queue is a FIFO queue implemented as a chunked linked list. The default chunk
// size is 128.
type Queue[T any] struct {
	pool       *sync.Pool
	head, tail *queueChunk[T]
}

// NewQueue returns a Queue of T.
func NewQueue[T any](opts ...Option[T]) (*Queue[T], error) {
	q := &Queue[T]{
		pool: sharedEventQueueChunkSyncPool[T](),
	}
	for _, opt := range opts {
		if err := opt.apply(q); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// Enqueue adds an element to the back of the queue.
func (q *Queue[T]) Enqueue(e T) {
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
func (q *Queue[T]) Empty() bool {
	return q.head == nil || q.head.empty()
}

// Dequeue removes an element from the front of the queue and returns it.
func (q *Queue[T]) Dequeue() (e T, ok bool) {
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
		putPooledEventQueueChunk(q.pool, removed)
	}

	return e, true
}

func (q *Queue[T]) purge() {
	for q.head != nil {
		remove := q.head
		q.head = q.head.next
		// The previous value of q.head will be garbage collected.
		putPooledEventQueueChunk(q.pool, remove)
	}
	q.tail = q.head
}

const eventSize = 1000

type queueChunk[T any] struct {
	events     [eventSize]T
	head, tail int
	next       *queueChunk[T] // linked-list element
}

func (c *queueChunk[T]) push(e T) (inserted bool) {
	if c.tail == len(c.events) {
		return false
	}

	c.events[c.tail] = e
	c.tail++
	return true
}

func (c *queueChunk[T]) pop() (e T, ok bool) {
	if c.head == c.tail {
		return e, false
	}

	e = c.events[c.head]
	var zeroValue T
	// Clear the value to help the GC.
	c.events[c.head] = zeroValue
	c.head++
	return e, true
}

// finished returns true if all events have been inserted and consumed from the chunk.
func (c *queueChunk[T]) finished() bool {
	return c.head == len(c.events)
}

func (c *queueChunk[T]) empty() bool {
	return c.tail == c.head
}
