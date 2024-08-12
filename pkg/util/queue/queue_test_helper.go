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

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
)

type testQueueItem struct {
	i int64
}

type testQueueInterface interface {
	Enqueue(*testQueueItem)
	Empty() bool
	Dequeue() (*testQueueItem, bool)
	purge()
	checkInvariants(*testing.T)
	checkNil(*testing.T)
}

func (q *Queue[T]) checkNil(t *testing.T) {
	require.Nil(t, q.head)
	require.Nil(t, q.tail)
}

func (q *QueueWithFixedChunkSize[T]) checkNil(t *testing.T) {
	require.Nil(t, q.head)
	require.Nil(t, q.tail)
}

func (q *Queue[T]) checkInvariants(t *testing.T) {
	if q.head == nil && q.tail == nil {
		require.True(t, q.Empty())
	} else if q.head != nil && q.tail == nil {
		t.Fatal("head is nil but tail is non-nil")
	} else if q.head == nil && q.tail != nil {
		t.Fatal("tail is nil but head is non-nil")
	} else {
		// The queue maintains an invariant that it contains no finished chunks.
		require.False(t, q.head.finished())
		require.False(t, q.tail.finished())

		if q.head == q.tail {
			require.Nil(t, q.head.next)
		} else {
			// q.tail is non-nil and not equal to q.head. There must be a non-empty
			// chunk after q.head.
			require.False(t, q.Empty())
			if q.head.empty() {
				require.False(t, q.head.next.empty())
			}
			// The q.tail is never empty in this case. The tail can only be empty
			// when it is equal to q.head and q.head is empty.
			require.False(t, q.tail.empty())
		}
	}
}

func (q *QueueWithFixedChunkSize[T]) checkInvariants(t *testing.T) {
	if q.head == nil && q.tail == nil {
		require.True(t, q.Empty())
	} else if q.head != nil && q.tail == nil {
		t.Fatal("head is nil but tail is non-nil")
	} else if q.head == nil && q.tail != nil {
		t.Fatal("tail is nil but head is non-nil")
	} else {
		// The queue maintains an invariant that it contains no finished chunks.
		require.False(t, q.head.finished())
		require.False(t, q.tail.finished())

		if q.head == q.tail {
			require.Nil(t, q.head.next)
		} else {
			// q.tail is non-nil and not equal to q.head. There must be a non-empty
			// chunk after q.head.
			require.False(t, q.Empty())
			if q.head.empty() {
				require.False(t, q.head.next.empty())
			}
			// The q.tail is never empty in this case. The tail can only be empty
			// when it is equal to q.head and q.head is empty.
			require.False(t, q.tail.empty())
		}
	}
}

func runQueueTest(t *testing.T, q testQueueInterface, eventCount int) {
	rng, _ := randutil.NewTestRand()
	// Add one event and remove it
	assert.True(t, q.Empty())
	q.Enqueue(&testQueueItem{})
	assert.False(t, q.Empty())
	_, ok := q.Dequeue()
	assert.True(t, ok)
	assert.True(t, q.Empty())

	// Fill events and then pop each one, ensuring empty() returns the correct
	// value each time.
	q.checkInvariants(t)
	for i := 0; i < eventCount; i++ {
		q.Enqueue(&testQueueItem{})
	}
	q.checkInvariants(t)
	for {
		assert.Equal(t, eventCount <= 0, q.Empty())
		_, ok = q.Dequeue()
		if !ok {
			assert.True(t, q.Empty())
			break
		} else {
			eventCount--
		}
		q.checkInvariants(t)
	}
	assert.Equal(t, 0, eventCount)
	q.Enqueue(&testQueueItem{})
	assert.False(t, q.Empty())
	q.Dequeue()
	assert.True(t, q.Empty())

	// Add events and assert they are consumed in fifo order.
	var lastPop int64 = -1
	var lastPush int64 = -1
	q.checkInvariants(t)
	for eventCount > 0 {
		op := rng.Intn(5)
		if op < 3 {
			q.Enqueue(&testQueueItem{i: lastPush + 1})
			lastPush++
		} else {
			e, ok := q.Dequeue()
			if !ok {
				assert.Equal(t, lastPop, lastPush)
				assert.True(t, q.Empty())
			} else {
				assert.Equal(t, lastPop+1, e.i)
				lastPop++
				eventCount--
			}
		}
		q.checkInvariants(t)
	}

	q.purge()
	q.checkNil(t)
}
