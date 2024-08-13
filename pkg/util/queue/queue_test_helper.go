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
)

type testQueueItem struct {
	i int64
}

type testQueueInterface interface {
	Enqueue(*testQueueItem)
	Empty() bool
	Dequeue() (*testQueueItem, bool)
	purge()
	checkInvariants(*testing.T, int64)
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

func (q *Queue[T]) checkInvariants(t *testing.T, _ int64) {
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

func (q *QueueWithFixedChunkSize[T]) checkInvariants(t *testing.T, eventCount int64) {
	require.Equal(t, eventCount, q.Len())
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
	t.Run("basic operation: add one event and remove it", func(t *testing.T) {
		// Add one event and remove it
		require.True(t, q.Empty())
		q.Enqueue(&testQueueItem{})
		require.False(t, q.Empty())
		_, ok := q.Dequeue()
		require.True(t, ok)
		require.True(t, q.Empty())
		q.checkInvariants(t, 0)
	})

	t.Run("fill and empty queue", func(t *testing.T) {
		for i := 0; i < eventCount; i++ {
			q.Enqueue(&testQueueItem{})
		}
		q.checkInvariants(t, int64(eventCount))
		for eventCount != 0 {
			require.Equal(t, eventCount <= 0, q.Empty())
			_, ok := q.Dequeue()
			require.True(t, ok)
			eventCount--
		}
		require.True(t, q.Empty())
		q.checkInvariants(t, int64(0))
		_, ok := q.Dequeue()
		require.False(t, ok)
		require.True(t, q.Empty())
	})

	t.Run("purge is noop for an empty queue", func(t *testing.T) {
		q.purge()
		q.checkNil(t)
		q.checkInvariants(t, int64(0))
		require.True(t, q.Empty())
	})

	t.Run("empty queue", func(t *testing.T) {
		q.Enqueue(&testQueueItem{})
		require.False(t, q.Empty())
		q.Dequeue()
		require.True(t, q.Empty())
	})

	t.Run("fill and empty queue with random operations", func(t *testing.T) {
		// Add events and assert they are consumed in fifo order.
		var lastPop int64 = -1
		var lastPush int64 = -1
		for eventCount > 0 {
			q.checkInvariants(t, int64(eventCount))
			rng, _ := randutil.NewTestRand()
			op := rng.Intn(5)
			if op < 3 {
				v := lastPush + 1
				q.Enqueue(&testQueueItem{i: v})
				lastPush++
			} else {
				e, ok := q.Dequeue()
				if !ok {
					require.Equal(t, lastPop, lastPush)
					require.True(t, q.Empty())
				} else {
					require.Equal(t, lastPop+1, e.i)
					lastPop++
					eventCount--
				}
			}
		}

		t.Run("purge", func(t *testing.T) {
			for eventCount > 0 {
				q.checkInvariants(t, int64(eventCount))
				rng, _ := randutil.NewTestRand()
				op := rng.Intn(15)
				sum := int64(0)
				if op < 10 {
					v := int64(rng.Intn(20000))
					q.Enqueue(&testQueueItem{i: v})
					sum += v
				} else if op < 12 {
					e, ok := q.Dequeue()
					if !ok {
						require.True(t, q.Empty())
					} else {
						sum -= e.i
						eventCount--
					}
				} else {
					fixedQ, ok := q.(*QueueWithFixedChunkSize[*testQueueItem])
					if op < 13 && !ok {
						q.purge()
						q.checkNil(t)
						eventCount = 0
						q.checkInvariants(t, int64(0))
					} else {
						actualSum := int64(0)
						fixedQ.removeAll(func(e *testQueueItem) {
							actualSum += e.i
							eventCount--
						})
						fixedQ.checkNil(t)
						require.Equal(t, sum, actualSum)
					}
				}
			}
		})
	})
}
