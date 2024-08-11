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
)

type testQueueItem struct {
	t int64
}

func BenchmarkQueue(b *testing.B) {
	b.Run("", func(b *testing.B) {
		b.ReportAllocs()
		rng, _ := randutil.NewTestRand()
		eventCount := 2000000
		q, _ := NewQueue[*testQueueItem]()
		// Add one event and remove it
		q.Enqueue(&testQueueItem{
			t: int64(*new(int)),
		})
		q.Dequeue()

		// Fill 5 chunks and then pop each one, ensuring empty() returns the correct
		// value each time.
		for i := 0; i < eventCount; i++ {
			q.Enqueue(&testQueueItem{
				t: int64(*new(int)),
			})
		}
		for {
			_, ok := q.Dequeue()
			if !ok {
				break
			} else {
				eventCount--
			}
		}
		q.Enqueue(&testQueueItem{
			t: int64(*new(int)),
		})
		q.Dequeue()

		// Add events to fill 5 chunks and assert they are consumed in fifo order.
		var lastPop int64 = -1
		var lastPush int64 = -1
		for eventCount > 0 {
			op := rng.Intn(5)
			if op < 3 {
				q.Enqueue(&testQueueItem{t: lastPush + 1})
				lastPush++
			} else {
				_, ok := q.Dequeue()
				if ok {
					lastPop++
					eventCount--
				}
			}
		}
		q.purge()
	})
}
