// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RegistrationFinished is a callback used by buffered steams when all events
// were processed and resources released.
type RegistrationFinished func()

// BufferedStream is an interface for an outbound queue.
type BufferedStream interface {
	// Send writes events into the buffer. Returns true if event was successfully
	// written, false if buffered stream overflowed. BufferedStream could still
	// hold to allocations and it is not safe to assume that resources allocated
	// for writes were released.
	Send(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	// SendUnbuffered allows sending events to underlying stream bypassing
	// buffering. It it intended to be used for catchUpScans where data is read
	// from replica directly.
	SendUnbuffered(event *kvpb.RangeFeedEvent) error
	// SendError drops all pending events and send error to the remote receiver.
	// All events are drained prior to method returning.
	SendError(err *kvpb.Error)
}

// MuxBufferedStream is a BufferedStream provided by MuxRangeFeeds for processors
// to send data.
type MuxBufferedStream struct {
	streamID int64
	rangeID  roachpb.RangeID
	muxer    *StreamMuxer
}

var _ BufferedStream = (*MuxBufferedStream)(nil)

func NewMuxBufferedStream(
	streamID int64, rangeID roachpb.RangeID, done RegistrationFinished, muxer *StreamMuxer,
) *MuxBufferedStream {
	s := &MuxBufferedStream{
		streamID: streamID,
		rangeID:  rangeID,
		muxer:    muxer,
	}
	muxer.addProducer(streamID, rangeID, done)
	return s
}

func (s *MuxBufferedStream) Send(event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation) {
	s.muxer.publish(s.streamID, s.rangeID, event, alloc)
}

func (s *MuxBufferedStream) SendUnbuffered(event *kvpb.RangeFeedEvent) error {
	return s.muxer.output.Send(&kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	})
}

func (s *MuxBufferedStream) SendError(err *kvpb.Error) {
	s.muxer.publishError(s.streamID, s.rangeID, err)
}

type sharedMuxEvent struct {
	streamID int64
	rangeID  roachpb.RangeID
	event    *kvpb.RangeFeedEvent
	alloc    *SharedBudgetAllocation
	err      *kvpb.Error
}

type producer struct {
	syncutil.Mutex
	id      int64
	rangeID roachpb.RangeID
	// If true, writes would be rejected. Used to synchronize with data removal.
	disconnected bool
	done         RegistrationFinished
}

type StreamMuxer struct {
	output   kvpb.MuxRangeFeedEventSink
	capacity int

	// Queue of pending events.
	queueMu struct {
		syncutil.Mutex
		buffer   eventQueue
		overflow bool
	}
	dataC chan struct{}

	// Mostly static list of producer callbacks.
	mu struct {
		syncutil.RWMutex
		prods map[int64]*producer
	}
	cleanup chan int64
}

func NewStreamMuxer(output kvpb.MuxRangeFeedEventSink, capacity int) *StreamMuxer {
	r := &StreamMuxer{
		output:   output,
		capacity: capacity,
		dataC:    make(chan struct{}, 1),
		// Note that cleanup queue size is picked arbitrary to give some room for
		// streams that would terminate in parallel to avoid immediate blocking.
		cleanup: make(chan int64, 10),
	}
	r.queueMu.buffer = newEventQueue()
	r.mu.prods = make(map[int64]*producer)
	return r
}

func (m *StreamMuxer) addProducer(id int64, rangeID roachpb.RangeID, done RegistrationFinished) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.mu.prods[id]; ok {
		if buildutil.CrdbTestBuild {
			log.Fatalf(context.Background(), "attempt to register completions callback for mux stream %d multiple times",
				id)
		}
	}
	m.mu.prods[id] = &producer{id: id, rangeID: rangeID, done: done}
}

// Registration overflown. No more events will be accepted from producer.
// think if we should return error here, not bool.
func (m *StreamMuxer) publish(
	id int64, rangeID roachpb.RangeID, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	// Check if it is a valid producer before accepting allocation from it.
	m.mu.RLock()
	p, ok := m.mu.prods[id]
	m.mu.RUnlock()
	if !ok {
		return
	}

	// All producer operations are performed under its lock.
	p.Lock()
	defer p.Unlock()
	// We don't want to accept more entries
	if p.disconnected {
		return
	}
	alloc.Use(context.Background())
	if !m.addToQueueInternal(sharedMuxEvent{
		streamID: id,
		rangeID:  rangeID,
		event:    event,
		alloc:    alloc,
	}) {
		alloc.Release(context.Background())
		return
	}
	// Notify output loop about data.
	select {
	case m.dataC <- struct{}{}:
	default:
	}
}

func (m *StreamMuxer) addToQueueInternal(event sharedMuxEvent) bool {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	if m.queueMu.overflow {
		return false
	}
	if m.queueMu.buffer.len() >= m.capacity {
		m.queueMu.overflow = true
		return false
	}
	m.queueMu.buffer.pushBack(event)
	return true
}

// publishError requests immediate termination of consumer. It is not guaranteed
// that done callback is called synchronously and implementation could defer it
// until it cleans up underlying events and allocations. Pending data for
// provided id could be discarded if it wasn't sent.
func (m *StreamMuxer) publishError(id int64, rangeID roachpb.RangeID, err *kvpb.Error) {
	m.mu.RLock()
	p, ok := m.mu.prods[id]
	m.mu.RUnlock()
	if !ok {
		return
	}
	p.Lock()
	if p.disconnected {
		p.Unlock()
		return
	}
	p.disconnected = true
	m.addErrorToQueueInternal(sharedMuxEvent{
		streamID: id,
		rangeID:  rangeID,
		err:      err,
	})
	p.Unlock()
	// Notify callback without holding a lock. We need to defer that to worker
	// loop to avoid deadlocks and blocking scheduler workers from processor.
	m.cleanup <- id
	// Notify output loop about data.
	select {
	case m.dataC <- struct{}{}:
	default:
	}
}

func (m *StreamMuxer) addErrorToQueueInternal(event sharedMuxEvent) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	m.queueMu.buffer.remove(context.Background(), event.streamID)
	m.queueMu.buffer.pushBack(event)
}

func (m *StreamMuxer) OutputLoop(ctx context.Context) {
	for {
		select {
		case <-m.dataC:
			for {
				m.queueMu.Lock()
				e, ok := m.queueMu.buffer.popFront()
				overflow, remains := m.queueMu.overflow, m.queueMu.buffer.len()
				m.queueMu.Unlock()
				if !ok {
					break
				}
				err := m.sendEventToSink(ctx, e)
				if err != nil {
					// Output is terminated by stream, wind down everything.
					m.cleanupProducers(ctx, nil)
					return
				}
				if overflow && remains == 0 {
					m.cleanupProducers(ctx, newErrBufferCapacityExceeded())
					return
				}
			}
		case id := <-m.cleanup:
			m.cleanupProducerIfExists(id)
		case <-ctx.Done():
			// Output is terminated by context.
			m.cleanupProducers(ctx, kvpb.NewError(ctx.Err()))
			return
			// TODO(wenyihu6)L check if getting rid of this case is fine
			//case <-m.output.Context().Done():
			//	// Output is terminated by stream context.
			//	m.cleanupProducers(ctx, nil)
			//	return
		}
	}
}

func (m *StreamMuxer) sendEventToSink(ctx context.Context, e sharedMuxEvent) error {
	var err error
	switch {
	case e.event != nil:
		err = m.output.Send(&kvpb.MuxRangeFeedEvent{
			RangeFeedEvent: *e.event,
			RangeID:        e.rangeID,
			StreamID:       e.streamID,
		})
	case e.err != nil:
		errEvent := &kvpb.MuxRangeFeedEvent{
			RangeID:  e.rangeID,
			StreamID: e.streamID,
		}
		errEvent.SetValue(&kvpb.RangeFeedError{
			Error: *e.err,
		})
		err = m.output.Send(errEvent)
	default:
		// We can have empty entries for streams that were purged by
		// posting errors.
	}
	e.alloc.Release(ctx)
	return err
}

// cleanup will first drain all pending data to free pooled events and budget
// allocations and then notify all producers that data is removed.
func (m *StreamMuxer) cleanupProducers(ctx context.Context, streamErr *kvpb.Error) {
	m.queueMu.Lock()
	m.queueMu.buffer.removeAll(ctx)
	m.queueMu.Unlock()

	var callbacks []*producer
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for id, p := range m.mu.prods {
			delete(m.mu.prods, id)
			p.Lock()
			if !p.disconnected {
				callbacks = append(callbacks, p)
			}
			p.Unlock()
		}
	}()

	// Notify callbacks not from under lock just in case they call us back to
	// unregister or post an error of any sort.
	var err error
	for _, p := range callbacks {
		if err != nil && streamErr != nil {
			errEvent := &kvpb.MuxRangeFeedEvent{
				RangeID:  p.rangeID,
				StreamID: p.id,
			}
			errEvent.SetValue(&kvpb.RangeFeedError{
				Error: *streamErr,
			})
			err = m.output.Send(errEvent)
		}
		p.done()
	}
}

func (m *StreamMuxer) cleanupProducerIfExists(id int64) {
	p := func() *producer {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.mu.prods[id]
	}()
	if p != nil {
		p.done()
	}
}

// Number of queue elements allocated at once to amortize queue allocations.
const eventQueueChunkSize = 4000

// idQueueChunk is a queue chunk of a fixed size which idQueue uses to extend
// its storage. Chunks are kept in the pool to reduce allocations.
type eventQueueChunk struct {
	data      [eventQueueChunkSize]sharedMuxEvent
	nextChunk *eventQueueChunk
}

var sharedEventQueueChunkSyncPool = sync.Pool{
	New: func() interface{} {
		return new(eventQueueChunk)
	},
}

func getPooledEventQueueChunk() *eventQueueChunk {
	return sharedEventQueueChunkSyncPool.Get().(*eventQueueChunk)
}

func putPooledEventQueueChunk(e *eventQueueChunk) {
	*e = eventQueueChunk{}
	sharedEventQueueChunkSyncPool.Put(e)
}

// eventQueue stores pending processor ID's. Internally data is stored in
// eventQueueChunk sized arrays that are added as needed and discarded once
// reader and writers finish working with it. Since we only have a single
// scheduler per store, we don't use a pool as only reuse could happen within
// the same queue and in that case we can just increase chunk size.
type eventQueue struct {
	first, last *eventQueueChunk
	read, write int
	size        int
}

func newEventQueue() eventQueue {
	chunk := getPooledEventQueueChunk()
	return eventQueue{
		first: chunk,
		last:  chunk,
	}
}

func (q *eventQueue) pushBack(event sharedMuxEvent) {
	if q.write == eventQueueChunkSize {
		nexChunk := getPooledEventQueueChunk()
		q.last.nextChunk = nexChunk
		q.last = nexChunk
		q.write = 0
	}
	q.last.data[q.write] = event
	q.write++
	q.size++
}

func (q *eventQueue) popFront() (sharedMuxEvent, bool) {
	if q.first == q.last && q.read == q.write {
		return sharedMuxEvent{}, false
	}
	if q.read == eventQueueChunkSize {
		removed := q.first
		q.first = q.first.nextChunk
		putPooledEventQueueChunk(removed)
		q.read = 0
	}
	res := q.first.data[q.read]
	q.first.data[q.read] = sharedMuxEvent{}
	q.read++
	q.size--
	return res, true
}

// remove releases allocations and zero out entries that belong to particular
// streamID. Queue size is reduced by amount of removed entries, but empty
// entries stay until their chunks are removed. Removed entries are not counted
// against capacity when determining overflow.
func (q *eventQueue) remove(ctx context.Context, streamID int64) {
	start := q.read
	for chunk := q.first; chunk != nil; chunk = chunk.nextChunk {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			if chunk.data[i].streamID == streamID {
				chunk.data[i].alloc.Release(ctx)
				chunk.data[i] = sharedMuxEvent{}
				q.size--
			}
		}
		start = 0
	}
}

// removeAll removes aren releases all entries in the queue.
func (q *eventQueue) removeAll(ctx context.Context) {
	start := q.read
	for chunk := q.first; chunk != nil; {
		max := eventQueueChunkSize
		if chunk.nextChunk == nil {
			max = q.write
		}
		for i := start; i < max; i++ {
			chunk.data[i].alloc.Release(ctx)
			chunk.data[i] = sharedMuxEvent{}
		}
		next := chunk.nextChunk
		putPooledEventQueueChunk(chunk)
		chunk = next
		start = 0
	}
	q.first = q.last
	q.read = 0
	q.write = 0
	q.size = 0
}

func (q *eventQueue) len() int {
	return q.size
}
