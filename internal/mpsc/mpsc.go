// Package mpsc implements a bounded, lock-free, multi-producer
// single-consumer queue of func() values.
//
// The algorithm is Dmitry Vyukov's bounded MPMC queue specialised to a single
// consumer: every cell carries an atomic sequence number, producers claim a
// position by CAS on the tail counter, and the consumer advances the head
// counter without CAS because it is the only reader. Push and Pop never block
// and never allocate.
package mpsc

import "sync/atomic"

// cacheLinePad separates hot fields so producers and the consumer do not
// false-share a cache line.
type cacheLinePad [56]byte

// cell is one queue slot. seq encodes the slot's state relative to the
// position counters: seq == pos means free for the producer claiming pos,
// seq == pos+1 means filled and ready for the consumer at pos.
type cell struct {
	seq atomic.Uint64
	fn  func()
}

// Queue is a bounded lock-free multi-producer single-consumer queue of
// func() values. Push may be called from any goroutine; Pop must only be
// called from a single consumer goroutine at a time. The zero value is not
// usable; create queues with New.
type Queue struct {
	_     cacheLinePad
	tail  atomic.Uint64 // next position to be claimed by a producer
	_     cacheLinePad
	head  atomic.Uint64 // next position to be consumed; written only by the consumer
	_     cacheLinePad
	mask  uint64
	cells []cell
	_     cacheLinePad
}

// New returns a queue whose capacity is capacity rounded up to a power of
// two, with a minimum of 2.
func New(capacity int) *Queue {
	n := uint64(2)
	for capacity > 0 && n < uint64(capacity) {
		n <<= 1
	}
	q := &Queue{mask: n - 1, cells: make([]cell, n)}
	for i := range q.cells {
		q.cells[i].seq.Store(uint64(i))
	}
	return q
}

// Push enqueues fn. It returns false, without blocking, when the queue is
// full. Push is lock-free, safe to call from any goroutine, and never
// allocates: the func value itself is stored in the slot.
func (q *Queue) Push(fn func()) bool {
	pos := q.tail.Load()
	for {
		c := &q.cells[pos&q.mask]
		seq := c.seq.Load()
		switch dif := int64(seq - pos); {
		case dif == 0:
			if q.tail.CompareAndSwap(pos, pos+1) {
				c.fn = fn
				c.seq.Store(pos + 1)
				return true
			}
			pos = q.tail.Load()
		case dif < 0:
			// The cell still holds the item from the previous lap.
			return false
		default:
			// Another producer claimed pos; reload and retry.
			pos = q.tail.Load()
		}
	}
}

// Pop dequeues the oldest item. It must only be called by the single
// consumer. It returns false when the queue is empty, which includes the
// brief window in which the oldest claimed slot is still being written by
// its producer (Len may then be non-zero while Pop reports empty; the item
// becomes visible once that Push returns). The slot's func reference is
// cleared so the queue does not retain garbage.
func (q *Queue) Pop() (func(), bool) {
	pos := q.head.Load()
	c := &q.cells[pos&q.mask]
	if int64(c.seq.Load()-(pos+1)) < 0 {
		return nil, false
	}
	fn := c.fn
	c.fn = nil
	c.seq.Store(pos + q.mask + 1)
	q.head.Store(pos + 1)
	return fn, true
}

// Len returns the approximate number of queued items (tail - head), clamped
// to [0, Cap()]. It is safe to call from any goroutine. Items whose push is
// still in progress are counted.
func (q *Queue) Len() int {
	h := q.head.Load()
	t := q.tail.Load()
	if t <= h {
		return 0
	}
	n := t - h
	if n > q.mask+1 {
		n = q.mask + 1
	}
	return int(n)
}

// Cap returns the queue's capacity (the rounded value passed to New).
func (q *Queue) Cap() int { return int(q.mask + 1) }
