// Package ring implements the per-connection outbound byte queue.
//
// A Ring stores bytes in a chain of slab segments taken from a per-loop
// slab.Allocator; it never grows by append reallocation. Each segment is a
// circular buffer. Writes go only to the tail segment and reads only come
// from the head segment, so only the head can hold data that wraps around
// the end of its slab, and only when it is also the tail. A connection whose
// pending bytes stay within one 4 KiB slab therefore never calls the
// allocator after its first write.
//
// Growth policy: when the tail segment is full (or there is none), a new
// segment is taken whose class is the smallest that fits the bytes still to
// be written by the current Write, capped at 256 KiB, but never smaller than
// the class one step above the current tail (4K -> 16K -> 64K -> 256K). The
// geometric floor keeps a backlog built from many small writes in few
// segments; writes larger than 256 KiB span several 256 KiB segments.
//
// A Ring holds no segment until its first non-empty Write. Consume keeps the
// last segment (emptied) for reuse; Reset returns every segment.
//
// Not goroutine-safe.
package ring

import (
	"github.com/robert-malhotra/twine/internal/slab"
	"golang.org/x/sys/unix"
)

// segment is one slab in the chain, used as a circular buffer: the readable
// bytes are buf[r:r+n] taken modulo len(buf).
type segment struct {
	buf  []byte
	r    int // read offset
	n    int // readable bytes
	next *segment
}

// Ring is a byte FIFO backed by slab segments. Zero value is unusable; call
// Init. Not goroutine-safe. A Ring must not be copied after Init: it holds a
// pointer to its own embedded first segment.
type Ring struct {
	alloc     *slab.Allocator
	head      *segment
	tail      *segment
	length    int
	highWater int

	// spare is a free list of segment structs (linked through next) reused
	// before allocating a new one. It survives Reset and Init.
	spare *segment
	// inline is the first segment struct, embedded so a single-segment
	// ring never heap-allocates a segment struct. inlineBusy reports
	// whether it is currently in the chain.
	inline     segment
	inlineBusy bool
}

// Init binds r to an allocator. Init may be called on a Ring embedded in a
// pooled conn struct repeatedly (after Reset). If r still holds segments
// they are first returned to the allocator they came from. Init resets the
// high water mark.
func (r *Ring) Init(a *slab.Allocator) {
	if r.head != nil {
		r.Reset()
	}
	r.alloc = a
	r.length = 0
	r.highWater = 0
}

// Len returns readable bytes.
func (r *Ring) Len() int { return r.length }

// Write appends b. Grows by chaining slabs from the allocator; copies b.
// Writing an empty slice is a no-op and never takes a segment.
func (r *Ring) Write(b []byte) {
	if len(b) == 0 {
		return
	}
	r.length += len(b)
	if r.length > r.highWater {
		r.highWater = r.length
	}
	for len(b) > 0 {
		s := r.tail
		if s == nil || s.n == len(s.buf) {
			s = r.grow(len(b))
		}
		size := len(s.buf)
		w := s.r + s.n // write position, possibly past the end
		var room int
		if w >= size {
			w -= size
			room = s.r - w
		} else {
			room = size - w
		}
		c := copy(s.buf[w:w+room], b)
		s.n += c
		b = b[c:]
	}
}

// grow appends a new empty tail segment sized for need more bytes and
// returns it.
func (r *Ring) grow(need int) *segment {
	if need > slab.Classes[len(slab.Classes)-1] {
		need = slab.Classes[len(slab.Classes)-1]
	}
	if r.tail != nil {
		if floor := nextClass(len(r.tail.buf)); need < floor {
			need = floor
		}
	}
	s := r.getSeg()
	s.buf = r.alloc.Get(need)
	s.r, s.n, s.next = 0, 0, nil
	if r.tail == nil {
		r.head = s
	} else {
		r.tail.next = s
	}
	r.tail = s
	return s
}

// nextClass returns the class one step above size, or the largest class if
// size is already the largest.
func nextClass(size int) int {
	cs := slab.Classes
	for i := 0; i < len(cs)-1; i++ {
		if size <= cs[i] {
			return cs[i+1]
		}
	}
	return cs[len(cs)-1]
}

// getSeg returns an unused segment struct: the inline one if free, then the
// spare list, and only then a fresh allocation.
func (r *Ring) getSeg() *segment {
	if !r.inlineBusy {
		r.inlineBusy = true
		return &r.inline
	}
	if s := r.spare; s != nil {
		r.spare = s.next
		s.next = nil
		return s
	}
	return new(segment)
}

// releaseSeg returns s's slab to the allocator and pools the struct.
func (r *Ring) releaseSeg(s *segment) {
	r.alloc.Put(s.buf)
	s.buf = nil
	s.r, s.n = 0, 0
	if s == &r.inline {
		s.next = nil
		r.inlineBusy = false
		return
	}
	s.next = r.spare
	r.spare = s
}

// Writev appends each slice in order (one Write per slice, no extra copy).
func (r *Ring) Writev(bs [][]byte) {
	for _, b := range bs {
		r.Write(b)
	}
}

// Iovecs fills dst with the readable regions in FIFO order and returns the
// filled prefix. It stops when dst is full. Never allocates. A segment may
// contribute two iovecs when its data wraps.
func (r *Ring) Iovecs(dst []unix.Iovec) []unix.Iovec {
	i := 0
	for s := r.head; s != nil && i < len(dst); s = s.next {
		if s.n == 0 {
			continue
		}
		size := len(s.buf)
		end := s.r + s.n
		first := end
		if first > size {
			first = size
		}
		dst[i].Base = &s.buf[s.r]
		dst[i].SetLen(first - s.r)
		i++
		if end > size && i < len(dst) {
			dst[i].Base = &s.buf[0]
			dst[i].SetLen(end - size)
			i++
		}
	}
	return dst[:i]
}

// Peek returns the first contiguous readable region (nil when empty).
func (r *Ring) Peek() []byte {
	s := r.head
	if s == nil || s.n == 0 {
		return nil
	}
	end := s.r + s.n
	if end > len(s.buf) {
		end = len(s.buf)
	}
	return s.buf[s.r:end:end]
}

// Consume drops n bytes from the front (n <= Len). Segments that become empty
// are returned to the allocator, except the last one, which is kept (reset to
// empty) so the next Write needs no allocation. Consume panics if n < 0 or
// n > Len.
func (r *Ring) Consume(n int) {
	if n < 0 || n > r.length {
		panic("ring: Consume out of range")
	}
	r.length -= n
	for n > 0 {
		s := r.head
		c := s.n
		if n < c {
			s.r += n
			if s.r >= len(s.buf) {
				s.r -= len(s.buf)
			}
			s.n -= n
			return
		}
		n -= c
		s.n = 0
		s.r = 0
		if s.next == nil {
			return // last segment: keep it, empty
		}
		r.head = s.next
		r.releaseSeg(s)
	}
}

// Reset drops all data and returns every segment to the allocator. The high
// water mark is kept; segment structs stay pooled in r for reuse.
func (r *Ring) Reset() {
	for s := r.head; s != nil; {
		next := s.next
		r.releaseSeg(s)
		s = next
	}
	r.head, r.tail = nil, nil
	r.length = 0
}

// HighWater returns the maximum Len observed since Init/ResetHighWater.
func (r *Ring) HighWater() int { return r.highWater }

// ResetHighWater sets the high water mark to the current Len.
func (r *Ring) ResetHighWater() { r.highWater = r.length }
