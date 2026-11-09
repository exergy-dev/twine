// Package slab is a size-classed []byte allocator owned by a single event
// loop.
//
// Slabs come in four classes (4 KiB, 16 KiB, 64 KiB, 256 KiB = MaxClass).
// Each class has a free list that is a preallocated slice of []byte, so
// taking a slab from a non-empty free list and returning one to a free list
// with spare capacity never allocates. Requests larger than MaxClass are a
// caller bug: the only caller, ring, never asks for more than MaxClass.
//
// An Allocator is not goroutine-safe: each loop owns its own instance.
package slab

// Class sizes, ascending.
const (
	class4K  = 4 << 10
	class16K = 16 << 10
	class64K = 64 << 10
	// MaxClass is the largest class size and the largest n Get accepts.
	MaxClass = 256 << 10
)

// Classes are the slab sizes, ascending. Read-only.
var Classes = [...]int{class4K, class16K, class64K, MaxClass}

// numClasses is the number of size classes.
const numClasses = len(Classes)

// initialFreeSlots is the preallocated capacity of each class free list.
const initialFreeSlots = 64

// Allocator hands out slabs from per-class free lists and tracks usage
// counters. Create one with New. Not goroutine-safe.
type Allocator struct {
	free [numClasses][][]byte

	inUse     int
	freeBytes int
	allocated int

	gets, puts, misses uint64
}

// New returns an Allocator whose free lists are preallocated with 64 slots
// per class, so Put does not allocate until a class holds more than 64 free
// slabs.
func New() *Allocator {
	a := &Allocator{}
	for i := range a.free {
		a.free[i] = make([][]byte, 0, initialFreeSlots)
	}
	return a
}

// classIndex returns the index of the smallest class >= n, or -1 if n is
// larger than MaxClass. n <= 0 maps to the smallest class.
func classIndex(n int) int {
	switch {
	case n <= class4K:
		return 0
	case n <= class16K:
		return 1
	case n <= class64K:
		return 2
	case n <= MaxClass:
		return 3
	}
	return -1
}

// exactClassIndex returns the index of the class whose size is exactly c, or
// -1 if c is not a class size.
func exactClassIndex(c int) int {
	switch c {
	case class4K:
		return 0
	case class16K:
		return 1
	case class64K:
		return 2
	case MaxClass:
		return 3
	}
	return -1
}

// Get returns a slice with len == cap == the smallest class >= n. n must be
// at most MaxClass; a larger n panics. Get from a non-empty free list does
// not allocate. The contents of a reused slab are not cleared.
func (a *Allocator) Get(n int) []byte {
	a.gets++
	i := classIndex(n)
	fl := a.free[i] // i == -1 (n > MaxClass) panics here
	if k := len(fl); k > 0 {
		b := fl[k-1]
		fl[k-1] = nil
		a.free[i] = fl[:k-1]
		a.freeBytes -= len(b)
		a.inUse += len(b)
		return b
	}
	a.misses++
	a.inUse += Classes[i]
	a.noteHighWater()
	return make([]byte, Classes[i])
}

// Put returns b (identified by cap) to its class free list. Slices whose cap
// is not a class size are dropped without touching the byte counters. Put
// never allocates once a free list has been grown (free lists are slices of
// []byte that grow by append; New preallocates 64 slots per class).
//
// Put must only be given slices obtained from Get, each at most once.
func (a *Allocator) Put(b []byte) {
	a.puts++
	c := cap(b)
	i := exactClassIndex(c)
	if i < 0 {
		return
	}
	a.free[i] = append(a.free[i], b[:c])
	a.inUse -= c
	a.freeBytes += c
}

// noteHighWater updates the Allocated high water mark.
func (a *Allocator) noteHighWater() {
	if t := a.inUse + a.freeBytes; t > a.allocated {
		a.allocated = t
	}
}

// Stats is a snapshot of an Allocator's counters.
type Stats struct {
	InUse     int    // bytes currently handed out
	Free      int    // bytes sitting in free lists
	Allocated int    // high water mark of InUse+Free, in bytes
	Gets      uint64 // calls to Get
	Puts      uint64 // calls to Put
	Misses    uint64 // Gets that had to make() a new slice
}

// Stats returns a snapshot of the allocator's counters.
func (a *Allocator) Stats() Stats {
	return Stats{
		InUse:     a.inUse,
		Free:      a.freeBytes,
		Allocated: a.allocated,
		Gets:      a.gets,
		Puts:      a.puts,
		Misses:    a.misses,
	}
}
