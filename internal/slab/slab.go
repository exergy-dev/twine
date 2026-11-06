// Package slab is a size-classed []byte allocator owned by a single event
// loop.
//
// Slabs come in four classes (4 KiB, 16 KiB, 64 KiB, 256 KiB). Each class has
// a free list that is a preallocated slice of []byte, so taking a slab from a
// non-empty free list and returning one to a free list with spare capacity
// never allocates. Requests larger than the largest class get a fresh,
// non-pooled slice rounded up to a multiple of 256 KiB.
//
// An Allocator is not goroutine-safe: each loop owns its own instance.
package slab

// Classes are the slab sizes, ascending.
var Classes = [...]int{4 << 10, 16 << 10, 64 << 10, 256 << 10}

// numClasses is the number of size classes.
const numClasses = len(Classes)

// maxClass is the largest class size; oversize requests are rounded up to a
// multiple of it.
const maxClass = 256 << 10

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
// larger than every class. n <= 0 maps to the smallest class.
func classIndex(n int) int {
	switch {
	case n <= Classes[0]:
		return 0
	case n <= Classes[1]:
		return 1
	case n <= Classes[2]:
		return 2
	case n <= Classes[3]:
		return 3
	}
	return -1
}

// exactClassIndex returns the index of the class whose size is exactly c, or
// -1 if c is not a class size.
func exactClassIndex(c int) int {
	switch c {
	case Classes[0]:
		return 0
	case Classes[1]:
		return 1
	case Classes[2]:
		return 2
	case Classes[3]:
		return 3
	}
	return -1
}

// ClassFor returns the class size for n or -1 if larger than all classes.
// n <= 0 maps to the smallest class.
func ClassFor(n int) int {
	i := classIndex(n)
	if i < 0 {
		return -1
	}
	return Classes[i]
}

// Get returns a slice with len == cap == the smallest class >= n. n larger
// than the largest class returns a fresh make([]byte, n) rounded up to a
// multiple of 256 KiB that is not pooled (Put frees it to the GC). Get from a
// non-empty free list does not allocate. The contents of a reused slab are
// not cleared.
func (a *Allocator) Get(n int) []byte {
	a.gets++
	i := classIndex(n)
	if i < 0 {
		size := (n + maxClass - 1) / maxClass * maxClass
		a.misses++
		a.inUse += size
		a.noteHighWater()
		return make([]byte, size)
	}
	fl := a.free[i]
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
// is not a class are dropped. Put never allocates once a free list has been
// grown (free lists are slices of []byte that grow by append; New
// preallocates 64 slots per class).
//
// Put must only be given slices obtained from Get, each at most once. A slice
// whose cap is larger than the largest class and a multiple of 256 KiB is
// assumed to be an oversize slice from Get: it is dropped to the GC and its
// size is removed from InUse. Any other non-class slice is dropped without
// touching the byte counters.
func (a *Allocator) Put(b []byte) {
	a.puts++
	c := cap(b)
	i := exactClassIndex(c)
	if i < 0 {
		if c > maxClass && c%maxClass == 0 {
			a.inUse -= c
		}
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

// Trim releases free slabs above keep bytes per class back to the GC: each
// class keeps at most keep/classSize free slabs. keep <= 0 releases every
// free slab. The free lists keep their backing arrays, so later Puts do not
// allocate.
func (a *Allocator) Trim(keep int) {
	if keep < 0 {
		keep = 0
	}
	for i := range a.free {
		fl := a.free[i]
		n := keep / Classes[i]
		if len(fl) <= n {
			continue
		}
		for j := n; j < len(fl); j++ {
			a.freeBytes -= len(fl[j])
			fl[j] = nil
		}
		a.free[i] = fl[:n]
	}
}
