package slab

import (
	"math/rand"
	"testing"
	"testing/quick"
)

func TestClassFor(t *testing.T) {
	cases := []struct{ n, want int }{
		{-5, 4 << 10}, {0, 4 << 10}, {1, 4 << 10}, {4 << 10, 4 << 10},
		{4<<10 + 1, 16 << 10}, {16 << 10, 16 << 10}, {16<<10 + 1, 64 << 10},
		{64 << 10, 64 << 10}, {64<<10 + 1, 256 << 10}, {256 << 10, 256 << 10},
		{256<<10 + 1, -1}, {1 << 30, -1},
	}
	for _, c := range cases {
		if got := ClassFor(c.n); got != c.want {
			t.Errorf("ClassFor(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

func TestGetSizes(t *testing.T) {
	a := New()
	for _, n := range []int{0, 1, 4096, 4097, 16384, 65536, 65537, 262144} {
		b := a.Get(n)
		if want := ClassFor(n); len(b) != want || cap(b) != want {
			t.Fatalf("Get(%d): len %d cap %d, want %d", n, len(b), cap(b), want)
		}
		a.Put(b)
	}
}

func TestOversize(t *testing.T) {
	a := New()
	for _, c := range []struct{ n, want int }{
		{256<<10 + 1, 512 << 10},
		{512 << 10, 512 << 10},
		{1<<20 + 7, 5 * 256 << 10},
	} {
		b := a.Get(c.n)
		if len(b) != c.want || cap(b) != c.want {
			t.Fatalf("Get(%d): len %d cap %d, want %d", c.n, len(b), cap(b), c.want)
		}
		if s := a.Stats(); s.InUse != c.want {
			t.Fatalf("InUse after oversize Get = %d, want %d", s.InUse, c.want)
		}
		a.Put(b)
		s := a.Stats()
		if s.InUse != 0 || s.Free != 0 {
			t.Fatalf("oversize Put pooled it: %+v", s)
		}
	}
	// A second oversize Get must make() again (never pooled).
	before := a.Stats().Misses
	a.Put(a.Get(300 << 10))
	if a.Stats().Misses != before+1 {
		t.Fatal("oversize Get was served from a pool")
	}
}

func TestPutDropsForeign(t *testing.T) {
	a := New()
	a.Put(make([]byte, 100))
	a.Put(make([]byte, 5000))
	s := a.Stats()
	if s.Free != 0 || s.InUse != 0 || s.Puts != 2 {
		t.Fatalf("foreign Put changed pools: %+v", s)
	}
}

func TestReuseAndStats(t *testing.T) {
	a := New()
	b := a.Get(100)
	b[0] = 42
	a.Put(b[:10]) // short len, full cap: still pooled, restored to full len
	s := a.Stats()
	want := Stats{InUse: 0, Free: 4096, Allocated: 4096, Gets: 1, Puts: 1, Misses: 1}
	if s != want {
		t.Fatalf("stats %+v, want %+v", s, want)
	}
	c := a.Get(4000)
	if len(c) != 4096 || &c[0] != &b[0] {
		t.Fatal("Get did not reuse the freed slab at full length")
	}
	s = a.Stats()
	want = Stats{InUse: 4096, Free: 0, Allocated: 4096, Gets: 2, Puts: 1, Misses: 1}
	if s != want {
		t.Fatalf("stats %+v, want %+v", s, want)
	}
}

func TestTrim(t *testing.T) {
	a := New()
	var held [][]byte
	for i := 0; i < 10; i++ {
		held = append(held, a.Get(4096), a.Get(256<<10))
	}
	for _, b := range held {
		a.Put(b)
	}
	if s := a.Stats(); s.Free != 10*(4096+256<<10) || s.Allocated != s.Free {
		t.Fatalf("before trim: %+v", s)
	}
	a.Trim(3 * 4096) // keeps 3 x 4K, 0 x 256K
	s := a.Stats()
	if s.Free != 3*4096 || len(a.free[0]) != 3 || len(a.free[3]) != 0 {
		t.Fatalf("after trim: %+v lens %d %d", s, len(a.free[0]), len(a.free[3]))
	}
	if s.Allocated != 10*(4096+256<<10) {
		t.Fatalf("Allocated is a high water mark, got %d", s.Allocated)
	}
	if cap(a.free[3]) < initialFreeSlots {
		t.Fatal("Trim dropped the free list backing array")
	}
	for _, b := range a.free[3][:cap(a.free[3])] {
		if b != nil {
			t.Fatal("Trim left a reference to a released slab")
		}
	}
	a.Trim(0)
	if s := a.Stats(); s.Free != 0 {
		t.Fatalf("Trim(0) left %d free", s.Free)
	}
	a.Trim(-1)
}

func TestZeroAlloc(t *testing.T) {
	a := New()
	// Warm every class so the free lists are non-empty.
	for _, c := range Classes {
		a.Put(a.Get(c))
	}
	allocs := testing.AllocsPerRun(1000, func() {
		for _, c := range Classes {
			a.Put(a.Get(c))
		}
	})
	if allocs != 0 {
		t.Fatalf("Get/Put on warm free lists allocates: %v", allocs)
	}
	// Put into the preallocated 64 slots never allocates, even from empty.
	b := New()
	held := make([][]byte, initialFreeSlots)
	for i := range held {
		held[i] = b.Get(4096)
	}
	i := 0
	allocs = testing.AllocsPerRun(initialFreeSlots-1, func() {
		b.Put(held[i])
		i++
	})
	if allocs != 0 {
		t.Fatalf("Put into preallocated free list allocates: %v", allocs)
	}
}

// TestQuickAccounting runs random Get/Put sequences and checks the byte
// counters against an independent model.
func TestQuickAccounting(t *testing.T) {
	f := func(ops []uint32, seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		a := New()
		var held [][]byte
		inUse := 0
		hwm := 0
		for _, op := range ops {
			if op%3 != 0 || len(held) == 0 {
				n := int(op>>2) % (600 << 10)
				b := a.Get(n)
				if len(b) < n || len(b) != cap(b) {
					return false
				}
				if c := ClassFor(n); c > 0 && len(b) != c {
					return false
				}
				held = append(held, b)
				inUse += len(b)
			} else {
				k := rng.Intn(len(held))
				inUse -= len(held[k])
				a.Put(held[k])
				held[k] = held[len(held)-1]
				held = held[:len(held)-1]
			}
			s := a.Stats()
			if s.InUse != inUse {
				return false
			}
			if t := s.InUse + s.Free; t > hwm {
				hwm = t
			}
			if s.Allocated != hwm || s.Free < 0 {
				return false
			}
			free := 0
			for i, fl := range a.free {
				for _, b := range fl {
					if len(b) != Classes[i] {
						return false
					}
					free += len(b)
				}
			}
			if free != s.Free {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkGetPut(b *testing.B) {
	a := New()
	a.Put(a.Get(4096))
	b.ReportAllocs()
	for b.Loop() {
		a.Put(a.Get(4096))
	}
}
