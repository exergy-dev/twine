package ring

import (
	"bytes"
	"math/rand"
	"testing"
	"testing/quick"
	"unsafe"

	"github.com/exergy-dev/twine/internal/slab"
	"golang.org/x/sys/unix"
)

const (
	k4   = 4 << 10
	k16  = 16 << 10
	k64  = 64 << 10
	k256 = 256 << 10
)

// pattern returns n bytes whose values depend on start, so misordering shows.
func pattern(start, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((start + i) * 7 >> 3)
	}
	return b
}

// iovBytes concatenates the memory described by iovs.
func iovBytes(iovs []unix.Iovec) []byte {
	var out []byte
	for _, iov := range iovs {
		out = append(out, unsafe.Slice((*byte)(unsafe.Pointer(iov.Base)), iov.Len)...)
	}
	return out
}

// segSizes returns the slab sizes in the chain, head first.
func segSizes(r *Ring) []int {
	var out []int
	for s := r.head; s != nil; s = s.next {
		out = append(out, len(s.buf))
	}
	return out
}

// checkInvariants verifies internal consistency of r.
func checkInvariants(t *testing.T, r *Ring) {
	t.Helper()
	if msg := r.verify(); msg != "" {
		t.Fatalf("%s (segments %v, Len %d)", msg, segSizes(r), r.Len())
	}
}

// contentCheck verifies the ring content equals want via Iovecs (with a large
// and a tiny dst) and Peek, without consuming.
func contentCheck(t *testing.T, r *Ring, want []byte) {
	t.Helper()
	var iovs [64]unix.Iovec
	got := iovBytes(r.Iovecs(iovs[:]))
	if r.Len() != len(want) {
		t.Fatalf("Len %d, want %d", r.Len(), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("iovec content mismatch (len %d vs %d)", len(got), len(want))
	}
	p := r.Peek()
	if len(want) == 0 {
		if p != nil {
			t.Fatal("Peek on empty ring returned non-nil")
		}
	} else if len(p) == 0 || !bytes.Equal(p, want[:len(p)]) {
		t.Fatal("Peek is not a prefix of the content")
	}
}

func newRing() (*Ring, *slab.Allocator) {
	a := slab.New()
	r := &Ring{}
	r.Init(a)
	return r, a
}

func TestNoSegmentUntilWrite(t *testing.T) {
	r, a := newRing()
	r.Write(nil)
	r.Write([]byte{})
	r.Writev([][]byte{nil, {}})
	if r.head != nil || a.Stats().Gets != 0 {
		t.Fatal("empty writes took a segment")
	}
	if r.Peek() != nil || len(r.Iovecs(make([]unix.Iovec, 4))) != 0 {
		t.Fatal("empty ring exported data")
	}
	r.Consume(0)
	r.Reset()
	if a.Stats().Gets != 0 {
		t.Fatal("Consume(0)/Reset on fresh ring touched the allocator")
	}
}

func TestSmallStaysInOneSlab(t *testing.T) {
	r, a := newRing()
	msg := pattern(0, 100)
	for i := 0; i < 10000; i++ {
		r.Write(msg)
		if i%3 == 0 {
			var iovs [4]unix.Iovec
			if got := iovBytes(r.Iovecs(iovs[:])); !bytes.Equal(got, msg) {
				t.Fatalf("iteration %d: content mismatch", i)
			}
		}
		r.Consume(100)
	}
	s := a.Stats()
	if s.Gets != 1 || s.Misses != 1 || s.Puts != 0 || s.InUse != k4 {
		t.Fatalf("stats after 10000 small cycles: %+v", s)
	}
	if got := segSizes(r); len(got) != 1 || got[0] != k4 {
		t.Fatalf("segments %v", got)
	}
	r.Reset()
	if s := a.Stats(); s.InUse != 0 || s.Puts != 1 {
		t.Fatalf("after Reset: %+v", s)
	}
	if r.head != nil || r.tail != nil || r.Len() != 0 {
		t.Fatal("Reset left segments")
	}
}

// TestWrapInsideSegment exercises true circular use of a single 4K slab.
func TestWrapInsideSegment(t *testing.T) {
	r, a := newRing()
	var ref []byte
	off := 0
	w := func(n int) {
		p := pattern(off, n)
		off += n
		r.Write(p)
		ref = append(ref, p...)
	}
	w(3000)
	r.Consume(2500)
	ref = ref[2500:]
	w(3000) // 500 pending at [2500,3000); 1096 to the end, 1904 wraps
	if got := segSizes(r); len(got) != 1 {
		t.Fatalf("wrapped write grew the chain: %v", got)
	}
	var iovs [4]unix.Iovec
	v := r.Iovecs(iovs[:])
	if len(v) != 2 || v[0].Len != 1596 || v[1].Len != 1904 {
		t.Fatalf("wrapped iovecs %+v", v)
	}
	if v[1].Base != &r.head.buf[0] {
		t.Fatal("second iovec does not start at the slab start")
	}
	contentCheck(t, r, ref)
	if p := r.Peek(); len(p) != 1596 {
		t.Fatalf("Peek len %d, want 1596", len(p))
	}
	// One-iovec budget gets only the first region.
	if v := r.Iovecs(iovs[:1]); len(v) != 1 || v[0].Len != 1596 {
		t.Fatalf("truncated iovecs %+v", v)
	}
	// Fill the slab exactly (wrapped), then one more byte chains.
	w(k4 - r.Len())
	if got := segSizes(r); len(got) != 1 || r.Len() != k4 {
		t.Fatalf("exact fill: %v len %d", got, r.Len())
	}
	contentCheck(t, r, ref)
	w(1)
	if got := segSizes(r); len(got) != 2 || got[1] != k16 {
		t.Fatalf("after overflow: %v", got)
	}
	v = r.Iovecs(iovs[:])
	if len(v) != 3 {
		t.Fatalf("wrapped head + tail should give 3 iovecs, got %d", len(v))
	}
	contentCheck(t, r, ref)
	// Consume across the wrap point inside the head.
	r.Consume(2000)
	ref = ref[2000:]
	contentCheck(t, r, ref)
	checkInvariants(t, r)
	r.Consume(r.Len())
	if got := segSizes(r); len(got) != 1 || got[0] != k16 {
		t.Fatalf("after drain kept %v, want the 16K tail", got)
	}
	if s := a.Stats(); s.InUse != k16 || s.Free != k4 {
		t.Fatalf("after drain: %+v", s)
	}
}

func TestBoundaryAtWriteEnd(t *testing.T) {
	r, a := newRing()
	r.Write(pattern(0, k4))
	if got := segSizes(r); len(got) != 1 || r.tail.n != k4 {
		t.Fatalf("exact 4K write: %v", got)
	}
	if a.Stats().Gets != 1 {
		t.Fatal("exact-fit write chained eagerly")
	}
	r.Write(pattern(k4, 10))
	if got := segSizes(r); len(got) != 2 || got[1] != k16 {
		t.Fatalf("next write: %v", got)
	}
	contentCheck(t, r, pattern(0, k4+10))
}

func TestConsumeToSegmentBoundary(t *testing.T) {
	r, a := newRing()
	ref := pattern(0, k4+100)
	r.Write(ref[:k4])
	r.Write(ref[k4:])
	r.Consume(k4) // exactly the first segment
	if got := segSizes(r); len(got) != 1 || got[0] != k16 {
		t.Fatalf("after boundary consume: %v", got)
	}
	if s := a.Stats(); s.Puts != 1 || s.Free != k4 {
		t.Fatalf("head slab not returned: %+v", s)
	}
	if p := r.Peek(); !bytes.Equal(p, ref[k4:]) {
		t.Fatal("Peek after boundary consume is not the next segment's data")
	}
	contentCheck(t, r, ref[k4:])
	checkInvariants(t, r)
	// Consume exactly to the end of the last segment keeps it, empty, at r=0.
	r.Consume(100)
	if r.head == nil || r.head != r.tail || r.head.n != 0 || r.head.r != 0 {
		t.Fatal("last segment not kept empty")
	}
	if r.Peek() != nil {
		t.Fatal("Peek on drained ring not nil")
	}
	gets := a.Stats().Gets
	r.Write(pattern(0, 1000))
	if a.Stats().Gets != gets {
		t.Fatal("write into kept segment called the allocator")
	}
}

func TestConsumeZeroAndPanics(t *testing.T) {
	r, _ := newRing()
	r.Write(pattern(0, 10))
	r.Consume(0)
	contentCheck(t, r, pattern(0, 10))
	for _, n := range []int{-1, 11} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("Consume(%d) did not panic", n)
				}
			}()
			r.Consume(n)
		}()
	}
}

func TestGrowthClasses(t *testing.T) {
	cases := []struct {
		name   string
		writes []int
		want   []int
	}{
		{"one 4K", []int{100}, []int{k4}},
		{"first fits 16K", []int{10000}, []int{k16}},
		{"first fits 64K", []int{k16 + 1}, []int{k64}},
		{"first fits 256K", []int{k64 + 1}, []int{k256}},
		{"exact 256K", []int{k256}, []int{k256}},
		{"large spans 256K", []int{3*k256 + 5}, []int{k256, k256, k256, k256}},
		{"remainder smallest fit", []int{100, k64}, []int{k4, k64}},
		{"remainder above floor", []int{100, k4 + k64}, []int{k4, k256}},
		{"geometric floor", []int{k4, 1, k16, 1, k64, 1}, []int{k4, k16, k64, k256}},
		{"all four classes", []int{k4, k16, k64, k256}, []int{k4, k16, k64, k256}},
		{"small backlog", repeat(100, 3000), []int{k4, k16, k64, k256}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, a := newRing()
			var ref []byte
			for _, n := range c.writes {
				p := pattern(len(ref), n)
				r.Write(p)
				ref = append(ref, p...)
			}
			got := segSizes(r)
			if !equalInts(got, c.want) {
				t.Fatalf("segments %v, want %v", got, c.want)
			}
			checkInvariants(t, r)
			contentCheck(t, r, ref)
			r.Reset()
			if s := a.Stats(); s.InUse != 0 {
				t.Fatalf("Reset left %d bytes in use", s.InUse)
			}
		})
	}
}

func repeat(n, times int) []int {
	out := make([]int, times)
	for i := range out {
		out[i] = n
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestResetThenReuse(t *testing.T) {
	a := slab.New()
	var r Ring
	for round := 0; round < 3; round++ {
		r.Init(a)
		if r.HighWater() != 0 {
			t.Fatal("Init did not reset HighWater")
		}
		ref := pattern(round, 300<<10)
		r.Write(ref)
		r.Consume(1000)
		contentCheck(t, &r, ref[1000:])
		r.Reset()
		if r.HighWater() != 300<<10 {
			t.Fatalf("Reset changed HighWater to %d", r.HighWater())
		}
		if s := a.Stats(); s.InUse != 0 {
			t.Fatalf("round %d: InUse %d after Reset", round, s.InUse)
		}
		if r.Len() != 0 || r.Peek() != nil || r.head != nil || r.inlineBusy {
			t.Fatal("Reset left state behind")
		}
	}
	// Spare segment structs must not be duplicated by repeated Init/Reset.
	n := 0
	for s := r.spare; s != nil; s = s.next {
		n++
		if n > 10 {
			t.Fatal("spare list grows across Init/Reset cycles")
		}
	}
	if n != 1 { // 300K = 256K + 256K: inline struct + 1 spare
		t.Fatalf("spare list has %d structs, want 1", n)
	}
	// Init without Reset releases what is held.
	r.Write(pattern(0, 5000))
	r.Init(a)
	if s := a.Stats(); s.InUse != 0 || r.Len() != 0 {
		t.Fatalf("Init without Reset leaked: %+v", s)
	}
}

func TestHighWater(t *testing.T) {
	r, _ := newRing()
	r.Write(make([]byte, 500))
	r.Consume(400)
	r.Write(make([]byte, 200))
	if r.HighWater() != 500 {
		t.Fatalf("HighWater %d, want 500", r.HighWater())
	}
	r.ResetHighWater()
	if r.HighWater() != 300 {
		t.Fatalf("after ResetHighWater: %d, want current Len 300", r.HighWater())
	}
	r.Write(make([]byte, 50))
	if r.HighWater() != 350 {
		t.Fatalf("HighWater %d, want 350", r.HighWater())
	}
}

func TestWritevOrder(t *testing.T) {
	r, _ := newRing()
	ref := pattern(0, 20000)
	r.Writev([][]byte{ref[:3], nil, ref[3:5000], ref[5000:5000], ref[5000:]})
	contentCheck(t, r, ref)
}

func TestIovecsBudget(t *testing.T) {
	r, _ := newRing()
	ref := pattern(0, k4+k16+10)
	r.Write(ref[:k4])
	r.Write(ref[k4 : k4+k16])
	r.Write(ref[k4+k16:])
	var iovs [8]unix.Iovec
	for budget := 0; budget <= 8; budget++ {
		v := r.Iovecs(iovs[:budget])
		got := iovBytes(v)
		if len(v) > budget || !bytes.Equal(got, ref[:len(got)]) {
			t.Fatalf("budget %d: bad prefix", budget)
		}
	}
	if v := r.Iovecs(iovs[:]); len(v) != 3 {
		t.Fatalf("want 3 iovecs, got %d", len(v))
	}
}

// TestQuick compares the Ring to a reference []byte queue over random
// writes and consumes, cross-checking Iovecs, Peek and allocator accounting.
func TestQuick(t *testing.T) {
	f := func(ops []uint32, seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		a := slab.New()
		var r Ring
		r.Init(a)
		var ref []byte
		written := 0
		hw := 0
		var iovs [16]unix.Iovec
		for _, op := range ops {
			switch op % 4 {
			case 0, 1: // write
				var n int
				switch (op >> 2) % 8 {
				case 0:
					n = 0
				case 1, 2, 3:
					n = int(op>>5) % 300
				case 4, 5:
					n = int(op>>5) % (8 << 10)
				case 6:
					n = int(op>>5) % (80 << 10)
				default:
					n = int(op>>5) % (600 << 10)
				}
				p := pattern(written, n)
				written += n
				if (op>>4)%2 == 0 || n < 2 {
					r.Write(p)
				} else {
					r.Writev([][]byte{p[:n/2], p[n/2:]})
				}
				ref = append(ref, p...)
			case 2: // consume
				n := 0
				if len(ref) > 0 {
					switch (op >> 2) % 3 {
					case 0:
						n = len(ref)
					default:
						n = rng.Intn(len(ref) + 1)
					}
				}
				p := r.Peek()
				if len(ref) > 0 && (len(p) == 0 || !bytes.Equal(p, ref[:len(p)])) {
					return false
				}
				r.Consume(n)
				ref = ref[n:]
			default: // Reset occasionally
				if (op>>2)%16 == 0 {
					r.Reset()
					ref = ref[:0]
				}
			}
			if len(ref) > hw {
				hw = len(ref)
			}
			if r.Len() != len(ref) || r.HighWater() != hw {
				return false
			}
			v := r.Iovecs(iovs[:])
			got := iovBytes(v)
			if len(v) < len(iovs) {
				if !bytes.Equal(got, ref) {
					return false
				}
			} else if !bytes.Equal(got, ref[:len(got)]) {
				return false
			}
			held := 0
			for s := r.head; s != nil; s = s.next {
				held += len(s.buf)
			}
			if a.Stats().InUse != held {
				return false
			}
		}
		// Drain via Peek/Consume and compare.
		var out []byte
		for r.Len() > 0 {
			p := r.Peek()
			out = append(out, p...)
			r.Consume(len(p))
		}
		if !bytes.Equal(out, ref) {
			return false
		}
		r.Reset()
		return a.Stats().InUse == 0
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}

func TestZeroAlloc(t *testing.T) {
	a := slab.New()
	var r Ring
	r.Init(a)
	small := make([]byte, 100)
	r.Write(small)
	r.Consume(100)
	if n := testing.AllocsPerRun(1000, func() {
		r.Write(small)
		r.Consume(100)
	}); n != 0 {
		t.Fatalf("Write/Consume into existing capacity allocates: %v", n)
	}
	bs := [][]byte{small[:10], small[:20], small}
	if n := testing.AllocsPerRun(1000, func() {
		r.Writev(bs)
		r.Consume(130)
	}); n != 0 {
		t.Fatalf("Writev allocates: %v", n)
	}
	r.Write(make([]byte, 3*k256))
	var iovs [8]unix.Iovec
	var sink int
	if n := testing.AllocsPerRun(1000, func() {
		sink += len(r.Iovecs(iovs[:]))
		sink += len(r.Peek())
	}); n != 0 {
		t.Fatalf("Iovecs/Peek allocate: %v", n)
	}
	_ = sink
	r.Reset()

	// Chaining every iteration: a 300 KiB write into a kept 256 KiB tail
	// needs a new segment each time; warm pools mean no allocation.
	big := make([]byte, 300<<10)
	for i := 0; i < 3; i++ {
		r.Write(big)
		r.Consume(len(big))
	}
	gets := a.Stats().Gets
	if n := testing.AllocsPerRun(100, func() {
		r.Write(big)
		r.Consume(len(big))
	}); n != 0 {
		t.Fatalf("chaining Write/Consume with warm pools allocates: %v", n)
	}
	if a.Stats().Gets == gets {
		t.Fatal("chaining gate did not exercise the allocator")
	}
	// Reset + rewrite with warm pools (pooled conn reuse) also allocates nothing.
	if n := testing.AllocsPerRun(100, func() {
		r.Reset()
		r.Init(a)
		r.Write(big)
	}); n != 0 {
		t.Fatalf("Reset/Init/Write with warm pools allocates: %v", n)
	}
}

func BenchmarkWriteConsume(b *testing.B) {
	for _, size := range []int{64, 1024, 16 << 10, 300 << 10} {
		b.Run(byteSize(size), func(b *testing.B) {
			a := slab.New()
			var r Ring
			r.Init(a)
			p := make([]byte, size)
			r.Write(p)
			r.Consume(size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				r.Write(p)
				r.Consume(size)
			}
		})
	}
}

func BenchmarkIovecs(b *testing.B) {
	a := slab.New()
	var r Ring
	r.Init(a)
	r.Write(make([]byte, 3000))
	r.Consume(2000)
	r.Write(make([]byte, 2500)) // wrapped head
	r.Write(make([]byte, 600<<10))
	var iovs [64]unix.Iovec
	b.ReportAllocs()
	for b.Loop() {
		if len(r.Iovecs(iovs[:])) == 0 {
			b.Fatal("no iovecs")
		}
	}
}

func byteSize(n int) string {
	switch {
	case n >= 1<<10 && n%(1<<10) == 0:
		return itoa(n>>10) + "KiB"
	default:
		return itoa(n) + "B"
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
