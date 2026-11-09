package mpsc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
)

func TestNewRoundsCapacity(t *testing.T) {
	cases := []struct{ in, want int }{
		{-5, 2}, {0, 2}, {1, 2}, {2, 2}, {3, 4}, {4, 4}, {5, 8},
		{1000, 1024}, {1024, 1024}, {1025, 2048}, {65536, 65536},
	}
	for _, c := range cases {
		if got := New(c.in).Cap(); got != c.want {
			t.Errorf("New(%d).Cap() = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestEmptyPop(t *testing.T) {
	q := New(4)
	if fn, ok := q.Pop(); ok || fn != nil {
		t.Fatalf("Pop on empty queue = (%v, %v), want (nil, false)", fn != nil, ok)
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}

func TestFullPush(t *testing.T) {
	q := New(8)
	noop := func() {}
	for i := 0; i < 8; i++ {
		if !q.Push(noop) {
			t.Fatalf("Push %d failed on non-full queue", i)
		}
	}
	if q.Len() != 8 {
		t.Fatalf("Len = %d, want 8", q.Len())
	}
	if q.Push(noop) {
		t.Fatal("Push on full queue returned true")
	}
	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop on full queue failed")
	}
	if !q.Push(noop) {
		t.Fatal("Push after Pop failed")
	}
	if q.Push(noop) {
		t.Fatal("Push on refilled queue returned true")
	}
}

// TestWraparoundFIFO cycles the queue many laps with varying fill levels and
// checks strict FIFO order against a reference slice.
func TestWraparoundFIFO(t *testing.T) {
	f := func(ops []uint8) bool {
		q := New(16)
		var got int
		fns := make([]func(), 4096)
		for i := range fns {
			i := i
			fns[i] = func() { got = i }
		}
		var ref []int
		next := 0
		for _, op := range ops {
			if op&1 == 0 {
				n := int(op>>1) % 20
				for j := 0; j < n; j++ {
					ok := q.Push(fns[next%len(fns)])
					if ok != (len(ref) < 16) {
						return false
					}
					if ok {
						ref = append(ref, next%len(fns))
						next++
					}
				}
			} else {
				n := int(op>>1) % 20
				for j := 0; j < n; j++ {
					fn, ok := q.Pop()
					if ok != (len(ref) > 0) {
						return false
					}
					if !ok {
						break
					}
					fn()
					if got != ref[0] {
						return false
					}
					ref = ref[1:]
				}
			}
			if q.Len() != len(ref) {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

func TestPopClearsSlot(t *testing.T) {
	q := New(2)
	q.Push(func() {})
	q.Pop()
	for i := range q.cells {
		if q.cells[i].fn != nil {
			t.Fatalf("cell %d retains func after Pop", i)
		}
	}
}

// TestStress runs 64 producers concurrently with a draining consumer and
// checks every item arrives exactly once and in per-producer FIFO order.
func TestStress(t *testing.T) {
	const producers = 64
	perProducer := 1_000_000 / producers
	if testing.Short() {
		perProducer = 10_000
	}

	// Each item records (producer, index) into these variables, which only
	// the consumer goroutine touches (the funcs run on the consumer).
	var gotP, gotI int
	fns := make([][]func(), producers)
	for p := range fns {
		fns[p] = make([]func(), perProducer)
		for i := range fns[p] {
			p, i := p, i
			fns[p][i] = func() { gotP, gotI = p, i }
		}
	}

	q := New(1024) // much smaller than the total so the full path is exercised
	var start, wg sync.WaitGroup
	var fullHits atomic.Int64
	start.Add(1)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			start.Wait()
			for i := 0; i < perProducer; i++ {
				for !q.Push(fns[p][i]) {
					fullHits.Add(1)
					runtime.Gosched()
				}
			}
		}(p)
	}

	next := make([]int, producers)
	total := producers * perProducer
	done := make(chan struct{})
	var failure string
	go func() {
		defer close(done)
		for n := 0; n < total; {
			fn, ok := q.Pop()
			if !ok {
				runtime.Gosched()
				continue
			}
			fn()
			if gotI != next[gotP] {
				failure = "out-of-order or duplicate item"
				return
			}
			next[gotP]++
			n++
		}
	}()
	start.Done()
	wg.Wait()
	<-done
	if failure != "" {
		t.Fatal(failure)
	}
	for p, n := range next {
		if n != perProducer {
			t.Fatalf("producer %d: received %d items, want %d", p, n, perProducer)
		}
	}
	if fn, ok := q.Pop(); ok || fn != nil {
		t.Fatal("queue not empty after draining all items")
	}
	t.Logf("total=%d full-retries=%d", total, fullHits.Load())
}

func TestZeroAllocs(t *testing.T) {
	q := New(1024)
	noop := func() {}
	if a := testing.AllocsPerRun(1000, func() { q.Push(noop) }); a != 0 {
		t.Fatalf("Push allocates: %v", a)
	}
	if a := testing.AllocsPerRun(1000, func() { q.Pop() }); a != 0 {
		t.Fatalf("Pop allocates: %v", a)
	}
	if a := testing.AllocsPerRun(1000, func() {
		q.Push(noop)
		fn, _ := q.Pop()
		fn()
	}); a != 0 {
		t.Fatalf("Push+Pop allocates: %v", a)
	}
}

// BenchmarkPush16Producers measures throughput with 16 producer goroutines
// pushing concurrently while a single consumer drains. ns/op is wall time per
// item delivered.
func BenchmarkPush16Producers(b *testing.B) {
	const producers = 16
	q := New(65536)
	noop := func() {}
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		n := b.N / producers
		if p < b.N%producers {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				for !q.Push(noop) {
					runtime.Gosched()
				}
			}
		}(n)
	}
	for got := 0; got < b.N; {
		if fn, ok := q.Pop(); ok {
			fn()
			got++
		} else {
			runtime.Gosched()
		}
	}
	wg.Wait()
}

// BenchmarkPushPopSingle measures uncontended Push+Pop on one goroutine.
func BenchmarkPushPopSingle(b *testing.B) {
	q := New(1024)
	noop := func() {}
	b.ReportAllocs()
	for b.Loop() {
		q.Push(noop)
		q.Pop()
	}
}
