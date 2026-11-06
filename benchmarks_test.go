package twine

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Micro-benchmarks for design Section 12 (timers_1m, callfromthread) and the
// Phase 3 acceptance gates. The network harness lives in the bench module.

// benchNoop is the timer callback; hoisted so CallLater never sees a fresh
// closure.
var benchNoop = func() {}

// liveTimers is the population of pending timers the timer benchmarks keep
// on the wheel while measuring.
const liveTimers = 1 << 20

// benchDelays returns n deterministic delays in [1ms, 1ms+span).
func benchDelays(n int, span time.Duration) []time.Duration {
	rng := rand.New(rand.NewPCG(1, 2))
	d := make([]time.Duration, n)
	for i := range d {
		d[i] = time.Millisecond + time.Duration(rng.Int64N(int64(span)))
	}
	return d
}

// benchOnLoop runs fn on l and waits for it, with a timeout generous enough
// for setup of a million timers plus a full benchtime.
func benchOnLoop(b *testing.B, l *Loop, fn func()) {
	b.Helper()
	done := make(chan struct{})
	l.CallFromThread(func() { defer close(done); fn() })
	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		b.Fatal("loop did not run benchmark task")
	}
}

// BenchmarkTimers1M measures CallLater on a wheel holding 1M live timers.
// insert+cancel: one op is CallLater followed by Stop (the Phase 3 gate is
// <= 50 ns and 0 allocs). fire: one op is one timer expiring and running its
// callback during a wheel advance.
func BenchmarkTimers1M(b *testing.B) {
	b.Run("insert+cancel", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		delays := benchDelays(4096, time.Hour)
		benchOnLoop(b, l, func() {
			for i := 0; i < liveTimers; i++ {
				l.CallLater(delays[i&4095]+time.Hour, benchNoop)
			}
			// One untimed insert+cancel faults in any lazily built state.
			l.CallLater(time.Second, benchNoop).Stop()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.CallLater(delays[i&4095], benchNoop).Stop()
			}
			b.StopTimer()
			if got := l.wheel.Len(); got != liveTimers {
				b.Errorf("wheel holds %d timers, want %d", got, liveTimers)
			}
		})
	})
	b.Run("fire", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		delays := benchDelays(4096, time.Second)
		benchOnLoop(b, l, func() {
			// Drive the wheel with a synthetic clock: each batch is
			// scheduled within 1 s of now and fired by advancing 2 s. The
			// engine is discarded afterwards, so leaving its wheel ahead of
			// the real clock is harmless.
			now := l.nowNs
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			for left := b.N; left > 0; {
				batch := min(left, liveTimers)
				l.nowNs = now
				for i := 0; i < batch; i++ {
					l.CallLater(delays[i&4095], benchNoop)
				}
				now += int64(2 * time.Second)
				b.StartTimer()
				fired := l.wheel.Advance(now)
				b.StopTimer()
				if fired != batch {
					b.Errorf("fired %d of %d", fired, batch)
					return
				}
				left -= batch
			}
		})
	})
}

// BenchmarkTimeAfterFunc1M is the baseline for BenchmarkTimers1M: the Go
// runtime timer heap with 1M live timers, one op = AfterFunc + Stop.
func BenchmarkTimeAfterFunc1M(b *testing.B) {
	delays := benchDelays(4096, time.Hour)
	live := make([]*time.Timer, liveTimers)
	for i := range live {
		live[i] = time.AfterFunc(delays[i&4095]+time.Hour, benchNoop)
	}
	defer func() {
		for _, t := range live {
			t.Stop()
		}
	}()
	b.Run("insert+cancel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			time.AfterFunc(delays[i&4095], benchNoop).Stop()
		}
	})
}

// producers is the producer count for the cross-thread benchmarks (design
// Section 12: "16 producers").
const producers = 16

// inflightWindow bounds tasks pushed but not yet run per producer in the
// windowed variants: 16 x 2048 = 32K, half the default 64K MPSC ring, so the
// lock-free path is measured rather than the overflow spill.
const inflightWindow = 2048

// paddedCounter keeps per-producer counters on separate cache lines.
type paddedCounter struct {
	n atomic.Int64
	_ [56]byte
}

// stallTimeout is how long a cross-thread benchmark waits without progress
// before declaring a lost wakeup instead of hanging the run.
const stallTimeout = 5 * time.Second

// waitProgress polls count until it reaches want. It returns false if count
// makes no progress for stallTimeout.
func waitProgress(count func() int64, want int64) bool {
	last, lastAt := count(), time.Now()
	for {
		n := count()
		if n >= want {
			return true
		}
		if n != last {
			last, lastAt = n, time.Now()
		} else if time.Since(lastAt) > stallTimeout {
			return false
		}
		time.Sleep(20 * time.Microsecond)
	}
}

// crossThreadBench drives producers goroutines that each submit b.N/producers
// preallocated functions through submit and waits until every function has
// run. With window > 0 a producer never has more than window functions
// outstanding. A run that stops making progress fails with a lost-wakeup
// diagnosis rather than hanging.
func crossThreadBench(b *testing.B, window int, submit func(fn func())) {
	ran := make([]paddedCounter, producers)
	fns := make([]func(), producers)
	for p := range fns {
		c := &ran[p].n
		fns[p] = func() { c.Add(1) }
	}
	quota := make([]int64, producers)
	for p := range quota {
		quota[p] = int64(b.N / producers)
		if p < b.N%producers {
			quota[p]++
		}
	}
	total := func() int64 {
		var t int64
		for p := range ran {
			t += ran[p].n.Load()
		}
		return t
	}
	var (
		wg      sync.WaitGroup
		stalled atomic.Bool
	)
	b.ResetTimer()
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			fn, c, q := fns[p], &ran[p].n, quota[p]
			for i := int64(0); i < q; i++ {
				if window > 0 {
					for i-c.Load() >= int64(window) {
						if stalled.Load() {
							return
						}
						runtime.Gosched()
					}
				}
				submit(fn)
			}
		}(p)
	}
	ok := waitProgress(total, int64(b.N))
	b.StopTimer()
	stalled.Store(!ok)
	wg.Wait()
	if !ok {
		b.Fatalf("lost wakeup: %d of %d tasks ran, no progress for %v", total(), b.N, stallTimeout)
	}
}

// BenchmarkCallFromThread is design Section 12 callfromthread: 16 producer
// goroutines push a preallocated func to one loop. wakeups/op is the eventfd
// writes per call (the Phase 3 gate is <= 0.1). window=2048 bounds each
// producer's outstanding tasks so the MPSC ring never fills; flood lets
// producers run unbounded, which spills into the mutex-guarded overflow.
func BenchmarkCallFromThread(b *testing.B) {
	for _, v := range []struct {
		name   string
		window int
	}{{"window=2048", inflightWindow}, {"flood", 0}} {
		b.Run(v.name, func(b *testing.B) {
			e := startEngine(b, Loops(1))
			l := e.loops[0]
			onLoop(b, l, func() {}) // settle the startup wakeup
			m0 := l.Metrics()
			b.ReportAllocs()
			crossThreadBench(b, v.window, l.CallFromThread)
			m1 := l.Metrics()
			b.ReportMetric(float64(m1.Wakeups-m0.Wakeups)/float64(b.N), "wakeups/op")
			b.ReportMetric(float64(m1.WakeupsCoalesced-m0.WakeupsCoalesced)/float64(b.N), "coalesced/op")
			b.ReportMetric(float64(m1.Iterations-m0.Iterations)/float64(b.N), "iterations/op")
		})
	}
}

// BenchmarkChannelSend is the baseline for BenchmarkCallFromThread: the same
// 16 producers send the preallocated func on a buffered channel (capacity
// 64K, like the default task ring) to one consumer goroutine that selects on
// it and a quit channel.
func BenchmarkChannelSend(b *testing.B) {
	for _, v := range []struct {
		name   string
		window int
	}{{"window=2048", inflightWindow}, {"flood", 0}} {
		b.Run(v.name, func(b *testing.B) {
			ch := make(chan func(), 64<<10)
			quit := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					select {
					case fn := <-ch:
						fn()
					case <-quit:
						return
					}
				}
			}()
			b.ReportAllocs()
			crossThreadBench(b, v.window, func(fn func()) { ch <- fn })
			close(quit)
			<-done
		})
	}
}

// BenchmarkDeferToThread measures a round trip through the worker pool.
// await: the benchmark goroutine calls DeferToThread and Awaits the result.
// then: the result is observed by a Then callback on the loop (the full
// worker -> CallFromThread -> loop path). parallel: await from GOMAXPROCS
// goroutines.
func BenchmarkDeferToThread(b *testing.B) {
	work := func() (int, error) { return 1, nil }
	ctx := context.Background()
	b.Run("await", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := DeferToThread(l, work).Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("then", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		done := make(chan struct{}, 1)
		cb := func(int) { done <- struct{}{} }
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			DeferToThread(l, work).Then(cb)
			select {
			case <-done:
			case <-time.After(stallTimeout):
				b.Fatalf("lost wakeup: Then callback %d did not run within %v", i, stallTimeout)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := DeferToThread(l, work).Await(ctx); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

// BenchmarkFutureResolve measures NewFuture + Then + Resolve. onloop: all on
// the loop, so callbacks dispatch inline. offloop: created and resolved by
// the benchmark goroutine, so each resolution is routed to the loop with
// CallFromThread; the run ends when every callback has run.
func BenchmarkFutureResolve(b *testing.B) {
	b.Run("onloop", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		var sum int
		cb := func(v int) { sum += v }
		benchOnLoop(b, l, func() {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, r := NewFuture[int](l)
				f.Then(cb)
				r.Resolve(1)
			}
			b.StopTimer()
		})
		if sum != b.N {
			b.Fatalf("callbacks ran %d times, want %d", sum, b.N)
		}
	})
	b.Run("offloop", func(b *testing.B) {
		e := startEngine(b, Loops(1))
		l := e.loops[0]
		var ran atomic.Int64
		cb := func(int) { ran.Add(1) }
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f, r := NewFuture[int](l)
			f.Then(cb)
			r.Resolve(1)
		}
		ok := waitProgress(ran.Load, int64(b.N))
		b.StopTimer()
		if !ok {
			b.Fatalf("lost wakeup: %d of %d callbacks ran, no progress for %v", ran.Load(), b.N, stallTimeout)
		}
	})
}
