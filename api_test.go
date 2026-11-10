package twine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/twine/internal/mpsc"
)

func TestFutureResult(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	f, r := NewFuture[int](l)
	if v, err := f.Result(); v != 0 || err != nil || f.Done() {
		t.Fatalf("pending Result = %d, %v (Done %v)", v, err, f.Done())
	}
	r.Resolve(7)
	r.Reject(errors.New("ignored")) // second settle is a no-op
	if v, err := f.Result(); v != 7 || err != nil || !f.Done() {
		t.Fatalf("resolved Result = %d, %v (Done %v)", v, err, f.Done())
	}
	boom := errors.New("boom")
	g := Rejected[int](l, boom)
	if v, err := g.Result(); v != 0 || err != boom {
		t.Fatalf("rejected Result = %d, %v", v, err)
	}
}

// A Future with a nil loop runs its callbacks synchronously.
func TestFutureNilLoop(t *testing.T) {
	f, r := NewFuture[string](nil)
	var got string
	f.Then(func(s string) { got = s })
	r.Resolve("now")
	if got != "now" {
		t.Fatalf("Then on a nil-loop Future ran %q, want synchronous %q", got, "now")
	}
	var caught error
	Rejected[int](nil, ErrClosed).Catch(func(err error) { caught = err })
	if caught != ErrClosed {
		t.Fatalf("Catch on Rejected(nil) = %v", caught)
	}
	if v, err := Resolved[int](nil, 3).Await(context.Background()); v != 3 || err != nil {
		t.Fatalf("Await on Resolved(nil) = %d, %v", v, err)
	}
}

func TestAnyEmpty(t *testing.T) {
	e := startEngine(t, Loops(1))
	if _, err := awaitT(t, Any[int](e.loops[0])); !errors.Is(err, ErrNoFutures) {
		t.Fatalf("Any() = %v, want ErrNoFutures", err)
	}
	if v, err := awaitT(t, All[int](e.loops[0])); err != nil || v == nil || len(v) != 0 {
		t.Fatalf("All() = %v, %v; want an empty slice", v, err)
	}
}

func TestTimerActive(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	fired := make(chan struct{})
	var tm, zero, stopped *Timer
	onLoop(t, l, func() {
		tm = l.CallLater(5*time.Millisecond, func() { close(fired) })
		zero = l.CallLater(0, func() {})
		stopped = l.CallLater(time.Hour, func() {})
		if !tm.Active() || !zero.Active() || !stopped.Active() {
			t.Errorf("new timers inactive: %v %v %v", tm.Active(), zero.Active(), stopped.Active())
		}
		stopped.Stop()
		if stopped.Active() {
			t.Error("stopped timer still Active")
		}
	})
	<-fired
	onLoop(t, l, func() {
		if tm.Active() || zero.Active() {
			t.Errorf("fired timers still Active: %v %v", tm.Active(), zero.Active())
		}
		tm.Reset(time.Hour) // Reset does not re-arm a fired timer
		if tm.Active() {
			t.Error("Reset re-armed a fired timer")
		}
	})
}

// DeferToThread from the loop never blocks it: with one worker busy, jobs
// beyond the submission queue spill to a helper goroutine while the loop
// keeps serving connections, and every job still runs.
func TestDeferToThreadSpill(t *testing.T) {
	e := startEngine(t, Loops(1), WorkerPool(1))
	l := e.loops[0]
	lst := listen(t, e, echoFactory())
	c := dialTCP(t, lst.Addr().String())

	release := make(chan struct{})
	const jobs = 20 // queue capacity is 4 per worker
	var ran atomic.Int32
	futs := make([]*Future[int], jobs)
	onLoop(t, l, func() {
		for i := range futs {
			futs[i] = DeferToThread(l, func() (int, error) {
				<-release
				ran.Add(1)
				return i, nil
			})
		}
	})
	if n := len(e.pool.jobs); n != cap(e.pool.jobs) {
		t.Logf("submission queue holds %d of %d", n, cap(e.pool.jobs))
	}
	start := time.Now()
	echoOnce(t, c, []byte("loop still serves"))
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("echo took %v while the worker pool was saturated", d)
	}
	close(release)
	for i, f := range futs {
		if v, err := awaitT(t, f); err != nil || v != i {
			t.Fatalf("job %d = %d, %v", i, v, err)
		}
	}
	if ran.Load() != jobs {
		t.Fatalf("%d of %d jobs ran", ran.Load(), jobs)
	}
}

// When the lock-free task ring is full, CallFromThread spills into the
// overflow slice and every task still runs once, in FIFO order per producer.
func TestCallFromThreadOverflow(t *testing.T) {
	e := New(Loops(1), Watchdog(-1))
	l := e.loops[0]
	l.tasks = mpsc.New(8) // tiny ring; loops exist before Run, pollers do not
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	t.Cleanup(func() {
		e.Stop()
		if err := <-errCh; err != nil {
			t.Errorf("Run: %v", err)
		}
	})

	gate := make(chan struct{})
	l.CallFromThread(func() { <-gate }) // hold the loop so the ring fills
	const producers, per = 4, 500
	var mu sync.Mutex
	seen := make([][]int, producers)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				l.CallFromThread(func() {
					mu.Lock()
					seen[p] = append(seen[p], i)
					mu.Unlock()
				})
			}
		}()
	}
	wg.Wait()
	if !l.overflowActive.Load() {
		t.Error("ring of 8 never spilled")
	}
	close(gate)
	done := make(chan struct{})
	l.CallFromThread(func() { close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("overflowed tasks never drained")
	}
	mu.Lock()
	defer mu.Unlock()
	for p, s := range seen {
		if len(s) != per {
			t.Fatalf("producer %d: %d of %d tasks ran", p, len(s), per)
		}
		for i, v := range s {
			if v != i {
				t.Fatalf("producer %d: task %d ran in position %d", p, v, i)
			}
		}
	}
}
