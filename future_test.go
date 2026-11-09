package twine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// awaitT awaits f from the test goroutine with a timeout.
// A panic in Await (e.g. a false ErrAwaitOnLoop) is reported as a test error
// instead of killing the test binary.
func awaitT[T any](t testing.TB, f *Future[T]) (v T, err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Future.Await panicked on a non-loop goroutine: %v", r)
			err = fmt.Errorf("Await panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err = f.Await(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Future did not settle within 5s")
	}
	return v, err
}

// cbLog records callback invocations and whether each ran on the loop.
type cbLog struct {
	l       *Loop
	mu      sync.Mutex
	events  []string
	offLoop []string
	done    chan struct{}
	want    int
}

func newCBLog(l *Loop, want int) *cbLog { return &cbLog{l: l, done: make(chan struct{}), want: want} }

func (c *cbLog) add(ev string) {
	in := c.l.InLoop()
	c.mu.Lock()
	c.events = append(c.events, ev)
	if !in {
		c.offLoop = append(c.offLoop, ev)
	}
	if len(c.events) == c.want {
		close(c.done)
	}
	c.mu.Unlock()
}

func (c *cbLog) wait(t testing.TB) []string {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		c.mu.Lock()
		t.Errorf("only %d of %d callbacks ran: %v", len(c.events), c.want, c.events)
		c.mu.Unlock()
	}
	// Give stray extra callbacks a moment to show up.
	time.Sleep(10 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.offLoop) > 0 {
		t.Errorf("callbacks ran off the loop: %v", c.offLoop)
	}
	return append([]string(nil), c.events...)
}

func TestFutureResolveCallbackOrder(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	f, r := NewFuture[int](l)
	log := newCBLog(l, 5)
	f.Then(func(v int) { log.add(fmt.Sprint("then1:", v)) }).
		Catch(func(err error) { log.add("catch") }).
		Then(func(v int) { log.add(fmt.Sprint("then2:", v)) }).
		Finally(func() { log.add("finally1") }).
		Then(func(v int) { log.add(fmt.Sprint("then3:", v)) }).
		Finally(func() { log.add("finally2") })
	if f.Done() {
		t.Fatal("Done() before Resolve")
	}
	go r.Resolve(5) // off-loop resolution is routed to the loop
	got := log.wait(t)
	want := "[then1:5 then2:5 then3:5 finally1 finally2]"
	if fmt.Sprint(got) != want {
		t.Errorf("callbacks = %v, want %s", got, want)
	}
	if !f.Done() {
		t.Error("Done() false after Resolve")
	}
	// Resolve twice / Reject after Resolve are no-ops.
	r.Resolve(6)
	r.Reject(errBoom)
	time.Sleep(20 * time.Millisecond)
	if v, err := awaitT(t, f); v != 5 || err != nil {
		t.Errorf("after second Resolve/Reject: Await = %v, %v; want 5, nil", v, err)
	}
	log.mu.Lock()
	if len(log.events) != 5 {
		t.Errorf("callbacks re-ran after a second Resolve: %v", log.events)
	}
	log.mu.Unlock()
}

func TestFutureRejectCallbackOrder(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	f, r := NewFuture[int](l)
	log := newCBLog(l, 4)
	f.Catch(func(err error) { log.add("catch1:" + err.Error()) }).
		Then(func(int) { log.add("then") }).
		Finally(func() { log.add("finally") }).
		Catch(func(err error) { log.add("catch2:" + err.Error()) }).
		Catch(func(err error) { log.add("catch3") })
	onLoop(t, l, func() { r.Reject(errBoom) }) // on-loop resolution
	got := log.wait(t)
	want := "[catch1:boom catch2:boom catch3 finally]"
	if fmt.Sprint(got) != want {
		t.Errorf("callbacks = %v, want %s", got, want)
	}
	r.Reject(errors.New("second"))
	r.Resolve(1)
	if _, err := awaitT(t, f); err != errBoom {
		t.Errorf("Await err = %v, want errBoom (second Reject/Resolve must be no-ops)", err)
	}
}

func TestFutureThenPanicRejects(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	f, r := NewFuture[int](l)
	log := newCBLog(l, 3)
	var caught atomic.Value
	f.Then(func(int) { log.add("then1"); panic(errBoom) }).
		Then(func(int) { log.add("then2-must-not-run") }).
		Catch(func(err error) { caught.Store(err); log.add("catch") }).
		Finally(func() { log.add("finally") })
	r.Resolve(1)
	got := log.wait(t)
	if fmt.Sprint(got) != "[then1 catch finally]" {
		t.Errorf("callbacks = %v, want [then1 catch finally]", got)
	}
	err, _ := caught.Load().(error)
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Catch got %v, want *PanicError", err)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("errors.Is(PanicError, errBoom) = false")
	}
	// The Future is now rejected for later observers too.
	if _, aerr := awaitT(t, f); !errors.As(aerr, &pe) {
		t.Errorf("Await after panicking Then = %v, want *PanicError", aerr)
	}
	late := make(chan error, 1)
	f.Catch(func(err error) { late <- err })
	if lerr, ok := recvErr(t, late, 5*time.Second, "late Catch"); ok && !errors.As(lerr, &pe) {
		t.Errorf("late Catch got %v, want *PanicError", lerr)
	}
	// The loop survived.
	onLoop(t, l, func() {})
}

func TestFutureAwaitOnLoopPanics(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	var rec any
	onLoop(t, l, func() {
		f, _ := NewFuture[int](l)
		defer func() { rec = recover() }()
		_, _ = f.Await(context.Background())
	})
	if err, ok := rec.(error); !ok || !errors.Is(err, ErrAwaitOnLoop) {
		t.Errorf("Await on the loop recovered %v, want panic(ErrAwaitOnLoop)", rec)
	}
}

func TestFutureAwaitFromGoroutine(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	t.Run("resolves", func(t *testing.T) {
		f, r := NewFuture[string](l)
		l.CallFromThread(func() { l.CallLater(20*time.Millisecond, func() { r.Resolve("done") }) })
		if v, err := awaitT(t, f); v != "done" || err != nil {
			t.Errorf("Await = %q, %v", v, err)
		}
	})
	t.Run("ctx cancel", func(t *testing.T) {
		f, r := NewFuture[string](l)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		start := time.Now()
		v, err := f.Await(ctx)
		if !errors.Is(err, context.Canceled) || v != "" {
			t.Errorf("Await = %q, %v; want \"\", context.Canceled", v, err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Errorf("Await returned %v after cancel", d)
		}
		// The Future is unaffected and can still settle.
		r.Resolve("later")
		if v, err := awaitT(t, f); v != "later" || err != nil {
			t.Errorf("Await after cancel+resolve = %q, %v", v, err)
		}
	})
	t.Run("many waiters", func(t *testing.T) {
		f, r := NewFuture[int](l)
		var wg sync.WaitGroup
		var bad atomic.Int32
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if v, err := f.Await(context.Background()); v != 9 || err != nil {
					bad.Add(1)
				}
			}()
		}
		time.Sleep(10 * time.Millisecond)
		r.Resolve(9)
		wg.Wait()
		if bad.Load() != 0 {
			t.Errorf("%d waiters got the wrong result", bad.Load())
		}
	})
}

func TestFutureResolvedRejectedHelpers(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	rf := Resolved(l, 3)
	if !rf.Done() {
		t.Error("Resolved(...).Done() = false")
	}
	if v, err := awaitT(t, rf); v != 3 || err != nil {
		t.Errorf("Resolved Await = %v, %v", v, err)
	}
	jf := Rejected[int](l, errBoom)
	if !jf.Done() {
		t.Error("Rejected(...).Done() = false")
	}
	if _, err := awaitT(t, jf); err != errBoom {
		t.Errorf("Rejected Await err = %v", err)
	}
	if rf.Loop() != l || jf.Loop() != l {
		t.Error("Loop() does not return the Future's loop")
	}
}

func TestFutureCallbacksAfterSettlement(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	rf := Resolved(l, 7)
	jf := Rejected[int](l, errBoom)
	time.Sleep(10 * time.Millisecond) // the settle dispatch has run
	log := newCBLog(l, 5)
	// Registered from a foreign goroutine after settlement.
	rf.Then(func(v int) { log.add(fmt.Sprint("then:", v)) }).Finally(func() { log.add("rfinally") })
	jf.Then(func(int) { log.add("jthen-must-not-run") }).Catch(func(err error) { log.add("catch:" + err.Error()) })
	// Registered on the loop after settlement.
	l.CallFromThread(func() {
		rf.Then(func(v int) { log.add(fmt.Sprint("loopthen:", v)) })
		jf.Finally(func() { log.add("jfinally") })
	})
	got := log.wait(t)
	seen := map[string]int{}
	for _, ev := range got {
		seen[ev]++
	}
	for _, ev := range []string{"then:7", "rfinally", "catch:boom", "loopthen:7", "jfinally"} {
		if seen[ev] != 1 {
			t.Errorf("callback %q ran %d times, want 1 (all: %v)", ev, seen[ev], got)
		}
	}
	if seen["jthen-must-not-run"] != 0 {
		t.Errorf("Then ran on a rejected Future: %v", got)
	}
}

// Callbacks never run concurrently with each other or with loop code: the
// plain (non-atomic) counter would be flagged by -race otherwise.
func TestFutureNoConcurrentCallbacks(t *testing.T) {
	const n = 300
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	var counter int // touched only on the loop
	var active, overlaps, offLoop atomic.Int32
	cb := func() {
		if !l.InLoop() {
			offLoop.Add(1)
		}
		if active.Add(1) != 1 {
			overlaps.Add(1)
		}
		counter++
		active.Add(-1)
	}
	var wg sync.WaitGroup
	futs := make([]*Future[int], n)
	ress := make([]Resolver[int], n)
	for i := range futs {
		futs[i], ress[i] = NewFuture[int](l)
	}
	for i := 0; i < n; i++ {
		wg.Add(3)
		go func(i int) { defer wg.Done(); futs[i].Then(func(int) { cb() }).Finally(cb) }(i)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				ress[i].Resolve(i)
			} else {
				l.CallFromThread(func() { ress[i].Resolve(i) })
			}
		}(i)
		go func(i int) { defer wg.Done(); futs[i].Then(func(int) { cb() }) }(i)
	}
	// Loop code running alongside.
	for i := 0; i < n; i++ {
		l.CallFromThread(cb)
	}
	wg.Wait()
	var got int
	if !waitUntil(5*time.Second, func() bool {
		onLoop(t, l, func() { got = counter })
		return got == 4*n
	}) {
		t.Errorf("counter = %d, want %d", got, 4*n)
	}
	if overlaps.Load() != 0 {
		t.Errorf("%d overlapping callback executions", overlaps.Load())
	}
	if offLoop.Load() != 0 {
		t.Errorf("%d callbacks ran off the loop", offLoop.Load())
	}
}

func TestFutureAll(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	t.Run("resolves in input order", func(t *testing.T) {
		fs := make([]*Future[int], 5)
		rs := make([]Resolver[int], 5)
		for i := range fs {
			fs[i], rs[i] = NewFuture[int](l)
		}
		all := All(l, fs...)
		for i := len(rs) - 1; i >= 0; i-- { // resolve in reverse
			rs[i].Resolve(i * 10)
			if i > 0 && all.Done() {
				t.Fatalf("All settled before every input resolved")
			}
		}
		v, err := awaitT(t, all)
		if err != nil || fmt.Sprint(v) != "[0 10 20 30 40]" {
			t.Errorf("All = %v, %v; want [0 10 20 30 40]", v, err)
		}
	})
	t.Run("rejects on first rejection", func(t *testing.T) {
		fs := make([]*Future[int], 4)
		rs := make([]Resolver[int], 4)
		for i := range fs {
			fs[i], rs[i] = NewFuture[int](l)
		}
		all := All(l, fs...)
		first := errors.New("first")
		rs[0].Resolve(1)
		rs[2].Reject(first)
		// All must reject now without waiting for the pending inputs.
		_, err := awaitT(t, all)
		if err != first {
			t.Errorf("All err = %v, want first rejection", err)
		}
		rs[1].Reject(errors.New("second"))
		rs[3].Resolve(4)
		time.Sleep(10 * time.Millisecond)
		if _, err := awaitT(t, all); err != first {
			t.Errorf("All err changed to %v after later inputs settled", err)
		}
	})
	t.Run("empty", func(t *testing.T) {
		v, err := awaitT(t, All[int](l))
		if err != nil || v == nil || len(v) != 0 {
			t.Errorf("All() = %#v, %v; want empty slice", v, err)
		}
	})
}

func TestFutureAny(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	t.Run("first value wins", func(t *testing.T) {
		f1, r1 := NewFuture[string](l)
		f2, r2 := NewFuture[string](l)
		f3, r3 := NewFuture[string](l)
		anyF := Any(l, f1, f2, f3)
		r3.Reject(errors.New("x"))
		r2.Resolve("two")
		r1.Resolve("one")
		if v, err := awaitT(t, anyF); v != "two" || err != nil {
			t.Errorf("Any = %q, %v; want \"two\"", v, err)
		}
	})
	t.Run("rejects only when all reject", func(t *testing.T) {
		f1, r1 := NewFuture[string](l)
		f2, r2 := NewFuture[string](l)
		anyF := Any(l, f1, f2)
		r1.Reject(errors.New("e1"))
		time.Sleep(20 * time.Millisecond)
		if anyF.Done() {
			t.Fatal("Any settled after one of two inputs rejected")
		}
		last := errors.New("e2")
		r2.Reject(last)
		if _, err := awaitT(t, anyF); err == nil {
			t.Error("Any resolved although every input rejected")
		} else if err != last {
			t.Logf("Any rejected with %v", err)
		}
	})
}

func TestFutureMap(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	f, r := NewFuture[int](l)
	m := Map(f, func(v int) (string, error) {
		if !l.InLoop() {
			return "", errors.New("Map fn ran off the loop")
		}
		return fmt.Sprint("v=", v), nil
	})
	if m.Loop() != l {
		t.Error("Map result is on a different loop")
	}
	r.Resolve(4)
	if v, err := awaitT(t, m); v != "v=4" || err != nil {
		t.Errorf("Map = %q, %v", v, err)
	}

	f2, r2 := NewFuture[int](l)
	m2 := Map(f2, func(int) (string, error) { return "", errBoom })
	r2.Resolve(1)
	if _, err := awaitT(t, m2); err != errBoom {
		t.Errorf("Map with fn error = %v, want errBoom", err)
	}

	f3, r3 := NewFuture[int](l)
	called := atomic.Bool{}
	m3 := Map(f3, func(int) (string, error) { called.Store(true); return "x", nil })
	r3.Reject(errBoom)
	if _, err := awaitT(t, m3); err != errBoom {
		t.Errorf("Map of rejected = %v, want errBoom", err)
	}
	if called.Load() {
		t.Error("Map fn called for a rejected Future")
	}
}

func TestFutureSleep(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	const d = 30 * time.Millisecond
	elapsed := make(chan time.Duration, 1)
	var f *Future[struct{}]
	onLoop(t, l, func() {
		start := time.Now()
		f = Sleep(l, d)
		f.Then(func(struct{}) { elapsed <- time.Since(start) })
	})
	if _, err := awaitT(t, f); err != nil {
		t.Fatal(err)
	}
	select {
	case el := <-elapsed:
		if el < d-2*time.Millisecond || el > d+500*time.Millisecond {
			t.Errorf("Sleep(%v) resolved after %v", d, el)
		}
	case <-time.After(5 * time.Second):
		t.Error("Sleep's Then never ran")
	}
}

func TestDeferToThread(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	t.Run("value", func(t *testing.T) {
		var inLoop atomic.Bool
		inLoop.Store(true)
		thenOnLoop := make(chan bool, 1)
		var f *Future[int]
		onLoop(t, l, func() {
			f = DeferToThread(l, func() (int, error) {
				inLoop.Store(l.InLoop())
				time.Sleep(10 * time.Millisecond) // blocking is fine here
				return 42, nil
			})
			f.Then(func(int) { thenOnLoop <- l.InLoop() })
		})
		if v, err := awaitT(t, f); v != 42 || err != nil {
			t.Errorf("DeferToThread = %v, %v", v, err)
		}
		if inLoop.Load() {
			t.Error("DeferToThread fn ran on the loop")
		}
		if on := <-thenOnLoop; !on {
			t.Error("DeferToThread Then ran off the loop")
		}
	})
	t.Run("error", func(t *testing.T) {
		f := DeferToThread(l, func() (int, error) { return 0, errBoom })
		if _, err := awaitT(t, f); err != errBoom {
			t.Errorf("err = %v, want errBoom", err)
		}
	})
	t.Run("panic", func(t *testing.T) {
		f := DeferToThread(l, func() (int, error) { panic(errBoom) })
		_, err := awaitT(t, f)
		var pe *PanicError
		if !errors.As(err, &pe) || !errors.Is(err, errBoom) {
			t.Errorf("err = %v, want *PanicError wrapping errBoom", err)
		}
	})
	t.Run("many", func(t *testing.T) {
		const n = 200
		fs := make([]*Future[int], n)
		onLoop(t, l, func() {
			for i := range fs {
				i := i
				fs[i] = DeferToThread(l, func() (int, error) { time.Sleep(time.Millisecond); return i, nil })
			}
		})
		v, err := awaitT(t, All(l, fs...))
		if err != nil || len(v) != n {
			t.Fatalf("All(DeferToThread...) = %d values, %v", len(v), err)
		}
		for i := range v {
			if v[i] != i {
				t.Fatalf("value %d = %d", i, v[i])
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Timers.
// ---------------------------------------------------------------------------

// stopTimer calls tm.Stop on the loop and reports the result or whether it
// panicked (Stop on a dead Timer panics in debug builds).
func stopTimer(tm *Timer) (ok, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	return tm.Stop(), false
}

func TestTimerZeroDelayRunsNextIteration(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	type res struct {
		reentrant      bool
		schedIt, ranIt uint64
	}
	ch := make(chan res, 1)
	l.CallFromThread(func() {
		var r res
		ran := false
		r.schedIt = l.metrics.iterations.Load()
		l.CallLater(0, func() {
			ran = true
			r.ranIt = l.metrics.iterations.Load()
			ch <- r
		})
		if ran {
			r.reentrant = true
		}
	})
	select {
	case r := <-ch:
		if r.reentrant {
			t.Error("CallLater(0) ran re-entrantly inside CallLater")
		}
		if r.ranIt <= r.schedIt {
			t.Errorf("CallLater(0) ran in iteration %d, scheduled in %d; want a later iteration", r.ranIt, r.schedIt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CallLater(0) never ran")
	}

	// A self-re-arming zero-delay timer does not starve other work.
	var stop atomic.Bool
	var spins atomic.Int64
	var rearm func()
	rearm = func() {
		spins.Add(1)
		if !stop.Load() {
			l.CallLater(0, rearm)
		}
	}
	l.CallFromThread(func() { l.CallLater(0, rearm) })
	other := make(chan struct{})
	l.CallFromThread(func() { close(other) })
	select {
	case <-other:
	case <-time.After(5 * time.Second):
		t.Error("a re-arming CallLater(0) starved CallFromThread")
	}
	stop.Store(true)
	onLoop(t, l, func() {})
}

func TestTimerStopAndReset(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	var fired atomic.Int32
	type stopRes struct{ first, second, secondPanicked bool }
	var sr, srDue stopRes
	onLoop(t, l, func() {
		tm := l.CallLater(time.Hour, func() { fired.Add(1) })
		sr.first, _ = stopTimer(tm)
		sr.second, sr.secondPanicked = stopTimer(tm)
		tz := l.CallLater(0, func() { fired.Add(1) })
		srDue.first, _ = stopTimer(tz)
		srDue.second, srDue.secondPanicked = stopTimer(tz)
	})
	if !sr.first || sr.second {
		t.Errorf("Stop on pending timer: first=%v second=%v; want true then false", sr.first, sr.second)
	}
	if !srDue.first || srDue.second {
		t.Errorf("Stop on CallLater(0) timer: first=%v second=%v; want true then false", srDue.first, srDue.second)
	}
	if sr.secondPanicked || srDue.secondPanicked {
		t.Errorf("second Stop panicked (%v, %v); Stop on a stopped Timer is a no-op", sr.secondPanicked, srDue.secondPanicked)
	}
	time.Sleep(20 * time.Millisecond)
	if n := fired.Load(); n != 0 {
		t.Errorf("stopped timers fired %d times", n)
	}

	// Reset earlier.
	firedAt := make(chan time.Duration, 2)
	onLoop(t, l, func() {
		start := time.Now()
		tm := l.CallLater(time.Hour, func() { firedAt <- time.Since(start) })
		tm.Reset(20 * time.Millisecond)
	})
	select {
	case d := <-firedAt:
		if d < 18*time.Millisecond || d > time.Second {
			t.Errorf("Reset(20ms) timer fired after %v", d)
		}
	case <-time.After(5 * time.Second):
		t.Error("timer Reset from 1h to 20ms never fired")
	}

	// Reset later.
	onLoop(t, l, func() {
		start := time.Now()
		tm := l.CallLater(20*time.Millisecond, func() { firedAt <- time.Since(start) })
		tm.Reset(300 * time.Millisecond)
	})
	select {
	case d := <-firedAt:
		if d < 290*time.Millisecond {
			t.Errorf("timer Reset to 300ms fired after %v", d)
		}
	case <-time.After(5 * time.Second):
		t.Error("timer Reset to 300ms never fired")
	}

	// Stop after fire returns false, and a stale handle (fired or stopped)
	// never affects a timer created later.
	done := make(chan struct{})
	var tm, stopped *Timer
	onLoop(t, l, func() { tm = l.CallLater(time.Millisecond, func() { close(done) }) })
	<-done
	var afterFire, afterFirePanicked bool
	later := make(chan struct{})
	onLoop(t, l, func() {
		stopped = l.CallLater(time.Hour, func() {})
		stopped.Stop()
		l.CallLater(30*time.Millisecond, func() { close(later) })
		afterFire, afterFirePanicked = stopTimer(tm)
		tm.Reset(time.Hour)
		stopped.Stop()
		stopped.Reset(time.Hour)
	})
	if afterFire || afterFirePanicked {
		t.Errorf("Stop on a fired timer = %v (panicked %v), want false", afterFire, afterFirePanicked)
	}
	select {
	case <-later:
	case <-time.After(5 * time.Second):
		t.Error("Stop/Reset on stale Timer handles cancelled or moved a later timer")
	}
}
