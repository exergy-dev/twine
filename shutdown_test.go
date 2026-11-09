package twine

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runningEngine is an Engine whose Run result the test asserts on.
type runningEngine struct {
	e     *Engine
	errCh chan error
	runG  atomic.Int64 // goroutine id of the Run goroutine
}

func runEngineCtx(t *testing.T, ctx context.Context, opts ...Option) *runningEngine {
	t.Helper()
	opts = append([]Option{Watchdog(0)}, opts...)
	re := &runningEngine{e: New(opts...), errCh: make(chan error, 1)}
	go func() {
		re.runG.Store(goid())
		re.errCh <- re.e.Run(ctx)
	}()
	if !waitUntil(5*time.Second, func() bool {
		for _, l := range re.e.Loops() {
			if !l.started.Load() {
				return false
			}
		}
		return true
	}) {
		t.Fatal("loops did not start")
	}
	t.Cleanup(func() {
		re.e.Stop()
		select {
		case <-re.e.Done():
		case <-time.After(15 * time.Second):
			t.Error("engine did not stop at cleanup")
		}
	})
	return re
}

func runEngine(t *testing.T, opts ...Option) *runningEngine {
	return runEngineCtx(t, context.Background(), opts...)
}

// waitRun waits for Run to return and reports its error and how long it took.
func (re *runningEngine) waitRun(t *testing.T, timeout time.Duration) (time.Duration, bool, error) {
	t.Helper()
	start := time.Now()
	select {
	case err := <-re.errCh:
		return time.Since(start), true, err
	case <-time.After(timeout):
		t.Errorf("Run did not return within %v", timeout)
		return timeout, false, nil
	}
}

func TestShutdownFlushesPendingWrites(t *testing.T) {
	const size = 4 << 20
	re := runEngine(t, Loops(2))
	var lostCount atomic.Int32
	lostCh := make(chan error, 1)
	trCh := make(chan Transport, 1)
	lst := listen(t, re.e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				buf := make([]byte, size)
				fillPattern(buf, 0)
				tr.Write(buf)
				trCh <- tr
			},
			lost: func(_ *hookProto, err error) { lostCount.Add(1); lostCh <- err },
		}
	}), SocketBuffers(0, 32<<10))
	addr := lst.Addr().String()
	c := dialTCP(t, addr) // slow client: does not read until Stop
	tr := <-trCh
	var pending int
	onLoop(t, tr.Loop(), func() { pending = tr.PendingWrite() })
	if pending == 0 {
		t.Fatal("nothing pending before Stop; the test needs queued writes")
	}
	t.Logf("%d bytes pending at Stop", pending)
	re.e.Stop()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, bad, err := readAllPattern(c)
	if err != nil {
		t.Errorf("client read: %v", err)
	}
	if n != size || bad >= 0 {
		t.Errorf("client received %d of %d bytes (first bad offset %d) across graceful shutdown", n, size, bad)
	}
	c.Close()
	if _, ok, err := re.waitRun(t, 10*time.Second); ok && err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if err, ok := recvErr(t, lostCh, time.Second, "ConnectionLost"); ok && err != nil {
		t.Errorf("ConnectionLost(%v), want nil after a graceful flush", err)
	}
	if n := lostCount.Load(); n != 1 {
		t.Errorf("ConnectionLost called %d times, want 1", n)
	}
	// Listeners are closed.
	if c2, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c2.Close()
		t.Error("listener still accepting after Run returned")
	}
}

func TestShutdownConnectionLostOncePerConn(t *testing.T) {
	const conns = 100
	re := runEngine(t, Loops(4))
	var made atomic.Int32
	var mu sync.Mutex
	lostCounts := map[*hookProto]int{}
	var lostErrs []error
	lst := listen(t, re.e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				made.Add(1)
				buf := make([]byte, 128<<10)
				fillPattern(buf, 0)
				tr.Write(buf)
			},
			lost: func(p *hookProto, err error) {
				mu.Lock()
				lostCounts[p]++
				if err != nil {
					lostErrs = append(lostErrs, err)
				}
				mu.Unlock()
			},
		}
	}))
	var wg sync.WaitGroup
	var short atomic.Int32
	for i := 0; i < conns; i++ {
		c := dialTCP(t, lst.Addr().String())
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			_ = c.SetReadDeadline(time.Now().Add(20 * time.Second))
			n, bad, err := readAllPattern(c) // reads to EOF, then closes
			if err != nil || n != 128<<10 || bad >= 0 {
				short.Add(1)
			}
		}()
	}
	if !waitUntil(5*time.Second, func() bool { return made.Load() == conns }) {
		t.Fatalf("only %d of %d connections made", made.Load(), conns)
	}
	re.e.Stop()
	wg.Wait()
	if _, ok, err := re.waitRun(t, 10*time.Second); ok && err != nil {
		t.Errorf("Run returned %v", err)
	}
	if s := short.Load(); s != 0 {
		t.Errorf("%d clients did not receive their full 128 KiB before EOF", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lostCounts) != conns {
		t.Errorf("ConnectionLost delivered to %d of %d protocols", len(lostCounts), conns)
	}
	for _, n := range lostCounts {
		if n != 1 {
			t.Errorf("a protocol got ConnectionLost %d times", n)
			break
		}
	}
	if len(lostErrs) > 0 {
		t.Errorf("%d graceful closes reported errors, first: %v", len(lostErrs), lostErrs[0])
	}
	m := sumMetrics(re.e)
	if m.Accepts != conns || m.Closes != conns || m.Connections != 0 {
		t.Errorf("metrics after shutdown: Accepts=%d Closes=%d Connections=%d, want %d/%d/0", m.Accepts, m.Closes, m.Connections, conns, conns)
	}
}

func TestOnShutdownHooks(t *testing.T) {
	re := runEngine(t, Loops(2))
	var lostCalled atomic.Bool
	lst := listen(t, re.e, hookFactory(func() *hookProto {
		return &hookProto{lost: func(*hookProto, error) { lostCalled.Store(true) }}
	}))
	c := dialTCP(t, lst.Addr().String())
	if !waitUntil(5*time.Second, func() bool { return sumMetrics(re.e).Connections == 1 }) {
		t.Fatal("connection not accepted")
	}
	var mu sync.Mutex
	var order []int
	var gids []int64
	var lostBeforeHook atomic.Bool
	for i := 1; i <= 3; i++ {
		re.e.OnShutdown(func() {
			if lostCalled.Load() {
				lostBeforeHook.Store(true)
			}
			mu.Lock()
			order = append(order, i)
			gids = append(gids, goid())
			mu.Unlock()
		})
	}
	re.e.Stop()
	re.e.Stop() // idempotent
	// The client closes so the graceful close completes promptly.
	go func() {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, _ = readAllPattern(c)
		c.Close()
	}()
	if _, ok, err := re.waitRun(t, 10*time.Second); ok && err != nil {
		t.Errorf("Run returned %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("hooks ran %v, want [1 2 3] exactly once each", order)
	}
	for _, g := range gids {
		if g != re.runG.Load() {
			t.Errorf("hook ran on goroutine %d, Run goroutine is %d", g, re.runG.Load())
			break
		}
	}
	if lostBeforeHook.Load() {
		t.Error("a connection was closed before the OnShutdown hooks ran")
	}
	if !lostCalled.Load() {
		t.Error("connection not closed by shutdown")
	}
}

func TestStopBeforeRun(t *testing.T) {
	e := New(Loops(1), Watchdog(0))
	e.Stop()
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run after Stop-before-Run returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after Stop-before-Run")
	}
	select {
	case <-e.Done():
	default:
		t.Error("Done() not closed")
	}
}

func TestRunAfterStop(t *testing.T) {
	re := runEngine(t, Loops(1))
	re.e.Stop()
	if _, ok, err := re.waitRun(t, 10*time.Second); ok && err != nil {
		t.Errorf("first Run returned %v, want nil", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- re.e.Run(context.Background()) }()
	select {
	case err := <-errCh:
		t.Logf("Run after a completed Run returned %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("second Run did not return promptly")
	}
}

func TestContextCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	re := runEngineCtx(t, ctx, Loops(2))
	lostCh := make(chan error, 1)
	lst := listen(t, re.e, hookFactory(func() *hookProto {
		return &hookProto{lost: func(_ *hookProto, err error) { lostCh <- err }}
	}))
	c := dialTCP(t, lst.Addr().String())
	if !waitUntil(5*time.Second, func() bool { return sumMetrics(re.e).Connections == 1 }) {
		t.Fatal("connection not accepted")
	}
	cancel()
	// Client sees EOF (graceful close), then closes its side.
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, _, err := readAllPattern(c); err != nil || n != 0 {
		t.Errorf("client read %d bytes, %v; want EOF", n, err)
	}
	c.Close()
	if _, ok, err := re.waitRun(t, 10*time.Second); ok && err != nil {
		t.Errorf("Run returned %v after ctx cancel, want nil", err)
	}
	if err, ok := recvErr(t, lostCh, time.Second, "ConnectionLost"); ok && err != nil {
		t.Errorf("ConnectionLost(%v), want nil", err)
	}
}

func TestShutdownTimeoutAbortsStuckConn(t *testing.T) {
	const timeout = 300 * time.Millisecond
	re := runEngine(t, Loops(1), ShutdownTimeout(timeout), WriteBufferLimit(64<<20))
	lostCh := make(chan error, 1)
	trCh := make(chan Transport, 1)
	lst := listen(t, re.e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				tr.Write(make([]byte, 16<<20))
				trCh <- tr
			},
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}), SocketBuffers(0, 16<<10))
	dialTCP(t, lst.Addr().String()) // never reads, never closes
	tr := <-trCh
	var pending int
	onLoop(t, tr.Loop(), func() { pending = tr.PendingWrite() })
	if pending == 0 {
		t.Fatal("nothing pending; the test needs a stuck flush")
	}
	start := time.Now()
	re.e.Stop()
	_, ok, err := re.waitRun(t, 10*time.Second)
	el := time.Since(start)
	if ok && err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	t.Logf("Run returned %v after Stop (ShutdownTimeout %v)", el, timeout)
	if el < timeout-20*time.Millisecond {
		t.Errorf("Run returned after %v, before ShutdownTimeout %v", el, timeout)
	}
	if el > timeout+2*time.Second {
		t.Errorf("Run returned after %v, ShutdownTimeout %v", el, timeout)
	}
	if lerr, ok := recvErr(t, lostCh, time.Second, "ConnectionLost"); ok && !errors.Is(lerr, ErrShutdown) {
		t.Errorf("ConnectionLost(%v), want ErrShutdown", lerr)
	}
}

func TestCallFromThreadAfterLoopExit(t *testing.T) {
	re := runEngine(t, Loops(1))
	l := re.e.Loops()[0]
	re.e.Stop()
	if _, ok, _ := re.waitRun(t, 10*time.Second); !ok {
		t.FailNow()
	}
	if !l.Stopped() {
		t.Fatal("Stopped() = false after Run returned")
	}
	before := l.Metrics().BacklogDrops
	var ran atomic.Bool
	l.CallFromThread(func() { ran.Store(true) })
	time.Sleep(50 * time.Millisecond)
	if ran.Load() {
		t.Error("CallFromThread fn ran after the loop exited")
	}
	if d := l.Metrics().BacklogDrops - before; d != 1 {
		t.Errorf("BacklogDrops increased by %d, want 1", d)
	}
}

// After a loop exits, its OS thread is unlocked and reused by other
// goroutines; InLoop must not report true for them.
func TestInLoopFalseAfterExit(t *testing.T) {
	re := runEngine(t, Loops(4))
	re.e.Stop()
	if _, ok, _ := re.waitRun(t, 10*time.Second); !ok {
		t.FailNow()
	}
	var falsePositives atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				for _, l := range re.e.Loops() {
					if l.InLoop() {
						falsePositives.Add(1)
					}
				}
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
	for _, l := range re.e.Loops() {
		if l.InLoop() {
			falsePositives.Add(1)
		}
	}
	if n := falsePositives.Load(); n > 0 {
		t.Errorf("InLoop() returned true %d times on non-loop goroutines after the loops exited", n)
	}
}
