package twine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// enableDebug turns on the debug checks for one test. It must be called
// before the engine starts so that the restore (a Cleanup registered first,
// hence run last) happens after the loops have exited.
func enableDebug(t *testing.T) {
	old := debugEnabled
	debugEnabled = true
	t.Cleanup(func() { debugEnabled = old })
}

func TestDebugWriteOffLoopPanics(t *testing.T) {
	enableDebug(t)
	e := startEngine(t, Loops(1))
	trCh := make(chan Transport, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) { trCh <- tr }}
	}))
	dialTCP(t, lst.Addr().String())
	tr := <-trCh
	for name, call := range map[string]func(){
		"Write":          func() { tr.Write([]byte("x")) },
		"Writev":         func() { tr.Writev([]byte("x")) },
		"LoseConnection": tr.LoseConnection,
		"PauseReading":   tr.PauseReading,
		"SetIdleTimeout": func() { tr.SetIdleTimeout(time.Second) },
	} {
		rec := func() (r any) {
			defer func() { r = recover() }()
			call()
			return nil
		}()
		err, _ := rec.(error)
		var pe *PanicError
		if !errors.As(err, &pe) || !errors.Is(err, ErrNotInLoop) {
			t.Errorf("%s off the loop: recovered %v (%T), want *PanicError wrapping ErrNotInLoop", name, rec, rec)
			continue
		}
		if !strings.Contains(string(pe.Stack), "TestDebugWriteOffLoopPanics") {
			t.Errorf("%s: PanicError.Stack does not show the caller", name)
		}
	}
	// On the loop the same calls are fine.
	onLoop(t, tr.Loop(), func() { tr.Write([]byte("ok")) })
}

func TestDebugReadBufferPoisoned(t *testing.T) {
	enableDebug(t)
	e := startEngine(t, Loops(1))
	type res struct {
		during   string
		after    []byte
		inLoopOK bool
	}
	resCh := make(chan res, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		var retained []byte
		return &hookProto{data: func(p *hookProto, b []byte) {
			if retained != nil {
				return
			}
			retained = b // the bug under test: keeping the loop buffer
			during := string(b)
			l := p.t.Loop()
			// Runs at the start of the next iteration, before any read.
			l.CallFromThread(func() {
				resCh <- res{during: during, after: append([]byte(nil), retained...), inLoopOK: l.InLoop()}
			})
		}}
	}))
	c := dialTCP(t, lst.Addr().String())
	if _, err := c.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-resCh:
		if r.during != "hello world" {
			t.Errorf("DataReceived saw %q", r.during)
		}
		for i, b := range r.after {
			if b != 0xDE {
				t.Errorf("retained slice byte %d = %#x after DataReceived returned, want poison 0xDE (%q)", i, b, r.after)
				break
			}
		}
		if !r.inLoopOK {
			t.Error("CallFromThread task ran off the loop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DataReceived never ran")
	}
}

// Timers are not pooled, so a handle kept after its timer fired is harmless:
// in debug mode Stop/Reset on it are no-ops (no panic) and can never touch a
// later timer. Calling a live Timer off its loop still panics.
func TestDebugStaleTimerHandle(t *testing.T) {
	enableDebug(t)
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	fired := make(chan struct{})
	var tm *Timer
	onLoop(t, l, func() { tm = l.CallLater(time.Millisecond, func() { close(fired) }) })
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("timer never fired")
	}
	later := make(chan struct{})
	var rec any
	var ret bool
	onLoop(t, l, func() {
		l.CallLater(30*time.Millisecond, func() { close(later) })
		defer func() { rec = recover() }()
		ret = tm.Stop()
		tm.Reset(time.Hour)
	})
	if rec != nil {
		t.Errorf("Stop/Reset on a fired timer panicked in debug mode: %v", rec)
	}
	if ret {
		t.Error("Stop on a fired timer returned true")
	}
	select {
	case <-later:
	case <-time.After(5 * time.Second):
		t.Error("a stale Timer handle cancelled or moved a later timer")
	}
	// Off the loop, a live timer's Stop panics with ErrNotInLoop.
	onLoop(t, l, func() { tm = l.CallLater(time.Hour, func() {}) })
	func() {
		defer func() {
			r := recover()
			if err, _ := r.(error); !errors.Is(err, ErrNotInLoop) {
				t.Errorf("Timer.Stop off the loop recovered %v, want ErrNotInLoop", r)
			}
		}()
		tm.Stop()
	}()
	onLoop(t, l, func() { tm.Stop() })
}

// captureHandler is a slog.Handler that records every record.
type captureHandler struct {
	mu   sync.Mutex
	recs []capturedRecord
}

type capturedRecord struct {
	msg   string
	level slog.Level
	attrs map[string]string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	cr := capturedRecord{msg: r.Message, level: r.Level, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		cr.attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.recs = append(h.recs, cr)
	h.mu.Unlock()
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) matching(substr string) []capturedRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []capturedRecord
	for _, r := range h.recs {
		if strings.Contains(r.msg, substr) {
			out = append(out, r)
		}
	}
	return out
}

type sleepyProto struct {
	t     Transport
	sleep time.Duration
}

func (p *sleepyProto) ConnectionMade(t Transport) { p.t = t }
func (p *sleepyProto) DataReceived(b []byte)      { time.Sleep(p.sleep); p.t.Write(b) }
func (p *sleepyProto) ConnectionLost(error)       {}

func TestWatchdogSlowCallback(t *testing.T) {
	h := &captureHandler{}
	e := startEngine(t, Loops(1), Watchdog(time.Millisecond), Logger(slog.New(h)))
	lst := listen(t, e, FactoryFunc(func(net.Addr) Protocol { return &sleepyProto{sleep: 5 * time.Millisecond} }))
	c := dialTCP(t, lst.Addr().String())

	// One slow callback produces a warning naming the protocol type.
	echoOnce(t, c, []byte("slow"))
	if !waitUntil(2*time.Second, func() bool { return len(h.matching("slow callback")) > 0 }) {
		t.Fatal("no \"slow callback\" warning after a 5ms DataReceived with Watchdog(1ms)")
	}
	recs := h.matching("slow callback")
	r := recs[0]
	if r.level != slog.LevelWarn {
		t.Errorf("slow callback logged at %v, want WARN", r.level)
	}
	wantType := fmt.Sprintf("%T", &sleepyProto{})
	if r.attrs["protocol"] != wantType {
		t.Errorf("warning protocol attr = %q, want %q (attrs %v)", r.attrs["protocol"], wantType, r.attrs)
	}
	if r.attrs["loop"] != "0" {
		t.Errorf("warning loop attr = %q, want 0", r.attrs["loop"])
	}
	if e.Loops()[0].Metrics().SlowCallbacks == 0 {
		t.Error("SlowCallbacks metric is 0")
	}

	// Rate limit: 50 more slow callbacks within a second log at most 2 lines.
	time.Sleep(1100 * time.Millisecond) // start a fresh rate-limit window
	before := len(h.matching("slow callback"))
	start := time.Now()
	for i := 0; i < 50; i++ {
		echoOnce(t, c, []byte("x"))
	}
	el := time.Since(start)
	time.Sleep(10 * time.Millisecond)
	got := len(h.matching("slow callback")) - before
	t.Logf("50 slow callbacks in %v produced %d warnings", el, got)
	if el < time.Second && got > 2 {
		t.Errorf("%d slow-callback warnings for 50 slow callbacks in %v; rate limit is 1/s per loop", got, el)
	}
	if got < 1 {
		t.Error("no warning for 50 slow callbacks in a fresh window")
	}
}

// An idle engine's watchdog backs off from one sample per millisecond to one
// per watchdogIdleInterval, and returns to the short interval under load.
func TestWatchdogIdleBackoff(t *testing.T) {
	e := startEngine(t, Loops(2), Watchdog(time.Millisecond))
	time.Sleep(200 * time.Millisecond) // reach the idle interval
	s0 := e.wdSamples.Load()
	time.Sleep(500 * time.Millisecond)
	idle := e.wdSamples.Load() - s0
	t.Logf("%d watchdog samples in 500ms idle", idle)
	if idle > 40 {
		t.Errorf("%d watchdog samples in 500ms on an idle engine; want about %d", idle, 500*time.Millisecond/watchdogIdleInterval)
	}

	// Under load (a timer keeping a loop iterating) it samples every 1 ms.
	l := e.loops[0]
	stop := make(chan struct{})
	var tick func()
	tick = func() {
		select {
		case <-stop:
		default:
			l.CallLater(time.Millisecond, tick)
		}
	}
	l.CallFromThread(tick)
	time.Sleep(100 * time.Millisecond)
	s1 := e.wdSamples.Load()
	time.Sleep(200 * time.Millisecond)
	busy := e.wdSamples.Load() - s1
	close(stop)
	t.Logf("%d watchdog samples in 200ms busy", busy)
	if busy < 50 {
		t.Errorf("%d watchdog samples in 200ms on a busy loop; want about 200", busy)
	}
}

// Writes from a debug-mode echo still work end to end (assertions do not
// fire on the loop).
func TestDebugModeEcho(t *testing.T) {
	enableDebug(t)
	e := startEngine(t, Loops(2))
	lst := listen(t, e, echoFactory())
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", lst.Addr().String())
			if err != nil {
				bad.Add(1)
				return
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			msg := []byte("debug echo payload")
			got := make([]byte, len(msg))
			for j := 0; j < 50; j++ {
				if _, err := c.Write(msg); err != nil {
					bad.Add(1)
					return
				}
				if _, err := io.ReadFull(c, got); err != nil || string(got) != string(msg) {
					bad.Add(1)
					return
				}
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Errorf("%d debug-mode echo clients failed", bad.Load())
	}
	if p := sumMetrics(e).Panics; p != 0 {
		t.Errorf("Panics = %d in debug-mode echo", p)
	}
}
