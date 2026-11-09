package http

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// fakeTransport is an in-memory twine.Transport. It records written bytes
// and close requests and lets tests drive a protocol without sockets.
type fakeTransport struct {
	loop    *twine.Loop
	out     []byte
	writes  int
	closing bool
	aborted bool
	paused  bool
	idle    time.Duration
	ctx     any
}

var (
	fakePeer = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}
	fakeHost = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 80}
)

func (f *fakeTransport) Write(b []byte) { f.out = append(f.out, b...); f.writes++ }
func (f *fakeTransport) Writev(bs ...[]byte) {
	for _, b := range bs {
		f.out = append(f.out, b...)
	}
	f.writes++
}
func (f *fakeTransport) LoseConnection()                       { f.closing = true }
func (f *fakeTransport) Abort()                                { f.aborted = true }
func (f *fakeTransport) GetPeer() net.Addr                     { return fakePeer }
func (f *fakeTransport) GetHost() net.Addr                     { return fakeHost }
func (f *fakeTransport) Loop() *twine.Loop                     { return f.loop }
func (f *fakeTransport) SetIdleTimeout(d time.Duration)        { f.idle = d }
func (f *fakeTransport) SetContext(v any)                      { f.ctx = v }
func (f *fakeTransport) Context() any                          { return f.ctx }
func (f *fakeTransport) PauseReading()                         { f.paused = true }
func (f *fakeTransport) ResumeReading()                        { f.paused = false }
func (f *fakeTransport) RegisterProducer(twine.Producer, bool) {}
func (f *fakeTransport) UnregisterProducer()                   {}
func (f *fakeTransport) PendingWrite() int                     { return 0 }
func (f *fakeTransport) reset()                                { f.out = f.out[:0]; f.writes = 0 }
func (f *fakeTransport) String() string                        { return string(f.out) }

// newFakeConn builds a server connection attached to ft. It must run on
// ft.loop.
func newFakeConn(s *Server, ft *fakeTransport) *conn {
	c := s.Factory().BuildProtocol(fakePeer).(*conn)
	c.ConnectionMade(ft)
	return c
}

// testLoop starts a one-loop engine for the duration of the test and
// returns its loop.
func testLoop(t testing.TB) *twine.Loop {
	t.Helper()
	return startEngine(t, twine.Loops(1), twine.Watchdog(0)).Loops()[0]
}

// harness drives one server connection over a fakeTransport on a real
// loop, so pipelining yields run through the loop's CallLater as they do
// in production.
type harness struct {
	t  testing.TB
	l  *twine.Loop
	ft *fakeTransport
	c  *conn
}

func newHarness(t testing.TB, l *twine.Loop, s *Server) *harness {
	t.Helper()
	h := &harness{t: t, l: l, ft: &fakeTransport{loop: l}}
	onLoop(t, l, func() { h.c = newFakeConn(s, h.ft) })
	return h
}

// send delivers each chunk with its own DataReceived and lets the
// connection settle after each one, as the core does: while a continuation
// is pending reading is paused, so the next read waits for it.
func (h *harness) send(chunks ...[]byte) {
	h.t.Helper()
	for _, ch := range chunks {
		onLoop(h.t, h.l, func() { h.c.DataReceived(ch) })
		h.settle()
	}
}

// sendString is send for string chunks.
func (h *harness) sendString(chunks ...string) {
	h.t.Helper()
	for _, s := range chunks {
		h.send([]byte(s))
	}
}

// settle waits until the connection has no continuation scheduled and
// reading is not paused, or the connection is closing.
func (h *harness) settle() {
	h.t.Helper()
	for i := 0; ; i++ {
		busy := false
		onLoop(h.t, h.l, func() { busy = !h.c.closed && (h.c.resumePending || h.ft.paused) })
		if !busy {
			return
		}
		if i == 100000 {
			h.t.Fatal("connection did not settle")
		}
	}
}

// out returns everything written so far.
func (h *harness) out() string {
	var s string
	onLoop(h.t, h.l, func() { s = h.ft.String() })
	return s
}

// closing reports whether the server asked to close the connection.
func (h *harness) closing() bool {
	var c bool
	onLoop(h.t, h.l, func() { c = h.ft.closing })
	return c
}

// lose ends the connection as the core would.
func (h *harness) lose() { onLoop(h.t, h.l, func() { h.c.ConnectionLost(nil) }) }

// serveOnce feeds raw through a fresh connection on l in one chunk and
// returns the output and whether the server closed.
func serveOnce(t testing.TB, l *twine.Loop, s *Server, raw string) (string, bool) {
	t.Helper()
	h := newHarness(t, l, s)
	h.sendString(raw)
	out, closing := h.out(), h.closing()
	h.lose()
	return out, closing
}

// startEngine runs an engine for the duration of the test.
func startEngine(t testing.TB, opts ...twine.Option) *twine.Engine {
	t.Helper()
	e := twine.New(opts...)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- e.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errc:
		case <-time.After(10 * time.Second):
			t.Error("engine did not stop")
		}
	})
	return e
}

// onLoop runs fn on l and waits for it.
func onLoop(t testing.TB, l *twine.Loop, fn func()) {
	t.Helper()
	done := make(chan struct{})
	l.CallFromThread(func() {
		defer close(done)
		fn()
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("loop callback timed out")
	}
}

// discardLogger swallows expected panic logs.
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
