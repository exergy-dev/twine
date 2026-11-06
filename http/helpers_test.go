package http

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/robert-malhotra/twine"
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
	ctx     any
}

var fakePeer = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}

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
func (f *fakeTransport) GetHost() net.Addr                     { return fakePeer }
func (f *fakeTransport) Loop() *twine.Loop                     { return f.loop }
func (f *fakeTransport) SetIdleTimeout(time.Duration)          {}
func (f *fakeTransport) SetContext(v any)                      { f.ctx = v }
func (f *fakeTransport) Context() any                          { return f.ctx }
func (f *fakeTransport) PauseReading()                         { f.paused = true }
func (f *fakeTransport) ResumeReading()                        { f.paused = false }
func (f *fakeTransport) RegisterProducer(twine.Producer, bool) {}
func (f *fakeTransport) UnregisterProducer()                   {}
func (f *fakeTransport) PendingWrite() int                     { return 0 }
func (f *fakeTransport) reset()                                { f.out = f.out[:0]; f.writes = 0 }
func (f *fakeTransport) String() string                        { return string(f.out) }

// newFakeConn builds a server connection attached to ft.
func newFakeConn(s *Server, ft *fakeTransport) *conn {
	c := s.Factory().BuildProtocol(fakePeer).(*conn)
	c.ConnectionMade(ft)
	return c
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
