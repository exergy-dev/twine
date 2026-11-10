package twine

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// startEngine runs an engine with the given options and stops it at test end.
func startEngine(t testing.TB, opts ...Option) *Engine {
	t.Helper()
	opts = append([]Option{Watchdog(-1)}, opts...)
	e := New(opts...)
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	// Wait for loops to start.
	deadline := time.Now().Add(5 * time.Second)
	for _, l := range e.loops {
		for !l.started.Load() {
			if time.Now().After(deadline) {
				t.Fatal("loops did not start")
			}
			time.Sleep(time.Millisecond)
		}
	}
	t.Cleanup(func() {
		e.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("engine did not stop")
		}
	})
	return e
}

type echoProto struct{ t Transport }

func (p *echoProto) ConnectionMade(t Transport) { p.t = t }
func (p *echoProto) DataReceived(b []byte)      { p.t.Write(b) }
func (p *echoProto) ConnectionLost(error)       {}

func echoFactory() Factory {
	return func(net.Addr) Protocol { return &echoProto{} }
}

func TestRunTwice(t *testing.T) {
	e := New(Loops(1), Watchdog(-1))
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	for !e.loops[0].started.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := e.Run(context.Background()); !errors.Is(err, ErrRunTwice) {
		t.Errorf("Run while running = %v, want ErrRunTwice", err)
	}
	e.Stop()
	if err := <-errCh; err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := e.Run(context.Background()); !errors.Is(err, ErrRunTwice) {
		t.Errorf("Run after Run = %v, want ErrRunTwice", err)
	}

	// Stop before Run: Run returns nil at once.
	e2 := New(Loops(1), Watchdog(-1))
	e2.Stop()
	if err := e2.Run(context.Background()); err != nil {
		t.Errorf("Run after Stop = %v, want nil", err)
	}
}

// onLoop runs fn on l and waits for it: up to 10 seconds in a test, and up
// to 5 minutes in a benchmark (setup of a million timers plus a benchtime).
func onLoop(t testing.TB, l *Loop, fn func()) {
	t.Helper()
	timeout := 10 * time.Second
	if _, ok := t.(*testing.B); ok {
		timeout = 5 * time.Minute
	}
	done := make(chan struct{})
	l.CallFromThread(func() { defer close(done); fn() })
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("loop did not run task")
	}
}
