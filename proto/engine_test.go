package proto

import (
	"context"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// startEngine runs an engine with n loops and stops it when the test ends.
// It returns once every loop is running.
func startEngine(t testing.TB, n int) *twine.Engine {
	t.Helper()
	e := twine.New(twine.Loops(n), twine.Watchdog(-1), twine.ShutdownTimeout(2*time.Second))
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	for _, l := range e.Loops() {
		// Tasks queued before start run on the first iteration, so this is
		// the readiness barrier.
		onLoop(t, l, func() {})
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

// onLoop runs fn on l and waits for it.
func onLoop(t testing.TB, l *twine.Loop, fn func()) {
	t.Helper()
	done := make(chan struct{})
	l.CallFromThread(func() { defer close(done); fn() })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("loop did not run task")
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
