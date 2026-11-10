//go:build twine_debug

package wheel

import (
	"testing"
	"time"
)

func TestAdvanceReentryPanics(t *testing.T) {
	w := New(time.Millisecond, 0)
	var inner Timer
	inner.Fn = func() { w.Advance(10 * ms) }
	w.Add(&inner, ms)
	func() {
		defer func() {
			if got := recover(); got != "wheel: Advance called from inside a timer's Fn" {
				t.Fatalf("recover() = %v, want the re-entry panic", got)
			}
		}()
		w.Advance(2 * ms)
	}()
	// The panic unwound through Advance (as when the loop recovers a
	// panicking callback): the guard must be cleared, not stuck.
	fired := false
	var next Timer
	next.Fn = func() { fired = true }
	w.Add(&next, 3*ms)
	w.Advance(3 * ms)
	if !fired {
		t.Fatal("Advance after a recovered re-entry panic did not fire")
	}
}
