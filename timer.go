package twine

import (
	"time"
	"unsafe"

	"github.com/exergy-dev/twine/internal/wheel"
)

// Timer is the handle returned by Loop.CallLater. It is loop-affine. Handles
// are carved from a per-loop slab (one allocation per 256 timers) and never
// reused, so a handle kept after the timer fired can never cancel or move a
// later timer. Stop and Reset on a fired or stopped Timer are harmless
// no-ops; unlike time.Timer.Reset, Reset does not re-arm a timer that has
// already fired or been stopped (call CallLater again for that).
type Timer struct {
	w    wheel.Timer // must stay the first field: fireNode converts back from it
	loop *Loop
	fn   func()
	due  bool // queued in Loop.due (zero delay) rather than the wheel
}

// Compile-time checks for fireNode, which converts a *wheel.Timer back to
// its *Timer: valid only while w is Timer's first field. A non-zero offset
// makes the first constant negative, which overflows uintptr.
const (
	_ = -unsafe.Offsetof(Timer{}.w)
	_ = unsafe.Sizeof(Timer{}) - unsafe.Sizeof(wheel.Timer{})
)

// fireNode is the wheel callback (wheel.Timer.FnT); storing a package-level
// function instead of a method value keeps CallLater at one allocation.
func fireNode(w *wheel.Timer) { (*Timer)(unsafe.Pointer(w)).fire() }

// fire runs the user callback once.
func (t *Timer) fire() {
	l := t.loop
	fn := t.fn
	t.fn = nil
	if fn == nil {
		return
	}
	l.cbStart.Store(l.nowNs)
	defer l.endCallback()
	defer l.recoverTimer()
	fn()
}

func (l *Loop) recoverTimer() {
	if r := recover(); r != nil {
		l.logPanic("twine: panic in timer callback", capturePanic(r))
	}
}

// Stop cancels the timer. It returns true if the timer was pending and is now
// cancelled, false if it had already fired or been stopped.
func (t *Timer) Stop() bool {
	l := t.loop
	l.assertInLoop()
	if t.due {
		t.due = false
		t.fn = nil
		return true
	}
	if !l.wheel.Remove(&t.w) {
		return false
	}
	t.fn = nil
	return true
}

// Reset reschedules a pending timer to fire after d from now. Calling Reset
// on a fired or stopped timer is a no-op.
func (t *Timer) Reset(d time.Duration) {
	l := t.loop
	l.assertInLoop()
	if t.due {
		if d <= 0 {
			return
		}
		t.due = false // fireDue skips it; the wheel owns it now
		l.wheel.Add(&t.w, l.nowNs+int64(d))
		return
	}
	if !t.w.Scheduled() {
		return
	}
	if d <= 0 {
		l.wheel.Remove(&t.w)
		t.due = true
		l.due = append(l.due, t)
		return
	}
	l.wheel.Add(&t.w, l.nowNs+int64(d))
}

// Active reports whether the timer is still pending.
func (t *Timer) Active() bool { return t.due || t.w.Scheduled() }
