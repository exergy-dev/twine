package twine

import (
	"time"

	"github.com/robert-malhotra/twine/internal/wheel"
)

// Timer is the handle returned by Loop.CallLater. It is loop-affine and
// single-use: once it has fired or Stop has returned true it is recycled and
// must not be touched again. Reset before firing moves the deadline.
type Timer struct {
	w    wheel.Timer
	loop *Loop
	fn   func()
	due  bool // queued in Loop.due (zero delay) rather than the wheel
}

// fire is the wheel callback.
func (t *Timer) fire() {
	l := t.loop
	fn := t.fn
	l.putTimer(t)
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
		pe := capturePanic(r)
		l.metrics.Panics.Add(1)
		l.cfg.logger.Error("twine: panic in timer callback", "loop", l.id, "panic", pe.Value, "stack", string(pe.Stack))
	}
}

// Stop cancels the timer. It returns true if the timer was pending and is now
// cancelled, false if it already fired or was stopped. After a true return
// the handle is dead.
func (t *Timer) Stop() bool {
	l := t.loop
	if l == nil {
		if debugEnabled {
			panic("twine: Stop on a dead Timer")
		}
		return false
	}
	l.assertInLoop()
	if t.due {
		t.due = false
		t.fn = nil
		return true // fireDue recycles it
	}
	if !l.wheel.Remove(&t.w) {
		return false
	}
	l.putTimer(t)
	return true
}

// Reset reschedules a pending timer to fire after d from now. Calling Reset
// on a fired or stopped timer is a no-op (panics in debug builds).
func (t *Timer) Reset(d time.Duration) {
	l := t.loop
	if l == nil {
		if debugEnabled {
			panic("twine: Reset on a dead Timer")
		}
		return
	}
	l.assertInLoop()
	if t.due {
		if d <= 0 {
			return
		}
		// Move from the due list onto the wheel; fireDue skips it because
		// it is now Scheduled.
		t.due = false
		l.wheel.Add(&t.w, l.nowNs+int64(d))
		return
	}
	if !t.w.Scheduled() {
		return
	}
	l.wheel.Add(&t.w, l.nowNs+int64(d))
}

// Active reports whether the timer is still pending.
func (t *Timer) Active() bool { return t.loop != nil && (t.due || t.w.Scheduled()) }
