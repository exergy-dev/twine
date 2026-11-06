package twine

import (
	"time"

	"github.com/robert-malhotra/twine/internal/wheel"
)

// Timer is the handle returned by Loop.CallLater. It is loop-affine. Each
// CallLater allocates one Timer (the wheel node itself is intrusive and
// allocation-free); Timers are deliberately not pooled so that a handle kept
// after the timer fired can never cancel or move a later timer. Stop and
// Reset on a fired or stopped Timer are harmless no-ops.
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
		pe := capturePanic(r)
		l.metrics.Panics.Add(1)
		l.cfg.logger.Error("twine: panic in timer callback", "loop", l.id, "panic", pe.Value, "stack", string(pe.Stack))
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
