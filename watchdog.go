package twine

import (
	"bytes"
	"runtime"
	"strconv"
	"time"
)

// watchdogIdleInterval is the longest sampling interval the watchdog backs
// off to while every loop is idle.
const watchdogIdleInterval = 50 * time.Millisecond

// watchdog samples every loop's "current callback started at" timestamp and
// logs loops whose callback has run longer than the threshold. The log line
// carries the loop id, the protocol type and the loop goroutine's stack, and
// is rate-limited to once per second per loop. The stack comes from
// runtime.Stack(all) filtered to the loop goroutine, which is portable and
// needs no signal handler (a signal to the locked thread was the
// alternative).
//
// While any loop is busy the watchdog samples every threshold/2 (at least
// 1 ms). A loop is idle when no callback is running and it has finished no
// iteration since the previous sample (it is blocked in poll); while every
// loop is idle the interval doubles up to watchdogIdleInterval, so an idle
// engine costs about 20 wakeups a second instead of 1000. The first sample
// that finds a loop busy drops back to the short interval.
//
// Sampling alone would miss a callback that starts and ends between two
// samples, so each loop also measures, from clock reads it already makes,
// how long it spent between one poll returning and the next poll starting.
// A span at or above the threshold is reported at the next sample whatever
// the interval, with the protocol when exactly one callback ran in it; a
// callback that is still running is reported once it has run for threshold
// plus the current interval, with the loop goroutine's stack.
func (e *Engine) watchdog() {
	threshold := e.cfg.watchdog
	base := threshold / 2
	if base < time.Millisecond {
		base = time.Millisecond
	}
	interval := base
	timer := time.NewTimer(interval)
	defer timer.Stop()
	lastReport := make([]time.Time, len(e.loops))
	lastIter := make([]uint64, len(e.loops))
	for {
		select {
		case <-e.doneCh:
			return
		case <-timer.C:
		}
		e.wdSamples.Add(1)
		idle := true
		for i, l := range e.loops {
			iter := l.metrics.iterations.Load()
			start := l.cbStart.Load()
			if iter != lastIter[i] || start != 0 {
				idle = false
			}
			lastIter[i] = iter
			now := time.Now()
			if d := l.slowNs.Load(); d > 0 {
				// A dispatch that already finished, measured by the loop. While
				// the per-loop rate limit is active the value stays put (the
				// loop keeps the maximum), so it is reported at a later sample
				// rather than lost.
				idle = false
				if now.Sub(lastReport[i]) >= time.Second && l.slowNs.CompareAndSwap(d, 0) {
					lastReport[i] = now
					l.metrics.slowCallbacks.Add(1)
					proto := "<task>"
					if l.slowMulti.Load() {
						proto = "<multiple>"
					} else if c := l.slowProto.Load(); c != nil {
						proto = protoNameOf(c)
					}
					e.cfg.logger.Warn("twine: slow callback",
						"loop", l.id, "elapsed", time.Duration(d), "protocol", proto, "completed", true)
				}
			}
			if start == 0 {
				continue
			}
			elapsed := time.Since(l.epoch) - time.Duration(start)
			if elapsed < threshold {
				continue
			}
			if now.Sub(lastReport[i]) < time.Second {
				continue
			}
			lastReport[i] = now
			l.metrics.slowCallbacks.Add(1)
			proto := "<none>"
			if c := l.cbProto.Load(); c != nil {
				proto = protoNameOf(c)
			}
			e.cfg.logger.Warn("twine: slow callback",
				"loop", l.id, "elapsed", elapsed, "protocol", proto,
				"stack", goroutineStack(l.gid.Load()))
		}
		if idle {
			interval = min(2*interval, max(watchdogIdleInterval, base))
		} else {
			interval = base
		}
		timer.Reset(interval)
	}
}

// protoNameOf is the protocol type name recorded on c, or "<none>".
func protoNameOf(c *conn) string {
	if n := c.protoName.Load(); n != nil {
		return *n
	}
	return "<none>"
}

// goroutineStack returns the stack of goroutine gid from a full dump.
func goroutineStack(gid int64) string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	buf = buf[:n]
	prefix := []byte("goroutine " + strconv.FormatInt(gid, 10) + " ")
	for _, blk := range bytes.Split(buf, []byte("\n\n")) {
		if bytes.HasPrefix(blk, prefix) {
			return string(blk)
		}
	}
	return "<loop goroutine not found>"
}
