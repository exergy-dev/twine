package twine

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"time"
)

// watchdog samples every loop's "current callback started at" timestamp and
// logs loops whose callback has run longer than the threshold. The log line
// carries the loop id, the protocol type and the loop goroutine's stack, and
// is rate-limited to once per second per loop.
//
// Design Section 11 suggests dumping the locked thread via a signal; we use
// runtime.Stack(all) filtered to the loop goroutine instead, which is
// portable and needs no signal handler.
func (e *Engine) watchdog() {
	threshold := e.cfg.watchdog
	interval := threshold / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	lastReport := make([]time.Time, len(e.loops))
	for {
		select {
		case <-e.doneCh:
			return
		case <-tick.C:
		}
		for i, l := range e.loops {
			start := l.cbStart.Load()
			if start == 0 {
				continue
			}
			elapsed := time.Since(l.epoch) - time.Duration(start)
			if elapsed < threshold {
				continue
			}
			now := time.Now()
			if now.Sub(lastReport[i]) < time.Second {
				continue
			}
			lastReport[i] = now
			l.metrics.SlowCallbacks.Add(1)
			proto := "<none>"
			if c := l.cbProto.Load(); c != nil && c.proto != nil {
				proto = fmt.Sprintf("%T", c.proto)
			}
			e.cfg.logger.Warn("twine: slow callback",
				"loop", l.id, "elapsed", elapsed, "protocol", proto,
				"stack", goroutineStack(l.gid.Load()))
		}
	}
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
