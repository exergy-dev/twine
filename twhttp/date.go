package twhttp

import (
	"net/http"
	"time"

	"github.com/exergy-dev/twine"
)

// dateState is the per-loop cached "Date: ...\r\n" header line. It is
// touched only on its loop; Server.mu guards only the map that finds it,
// and that map is used at connection setup, not per request.
type dateState struct {
	s      *Server
	loop   *twine.Loop
	line   []byte // "Date: <IMF-fixdate>\r\n"
	conns  int
	armed  bool
	tickFn func()
}

// dateFor returns the date cache for l, creating it and arming its
// once-per-second refresh timer on first use. Called from ConnectionMade on
// l.
//
// The first cache created for an engine registers an OnShutdown hook that
// drops all of that engine's caches, whose timers stop with its loops.
// Registering here rather than in Listen covers servers attached with
// Factory as well. A hook registered after shutdown has begun never runs;
// that needs a connection made during shutdown on a loop with no cache,
// and costs one stale map entry.
//
// This differs from the twtls package's per-loop scratch buffer, which keys a
// package-level map by weak.Pointer and lets a runtime.AddCleanup drop the
// entry once the loop is collected. That works there because the buffer is
// passive. The date cache owns a timer on its loop and calls loop.Now and
// loop.CallLater from the timer callback, so it must hold the loop: a
// weak-keyed map whose value refers to the key never sees the key die, and
// a stopped engine's caches would stay until its loops were collected
// rather than being dropped at Stop (TestDateCacheCleanup checks the
// latter). The explicit hook is the simpler contract for a stateful,
// per-Server cache.
func (s *Server) dateFor(l *twine.Loop) *dateState {
	s.mu.Lock()
	d := s.dates[l]
	if d == nil {
		d = &dateState{s: s, loop: l}
		d.tickFn = d.tick
		if s.dates == nil {
			s.dates = make(map[*twine.Loop]*dateState)
			s.engines = make(map[*twine.Engine]struct{})
		}
		s.dates[l] = d
		e := l.Engine()
		if _, hooked := s.engines[e]; !hooked {
			s.engines[e] = struct{}{}
			e.OnShutdown(func() { s.forgetEngine(e) })
		}
	}
	s.mu.Unlock()
	d.conns++
	if !d.armed {
		d.refresh(l.Now())
		d.arm()
	}
	return d
}

// forgetEngine drops the caches of an engine that is shutting down; their
// timers will never fire again.
func (s *Server) forgetEngine(e *twine.Engine) {
	s.mu.Lock()
	for _, l := range e.Loops() {
		delete(s.dates, l)
	}
	delete(s.engines, e)
	s.mu.Unlock()
}

// release is called from ConnectionLost.
func (d *dateState) release() { d.conns-- }

// arm schedules the next refresh just after the next second boundary.
func (d *dateState) arm() {
	now := d.loop.Now()
	delay := time.Second - time.Duration(now.Nanosecond()) + time.Millisecond
	d.armed = true
	d.loop.CallLater(delay, d.tickFn)
}

// tick is the timer callback: regenerate the line and re-arm, or retire
// the cache when the loop has no HTTP connections left.
func (d *dateState) tick() {
	d.armed = false
	if d.conns <= 0 {
		d.s.mu.Lock()
		if d.s.dates[d.loop] == d {
			delete(d.s.dates, d.loop)
		}
		d.s.mu.Unlock()
		return
	}
	d.refresh(d.loop.Now())
	d.arm()
}

// refresh formats now into the cached line, reusing its storage.
func (d *dateState) refresh(now time.Time) {
	d.line = append(d.line[:0], "Date: "...)
	d.line = now.UTC().AppendFormat(d.line, http.TimeFormat)
	d.line = append(d.line, '\r', '\n')
}
