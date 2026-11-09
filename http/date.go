package http

import (
	nethttp "net/http"
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
func (s *Server) dateFor(l *twine.Loop) *dateState {
	s.mu.Lock()
	d := s.dates[l]
	if d == nil {
		d = &dateState{s: s, loop: l}
		d.tickFn = d.tick
		if s.dates == nil {
			s.dates = make(map[*twine.Loop]*dateState)
		}
		s.dates[l] = d
	}
	s.mu.Unlock()
	d.conns++
	if !d.armed {
		d.refresh(l.Now())
		d.arm()
	}
	return d
}

// forgetLoops drops the caches of an engine that is shutting down; their
// timers will never fire again.
func (s *Server) forgetLoops(loops []*twine.Loop) {
	s.mu.Lock()
	for _, l := range loops {
		delete(s.dates, l)
	}
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
	d.line = now.UTC().AppendFormat(d.line, nethttp.TimeFormat)
	d.line = append(d.line, '\r', '\n')
}
