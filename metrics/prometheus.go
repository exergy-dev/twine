// Package metrics exposes a twine Engine's per-loop counters to monitoring
// systems. It lives outside the root package so that programs that do not
// serve metrics do not link net/http.
package metrics

import (
	"net/http"
	"sort"
	"strconv"

	"github.com/exergy-dev/twine"
)

// series is one exported metric: its name, help text, Prometheus type and
// how to read it from a snapshot.
type series struct {
	name, help, kind string
	get              func(*twine.MetricsSnapshot) float64
}

// allSeries is built once and sorted by name.
var allSeries = func() []series {
	s := []series{
		{"twine_connections", "Open connections", "gauge", func(s *twine.MetricsSnapshot) float64 { return float64(s.Connections) }},
		{"twine_accepts_total", "Accepted connections", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Accepts) }},
		{"twine_closes_total", "Closed connections", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Closes) }},
		{"twine_reads_total", "read(2) calls returning data", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Reads) }},
		{"twine_read_bytes_total", "Bytes read", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.ReadBytes) }},
		{"twine_write_bytes_total", "Bytes written", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.WriteBytes) }},
		{"twine_writev_total", "Flush writev calls", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Writev) }},
		{"twine_write_direct_hit_total", "Writes that bypassed the ring", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.WriteDirectHit) }},
		{"twine_write_after_close_total", "Writes on closed transports", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.WriteAfterClose) }},
		{"twine_timers_fired_total", "Timers fired", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.TimersFired) }},
		{"twine_tasks_run_total", "CallFromThread tasks run", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.TasksRun) }},
		{"twine_wakeups_total", "Loop wakeups issued", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Wakeups) }},
		{"twine_wakeups_coalesced_total", "Wakeups avoided by coalescing", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.WakeupsCoalesced) }},
		{"twine_iterations_total", "Loop iterations", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Iterations) }},
		{"twine_poll_block_seconds_total", "Time blocked in poll", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.PollBlockNanos) / 1e9 }},
		{"twine_panics_total", "Recovered panics", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Panics) }},
		{"twine_backlog_drops_total", "Tasks dropped after loop exit", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.BacklogDrops) }},
		{"twine_accept_errors_total", "accept(2) errors", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.AcceptErrors) }},
		{"twine_datagrams_total", "Datagrams received", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.Datagrams) }},
		{"twine_datagrams_sent_total", "Datagrams sent", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.DatagramsSent) }},
		{"twine_slow_callbacks_total", "Watchdog reports", "counter", func(s *twine.MetricsSnapshot) float64 { return float64(s.SlowCallbacks) }},
		{"twine_iteration_seconds_p50", "Iteration duration p50 over 10s", "gauge", func(s *twine.MetricsSnapshot) float64 { return s.IterationP50.Seconds() }},
		{"twine_iteration_seconds_p99", "Iteration duration p99 over 10s", "gauge", func(s *twine.MetricsSnapshot) float64 { return s.IterationP99.Seconds() }},
		{"twine_iteration_seconds_max", "Iteration duration max over 10s", "gauge", func(s *twine.MetricsSnapshot) float64 { return s.IterationMax.Seconds() }},
		{"twine_slab_in_use_bytes", "Slab bytes handed out", "gauge", func(s *twine.MetricsSnapshot) float64 { return float64(s.SlabInUse) }},
		{"twine_slab_free_bytes", "Slab bytes in free lists", "gauge", func(s *twine.MetricsSnapshot) float64 { return float64(s.SlabFree) }},
		{"twine_ring_high_water_bytes", "Largest pending write seen", "gauge", func(s *twine.MetricsSnapshot) float64 { return float64(s.RingHighWater) }},
	}
	sort.Slice(s, func(i, j int) bool { return s[i].name < s[j].name })
	return s
}()

// PrometheusHandler returns an http.Handler that renders every loop of e in
// the Prometheus text exposition format (version 0.0.4), one series per
// metric labelled loop="<id>". It is written by hand so the module stays free
// of third-party dependencies. Each request takes a fresh Engine.Metrics
// snapshot; it is safe to serve from any goroutine.
func PrometheusHandler(e *twine.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		snaps := e.Metrics()
		buf := make([]byte, 0, len(allSeries)*(96+48*len(snaps)))
		for i := range allSeries {
			s := &allSeries[i]
			buf = append(buf, "# HELP "...)
			buf = append(buf, s.name...)
			buf = append(buf, ' ')
			buf = append(buf, s.help...)
			buf = append(buf, "\n# TYPE "...)
			buf = append(buf, s.name...)
			buf = append(buf, ' ')
			buf = append(buf, s.kind...)
			buf = append(buf, '\n')
			for j := range snaps {
				buf = append(buf, s.name...)
				buf = append(buf, `{loop="`...)
				buf = strconv.AppendInt(buf, int64(snaps[j].LoopID), 10)
				buf = append(buf, `"} `...)
				buf = strconv.AppendFloat(buf, s.get(&snaps[j]), 'g', -1, 64)
				buf = append(buf, '\n')
			}
		}
		_, _ = w.Write(buf)
	})
}
