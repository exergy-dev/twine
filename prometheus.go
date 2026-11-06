package twine

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// PrometheusHandler returns an http.Handler that renders every loop's
// metrics in the Prometheus text exposition format. It is written by hand so
// the root module stays free of third-party dependencies.
func (e *Engine) PrometheusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var sb strings.Builder
		snaps := e.Metrics()
		type series struct {
			name, help, kind string
			get              func(MetricsSnapshot) float64
		}
		all := []series{
			{"twine_connections", "Open connections", "gauge", func(s MetricsSnapshot) float64 { return float64(s.Connections) }},
			{"twine_accepts_total", "Accepted connections", "counter", func(s MetricsSnapshot) float64 { return float64(s.Accepts) }},
			{"twine_closes_total", "Closed connections", "counter", func(s MetricsSnapshot) float64 { return float64(s.Closes) }},
			{"twine_reads_total", "read(2) calls returning data", "counter", func(s MetricsSnapshot) float64 { return float64(s.Reads) }},
			{"twine_read_bytes_total", "Bytes read", "counter", func(s MetricsSnapshot) float64 { return float64(s.ReadBytes) }},
			{"twine_write_bytes_total", "Bytes written", "counter", func(s MetricsSnapshot) float64 { return float64(s.WriteBytes) }},
			{"twine_writev_total", "Flush writev calls", "counter", func(s MetricsSnapshot) float64 { return float64(s.Writev) }},
			{"twine_write_direct_hit_total", "Writes that bypassed the ring", "counter", func(s MetricsSnapshot) float64 { return float64(s.WriteDirectHit) }},
			{"twine_write_after_close_total", "Writes on closed transports", "counter", func(s MetricsSnapshot) float64 { return float64(s.WriteAfterClose) }},
			{"twine_timers_fired_total", "Timers fired", "counter", func(s MetricsSnapshot) float64 { return float64(s.TimersFired) }},
			{"twine_tasks_run_total", "CallFromThread tasks run", "counter", func(s MetricsSnapshot) float64 { return float64(s.TasksRun) }},
			{"twine_wakeups_total", "eventfd wakeups", "counter", func(s MetricsSnapshot) float64 { return float64(s.Wakeups) }},
			{"twine_wakeups_coalesced_total", "Wakeups avoided by coalescing", "counter", func(s MetricsSnapshot) float64 { return float64(s.WakeupsCoalesced) }},
			{"twine_iterations_total", "Loop iterations", "counter", func(s MetricsSnapshot) float64 { return float64(s.Iterations) }},
			{"twine_poll_block_seconds_total", "Time blocked in poll", "counter", func(s MetricsSnapshot) float64 { return float64(s.PollBlockNanos) / 1e9 }},
			{"twine_panics_total", "Recovered panics", "counter", func(s MetricsSnapshot) float64 { return float64(s.Panics) }},
			{"twine_backlog_drops_total", "Tasks dropped after loop exit", "counter", func(s MetricsSnapshot) float64 { return float64(s.BacklogDrops) }},
			{"twine_accept_errors_total", "accept(2) errors", "counter", func(s MetricsSnapshot) float64 { return float64(s.AcceptErrors) }},
			{"twine_datagrams_total", "Datagrams received", "counter", func(s MetricsSnapshot) float64 { return float64(s.Datagrams) }},
			{"twine_datagrams_sent_total", "Datagrams sent", "counter", func(s MetricsSnapshot) float64 { return float64(s.DatagramsSent) }},
			{"twine_slow_callbacks_total", "Watchdog reports", "counter", func(s MetricsSnapshot) float64 { return float64(s.SlowCallbacks) }},
			{"twine_iteration_seconds_p50", "Iteration duration p50 over 10s", "gauge", func(s MetricsSnapshot) float64 { return s.IterationP50.Seconds() }},
			{"twine_iteration_seconds_p99", "Iteration duration p99 over 10s", "gauge", func(s MetricsSnapshot) float64 { return s.IterationP99.Seconds() }},
			{"twine_iteration_seconds_max", "Iteration duration max over 10s", "gauge", func(s MetricsSnapshot) float64 { return s.IterationMax.Seconds() }},
			{"twine_slab_in_use_bytes", "Slab bytes handed out", "gauge", func(s MetricsSnapshot) float64 { return float64(s.SlabInUse) }},
			{"twine_slab_free_bytes", "Slab bytes in free lists", "gauge", func(s MetricsSnapshot) float64 { return float64(s.SlabFree) }},
			{"twine_ring_high_water_bytes", "Largest pending write seen", "gauge", func(s MetricsSnapshot) float64 { return float64(s.RingHighWater) }},
		}
		sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })
		for _, s := range all {
			fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s %s\n", s.name, s.help, s.name, s.kind)
			for _, snap := range snaps {
				fmt.Fprintf(&sb, "%s{loop=\"%d\"} %g\n", s.name, snap.LoopID, s.get(snap))
			}
		}
		_, _ = w.Write([]byte(sb.String()))
	})
}
