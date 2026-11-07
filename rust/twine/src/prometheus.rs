//! Prometheus text exposition rendering of [`MetricsSnapshot`]s (design
//! Section 11). Counters carry the `_total` suffix, gauges are plain, and the
//! iteration busy-time percentiles are a `summary` in seconds.

use std::fmt::Write;

use crate::engine::EngineHandle;
use crate::metrics::MetricsSnapshot;

type Get = fn(&MetricsSnapshot) -> u64;

/// `(name, help, getter)` for every counter (rendered as `twine_<name>_total`).
const COUNTERS: &[(&str, &str, Get)] = &[
    ("accepts", "Accepted connections.", |s| s.accepts),
    ("closes", "Closed connections.", |s| s.closes),
    ("reads", "read(2) calls that returned data.", |s| s.reads),
    ("read_bytes", "Bytes read.", |s| s.read_bytes),
    ("write_bytes", "Bytes written.", |s| s.write_bytes),
    ("writev", "Flush writev(2) calls.", |s| s.writev),
    ("write_direct_hit", "Writes that bypassed the ring.", |s| {
        s.write_direct_hit
    }),
    ("write_after_close", "Writes on closed transports.", |s| {
        s.write_after_close
    }),
    ("timers_fired", "Timers fired.", |s| s.timers_fired),
    ("tasks_run", "call_from_thread tasks run.", |s| s.tasks_run),
    ("wakeups", "Wakeup syscalls issued.", |s| s.wakeups),
    ("wakeups_coalesced", "Wakeups avoided by coalescing.", |s| {
        s.wakeups_coalesced
    }),
    ("iterations", "Loop iterations.", |s| s.iterations),
    ("panics", "Recovered panics.", |s| s.panics),
    (
        "backlog_drops",
        "Tasks dropped after the loop exited.",
        |s| s.backlog_drops,
    ),
    ("accept_errors", "accept(2) errors.", |s| s.accept_errors),
    ("datagrams", "Datagrams received.", |s| s.datagrams),
    ("datagrams_sent", "Datagrams sent.", |s| s.datagrams_sent),
    (
        "slow_callbacks",
        "Callbacks reported by the watchdog.",
        |s| s.slow_callbacks,
    ),
];

/// `(name, help, getter)` for every gauge (rendered as `twine_<name>`).
const GAUGES: &[(&str, &str, Get)] = &[
    ("connections", "Currently open TCP connections.", |s| {
        s.connections
    }),
    (
        "slab_in_use_bytes",
        "Slab bytes handed out to rings.",
        |s| s.slab_in_use as u64,
    ),
    (
        "slab_free_bytes",
        "Slab bytes sitting in free lists.",
        |s| s.slab_free as u64,
    ),
    (
        "timers_active",
        "Timers currently scheduled on the loop's wheel.",
        |s| s.timers_active,
    ),
    (
        "ring_high_water_bytes",
        "Largest pending-write backlog seen.",
        |s| s.ring_high_water as u64,
    ),
    (
        "slots_live",
        "Connection-table entries holding a boxed slot.",
        |s| s.slots_live,
    ),
    (
        "slots_table",
        "Length of the fd-indexed connection table.",
        |s| s.slots_table,
    ),
];

/// Renders `snapshots` in the Prometheus text exposition format: one `# HELP`
/// and `# TYPE` line per metric, then one `twine_<metric>{loop="N"} value`
/// sample per loop.
///
/// Counters are `twine_<name>_total`; gauges are plain; poll blocking time is
/// `twine_poll_block_seconds_total`; iteration busy time is the summary
/// `twine_iteration_seconds{loop="N",quantile="0.5"|"0.99"}` plus the gauge
/// `twine_iteration_max_seconds`.
pub fn render(snapshots: &[MetricsSnapshot]) -> String {
    let mut out = String::with_capacity(4096);
    for &(name, help, get) in COUNTERS {
        let _ = writeln!(out, "# HELP twine_{name}_total {help}");
        let _ = writeln!(out, "# TYPE twine_{name}_total counter");
        for s in snapshots {
            let _ = writeln!(
                out,
                "twine_{name}_total{{loop=\"{}\"}} {}",
                s.loop_id,
                get(s)
            );
        }
    }
    let _ = writeln!(
        out,
        "# HELP twine_poll_block_seconds_total Time spent blocked in the poller."
    );
    let _ = writeln!(out, "# TYPE twine_poll_block_seconds_total counter");
    for s in snapshots {
        let _ = writeln!(
            out,
            "twine_poll_block_seconds_total{{loop=\"{}\"}} {}",
            s.loop_id,
            s.poll_block_nanos as f64 / 1e9
        );
    }
    for &(name, help, get) in GAUGES {
        let _ = writeln!(out, "# HELP twine_{name} {help}");
        let _ = writeln!(out, "# TYPE twine_{name} gauge");
        for s in snapshots {
            let _ = writeln!(out, "twine_{name}{{loop=\"{}\"}} {}", s.loop_id, get(s));
        }
    }
    let _ = writeln!(
        out,
        "# HELP twine_iteration_seconds Loop iteration busy time over the last 10 s."
    );
    let _ = writeln!(out, "# TYPE twine_iteration_seconds summary");
    for s in snapshots {
        let _ = writeln!(
            out,
            "twine_iteration_seconds{{loop=\"{}\",quantile=\"0.5\"}} {}",
            s.loop_id,
            s.iteration_p50.as_secs_f64()
        );
        let _ = writeln!(
            out,
            "twine_iteration_seconds{{loop=\"{}\",quantile=\"0.99\"}} {}",
            s.loop_id,
            s.iteration_p99.as_secs_f64()
        );
    }
    let _ = writeln!(
        out,
        "# HELP twine_iteration_max_seconds Longest iteration busy time over the last 10 s."
    );
    let _ = writeln!(out, "# TYPE twine_iteration_max_seconds gauge");
    for s in snapshots {
        let _ = writeln!(
            out,
            "twine_iteration_max_seconds{{loop=\"{}\"}} {}",
            s.loop_id,
            s.iteration_max.as_secs_f64()
        );
    }
    out
}

/// Renders the current metrics of every loop of `e` (see [`render`]).
pub fn render_engine(e: &EngineHandle) -> String {
    render(&e.metrics())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn snap(id: usize) -> MetricsSnapshot {
        MetricsSnapshot {
            loop_id: id,
            connections: 3,
            accepts: 5,
            closes: 2,
            reads: 11,
            read_bytes: 1024,
            write_bytes: 2048,
            writev: 7,
            write_direct_hit: 13,
            write_after_close: 1,
            timers_fired: 4,
            tasks_run: 6,
            wakeups: 8,
            wakeups_coalesced: 9,
            iterations: 100,
            poll_block_nanos: 1_500_000_000,
            panics: 1,
            backlog_drops: 2,
            accept_errors: 3,
            datagrams: 4,
            datagrams_sent: 5,
            slow_callbacks: 6,
            iteration_p50: Duration::from_micros(2),
            iteration_p99: Duration::from_millis(1),
            iteration_max: Duration::from_millis(250),
            slab_in_use: 4096,
            slab_free: 8192,
            ring_high_water: 65536,
            timers_active: 17,
            slots_live: 5,
            slots_table: 1024,
        }
    }

    #[test]
    fn exact_format_for_one_loop() {
        let out = render(&[snap(0)]);
        assert!(out.contains(
            "# HELP twine_accepts_total Accepted connections.\n\
             # TYPE twine_accepts_total counter\n\
             twine_accepts_total{loop=\"0\"} 5\n"
        ));
        assert!(out.contains("# TYPE twine_connections gauge\ntwine_connections{loop=\"0\"} 3\n"));
        assert!(out.contains("twine_poll_block_seconds_total{loop=\"0\"} 1.5\n"));
        assert!(out.contains(
            "# TYPE twine_iteration_seconds summary\n\
             twine_iteration_seconds{loop=\"0\",quantile=\"0.5\"} 0.000002\n\
             twine_iteration_seconds{loop=\"0\",quantile=\"0.99\"} 0.001\n"
        ));
        assert!(out.contains(
            "# TYPE twine_iteration_max_seconds gauge\n\
             twine_iteration_max_seconds{loop=\"0\"} 0.25\n"
        ));
        assert_eq!(out.matches("# TYPE twine_accepts_total").count(), 1);
    }

    #[test]
    fn every_field_appears_for_every_loop() {
        let out = render(&[snap(0), snap(1)]);
        // Each snapshot field -> the series that carries it.
        let series = [
            "twine_connections",
            "twine_accepts_total",
            "twine_closes_total",
            "twine_reads_total",
            "twine_read_bytes_total",
            "twine_write_bytes_total",
            "twine_writev_total",
            "twine_write_direct_hit_total",
            "twine_write_after_close_total",
            "twine_timers_fired_total",
            "twine_tasks_run_total",
            "twine_wakeups_total",
            "twine_wakeups_coalesced_total",
            "twine_iterations_total",
            "twine_poll_block_seconds_total",
            "twine_panics_total",
            "twine_backlog_drops_total",
            "twine_accept_errors_total",
            "twine_datagrams_total",
            "twine_datagrams_sent_total",
            "twine_slow_callbacks_total",
            "twine_slab_in_use_bytes",
            "twine_slab_free_bytes",
            "twine_ring_high_water_bytes",
            "twine_timers_active",
            "twine_slots_live",
            "twine_slots_table",
            "twine_iteration_max_seconds",
        ];
        for name in series {
            for l in 0..2 {
                assert!(
                    out.contains(&format!("{name}{{loop=\"{l}\"}} ")),
                    "missing {name} for loop {l}"
                );
            }
            assert_eq!(out.matches(&format!("# TYPE {name} ")).count(), 1, "{name}");
        }
        for l in 0..2 {
            assert!(out.contains(&format!(
                "twine_iteration_seconds{{loop=\"{l}\",quantile=\"0.5\"}}"
            )));
            assert!(out.contains(&format!(
                "twine_iteration_seconds{{loop=\"{l}\",quantile=\"0.99\"}}"
            )));
        }
    }
}
