//! Per-loop counters, the iteration-duration histogram and `MetricsSnapshot`
//! (design Section 11). Counters are plain atomics: most are written by the
//! loop thread only (so a relaxed load + store suffices) and read from anywhere.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const HIST_SLOTS: usize = 10;
const HIST_BUCKETS: usize = 40;

/// Increments a counter that only the owning loop thread writes.
#[inline(always)]
pub(crate) fn bump(c: &AtomicU64) {
    c.store(c.load(Relaxed).wrapping_add(1), Relaxed);
}

/// Adds to a counter that only the owning loop thread writes.
#[inline(always)]
pub(crate) fn add(c: &AtomicU64, n: u64) {
    c.store(c.load(Relaxed).wrapping_add(n), Relaxed);
}

/// Decrements a gauge that only the owning loop thread writes.
#[inline(always)]
pub(crate) fn dec(c: &AtomicU64) {
    c.store(c.load(Relaxed).wrapping_sub(1), Relaxed);
}

/// Log2-bucketed histogram over a sliding 10-second window of one-second
/// slots. `observe` runs on the loop; `percentiles` from anywhere.
pub(crate) struct Histogram {
    buckets: [[AtomicU64; HIST_BUCKETS]; HIST_SLOTS],
    max_ns: [AtomicU64; HIST_SLOTS],
    /// The second each slot was last written for (`u64::MAX` = never).
    slot_sec: [AtomicU64; HIST_SLOTS],
}

impl Histogram {
    fn new() -> Histogram {
        Histogram {
            buckets: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            max_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            slot_sec: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
        }
    }

    /// Records one iteration of `ns` nanoseconds observed during second `sec`.
    pub(crate) fn observe(&self, ns: u64, sec: u64) {
        let slot = (sec % HIST_SLOTS as u64) as usize;
        if self.slot_sec[slot].load(Relaxed) != sec {
            for b in &self.buckets[slot] {
                b.store(0, Relaxed);
            }
            self.max_ns[slot].store(0, Relaxed);
            self.slot_sec[slot].store(sec, Relaxed);
        }
        let b = ((64 - ns.leading_zeros()) as usize).min(HIST_BUCKETS - 1);
        bump(&self.buckets[slot][b]);
        if ns > self.max_ns[slot].load(Relaxed) {
            self.max_ns[slot].store(ns, Relaxed);
        }
    }

    fn percentiles(&self, cur_sec: u64) -> (Duration, Duration, Duration) {
        let mut merged = [0u64; HIST_BUCKETS];
        let mut total = 0u64;
        let mut maxv = 0u64;
        for s in 0..HIST_SLOTS {
            let stamp = self.slot_sec[s].load(Relaxed);
            if stamp == u64::MAX || stamp + HIST_SLOTS as u64 <= cur_sec {
                continue; // never written, or older than the window
            }
            for (b, m) in merged.iter_mut().enumerate() {
                let v = self.buckets[s][b].load(Relaxed);
                *m += v;
                total += v;
            }
            maxv = maxv.max(self.max_ns[s].load(Relaxed));
        }
        if total == 0 {
            return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        }
        let pick = |q: f64| -> Duration {
            let target = ((total as f64) * q) as u64;
            let mut acc = 0u64;
            for (b, m) in merged.iter().enumerate() {
                acc += m;
                if acc >= target {
                    if b == 0 {
                        return Duration::ZERO;
                    }
                    return Duration::from_nanos(1u64 << b);
                }
            }
            Duration::from_nanos(maxv)
        };
        (pick(0.5), pick(0.99), Duration::from_nanos(maxv))
    }
}

/// Per-loop counters. Updated on the loop without contention and readable from
/// any thread through [`LoopMetrics::snapshot`].
pub struct LoopMetrics {
    pub(crate) loop_id: usize,
    pub(crate) epoch: Instant,
    pub(crate) connections: AtomicU64,
    pub(crate) accepts: AtomicU64,
    pub(crate) closes: AtomicU64,
    pub(crate) reads: AtomicU64,
    pub(crate) read_bytes: AtomicU64,
    pub(crate) write_bytes: AtomicU64,
    pub(crate) writev: AtomicU64,
    pub(crate) write_direct_hit: AtomicU64,
    pub(crate) write_after_close: AtomicU64,
    pub(crate) timers_fired: AtomicU64,
    pub(crate) tasks_run: AtomicU64,
    /// Multi-writer: incremented by `call_from_thread` callers.
    pub(crate) wakeups: AtomicU64,
    /// Multi-writer: incremented by `call_from_thread` callers.
    pub(crate) wakeups_coalesced: AtomicU64,
    pub(crate) iterations: AtomicU64,
    pub(crate) poll_block_nanos: AtomicU64,
    pub(crate) panics: AtomicU64,
    /// Multi-writer: incremented by `call_from_thread` callers.
    pub(crate) backlog_drops: AtomicU64,
    pub(crate) accept_errors: AtomicU64,
    pub(crate) datagrams: AtomicU64,
    pub(crate) datagrams_sent: AtomicU64,
    /// Incremented by the watchdog thread.
    pub(crate) slow_callbacks: AtomicU64,
    pub(crate) slab_in_use: AtomicU64,
    /// Timers currently scheduled on the wheel (gauge, refreshed each iteration).
    pub(crate) timers_active: AtomicU64,
    pub(crate) slots_live: AtomicU64,
    pub(crate) slots_table: AtomicU64,
    pub(crate) slab_free: AtomicU64,
    pub(crate) ring_high_water: AtomicU64,
    /// Monotonic ns (loop epoch) + 1 when the current callback began; 0 when idle.
    pub(crate) cb_start: AtomicU64,
    /// Index + 1 into `names` of the protocol type running the current callback; 0 = none.
    pub(crate) cb_proto: AtomicU32,
    /// Interned protocol type names (see `LoopMetrics::proto_name`).
    pub(crate) names: Mutex<Vec<&'static str>>,
    pub(crate) hist: Histogram,
}

impl LoopMetrics {
    pub(crate) fn new(loop_id: usize, epoch: Instant) -> LoopMetrics {
        let z = || AtomicU64::new(0);
        LoopMetrics {
            loop_id,
            epoch,
            connections: z(),
            accepts: z(),
            closes: z(),
            reads: z(),
            read_bytes: z(),
            write_bytes: z(),
            writev: z(),
            write_direct_hit: z(),
            write_after_close: z(),
            timers_fired: z(),
            tasks_run: z(),
            wakeups: z(),
            wakeups_coalesced: z(),
            iterations: z(),
            poll_block_nanos: z(),
            panics: z(),
            backlog_drops: z(),
            accept_errors: z(),
            datagrams: z(),
            datagrams_sent: z(),
            slow_callbacks: z(),
            slab_in_use: z(),
            timers_active: z(),
            slots_live: z(),
            slots_table: z(),
            slab_free: z(),
            ring_high_water: z(),
            cb_start: z(),
            cb_proto: AtomicU32::new(0),
            names: Mutex::new(Vec::new()),
            hist: Histogram::new(),
        }
    }

    /// The name of the protocol type currently inside a callback, if any
    /// (the watchdog reads this together with `cb_start`).
    #[allow(dead_code)] // read by the watchdog thread (added with watchdog.rs)
    pub(crate) fn proto_name(&self) -> Option<&'static str> {
        let idx = self.cb_proto.load(Relaxed);
        if idx == 0 {
            return None;
        }
        let names = self.names.lock().unwrap_or_else(|e| e.into_inner());
        names.get(idx as usize - 1).copied()
    }

    /// Takes a consistent-enough copy of every counter.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let cur_sec = self.epoch.elapsed().as_secs();
        let (p50, p99, max) = self.hist.percentiles(cur_sec);
        MetricsSnapshot {
            loop_id: self.loop_id,
            connections: self.connections.load(Relaxed),
            accepts: self.accepts.load(Relaxed),
            closes: self.closes.load(Relaxed),
            reads: self.reads.load(Relaxed),
            read_bytes: self.read_bytes.load(Relaxed),
            write_bytes: self.write_bytes.load(Relaxed),
            writev: self.writev.load(Relaxed),
            write_direct_hit: self.write_direct_hit.load(Relaxed),
            write_after_close: self.write_after_close.load(Relaxed),
            timers_fired: self.timers_fired.load(Relaxed),
            tasks_run: self.tasks_run.load(Relaxed),
            wakeups: self.wakeups.load(Relaxed),
            wakeups_coalesced: self.wakeups_coalesced.load(Relaxed),
            iterations: self.iterations.load(Relaxed),
            poll_block_nanos: self.poll_block_nanos.load(Relaxed),
            panics: self.panics.load(Relaxed),
            backlog_drops: self.backlog_drops.load(Relaxed),
            accept_errors: self.accept_errors.load(Relaxed),
            datagrams: self.datagrams.load(Relaxed),
            datagrams_sent: self.datagrams_sent.load(Relaxed),
            slow_callbacks: self.slow_callbacks.load(Relaxed),
            iteration_p50: p50,
            iteration_p99: p99,
            iteration_max: max,
            slab_in_use: self.slab_in_use.load(Relaxed) as usize,
            slab_free: self.slab_free.load(Relaxed) as usize,
            timers_active: self.timers_active.load(Relaxed),
            slots_live: self.slots_live.load(Relaxed),
            slots_table: self.slots_table.load(Relaxed),
            ring_high_water: self.ring_high_water.load(Relaxed) as usize,
        }
    }
}

/// A plain copy of one loop's [`LoopMetrics`] plus derived values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Index of the loop within its engine.
    pub loop_id: usize,
    /// Currently open TCP connections (gauge; listeners are not counted).
    pub connections: u64,
    /// Accepted connections.
    pub accepts: u64,
    /// Closed connections.
    pub closes: u64,
    /// `read(2)` calls that returned data.
    pub reads: u64,
    /// Bytes read.
    pub read_bytes: u64,
    /// Bytes written.
    pub write_bytes: u64,
    /// Flush `writev(2)` calls.
    pub writev: u64,
    /// Writes that bypassed the ring entirely.
    pub write_direct_hit: u64,
    /// Writes on closed, closing or stale transports (counted no-ops).
    pub write_after_close: u64,
    /// Timers fired.
    pub timers_fired: u64,
    /// `call_from_thread` tasks run.
    pub tasks_run: u64,
    /// Wakeup syscalls issued to the loop.
    pub wakeups: u64,
    /// Wakeups avoided by coalescing.
    pub wakeups_coalesced: u64,
    /// Loop iterations.
    pub iterations: u64,
    /// Nanoseconds spent blocked in the poller.
    pub poll_block_nanos: u64,
    /// Recovered panics.
    pub panics: u64,
    /// Tasks dropped because the loop had already exited.
    pub backlog_drops: u64,
    /// `accept(2)` errors.
    pub accept_errors: u64,
    /// Datagrams received.
    pub datagrams: u64,
    /// Datagrams sent.
    pub datagrams_sent: u64,
    /// Watchdog reports.
    pub slow_callbacks: u64,
    /// Iteration busy-time p50 over the last 10 s (log2 bucket upper bound).
    pub iteration_p50: Duration,
    /// Iteration busy-time p99 over the last 10 s (log2 bucket upper bound).
    pub iteration_p99: Duration,
    /// Longest iteration busy time over the last 10 s.
    pub iteration_max: Duration,
    /// Slab bytes handed out to rings.
    pub slab_in_use: usize,
    /// Slab bytes sitting in free lists.
    pub slab_free: usize,
    /// Largest pending-write backlog seen.
    pub ring_high_water: usize,
    /// Timers currently scheduled on the loop's wheel (gauge, sampled once per iteration).
    pub timers_active: u64,
    /// Connection-table entries holding a boxed slot: open connections plus
    /// listener and UDP sockets (gauge).
    pub slots_live: u64,
    /// Length of the fd-indexed connection table; each entry costs 16 bytes
    /// plus a box while live (gauge).
    pub slots_table: u64,
}
