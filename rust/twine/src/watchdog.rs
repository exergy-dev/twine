//! The slow-callback watchdog (design Section 11).
//!
//! Every loop publishes, before dispatching a callback, `cb_start` (the loop's
//! monotonic nanoseconds since its epoch, plus one) and `cb_proto` (the
//! interned protocol type name), and clears `cb_start` to 0 afterwards. One
//! `twine-watchdog` thread samples every loop each `threshold / 2` (clamped to
//! 100 µs ..= 10 ms).
//!
//! **Clock.** [`LoopMetrics`](crate::metrics::LoopMetrics) carries the loop's `epoch: Instant`, which is
//! readable from any thread, so the elapsed time of a running callback is
//! `epoch.elapsed() - (cb_start - 1)` directly; no double sampling is needed.
//!
//! **Granularity.** The loop refreshes its cached clock once per iteration, so
//! `cb_start` is the start of the *iteration* the callback belongs to. A single
//! callback longer than the threshold is always reported; additionally an
//! iteration whose callbacks together run longer than the threshold may be
//! reported (the elapsed time then includes earlier callbacks of the same
//! iteration). The watchdog counts one `slow_callbacks` per distinct `cb_start`
//! value (so a callback observed by several samples counts once) and logs at
//! most once per second per loop at WARN.
//!
//! The sampling path performs no allocation; only a log record allocates.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::engine::EngineHandle;
use crate::event_loop::LoopHandle;

/// Minimum interval between WARN logs for one loop.
const LOG_EVERY: Duration = Duration::from_secs(1);
const MIN_SAMPLE: Duration = Duration::from_micros(100);
const MAX_SAMPLE: Duration = Duration::from_millis(10);

/// Handle to a running watchdog thread. Dropping it does not stop the thread;
/// call [`WatchdogHandle::stop`], or let the engine stop it.
pub struct WatchdogHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl WatchdogHandle {
    /// Asks the watchdog thread to exit and waits for it.
    pub fn stop(mut self) {
        self.stop.store(true, Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }

    /// True while the watchdog thread is still running.
    pub fn is_running(&self) -> bool {
        self.join.as_ref().is_some_and(|j| !j.is_finished())
    }
}

/// Starts the watchdog for `engine` using `Options::watchdog` as the threshold.
/// Returns `None` when the watchdog is disabled (`watchdog(None)`) or the thread
/// could not be spawned. The thread exits when the engine stops (an
/// `on_shutdown` hook sets its stop flag; `is_stopped` is polled as a backstop
/// for engines stopped before `run`).
pub fn spawn(engine: &EngineHandle) -> Option<WatchdogHandle> {
    let threshold = engine.0.opts.watchdog?;
    start_with(engine, threshold)
}

/// Crate-internal entry for `Engine::run`.
pub(crate) fn start(engine: &EngineHandle) -> Option<WatchdogHandle> {
    spawn(engine)
}

fn start_with(engine: &EngineHandle, threshold: Duration) -> Option<WatchdogHandle> {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    engine.on_shutdown(move || flag.store(true, Relaxed));
    let loops = engine.loop_handles();
    let eng = engine.clone();
    let flag = stop.clone();
    let join = std::thread::Builder::new()
        .name("twine-watchdog".into())
        .spawn(move || run(&eng, &loops, threshold, &flag))
        .ok()?;
    Some(WatchdogHandle {
        stop,
        join: Some(join),
    })
}

struct LoopState {
    /// The `cb_start` value already counted, so one callback counts once.
    counted: u64,
    last_log: Option<Instant>,
}

fn run(engine: &EngineHandle, loops: &[LoopHandle], threshold: Duration, stop: &AtomicBool) {
    let period = (threshold / 2).clamp(MIN_SAMPLE, MAX_SAMPLE);
    let mut states: Vec<LoopState> = loops
        .iter()
        .map(|_| LoopState {
            counted: 0,
            last_log: None,
        })
        .collect();
    let threshold_ns = threshold.as_nanos() as u64;
    while !stop.load(Relaxed) && !engine.is_stopped() {
        std::thread::sleep(period);
        for (lh, st) in loops.iter().zip(states.iter_mut()) {
            sample(lh, st, threshold_ns);
        }
    }
}

fn sample(lh: &LoopHandle, st: &mut LoopState, threshold_ns: u64) {
    let m = &*lh.0.metrics;
    let start = m.cb_start.load(Relaxed);
    if start == 0 || start == st.counted {
        return;
    }
    let proto = m.proto_name();
    // Re-check after reading the name: the callback may have just finished.
    if m.cb_start.load(Relaxed) != start {
        return;
    }
    let now_ns = m.epoch.elapsed().as_nanos() as u64;
    let elapsed = now_ns.saturating_sub(start - 1);
    if elapsed < threshold_ns {
        return;
    }
    st.counted = start;
    m.slow_callbacks.fetch_add(1, Relaxed);
    let due = st.last_log.is_none_or(|t| t.elapsed() >= LOG_EVERY);
    if due {
        st.last_log = Some(Instant::now());
        log::warn!(
            "twine: slow callback on loop {}: {} running for {:?}",
            m.loop_id,
            proto.unwrap_or("<task or timer>"),
            Duration::from_nanos(elapsed)
        );
    }
}
