//! `LoopHandle::call_from_thread` throughput: producer threads hammer one loop
//! with empty tasks; reports calls/s, tasks run and `wakeups / calls` taken from
//! the loop's `MetricsSnapshot` (coalescing keeps this far below 1).
//!
//! The task queue spills to an unbounded vector, so producers that outrun the loop
//! would only measure memory growth. Each producer therefore pauses while its
//! estimated share of the backlog exceeds ~1M tasks (a monitor thread publishes
//! the loop's `tasks_run` every millisecond); the reported rate is what the loop
//! sustains.
//!
//! Flags: `--threads 16`, `-d 5s`.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use twine::Engine;
use twine_bench::Args;

const USAGE: &str = "usage: callfromthread [--threads 16] [-d 5s]";

fn main() {
    let a = Args::parse(USAGE, &["threads", "d", "duration"]);
    let threads = a.usize("threads", 16);
    let dur = a.duration("d", a.duration("duration", Duration::from_secs(5)));
    let eng = Engine::builder().loops(1).build().expect("engine");
    let h = eng.loop_handles().remove(0);
    let eh = eng.handle();
    let runner = std::thread::spawn(move || eng.run());
    // Barrier: the loop is running once it executes a task.
    h.call(|_| ()).wait().expect("loop start");
    let before = h.metrics();

    let stop = Arc::new(AtomicBool::new(false));
    let executed = Arc::new(AtomicU64::new(0));
    let base_tasks = before.tasks_run;
    let monitor = {
        let (h, stop, executed) = (h.clone(), stop.clone(), executed.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                executed.store(h.metrics().tasks_run - base_tasks, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let start = Instant::now();
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let (h, stop, executed) = (h.clone(), stop.clone(), executed.clone());
            std::thread::spawn(move || {
                let (mut ok, mut err) = (0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    // Estimated backlog: all producers have made about as many calls as we have.
                    while ok * threads as u64 > executed.load(Ordering::Relaxed) + (1 << 20)
                        && !stop.load(Ordering::Relaxed)
                    {
                        std::thread::yield_now();
                    }
                    for _ in 0..64 {
                        match h.call_from_thread(|_| {}) {
                            Ok(()) => ok += 1,
                            Err(_) => err += 1,
                        }
                    }
                }
                (ok, err)
            })
        })
        .collect();
    std::thread::sleep(dur);
    stop.store(true, Ordering::Relaxed);
    let (mut calls, mut errs) = (0u64, 0u64);
    for w in workers {
        let (o, e) = w.join().expect("producer");
        calls += o;
        errs += e;
    }
    let el = start.elapsed().as_secs_f64();
    let _ = monitor.join();
    // Let the loop drain what is queued, then read the counters.
    h.call(|_| ()).wait().expect("drain");
    let after = h.metrics();
    let wakeups = after.wakeups - before.wakeups;
    let ran = after.tasks_run - before.tasks_run;
    println!("callfromthread threads={threads} window={el:.2}s");
    println!(
        "  calls   {calls} ({:.2}M/s), rejected {errs}",
        calls as f64 / el / 1e6
    );
    println!(
        "  tasks run {ran}, wakeups {wakeups}, coalesced {}",
        after.wakeups_coalesced - before.wakeups_coalesced
    );
    println!(
        "RESULT callfromthread threads={threads} ops_per_s={:.0} wakeups_per_call={:.6}",
        calls as f64 / el,
        wakeups as f64 / calls.max(1) as f64
    );
    eh.stop();
    let _ = runner.join();
}
