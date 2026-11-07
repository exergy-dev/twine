//! Timer microbenchmark: 1M `Loop::call_later` inserts, 1M `cancel_timer`s, a
//! steady-state insert+cancel cycle with 1M timers active, and firing 1M due
//! timers, all inside one `call_from_thread` task on a single loop.
//!
//! Flags: `--n 1000000`. Prints ns/op per phase and a `RESULT timers ...` line.
#![forbid(unsafe_code)]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use twine::{Engine, Loop, TimerId};
use twine_bench::Args;

const USAGE: &str = "usage: timers [--n 1000000]";

/// Deterministic spread of deadlines over 1 s .. 60 s so timers land in every wheel level.
fn delay(i: usize) -> Duration {
    let x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40;
    Duration::from_millis(1000 + x % 59_000)
}

fn main() {
    let n = Args::parse(USAGE, &["n"]).usize("n", 1_000_000);
    let eng = Engine::builder().loops(1).build().expect("engine");
    let h = eng.loop_handles().remove(0);
    let (tx, rx) = mpsc::channel::<[f64; 4]>();
    let eh = eng.handle();
    let runner = std::thread::spawn(move || eng.run());
    h.call_from_thread(move |lp: &mut Loop| {
        let ns = |t: Instant| t.elapsed().as_nanos() as f64 / n as f64;
        let mut ids: Vec<TimerId> = Vec::with_capacity(n);
        let t = Instant::now();
        for i in 0..n {
            ids.push(lp.call_later(delay(i), |_| {}));
        }
        let insert = ns(t);
        let t = Instant::now();
        let mut cancelled = 0;
        for id in ids.drain(..) {
            cancelled += usize::from(lp.cancel_timer(id));
        }
        let cancel = ns(t);
        assert_eq!(cancelled, n, "every cancel must succeed");

        // Steady state: n timers active; each op inserts one and cancels the oldest.
        let mut ring: Vec<TimerId> = (0..n).map(|i| lp.call_later(delay(i), |_| {})).collect();
        let t = Instant::now();
        for (i, slot) in ring.iter_mut().enumerate() {
            let new = lp.call_later(delay(i + n), |_| {});
            lp.cancel_timer(std::mem::replace(slot, new));
        }
        let cycle = ns(t);
        for id in ring {
            lp.cancel_timer(id);
        }

        // Fire: n timers due 1 ms from now; the task outlasts that, so all are due
        // when the loop next advances the wheel. Measure first to last callback.
        let fired = Rc::new(Cell::new(0usize));
        let first: Rc<Cell<Option<Instant>>> = Rc::new(Cell::new(None));
        for _ in 0..n {
            let (fired, first, tx) = (fired.clone(), first.clone(), tx.clone());
            lp.call_later(Duration::from_millis(1), move |_| {
                let now = Instant::now();
                if first.get().is_none() {
                    first.set(Some(now));
                }
                fired.set(fired.get() + 1);
                if fired.get() == n {
                    let span = now.duration_since(first.get().unwrap()).as_nanos() as f64;
                    let _ = tx.send([insert, cancel, cycle, span / n as f64]);
                }
            });
        }
    })
    .expect("submit");
    let r = rx
        .recv_timeout(Duration::from_secs(120))
        .expect("benchmark timed out");
    println!("timers n={n}");
    println!("  insert               {:8.1} ns/op", r[0]);
    println!("  cancel               {:8.1} ns/op", r[1]);
    println!(
        "  insert+cancel cycle  {:8.1} ns/op  ({n} timers active)",
        r[2]
    );
    println!("  fire                 {:8.1} ns/op", r[3]);
    println!(
        "RESULT timers n={n} insert_ns={:.1} cancel_ns={:.1} cycle_ns={:.1} fire_ns={:.1}",
        r[0], r[1], r[2], r[3]
    );
    eh.stop();
    let _ = runner.join();
}
