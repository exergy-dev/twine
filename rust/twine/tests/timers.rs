//! Timers, idle timeouts, connection token timers and `call_from_thread`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use twine::{factory, Engine, Error, Options, Protocol, TimerId, Transport};

// --- idle timeout ---------------------------------------------------------------

struct Idle {
    lost: Arc<Mutex<Option<(Instant, String)>>>,
    timeout: Duration,
    data: Arc<AtomicUsize>,
}

impl Protocol for Idle {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_idle_timeout(Some(self.timeout));
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, d: &mut [u8]) {
        self.data.fetch_add(d.len(), SeqCst);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        *self.lost.lock().unwrap() = Some((
            Instant::now(),
            r.map_or("clean".into(), |e| format!("{e:?}")),
        ));
    }
}

type LostSlot = Arc<Mutex<Option<(Instant, String)>>>;

fn idle_server(timeout: Duration) -> (Server, LostSlot, Arc<AtomicUsize>) {
    let lost = Arc::new(Mutex::new(None));
    let data = Arc::new(AtomicUsize::new(0));
    let (l2, d2) = (lost.clone(), data.clone());
    let srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Idle {
                lost: l2.clone(),
                timeout,
                data: d2.clone(),
            })
        }),
    );
    (srv, lost, data)
}

#[test]
fn idle_timeout_fires_within_a_tick() {
    let (mut srv, lost, _) = idle_server(Duration::from_millis(50));
    let t0 = Instant::now();
    let _c = srv.connect();
    assert!(wait_until(Duration::from_secs(2), || lost
        .lock()
        .unwrap()
        .is_some()));
    let (at, why) = lost.lock().unwrap().clone().unwrap();
    let dt = at.duration_since(t0);
    eprintln!("idle timeout fired after {dt:?}");
    assert!(why.contains("IdleTimeout"), "reason: {why}");
    assert!(dt >= Duration::from_millis(45), "fired early: {dt:?}");
    assert!(dt < Duration::from_secs(1), "fired late: {dt:?}");
    srv.stop();
}

#[test]
fn activity_resets_idle_timeout() {
    let (mut srv, lost, data) = idle_server(Duration::from_millis(80));
    let mut c = srv.connect();
    // 12 bytes, 30 ms apart: 360 ms of life against an 80 ms timeout.
    for _ in 0..12 {
        c.write_all_bytes(b"x");
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            lost.lock().unwrap().is_none(),
            "activity must reset the idle timer"
        );
    }
    assert_eq!(data.load(SeqCst), 12);
    let last = Instant::now();
    assert!(wait_until(Duration::from_secs(2), || lost
        .lock()
        .unwrap()
        .is_some()));
    let (at, why) = lost.lock().unwrap().clone().unwrap();
    assert!(why.contains("IdleTimeout"));
    let dt = at.duration_since(last);
    assert!(dt < Duration::from_secs(1), "{dt:?}");
    srv.stop();
}

trait WriteAllBytes {
    fn write_all_bytes(&mut self, b: &[u8]);
}
impl WriteAllBytes for std::net::TcpStream {
    fn write_all_bytes(&mut self, b: &[u8]) {
        use std::io::Write;
        self.write_all(b).unwrap();
    }
}

// --- call_from_thread -------------------------------------------------------------

static CALLS: AtomicUsize = AtomicUsize::new(0);

#[test]
fn call_from_thread_100_threads_x_100_calls_coalesces_wakeups() {
    let eng = Arc::new(Engine::new(Options::default().loops(1)).unwrap());
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    std::thread::sleep(Duration::from_millis(100)); // loop is blocked in poll
    let lh = eng.loop_handles()[0].clone();
    assert!(!lh.in_loop());
    CALLS.store(0, SeqCst);
    let threads: Vec<_> = (0..100)
        .map(|_| {
            let lh = lh.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    lh.call_from_thread(|lp| {
                        assert!(lp.handle().in_loop());
                        CALLS.fetch_add(1, SeqCst);
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(wait_until(Duration::from_secs(10), || CALLS.load(SeqCst) == 10_000));
    let m = lh.metrics();
    eprintln!(
        "wakeups={} coalesced={} tasks_run={}",
        m.wakeups, m.wakeups_coalesced, m.tasks_run
    );
    // Exact invariant: every call from a foreign thread increments exactly one of
    // `wakeups` (won the coalescing flag; one wake syscall) or `wakeups_coalesced`.
    assert_eq!(m.wakeups + m.wakeups_coalesced, 10_000);
    assert!(m.wakeups <= 10_000);
    assert_eq!(m.tasks_run, 10_000);
    eng.stop();
    j.join().unwrap().unwrap();
    // After the loop exited, calls fail and are counted.
    assert!(lh.is_stopped());
    assert!(matches!(
        lh.call_from_thread(|_| {}),
        Err(Error::LoopStopped)
    ));
    assert_eq!(lh.metrics().backlog_drops, 1);
}

#[test]
fn call_from_thread_overflow_spills_and_keeps_order() {
    // A queue of 2 forces nearly everything through the overflow vector.
    let eng = Arc::new(Engine::new(Options::default().loops(1).task_queue_size(2)).unwrap());
    let lh = eng.loop_handles()[0].clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    // Queued before the loop starts: drained in order at startup.
    for i in 0..5000usize {
        let seen = seen.clone();
        lh.call_from_thread(move |_| seen.lock().unwrap().push(i))
            .unwrap();
    }
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    assert!(wait_until(Duration::from_secs(10), || seen
        .lock()
        .unwrap()
        .len()
        == 5000));
    let v = seen.lock().unwrap().clone();
    assert!(
        v.windows(2).all(|w| w[0] < w[1]),
        "FIFO order across the spill"
    );
    eng.stop();
    j.join().unwrap().unwrap();
}

// --- Loop::call_later --------------------------------------------------------------

fn on_loop<T: Send + 'static>(
    eng: &Arc<Engine>,
    f: impl FnOnce(&mut twine::Loop) -> T + Send + 'static,
) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    eng.loop_handles()[0]
        .call_from_thread(move |lp| {
            let _ = tx.send(f(lp));
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(10)).unwrap()
}

fn running_engine() -> (Arc<Engine>, std::thread::JoinHandle<twine::Result<()>>) {
    let eng = Arc::new(Engine::new(Options::default().loops(1)).unwrap());
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    (eng, j)
}

#[test]
fn call_later_ordering_cancel_and_reset() {
    let (eng, j) = running_engine();
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let o = order.clone();
    let (cancelled, second_cancel, reset_ok): (bool, bool, bool) = on_loop(&eng, move |lp| {
        let push = |o: &Arc<Mutex<Vec<u32>>>, n: u32| {
            let o = o.clone();
            move |_: &mut twine::Loop| o.lock().unwrap().push(n)
        };
        lp.call_later(Duration::from_millis(60), push(&o, 3));
        lp.call_later(Duration::from_millis(20), push(&o, 1));
        lp.call_later(Duration::from_millis(40), push(&o, 2));
        let dead: TimerId = lp.call_later(Duration::from_millis(30), push(&o, 99));
        let c1 = lp.cancel_timer(dead);
        let c2 = lp.cancel_timer(dead);
        // 200 ms timer reset to fire before the 60 ms one finishes: slot 0 (10 ms).
        let moved = lp.call_later(Duration::from_millis(200), push(&o, 0));
        let r = lp.reset_timer(moved, Duration::from_millis(10));
        (c1, c2, r)
    });
    assert!(cancelled);
    assert!(!second_cancel);
    assert!(reset_ok);
    assert!(wait_until(Duration::from_secs(5), || order
        .lock()
        .unwrap()
        .len()
        == 4));
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        *order.lock().unwrap(),
        vec![0, 1, 2, 3],
        "cancelled timer must not fire"
    );
    eng.stop();
    j.join().unwrap().unwrap();
}

#[test]
fn call_later_zero_runs_next_iteration_never_reentrantly() {
    let (eng, j) = running_engine();
    let ran = Arc::new(AtomicBool::new(false));
    let task_iters = Arc::new(AtomicU64::new(0));
    let timer_iters = Arc::new(AtomicU64::new(0));
    let (r2, ti, tm) = (ran.clone(), task_iters.clone(), timer_iters.clone());
    let ran_inside = on_loop(&eng, move |lp| {
        let (r3, tm2) = (r2.clone(), tm.clone());
        ti.store(lp.metrics().iterations, SeqCst);
        lp.call_later(Duration::ZERO, move |lp| {
            tm2.store(lp.metrics().iterations, SeqCst);
            r3.store(true, SeqCst);
        });
        r2.load(SeqCst) // must still be false: not re-entrant
    });
    assert!(!ran_inside);
    assert!(wait_until(Duration::from_secs(5), || ran.load(SeqCst)));
    assert!(
        timer_iters.load(SeqCst) > task_iters.load(SeqCst),
        "zero-delay timer must run on a later iteration (task {} timer {})",
        task_iters.load(SeqCst),
        timer_iters.load(SeqCst)
    );
    // A self re-arming zero timer runs once per iteration and does not starve I/O/tasks.
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    on_loop(&eng, move |lp| {
        fn rearm(lp: &mut twine::Loop, n: Arc<AtomicUsize>) {
            if n.fetch_add(1, SeqCst) < 1000 {
                lp.call_later(Duration::ZERO, move |lp| rearm(lp, n));
            }
        }
        rearm(lp, n2);
    });
    // Tasks still get through while the zero timer spins.
    on_loop(&eng, |_| ());
    assert!(wait_until(Duration::from_secs(5), || n.load(SeqCst) > 1000));
    eng.stop();
    j.join().unwrap().unwrap();
}

#[test]
fn call_soon_runs_after_current_task() {
    let (eng, j) = running_engine();
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let l = log.clone();
    on_loop(&eng, move |lp| {
        let l2 = l.clone();
        lp.call_soon(move |_| l2.lock().unwrap().push("soon"));
        l.lock().unwrap().push("task");
    });
    assert!(wait_until(Duration::from_secs(5), || log
        .lock()
        .unwrap()
        .len()
        == 2));
    assert_eq!(*log.lock().unwrap(), vec!["task", "soon"]);
    eng.stop();
    j.join().unwrap().unwrap();
}

#[test]
fn panicking_task_and_timer_do_not_kill_the_loop() {
    let (eng, j) = running_engine();
    let lh = eng.loop_handles()[0].clone();
    lh.call_from_thread(|_| panic!("task boom")).unwrap();
    lh.call_from_thread(|lp| {
        lp.call_later(Duration::from_millis(5), |_| panic!("timer boom"));
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(on_loop(&eng, |_| 7), 7, "loop survived");
    assert_eq!(lh.metrics().panics, 2);
    eng.stop();
    j.join().unwrap().unwrap();
}

// --- Transport::call_later ---------------------------------------------------------

#[derive(Default)]
struct Tokens {
    fired: Mutex<Vec<(u64, Instant)>>,
    lost: AtomicBool,
}

struct TokenProto(Arc<Tokens>);

impl Protocol for TokenProto {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.call_later(Duration::from_millis(30), 2);
        t.call_later(Duration::from_millis(10), 1);
        let dead = t.call_later(Duration::from_millis(20), 666);
        assert!(t.cancel_timer(dead));
        t.call_later(Duration::ZERO, 0);
        // Armed far in the future and left pending: must never be delivered after close.
        t.call_later(Duration::from_millis(400), 4242);
    }
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        t.abort();
    }
    fn timer_fired(&mut self, _t: &mut Transport<'_>, token: u64) {
        assert!(
            !self.0.lost.load(SeqCst),
            "timer delivered after connection_lost"
        );
        self.0.fired.lock().unwrap().push((token, Instant::now()));
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.0.lost.store(true, SeqCst);
    }
}

#[test]
fn conn_timer_tokens_delivered_in_order_and_not_after_close() {
    let st = Arc::new(Tokens::default());
    let s2 = st.clone();
    let mut srv = Server::start(1, factory(move |_, _| Box::new(TokenProto(s2.clone()))));
    let mut c = srv.connect();
    assert!(wait_until(Duration::from_secs(2), || st
        .fired
        .lock()
        .unwrap()
        .len()
        == 3));
    let toks: Vec<u64> = st.fired.lock().unwrap().iter().map(|(t, _)| *t).collect();
    assert_eq!(toks, vec![0, 1, 2]);
    // Close the connection while the 400 ms token is pending, then wait past it.
    use std::io::Write;
    c.write_all(b"x").unwrap();
    assert!(wait_until(Duration::from_secs(2), || st.lost.load(SeqCst)));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        st.fired.lock().unwrap().len(),
        3,
        "token 4242 must not be delivered"
    );
    srv.stop();
}
