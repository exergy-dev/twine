//! Allocation gates (design Sections 7.3 and 12). The counter is per thread, so
//! the protocols and tasks below sample `twine_alloccount::count()` on the LOOP
//! thread and report the largest allocation delta seen between consecutive
//! callbacks. Those deltas span the whole iteration in between (poll, dispatch,
//! flush, timers, recycle), so a zero is a strong statement.

mod common;

use std::io::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, Error, ListenOptions, Options, Protocol, Transport};
use twine_alloccount::count;

#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

/// Tracks the largest allocation delta between consecutive samples.
#[derive(Default)]
struct Gate {
    last: AtomicU64,
    max: AtomicU64,
    samples: AtomicUsize,
    warm: usize,
}

impl Gate {
    fn new(warm: usize) -> Arc<Gate> {
        Arc::new(Gate {
            last: AtomicU64::new(u64::MAX),
            warm,
            ..Default::default()
        })
    }
    /// Call on the loop thread at the start of each sampled callback.
    fn sample(&self) {
        let now = count();
        let prev = self.last.swap(now, SeqCst);
        let n = self.samples.fetch_add(1, SeqCst);
        if prev != u64::MAX && n >= self.warm {
            self.max.fetch_max(now - prev, SeqCst);
        }
    }
    fn max(&self) -> u64 {
        self.max.load(SeqCst)
    }
    fn measured(&self) -> usize {
        self.samples.load(SeqCst).saturating_sub(self.warm)
    }
}

fn one_loop() -> Options {
    Options::default()
        .loops(1)
        .shutdown_timeout(Duration::from_millis(500))
}

// --- read dispatch (ZST protocol, idle timeout re-link on every read) --------------

struct ReadGate(Arc<Gate>);
impl Protocol for ReadGate {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_idle_timeout(Some(Duration::from_secs(30)));
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {
        self.0.sample();
    }
}

#[test]
fn gate_read_dispatch_and_idle_relink_allocate_nothing() {
    let g = Gate::new(20);
    let g2 = g.clone();
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(move |_, _| Box::new(ReadGate(g2.clone()))),
    );
    let mut c = srv.connect();
    for _ in 0..520 {
        c.write_all(b"0123456789abcdef").unwrap();
        std::thread::sleep(Duration::from_micros(300));
    }
    assert!(wait_until(Duration::from_secs(5), || g.measured() >= 100));
    eprintln!(
        "GATE read dispatch + idle re-link: max allocs between reads = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert_eq!(g.max(), 0);
    drop(c);
    srv.stop();
}

// --- direct write hit (exact, inside one callback) ------------------------------------

struct DirectGate {
    worst: Arc<AtomicU64>,
    hits: Arc<AtomicUsize>,
}
impl Protocol for DirectGate {
    fn data_received(&mut self, t: &mut Transport<'_>, d: &mut [u8]) {
        let before = count();
        t.write(d);
        let delta = count() - before;
        self.worst.fetch_max(delta, SeqCst);
        if t.pending_write() == 0 {
            self.hits.fetch_add(1, SeqCst);
        }
    }
}

#[test]
fn gate_direct_write_hit_allocates_nothing() {
    let worst = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let (w2, h2) = (worst.clone(), hits.clone());
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(move |_, _| {
            Box::new(DirectGate {
                worst: w2.clone(),
                hits: h2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    for _ in 0..300 {
        c.write_all(b"ping-ping-ping!!").unwrap();
        let _ = read_n(&mut c, 16);
    }
    eprintln!(
        "GATE direct write hit: max allocs inside write = {} ({} direct hits)",
        worst.load(SeqCst),
        hits.load(SeqCst)
    );
    assert!(hits.load(SeqCst) >= 250);
    assert_eq!(worst.load(SeqCst), 0);
    let m = srv.eng.metrics()[0].clone();
    assert!(m.write_direct_hit >= 250);
    drop(c);
    srv.stop();
}

// --- buffered write + flush, once the ring has its slab --------------------------------

struct BufferedGate {
    gate: Arc<Gate>,
    buf: Vec<u8>,
    buffered_rounds: Arc<AtomicUsize>,
}
impl Protocol for BufferedGate {
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        self.gate.sample();
        t.write(&self.buf);
        if t.pending_write() > 0 {
            self.buffered_rounds.fetch_add(1, SeqCst);
        }
    }
}

#[test]
fn gate_buffered_write_and_flush_allocate_nothing_after_warmup() {
    let gate = Gate::new(6);
    let rounds = Arc::new(AtomicUsize::new(0));
    let (g2, r2) = (gate.clone(), rounds.clone());
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default().socket_buffers(0, 4096),
        factory(move |_, _| {
            Box::new(BufferedGate {
                gate: g2.clone(),
                buf: vec![7u8; 64 << 10],
                buffered_rounds: r2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    for _ in 0..60 {
        c.write_all(b"x").unwrap();
        let got = read_n(&mut c, 64 << 10);
        assert!(got.iter().all(|b| *b == 7));
    }
    eprintln!(
        "GATE buffered write+flush: max allocs between rounds = {} ({} rounds, {} buffered, writev calls {})",
        gate.max(),
        gate.measured(),
        rounds.load(SeqCst),
        srv.eng.metrics()[0].writev
    );
    assert!(
        rounds.load(SeqCst) >= 50,
        "rounds must actually exercise the ring"
    );
    assert!(srv.eng.metrics()[0].writev >= 50);
    assert_eq!(gate.max(), 0);
    drop(c);
    srv.stop();
}

// --- timer fire: Transport::call_later tokens ----------------------------------------------

struct TimerGate {
    gate: Arc<Gate>,
    left: u32,
}
impl Protocol for TimerGate {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.call_later(Duration::from_millis(1), 1);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        self.gate.sample();
        if self.left > 0 {
            self.left -= 1;
            // Alternate between a wheel timer and a zero-delay (due-list) timer.
            let d = if token % 2 == 0 {
                Duration::ZERO
            } else {
                Duration::from_millis(1)
            };
            t.call_later(d, token + 1);
        }
    }
}

#[test]
fn gate_conn_timer_fire_allocates_nothing() {
    let gate = Gate::new(20);
    let g2 = gate.clone();
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(move |_, _| {
            Box::new(TimerGate {
                gate: g2.clone(),
                left: 400,
            })
        }),
    );
    let c = srv.connect();
    assert!(wait_until(Duration::from_secs(10), || gate.measured() >= 300));
    eprintln!(
        "GATE conn timer fire (+re-arm, wheel and zero-delay): max allocs = {} ({} samples)",
        gate.max(),
        gate.measured()
    );
    assert_eq!(gate.max(), 0);
    drop(c);
    srv.stop();
}

// --- idle timer fire -----------------------------------------------------------------------

struct IdleGate {
    before: Arc<AtomicU64>,
    delta: Arc<AtomicU64>,
    done: Arc<AtomicUsize>,
}
impl Protocol for IdleGate {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_idle_timeout(Some(Duration::from_millis(20)));
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {
        self.before.store(count(), SeqCst);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        assert!(matches!(r, Some(Error::IdleTimeout)));
        self.delta
            .fetch_max(count() - self.before.load(SeqCst), SeqCst);
        self.done.fetch_add(1, SeqCst);
    }
}

#[test]
fn gate_idle_timer_fire_and_close_allocate_nothing() {
    let (before, delta, done) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let (b2, d2, n2) = (before.clone(), delta.clone(), done.clone());
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(move |_, _| {
            Box::new(IdleGate {
                before: b2.clone(),
                delta: d2.clone(),
                done: n2.clone(),
            })
        }),
    );
    // The first connection warms the slot; later ones are measured.
    for _ in 0..8 {
        let mut c = srv.connect();
        c.write_all(b"x").unwrap();
        let n = done.load(SeqCst);
        assert!(wait_until(Duration::from_secs(5), || done.load(SeqCst) > n));
        if done.load(SeqCst) == 1 {
            delta.store(0, SeqCst); // discard warm-up
        }
    }
    eprintln!(
        "GATE idle timer fire -> abort -> close -> connection_lost: max allocs = {}",
        delta.load(SeqCst)
    );
    assert_eq!(delta.load(SeqCst), 0);
    srv.stop();
}

// --- call_from_thread drain ---------------------------------------------------------------

static TASK_GATE: AtomicU64 = AtomicU64::new(u64::MAX);
static TASK_MAX: AtomicU64 = AtomicU64::new(0);
static TASK_N: AtomicUsize = AtomicUsize::new(0);

fn task_sample() {
    let now = count();
    let prev = TASK_GATE.swap(now, SeqCst);
    let n = TASK_N.fetch_add(1, SeqCst);
    if prev != u64::MAX && n >= 50 {
        TASK_MAX.fetch_max(now - prev, SeqCst);
    }
}

#[test]
fn gate_call_from_thread_drain_allocates_nothing() {
    let eng = Arc::new(twine::Engine::new(one_loop()).unwrap());
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    std::thread::sleep(Duration::from_millis(50));
    let lh = eng.loop_handles()[0].clone();
    // Capture-free closures: `Box<dyn FnOnce>` of a ZST allocates nothing.
    for _ in 0..2000 {
        lh.call_from_thread(|_| task_sample()).unwrap();
        if TASK_N.load(SeqCst) % 7 == 0 {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
    assert!(wait_until(Duration::from_secs(10), || TASK_N.load(SeqCst) == 2000));
    eprintln!(
        "GATE call_from_thread drain: max allocs between tasks = {} ({} tasks)",
        TASK_MAX.load(SeqCst),
        TASK_N.load(SeqCst)
    );
    assert_eq!(TASK_MAX.load(SeqCst), 0);
    eng.stop();
    j.join().unwrap().unwrap();
}

// --- accept into an existing slot -------------------------------------------------------------

static ACC_START: AtomicU64 = AtomicU64::new(0);
static ACC_MAX: AtomicU64 = AtomicU64::new(0);
static ACC_N: AtomicUsize = AtomicUsize::new(0);

struct Zst;
impl Protocol for Zst {
    fn connection_made(&mut self, _t: &mut Transport<'_>) {
        let d = count() - ACC_START.load(SeqCst);
        if ACC_N.fetch_add(1, SeqCst) >= 3 {
            ACC_MAX.fetch_max(d, SeqCst);
        }
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

#[test]
fn gate_accept_into_existing_slot_allocates_nothing() {
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(|_, _| {
            ACC_START.store(count(), SeqCst);
            Box::new(Zst)
        }),
    );
    for _ in 0..40 {
        let c = srv.connect();
        drop(c);
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_until(Duration::from_secs(5), || ACC_N.load(SeqCst) == 40));
    eprintln!("GATE accept (factory -> slot setup -> poller add -> connection_made): max allocs = {} over {} accepts", ACC_MAX.load(SeqCst), ACC_N.load(SeqCst) - 3);
    assert_eq!(ACC_MAX.load(SeqCst), 0);
    srv.stop();
}

// --- the instrumentation itself ----------------------------------------------------------------

struct Allocating(Arc<Gate>, Mutex<Vec<Vec<u8>>>);
impl Protocol for Allocating {
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {
        self.0.sample();
        self.1.lock().unwrap().push(vec![0u8; 32]);
    }
}

/// Guards against a vacuous pass: a protocol that allocates must be seen.
#[test]
fn gate_instrumentation_detects_allocations() {
    let g = Gate::new(2);
    let g2 = g.clone();
    let mut srv = Server::start_with(
        one_loop(),
        ListenOptions::default(),
        factory(move |_, _| Box::new(Allocating(g2.clone(), Mutex::new(Vec::new())))),
    );
    let mut c = srv.connect();
    for _ in 0..20 {
        c.write_all(b"x").unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(wait_until(Duration::from_secs(5), || g.measured() >= 10));
    assert!(g.max() >= 1, "the gate must see a deliberate allocation");
    drop(c);
    srv.stop();
}
