//! Partial writes, close semantics, pause/resume, cross-connection writes,
//! producers and listener lifecycle.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{
    factory, ConnId, Engine, Error, ListenOptions, Options, Producer, Protocol, Transport,
    TransportHandle,
};

// --- partial writes -----------------------------------------------------------

struct Blaster {
    total: usize,
    lose: bool,
}

impl Protocol for Blaster {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let buf: Vec<u8> = (0..self.total).map(pattern).collect();
        t.write(&buf);
        if self.lose {
            t.lose_connection();
        }
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _data: &mut [u8]) {}
}

fn partial_writes(total: usize, chunk: usize, limit: usize) {
    let opts = Options::default().loops(1).write_buffer_limit(limit);
    let lopts = ListenOptions::default().socket_buffers(0, 4096);
    let mut srv = Server::start_with(
        opts,
        lopts,
        factory(move |_, _| Box::new(Blaster { total, lose: true })),
    );
    let mut s = srv.connect();
    // A tiny receive window as well, and slow reads.
    let mut got = 0usize;
    let mut buf = vec![0u8; chunk];
    loop {
        let n = s.read(&mut buf).expect("read");
        if n == 0 {
            break;
        }
        for (i, b) in buf[..n].iter().enumerate() {
            assert_eq!(*b, pattern(got + i), "byte {} differs", got + i);
        }
        got += n;
        if got % (1 << 20) < n {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(got, total);
    drop(s);
    srv.stop();
}

#[test]
fn partial_writes_8mib_byte_exact() {
    partial_writes(8 << 20, 64 << 10, 16 << 20);
}

#[test]
#[ignore = "100 MiB soak; run with --ignored"]
fn partial_writes_100mib_byte_exact() {
    partial_writes(100 << 20, 256 << 10, 256 << 20);
}

// --- lose_connection / abort ----------------------------------------------------

#[derive(Default)]
struct Life {
    made: AtomicUsize,
    lost: AtomicUsize,
    lost_with_err: AtomicUsize,
}

struct Lifecycle {
    life: Arc<Life>,
    on_data_abort: bool,
    greeting: Option<&'static [u8]>,
}

impl Protocol for Lifecycle {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.life.made.fetch_add(1, SeqCst);
        if let Some(g) = self.greeting {
            t.write(g);
            t.lose_connection();
        }
    }
    fn data_received(&mut self, t: &mut Transport<'_>, _data: &mut [u8]) {
        if self.on_data_abort {
            t.write(b"you will not see this"); // buffered/direct, then discarded or sent: either is fine
            t.abort();
            t.write(b"after abort"); // counted no-op
        }
    }
    fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
        assert!(!t.is_open());
        self.life.lost.fetch_add(1, SeqCst);
        if reason.is_some() {
            self.life.lost_with_err.fetch_add(1, SeqCst);
        }
    }
}

#[test]
fn lose_connection_flushes_then_eof() {
    let life = Arc::new(Life::default());
    let l2 = life.clone();
    let mut srv = Server::start(
        2,
        factory(move |_, _| {
            Box::new(Lifecycle {
                life: l2.clone(),
                on_data_abort: false,
                greeting: Some(b"hello, goodbye"),
            })
        }),
    );
    for _ in 0..5 {
        let mut s = srv.connect();
        let all = read_to_end_lossy(&mut s);
        assert_eq!(all, b"hello, goodbye");
        drop(s);
    }
    assert!(wait_until(Duration::from_secs(5), || life
        .lost
        .load(SeqCst)
        == 5));
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        life.lost.load(SeqCst),
        5,
        "connection_lost exactly once per connection"
    );
    assert_eq!(
        life.lost_with_err.load(SeqCst),
        0,
        "clean close has no reason"
    );
    srv.stop();
}

#[test]
fn abort_closes_immediately_and_lost_runs_once() {
    let life = Arc::new(Life::default());
    let l2 = life.clone();
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Lifecycle {
                life: l2.clone(),
                on_data_abort: true,
                greeting: None,
            })
        }),
    );
    let mut s = srv.connect();
    let started = std::time::Instant::now();
    s.write_all(b"x").unwrap();
    let mut buf = [0u8; 64];
    let r = s.read(&mut buf);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "abort must not linger"
    );
    // The direct write before the abort may or may not arrive, but only that
    // text can: never the write issued after the abort.
    let mut seen = Vec::new();
    if let Ok(n) = r {
        seen.extend_from_slice(&buf[..n]);
    }
    // Drain to EOF/reset.
    seen.extend(read_to_end_lossy(&mut s));
    assert!(
        b"you will not see this".starts_with(&seen),
        "unexpected bytes after abort: {:?}",
        String::from_utf8_lossy(&seen)
    );
    assert!(wait_until(Duration::from_secs(5), || life
        .lost
        .load(SeqCst)
        == 1));
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(life.lost.load(SeqCst), 1);
    assert_eq!(
        life.lost_with_err.load(SeqCst),
        0,
        "abort reports connection_lost(None)"
    );
    let m = srv.eng.metrics();
    assert!(
        m.iter().map(|m| m.write_after_close).sum::<u64>() >= 1,
        "write after abort is counted"
    );
    srv.stop();
}

#[test]
fn peer_close_reports_clean_lost() {
    let life = Arc::new(Life::default());
    let l2 = life.clone();
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Lifecycle {
                life: l2.clone(),
                on_data_abort: false,
                greeting: None,
            })
        }),
    );
    let s = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || life
        .made
        .load(SeqCst)
        == 1));
    drop(s);
    assert!(wait_until(Duration::from_secs(5), || life
        .lost
        .load(SeqCst)
        == 1));
    assert_eq!(life.lost_with_err.load(SeqCst), 0);
    srv.stop();
}

// --- pause / resume reading -----------------------------------------------------

#[derive(Default)]
struct Pause {
    got: AtomicUsize,
    handle: Mutex<Option<TransportHandle>>,
}

struct Pauser(Arc<Pause>);

impl Protocol for Pauser {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        *self.0.handle.lock().unwrap() = Some(t.handle());
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        self.0.got.fetch_add(data.len(), SeqCst);
        if data.contains(&b'P') {
            t.pause_reading();
        }
    }
}

#[test]
fn pause_and_resume_reading_with_immediate_read() {
    let st = Arc::new(Pause::default());
    let s2 = st.clone();
    let mut srv = Server::start(1, factory(move |_, _| Box::new(Pauser(s2.clone()))));
    let mut c = srv.connect();
    c.write_all(b"P").unwrap();
    assert!(wait_until(Duration::from_secs(5), || st.got.load(SeqCst) == 1));
    // Data arriving while paused must not be delivered.
    c.write_all(&[7u8; 1000]).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(st.got.load(SeqCst), 1, "no data_received while paused");
    c.write_all(&[8u8; 500]).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(st.got.load(SeqCst), 1);
    // Resume: the edge was consumed while paused, so the resume must read.
    let h = st.handle.lock().unwrap().clone().unwrap();
    h.resume_reading();
    assert!(
        wait_until(Duration::from_secs(5), || st.got.load(SeqCst) == 1501),
        "got {}",
        st.got.load(SeqCst)
    );
    // Reading continues normally after resume.
    c.write_all(&[9u8; 10]).unwrap();
    assert!(wait_until(Duration::from_secs(5), || st.got.load(SeqCst) == 1511));
    srv.stop();
}

// --- cross-connection writes ----------------------------------------------------

static HUB: Mutex<Vec<ConnId>> = Mutex::new(Vec::new());
static HUB_HANDLES: Mutex<Vec<TransportHandle>> = Mutex::new(Vec::new());

struct Hub;
impl Protocol for Hub {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        HUB.lock().unwrap().push(t.id());
        HUB_HANDLES.lock().unwrap().push(t.handle());
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        let me = t.id();
        let others: Vec<ConnId> = HUB
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|i| *i != me)
            .collect();
        for o in others {
            if let Some(mut other) = t.loop_mut().conn(o) {
                other.write(data);
            }
        }
    }
}

#[test]
fn same_loop_cross_connection_write_and_handle_from_thread() {
    HUB.lock().unwrap().clear();
    HUB_HANDLES.lock().unwrap().clear();
    let mut srv = Server::start(1, factory(|_, _| Box::new(Hub)));
    let mut a = srv.connect();
    let mut b = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || HUB
        .lock()
        .unwrap()
        .len()
        == 2));
    a.write_all(b"from-a").unwrap();
    assert_eq!(read_n(&mut b, 6), b"from-a");
    b.write_all(b"from-b!").unwrap();
    assert_eq!(read_n(&mut a, 7), b"from-b!");
    // TransportHandle::write from another thread.
    let ha = HUB_HANDLES.lock().unwrap()[0].clone();
    let t = std::thread::spawn(move || {
        ha.write(b"thread-write".to_vec());
        ha.write("static-str");
    });
    t.join().unwrap();
    // Connection 0 is whichever accepted first; one of a/b receives it.
    let first_is_a = {
        a.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut buf = [0u8; 22];
        match a.read_exact(&mut buf) {
            Ok(()) => {
                assert_eq!(&buf, b"thread-writestatic-str");
                true
            }
            Err(_) => false,
        }
    };
    if !first_is_a {
        assert_eq!(read_n(&mut b, 22), b"thread-writestatic-str");
    }
    srv.stop();
}

// --- streaming producer ---------------------------------------------------------

#[derive(Default)]
struct ProdStats {
    pauses: AtomicUsize,
    resumes: AtomicUsize,
    lost: AtomicUsize,
}

struct Prod {
    sent: usize,
    total: usize,
    st: Arc<ProdStats>,
}

impl Prod {
    fn pump(&mut self, t: &mut Transport<'_>) {
        let chunk = [0xA5u8; 16 << 10];
        while self.sent < self.total && !t.write_blocked() {
            let n = chunk.len().min(self.total - self.sent);
            t.write(&chunk[..n]);
            self.sent += n;
        }
        if self.sent == self.total {
            t.unregister_producer();
            t.lose_connection();
        }
    }
}

impl Protocol for Prod {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.register_producer(Producer::This, true);
        self.pump(t);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn pause_producing(&mut self, _t: &mut Transport<'_>) {
        self.st.pauses.fetch_add(1, SeqCst);
    }
    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        self.st.resumes.fetch_add(1, SeqCst);
        self.pump(t);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.st.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn streaming_producer_pauses_and_resumes_in_order() {
    let st = Arc::new(ProdStats::default());
    let s2 = st.clone();
    let total = 4 << 20;
    let opts = Options::default()
        .loops(1)
        .write_high_water(64 << 10)
        .write_low_water(16 << 10);
    let mut srv = Server::start_with(
        opts,
        ListenOptions::default().socket_buffers(0, 4096),
        factory(move |_, _| {
            Box::new(Prod {
                sent: 0,
                total,
                st: s2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    let mut got = 0;
    let mut buf = vec![0u8; 32 << 10];
    loop {
        let n = c.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        assert!(buf[..n].iter().all(|b| *b == 0xA5));
        got += n;
        std::thread::sleep(Duration::from_micros(200));
    }
    assert_eq!(got, total);
    drop(c);
    assert!(wait_until(Duration::from_secs(5), || st.lost.load(SeqCst) == 1));
    let (p, r) = (st.pauses.load(SeqCst), st.resumes.load(SeqCst));
    assert!(p >= 1, "slow reader must trigger backpressure");
    assert_eq!(p, r, "every pause is followed by exactly one resume");
    srv.stop();
}

// --- listener lifecycle ---------------------------------------------------------

struct Echo;
impl Protocol for Echo {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

fn echo_once(addr: std::net::SocketAddr) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(b"ping").unwrap();
    assert_eq!(read_n(&mut s, 4), b"ping");
}

#[test]
fn listen_after_run_works() {
    let eng = Arc::new(Engine::new(Options::default().loops(2)).unwrap());
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    std::thread::sleep(Duration::from_millis(50)); // loops are up and idle
    let lst = eng
        .listen_tcp(
            "127.0.0.1:0",
            factory(|_, _| Box::new(Echo)),
            ListenOptions::default(),
        )
        .unwrap();
    echo_once(lst.addr());
    eng.stop();
    j.join().unwrap().unwrap();
}

#[test]
fn listen_before_run_works_and_stop_before_run_closes() {
    let eng = Engine::new(Options::default().loops(2)).unwrap();
    let lst = eng
        .listen_tcp(
            "127.0.0.1:0",
            factory(|_, _| Box::new(Echo)),
            ListenOptions::default(),
        )
        .unwrap();
    let addr = lst.addr();
    eng.stop();
    eng.run().unwrap(); // returns immediately
    assert!(eng.is_stopped());
    assert!(
        TcpStream::connect(addr).is_err(),
        "listener sockets are released"
    );
    assert!(matches!(
        eng.listen_tcp(
            "127.0.0.1:0",
            factory(|_, _| Box::new(Echo)),
            ListenOptions::default()
        ),
        Err(Error::EngineStopped)
    ));
}

#[test]
fn listener_close_stops_new_accepts_only() {
    let mut srv = Server::start(2, factory(|_, _| Box::new(Echo)));
    let mut existing = srv.connect();
    existing.write_all(b"one").unwrap();
    assert_eq!(read_n(&mut existing, 3), b"one");
    srv.lst.close();
    assert!(
        wait_until(Duration::from_secs(5), || TcpStream::connect(srv.addr)
            .is_err()),
        "new connections must be refused after close"
    );
    // The existing connection is unaffected.
    existing.write_all(b"two").unwrap();
    assert_eq!(read_n(&mut existing, 3), b"two");
    srv.stop();
}

#[test]
fn run_twice_is_an_error_and_wait_returns() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(Echo)));
    std::thread::sleep(Duration::from_millis(30));
    assert!(srv.eng.run().is_err());
    let h = srv.eng.handle();
    let w = std::thread::spawn(move || h.wait());
    srv.stop();
    w.join().unwrap();
}

// --- hangup must survive the per-event read cap -----------------------------------

#[derive(Default)]
struct HangupState {
    lost: AtomicUsize,
    got: AtomicUsize,
    stalled: AtomicUsize,
}

struct StallOnce(Arc<HangupState>);

impl Protocol for StallOnce {
    fn data_received(&mut self, _t: &mut Transport<'_>, data: &mut [u8]) {
        // Stall the loop once so the rest of the data and the FIN are queued in
        // the socket before the next event.
        if self.0.stalled.fetch_add(1, SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(300));
        }
        self.0.got.fetch_add(data.len(), SeqCst);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {
        self.0.lost.fetch_add(1, SeqCst);
    }
}

/// A hangup is reported once by edge-triggered polling. When the event that
/// carries it also carries more data than the per-event read cap allows, the
/// re-armed read next iteration must keep reading to EOF instead of stopping at
/// the final short read, which would leave the conn open until shutdown.
#[test]
fn hangup_survives_read_cap() {
    let st = Arc::new(HangupState::default());
    let s2 = st.clone();
    let opts = Options::default()
        .loops(1)
        .read_buffer_size(4096)
        .max_reads_per_event(2)
        .watchdog(None)
        .shutdown_timeout(Duration::from_millis(500));
    let mut srv = Server::start_with(
        opts,
        ListenOptions::default(),
        factory(move |_, _| Box::new(StallOnce(s2.clone()))),
    );
    let mut c = srv.connect();
    let total = 100 * 1024 + 100; // ends on a short read
    c.write_all(&[1u8; 4096]).unwrap();
    std::thread::sleep(Duration::from_millis(50)); // loop is inside the stalled callback
    c.write_all(&vec![1u8; total - 4096]).unwrap();
    drop(c);
    assert!(
        wait_until(Duration::from_secs(3), || st.lost.load(SeqCst) == 1),
        "connection_lost not delivered: got {} of {total} bytes",
        st.got.load(SeqCst)
    );
    assert_eq!(st.got.load(SeqCst), total);
    srv.stop();
}
