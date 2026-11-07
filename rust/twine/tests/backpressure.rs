//! Backpressure: streaming and pull producers, the `PauseReading` proxy, boxed
//! producers, `write_buffer_limit` and `write_blocked`.

mod common;

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{
    factory, ConnId, Error, ListenOptions, Options, Producer, Protocol, PushProducer, Transport,
};

// --- fast periodic byte pattern (per-byte checks are too slow in debug builds) ---------

const PERIOD: usize = 65536;

fn block() -> &'static [u8] {
    static B: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    B.get_or_init(|| (0..PERIOD).map(pattern).collect())
}

/// `n` bytes of the stream starting at stream offset `off`.
fn fill_pat(off: usize, n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n);
    while v.len() < n {
        let o = (off + v.len()) % PERIOD;
        let seg = (PERIOD - o).min(n - v.len());
        v.extend_from_slice(&block()[o..o + seg]);
    }
    v
}

/// Asserts `got` equals the stream bytes starting at offset `off`.
fn check_pat(off: usize, got: &[u8]) {
    let mut i = 0;
    while i < got.len() {
        let o = (off + i) % PERIOD;
        let seg = (PERIOD - o).min(got.len() - i);
        assert!(
            got[i..i + seg] == block()[o..o + seg],
            "stream differs near byte {}",
            off + i
        );
        i += seg;
    }
}

const HIGH: usize = 64 << 10;
const LOW: usize = 16 << 10;

fn small_buf_opts() -> (Options, ListenOptions) {
    (
        Options::default()
            .loops(1)
            .write_high_water(HIGH)
            .write_low_water(LOW)
            // A missed FIN must not push the `lost == 1` waits past their budget.
            .close_linger(Duration::from_millis(500)),
        ListenOptions::default().socket_buffers(0, 4096),
    )
}

// --- (a) push producer ------------------------------------------------------------

#[derive(Debug, PartialEq, Clone, Copy)]
enum Ev {
    Pause(usize),
    Resume(usize),
}

#[derive(Default)]
struct PushLog {
    events: Mutex<Vec<Ev>>,
    lost: AtomicUsize,
}

struct Pusher {
    sent: usize,
    total: usize,
    log: Arc<PushLog>,
}

impl Pusher {
    fn pump(&mut self, t: &mut Transport<'_>) {
        while self.sent < self.total && !t.write_blocked() {
            let n = (64 << 10).min(self.total - self.sent);
            let chunk: Vec<u8> = fill_pat(self.sent, n);
            t.write(&chunk);
            self.sent += n;
        }
        if self.sent == self.total {
            t.unregister_producer();
            t.lose_connection();
        }
    }
}

impl Protocol for Pusher {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.register_producer(Producer::This, true);
        self.pump(t);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        let p = t.pending_write();
        self.log.events.lock().unwrap().push(Ev::Pause(p));
    }
    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        let p = t.pending_write();
        self.log.events.lock().unwrap().push(Ev::Resume(p));
        self.pump(t);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.log.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn push_producer_alternates_strictly_and_is_byte_exact() {
    let log = Arc::new(PushLog::default());
    let l2 = log.clone();
    let total = 4 << 20;
    let (opts, lopts) = small_buf_opts();
    let mut srv = Server::start_with(
        opts,
        lopts,
        factory(move |_, _| {
            Box::new(Pusher {
                sent: 0,
                total,
                log: l2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    let mut got = 0usize;
    let mut buf = vec![0u8; 16 << 10];
    loop {
        let n = c.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        check_pat(got, &buf[..n]);
        got += n;
        std::thread::sleep(Duration::from_micros(100));
    }
    assert_eq!(got, total);
    drop(c); // lose_connection waits for the peer to close
    assert!(wait_until(Duration::from_secs(5), || log.lost.load(SeqCst) == 1));
    let ev = log.events.lock().unwrap().clone();
    assert!(
        ev.len() >= 4,
        "slow reader must trigger repeated backpressure: {ev:?}"
    );
    for (i, e) in ev.iter().enumerate() {
        match (i % 2, e) {
            (0, Ev::Pause(p)) => assert!(*p > HIGH, "pause at {p} <= high water"),
            (1, Ev::Resume(p)) => assert!(*p <= LOW, "resume at {p} > low water"),
            _ => panic!("events must alternate pause, resume, ...: {ev:?}"),
        }
    }
    srv.stop();
}

// --- (b) pull producer ------------------------------------------------------------

#[derive(Default)]
struct PullLog {
    resumes: AtomicUsize,
    lost: AtomicUsize,
}

struct Puller {
    left: usize,
    sent: usize,
    chunk: usize,
    log: Arc<PullLog>,
}

impl Protocol for Puller {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.register_producer(Producer::This, false);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        self.log.resumes.fetch_add(1, SeqCst);
        let data: Vec<u8> = fill_pat(self.sent, self.chunk);
        self.sent += self.chunk;
        t.write(&data);
        self.left -= 1;
        if self.left == 0 {
            t.unregister_producer();
            t.lose_connection();
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.log.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn pull_producer_resumes_once_immediately_and_once_per_drain() {
    let log = Arc::new(PullLog::default());
    let l2 = log.clone();
    let (chunk, chunks) = (1 << 20, 3);
    let (opts, lopts) = small_buf_opts();
    let mut srv = Server::start_with(
        opts.write_buffer_limit(32 << 20),
        lopts,
        factory(move |_, _| {
            Box::new(Puller {
                left: chunks,
                sent: 0,
                chunk,
                log: l2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    // Immediately: one resume, then nothing while the 1 MiB chunk is draining.
    assert!(wait_until(Duration::from_secs(5), || log
        .resumes
        .load(SeqCst)
        == 1));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        log.resumes.load(SeqCst),
        1,
        "no resume while the ring is not drained"
    );
    let total = chunk * chunks;
    let mut got = 0usize;
    let mut buf = vec![0u8; 256 << 10];
    while got < total {
        let n = c.read(&mut buf).unwrap();
        assert_ne!(n, 0, "early EOF at {got}");
        check_pat(got, &buf[..n]);
        got += n;
    }
    assert_eq!(read_to_end_lossy(&mut c).len(), 0);
    drop(c); // lose_connection waits for the peer to close
    assert!(wait_until(Duration::from_secs(5), || log.lost.load(SeqCst) == 1));
    assert_eq!(
        log.resumes.load(SeqCst),
        chunks,
        "one resume for the registration plus one per drain that follows a chunk"
    );
    srv.stop();
}

// --- (c) PauseReading proxy ---------------------------------------------------------

#[derive(Default)]
struct Proxy {
    b_id: Mutex<Option<ConnId>>,
}

/// The slow downstream connection; only a sink for the proxy's writes.
struct Down {
    px: Arc<Proxy>,
}
impl Protocol for Down {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        *self.px.b_id.lock().unwrap() = Some(t.id());
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

/// The upstream connection: forwards everything to B.
struct Up {
    b: ConnId,
}
impl Protocol for Up {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let me = t.id();
        t.loop_mut()
            .with_protocol::<Down, _>(self.b, |_, bt| {
                bt.register_producer(Producer::PauseReading(me), true)
            })
            .unwrap();
    }
    fn data_received(&mut self, t: &mut Transport<'_>, d: &mut [u8]) {
        t.loop_mut()
            .with_protocol::<Down, _>(self.b, |_, bt| bt.write(d))
            .unwrap();
    }
}

#[test]
fn pause_reading_proxy_stalls_and_resumes_upstream() {
    let px = Arc::new(Proxy::default());
    let (p1, p2) = (px.clone(), px.clone());
    // A larger send buffer than elsewhere keeps the 32 MiB drain fast.
    let (opts, _) = small_buf_opts();
    let lopts = ListenOptions::default().socket_buffers(0, 64 << 10);
    let mut srv = Server::start_with(
        opts,
        lopts,
        factory(move |_, _| {
            // The downstream connection is the first to arrive; later ones are upstream.
            match *p1.b_id.lock().unwrap() {
                None => Box::new(Down { px: p1.clone() }) as Box<dyn Protocol>,
                Some(b) => Box::new(Up { b }),
            }
        }),
    );
    let _ = p2;
    let mut b = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || px
        .b_id
        .lock()
        .unwrap()
        .is_some()));
    let mut a = srv.connect();

    let total = 32usize << 20;
    let sent = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let (s2, d2) = (sent.clone(), done.clone());
    let writer = std::thread::spawn(move || {
        let mut off = 0usize;
        while off < total {
            let n = (16 << 10).min(total - off);
            let chunk: Vec<u8> = fill_pat(off, n);
            if a.write_all(&chunk).is_err() {
                return;
            }
            off += n;
            s2.store(off, SeqCst);
        }
        d2.store(true, SeqCst);
    });

    // A stalls well short of the total while B is not reading: wait for the
    // counter to stay unchanged for 500 ms.
    let mut last = usize::MAX;
    let mut stable = 0;
    let t0 = std::time::Instant::now();
    while stable < 10 {
        assert!(
            t0.elapsed() < Duration::from_secs(20),
            "sender never stalled"
        );
        std::thread::sleep(Duration::from_millis(50));
        let now = sent.load(SeqCst);
        if now == last {
            stable += 1;
        } else {
            stable = 0;
            last = now;
        }
    }
    let stalled_at = sent.load(SeqCst);
    assert!(
        stalled_at < total,
        "A must stall while B is slow ({stalled_at})"
    );
    assert!(!done.load(SeqCst));

    // B drains; A resumes and finishes; every byte arrives in order.
    let mut got = 0usize;
    let mut buf = vec![0u8; 64 << 10];
    while got < total {
        let n = b.read(&mut buf).unwrap();
        assert_ne!(n, 0, "early EOF at {got}");
        check_pat(got, &buf[..n]);
        got += n;
    }
    assert!(
        sent.load(SeqCst) > stalled_at,
        "A continued after B drained"
    );
    writer.join().unwrap();
    assert!(done.load(SeqCst));
    srv.stop();
}

// --- (d) boxed producer ---------------------------------------------------------------

#[derive(Default)]
struct FileStats {
    resumes: AtomicUsize,
    stops: AtomicUsize,
    lost: AtomicUsize,
}

struct FileSender {
    off: usize,
    st: Arc<FileStats>,
}

impl PushProducer for FileSender {
    fn pause_producing(&mut self, _c: &mut Transport<'_>) {}
    fn resume_producing(&mut self, c: &mut Transport<'_>) {
        self.st.resumes.fetch_add(1, SeqCst);
        let n = 256 << 10;
        let chunk: Vec<u8> = fill_pat(self.off, n);
        self.off += n;
        c.write(&chunk);
    }
    fn stop_producing(&mut self, _c: &mut Transport<'_>) {
        self.st.stops.fetch_add(1, SeqCst);
    }
}

struct SendsFile {
    st: Arc<FileStats>,
}
impl Protocol for SendsFile {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.register_producer(
            Producer::Boxed(Box::new(FileSender {
                off: 0,
                st: self.st.clone(),
            })),
            false,
        );
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        // Twisted order: the producer is stopped before connection_lost.
        assert_eq!(self.st.stops.load(SeqCst), 1);
        self.st.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn boxed_producer_streams_and_is_stopped_on_close() {
    let st = Arc::new(FileStats::default());
    let s2 = st.clone();
    let (opts, lopts) = small_buf_opts();
    let mut srv = Server::start_with(
        opts,
        lopts,
        factory(move |_, _| Box::new(SendsFile { st: s2.clone() })),
    );
    let mut c = srv.connect();
    let total = 2 << 20;
    let mut got = 0usize;
    let mut buf = vec![0u8; 64 << 10];
    while got < total {
        let n = c.read(&mut buf).unwrap();
        assert_ne!(n, 0);
        check_pat(got, &buf[..n]);
        got += n;
    }
    assert!(
        st.resumes.load(SeqCst) >= 2,
        "producer is pulled again after each drain"
    );
    assert_eq!(st.stops.load(SeqCst), 0);
    drop(c);
    assert!(wait_until(Duration::from_secs(5), || st.lost.load(SeqCst) == 1));
    assert_eq!(
        st.stops.load(SeqCst),
        1,
        "stop_producing exactly once on close"
    );
    srv.stop();
}

// --- (e) write_buffer_limit ------------------------------------------------------------

struct Flood {
    reasons: Arc<Mutex<Vec<String>>>,
}
impl Protocol for Flood {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.write(&vec![0x5Au8; 32 << 20]);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        let s = match r {
            Some(Error::WriteBufferFull) => "WriteBufferFull".to_string(),
            Some(e) => format!("{e:?}"),
            None => "clean".to_string(),
        };
        self.reasons.lock().unwrap().push(s);
    }
}

#[test]
fn write_buffer_limit_aborts_with_write_buffer_full() {
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let r2 = reasons.clone();
    let (opts, lopts) = small_buf_opts();
    let mut srv = Server::start_with(
        opts.write_buffer_limit(1 << 20),
        lopts,
        factory(move |_, _| {
            Box::new(Flood {
                reasons: r2.clone(),
            })
        }),
    );
    let c = srv.connect(); // never reads
    assert!(wait_until(Duration::from_secs(10), || !reasons
        .lock()
        .unwrap()
        .is_empty()));
    assert_eq!(
        *reasons.lock().unwrap(),
        vec!["WriteBufferFull".to_string()]
    );
    drop(c);
    srv.stop();
}

// --- (f) write_blocked ------------------------------------------------------------------

#[derive(Default)]
struct Blocked {
    before: AtomicBool,
    after: AtomicBool,
    pending: AtomicUsize,
    done: AtomicBool,
}

struct Prober {
    st: Arc<Blocked>,
}
impl Protocol for Prober {
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        self.st.before.store(t.write_blocked(), SeqCst);
        t.write(&vec![1u8; 16 << 20]);
        self.st.after.store(t.write_blocked(), SeqCst);
        self.st.pending.store(t.pending_write(), SeqCst);
        self.st.done.store(true, SeqCst);
    }
}

#[test]
fn write_blocked_is_true_right_after_exceeding_high_water() {
    let st = Arc::new(Blocked::default());
    let s2 = st.clone();
    let (opts, lopts) = small_buf_opts();
    let mut srv = Server::start_with(
        opts.write_buffer_limit(64 << 20),
        lopts,
        factory(move |_, _| Box::new(Prober { st: s2.clone() })),
    );
    let mut c = srv.connect();
    c.write_all(b"go").unwrap();
    assert!(wait_until(Duration::from_secs(5), || st.done.load(SeqCst)));
    assert!(!st.before.load(SeqCst));
    assert!(
        st.after.load(SeqCst),
        "write_blocked must see the fresh backlog"
    );
    assert!(st.pending.load(SeqCst) > HIGH);
    drop(c);
    srv.stop();
}
