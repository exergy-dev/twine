//! tunsink-rs: Rust port of examples/tunsink. Accepts TCP clients and writes
//! every length-prefixed frame (4-byte big-endian length + one IP packet) to a
//! TUN device and a Kafka topic.
//!
//! Modes: `channel` copies each frame once and hands it to sink threads that
//! write the TUN and produce to Kafka; `direct` writes the TUN on the loop from
//! the shared read buffer (no copy) and only Kafka goes through the sinks.
//! Kafka is disabled when `-brokers` is empty.
//!
//! With `-aes` every frame body is `nonce(12) || ciphertext || tag(16)`
//! (AES-256-GCM) and is decrypted on the loop before the TUN write and the
//! sink; the TUN and Kafka receive the plaintext. Frames that fail to decrypt
//! are counted in `decrypt_err` and skipped.

mod aes;
mod args;
mod kafka;
mod tun;

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender, TrySendError};
use twine::{ConnId, Engine, Error, ListenOptions, LoopHandle, Transport};
use twine_proto::lengthprefix::{FrameFormat, FrameHandler, LengthPrefixed};

use aes::Aes;
use kafka::Kafka;

const MAX_PACKET: usize = 65535;
const SINK_CAP: usize = 4096;

pub struct Stats {
    tun_fd: i32,
    frames: AtomicU64,
    bytes: AtomicU64,
    tun_err: AtomicU64,
    pub kafka_ok: AtomicU64,
    pub kafka_err: AtomicU64,
    pauses: AtomicU64,
    decrypt_err: AtomicU64,
    /// Guards the single "first produce error" log line.
    pub first_err: std::sync::Once,
}

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, m: &log::Metadata<'_>) -> bool {
        m.level() <= log::max_level()
    }
    fn log(&self, r: &log::Record<'_>) {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let s = d.as_secs() % 86400;
        let _ = writeln!(
            std::io::stderr().lock(),
            "{:02}:{:02}:{:02} {}",
            s / 3600,
            s / 60 % 60,
            s % 60,
            r.args()
        );
    }
    fn flush(&self) {}
}

/// A pause request: the drainer thread pushes `pending` into the sink channel
/// and then resumes the connection on its loop.
struct DrainReq {
    id: ConnId,
    pending: Arc<Mutex<Vec<Vec<u8>>>>,
    paused: Arc<AtomicBool>,
    lh: LoopHandle,
}

/// One TCP connection. Everything except `pending` is touched only on the
/// connection's loop thread.
struct Client {
    st: Arc<Stats>,
    /// None when nothing needs the sink (direct mode, Kafka off).
    tx: Option<Sender<Vec<u8>>>,
    req_tx: Sender<DrainReq>,
    direct: bool,
    /// Set/cleared only on the loop thread (shared so the drainer's loop
    /// callback can clear it).
    paused: Arc<AtomicBool>,
    /// Shared with the drainer under the mutex.
    pending: Arc<Mutex<Vec<Vec<u8>>>>,
    /// Set with `-aes`: every frame is decrypted before the TUN and the sink.
    aes: Option<Arc<Aes>>,
    /// Reused decrypt output when nothing needs an owned copy (direct mode,
    /// Kafka off).
    scratch: Vec<u8>,
}

impl Client {
    fn new(
        st: Arc<Stats>,
        tx: Option<Sender<Vec<u8>>>,
        req_tx: Sender<DrainReq>,
        direct: bool,
        aes: Option<Arc<Aes>>,
    ) -> Self {
        let scratch = if aes.is_some() && tx.is_none() {
            Vec::with_capacity(MAX_PACKET + aes::OVERHEAD)
        } else {
            Vec::new()
        };
        Client {
            st,
            tx,
            req_tx,
            direct,
            paused: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(Mutex::new(Vec::new())),
            aes,
            scratch,
        }
    }

    /// Hands an owned packet to the sink, pausing the connection when the
    /// sink is full.
    fn enqueue(&mut self, t: &mut Transport<'_>, mut pkt: Vec<u8>) {
        let Some(tx) = &self.tx else { return };
        if !self.paused.load(Relaxed) {
            match tx.try_send(pkt) {
                Ok(()) => return,
                Err(TrySendError::Disconnected(_)) => return,
                Err(TrySendError::Full(p)) => pkt = p,
            }
            // Sink is full. Stop reading from this client; the drainer thread
            // pushes the backlog and asks the loop to resume, so TCP
            // backpressure reaches the sender instead of frames being dropped.
            self.paused.store(true, Relaxed);
            self.st.pauses.fetch_add(1, Relaxed);
            t.pause_reading();
            self.pending.lock().unwrap().push(pkt);
            let _ = self.req_tx.send(DrainReq {
                id: t.id(),
                pending: self.pending.clone(),
                paused: self.paused.clone(),
                lh: t.loop_handle().clone(),
            });
            return;
        }
        // Frames still arrive from the chunk already in the read buffer while
        // paused; queue them for the drainer.
        self.pending.lock().unwrap().push(pkt);
    }
}

impl FrameHandler for Client {
    fn frame_received(&mut self, t: &mut Transport<'_>, f: &[u8]) {
        self.st.frames.fetch_add(1, Relaxed);
        self.st.bytes.fetch_add(f.len() as u64, Relaxed);
        let Some(aes) = &self.aes else {
            if self.direct && self.st.tun_fd >= 0 && !tun::write(self.st.tun_fd, f) {
                self.st.tun_err.fetch_add(1, Relaxed);
            }
            if self.tx.is_some() {
                self.enqueue(t, f.to_vec()); // f is only valid during the call
            }
            return;
        };
        // Decrypt into the destination: the owned packet for the sink (this
        // replaces the plain copy) or the reusable scratch buffer.
        let mut pkt = Vec::new();
        let dst = if self.tx.is_some() {
            &mut pkt
        } else {
            &mut self.scratch
        };
        if !aes.open_into(f, dst) {
            self.st.decrypt_err.fetch_add(1, Relaxed);
            return;
        }
        if self.direct && self.st.tun_fd >= 0 && !tun::write(self.st.tun_fd, dst) {
            self.st.tun_err.fetch_add(1, Relaxed);
        }
        if self.tx.is_some() {
            self.enqueue(t, pkt);
        }
    }

    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<Error>) {
        match reason {
            None | Some(Error::Closed) | Some(Error::Shutdown) => {}
            Some(e) => log::info!("connection lost: {e}"),
        }
    }
}

/// Pushes every pending frame into the sink channel with a blocking send.
/// Returns false when the sinks are gone.
fn push_pending(tx: &Sender<Vec<u8>>, pending: &Mutex<Vec<Vec<u8>>>) -> bool {
    loop {
        let batch = std::mem::take(&mut *pending.lock().unwrap());
        if batch.is_empty() {
            return true;
        }
        for p in batch {
            if tx.send(p).is_err() {
                return false;
            }
        }
    }
}

/// The drainer: serves pause requests FIFO. Go starts a goroutine per drain,
/// each blocked in a channel send (they queue FIFO on the channel's sendq); here
/// one thread does the same with a plain blocking `send`, then resumes the
/// connection through `call_from_thread`. Sinks are independent consumers, so
/// the blocked send always completes. Exits once `done` is set and the queue is empty.
fn drainer(
    req_rx: Receiver<DrainReq>,
    req_tx: Sender<DrainReq>,
    tx: Sender<Vec<u8>>,
    done: Arc<AtomicBool>,
) {
    // The drainer keeps a request sender itself (for resubmission), so the
    // channel never disconnects; `done` ends it once the queue is empty.
    loop {
        let req = match req_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(r) => r,
            Err(RecvTimeoutError::Timeout) => {
                if done.load(Relaxed) && req_rx.is_empty() {
                    return;
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => return,
        };
        if !push_pending(&tx, &req.pending) {
            continue; // sinks gone
        }
        let DrainReq {
            id,
            pending,
            paused,
            lh,
        } = req;
        let (rtx, p2, paused2, lh2) = (req_tx.clone(), pending.clone(), paused.clone(), lh.clone());
        let r = lh.call_from_thread(move |lp| {
            // On the loop: no concurrent appends are possible here.
            if !p2.lock().unwrap().is_empty() {
                let _ = rtx.send(DrainReq {
                    id,
                    pending: p2,
                    paused: paused2,
                    lh: lh2,
                });
                return;
            }
            paused2.store(false, Relaxed);
            // The connection may be gone.
            if let Some(mut t) = lp.conn(id) {
                t.resume_reading();
            }
        });
        if r.is_err() {
            // Loop stopped: nothing appends any more, flush what is left.
            push_pending(&tx, &pending);
        }
    }
}

fn main() {
    static L: StderrLogger = StderrLogger;
    let _ = log::set_logger(&L);
    log::set_max_level(log::LevelFilter::Info);
    let cfg = args::parse();

    // Block SIGTERM/SIGINT before any thread exists so every thread inherits
    // the mask; one dedicated thread sigwait()s for them.
    // SAFETY: sigset_t is plain data; sigemptyset/sigaddset initialise it and
    // pthread_sigmask only reads it.
    let sigset = unsafe {
        let mut s: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut s);
        libc::sigaddset(&mut s, libc::SIGTERM);
        libc::sigaddset(&mut s, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &s, std::ptr::null_mut());
        s
    };

    let mut tun_fd = -1;
    if !cfg.tun.is_empty() {
        match tun::open(&cfg.tun) {
            Ok(fd) => tun_fd = fd,
            Err(e) => {
                eprintln!("tunsink: open tun {:?}: {e}", cfg.tun);
                std::process::exit(1);
            }
        }
    }
    let st = Arc::new(Stats {
        tun_fd,
        frames: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        tun_err: AtomicU64::new(0),
        kafka_ok: AtomicU64::new(0),
        kafka_err: AtomicU64::new(0),
        pauses: AtomicU64::new(0),
        decrypt_err: AtomicU64::new(0),
        first_err: std::sync::Once::new(),
    });

    let kc: Option<Arc<Kafka>> = if cfg.brokers.is_empty() {
        None
    } else {
        match Kafka::new(&cfg.brokers, &cfg.topic, cfg.kafka_drop, st.clone()) {
            Ok(k) => Some(Arc::new(k)),
            Err(e) => {
                eprintln!("tunsink: kafka: {e}");
                std::process::exit(1);
            }
        }
    };

    let direct = cfg.direct;
    let need_sink = !direct || kc.is_some();
    let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = bounded(SINK_CAP);
    let closing = Arc::new(AtomicBool::new(false));
    let mut sink_threads = Vec::new();
    for i in 0..cfg.sinks {
        let (rx, st, kc, closing) = (rx.clone(), st.clone(), kc.clone(), closing.clone());
        sink_threads.push(
            std::thread::Builder::new()
                .name(format!("sink-{i}"))
                .spawn(move || loop {
                    match rx
                        .try_recv()
                        .map_err(|_| RecvTimeoutError::Timeout)
                        .or_else(|_| rx.recv_timeout(Duration::from_millis(50)))
                    {
                        Ok(pkt) => {
                            if !direct && st.tun_fd >= 0 && !tun::write(st.tun_fd, &pkt) {
                                st.tun_err.fetch_add(1, Relaxed);
                            }
                            if let Some(k) = &kc {
                                k.produce(&pkt);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            if closing.load(Relaxed) && rx.is_empty() {
                                break;
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                })
                .expect("spawn sink"),
        );
    }
    drop(rx);
    let (req_tx, req_rx) = unbounded::<DrainReq>();
    let drain_done = Arc::new(AtomicBool::new(false));
    let drainer_thread = {
        let (rtx, tx, done) = (req_tx.clone(), tx.clone(), drain_done.clone());
        std::thread::Builder::new()
            .name("drainer".into())
            .spawn(move || drainer(req_rx, rtx, tx, done))
            .expect("spawn drainer")
    };

    let mut b = Engine::builder()
        .loops(cfg.loops)
        .watchdog((!cfg.watchdog.is_zero()).then_some(cfg.watchdog));
    if cfg.spin >= 0 {
        b = b.spin_iterations(cfg.spin as usize);
    }
    if !cfg.pin.is_empty() {
        b = b.pin_loops(cfg.pin.clone());
    }
    let eng = match b.build() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("tunsink: {e}");
            std::process::exit(1);
        }
    };

    let cipher = cfg.aes.then(|| Arc::new(Aes::new(&cfg.key)));
    let fmt =
        FrameFormat::new().with_max_frame(MAX_PACKET + if cfg.aes { aes::OVERHEAD } else { 0 });
    let addr = if cfg.addr.starts_with(':') {
        format!("0.0.0.0{}", cfg.addr)
    } else {
        cfg.addr.clone()
    };
    let (fst, ftx, freq) = (st.clone(), tx.clone(), req_tx.clone());
    let ln = match eng.listen_tcp(
        addr.as_str(),
        twine::factory(move |_, _| {
            Box::new(LengthPrefixed::with_format(
                Client::new(
                    fst.clone(),
                    need_sink.then(|| ftx.clone()),
                    freq.clone(),
                    direct,
                    cipher.clone(),
                ),
                fmt,
            ))
        }),
        ListenOptions::default(),
    ) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("tunsink: listen {addr}: {e}");
            std::process::exit(1);
        }
    };
    log::info!(
        "tunsink: tcp{} -> tun={:?} kafka={:?} mode={} loops={} aes={}",
        ln.addr(),
        cfg.tun,
        cfg.brokers,
        if direct { "direct" } else { "channel" },
        cfg.loops,
        cfg.aes
    );

    let h = eng.handle();
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let mut sig = 0;
            // SAFETY: sigset was initialised above; sig is a valid out pointer.
            unsafe { libc::sigwait(&sigset, &mut sig) };
            h.stop();
        })
        .expect("spawn signal thread");

    let (stop_tx, stop_rx) = bounded::<()>(1);
    let stats_thread = (!cfg.stats.is_zero()).then(|| {
        let st = st.clone();
        let every = cfg.stats;
        std::thread::spawn(move || {
            let (mut lf, mut lb, mut lk) = (0u64, 0u64, 0u64);
            let mut last = Instant::now();
            while stop_rx.recv_timeout(every) == Err(RecvTimeoutError::Timeout) {
                let now = Instant::now();
                let (f, b, k) = (
                    st.frames.load(Relaxed),
                    st.bytes.load(Relaxed),
                    st.kafka_ok.load(Relaxed),
                );
                let dt = now.duration_since(last).as_secs_f64();
                eprintln!(
                    "stats frames/s={:.0} MB/s={:.1} kafka/s={:.0} pauses={} tun_err={} kafka_err={} decrypt_err={}",
                    (f - lf) as f64 / dt,
                    (b - lb) as f64 / dt / 1e6,
                    (k - lk) as f64 / dt,
                    st.pauses.load(Relaxed),
                    st.tun_err.load(Relaxed),
                    st.kafka_err.load(Relaxed),
                    st.decrypt_err.load(Relaxed)
                );
                (lf, lb, lk, last) = (f, b, k, now);
            }
        })
    });

    if let Err(e) = eng.run() {
        eprintln!("tunsink: {e}");
        std::process::exit(1);
    }
    drop(stop_tx);
    if let Some(t) = stats_thread {
        let _ = t.join();
    }
    // Drop everything holding a sender (listener factory, protocols), then ours,
    // so the sinks drain what is queued and exit.
    drop(ln);
    drop(eng);
    // The drainer finishes every queued list, then exits (all protocols died
    // with the engine, so no new requests can arrive).
    drop(req_tx);
    drain_done.store(true, Relaxed);
    let _ = drainer_thread.join();
    closing.store(true, Relaxed);
    drop(tx);
    for t in sink_threads {
        let _ = t.join();
    }
    if let Some(k) = &kc {
        k.flush(Duration::from_secs(15));
    }
    if tun_fd >= 0 {
        tun::close(tun_fd);
    }
    log::info!(
        "total frames={} bytes={} tun_err={} kafka_ok={} kafka_err={} pauses={} decrypt_err={}",
        st.frames.load(Relaxed),
        st.bytes.load(Relaxed),
        st.tun_err.load(Relaxed),
        st.kafka_ok.load(Relaxed),
        st.kafka_err.load(Relaxed),
        st.pauses.load(Relaxed),
        st.decrypt_err.load(Relaxed)
    );
}

#[cfg(test)]
mod tests {
    //! Drainer hand-off over real twine loops and loopback TCP, without TUN or
    //! Kafka: a tiny bounded sink channel and a slow (or stalled) consumer.

    use super::*;
    use std::net::{SocketAddr, TcpStream};
    use std::thread::JoinHandle;

    /// Records the connection's id and loop, then delegates to `Client`.
    struct Spy {
        inner: Client,
        seen: Arc<Mutex<Option<(ConnId, LoopHandle)>>>,
    }

    impl FrameHandler for Spy {
        fn frame_received(&mut self, t: &mut Transport<'_>, f: &[u8]) {
            self.seen
                .lock()
                .unwrap()
                .get_or_insert_with(|| (t.id(), t.loop_handle().clone()));
            self.inner.frame_received(t, f);
        }
        fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
            self.inner.connection_lost(t, reason);
        }
    }

    struct Rig {
        st: Arc<Stats>,
        eng: Arc<Engine>,
        addr: SocketAddr,
        run: Option<JoinHandle<twine::Result<()>>>,
        drainer: Option<JoinHandle<()>>,
        done: Arc<AtomicBool>,
        paused: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
        seen: Arc<Mutex<Option<(ConnId, LoopHandle)>>>,
        _ln: twine::Listener,
    }

    /// Engine + drainer wired as in `main` (channel mode, no TUN), sinking
    /// into `tx`.
    fn rig(tx: Sender<Vec<u8>>) -> Rig {
        rig_with(Some(tx), false, -1, None)
    }

    /// Engine + drainer wired as in `main`. `tx` None is direct mode with
    /// Kafka off (the drainer then gets a channel nothing uses).
    fn rig_with(
        tx: Option<Sender<Vec<u8>>>,
        direct: bool,
        tun_fd: i32,
        cipher: Option<Arc<Aes>>,
    ) -> Rig {
        let st = Arc::new(Stats {
            tun_fd,
            frames: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            tun_err: AtomicU64::new(0),
            kafka_ok: AtomicU64::new(0),
            kafka_err: AtomicU64::new(0),
            pauses: AtomicU64::new(0),
            decrypt_err: AtomicU64::new(0),
            first_err: std::sync::Once::new(),
        });
        let (req_tx, req_rx) = unbounded::<DrainReq>();
        let done = Arc::new(AtomicBool::new(false));
        let drainer = {
            let dtx = tx.clone().unwrap_or_else(|| bounded(1).0);
            let (rtx, done) = (req_tx.clone(), done.clone());
            std::thread::spawn(move || drainer(req_rx, rtx, dtx, done))
        };
        let max = MAX_PACKET + if cipher.is_some() { aes::OVERHEAD } else { 0 };
        let eng = Arc::new(Engine::builder().loops(1).watchdog(None).build().unwrap());
        let paused = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(None));
        let (fst, fpaused, fseen) = (st.clone(), paused.clone(), seen.clone());
        let ln = eng
            .listen_tcp(
                "127.0.0.1:0",
                twine::factory(move |_, _| {
                    let c = Client::new(
                        fst.clone(),
                        tx.clone(),
                        req_tx.clone(),
                        direct,
                        cipher.clone(),
                    );
                    fpaused.lock().unwrap().push(c.paused.clone());
                    Box::new(LengthPrefixed::with_format(
                        Spy {
                            inner: c,
                            seen: fseen.clone(),
                        },
                        FrameFormat::new().with_max_frame(max),
                    ))
                }),
                ListenOptions::default(),
            )
            .unwrap();
        let addr = ln.addr();
        let e2 = eng.clone();
        let run = Some(std::thread::spawn(move || e2.run()));
        Rig {
            st,
            eng,
            addr,
            run,
            drainer: Some(drainer),
            done,
            paused,
            seen,
            _ln: ln,
        }
    }

    impl Rig {
        /// The same order as `main`: stop and join the engine, then the drainer.
        fn stop_engine(&mut self) {
            self.eng.stop();
            self.run.take().unwrap().join().unwrap().unwrap();
        }
        fn join_drainer(&mut self) {
            self.done.store(true, Relaxed);
            self.drainer.take().unwrap().join().unwrap();
        }
    }

    /// `n` frames carrying sequence numbers `from..from+n` (8-byte bodies).
    fn frames(from: u64, n: u64) -> Vec<u8> {
        let mut b = Vec::with_capacity(n as usize * 12);
        for i in from..from + n {
            b.extend_from_slice(&8u32.to_be_bytes());
            b.extend_from_slice(&i.to_be_bytes());
        }
        b
    }

    fn seq(p: &[u8]) -> u64 {
        u64::from_be_bytes(p.try_into().expect("8-byte frame"))
    }

    fn wait_until(d: Duration, mut f: impl FnMut() -> bool) -> bool {
        let t = Instant::now();
        while t.elapsed() < d {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        f()
    }

    /// Asks the loop whether the connection's reading is paused (None: closed).
    fn reading_paused(seen: &Mutex<Option<(ConnId, LoopHandle)>>) -> Option<bool> {
        let (id, lh) = seen.lock().unwrap().clone().expect("connection seen");
        let (tx, rx) = bounded(1);
        lh.call_from_thread(move |lp| {
            let _ = tx.send(lp.conn(id).map(|t| t.is_reading_paused()));
        })
        .unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap()
    }

    /// Sends `n` frames through a 2-slot sink to a consumer that takes `delay`
    /// per frame; asserts exactly-once, in-order delivery and that the
    /// connection is resumed afterwards.
    fn ordered_delivery(n: u64, delay: Duration) {
        let (tx, rx) = bounded::<Vec<u8>>(2);
        let mut r = rig(tx);
        let got = Arc::new(Mutex::new(Vec::<u64>::new()));
        let consumer = {
            let got = got.clone();
            std::thread::spawn(move || {
                for p in rx.iter() {
                    if !delay.is_zero() {
                        std::thread::sleep(delay);
                    }
                    got.lock().unwrap().push(seq(&p));
                }
            })
        };
        let count = || got.lock().unwrap().len() as u64;
        let mut c = TcpStream::connect(r.addr).unwrap();
        c.write_all(&frames(0, n)).unwrap();
        assert!(
            wait_until(Duration::from_secs(30), || count() >= n),
            "consumer got {} of {n}",
            count()
        );
        assert!(
            r.st.pauses.load(Relaxed) > 0,
            "the tiny channel never filled"
        );
        // Resumed: the client flag is cleared and the loop reads again.
        let paused = r.paused.lock().unwrap()[0].clone();
        assert!(wait_until(Duration::from_secs(5), || !paused.load(Relaxed)));
        assert_eq!(reading_paused(&r.seen), Some(false));
        // ...and frames sent after the backlog cleared still arrive.
        c.write_all(&frames(n, 100)).unwrap();
        assert!(wait_until(Duration::from_secs(10), || count() >= n + 100));
        drop(c); // otherwise the graceful close lingers until shutdown_timeout
        r.stop_engine();
        r.join_drainer();
        drop(r);
        consumer.join().unwrap();
        let got = got.lock().unwrap();
        assert_eq!(got.len() as u64, n + 100, "lost or duplicated");
        assert!(
            got.iter().copied().eq(0..n + 100),
            "reordered (first bad index {:?})",
            got.iter().zip(0..).position(|(&g, i)| g != i)
        );
    }

    /// The drainer is always the one waiting: pending is complete before it
    /// finishes, so each pause resumes on the first `call_from_thread`.
    #[test]
    fn slow_consumer_gets_every_frame_once_in_order_and_conn_resumes() {
        ordered_delivery(20_000, Duration::from_micros(20));
    }

    /// The drainer keeps up with the loop, so it can finish while the loop is
    /// still appending the rest of the chunk: the resume callback then finds
    /// pending non-empty and must resubmit instead of resuming.
    #[test]
    fn fast_consumer_races_the_loop_without_loss_or_reordering() {
        ordered_delivery(500_000, Duration::ZERO);
    }

    #[test]
    fn backlog_read_during_shutdown_linger_reaches_a_late_consumer() {
        let (tx, rx) = bounded::<Vec<u8>>(2);
        let mut r = rig(tx);
        let (go_tx, go_rx) = bounded::<()>(1);
        let consumer = std::thread::spawn(move || {
            go_rx.recv().unwrap(); // stalled until the engine has stopped
            rx.iter().map(|p| seq(&p)).collect::<Vec<u64>>()
        });
        // ~240 KB: more than one 64 KB read, so most of it is still in the
        // socket when the connection pauses; the FIN follows it.
        const N: u64 = 20_000;
        let mut c = TcpStream::connect(r.addr).unwrap();
        c.write_all(&frames(0, N)).unwrap();
        drop(c);
        assert!(wait_until(Duration::from_secs(5), || r
            .st
            .pauses
            .load(Relaxed)
            > 0));
        assert_eq!(reading_paused(&r.seen), Some(true));
        assert!(
            r.st.frames.load(Relaxed) < N,
            "nothing left for the linger phase"
        );
        // Graceful close un-pauses reading to find the FIN: the rest of the
        // socket lands in `pending` while the drainer is blocked on the full
        // channel, and the loop exits before it can resume the connection.
        r.stop_engine();
        assert_eq!(
            r.st.frames.load(Relaxed),
            N,
            "engine stopped before the FIN"
        );
        go_tx.send(()).unwrap();
        r.join_drainer();
        drop(r);
        let got = consumer.join().unwrap();
        assert_eq!(
            got,
            (0..N).collect::<Vec<_>>(),
            "lost, duplicated or reordered"
        );
    }

    fn key_a() -> Arc<Aes> {
        Arc::new(Aes::new(&aes::default_key()))
    }

    fn key_b() -> Arc<Aes> {
        Arc::new(Aes::new(&[0xff; 32]))
    }

    /// `n` frames sealed with `k` whose plaintexts are the sequence numbers
    /// `from..from+n` (8 bytes each, so each body is 8 + OVERHEAD).
    fn sealed_frames(k: &Aes, from: u64, n: u64) -> Vec<u8> {
        let mut b = Vec::new();
        for i in from..from + n {
            let mut nonce = [0u8; 12];
            nonce[..8].copy_from_slice(&i.to_le_bytes());
            let body = k.seal(nonce, &i.to_be_bytes());
            assert_eq!(body.len(), 8 + aes::OVERHEAD);
            b.extend_from_slice(&(body.len() as u32).to_be_bytes());
            b.extend_from_slice(&body);
        }
        b
    }

    /// Channel mode with AES: the sink receives the plaintext, in order.
    #[test]
    fn aes_frames_reach_the_sink_as_plaintext() {
        let (tx, rx) = unbounded::<Vec<u8>>();
        let mut r = rig_with(Some(tx), false, -1, Some(key_a()));
        const N: u64 = 5_000;
        let mut c = TcpStream::connect(r.addr).unwrap();
        c.write_all(&sealed_frames(&key_a(), 0, N)).unwrap();
        let got: Vec<u64> = (0..N)
            .map(|_| seq(&rx.recv_timeout(Duration::from_secs(10)).unwrap()))
            .collect();
        assert_eq!(got, (0..N).collect::<Vec<_>>());
        assert_eq!(r.st.frames.load(Relaxed), N);
        assert_eq!(r.st.bytes.load(Relaxed), N * (8 + aes::OVERHEAD as u64));
        assert_eq!(r.st.decrypt_err.load(Relaxed), 0);
        drop(c);
        r.stop_engine();
        r.join_drainer();
    }

    /// Wrong-key and short frames are counted and skipped; the connection
    /// stays open and later good frames still arrive.
    #[test]
    fn aes_bad_frames_count_decrypt_err_and_keep_the_connection() {
        let (tx, rx) = unbounded::<Vec<u8>>();
        let mut r = rig_with(Some(tx), false, -1, Some(key_a()));
        let mut c = TcpStream::connect(r.addr).unwrap();
        c.write_all(&sealed_frames(&key_b(), 0, 10)).unwrap();
        c.write_all(&frames(0, 3)).unwrap(); // 8-byte bodies: shorter than OVERHEAD
        assert!(wait_until(Duration::from_secs(5), || r
            .st
            .decrypt_err
            .load(Relaxed)
            == 13));
        assert!(rx.is_empty(), "a bad frame reached the sink");
        assert_eq!(reading_paused(&r.seen), Some(false), "connection closed");
        // Same stream: a frame sealed with the right key still gets through.
        c.write_all(&sealed_frames(&key_a(), 42, 1)).unwrap();
        let p = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(seq(&p), 42);
        assert_eq!(r.st.frames.load(Relaxed), 14);
        assert_eq!(r.st.decrypt_err.load(Relaxed), 13);
        drop(c);
        r.stop_engine();
        r.join_drainer();
    }

    /// Direct mode, Kafka off: the loop decrypts into the scratch buffer and
    /// writes the plaintext to the "TUN" (a datagram socket standing in for
    /// it, so packet boundaries are kept).
    #[test]
    fn aes_direct_mode_writes_plaintext_to_the_tun() {
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixDatagram;
        let (tun_w, tun_r) = UnixDatagram::pair().unwrap();
        tun_r
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut r = rig_with(None, true, tun_w.as_raw_fd(), Some(key_a()));
        const N: u64 = 200;
        let reader = std::thread::spawn(move || {
            let mut buf = [0u8; 64];
            (0..N)
                .map(|_| {
                    let n = tun_r.recv(&mut buf).unwrap();
                    seq(&buf[..n])
                })
                .collect::<Vec<u64>>()
        });
        let mut c = TcpStream::connect(r.addr).unwrap();
        let mut wire = sealed_frames(&key_b(), 0, 1); // skipped
        wire.extend_from_slice(&sealed_frames(&key_a(), 0, N));
        c.write_all(&wire).unwrap();
        assert_eq!(reader.join().unwrap(), (0..N).collect::<Vec<_>>());
        assert_eq!(r.st.decrypt_err.load(Relaxed), 1);
        assert_eq!(r.st.tun_err.load(Relaxed), 0);
        drop(c);
        r.stop_engine();
        r.join_drainer();
        drop(tun_w);
    }
}
