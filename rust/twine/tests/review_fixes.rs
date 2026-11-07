//! Regression tests for the core review fixes: linger deadline, producer stop
//! once, cross-loop ids, token-timer cleanup, write-filter bypass, pending-queue
//! budget, and the coverage gaps (EOF with pending writes, read cap, writev,
//! panic containment, stale handles).

mod common;

use std::any::Any;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use twine::protocol::WriteFilter;
use twine::{
    factory, ConnId, Error, ListenOptions, Loop, Options, Producer, Protocol, PushProducer,
    Transport, TransportHandle,
};

struct Echo;
impl Protocol for Echo {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

// --- 1. the linger deadline is not extended by peer activity ---------------------

type LostAt = Arc<Mutex<Option<(Instant, Option<String>)>>>;

struct Lingerer {
    at: Arc<Mutex<Option<Instant>>>,
    lost: LostAt,
}
impl Protocol for Lingerer {
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        if self.at.lock().unwrap().is_none() {
            *self.at.lock().unwrap() = Some(Instant::now());
            t.lose_connection();
            // Must not cancel or replace the linger deadline.
            t.set_idle_timeout(Some(Duration::from_secs(60)));
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        *self.lost.lock().unwrap() = Some((Instant::now(), r.map(|e| format!("{e:?}"))));
    }
}

#[test]
fn linger_deadline_is_not_extended_by_peer_activity() {
    let at = Arc::new(Mutex::new(None));
    let lost = Arc::new(Mutex::new(None));
    let (a2, l2) = (at.clone(), lost.clone());
    let mut srv = Server::start_with(
        Options::default()
            .loops(1)
            .close_linger(Duration::from_millis(200)),
        ListenOptions::default(),
        factory(move |_, _| {
            Box::new(Lingerer {
                at: a2.clone(),
                lost: l2.clone(),
            })
        }),
    );
    let mut c = srv.connect();
    c.write_all(b"x").unwrap();
    // Keep sending, never close: the peer's activity must not extend the linger.
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(1500) && lost.lock().unwrap().is_none() {
        let _ = c.write_all(b"y");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (when, why) = lost
        .lock()
        .unwrap()
        .clone()
        .expect("closed at close_linger");
    let dt = when.duration_since(at.lock().unwrap().unwrap());
    assert!(dt >= Duration::from_millis(180), "closed early: {dt:?}");
    assert!(dt < Duration::from_millis(1000), "linger extended: {dt:?}");
    assert_eq!(why, None, "linger expiry is a clean close");
    drop(c);
    srv.stop();
}

// --- 2. a producer registered on a closed connection is stopped once -------------

struct StopCount {
    stops: Arc<AtomicUsize>,
    lost: Arc<AtomicUsize>,
    producer: fn() -> Producer,
}
impl Protocol for StopCount {
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        t.abort();
        t.register_producer((self.producer)(), true);
    }
    fn stop_producing(&mut self, _t: &mut Transport<'_>) {
        self.stops.fetch_add(1, SeqCst);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn register_producer_on_closed_connection_stops_once() {
    let stops = Arc::new(AtomicUsize::new(0));
    let lost = Arc::new(AtomicUsize::new(0));
    let (s2, l2) = (stops.clone(), lost.clone());
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(StopCount {
                stops: s2.clone(),
                lost: l2.clone(),
                producer: || Producer::This,
            })
        }),
    );
    let mut c = srv.connect();
    c.write_all(b"x").unwrap();
    assert!(wait_until(Duration::from_secs(5), || lost.load(SeqCst) == 1));
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        stops.load(SeqCst),
        1,
        "stop_producing delivered exactly once"
    );
    srv.stop();
}

// --- 3/4. cross-loop ConnIds -------------------------------------------------------

struct Ident {
    ids: Arc<Mutex<Vec<ConnId>>>,
}
impl Protocol for Ident {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.ids.lock().unwrap().push(t.id());
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

#[test]
fn conn_id_from_another_loop_is_inert() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let i2 = ids.clone();
    let mut srv = Server::start(2, factory(move |_, _| Box::new(Ident { ids: i2.clone() })));
    let mut clients = Vec::new();
    let mut per_loop: [Option<ConnId>; 2] = [None, None];
    for _ in 0..64 {
        clients.push(srv.connect());
        assert!(wait_until(Duration::from_secs(5), || ids
            .lock()
            .unwrap()
            .len()
            == clients.len()));
        for id in ids.lock().unwrap().iter() {
            per_loop[id.loop_id()] = Some(*id);
        }
        if per_loop.iter().all(|p| p.is_some()) {
            break;
        }
    }
    let (a, b) = (
        per_loop[0].expect("a conn on loop 0"),
        per_loop[1].expect("a conn on loop 1"),
    );
    let handles = srv.eng.loop_handles();
    // `a` lives on loop 0; loop 1 must not resolve it.
    let none = handles[1]
        .call(move |lp| lp.conn(a).is_none())
        .wait()
        .unwrap();
    assert!(none, "Loop::conn of a foreign-loop id must be None");
    // Producer::Conn / PauseReading with a foreign id are ignored: a second
    // registration would panic ("already registered") if the first had stuck.
    let ok = handles[1]
        .call(move |lp| {
            let mut t = lp.conn(b).unwrap();
            t.register_producer(Producer::Conn(a), true);
            t.register_producer(Producer::PauseReading(a), true);
            t.register_producer(Producer::This, true);
            t.unregister_producer();
        })
        .wait();
    assert!(ok.is_ok(), "foreign producer ids must be ignored: {ok:?}");
    drop(clients);
    srv.stop();
}

// --- 5. token timers leave the wheel at close --------------------------------------

struct TimerHog;
impl Protocol for TimerHog {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let hour = Duration::from_secs(3600);
        t.call_later(hour, 1);
        t.call_later(hour, 2);
        let gone = t.call_later(hour, 3);
        assert!(t.cancel_timer(gone));
        t.call_later(Duration::from_millis(1), 4); // fires and prunes itself
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

fn timers_active(srv: &Server) -> u64 {
    srv.eng.metrics().iter().map(|m| m.timers_active).sum()
}

#[test]
fn token_timers_are_removed_when_the_connection_closes() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(TimerHog)));
    // Let the loop run an iteration so the baseline gauge is published.
    std::thread::sleep(Duration::from_millis(20));
    let base = timers_active(&srv);
    for _ in 0..10 {
        let conns: Vec<TcpStream> = (0..100).map(|_| srv.connect()).collect();
        assert!(wait_until(Duration::from_secs(10), || timers_active(&srv)
            >= base + 200));
        drop(conns);
        assert!(wait_until(Duration::from_secs(10), || srv
            .eng
            .metrics()
            .iter()
            .all(|m| m.connections == 0)));
    }
    assert!(
        wait_until(Duration::from_secs(5), || timers_active(&srv) == base),
        "1000 closed connections left {} timers behind",
        timers_active(&srv) - base
    );
    srv.stop();
}

// --- 7. write-filter bypass --------------------------------------------------------

const KEY: u8 = 0x5a;

struct Xor;
impl WriteFilter for Xor {
    fn write(&mut self, raw: &mut Transport<'_>, plain: &[u8]) {
        let v: Vec<u8> = plain.iter().map(|b| b ^ KEY).collect();
        raw.write(&v);
    }
    fn lose_connection(&mut self, raw: &mut Transport<'_>) {
        self.write(raw, b"<bye>");
        raw.lose_connection();
    }
}

/// A tiny "encrypting" wrapper protocol: XOR in both directions.
struct Wrap {
    inner: Box<dyn Protocol>,
    f: Xor,
}
impl Protocol for Wrap {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_filtered(true);
        self.inner.connection_made(&mut t.with_filter(&mut self.f));
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        for b in data.iter_mut() {
            *b ^= KEY;
        }
        self.inner
            .data_received(&mut t.with_filter(&mut self.f), data);
    }
    fn connection_lost(&mut self, t: &mut Transport<'_>, r: Option<Error>) {
        self.inner
            .connection_lost(&mut t.with_filter(&mut self.f), r);
    }
    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.pause_producing(&mut t.with_filter(&mut self.f));
    }
    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.resume_producing(&mut t.with_filter(&mut self.f));
    }
    fn stop_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.stop_producing(&mut t.with_filter(&mut self.f));
    }
    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        self.inner
            .timer_fired(&mut t.with_filter(&mut self.f), token);
    }
    fn visit(
        &mut self,
        t: &mut Transport<'_>,
        f: &mut dyn FnMut(&mut dyn Any, &mut Transport<'_>),
    ) {
        self.inner.visit(&mut t.with_filter(&mut self.f), f)
    }
}

struct HookOnce(bool);
impl PushProducer for HookOnce {
    fn pause_producing(&mut self, _c: &mut Transport<'_>) {}
    fn resume_producing(&mut self, c: &mut Transport<'_>) {
        if !self.0 {
            self.0 = true;
            c.write(b"from-hook");
        }
    }
    fn stop_producing(&mut self, _c: &mut Transport<'_>) {}
}

struct FilteredApp {
    handles: Arc<Mutex<Vec<TransportHandle>>>,
}
impl Protocol for FilteredApp {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.handles.lock().unwrap().push(t.handle());
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        match data.first() {
            Some(b'E') => t.write(data),
            Some(b'P') => t.register_producer(Producer::Boxed(Box::new(HookOnce(false))), false),
            _ => {}
        }
    }
}

fn filtered_server() -> (Server, Arc<Mutex<Vec<TransportHandle>>>) {
    let handles = Arc::new(Mutex::new(Vec::new()));
    let h2 = handles.clone();
    let srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Wrap {
                inner: Box::new(FilteredApp {
                    handles: h2.clone(),
                }),
                f: Xor,
            })
        }),
    );
    (srv, handles)
}

fn send_enc(c: &mut TcpStream, plain: &[u8]) {
    let v: Vec<u8> = plain.iter().map(|b| b ^ KEY).collect();
    c.write_all(&v).unwrap();
}

/// Reads `n` wire bytes, checks they are not the plaintext, returns the decoded form.
fn read_dec(c: &mut TcpStream, n: usize) -> Vec<u8> {
    let raw = read_n(c, n);
    let plain: Vec<u8> = raw.iter().map(|b| b ^ KEY).collect();
    assert_ne!(raw, plain);
    plain
}

#[test]
fn filtered_writes_all_pass_through_the_filter() {
    let (mut srv, handles) = filtered_server();

    // Own-view write (baseline).
    let mut c = srv.connect();
    send_enc(&mut c, b"Ehello");
    assert_eq!(read_dec(&mut c, 6), b"Ehello");
    assert!(wait_until(Duration::from_secs(5), || handles
        .lock()
        .unwrap()
        .len()
        == 1));
    let h = handles.lock().unwrap()[0].clone();

    // TransportHandle::write from another thread -> Pending/Task::Op -> visit -> filter.
    h.write(&b"thread"[..]);
    assert_eq!(read_dec(&mut c, 6), b"thread");

    // Foreign view via Loop::conn.
    let id = h.id();
    srv.eng.loop_handles()[0]
        .call(move |lp| lp.conn(id).unwrap().write(b"foreign"))
        .wait()
        .unwrap();
    assert_eq!(read_dec(&mut c, 7), b"foreign");
    srv.eng.loop_handles()[0]
        .call(move |lp| lp.conn(id).unwrap().writev(&[b"ab", b"cd"]))
        .wait()
        .unwrap();
    assert_eq!(read_dec(&mut c, 4), b"abcd");

    // Foreign lose_connection reaches the wrapper's close hook ("<bye>").
    srv.eng.loop_handles()[0]
        .call(move |lp| lp.conn(id).unwrap().lose_connection())
        .wait()
        .unwrap();
    let rest = read_to_end_lossy(&mut c);
    let plain: Vec<u8> = rest.iter().map(|b| b ^ KEY).collect();
    assert_eq!(plain, b"<bye>");
    drop(c);

    // A boxed producer hook on a filtered connection writes through the filter.
    let mut c = srv.connect();
    send_enc(&mut c, b"P");
    assert_eq!(read_dec(&mut c, 9), b"from-hook");
    drop(c);
    srv.stop();
}

// --- 8. the post-callback queue has a budget ----------------------------------------

fn spin(lp: &mut Loop, keep: Arc<AtomicBool>, n: Arc<AtomicUsize>) {
    if !keep.load(SeqCst) {
        return;
    }
    n.fetch_add(1, SeqCst);
    lp.call_soon(move |lp| spin(lp, keep, n));
}

#[test]
fn self_requeueing_call_soon_does_not_starve_io() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(Echo)));
    let keep = Arc::new(AtomicBool::new(true));
    let n = Arc::new(AtomicUsize::new(0));
    let (k2, n2) = (keep.clone(), n.clone());
    srv.eng.loop_handles()[0]
        .call_from_thread(move |lp| spin(lp, k2, n2))
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || n.load(SeqCst) > 5000));
    let mut c = srv.connect();
    for i in 0..50u8 {
        c.write_all(&[i; 100]).unwrap();
        assert_eq!(read_n(&mut c, 100), vec![i; 100]);
    }
    keep.store(false, SeqCst);
    drop(c);
    srv.stop();
}

// --- 17. coverage gaps ---------------------------------------------------------------

struct BigWriter {
    total: usize,
    vectored: bool,
}
impl Protocol for BigWriter {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let buf: Vec<u8> = (0..self.total).map(pattern).collect();
        if self.vectored {
            let third = self.total / 3;
            t.writev(&[
                &buf[..third],
                &[],
                &buf[third..2 * third],
                &buf[2 * third..],
            ]);
        } else {
            t.write(&buf);
        }
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

fn check_pattern(c: &mut TcpStream, total: usize) {
    let mut got = 0usize;
    let mut buf = vec![0u8; 64 << 10];
    while got < total {
        let n = c.read(&mut buf).expect("read");
        assert_ne!(n, 0, "early EOF at {got}");
        for (i, b) in buf[..n].iter().enumerate() {
            assert_eq!(*b, pattern(got + i), "byte {} differs", got + i);
        }
        got += n;
    }
}

#[test]
fn eof_with_pending_writes_still_delivers_everything_then_closes_clean() {
    let total = 4 << 20;
    let lost = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let l2 = lost.clone();
    struct P(usize, Arc<Mutex<Vec<Option<String>>>>);
    impl Protocol for P {
        fn connection_made(&mut self, t: &mut Transport<'_>) {
            let buf: Vec<u8> = (0..self.0).map(pattern).collect();
            t.write(&buf);
        }
        fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
        fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
            self.1.lock().unwrap().push(r.map(|e| format!("{e:?}")));
        }
    }
    let mut srv = Server::start_with(
        Options::default().loops(1).write_buffer_limit(64 << 20),
        ListenOptions::default().socket_buffers(0, 4096),
        factory(move |_, _| Box::new(P(total, l2.clone()))),
    );
    let mut c = srv.connect();
    // Half-close immediately: the server sees EOF while its ring is non-empty.
    c.shutdown(Shutdown::Write).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    check_pattern(&mut c, total);
    assert_eq!(
        read_to_end_lossy(&mut c).len(),
        0,
        "clean EOF after the data"
    );
    assert!(wait_until(Duration::from_secs(5), || lost
        .lock()
        .unwrap()
        .len()
        == 1));
    assert_eq!(*lost.lock().unwrap(), vec![None]);
    srv.stop();
}

#[test]
fn read_cap_ready_list_echoes_a_1mib_burst() {
    let mut srv = Server::start_with(
        Options::default()
            .loops(1)
            .max_reads_per_event(1)
            .read_buffer_size(4096),
        ListenOptions::default(),
        factory(|_, _| Box::new(Echo)),
    );
    let total = 1 << 20;
    let data: Vec<u8> = (0..total).map(pattern).collect();
    let mut c = srv.connect();
    let mut w = c.try_clone().unwrap();
    let d2 = data.clone();
    let writer = std::thread::spawn(move || w.write_all(&d2).unwrap());
    check_pattern(&mut c, total);
    writer.join().unwrap();
    drop(c);
    srv.stop();
}

#[test]
fn writev_direct_and_partial_are_byte_exact() {
    // Direct: small, one syscall.
    struct Small;
    impl Protocol for Small {
        fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
            t.writev(&[b"ab", b"", b"cde"]);
        }
    }
    let mut srv = Server::start(1, factory(|_, _| Box::new(Small)));
    let mut c = srv.connect();
    c.write_all(b"go").unwrap();
    assert_eq!(read_n(&mut c, 5), b"abcde");
    drop(c);
    srv.stop();

    // Partial: bigger than the socket buffers, the tail goes through the ring.
    let total = 3 << 20;
    let mut srv = Server::start_with(
        Options::default().loops(1).write_buffer_limit(64 << 20),
        ListenOptions::default().socket_buffers(0, 4096),
        factory(move |_, _| {
            Box::new(BigWriter {
                total,
                vectored: true,
            })
        }),
    );
    let mut c = srv.connect();
    check_pattern(&mut c, total);
    drop(c);
    srv.stop();
}

#[derive(Clone, Copy, PartialEq)]
enum Boom {
    Made,
    Timer,
    Hook,
    WithProto,
}

#[derive(Default)]
struct Outcomes {
    lost: Mutex<Vec<(usize, String)>>,
    ids: Mutex<Vec<ConnId>>,
}

struct Chaos {
    mode: Option<Boom>,
    seq: usize,
    st: Arc<Outcomes>,
}
struct HookBoom;
impl PushProducer for HookBoom {
    fn pause_producing(&mut self, _c: &mut Transport<'_>) {}
    fn resume_producing(&mut self, _c: &mut Transport<'_>) {
        panic!("hook exploded");
    }
    fn stop_producing(&mut self, _c: &mut Transport<'_>) {}
}
impl Protocol for Chaos {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.st.ids.lock().unwrap().push(t.id());
        match self.mode {
            Some(Boom::Made) => panic!("made exploded"),
            Some(Boom::Timer) => {
                t.call_later(Duration::from_millis(10), 1);
            }
            _ => {}
        }
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        match (self.mode, data.first()) {
            (Some(Boom::Hook), Some(b'H')) => {
                t.register_producer(Producer::Boxed(Box::new(HookBoom)), false)
            }
            (Some(Boom::WithProto), Some(b'W')) => {
                let other = self.st.ids.lock().unwrap()[0];
                let _ = t
                    .loop_mut()
                    .with_protocol::<Chaos, _>(other, |_, _| panic!("closure exploded"));
            }
            _ => t.write(data),
        }
    }
    fn timer_fired(&mut self, _t: &mut Transport<'_>, _token: u64) {
        panic!("timer exploded");
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        self.st.lost.lock().unwrap().push((
            self.seq,
            match r {
                None => "clean".into(),
                Some(Error::Panic(p)) => format!("panic:{}", p.message),
                Some(e) => format!("{e:?}"),
            },
        ));
    }
}

/// Connection 0 is the victim (misbehaves in `mode`), connection 1 is healthy
/// and must keep echoing.
fn chaos(mode: Boom, trigger: &[u8], survivor_is_good: bool) -> Vec<(usize, String)> {
    let st = Arc::new(Outcomes::default());
    let (s2, n) = (st.clone(), Arc::new(AtomicUsize::new(0)));
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            let seq = n.fetch_add(1, SeqCst);
            // WithProto: the *caller* (conn 1) invokes a closure on conn 0 that panics.
            let m = match (mode, seq) {
                (Boom::WithProto, 1) => Some(mode),
                (Boom::WithProto, _) => None,
                (_, 0) => Some(mode),
                _ => None,
            };
            Box::new(Chaos {
                mode: m,
                seq,
                st: s2.clone(),
            })
        }),
    );
    let mut victim = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || !st
        .ids
        .lock()
        .unwrap()
        .is_empty()));
    let mut good = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || st
        .ids
        .lock()
        .unwrap()
        .len()
        >= 2));
    if !trigger.is_empty() {
        // WithProto: the healthy-looking connection 1 is the caller that panics.
        let src = if mode == Boom::WithProto {
            &mut good
        } else {
            &mut victim
        };
        src.write_all(trigger).unwrap();
    }
    assert!(wait_until(Duration::from_secs(5), || !st
        .lost
        .lock()
        .unwrap()
        .is_empty()));
    // The other connection is unaffected.
    let survivor = if survivor_is_good {
        &mut good
    } else {
        &mut victim
    };
    survivor.write_all(b"still alive").unwrap();
    assert_eq!(read_n(survivor, 11), b"still alive");
    let out = st.lost.lock().unwrap().clone();
    srv.stop();
    out
}

#[test]
fn panic_in_connection_made_aborts_only_that_connection() {
    let lost = chaos(Boom::Made, b"", true);
    assert_eq!(lost, vec![(0, "panic:made exploded".to_string())]);
}

#[test]
fn panic_in_timer_fired_aborts_only_that_connection() {
    let lost = chaos(Boom::Timer, b"", true);
    assert_eq!(lost, vec![(0, "panic:timer exploded".to_string())]);
}

#[test]
fn panic_in_producer_hook_aborts_only_that_connection() {
    let lost = chaos(Boom::Hook, b"H", true);
    assert_eq!(lost, vec![(0, "panic:hook exploded".to_string())]);
}

#[test]
fn panic_in_with_protocol_closure_aborts_only_the_caller() {
    // Connection 1 calls into connection 0; the closure panics: 1 is aborted, 0 lives.
    let lost = chaos(Boom::WithProto, b"W", false);
    assert_eq!(lost, vec![(1, "panic:closure exploded".to_string())]);
}

#[test]
fn stop_twice_is_harmless() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(Echo)));
    let mut c = srv.connect();
    c.write_all(b"a").unwrap();
    assert_eq!(read_n(&mut c, 1), b"a");
    srv.eng.stop();
    srv.eng.stop();
    srv.stop(); // a third stop, then join: run returns Ok
    assert!(srv.eng.is_stopped());
}

#[test]
fn stale_handle_after_slot_reuse_is_inert_and_counted() {
    let hs = Arc::new(Mutex::new(Vec::new()));
    let h2 = hs.clone();
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Wrap {
                inner: Box::new(FilteredApp {
                    handles: h2.clone(),
                }),
                f: Xor,
            })
        }),
    );
    // Connection A: record its handle, close it.
    let a = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || hs
        .lock()
        .unwrap()
        .len()
        == 1));
    let stale = hs.lock().unwrap()[0].clone();
    drop(a);
    assert!(wait_until(Duration::from_secs(5), || srv
        .eng
        .metrics()
        .iter()
        .all(|m| m.connections == 0)));
    // Connection B likely reuses A's fd slot; either way the stale id is inert.
    let mut b = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || hs
        .lock()
        .unwrap()
        .len()
        == 2));
    let before: u64 = srv.eng.metrics().iter().map(|m| m.write_after_close).sum();
    stale.write(&b"zombie"[..]);
    assert!(wait_until(Duration::from_secs(5), || srv
        .eng
        .metrics()
        .iter()
        .map(|m| m.write_after_close)
        .sum::<u64>()
        > before));
    b.set_read_timeout(Some(Duration::from_millis(150)))
        .unwrap();
    let mut buf = [0u8; 16];
    assert!(
        b.read(&mut buf).is_err(),
        "stale write must not reach the new connection"
    );
    // B still works.
    b.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    send_enc(&mut b, b"Ehi");
    assert_eq!(read_dec(&mut b, 3), b"Ehi");
    drop(b);
    srv.stop();
}
