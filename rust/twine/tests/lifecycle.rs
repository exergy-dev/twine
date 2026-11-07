//! Panic containment and graceful shutdown.

mod common;

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, Error, Options, Protocol, Transport};

// --- panics -----------------------------------------------------------------------

#[derive(Default)]
struct Panics {
    lost: Mutex<Vec<String>>,
}

struct Bomb(Arc<Panics>);

impl Protocol for Bomb {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if data.starts_with(b"BOOM") {
            panic!("protocol exploded");
        }
        t.write(data);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        self.0.lost.lock().unwrap().push(match r {
            None => "clean".into(),
            Some(Error::Panic(p)) => format!("panic:{}", p.message),
            Some(e) => format!("{e:?}"),
        });
    }
}

#[test]
fn protocol_panic_aborts_only_that_connection() {
    let st = Arc::new(Panics::default());
    let s2 = st.clone();
    let mut srv = Server::start(1, factory(move |_, _| Box::new(Bomb(s2.clone()))));
    let mut good = srv.connect();
    let mut bad = srv.connect();
    good.write_all(b"hello").unwrap();
    assert_eq!(read_n(&mut good, 5), b"hello");
    bad.write_all(b"BOOM").unwrap();
    let mut buf = [0u8; 8];
    match bad.read(&mut buf) {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("unexpected {n} bytes"),
    }
    assert!(wait_until(Duration::from_secs(5), || !st
        .lost
        .lock()
        .unwrap()
        .is_empty()));
    assert_eq!(
        *st.lost.lock().unwrap(),
        vec!["panic:protocol exploded".to_string()]
    );
    // The other client keeps echoing, and so does a brand-new one.
    for i in 0..20u8 {
        good.write_all(&[i; 10]).unwrap();
        assert_eq!(read_n(&mut good, 10), vec![i; 10]);
    }
    let mut fresh = srv.connect();
    fresh.write_all(b"still alive").unwrap();
    assert_eq!(read_n(&mut fresh, 11), b"still alive");
    assert_eq!(srv.eng.metrics()[0].panics, 1);
    srv.stop();
}

struct LostPanics;
impl Protocol for LostPanics {
    fn data_received(&mut self, t: &mut Transport<'_>, d: &mut [u8]) {
        t.write(d);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        panic!("lost boom");
    }
}

#[test]
fn panic_in_connection_lost_is_contained() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(LostPanics)));
    let c = srv.connect();
    drop(c);
    assert!(wait_until(Duration::from_secs(5), || srv.eng.metrics()[0]
        .panics
        == 1));
    let mut c2 = srv.connect();
    c2.write_all(b"ok").unwrap();
    assert_eq!(read_n(&mut c2, 2), b"ok");
    srv.stop();
}

// --- graceful shutdown ---------------------------------------------------------------

#[derive(Default)]
struct Tally {
    made: AtomicUsize,
    lost: AtomicUsize,
    lost_err: AtomicUsize,
}

/// Writes `size` bytes of pattern on connection and keeps the connection open.
struct Pending {
    tally: Arc<Tally>,
    size: usize,
}

impl Protocol for Pending {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.tally.made.fetch_add(1, SeqCst);
        let buf: Vec<u8> = (0..self.size).map(pattern).collect();
        t.write(&buf);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        self.tally.lost.fetch_add(1, SeqCst);
        if r.is_some() {
            self.tally.lost_err.fetch_add(1, SeqCst);
        }
    }
}

#[test]
fn graceful_shutdown_flushes_pending_writes_and_calls_lost_once() {
    let tally = Arc::new(Tally::default());
    let t2 = tally.clone();
    let size = 1 << 20; // far above the socket buffers: guaranteed pending writes
    let opts = Options::default()
        .loops(2)
        .shutdown_timeout(Duration::from_secs(20));
    let mut srv = Server::start_with(
        opts,
        twine::ListenOptions::default().socket_buffers(0, 8192),
        factory(move |_, _| {
            Box::new(Pending {
                tally: t2.clone(),
                size,
            })
        }),
    );
    let conns: Vec<_> = (0..100).map(|_| srv.connect()).collect();
    assert!(wait_until(Duration::from_secs(10), || tally
        .made
        .load(SeqCst)
        == 100));
    let ran_hook = Arc::new(AtomicUsize::new(0));
    let rh = ran_hook.clone();
    srv.eng.on_shutdown(move || {
        rh.fetch_add(1, SeqCst);
    });
    let eng = srv.eng.clone();
    let stopper = std::thread::spawn(move || eng.stop());
    // Clients are slow readers that drain everything until EOF, then close.
    let readers: Vec<_> = conns
        .into_iter()
        .map(|mut c| {
            std::thread::spawn(move || {
                let mut got = 0usize;
                let mut buf = vec![0u8; 16 << 10];
                loop {
                    let n = c.read(&mut buf).expect("read");
                    if n == 0 {
                        break;
                    }
                    for (i, b) in buf[..n].iter().enumerate() {
                        assert_eq!(*b, pattern(got + i));
                    }
                    got += n;
                }
                got
            })
        })
        .collect();
    for r in readers {
        assert_eq!(r.join().unwrap(), size, "all bytes delivered before close");
    }
    stopper.join().unwrap();
    srv.stop(); // joins the run thread
    assert_eq!(ran_hook.load(SeqCst), 1);
    assert_eq!(
        tally.lost.load(SeqCst),
        100,
        "connection_lost exactly once per connection"
    );
    assert_eq!(
        tally.lost_err.load(SeqCst),
        0,
        "everything flushed and closed cleanly"
    );
}

#[test]
fn shutdown_timeout_aborts_stuck_connections_with_shutdown_error() {
    let tally = Arc::new(Tally::default());
    let t2 = tally.clone();
    let opts = Options::default()
        .loops(1)
        .shutdown_timeout(Duration::from_millis(300))
        .close_linger(Duration::from_secs(60));
    let mut srv = Server::start_with(
        opts,
        twine::ListenOptions::default().socket_buffers(0, 4096),
        factory(move |_, _| {
            Box::new(Pending {
                tally: t2.clone(),
                size: 4 << 20,
            })
        }),
    );
    // This client never reads, so the pending writes can never flush.
    let _stuck = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || tally
        .made
        .load(SeqCst)
        == 1));
    let started = std::time::Instant::now();
    srv.stop();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(tally.lost.load(SeqCst), 1);
    assert_eq!(
        tally.lost_err.load(SeqCst),
        1,
        "aborted with Error::Shutdown"
    );
}
