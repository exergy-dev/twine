//! `Loop::connect_tcp` and `LoopHandle::connect_tcp`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use twine::{factory, DialOptions, Engine, Error, LoopHandle, Options, Protocol, Transport};

const MS: fn(u64) -> Duration = Duration::from_millis;

struct Rig {
    eng: Arc<Engine>,
    lh: LoopHandle,
    join: Option<JoinHandle<twine::Result<()>>>,
}

fn rig() -> Rig {
    let eng = Arc::new(Engine::new(Options::default().loops(1).shutdown_timeout(MS(500))).unwrap());
    let lh = eng.next_loop();
    let e2 = eng.clone();
    let join = Some(std::thread::spawn(move || e2.run()));
    Rig { eng, lh, join }
}

impl Rig {
    fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().unwrap().unwrap();
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A std echo server on a thread; returns its address.
fn echo_server() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        while let Ok((mut s, _)) = l.accept() {
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while let Ok(n) = s.read(&mut buf) {
                    if n == 0 || s.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

struct Client {
    log: Arc<Mutex<Vec<&'static str>>>,
    data: Sender<Vec<u8>>,
}

impl Protocol for Client {
    fn connection_made(&mut self, _t: &mut Transport<'_>) {
        self.log.lock().unwrap().push("made");
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, d: &mut [u8]) {
        let _ = self.data.send(d.to_vec());
    }
}

#[test]
fn connect_made_before_resolve_and_echo() {
    let r = rig();
    let addr = echo_server();
    let log = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = channel();
    let (l2, l3) = (log.clone(), log.clone());
    let id =
        r.lh.submit(move |lp| {
            lp.connect_tcp(
                addr,
                move |_, _| Box::new(Client { log: l2, data: tx }),
                DialOptions::default(),
            )
            .then(move |lp, id| {
                l3.lock().unwrap().push("resolved");
                let mut t = lp.conn(id).expect("connection is live");
                assert_eq!(t.peer(), addr);
                t.write(b"hello");
                id
            })
        })
        .wait()
        .unwrap();
    assert_eq!(*log.lock().unwrap(), vec!["made", "resolved"]);
    assert_eq!(id.loop_id(), 0);
    let mut got = Vec::new();
    while got.len() < 5 {
        got.extend(rx.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    assert_eq!(got, b"hello");
}

#[test]
fn connect_refused_rejects_with_econnrefused() {
    let r = rig();
    let addr = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    for _ in 0..2 {
        // Twice: the failed slot must be recycled cleanly.
        let v = r
            .lh
            .submit(move |lp| lp.connect_tcp(addr, |_, _| unreachable!(), DialOptions::default()))
            .wait();
        match v {
            Err(e @ Error::Io(_)) => assert_eq!(e.raw_os_error(), Some(libc_econnrefused())),
            other => panic!("expected ECONNREFUSED, got {other:?}"),
        }
    }
    // The loop still connects afterwards.
    let ok = echo_server();
    let log = Arc::new(Mutex::new(Vec::new()));
    let (tx, _rx) = channel();
    let v =
        r.lh.submit(move |lp| {
            lp.connect_tcp(
                ok,
                move |_, _| Box::new(Client { log, data: tx }),
                DialOptions::default(),
            )
        })
        .wait();
    assert!(v.is_ok());
}

fn libc_econnrefused() -> i32 {
    111 // ECONNREFUSED on Linux
}

/// Result of probing the blackhole address.
const BLACKHOLE: &str = "10.255.255.1:9";

#[test]
fn connect_timeout_rejects() {
    let r = rig();
    let addr: SocketAddr = BLACKHOLE.parse().unwrap();
    let t0 = Instant::now();
    let v =
        r.lh.submit(move |lp| {
            lp.connect_tcp(
                addr,
                |_, _| unreachable!("must not connect"),
                DialOptions::default().timeout(Some(MS(100))),
            )
        })
        .wait();
    match v {
        Err(Error::ConnectTimeout) => {
            let el = t0.elapsed();
            assert!(el >= MS(100), "{el:?}");
            assert!(el < Duration::from_secs(3), "{el:?}");
        }
        Err(Error::Io(e)) => eprintln!("skipping: no route to {BLACKHOLE} ({e})"),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn handle_connect_from_foreign_thread_yields_usable_handle() {
    let r = rig();
    let addr = echo_server();
    let log = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    let h =
        r.lh.connect_tcp(
            addr,
            factory(move |_, _| {
                Box::new(Client {
                    log: log.clone(),
                    data: tx.lock().unwrap().clone(),
                })
            }),
            DialOptions::default(),
        )
        .wait()
        .unwrap();
    h.write(&b"ping"[..]);
    let mut got = Vec::new();
    while got.len() < 4 {
        got.extend(rx.recv_timeout(Duration::from_secs(5)).unwrap());
    }
    assert_eq!(got, b"ping");
    h.lose_connection();
}

#[test]
fn shutdown_during_pending_connect_rejects_with_engine_stopped() {
    let mut r = rig();
    let addr: SocketAddr = BLACKHOLE.parse().unwrap();
    let aw = r.lh.submit(move |lp| {
        lp.connect_tcp(
            addr,
            |_, _| unreachable!("must not connect"),
            DialOptions::default().timeout(None),
        )
    });
    std::thread::sleep(MS(200));
    if let Some(early) = aw.try_take() {
        eprintln!("skipping: connect to {BLACKHOLE} did not stay pending ({early:?})");
        return;
    }
    r.stop();
    match aw.wait() {
        Err(Error::EngineStopped) => {}
        other => panic!("expected EngineStopped, got {other:?}"),
    }
}

#[test]
fn connect_while_stopping_settles_cleanly() {
    // `stop()` only signals; a connect issued in the same task races the shutdown
    // and may either complete (then be closed by shutdown) or be rejected.
    let mut r = rig();
    let addr = echo_server();
    let eng = r.eng.clone();
    let log = Arc::new(Mutex::new(Vec::new()));
    let (tx, _rx) = channel();
    let v =
        r.lh.submit(move |lp| {
            eng.stop();
            lp.connect_tcp(
                addr,
                move |_, _| Box::new(Client { log, data: tx }),
                DialOptions::default(),
            )
        })
        .wait();
    assert!(
        matches!(
            v,
            Ok(_) | Err(Error::EngineStopped) | Err(Error::LoopStopped) | Err(Error::Closed)
        ),
        "{v:?}"
    );
    r.stop();
}
