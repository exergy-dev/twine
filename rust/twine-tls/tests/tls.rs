//! Integration tests for twine-tls: server side with std rustls clients, client
//! side against a std rustls server, error paths, handles and ALPN.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use rustls::{ClientConnection, ServerConnection, StreamOwned};
use twine::{DialOptions, Engine, Options};
use twine_tls::TlsOptions;

fn tls_client(
    cfg: Arc<rustls::ClientConfig>,
    addr: std::net::SocketAddr,
) -> StreamOwned<ClientConnection, TcpStream> {
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let c = ClientConnection::new(cfg, localhost()).unwrap();
    StreamOwned::new(c, s)
}

fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(31) ^ (i >> 7)) as u8
}

fn mib_roundtrip(versions: &[&'static rustls::SupportedProtocolVersion]) {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[]),
        Mode::Echo,
        2,
        TlsOptions::default(),
    );
    let mut c = tls_client(certs.client_config(versions, &[]), srv.addr);
    let total = 1 << 20;
    let chunk = 16 * 1024;
    let mut sent = 0usize;
    while sent < total {
        let buf: Vec<u8> = (sent..sent + chunk).map(pattern).collect();
        c.write_all(&buf).unwrap();
        let mut got = vec![0u8; chunk];
        c.read_exact(&mut got).unwrap();
        assert_eq!(got, buf, "mismatch at {sent}");
        sent += chunk;
    }
    let v = c.conn.protocol_version().unwrap();
    assert_eq!(v, versions[0].version);
    srv.stop();
}

#[test]
fn echo_1mib_tls13() {
    mib_roundtrip(&[&rustls::version::TLS13]);
}

#[test]
fn echo_1mib_tls12() {
    mib_roundtrip(&[&rustls::version::TLS12]);
}

#[test]
fn echo_20_clients_100_messages() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[]),
        Mode::Echo,
        4,
        TlsOptions::default(),
    );
    let cfg = certs.client_config(&[&rustls::version::TLS13], &[]);
    let hs: Vec<_> = (0..20)
        .map(|n| {
            let cfg = cfg.clone();
            let addr = srv.addr;
            std::thread::spawn(move || {
                let mut c = tls_client(cfg, addr);
                for m in 0..100usize {
                    let msg: Vec<u8> = (0..50 + m % 9)
                        .map(|i| pattern(n * 977 + m * 13 + i))
                        .collect();
                    c.write_all(&msg).unwrap();
                    let mut got = vec![0u8; msg.len()];
                    c.read_exact(&mut got).unwrap();
                    assert_eq!(got, msg, "client {n} msg {m}");
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    srv.stop();
}

#[test]
fn handshake_timeout_closes_and_reports() {
    let certs = Certs::new();
    let (etx, erx) = channel();
    let etx = Mutex::new(etx);
    let opts = TlsOptions::default()
        .handshake_timeout(Duration::from_millis(200))
        .on_handshake_error(move |e| {
            let _ = etx.lock().unwrap().send(twine_tls::is_handshake_timeout(e));
        });
    let mut srv = TlsServer::start(certs.server_config(&[]), Mode::Echo, 1, opts);
    let mut s = TcpStream::connect(srv.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let t0 = std::time::Instant::now();
    let mut buf = [0u8; 16];
    match s.read(&mut buf) {
        Ok(0) => {}
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("expected close, got {other:?}"),
    }
    assert!(
        t0.elapsed() >= Duration::from_millis(150),
        "closed too early: {:?}",
        t0.elapsed()
    );
    assert!(t0.elapsed() < Duration::from_secs(3));
    assert!(
        erx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "expected timeout error"
    );
    assert!(
        erx.recv_timeout(Duration::from_millis(300)).is_err(),
        "reported more than once"
    );
    // The inner protocol never saw the connection.
    if let Ok(ev) = srv.events.try_recv() {
        panic!("inner protocol got {ev:?}");
    }
    srv.stop();
}

#[test]
fn bad_client_hello_sends_alert_and_closes() {
    let certs = Certs::new();
    let (etx, erx) = channel();
    let etx = Mutex::new(etx);
    let opts = TlsOptions::default().on_handshake_error(move |e| {
        let _ = etx
            .lock()
            .unwrap()
            .send((twine_tls::is_handshake_timeout(e), e.to_string()));
    });
    let mut srv = TlsServer::start(certs.server_config(&[]), Mode::Echo, 1, opts);
    let mut s = TcpStream::connect(srv.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n garbage garbage garbage")
        .unwrap();
    let mut got = Vec::new();
    let _ = s.read_to_end(&mut got);
    assert!(!got.is_empty(), "expected a TLS alert before close");
    assert_eq!(got[0], 21, "expected alert record, got {got:?}");
    let (timeout, msg) = erx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(!timeout, "{msg}");
    assert!(srv.events.try_recv().is_err());
    srv.stop();
}

#[test]
fn inner_lose_connection_sends_close_notify() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[]),
        Mode::ByeCloses,
        1,
        TlsOptions::default(),
    );
    let mut c = tls_client(
        certs.client_config(&[&rustls::version::TLS13], &[]),
        srv.addr,
    );
    c.write_all(b"bye").unwrap();
    let mut out = Vec::new();
    // read_to_end only succeeds if the peer sent close_notify before EOF.
    c.read_to_end(&mut out).expect("clean close_notify");
    assert_eq!(out, b"farewell");
    // The server half-closed; it finishes once we close our end.
    drop(c);
    srv.wait_ev(|e| matches!(e, Ev::Lost(_)));
    srv.stop();
}

#[test]
fn client_close_notify_reaches_inner_as_clean_loss() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[]),
        Mode::Echo,
        1,
        TlsOptions::default(),
    );
    let mut c = tls_client(
        certs.client_config(&[&rustls::version::TLS12], &[]),
        srv.addr,
    );
    c.write_all(b"hi").unwrap();
    let mut b = [0u8; 2];
    c.read_exact(&mut b).unwrap();
    c.conn.send_close_notify();
    c.flush().unwrap();
    drop(c);
    match srv.wait_ev(|e| matches!(e, Ev::Lost(_))) {
        Ev::Lost(with_reason) => assert!(!with_reason),
        _ => unreachable!(),
    }
    srv.stop();
}

#[test]
fn handle_write_from_other_thread_is_encrypted_and_alpn() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[b"http/1.1"]),
        Mode::Silent,
        1,
        TlsOptions::default(),
    );
    let mut c = tls_client(
        certs.client_config(&[&rustls::version::TLS13], &[b"h2", b"http/1.1"]),
        srv.addr,
    );
    // Drive the handshake, then the server's inner protocol is up.
    c.write_all(b"x").unwrap();
    let h = match srv.wait_ev(|e| matches!(e, Ev::Made(_))) {
        Ev::Made(h) => h,
        _ => unreachable!(),
    };
    std::thread::spawn({
        let h = h.clone();
        move || h.write("hello from another thread")
    })
    .join()
    .unwrap();
    let mut got = vec![0u8; 25];
    c.read_exact(&mut got).unwrap();
    assert_eq!(got, b"hello from another thread");
    assert_eq!(c.conn.alpn_protocol(), Some(&b"http/1.1"[..]));

    // with_tls reports ALPN server-side.
    let (tx, rx) = channel();
    let id = h.id();
    h.loop_handle()
        .call_from_thread(move |lp| {
            let r = twine_tls::with_tls(lp, id, |tls, _t| {
                (
                    tls.alpn_protocol().map(|a| a.to_vec()),
                    tls.is_handshaking(),
                    tls.kernel_offloaded(),
                    tls.inner().type_name().contains("Probe"),
                )
            });
            tx.send(r.unwrap()).unwrap();
        })
        .unwrap();
    let (alpn, hs, ktls, inner_name) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(alpn.as_deref(), Some(&b"http/1.1"[..]));
    assert!(!hs);
    assert!(!ktls);
    assert!(inner_name);
    srv.stop();
}

#[test]
fn client_wrapper_against_std_rustls_server() {
    let certs = Certs::new();
    let scfg = certs.server_config(&[]);
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let st = std::thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = StreamOwned::new(ServerConnection::new(scfg).unwrap(), s);
        let mut b = [0u8; 4];
        c.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"ping");
        c.write_all(b"pong").unwrap();
        c.conn.send_close_notify();
        let _ = c.flush();
    });

    let eng = Arc::new(
        Engine::new(
            Options::default()
                .loops(1)
                .shutdown_timeout(Duration::from_millis(500)),
        )
        .unwrap(),
    );
    let e2 = eng.clone();
    let run = std::thread::spawn(move || e2.run());

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    let inner = twine::factory(move |_, _| {
        Box::new(Pinger {
            tx: tx.lock().unwrap().clone(),
        })
    });
    let f = twine_tls::client_factory(
        certs.client_config(&[&rustls::version::TLS13], &[]),
        localhost(),
        inner,
        TlsOptions::default(),
    );
    let lh = eng.next_loop();
    let h = lh
        .connect_tcp(addr, f, DialOptions::default())
        .wait()
        .unwrap();
    let mut got = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
            Ev::Data(d) => got.extend_from_slice(&d),
            Ev::Lost(_) => break,
            Ev::Made(_) => {}
        }
    }
    assert_eq!(got, b"pong");
    let _ = h;
    st.join().unwrap();
    eng.stop();
    run.join().unwrap().unwrap();
}

struct Pinger {
    tx: std::sync::mpsc::Sender<Ev>,
}
impl twine::Protocol for Pinger {
    fn connection_made(&mut self, t: &mut twine::Transport<'_>) {
        t.write(b"ping");
        let _ = self.tx.send(Ev::Made(t.handle()));
    }
    fn data_received(&mut self, _t: &mut twine::Transport<'_>, d: &mut [u8]) {
        let _ = self.tx.send(Ev::Data(d.to_vec()));
    }
    fn connection_lost(&mut self, _t: &mut twine::Transport<'_>, r: Option<twine::Error>) {
        let _ = self.tx.send(Ev::Lost(r.is_some()));
    }
}
