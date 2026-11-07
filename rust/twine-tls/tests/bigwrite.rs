//! Large writes through the TLS filter (more than rustls' 64 KiB buffer),
//! truncation vs close_notify reporting, record errors, HTTP over TLS.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use rustls::{ClientConnection, StreamOwned};
use twine::{factory, Engine, ListenOptions, Options, Protocol, Transport};
use twine_tls::TlsOptions;

fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(131) ^ (i >> 9)) as u8
}

fn payload(n: usize) -> Vec<u8> {
    (0..n).map(pattern).collect()
}

#[derive(Clone, Copy)]
enum How {
    /// One `write` from `connection_made`.
    WriteMade,
    /// One `write` on the first received byte (handshake long done).
    WriteData,
    /// `writev` of head + body + tail from `connection_made`.
    WritevMade,
}

struct Big {
    how: How,
    data: Vec<u8>,
    ev: Sender<String>,
}

impl Big {
    fn send(&self, t: &mut Transport<'_>) {
        match self.how {
            How::WriteMade | How::WriteData => t.write(&self.data),
            How::WritevMade => {
                let n = self.data.len();
                t.writev(&[
                    &self.data[..16],
                    &self.data[16..n - 16],
                    &self.data[n - 16..],
                ]);
            }
        }
    }
}

impl Protocol for Big {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        if !matches!(self.how, How::WriteData) {
            self.send(t);
        }
    }
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        if matches!(self.how, How::WriteData) {
            self.send(t);
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<twine::Error>) {
        let _ = self.ev.send(format!("{r:?}"));
    }
}

fn run_big(how: How, len: usize, version: &'static rustls::SupportedProtocolVersion) {
    let certs = Certs::new();
    let (tx, _rx) = channel();
    let tx = Mutex::new(tx);
    let data = payload(len);
    let d2 = data.clone();
    let inner = factory(move |_, _| {
        Box::new(Big {
            how,
            data: d2.clone(),
            ev: tx.lock().unwrap().clone(),
        })
    });
    let mut srv = start_with(certs.server_config(&[]), inner);
    let s = TcpStream::connect(srv.0).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let c = ClientConnection::new(certs.client_config(&[version], &[]), localhost()).unwrap();
    let mut c = StreamOwned::new(c, s);
    c.write_all(b"x").unwrap();
    let mut got = vec![0u8; len];
    c.read_exact(&mut got).unwrap();
    assert!(got == data, "payload mismatch");
    srv.1.stop();
}

/// (addr, server) for an arbitrary inner factory.
fn start_with(
    cfg: Arc<rustls::ServerConfig>,
    inner: impl twine::Factory,
) -> (std::net::SocketAddr, Stopper) {
    let eng = Arc::new(
        Engine::new(
            Options::default()
                .loops(1)
                .shutdown_timeout(Duration::from_millis(500)),
        )
        .unwrap(),
    );
    let lst = twine_tls::listen(
        &eng.handle(),
        "127.0.0.1:0",
        cfg,
        inner,
        TlsOptions::default(),
        ListenOptions::default(),
    )
    .unwrap();
    let addr = lst.addr();
    let e2 = eng.clone();
    let join = Some(std::thread::spawn(move || e2.run()));
    (addr, Stopper { eng, join })
}

struct Stopper {
    eng: Arc<Engine>,
    join: Option<std::thread::JoinHandle<twine::Result<()>>>,
}
impl Stopper {
    fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().unwrap().unwrap();
        }
    }
}
impl Drop for Stopper {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.stop();
        }
    }
}

#[test]
fn write_1mib_after_handshake_tls13() {
    run_big(How::WriteData, 1 << 20, &rustls::version::TLS13);
}

#[test]
fn write_1mib_after_handshake_tls12() {
    run_big(How::WriteData, 1 << 20, &rustls::version::TLS12);
}

#[test]
fn write_1mib_from_connection_made() {
    run_big(How::WriteMade, 1 << 20, &rustls::version::TLS13);
    run_big(How::WriteMade, 1 << 20, &rustls::version::TLS12);
}

#[test]
fn writev_head_body_tail_300kib() {
    run_big(
        How::WritevMade,
        16 + 300 * 1024 + 16,
        &rustls::version::TLS13,
    );
    run_big(
        How::WritevMade,
        16 + 300 * 1024 + 16,
        &rustls::version::TLS12,
    );
}

#[test]
fn http_over_tls_200kib_body() {
    let certs = Certs::new();
    let body = Arc::new(payload(200 * 1024));
    let b2 = body.clone();
    let server = twine_http::Server::new(twine_http::handler(move |ctx| ctx.write(&b2)));
    let eng = Arc::new(
        Engine::new(
            Options::default()
                .loops(1)
                .shutdown_timeout(Duration::from_millis(500)),
        )
        .unwrap(),
    );
    let inner = server.factory(&eng.handle());
    let lst = twine_tls::listen(
        &eng.handle(),
        "127.0.0.1:0",
        certs.server_config(&[]),
        inner,
        TlsOptions::default(),
        ListenOptions::default(),
    )
    .unwrap();
    let addr = lst.addr();
    let e2 = eng.clone();
    let join = std::thread::spawn(move || e2.run());
    let s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let c = ClientConnection::new(
        certs.client_config(&[&rustls::version::TLS13], &[]),
        localhost(),
    )
    .unwrap();
    let mut c = StreamOwned::new(c, s);
    for _ in 0..2 {
        c.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        let mut head = Vec::new();
        let mut b = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            c.read_exact(&mut b).unwrap();
            head.push(b[0]);
        }
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert!(head.contains("Content-Length: 204800"), "{head}");
        let mut got = vec![0u8; body.len()];
        c.read_exact(&mut got).unwrap();
        assert!(got == *body);
    }
    eng.stop();
    join.join().unwrap().unwrap();
}

// ---- loss reasons ----

struct Rec {
    ev: Sender<String>,
}
impl Protocol for Rec {
    fn data_received(&mut self, _t: &mut Transport<'_>, d: &mut [u8]) {
        let _ = self.ev.send(format!("data {}", d.len()));
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<twine::Error>) {
        let s = match r {
            None => "none".to_string(),
            Some(twine::Error::Io(e)) => format!("io {:?}", e.kind()),
            Some(e) => format!("other {e}"),
        };
        let _ = self.ev.send(format!("lost {s}"));
    }
}

fn rec_server() -> (std::net::SocketAddr, Stopper, Receiver<String>, Certs) {
    let certs = Certs::new();
    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    let inner = factory(move |_, _| {
        Box::new(Rec {
            ev: tx.lock().unwrap().clone(),
        })
    });
    let (addr, st) = start_with(certs.server_config(&[]), inner);
    (addr, st, rx, certs)
}

fn lost_reason(rx: &Receiver<String>) -> String {
    loop {
        let m = rx.recv_timeout(Duration::from_secs(10)).expect("event");
        if let Some(r) = m.strip_prefix("lost ") {
            return r.to_string();
        }
    }
}

/// Handshakes by hand over `s` and returns the connection.
fn handshake(
    certs: &Certs,
    s: &mut TcpStream,
    v: &'static rustls::SupportedProtocolVersion,
) -> ClientConnection {
    let mut c = ClientConnection::new(certs.client_config(&[v], &[]), localhost()).unwrap();
    while c.is_handshaking() {
        c.complete_io(s).unwrap();
    }
    c
}

fn drain_until_eof(s: &mut TcpStream) {
    let mut b = [0u8; 4096];
    while matches!(s.read(&mut b), Ok(n) if n > 0) {}
}

#[test]
fn truncation_without_close_notify_is_unexpected_eof() {
    for v in [&rustls::version::TLS13, &rustls::version::TLS12] {
        let (addr, mut st, rx, certs) = rec_server();
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = handshake(&certs, &mut s, v);
        c.writer().write_all(b"hello").unwrap();
        c.complete_io(&mut s).unwrap();
        s.shutdown(std::net::Shutdown::Write).unwrap();
        drain_until_eof(&mut s);
        assert_eq!(lost_reason(&rx), "io UnexpectedEof");
        st.stop();
    }
}

#[test]
fn close_notify_is_clean() {
    for v in [&rustls::version::TLS13, &rustls::version::TLS12] {
        let (addr, mut st, rx, certs) = rec_server();
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = handshake(&certs, &mut s, v);
        c.writer().write_all(b"hello").unwrap();
        c.send_close_notify();
        c.complete_io(&mut s).unwrap();
        s.shutdown(std::net::Shutdown::Write).unwrap();
        drain_until_eof(&mut s);
        assert_eq!(lost_reason(&rx), "none");
        st.stop();
    }
}

#[test]
fn bad_record_after_handshake_reports_invalid_data() {
    for v in [&rustls::version::TLS13, &rustls::version::TLS12] {
        let (addr, mut st, rx, certs) = rec_server();
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = handshake(&certs, &mut s, v);
        c.writer().write_all(b"good").unwrap();
        c.complete_io(&mut s).unwrap();
        // The good data is delivered, then a record that cannot decrypt.
        let mut junk = vec![23u8, 3, 3, 0, 40];
        junk.extend((0..40).map(|i| i as u8));
        s.write_all(&junk).unwrap();
        drain_until_eof(&mut s);
        // The server's close is graceful: it finishes once we close too.
        drop(s);
        assert_eq!(lost_reason(&rx), "io InvalidData");
        st.stop();
    }
}
