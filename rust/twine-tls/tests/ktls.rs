//! kernel TLS offload tests. On a host whose kernel lacks the `tls` module
//! every test still passes through the user-space fallback (it asserts
//! `kernel_offloaded() == false` when `ktls_available()` is false).

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use rustls::crypto::ring as ring_provider;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use twine::{DialOptions, Engine, Options, Protocol, Transport, TransportHandle};
use twine_tls::TlsOptions;

fn ktls_opts(on: bool) -> TlsOptions {
    TlsOptions::default().kernel_offload(on)
}

/// Connects and finishes the handshake, then waits for the server's
/// `connection_made` (so the server has seen the client's Finished and taken
/// its kTLS decision before any data is sent). Returns the server-side handle.
fn connect(
    cfg: Arc<ClientConfig>,
    srv: &TlsServer,
) -> (StreamOwned<ClientConnection, TcpStream>, TransportHandle) {
    let mut c = connect_eager(cfg, srv);
    while c.conn.is_handshaking() {
        c.conn.complete_io(&mut c.sock).unwrap();
    }
    // A flight that only the client's Finished completes: flush it now.
    while c.conn.wants_write() {
        c.conn.write_tls(&mut c.sock).unwrap();
    }
    let h = made(srv);
    (c, h)
}

/// Connects lazily: the first `write_all` sends Finished and data together.
fn connect_eager(
    cfg: Arc<ClientConfig>,
    srv: &TlsServer,
) -> StreamOwned<ClientConnection, TcpStream> {
    let s = TcpStream::connect(srv.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    StreamOwned::new(ClientConnection::new(cfg, localhost()).unwrap(), s)
}

/// Exchanges 1 MiB in each direction, byte-exact, returning the handle.
fn mib_echo(c: &mut StreamOwned<ClientConnection, TcpStream>) {
    let total = 1 << 20;
    let chunk = 16 * 1024;
    // Write-all then read-all would deadlock on socket buffers; interleave.
    let mut sent = 0usize;
    while sent < total {
        let buf: Vec<u8> = (sent..sent + chunk).map(pattern).collect();
        c.write_all(&buf).unwrap();
        let mut got = vec![0u8; chunk];
        c.read_exact(&mut got).unwrap();
        assert_eq!(got, buf, "mismatch at {sent}");
        sent += chunk;
    }
}

/// `must_offload`: TX offload is required when the kernel is available;
/// `kernel_rx`: the server asks for kernel RX (`TlsOptions::kernel_rx`) and the
/// client waits for the server before sending, so kernel RX is expected.
/// Without `kernel_rx` the receive side must stay in user space.
fn suite_test(
    suite: rustls::SupportedCipherSuite,
    must_offload: bool,
    kernel_rx: bool,
    wait: bool,
    name: &str,
) {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        ktls_server_config(&certs),
        Mode::ByeCloses,
        2,
        ktls_opts(true).kernel_rx(kernel_rx),
    );
    let cfg = client_cfg(&certs, Some(suite), false);
    let (mut c, h) = if wait {
        let (c, h) = connect(cfg, &srv);
        (c, Some(h))
    } else {
        (connect_eager(cfg, &srv), None)
    };
    mib_echo(&mut c);
    // Eager clients complete the handshake with their first write.
    let h = h.unwrap_or_else(|| made(&srv));
    let (off, rx) = query(&h);
    eprintln!(
        "{name}: ktls_available={} kernel_offloaded={off} kernel_rx={rx}",
        twine_tls::ktls_available()
    );
    if !twine_tls::ktls_available() {
        assert!(!off && !rx);
    } else {
        if must_offload {
            assert!(off, "{name}: expected TX offload");
        }
        if !kernel_rx {
            assert!(!rx, "{name}: RX must stay in user space by default");
        } else if wait && must_offload {
            assert!(rx, "{name}: kernel RX");
        }
    }
    // Server-initiated close: the client sees "farewell" then a clean
    // close_notify (read returns Ok(0), not UnexpectedEof).
    c.write_all(b"bye").unwrap();
    let mut rest = Vec::new();
    c.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"farewell");
    drop(c);
    let lost = srv.wait_ev(|e| matches!(e, Ev::Lost(_)));
    assert!(
        matches!(lost, Ev::Lost(false)),
        "server close was clean: {lost:?}"
    );
    srv.stop();
}

#[test]
fn aes128_gcm_offload() {
    // Default: transmit-only offload, user-space receive.
    suite_test(
        ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256,
        true,
        false,
        true,
        "aes128 (tx only)",
    );
}

#[test]
fn aes128_gcm_offload_kernel_rx() {
    suite_test(
        ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256,
        true,
        true,
        true,
        "aes128 (kernel rx)",
    );
}

#[test]
fn aes256_gcm_offload() {
    suite_test(
        ring_provider::cipher_suite::TLS13_AES_256_GCM_SHA384,
        true,
        true,
        false,
        "aes256 (data right behind Finished, kernel rx requested)",
    );
}

#[test]
fn chacha20_offload_or_fallback() {
    suite_test(
        ring_provider::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        false,
        false,
        true,
        "chacha20",
    );
}

#[test]
fn client_close_notify_with_offload() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(ktls_server_config(&certs), Mode::Echo, 1, ktls_opts(true));
    let (mut c, h) = connect(
        client_cfg(
            &certs,
            Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
            false,
        ),
        &srv,
    );
    c.write_all(b"hi").unwrap();
    let mut b = [0u8; 2];
    c.read_exact(&mut b).unwrap();
    let (off, rx) = query(&h);
    c.conn.send_close_notify();
    c.flush().unwrap();
    // User-space receive: the server answers with its own close_notify (clean
    // EOF). Kernel receive: the client's alert surfaces as EIO, the server
    // drops the connection with an error and the client sees a truncation.
    let mut rest = Vec::new();
    let r = c.read_to_end(&mut rest);
    drop(c);
    let lost = srv.wait_ev(|e| matches!(e, Ev::Lost(_)));
    eprintln!("client close_notify: offloaded={off} rx={rx} client_read={r:?} server={lost:?}");
    if !rx {
        assert!(r.is_ok(), "{r:?}");
        assert!(matches!(lost, Ev::Lost(false)), "{lost:?}");
    }
    srv.stop();
}

#[test]
fn tls12_not_offloaded() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(ktls_server_config(&certs), Mode::Echo, 1, ktls_opts(true));
    let (mut c, h) = connect(client_cfg(&certs, None, false), &srv);
    mib_echo(&mut c);
    assert_eq!(query(&h), (false, false));
    srv.stop();
}

#[test]
fn offload_disabled_by_option() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(ktls_server_config(&certs), Mode::Echo, 1, ktls_opts(false));
    let (mut c, h) = connect(
        client_cfg(
            &certs,
            Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
            false,
        ),
        &srv,
    );
    mib_echo(&mut c);
    assert_eq!(query(&h), (false, false));
    srv.stop();
}

#[test]
fn offload_requested_without_secret_extraction_falls_back() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(certs.server_config(&[]), Mode::Echo, 1, ktls_opts(true));
    let (mut c, h) = connect(
        client_cfg(
            &certs,
            Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
            false,
        ),
        &srv,
    );
    mib_echo(&mut c);
    assert_eq!(query(&h), (false, false));
    srv.stop();
}

#[test]
fn key_update_under_offload_loses_connection() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(ktls_server_config(&certs), Mode::Echo, 1, ktls_opts(true));
    let (mut c, h) = connect(
        client_cfg(
            &certs,
            Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
            false,
        ),
        &srv,
    );
    c.write_all(b"before").unwrap();
    let mut b = [0u8; 6];
    c.read_exact(&mut b).unwrap();
    let (off, rx) = query(&h);
    eprintln!("key_update: offloaded={off} rx={rx}");
    c.conn.refresh_traffic_keys().unwrap();
    c.write_all(b"after").unwrap();
    c.flush().unwrap();
    if off {
        // KeyUpdate cannot be honoured under kTLS: the server drops the
        // connection with an error and does not hang.
        let lost = srv.wait_ev(|e| matches!(e, Ev::Lost(_)));
        assert!(matches!(lost, Ev::Lost(true)), "{lost:?}");
        let mut rest = Vec::new();
        let _ = c.read_to_end(&mut rest); // EOF or reset, but it returns
    } else {
        // Fallback: rustls handles the KeyUpdate and the echo continues.
        let mut b = [0u8; 5];
        c.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"after");
    }
    srv.stop();
}

#[test]
fn openssl_tls13_with_offload() {
    if Command::new("openssl").arg("version").output().is_err() {
        eprintln!("SKIP: openssl not found");
        return;
    }
    let certs = Certs::new();
    let mut srv = TlsServer::start(ktls_server_config(&certs), Mode::Echo, 1, ktls_opts(true));
    let mut child = Command::new("openssl")
        .args([
            "s_client",
            "-connect",
            &srv.addr.to_string(),
            "-tls1_3",
            "-quiet",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    let (ltx, lrx) = channel();
    stdin.write_all(b"hello ktls\n").unwrap();
    stdin.flush().unwrap();
    std::thread::spawn(move || {
        let mut line = String::new();
        let r = out.read_line(&mut line);
        let _ = ltx.send((r.is_ok(), line));
    });
    let got = lrx.recv_timeout(Duration::from_secs(10));
    if got.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("openssl s_client: no echo within 10 s");
    }
    let (ok, line) = got.unwrap();
    assert!(ok);
    assert_eq!(line, "hello ktls\n");
    let (off, rx) = query(&made(&srv));
    eprintln!("openssl: offloaded={off} rx={rx}");
    assert_eq!(off, twine_tls::ktls_available());
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    srv.stop();
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

/// Client side: TX goes to the kernel, RX stays in user space (tickets and
/// close_notify handled by the built-in decryptor), against a std rustls server.
#[test]
fn client_tx_offload_against_std_server() {
    let certs = Certs::new();
    let scfg = certs.server_config(&[]);
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let st = std::thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut c = StreamOwned::new(rustls::ServerConnection::new(scfg).unwrap(), s);
        let mut b = [0u8; 4];
        c.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"ping");
        // 1 MiB back plus a second ping/pong after.
        let big: Vec<u8> = (0..1 << 20).map(pattern).collect();
        c.write_all(&big).unwrap();
        c.write_all(b"pong").unwrap();
        c.flush().unwrap();
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
        client_cfg(
            &certs,
            Some(ring_provider::cipher_suite::TLS13_AES_256_GCM_SHA384),
            true,
        ),
        localhost(),
        inner,
        ktls_opts(true),
    );
    let lh = eng.next_loop();
    let _id = lh
        .connect_tcp(addr, f, DialOptions::default())
        .wait()
        .unwrap();
    let mut got = Vec::new();
    let mut handle = None;
    let mut checked = false;
    let lost = loop {
        match rx.recv_timeout(Duration::from_secs(10)).unwrap() {
            Ev::Data(d) => {
                got.extend_from_slice(&d);
                if !checked && !got.is_empty() {
                    checked = true;
                    if let Some(h) = &handle {
                        let st = query(h);
                        eprintln!("client: (offloaded, kernel_rx) = {st:?}");
                        // The client never uses kernel RX (tickets would break it).
                        assert_eq!(st, (twine_tls::ktls_available(), false));
                    }
                }
            }
            Ev::Lost(l) => break l,
            Ev::Made(h) => handle = Some(h),
        }
    };
    assert!(!lost, "clean close_notify from the server");
    let mut want: Vec<u8> = (0..1 << 20).map(pattern).collect();
    want.extend_from_slice(b"pong");
    assert_eq!(got.len(), want.len());
    assert!(got == want);
    st.join().unwrap();
    eng.stop();
    run.join().unwrap().unwrap();
}

#[test]
fn availability_report() {
    eprintln!(
        "ktls_supported={} ktls_available={}",
        twine_tls::ktls_supported(),
        twine_tls::ktls_available()
    );
    assert_eq!(twine_tls::ktls_supported(), cfg!(target_os = "linux"));
}

/// Drives a TLS 1.3 client handshake by hand up to the point where the client
/// holds a ready Finished it has not yet sent.
fn hand_client(cfg: Arc<ClientConfig>, srv: &TlsServer) -> (ClientConnection, TcpStream) {
    let mut s = TcpStream::connect(srv.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut cc = ClientConnection::new(cfg, localhost()).unwrap();
    while cc.is_handshaking() {
        while cc.wants_write() {
            cc.write_tls(&mut s).unwrap();
        }
        if cc.is_handshaking() {
            assert!(cc.read_tls(&mut s).unwrap() > 0, "server closed early");
            cc.process_new_packets().unwrap();
        }
    }
    (cc, s)
}

/// Everything the client has queued for the wire, as bytes.
fn drain(cc: &mut ClientConnection) -> Vec<u8> {
    let mut v = Vec::new();
    while cc.wants_write() {
        cc.write_tls(&mut v).unwrap();
    }
    v
}

fn aes128_client(certs: &Certs) -> Arc<ClientConfig> {
    client_cfg(
        certs,
        Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
        false,
    )
}

/// The client's Finished and its first application records leave in one
/// `write_all`, with the last record cut short (its tail follows later), so
/// the server holds ciphertext behind the Finished: TX is offloaded, RX stays
/// in user space whatever `kernel_rx` says, and the data round-trips
/// byte-exact.
#[test]
fn data_right_behind_finished_stays_in_user_space_rx() {
    for kernel_rx in [false, true] {
        let certs = Certs::new();
        let mut srv = TlsServer::start(
            ktls_server_config(&certs),
            Mode::Echo,
            1,
            ktls_opts(true).kernel_rx(kernel_rx),
        );
        let (mut cc, mut s) = hand_client(aes128_client(&certs), &srv);
        let mut want = Vec::new();
        for i in 0..4usize {
            let part: Vec<u8> = (i * 1000..i * 1000 + 1000).map(pattern).collect();
            cc.writer().write_all(&part).unwrap();
            want.extend_from_slice(&part);
        }
        // Finished + four data records in a single write, minus the last 7
        // bytes (a partial record header and body).
        let wire = drain(&mut cc);
        let (head, tail) = wire.split_at(wire.len() - 7);
        s.write_all(head).unwrap();
        let h = made(&srv);
        let (off, rx) = query(&h);
        eprintln!("behind Finished (kernel_rx={kernel_rx}): offloaded={off} rx={rx}");
        assert_eq!(off, twine_tls::ktls_available(), "TX offloaded");
        assert!(!rx, "RX in user space");
        s.write_all(tail).unwrap();
        let mut c = StreamOwned::new(cc, s);
        let mut got = vec![0u8; want.len()];
        c.read_exact(&mut got).unwrap();
        assert!(got == want, "echo byte-exact");
        srv.stop();
    }
}

/// Kernel RX: several records arrive in one TCP segment and are all delivered,
/// in order, byte-exact.
#[test]
fn multiple_records_in_one_read_kernel_rx() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        ktls_server_config(&certs),
        Mode::Echo,
        1,
        ktls_opts(true).kernel_rx(true),
    );
    let (mut cc, mut s) = hand_client(aes128_client(&certs), &srv);
    // Finished alone, so the server can take kernel RX.
    s.write_all(&drain(&mut cc)).unwrap();
    let h = made(&srv);
    let (off, rx) = query(&h);
    eprintln!("multi-record: offloaded={off} rx={rx}");
    if twine_tls::ktls_available() {
        assert!(off && rx, "kernel RX expected");
    }
    let mut want = Vec::new();
    for (i, n) in [1usize, 100, 5000, 7, 16000, 3].into_iter().enumerate() {
        let part: Vec<u8> = (0..n).map(|j| pattern(j + i * 77)).collect();
        cc.writer().write_all(&part).unwrap();
        want.extend_from_slice(&part);
    }
    let wire = drain(&mut cc);
    s.write_all(&wire).unwrap();
    let mut c = StreamOwned::new(cc, s);
    let mut got = vec![0u8; want.len()];
    c.read_exact(&mut got).unwrap();
    assert!(got == want, "echo byte-exact");
    srv.stop();
}

/// Writes 2 MiB, then calls `lose_connection` twice in the same callback.
struct BurstClose {
    tx: Sender<Ev>,
}

impl Protocol for BurstClose {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let _ = self.tx.send(Ev::Made(t.handle()));
    }
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        let big: Vec<u8> = (0..2 << 20).map(pattern).collect();
        t.write(&big);
        t.lose_connection();
        t.lose_connection();
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<twine::Error>) {
        let _ = self.tx.send(Ev::Lost(r.is_some()));
    }
}

/// A second `lose_connection` while the close_notify is deferred behind a
/// large unsent write must not cut the stream short or skip the alert.
#[test]
fn double_lose_connection_with_slow_client() {
    for offload in [true, false] {
        let certs = Certs::new();
        let mut srv =
            TlsServer::start_custom(ktls_server_config(&certs), 1, ktls_opts(offload), |tx| {
                Box::new(BurstClose { tx })
            });
        let (mut c, h) = connect(aes128_client(&certs), &srv);
        let (off, _) = query(&h);
        assert_eq!(off, offload && twine_tls::ktls_available());
        c.write_all(b"go").unwrap();
        // Slow client: let the server's socket buffers fill first.
        std::thread::sleep(Duration::from_millis(300));
        let mut got = Vec::new();
        c.read_to_end(&mut got)
            .expect("clean close_notify after all data");
        assert_eq!(got.len(), 2 << 20, "offload={offload}");
        assert!(got.iter().enumerate().all(|(i, &b)| b == pattern(i)));
        drop(c);
        let lost = srv.wait_ev(|e| matches!(e, Ev::Lost(_)));
        assert!(matches!(lost, Ev::Lost(false)), "{lost:?}");
        srv.stop();
    }
}

/// Arms the two values the TLS layer reserves for itself.
struct TimerProbe {
    tx: Sender<Ev>,
    fired: Sender<u64>,
}

impl Protocol for TimerProbe {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.call_later(Duration::from_millis(30), u64::MAX - 1);
        t.call_later(Duration::from_millis(40), u64::MAX);
        let _ = self.tx.send(Ev::Made(t.handle()));
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, d: &mut [u8]) {
        let _ = self.tx.send(Ev::Data(d.to_vec()));
    }
    fn timer_fired(&mut self, _t: &mut Transport<'_>, token: u64) {
        let _ = self.fired.send(token);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<twine::Error>) {
        let _ = self.tx.send(Ev::Lost(r.is_some()));
    }
}

/// An inner protocol using the reserved token values still gets its timers and
/// the connection stays open.
#[test]
fn inner_protocol_reserved_tokens_are_forwarded() {
    for offload in [true, false] {
        let certs = Certs::new();
        let (ftx, frx) = channel();
        let ftx = Mutex::new(ftx);
        let mut srv = TlsServer::start_custom(
            ktls_server_config(&certs),
            1,
            ktls_opts(offload),
            move |tx| {
                Box::new(TimerProbe {
                    tx,
                    fired: ftx.lock().unwrap().clone(),
                })
            },
        );
        let (mut c, _h) = connect(aes128_client(&certs), &srv);
        let mut tokens = vec![
            frx.recv_timeout(Duration::from_secs(5)).unwrap(),
            frx.recv_timeout(Duration::from_secs(5)).unwrap(),
        ];
        tokens.sort_unstable();
        assert_eq!(tokens, [u64::MAX - 1, u64::MAX], "offload={offload}");
        c.write_all(b"still open").unwrap();
        c.flush().unwrap();
        loop {
            match srv.events.recv_timeout(Duration::from_secs(5)).unwrap() {
                Ev::Data(d) => {
                    assert_eq!(d, b"still open");
                    break;
                }
                Ev::Lost(_) => panic!("connection was closed by a reserved token"),
                Ev::Made(_) => {}
            }
        }
        srv.stop();
    }
}
