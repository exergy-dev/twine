//! UDP: echo, batching, large datagrams, IPv6, panics, lifecycle.

mod common;

use std::collections::HashSet;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use common::{pattern, wait_until};
use twine::{DatagramProtocol, DatagramTransport, Engine, ListenOptions, Listener, Options};

/// Counters shared between a test and its protocol.
#[derive(Default)]
struct Probe {
    starts: AtomicUsize,
    stops: AtomicUsize,
    /// A datagram arrived before `start_protocol` ran.
    early: AtomicBool,
}

struct Echo {
    p: Arc<Probe>,
}

impl DatagramProtocol for Echo {
    fn start_protocol(&mut self, t: &mut DatagramTransport<'_>) {
        assert!(t.host().port() != 0);
        self.p.starts.fetch_add(1, SeqCst);
    }
    fn datagram_received(
        &mut self,
        t: &mut DatagramTransport<'_>,
        data: &mut [u8],
        from: SocketAddr,
    ) {
        if self.p.starts.load(SeqCst) == 0 {
            self.p.early.store(true, SeqCst);
        }
        if data.starts_with(b"boom") {
            panic!("boom in datagram_received");
        }
        t.write_to(data, from);
    }
    fn stop_protocol(&mut self) {
        self.p.stops.fetch_add(1, SeqCst);
    }
}

struct Srv {
    eng: Arc<Engine>,
    lst: Listener,
    addr: SocketAddr,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl Srv {
    fn start(
        bind: &str,
        mk: impl FnOnce() -> Box<dyn DatagramProtocol> + Send + 'static,
    ) -> Option<Srv> {
        let eng = Arc::new(
            Engine::new(
                Options::default()
                    .loops(2)
                    .shutdown_timeout(Duration::from_millis(500)),
            )
            .unwrap(),
        );
        let lst = match eng.listen_udp(bind, move |_| mk(), ListenOptions::default()) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("skipping: cannot bind {bind}: {e}");
                return None;
            }
        };
        let addr = lst.addr();
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        Some(Srv {
            eng,
            lst,
            addr,
            join,
        })
    }
    fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().unwrap().unwrap();
        }
    }
}

impl Drop for Srv {
    fn drop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

fn echo(p: &Arc<Probe>) -> impl FnOnce() -> Box<dyn DatagramProtocol> + Send + 'static {
    let p = p.clone();
    move || Box::new(Echo { p })
}

fn client(to: SocketAddr) -> UdpSocket {
    let bind = if to.is_ipv4() {
        "127.0.0.1:0"
    } else {
        "[::1]:0"
    };
    let s = UdpSocket::bind(bind).unwrap();
    s.connect(to).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s
}

fn roundtrip_sizes(srv: &Srv) {
    let c = client(srv.addr);
    let mut buf = vec![0u8; 65536];
    for i in 0..100usize {
        let len = 1 + i * (60 * 1024 - 1) / 99;
        let msg: Vec<u8> = (0..len).map(|k| pattern(i * 7919 + k)).collect();
        c.send(&msg).unwrap();
        let n = c.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], &msg[..], "datagram {i} ({len} bytes)");
    }
}

#[test]
fn echo_100_sizes_and_lifecycle() {
    let p = Arc::new(Probe::default());
    let mut srv = Srv::start("127.0.0.1:0", echo(&p)).unwrap();
    roundtrip_sizes(&srv);
    assert_eq!(p.starts.load(SeqCst), 1);
    assert!(!p.early.load(SeqCst), "datagram before start_protocol");
    let m = srv
        .eng
        .loop_handles()
        .iter()
        .map(|l| l.metrics())
        .fold((0, 0), |a, m| (a.0 + m.datagrams, a.1 + m.datagrams_sent));
    assert!(m.0 >= 100 && m.1 >= 100, "metrics {m:?}");
    // Listener::close stops the protocol exactly once.
    srv.lst.close();
    srv.lst.close();
    assert!(wait_until(Duration::from_secs(3), || p.stops.load(SeqCst) == 1));
    std::thread::sleep(Duration::from_millis(100));
    srv.stop();
    assert_eq!(p.stops.load(SeqCst), 1);
    assert_eq!(p.starts.load(SeqCst), 1);
}

#[test]
fn large_datagram_over_8k() {
    let p = Arc::new(Probe::default());
    let srv = Srv::start("127.0.0.1:0", echo(&p)).unwrap();
    let c = client(srv.addr);
    let mut buf = vec![0u8; 65536];
    for len in [8192usize, 8193, 9000, 30000, 65000] {
        let msg: Vec<u8> = (0..len).map(pattern).collect();
        c.send(&msg).unwrap();
        let n = c.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], &msg[..], "{len} bytes");
    }
}

#[test]
fn batch_of_200_back_to_back() {
    let p = Arc::new(Probe::default());
    let mut srv = Srv::start("127.0.0.1:0", echo(&p)).unwrap();
    let c = client(srv.addr);
    for i in 0..200u32 {
        let mut m = i.to_be_bytes().to_vec();
        m.extend((0..60).map(|k| pattern(i as usize * 3 + k)));
        c.send(&m).unwrap();
    }
    let mut seen = HashSet::new();
    let mut buf = [0u8; 2048];
    while seen.len() < 200 {
        let n = c.recv(&mut buf).expect("echo missing");
        let i = u32::from_be_bytes(buf[..4].try_into().unwrap());
        let want: Vec<u8> = (0..60).map(|k| pattern(i as usize * 3 + k)).collect();
        assert_eq!(&buf[4..n], &want[..]);
        seen.insert(i);
    }
    srv.stop();
}

#[test]
fn ipv6_echo() {
    let p = Arc::new(Probe::default());
    let Some(mut srv) = Srv::start("[::1]:0", echo(&p)) else {
        return;
    };
    roundtrip_sizes(&srv);
    srv.stop();
    assert_eq!(p.stops.load(SeqCst), 1);
}

#[test]
fn panic_closes_socket_and_stops_protocol() {
    let p = Arc::new(Probe::default());
    let mut srv = Srv::start("127.0.0.1:0", echo(&p)).unwrap();
    let c = client(srv.addr);
    c.send(b"hello").unwrap();
    let mut buf = [0u8; 64];
    assert_eq!(c.recv(&mut buf).unwrap(), 5);
    c.send(b"boom!").unwrap();
    assert!(wait_until(Duration::from_secs(3), || p.stops.load(SeqCst) == 1));
    // The socket is closed: nothing echoes any more.
    c.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let _ = c.send(b"hello again");
    assert!(c.recv(&mut buf).is_err());
    srv.stop();
    assert_eq!(p.stops.load(SeqCst), 1);
}

#[test]
fn engine_shutdown_stops_once() {
    let p = Arc::new(Probe::default());
    let mut srv = Srv::start("127.0.0.1:0", echo(&p)).unwrap();
    roundtrip_sizes(&srv);
    srv.stop();
    assert_eq!(p.stops.load(SeqCst), 1);
    srv.lst.close(); // after the fact: must not stop it again
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(p.stops.load(SeqCst), 1);
}

/// Replies to each datagram "to <addr>" by sending "hello" to `<addr>` instead.
struct Forward;

impl DatagramProtocol for Forward {
    fn datagram_received(
        &mut self,
        t: &mut DatagramTransport<'_>,
        data: &mut [u8],
        _from: SocketAddr,
    ) {
        let s = std::str::from_utf8(data).unwrap();
        let to: SocketAddr = s.strip_prefix("to ").unwrap().parse().unwrap();
        t.write_to(b"hello", to);
        t.write_to(&vec![7u8; 20000], to); // over 8 KiB: direct sendto
    }
}

#[test]
fn write_to_other_destination() {
    let srv = Srv::start("127.0.0.1:0", || Box::new(Forward)).unwrap();
    let a = client(srv.addr);
    let b = UdpSocket::bind("127.0.0.1:0").unwrap();
    b.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    a.send(format!("to {}", b.local_addr().unwrap()).as_bytes())
        .unwrap();
    let mut buf = vec![0u8; 65536];
    let mut got = Vec::new();
    for _ in 0..2 {
        let (n, from) = b.recv_from(&mut buf).unwrap();
        assert_eq!(from, srv.addr);
        got.push(buf[..n].to_vec());
    }
    got.sort_by_key(|g| g.len());
    assert_eq!(got[0], b"hello");
    assert_eq!(got[1], vec![7u8; 20000]);
    a.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    assert!(a.recv(&mut buf).is_err(), "A must not get anything");
}
