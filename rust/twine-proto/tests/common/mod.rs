//! Shared test helpers: an xorshift PRNG and a one-listener engine.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use twine::{Engine, Factory, ListenOptions, Options};

/// xorshift64* PRNG (no `rand` dependency).
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Uniform-ish value in `0..n` (`n > 0`).
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    /// Value in `lo..=hi`.
    pub fn range(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below(hi - lo + 1)
    }
}

/// A running single-loop engine with one listener on 127.0.0.1:0.
pub struct Server {
    pub eng: Arc<Engine>,
    pub addr: SocketAddr,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl Server {
    pub fn start(f: impl Factory) -> Server {
        let eng = Arc::new(
            Engine::new(
                Options::default()
                    .loops(1)
                    .shutdown_timeout(Duration::from_millis(500)),
            )
            .expect("engine"),
        );
        let lst = eng
            .listen_tcp("127.0.0.1:0", f, ListenOptions::default())
            .expect("listen");
        let addr = lst.addr();
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        Server { eng, addr, join }
    }

    pub fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(self.addr).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        s.set_nodelay(true).unwrap();
        s
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Writes `data` in chunks of the given cycling sizes, flushing between them.
pub fn write_chunked(s: &mut TcpStream, data: &[u8], sizes: &[usize]) {
    let mut i = 0;
    let mut k = 0;
    while i < data.len() {
        let n = sizes[k % sizes.len()].min(data.len() - i);
        s.write_all(&data[i..i + n]).unwrap();
        s.flush().unwrap();
        i += n;
        k += 1;
    }
}

/// Reads exactly `n` bytes.
pub fn read_n(s: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    s.read_exact(&mut v).unwrap();
    v
}

/// Polls `cond` until true or `timeout`.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

/// Reads until EOF or error; returns what was read.
pub fn read_to_close(s: &mut TcpStream) -> Vec<u8> {
    let mut v = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => return v,
            Ok(n) => v.extend_from_slice(&buf[..n]),
        }
    }
}

/// A running engine with `loops` loops and no listeners; stopped on drop.
pub struct Rig {
    pub eng: Arc<Engine>,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl Rig {
    pub fn start(loops: usize) -> Rig {
        let eng = Arc::new(
            Engine::new(
                Options::default()
                    .loops(loops)
                    .shutdown_timeout(Duration::from_millis(500)),
            )
            .expect("engine"),
        );
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        Rig { eng, join }
    }

    pub fn listen(&self, addr: &str, f: impl Factory) -> twine::Listener {
        self.eng
            .listen_tcp(addr, f, ListenOptions::default())
            .expect("listen")
    }

    pub fn lh(&self, i: usize) -> twine::LoopHandle {
        self.eng.loop_handles()[i].clone()
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// A loopback address with (very likely) nothing listening on it.
pub fn closed_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}
