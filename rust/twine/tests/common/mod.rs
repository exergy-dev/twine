//! Shared helpers for the integration tests: start an engine on a background
//! thread, find its port, stop and join it.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use twine::{Engine, Factory, ListenOptions, Listener, Options};

/// A running engine plus its first listener.
pub struct Server {
    pub eng: Arc<Engine>,
    pub lst: Listener,
    pub addr: SocketAddr,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl Server {
    /// Listens (before `run`) on 127.0.0.1:0 and runs the engine on a thread.
    pub fn start(loops: usize, f: impl Factory) -> Server {
        Self::start_with(
            Options::default()
                .loops(loops)
                .shutdown_timeout(Duration::from_millis(500)),
            ListenOptions::default(),
            f,
        )
    }

    pub fn start_with(opts: Options, lopts: ListenOptions, f: impl Factory) -> Server {
        let eng = Arc::new(Engine::new(opts).expect("engine"));
        let lst = eng.listen_tcp("127.0.0.1:0", f, lopts).expect("listen");
        let addr = lst.addr();
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        Server {
            eng,
            lst,
            addr,
            join,
        }
    }

    pub fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(self.addr).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        s.set_nodelay(true).unwrap();
        s
    }

    /// Stops the engine and joins the run thread.
    pub fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().expect("run thread").expect("run result");
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.eng.stop();
            if let Some(j) = self.join.take() {
                let _ = j.join();
            }
        }
    }
}

/// Polls `cond` until true or `timeout`; returns whether it became true.
pub fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    cond()
}

/// Reads exactly `n` bytes.
pub fn read_n(s: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut v = vec![0u8; n];
    s.read_exact(&mut v).expect("read_exact");
    v
}

/// Reads until EOF (or a reset) and returns everything received.
pub fn read_to_end_lossy(s: &mut TcpStream) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = [0u8; 65536];
    loop {
        match s.read(&mut buf) {
            Ok(0) => return out,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(_) => return out,
        }
    }
}

/// Deterministic byte pattern for byte-exact checks.
pub fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(2654435761) >> 13) as u8
}

pub fn write_all(s: &mut TcpStream, b: &[u8]) {
    s.write_all(b).expect("write_all");
}
