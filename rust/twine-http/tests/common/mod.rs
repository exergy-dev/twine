//! Shared helpers: start an engine running an HTTP server, and a tiny
//! hand-written HTTP/1.1 client over `std::net::TcpStream`.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use twine::{Engine, ListenOptions, Options};
use twine_http::{Handler, Server};

/// A running engine with one HTTP listener.
pub struct TestServer {
    pub eng: Arc<Engine>,
    pub addr: SocketAddr,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl TestServer {
    pub fn start<H: Handler>(server: Server<H>) -> TestServer {
        Self::start_with(
            Options::default()
                .loops(1)
                .shutdown_timeout(Duration::from_millis(500)),
            server,
        )
    }

    pub fn start_with<H: Handler>(opts: Options, server: Server<H>) -> TestServer {
        let eng = Arc::new(Engine::new(opts).expect("engine"));
        let lst = server
            .listen(&eng, "127.0.0.1:0", ListenOptions::default())
            .expect("listen");
        let addr = lst.addr();
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        TestServer { eng, addr, join }
    }

    pub fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(self.addr).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.set_nodelay(true).unwrap();
        s
    }

    pub fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().expect("run thread").expect("run result");
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if self.join.is_some() {
            self.eng.stop();
            if let Some(j) = self.join.take() {
                let _ = j.join();
            }
        }
    }
}

/// A parsed response.
#[derive(Debug)]
pub struct Resp {
    pub status_line: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub raw_head: String,
}

impl Resp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Reads one response (head plus Content-Length body) from `s`, consuming
/// exactly its bytes from `pending` first. Returns `None` on EOF or timeout
/// before any byte arrived.
pub struct Reader {
    pub s: TcpStream,
    pending: Vec<u8>,
    /// The next response answers HEAD: no body despite Content-Length.
    pub head_only: bool,
}

impl Reader {
    pub fn new(s: TcpStream) -> Reader {
        Reader {
            s,
            pending: Vec::new(),
            head_only: false,
        }
    }

    fn fill(&mut self) -> bool {
        let mut b = [0u8; 16384];
        match self.s.read(&mut b) {
            Ok(0) | Err(_) => false,
            Ok(n) => {
                self.pending.extend_from_slice(&b[..n]);
                true
            }
        }
    }

    pub fn read_response(&mut self) -> Option<Resp> {
        let head_end = loop {
            if let Some(p) = self.pending.windows(4).position(|w| w == b"\r\n\r\n") {
                break p + 4;
            }
            if !self.fill() {
                return None;
            }
        };
        let head = String::from_utf8_lossy(&self.pending[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap().to_string();
        let status: u16 = status_line.split(' ').nth(1).unwrap().parse().unwrap();
        let mut headers = Vec::new();
        for l in lines {
            if let Some((n, v)) = l.split_once(':') {
                headers.push((n.to_string(), v.trim().to_string()));
            }
        }
        let cl: usize = headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
            .map(|(_, v)| v.parse().unwrap())
            .unwrap_or(0);
        let no_body = status < 200 || status == 204 || status == 304;
        let cl = if no_body || std::mem::take(&mut self.head_only) {
            0
        } else {
            cl
        };
        while self.pending.len() < head_end + cl {
            if !self.fill() {
                return None;
            }
        }
        let body = self.pending[head_end..head_end + cl].to_vec();
        self.pending.drain(..head_end + cl);
        Some(Resp {
            status_line,
            status,
            headers,
            body,
            raw_head: head,
        })
    }

    /// Reads until EOF; returns everything left (pending plus socket).
    pub fn read_to_end(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.pending);
        let mut b = [0u8; 16384];
        loop {
            match self.s.read(&mut b) {
                Ok(0) | Err(_) => return out,
                Ok(n) => out.extend_from_slice(&b[..n]),
            }
        }
    }

    pub fn write(&mut self, b: &[u8]) {
        self.s.write_all(b).expect("write");
    }
}
