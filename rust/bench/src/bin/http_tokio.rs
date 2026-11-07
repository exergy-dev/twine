//! Hand-written minimal HTTP/1.1 hello server on tokio: keep-alive and
//! pipelining, `\r\n\r\n` boundary scan, `200` with `Content-Length: 13` and a
//! `Date` header. The external baseline for `http_twine`.
//!
//! Flags: `--addr` (127.0.0.1:8080), `--workers N` (0 = one per CPU).
#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use twine_bench::{http_date, Args};

const USAGE: &str = "usage: http_tokio [--addr 127.0.0.1:8080] [--workers N]";
const HEAD: &[u8] = b"HTTP/1.1 200 OK\r\nServer: tokio\r\nContent-Type: text/plain\r\nDate: ";
const TAIL: &[u8] = b"\r\nContent-Length: 13\r\n\r\nHello, World!";

thread_local! {
    /// (unix second, full response) cached per worker thread.
    static RESP: RefCell<(u64, Vec<u8>)> = const { RefCell::new((0, Vec::new())) };
}

fn with_response<R>(f: impl FnOnce(&[u8]) -> R) -> R {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    RESP.with(|c| {
        let mut c = c.borrow_mut();
        if c.0 != now {
            c.1.clear();
            c.1.extend_from_slice(HEAD);
            c.1.extend_from_slice(&http_date(now));
            c.1.extend_from_slice(TAIL);
            c.0 = now;
        }
        f(&c.1)
    })
}

async fn serve(mut s: TcpStream) {
    let _ = s.set_nodelay(true);
    let mut buf = vec![0u8; 64 * 1024];
    let mut have = 0usize;
    let mut out = Vec::with_capacity(4096);
    loop {
        let n = match s.read(&mut buf[have..]).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let end = have + n;
        let (mut start, mut reqs) = (0usize, 0usize);
        // Count complete requests (GET only, no bodies). Unconsumed bytes are
        // compacted to the front and rescanned, so a split boundary is found.
        let mut i = start;
        while i + 4 <= end {
            if &buf[i..i + 4] == b"\r\n\r\n" {
                reqs += 1;
                i += 4;
                start = i;
            } else {
                i += 1;
            }
        }
        if reqs > 0 {
            out.clear();
            with_response(|r| {
                for _ in 0..reqs {
                    out.extend_from_slice(r);
                }
            });
            if s.write_all(&out).await.is_err() {
                return;
            }
        }
        buf.copy_within(start..end, 0);
        have = end - start;
        if have == buf.len() {
            return; // request head larger than the buffer
        }
    }
}

fn main() -> std::io::Result<()> {
    let a = Args::parse(USAGE, &["addr", "workers"]);
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_io();
    if a.usize("workers", 0) > 0 {
        rt.worker_threads(a.usize("workers", 0));
    }
    rt.build()?.block_on(async {
        let l = TcpListener::bind(a.str("addr", "127.0.0.1:8080")).await?;
        eprintln!("http_tokio listening on {}", l.local_addr()?);
        loop {
            let (s, _) = l.accept().await?;
            tokio::spawn(serve(s));
        }
    })
}
