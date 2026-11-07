//! Allocation gates (design Sections 7.3 and 12). The counter is per thread, so
//! the handler samples `twine_alloccount::count()` on the LOOP thread and
//! reports the largest allocation delta seen between consecutive handler calls.
//! A delta spans the whole loop iteration in between (poll, read, parse, write
//! of the previous response, timers), so a zero is a strong statement.

mod common;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use twine_alloccount::count;
use twine_http::{handler, Server};

#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

const WARM: usize = 30;

struct Gate {
    last: AtomicU64,
    max: AtomicU64,
    samples: AtomicUsize,
}

impl Gate {
    fn new() -> Arc<Gate> {
        Arc::new(Gate {
            last: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
            samples: AtomicUsize::new(0),
        })
    }
    fn sample(&self) {
        let now = count();
        let prev = self.last.swap(now, SeqCst);
        let n = self.samples.fetch_add(1, SeqCst);
        if prev != u64::MAX && n >= WARM {
            self.max.fetch_max(now - prev, SeqCst);
        }
    }
    fn max(&self) -> u64 {
        self.max.load(SeqCst)
    }
    fn measured(&self) -> usize {
        self.samples.load(SeqCst).saturating_sub(WARM)
    }
}

fn gated(g: &Arc<Gate>) -> TestServer {
    let g = g.clone();
    TestServer::start(Server::new(handler(move |ctx| {
        g.sample();
        if ctx.is_method("POST") {
            // Echo the size, not the body, to keep the response small.
            let n = ctx.body().len();
            ctx.write(if n == 1024 { b"1024" } else { b"other" });
        } else {
            ctx.set_body_str("Hello, World!");
        }
    })))
}

#[test]
fn gate_keep_alive_gets_allocate_nothing() {
    let g = Gate::new();
    let srv = gated(&g);
    let mut c = Reader::new(srv.connect());
    for _ in 0..500 {
        c.write(
            b"GET /hello?x=1 HTTP/1.1\r\nHost: localhost\r\nUser-Agent: t\r\nAccept: */*\r\n\r\n",
        );
        assert_eq!(c.read_response().unwrap().body_str(), "Hello, World!");
    }
    eprintln!(
        "GATE keep-alive GET: max allocs between handler calls = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert!(g.measured() >= 400);
    assert_eq!(g.max(), 0);
}

#[test]
fn gate_pipelined_gets_allocate_nothing() {
    let g = Gate::new();
    let srv = gated(&g);
    let mut c = Reader::new(srv.connect());
    let one = b"GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let mut batch = Vec::new();
    for _ in 0..16 {
        batch.extend_from_slice(one);
    }
    for _ in 0..40 {
        c.write(&batch);
        for _ in 0..16 {
            assert_eq!(c.read_response().unwrap().body_str(), "Hello, World!");
        }
    }
    eprintln!(
        "GATE pipelined x16 GET: max allocs between handler calls = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert!(g.measured() >= 500);
    assert_eq!(g.max(), 0);
}

#[test]
fn gate_post_1k_body_allocates_nothing() {
    let g = Gate::new();
    let srv = gated(&g);
    let mut c = Reader::new(srv.connect());
    let body = vec![b'x'; 1024];
    let mut req = b"POST /p HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1024\r\n\r\n".to_vec();
    req.extend_from_slice(&body);
    for _ in 0..500 {
        c.write(&req);
        assert_eq!(c.read_response().unwrap().body_str(), "1024");
    }
    eprintln!(
        "GATE POST 1 KiB: max allocs between handler calls = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert!(g.measured() >= 400);
    assert_eq!(g.max(), 0);
}

#[test]
fn gate_chunked_post_allocates_nothing() {
    let g = Gate::new();
    let srv = gated(&g);
    let mut c = Reader::new(srv.connect());
    let mut req =
        b"POST /p HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    for _ in 0..4 {
        req.extend_from_slice(b"100\r\n");
        req.extend_from_slice(&[b'y'; 256]);
        req.extend_from_slice(b"\r\n");
    }
    req.extend_from_slice(b"0\r\nX-T: 1\r\n\r\n");
    for _ in 0..500 {
        c.write(&req);
        assert_eq!(c.read_response().unwrap().body_str(), "1024");
    }
    eprintln!(
        "GATE chunked POST 4x256: max allocs between handler calls = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert!(g.measured() >= 400);
    assert_eq!(g.max(), 0);
}

#[test]
fn gate_split_request_reassembly_allocates_nothing() {
    // A request arriving in two writes goes through the reassembly buffer; once
    // its capacity is established the path allocates nothing either.
    let g = Gate::new();
    let srv = gated(&g);
    let mut c = Reader::new(srv.connect());
    for _ in 0..300 {
        c.write(b"GET /hello HTTP/1.1\r\nHo");
        std::thread::sleep(Duration::from_micros(200));
        c.write(b"st: localhost\r\n\r\n");
        assert_eq!(c.read_response().unwrap().body_str(), "Hello, World!");
    }
    eprintln!(
        "GATE split GET (reassembly): max allocs between handler calls = {} ({} samples)",
        g.max(),
        g.measured()
    );
    assert_eq!(g.max(), 0);
}

#[test]
fn gate_detects_an_allocating_handler() {
    // Control: the same harness must report allocations when the handler makes one.
    let g = Gate::new();
    let g2 = g.clone();
    let srv = TestServer::start(Server::new(handler(move |ctx| {
        g2.sample();
        let v: Vec<u8> = Vec::with_capacity(64);
        std::hint::black_box(&v);
        ctx.write(b"x");
    })));
    let mut c = Reader::new(srv.connect());
    for _ in 0..100 {
        c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        c.read_response().unwrap();
    }
    assert!(g.max() >= 1, "gate failed to see a known allocation");
}
