//! Load generator for the echo and HTTP benchmarks (port of the Go
//! `bench/loadgen.go` + `twineload`).
//!
//! ```text
//! loadgen echo --addr 127.0.0.1:9000 -c 1000 -d 10s --size 64 --threads 4 --pin 24-27
//! loadgen http --addr 127.0.0.1:8080 -c 256 -d 10s --pipeline 16 --threads 8
//! loadgen http --addr 127.0.0.1:8080 -c 256 -d 10s --rate 800000      # open loop
//! ```
//!
//! Flags: `--mode echo|http` (or first positional), `--addr`, `-c/--conns`,
//! `-d/--duration` (measured window), `--warmup` (default 1s), `--size` (echo
//! message bytes), `--pipeline` (http requests sent back to back per batch),
//! `--rate` (requests/s; > 0 selects open-loop, wrk2 style), `--threads`
//! (event-loop threads; default = number of `--pin` CPUs or 4), `--pin a,b,c`
//! (thread i runs on cpus[i % len]), `--server-pid` (report that process's CPU).
//!
//! Closed loop (default): one batch (one echo message or `pipeline` HTTP requests)
//! in flight per connection; the next goes out when the whole batch has been
//! answered. Open loop: batches are issued on a fixed schedule independent of
//! responses, and latency is measured from the *intended* send time, so queueing
//! delay is charged to the server (no coordinated omission). Open-loop threads
//! busy-poll (`epoll_wait` with timeout 0).
//!
//! Latency unit: with `--pipeline 1` (and in echo mode) the percentiles are per
//! request; with `--pipeline N > 1` they are per *batch* of N pipelined requests
//! (the RESULT line carries `latency_unit=request|batch`).
//!
//! HTTP framing: `Content-Length`, `Transfer-Encoding: chunked` (chunk sizes parsed
//! up to the 0-chunk, trailers, final CRLF) and `Connection: close` are handled. On
//! `Connection: close` the connection is replaced by a fresh blocking connect after
//! its response (a body with neither length nor chunking ends at EOF). 204/304
//! responses have no body. Echo mode verifies the echoed bytes; mismatches count as
//! `errors`.
//!
//! Each thread owns an epoll instance, its share of the connections, a read
//! buffer and a latency histogram; nothing is shared on the hot path and nothing
//! allocates per message. Latency is per batch, in log-linear buckets (16 per
//! power of two, <= 6% error). Output ends with one `RESULT mode=... conns=...
//! rps=... p99_us=... ... conns_used=...` line (`conns_used` = distinct connections
//! that sent at least one request in the measured window; fields are only ever
//! appended). A failed `sched_setaffinity` or a worker setup failure aborts the run
//! with a non-zero exit.
//!
//! `unsafe` is confined to libc calls (sockets, epoll, affinity), each with a
//! SAFETY comment.
#![allow(unsafe_code)]

use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::os::fd::IntoRawFd;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use twine_bench::Args;

const USAGE: &str =
    "usage: loadgen echo|http [--addr 127.0.0.1:9000] [-c 1000] [-d 10s] [--warmup 1s]\n\
    \t[--size 64] [--pipeline 1] [--rate 0] [--threads N] [--pin a,b,c] [--server-pid PID]\n\
    note: with --pipeline N>1 the latency percentiles are per BATCH of N requests\n\
    (RESULT latency_unit=batch); otherwise per request (latency_unit=request).\n\
    HTTP: Content-Length, chunked and Connection: close responses are handled.";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Echo,
    Http,
}

#[derive(Clone)]
struct Cfg {
    mode: Mode,
    addr: SocketAddr,
    size: usize,
    depth: u32,
    rate: f64,
    pin: Vec<usize>,
}

const PHASE_WARM: u8 = 0;
const PHASE_RECORD: u8 = 1;
const PHASE_STOP: u8 = 2;

struct Shared {
    phase: AtomicU8,
    failed: AtomicUsize,
    abort: AtomicBool,
}

// ---------------------------------------------------------------------------
// Histogram: 16 linear sub-buckets per power of two, values in nanoseconds.

const SUB_BITS: u32 = 4;
const SUB: usize = 1 << SUB_BITS;

struct Histogram {
    counts: Box<[u64; 64 * SUB]>,
    n: u64,
    max: u64,
}

impl Histogram {
    fn new() -> Self {
        Histogram {
            counts: Box::new([0; 64 * SUB]),
            n: 0,
            max: 0,
        }
    }
    fn index(v: u64) -> usize {
        if v < SUB as u64 {
            return v as usize;
        }
        let e = (64 - v.leading_zeros()) - SUB_BITS; // >= 1
        e as usize * SUB + ((v >> (e - 1)) as usize & (SUB - 1))
    }
    fn upper(i: usize) -> u64 {
        let (e, s) = (i / SUB, i % SUB);
        if e == 0 {
            s as u64 + 1
        } else {
            ((SUB + s + 1) as u64) << (e - 1)
        }
    }
    #[inline]
    fn record(&mut self, v: u64) {
        self.counts[Self::index(v)] += 1;
        self.n += 1;
        self.max = self.max.max(v);
    }
    fn merge(&mut self, o: &Histogram) {
        for (a, b) in self.counts.iter_mut().zip(o.counts.iter()) {
            *a += *b;
        }
        self.n += o.n;
        self.max = self.max.max(o.max);
    }
    /// Bucket upper bound (ns) at quantile `q`, clamped to the observed max.
    fn percentile(&self, q: f64) -> u64 {
        if self.n == 0 {
            return 0;
        }
        let target = ((q * self.n as f64).ceil() as u64).clamp(1, self.n);
        let mut acc = 0;
        for (i, c) in self.counts.iter().enumerate() {
            acc += c;
            if acc >= target {
                return Self::upper(i).min(self.max);
            }
        }
        self.max
    }
}

// ---------------------------------------------------------------------------
// HTTP response framing.

/// Index of the next CRLF at or after `from`, `None` if there is none yet.
fn find_crlf(b: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    loop {
        let off = b.get(i..)?.iter().position(|&c| c == b'\r')?;
        let p = i + off;
        match b.get(p + 1) {
            None => return None,
            Some(b'\n') => return Some(p),
            Some(_) => i = p + 1,
        }
    }
}

fn contains_ci(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

/// Header `l` is `name:` (case-insensitive); returns the value bytes.
fn header_value<'a>(l: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    (l.len() > name.len() && l[..name.len()].eq_ignore_ascii_case(name)).then(|| &l[name.len()..])
}

enum Frame {
    Incomplete,
    /// A complete response of `len` bytes; `close` if the server will close.
    Done {
        len: usize,
        ok: bool,
        close: bool,
    },
    /// Headers complete, body runs to EOF (`Connection: close`, no framing).
    ToEof {
        ok: bool,
    },
}

/// Chunked body starting at `pos`: returns the offset just past the final CRLF.
fn chunked_end(b: &[u8], mut pos: usize) -> Result<Option<usize>, ()> {
    loop {
        let Some(p) = find_crlf(b, pos) else {
            return Ok(None);
        };
        let line = &b[pos..p];
        let hex = line.split(|&c| c == b';').next().unwrap_or(&[]);
        let hex = std::str::from_utf8(hex).map_err(|_| ())?.trim();
        let size = usize::from_str_radix(hex, 16).map_err(|_| ())?;
        pos = p + 2;
        if size == 0 {
            // Trailer lines until an empty line.
            loop {
                let Some(p) = find_crlf(b, pos) else {
                    return Ok(None);
                };
                let empty = p == pos;
                pos = p + 2;
                if empty {
                    return Ok(Some(pos));
                }
            }
        }
        let end = pos
            .checked_add(size)
            .and_then(|e| e.checked_add(2))
            .ok_or(())?;
        if b.len() < end {
            return Ok(None);
        }
        if &b[end - 2..end] != b"\r\n" {
            return Err(());
        }
        pos = end;
    }
}

/// Frames the response at the start of `b`.
fn frame(b: &[u8]) -> Result<Frame, ()> {
    let (mut line, mut cl, mut have_cl) = (0usize, 0usize, false);
    let (mut chunked, mut close) = (false, false);
    let p = loop {
        let Some(p) = find_crlf(b, line) else {
            return Ok(Frame::Incomplete);
        };
        if p == line {
            break p; // blank line: end of headers
        }
        let l = &b[line..p];
        if let Some(v) = header_value(l, b"content-length:") {
            cl = std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .ok_or(())?;
            have_cl = true;
        } else if let Some(v) = header_value(l, b"transfer-encoding:") {
            chunked = contains_ci(v, b"chunked");
        } else if let Some(v) = header_value(l, b"connection:") {
            close = contains_ci(v, b"close");
        }
        line = p + 2;
    };
    let body = p + 2;
    let ok = b.len() >= 12 && &b[8..12] == b" 200";
    let nobody = b.len() >= 12 && matches!(&b[9..12], b"204" | b"304");
    if nobody {
        return Ok(Frame::Done {
            len: body,
            ok,
            close,
        });
    }
    if chunked {
        return Ok(match chunked_end(b, body)? {
            None => Frame::Incomplete,
            Some(len) => Frame::Done { len, ok, close },
        });
    }
    if have_cl {
        let len = body.checked_add(cl).ok_or(())?;
        return Ok(if b.len() < len {
            Frame::Incomplete
        } else {
            Frame::Done { len, ok, close }
        });
    }
    Ok(if close {
        Frame::ToEof { ok }
    } else {
        Frame::Done {
            len: body,
            ok,
            close,
        }
    })
}

struct Scan {
    k: usize,
    used: usize,
    bad: usize,
    /// The last response asked to close the connection.
    close: bool,
    /// An EOF-delimited response is in progress (its `ok` flag).
    to_eof: Option<bool>,
}

/// Scans back-to-back responses.
fn scan(mut b: &[u8]) -> Result<Scan, ()> {
    let mut s = Scan {
        k: 0,
        used: 0,
        bad: 0,
        close: false,
        to_eof: None,
    };
    while !b.is_empty() {
        match frame(b)? {
            Frame::Incomplete => break,
            Frame::Done { len, ok, close } => {
                s.k += 1;
                s.bad += usize::from(!ok);
                s.used += len;
                b = &b[len..];
                if close {
                    s.close = true;
                    break;
                }
            }
            Frame::ToEof { ok } => {
                s.to_eof = Some(ok);
                break;
            }
        }
    }
    Ok(s)
}

// ---------------------------------------------------------------------------
// Per-thread worker.

struct Conn {
    fd: i32,
    /// Echo: bytes of the current message received so far.
    got: usize,
    /// Responses still expected for the batch in flight.
    left: u32,
    /// Latency origin of the batch in flight (ns on the worker clock).
    sent: u64,
    /// Bytes of `req` already written for the current batch.
    wpos: usize,
    /// Partial HTTP response carried between reads.
    carry: Vec<u8>,
    out_armed: bool,
    /// HTTP: an EOF-delimited response is in progress (its `ok` flag).
    to_eof: Option<bool>,
    /// Sent at least one request in the measured window.
    used: bool,
}

impl Conn {
    fn new(fd: i32) -> Conn {
        Conn {
            fd,
            got: 0,
            left: 0,
            sent: 0,
            wpos: 0,
            carry: Vec::new(),
            out_armed: false,
            to_eof: None,
            used: false,
        }
    }
}

#[derive(Default)]
struct Stats {
    count: u64,
    rx: u64,
    tx: u64,
    errors: u64,
    overdue: u64,
    non200: u64,
    conns_used: u64,
}

struct Worker {
    cfg: Cfg,
    ep: i32,
    conns: Vec<Conn>,
    req: Vec<u8>,
    rbuf: Vec<u8>,
    hist: Histogram,
    st: Stats,
    recording: bool,
    open_loop: bool,
    /// Free connections, round-robin: take from the front, return to the back.
    idle: VecDeque<u32>,
    /// Ticks currently due that were already counted as overdue.
    due_counted: u64,
    next_at: f64,
    interval: f64,
    alive: usize,
    base: Instant,
}

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

impl Worker {
    #[inline]
    fn now(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    fn ctl(&self, op: i32, fd: i32, idx: usize, events: u32) {
        let mut ev = libc::epoll_event {
            events,
            u64: idx as u64,
        };
        // SAFETY: `ev` is a valid epoll_event for the duration of the call; `ep`
        // and `fd` are descriptors owned by this worker.
        unsafe { libc::epoll_ctl(self.ep, op, fd, &mut ev) };
    }

    fn close_conn(&mut self, idx: usize, error: bool) {
        let c = &mut self.conns[idx];
        if c.fd < 0 {
            return;
        }
        let lg = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: fd is an open socket we own; `lg` is a valid linger struct of
        // the size passed; closing removes it from the epoll set.
        unsafe {
            libc::setsockopt(
                c.fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&lg as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as u32,
            );
            libc::close(c.fd);
        }
        c.fd = -1;
        self.alive -= 1;
        if error && self.recording {
            self.st.errors += 1;
        }
    }

    /// Starts a new batch on `idx`, with latency measured from `sent`.
    fn send_batch(&mut self, idx: usize, sent: u64) {
        let c = &mut self.conns[idx];
        c.sent = sent;
        c.left = self.cfg.depth;
        c.wpos = 0;
        if self.recording && !c.used {
            c.used = true;
            self.st.conns_used += 1;
        }
        self.flush(idx);
    }

    /// Writes the unsent remainder of `req`; arms EPOLLOUT if the socket is full.
    fn flush(&mut self, idx: usize) {
        loop {
            let c = &self.conns[idx];
            if c.fd < 0 {
                return;
            }
            let rest = &self.req[c.wpos..];
            // SAFETY: `rest` is a valid readable slice; fd is an open socket.
            let n =
                unsafe { libc::send(c.fd, rest.as_ptr().cast(), rest.len(), libc::MSG_NOSIGNAL) };
            if n < 0 {
                match errno() {
                    libc::EINTR => continue,
                    libc::EAGAIN => {
                        if !c.out_armed {
                            let fd = c.fd;
                            self.conns[idx].out_armed = true;
                            self.ctl(
                                libc::EPOLL_CTL_MOD,
                                fd,
                                idx,
                                (libc::EPOLLIN | libc::EPOLLOUT) as u32,
                            );
                        }
                    }
                    _ => self.close_conn(idx, true),
                }
                return;
            }
            let n = n as usize;
            if self.recording {
                self.st.tx += n as u64;
            }
            let c = &mut self.conns[idx];
            c.wpos += n;
            if c.wpos == self.req.len() {
                if c.out_armed {
                    c.out_armed = false;
                    let fd = c.fd;
                    self.ctl(libc::EPOLL_CTL_MOD, fd, idx, libc::EPOLLIN as u32);
                }
                return;
            }
        }
    }

    /// A batch completed at `now`: record latency and issue the next one.
    fn finish(&mut self, idx: usize, now: u64) {
        if self.recording {
            let c = &self.conns[idx];
            self.hist.record(now.saturating_sub(c.sent));
        }
        if !self.open_loop {
            self.send_batch(idx, now);
        } else if self.next_at <= now as f64 {
            // Backlog: a scheduled batch was waiting for a free connection.
            let at = self.take_tick();
            self.send_batch(idx, at);
        } else {
            self.idle.push_back(idx as u32);
        }
    }

    fn on_readable(&mut self, idx: usize) {
        let fd = self.conns[idx].fd;
        if fd < 0 {
            return;
        }
        // SAFETY: rbuf is a valid writable buffer of the length passed.
        let n = unsafe { libc::recv(fd, self.rbuf.as_mut_ptr().cast(), self.rbuf.len(), 0) };
        if n < 0 {
            if !matches!(errno(), libc::EAGAIN | libc::EINTR) {
                self.close_conn(idx, true);
            }
            return;
        }
        if n == 0 {
            // EOF ends an EOF-delimited HTTP body; anything else is a lost connection.
            if let Some(ok) = self.conns[idx].to_eof.take() {
                let now = self.now();
                if self.recording && !ok {
                    self.st.non200 += 1;
                }
                self.conns[idx].carry.clear();
                self.complete(idx, 1, now, true);
            } else {
                self.close_conn(idx, true);
            }
            return;
        }
        let n = n as usize;
        let now = self.now();
        if self.recording {
            self.st.rx += n as u64;
        }
        let c = &mut self.conns[idx];
        let mut close = false;
        let k = match self.cfg.mode {
            Mode::Echo => {
                let size = self.cfg.size;
                let data = &self.rbuf[..n];
                let (mut pos, mut i, mut bad) = (c.got, 0usize, 0u64);
                while i < n {
                    let take = (size - pos).min(n - i);
                    if data[i..i + take] != self.req[pos..pos + take] {
                        bad += 1;
                    }
                    i += take;
                    pos += take;
                    if pos == size {
                        pos = 0;
                    }
                }
                if self.recording {
                    self.st.errors += bad;
                }
                let total = c.got + n;
                c.got = total % size;
                total / size
            }
            Mode::Http => {
                let data = &self.rbuf[..n];
                let res = if c.carry.is_empty() {
                    scan(data).inspect(|s| {
                        if s.used < data.len() {
                            c.carry.extend_from_slice(&data[s.used..]);
                        }
                    })
                } else {
                    c.carry.extend_from_slice(data);
                    scan(&c.carry).inspect(|s| {
                        c.carry.drain(..s.used);
                    })
                };
                match res {
                    Ok(s) => {
                        if self.recording {
                            self.st.non200 += s.bad as u64;
                        }
                        c.to_eof = s.to_eof;
                        close = s.close;
                        s.k
                    }
                    Err(()) => {
                        self.close_conn(idx, true);
                        return;
                    }
                }
            }
        };
        self.complete(idx, k, now, close);
    }

    /// Accounts `k` complete responses on `idx`; reconnects first if `close`.
    fn complete(&mut self, idx: usize, k: usize, now: u64, close: bool) {
        let mut reconnected = false;
        for i in 0..k {
            let c = &mut self.conns[idx];
            if c.left == 0 {
                if self.recording {
                    self.st.errors += 1; // more responses than requests
                }
                continue;
            }
            c.left -= 1;
            let done = c.left == 0;
            if self.recording {
                self.st.count += 1;
            }
            if done {
                if close && i + 1 == k {
                    self.reconnect(idx);
                    reconnected = true;
                }
                self.finish(idx, now);
            }
        }
        if close && !reconnected {
            // The server closed with part of a pipelined batch unanswered.
            if self.recording {
                self.st.errors += 1;
            }
            self.reconnect(idx);
            if self.open_loop {
                self.idle.push_back(idx as u32);
            } else {
                self.send_batch(idx, now);
            }
        }
    }

    /// Replaces the socket of `idx` (blocking connect; used after `Connection: close`).
    fn reconnect(&mut self, idx: usize) {
        self.close_conn(idx, false);
        match TcpStream::connect_timeout(&self.cfg.addr, Duration::from_secs(2)) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                let _ = s.set_nonblocking(true);
                let fd = s.into_raw_fd();
                let used = self.conns[idx].used;
                self.conns[idx] = Conn::new(fd);
                self.conns[idx].used = used;
                self.alive += 1;
                self.ctl(libc::EPOLL_CTL_ADD, fd, idx, libc::EPOLLIN as u32);
            }
            Err(_) => {
                if self.recording {
                    self.st.errors += 1;
                }
            }
        }
    }

    /// Consumes the next scheduled tick, returning its intended send time.
    fn take_tick(&mut self) -> u64 {
        let at = self.next_at as u64;
        self.next_at += self.interval;
        self.due_counted = self.due_counted.saturating_sub(1);
        at
    }

    /// Issues every scheduled batch whose time has come to an idle connection.
    fn pace(&mut self, now: u64) {
        while self.next_at <= now as f64 {
            let Some(idx) = self.idle.pop_front() else {
                // No free connection: count each due tick once (not each spin).
                let due = ((now as f64 - self.next_at) / self.interval) as u64 + 1;
                if due > self.due_counted {
                    if self.recording {
                        self.st.overdue += due - self.due_counted;
                    }
                    self.due_counted = due;
                }
                return;
            };
            if self.conns[idx as usize].fd < 0 {
                continue; // connection lost; drop it from the pool
            }
            let at = self.take_tick();
            self.send_batch(idx as usize, at);
        }
    }
}

fn pin_thread(cpu: usize) -> io::Result<()> {
    // SAFETY: cpu_set_t is plain data; zeroed is its documented empty state;
    // sched_setaffinity(0, ..) affects only the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Marks the run as failed (the main thread exits non-zero after the barrier).
fn fail_run(shared: &Shared, msg: &str) {
    if shared.failed.fetch_add(1, Ordering::Relaxed) == 0 {
        eprintln!("loadgen: {msg}");
    }
    shared.abort.store(true, Ordering::Relaxed);
}

/// Pins, creates the epoll set and connects. May panic; the caller catches it.
fn setup(
    cfg: &Cfg,
    my_conns: usize,
    tid: usize,
    nthreads: usize,
    shared: &Shared,
    base: Instant,
) -> Worker {
    // SAFETY: plain syscall; the result is checked.
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(ep >= 0, "epoll_create1: {}", io::Error::last_os_error());

    let req: Vec<u8> = match cfg.mode {
        Mode::Echo => (0..cfg.size).map(|i| b'a' + (i % 26) as u8).collect(),
        Mode::Http => b"GET / HTTP/1.1\r\nHost: x\r\n\r\n".repeat(cfg.depth as usize),
    };
    let open_loop = cfg.rate > 0.0;
    let batch_rate = cfg.rate / nthreads as f64 / f64::from(cfg.depth);
    let mut w = Worker {
        ep,
        conns: Vec::with_capacity(my_conns),
        req,
        rbuf: vec![0u8; 64 * 1024],
        hist: Histogram::new(),
        st: Stats::default(),
        recording: false,
        open_loop,
        idle: VecDeque::with_capacity(my_conns),
        due_counted: 0,
        next_at: 0.0,
        interval: if open_loop { 1e9 / batch_rate } else { 0.0 },
        alive: 0,
        base,
        cfg: cfg.clone(),
    };

    if !cfg.pin.is_empty() {
        let cpu = cfg.pin[tid % cfg.pin.len()];
        if let Err(e) = pin_thread(cpu) {
            fail_run(shared, &format!("sched_setaffinity(cpu {cpu}) failed: {e}"));
            return w;
        }
    }

    // Connect (blocking, paced so the server's accept queue never overflows).
    for i in 0..my_conns {
        if shared.abort.load(Ordering::Relaxed) {
            break;
        }
        match TcpStream::connect_timeout(&cfg.addr, Duration::from_secs(10)) {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                let _ = s.set_nonblocking(true);
                let fd = s.into_raw_fd();
                let idx = w.conns.len();
                w.conns.push(Conn::new(fd));
                w.alive += 1;
                w.ctl(libc::EPOLL_CTL_ADD, fd, idx, libc::EPOLLIN as u32);
            }
            Err(e) => {
                fail_run(shared, &format!("connect error: {e}"));
                break;
            }
        }
        if i % 100 == 99 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    w
}

fn worker_main(
    cfg: Cfg,
    my_conns: usize,
    tid: usize,
    nthreads: usize,
    shared: Arc<Shared>,
    barrier: Arc<Barrier>,
    base: Instant,
) -> (Stats, Histogram) {
    // A panic during setup must still reach the barrier, or every other thread
    // (and main) would wait forever.
    let w = std::panic::catch_unwind(AssertUnwindSafe(|| {
        setup(&cfg, my_conns, tid, nthreads, &shared, base)
    }));
    let w = match w {
        Ok(w) => Some(w),
        Err(_) => {
            fail_run(&shared, "worker setup panicked");
            None
        }
    };
    barrier.wait();
    let Some(mut w) = w else {
        return (Stats::default(), Histogram::new());
    };
    if shared.abort.load(Ordering::Relaxed) {
        for i in 0..w.conns.len() {
            w.close_conn(i, false);
        }
        return (w.st, w.hist);
    }
    run(w, &shared)
}

fn run(mut w: Worker, shared: &Shared) -> (Stats, Histogram) {
    // Start.
    let open_loop = w.open_loop;
    let t0 = w.now();
    if open_loop {
        w.next_at = t0 as f64;
        w.idle.extend(0..w.conns.len() as u32);
    } else {
        for i in 0..w.conns.len() {
            w.send_batch(i, t0);
        }
    }
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 512];
    let timeout = if open_loop { 0 } else { 10 };
    loop {
        let phase = shared.phase.load(Ordering::Relaxed);
        if phase == PHASE_STOP || w.alive == 0 {
            break;
        }
        w.recording = phase == PHASE_RECORD;
        // SAFETY: `events` is a valid buffer of 512 entries.
        let n =
            unsafe { libc::epoll_wait(w.ep, events.as_mut_ptr(), events.len() as i32, timeout) };
        if n < 0 {
            if errno() == libc::EINTR {
                continue;
            }
            panic!("epoll_wait: {}", io::Error::last_os_error());
        }
        for ev in &events[..n as usize] {
            let (flags, idx) = (ev.events, ev.u64 as usize);
            if flags & libc::EPOLLOUT as u32 != 0 && w.conns[idx].out_armed {
                w.flush(idx);
            }
            if flags & (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32 != 0 {
                w.on_readable(idx);
            }
        }
        if open_loop {
            let now = w.now();
            w.pace(now);
        }
    }
    for i in 0..w.conns.len() {
        w.close_conn(i, false);
    }
    // SAFETY: ep is a descriptor we created.
    unsafe { libc::close(w.ep) };
    (w.st, w.hist)
}

// ---------------------------------------------------------------------------

/// utime+stime of `pid` in seconds, from /proc/`PID`/stat (USER_HZ is 100 on Linux).
fn proc_cpu(pid: &str) -> Option<f64> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &s[s.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the comm field: state is f[0], utime f[11], stime f[12].
    let (u, st): (f64, f64) = (f.get(11)?.parse().ok()?, f.get(12)?.parse().ok()?);
    Some((u + st) / 100.0)
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1e3
}

fn main() {
    let a = Args::parse(
        USAGE,
        &[
            "mode",
            "addr",
            "c",
            "conns",
            "d",
            "duration",
            "warmup",
            "size",
            "pipeline",
            "rate",
            "threads",
            "pin",
            "server-pid",
        ],
    );
    let mode = match a.positional.first().map(String::as_str).or(a.get("mode")) {
        Some("echo") => Mode::Echo,
        Some("http") => Mode::Http,
        other => {
            eprintln!("mode must be echo or http (got {other:?})\n{USAGE}");
            std::process::exit(2);
        }
    };
    let default_addr = if mode == Mode::Echo {
        "127.0.0.1:9000"
    } else {
        "127.0.0.1:8080"
    };
    let addr_s = a.str("addr", default_addr);
    let addr = addr_s
        .to_socket_addrs()
        .ok()
        .and_then(|mut i| i.next())
        .unwrap_or_else(|| {
            eprintln!("cannot resolve {addr_s}");
            std::process::exit(2)
        });
    let conns = a.usize("c", a.usize("conns", 1000)).max(1);
    let dur = a.duration("d", a.duration("duration", Duration::from_secs(10)));
    let warmup = a.duration("warmup", Duration::from_secs(1));
    let pin = a.cpus("pin");
    let threads = a
        .usize("threads", if pin.is_empty() { 4 } else { pin.len() })
        .clamp(1, conns);
    let depth = if mode == Mode::Http {
        a.usize("pipeline", 1).max(1) as u32
    } else {
        1
    };
    let rate = a.f64("rate", 0.0);
    if !rate.is_finite() || rate < 0.0 {
        eprintln!("bad value for --rate (need a finite number >= 0)");
        std::process::exit(2);
    }
    if !pin.is_empty() && threads != pin.len() {
        eprintln!(
            "loadgen: warning: {threads} threads for {} pinned cpus",
            pin.len()
        );
    }
    let cfg = Cfg {
        mode,
        addr,
        size: a.usize("size", 64).max(1),
        depth,
        rate,
        pin,
    };
    let server_pid = a.str("server-pid", "");

    let shared = Arc::new(Shared {
        phase: AtomicU8::new(PHASE_WARM),
        failed: AtomicUsize::new(0),
        abort: AtomicBool::new(false),
    });
    let barrier = Arc::new(Barrier::new(threads + 1));
    let base = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let share = conns / threads + usize::from(t < conns % threads);
            let (cfg, shared, barrier) = (cfg.clone(), shared.clone(), barrier.clone());
            std::thread::spawn(move || worker_main(cfg, share, t, threads, shared, barrier, base))
        })
        .collect();

    barrier.wait(); // all connected (or aborted)
    if shared.abort.load(Ordering::Relaxed) {
        for h in handles {
            let _ = h.join();
        }
        eprintln!(
            "loadgen: aborted: {} worker failure(s) (pin/connect/setup)",
            shared.failed.load(Ordering::Relaxed)
        );
        std::process::exit(1);
    }
    std::thread::sleep(warmup);
    let t_rec = Instant::now();
    shared.phase.store(PHASE_RECORD, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(20)); // let recording take effect
    let srv0 = proc_cpu(&server_pid);
    let cli0 = proc_cpu("self");
    let t_cpu = Instant::now();
    std::thread::sleep(dur);
    let srv1 = proc_cpu(&server_pid);
    let cli1 = proc_cpu("self");
    let cpu_el = t_cpu.elapsed().as_secs_f64();
    shared.phase.store(PHASE_STOP, Ordering::Relaxed);
    let el = t_rec.elapsed().as_secs_f64();

    let mut hist = Histogram::new();
    let mut tot = Stats::default();
    for h in handles {
        let Ok((s, hi)) = h.join() else {
            eprintln!("loadgen: worker panicked");
            std::process::exit(1);
        };
        tot.count += s.count;
        tot.rx += s.rx;
        tot.tx += s.tx;
        tot.errors += s.errors;
        tot.overdue += s.overdue;
        tot.non200 += s.non200;
        tot.conns_used += s.conns_used;
        hist.merge(&hi);
    }
    let rps = tot.count as f64 / el;
    let unit = if depth > 1 { "batch" } else { "request" };
    let mode_s = if mode == Mode::Echo { "echo" } else { "http" };
    println!(
        "mode={mode_s} addr={addr} conns={conns} threads={threads} window={el:.2}s {}",
        if rate > 0.0 {
            format!("open-loop offered={rate:.0}/s")
        } else {
            "closed-loop".to_string()
        }
    );
    println!(
        "msgs={} rate={rps:.0}/s ({:.0}/s per connection) pipeline/batch={depth} errors={} non200={} conns_used={}",
        tot.count,
        rps / conns as f64,
        tot.errors,
        tot.non200,
        tot.conns_used
    );
    println!(
        "latency_us p50={:.0} p90={:.0} p99={:.0} p99.9={:.0} max={:.0} (per {unit}, {} samples)",
        us(hist.percentile(0.5)),
        us(hist.percentile(0.9)),
        us(hist.percentile(0.99)),
        us(hist.percentile(0.999)),
        us(hist.max),
        hist.n
    );
    println!("bytes tx={} rx={}", tot.tx, tot.rx);
    if rate > 0.0 {
        println!(
            "offered={rate:.0}/s achieved={rps:.0}/s overdue_ticks={}",
            tot.overdue
        );
    }
    let cores = |a: Option<f64>, b: Option<f64>| match (a, b) {
        (Some(a), Some(b)) => (b - a) / cpu_el,
        _ => f64::NAN,
    };
    let (srv_cores, cli_cores) = (cores(srv0, srv1), cores(cli0, cli1));
    println!("cpu server_cores={srv_cores:.2} client_cores={cli_cores:.2}");
    println!(
        "RESULT mode={mode_s} conns={conns} threads={threads} pipeline={depth} rate={rate:.0} rps={rps:.0} \
         per_conn={:.1} p50_us={:.0} p90_us={:.0} p99_us={:.0} p999_us={:.0} max_us={:.0} \
         errors={} bytes_rx={} server_cores={srv_cores:.2} client_cores={cli_cores:.2} \
         latency_unit={unit} non200={} overdue={} conns_used={}",
        rps / conns as f64,
        us(hist.percentile(0.5)),
        us(hist.percentile(0.9)),
        us(hist.percentile(0.99)),
        us(hist.percentile(0.999)),
        us(hist.max),
        tot.errors,
        tot.rx,
        tot.non200,
        tot.overdue,
        tot.conns_used,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_length_and_pipelined() {
        let r = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
        let two = [&r[..], &r[..]].concat();
        let s = scan(&two).unwrap();
        assert_eq!((s.k, s.used, s.bad, s.close), (2, two.len(), 0, false));
        assert_eq!(scan(&r[..r.len() - 1]).unwrap().k, 0);
    }

    #[test]
    fn chunked_with_trailers() {
        let r = b"HTTP/1.1 404 NF\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2;x=y\r\nde\r\n0\r\nT: v\r\n\r\n";
        let s = scan(r).unwrap();
        assert_eq!((s.k, s.used, s.bad), (1, r.len(), 1));
        for cut in 1..r.len() {
            assert_eq!(scan(&r[..cut]).unwrap().k, 0, "cut {cut}");
        }
    }

    #[test]
    fn close_and_eof() {
        let r = b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1\r\n\r\nx";
        let s = scan(r).unwrap();
        assert!(s.close && s.k == 1);
        let e = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody";
        let s = scan(e).unwrap();
        assert_eq!((s.k, s.to_eof), (0, Some(true)));
    }
}
