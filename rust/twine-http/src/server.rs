//! [`Server`] and the per-connection protocol.

use std::cell::RefCell;
use std::net::{SocketAddr, ToSocketAddrs};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use twine::{
    Engine, EngineHandle, Error, Factory, ListenOptions, Listener, Loop, LoopLocal, Producer,
    Protocol, Transport,
};

use crate::ctx::{RequestCtx, Resp};
use crate::date::DateCache;
use crate::parser::{
    ChunkState, Limits, ParseError, Request, DEFAULT_MAX_BODY_BYTES, DEFAULT_MAX_HEADER_BYTES,
};

/// Default for [`Server::max_pipelined_requests`].
pub const DEFAULT_MAX_PIPELINED_REQUESTS: usize = 16;

/// The largest buffer a connection keeps between requests. Buffers grow as
/// needed and are reused; one that grew past this (a large body or response) is
/// released when it drains so idle connections stay small.
const MAX_RETAINED: usize = 64 << 10;

static BUF_HIGH_WATER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Test hook: the largest reassembly buffer (bytes) any connection in this
/// process has held since start or the last [`reset_buffer_high_water`].
#[doc(hidden)]
pub fn buffer_high_water() -> usize {
    BUF_HIGH_WATER.load(std::sync::atomic::Ordering::Relaxed)
}

/// Test hook: resets [`buffer_high_water`] to zero.
#[doc(hidden)]
pub fn reset_buffer_high_water() {
    BUF_HIGH_WATER.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
fn note_buf(n: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    if n > BUF_HIGH_WATER.load(Relaxed) {
        BUF_HIGH_WATER.fetch_max(n, Relaxed);
    }
}
/// Response body size above which the body is handed to `writev` next to the
/// head instead of being copied after it.
const LARGE_BODY: usize = 32 << 10;
/// Accumulated responses are written early beyond this size, so that
/// `write_blocked()` reflects them while a deep pipeline is being served.
const FLUSH_AT: usize = 16 << 10;
/// Timer token: continue serving buffered requests.
const TOKEN_RESUME: u64 = 1;

const DEFAULT_CONTENT_TYPE: &[u8] = b"Content-Type: text/plain; charset=utf-8\r\n";

/// Implemented by user code; called on the connection's loop for every
/// request. It must not block. The response is buffered in the
/// [`RequestCtx`] and written when `handle` returns.
///
/// Any `Fn(&mut RequestCtx<'_>) + Send + Sync + 'static` closure is a
/// `Handler`. Annotate the parameter (`|ctx: &mut RequestCtx<'_>| ...`) or
/// build the closure with [`handler`] so its signature is inferred.
pub trait Handler: Send + Sync + 'static {
    /// Handles one request.
    fn handle(&self, ctx: &mut RequestCtx<'_>);
}

impl<F> Handler for F
where
    F: Fn(&mut RequestCtx<'_>) + Send + Sync + 'static,
{
    fn handle(&self, ctx: &mut RequestCtx<'_>) {
        self(ctx)
    }
}

/// Pins a closure's signature so `handler(|ctx| ...)` infers
/// `&mut RequestCtx<'_>`.
pub fn handler<F>(f: F) -> F
where
    F: Fn(&mut RequestCtx<'_>) + Send + Sync + 'static,
{
    f
}

struct Inner<H> {
    handler: Arc<H>,
    limits: Limits,
    max_pipe: usize,
    /// `Server: name\r\n`, or empty.
    server_line: Vec<u8>,
    keep_alive: bool,
    idle_timeout: Option<Duration>,
}

/// An HTTP/1.1 server running on a twine [`Engine`]. Configure with the
/// builder methods, then call [`factory`](Self::factory) or
/// [`listen`](Self::listen). One `Server` may serve any number of listeners.
pub struct Server<H: Handler> {
    handler: Arc<H>,
    limits: Limits,
    max_pipe: usize,
    server_name: Option<&'static str>,
    keep_alive: bool,
    idle_timeout: Option<Duration>,
}

impl<H: Handler> Server<H> {
    /// A server with the default limits (8 KiB headers, 4 MiB body, 16
    /// pipelined requests), keep-alive on, and `Server: twine` sent.
    pub fn new(handler: H) -> Self {
        Server {
            handler: Arc::new(handler),
            limits: Limits {
                max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
                max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            },
            max_pipe: DEFAULT_MAX_PIPELINED_REQUESTS,
            server_name: Some("twine"),
            keep_alive: true,
            idle_timeout: None,
        }
    }

    /// Bounds the request line plus headers (default 8 KiB); exceeding it, or
    /// sending more than 64 header lines, yields 431.
    pub fn max_header_bytes(mut self, n: usize) -> Self {
        self.limits.max_header_bytes = n.max(1);
        self
    }

    /// Bounds a request body, declared or decoded (default 4 MiB); exceeding it yields 413.
    pub fn max_body_bytes(mut self, n: usize) -> Self {
        self.limits.max_body_bytes = n;
        self
    }

    /// Bounds how many requests are served from one read (default 16). The rest
    /// stay buffered, reading is paused and serving continues on the next loop
    /// iteration.
    pub fn max_pipelined_requests(mut self, n: usize) -> Self {
        self.max_pipe = n.max(1);
        self
    }

    /// The `Server` response header (default `Some("twine")`); `None` omits it.
    pub fn server_name(mut self, name: Option<&'static str>) -> Self {
        self.server_name = name;
        self
    }

    /// Whether connections are persistent (default true). When false every
    /// response carries `Connection: close`.
    pub fn keep_alive(mut self, on: bool) -> Self {
        self.keep_alive = on;
        self
    }

    /// Closes connections with no I/O for `d` (default: never).
    pub fn idle_timeout(mut self, d: Option<Duration>) -> Self {
        self.idle_timeout = d;
        self
    }

    fn inner(&self) -> Arc<Inner<H>> {
        let mut server_line = Vec::new();
        if let Some(n) = self.server_name {
            server_line.extend_from_slice(b"Server: ");
            server_line.extend(
                n.bytes()
                    .map(|c| if c == b'\r' || c == b'\n' { b' ' } else { c }),
            );
            server_line.extend_from_slice(b"\r\n");
        }
        let l = self.limits;
        Arc::new(Inner {
            handler: self.handler.clone(),
            limits: l,
            max_pipe: self.max_pipe,
            server_line,
            keep_alive: self.keep_alive,
            idle_timeout: self.idle_timeout,
        })
    }

    /// A [`Factory`] that serves HTTP on every connection it builds. Each
    /// connection allocates its protocol state once; requests on it allocate
    /// nothing. `eng` declares the per-loop `Date` cache.
    pub fn factory(&self, eng: &EngineHandle) -> impl Factory + Clone {
        let inner = self.inner();
        let date: LoopLocal<RefCell<DateCache>> = eng.loop_local(|| RefCell::new(DateCache::new()));
        twine::factory(
            move |lp: &mut Loop, _peer: SocketAddr| -> Box<dyn Protocol> {
                let d = lp.local(date);
                Box::new(HttpConn::new(inner.clone(), d))
            },
        )
    }

    /// Binds `addr` on `eng` and serves HTTP on it (shorthand for
    /// `eng.listen_tcp(addr, self.factory(..), opts)`).
    pub fn listen(
        &self,
        eng: &Engine,
        addr: impl ToSocketAddrs,
        opts: ListenOptions,
    ) -> twine::Result<Listener> {
        self.listen_on(&eng.handle(), addr, opts)
    }

    /// [`listen`](Self::listen) on an [`EngineHandle`].
    pub fn listen_on(
        &self,
        eng: &EngineHandle,
        addr: impl ToSocketAddrs,
        opts: ListenOptions,
    ) -> twine::Result<Listener> {
        eng.listen_tcp(addr, self.factory(eng), opts)
    }
}

/// The per-connection protocol.
struct HttpConn<H: Handler> {
    inner: Arc<Inner<H>>,
    date: Rc<RefCell<DateCache>>,
    req: Request,
    resp: Resp,
    cs: ChunkState,
    /// Set by `parse_one` when an incomplete chunked request has raw body
    /// bytes `head_len..consumed` that were already decoded and can be dropped
    /// from the reassembly buffer.
    chunk_drop: Option<(usize, usize)>,
    /// The current request's head is parsed.
    head_done: bool,
    /// `100 Continue` was sent for the current request.
    cont: bool,
    /// Incomplete request bytes (reassembly).
    buf: Vec<u8>,
    /// Decoded chunked body.
    chunk_body: Vec<u8>,
    /// Responses not yet written.
    out: Vec<u8>,
    closed: bool,
    /// A zero-delay continuation timer is armed.
    resume_pending: bool,
    /// The outbound ring is above its high-water mark: stop serving.
    prod_paused: bool,
}

impl<H: Handler> HttpConn<H> {
    fn new(inner: Arc<Inner<H>>, date: Rc<RefCell<DateCache>>) -> Self {
        HttpConn {
            inner,
            date,
            req: Request::new(),
            resp: Resp::new(),
            cs: ChunkState::new(0),
            chunk_drop: None,
            head_done: false,
            cont: false,
            buf: Vec::new(),
            chunk_body: Vec::new(),
            out: Vec::new(),
            closed: false,
            resume_pending: false,
            prod_paused: false,
        }
    }

    /// Continues serving buffered requests on the next loop iteration.
    fn schedule(&mut self, t: &mut Transport<'_>) {
        if self.resume_pending || self.closed {
            return;
        }
        self.resume_pending = true;
        t.call_later(Duration::ZERO, TOKEN_RESUME);
    }

    /// Serves requests buffered by a pipelining yield or a producer pause, then
    /// resumes reading.
    fn resume(&mut self, t: &mut Transport<'_>) {
        self.resume_pending = false;
        if self.closed || self.prod_paused {
            return;
        }
        if !self.buf.is_empty() {
            self.process_buf(t);
            self.flush_out(t);
        }
        if !self.resume_pending && !self.prod_paused && !self.closed {
            t.resume_reading();
        }
    }

    /// Serves what it can from `self.buf` and compacts the rest.
    fn process_buf(&mut self, t: &mut Transport<'_>) {
        let mut buf = std::mem::take(&mut self.buf);
        let n = self.process(t, &buf);
        if self.closed {
            buf.clear();
        } else {
            buf.drain(..n);
            if let Some((a, b)) = self.chunk_drop.take() {
                buf.drain(a..b);
                self.cs.rebase(b - a);
            }
            if buf.is_empty() && buf.capacity() > MAX_RETAINED {
                buf = Vec::new();
            }
        }
        self.buf = buf;
        self.reserve();
    }

    /// Grows `buf` once to hold a `Content-Length` request whose head is
    /// parsed, so the body does not trigger repeated growth. Reserves at most
    /// 64 KiB beyond what has arrived: a declared length is attacker-controlled.
    fn reserve(&mut self) {
        if !self.head_done || self.req.is_chunked() {
            return;
        }
        let Some(cl) = self.req.content_length().filter(|&c| c > 0) else {
            return;
        };
        let need = (self.req.head_len() + cl as usize).min(self.buf.len() + (64 << 10));
        if need > self.buf.len() {
            self.buf.reserve(need - self.buf.len());
        }
    }

    /// Serves complete requests from `data` and returns the bytes consumed. It
    /// stops at an incomplete request, after `max_pipelined_requests`
    /// (scheduling a continuation), on a producer pause or write backpressure,
    /// and on close.
    fn process(&mut self, t: &mut Transport<'_>, data: &[u8]) -> usize {
        let mut off = 0;
        let mut served = 0;
        self.chunk_drop = None;
        while !self.closed && off < data.len() && !self.prod_paused {
            if t.write_blocked() {
                // Backpressure: stop serving. `resume_producing` continues
                // (a Pause notification is guaranteed to be pending or
                // delivered whenever the ring is above the high-water mark).
                self.prod_paused = true;
                t.pause_reading();
                break;
            }
            if served >= self.inner.max_pipe {
                self.resume_pending = true;
                t.pause_reading();
                t.call_later(Duration::ZERO, TOKEN_RESUME);
                break;
            }
            match self.parse_one(t, &data[off..]) {
                Ok(None) => break,
                Err(e) => {
                    self.send_error(t, e.status());
                    break;
                }
                Ok(Some(n)) => {
                    self.serve(t, &data[off..off + n]);
                    off += n;
                    served += 1;
                    if self.resp.close {
                        self.close_conn(t);
                    } else if self.out.len() >= FLUSH_AT {
                        self.flush_out(t);
                    }
                }
            }
        }
        if self.closed {
            data.len()
        } else {
            off
        }
    }

    /// Parses (or continues parsing) the request at the start of `data`.
    /// `Ok(Some(len))` when complete, `Ok(None)` when more bytes are needed.
    fn parse_one(
        &mut self,
        _t: &mut Transport<'_>,
        data: &[u8],
    ) -> Result<Option<usize>, ParseError> {
        let limits = self.inner.limits;
        self.chunk_drop = None;
        if !self.head_done {
            // An incomplete head is parsed again from its start when more
            // bytes arrive; heads are at most max_header_bytes, so this is
            // cheap and keeps error detection independent of how the bytes
            // were split.
            let Some(h) = self.req.parse(data, &limits)? else {
                return Ok(None);
            };
            self.head_done = true;
            if self.req.is_chunked() {
                self.cs = ChunkState::new(h);
                self.chunk_body.clear();
            }
        }
        let total = if self.req.is_chunked() {
            match self.cs.decode(
                data,
                &mut self.chunk_body,
                limits.max_body_bytes,
                limits.max_header_bytes,
            )? {
                Some(n) => n,
                None => {
                    // The decoded prefix of the body lives in `chunk_body`;
                    // the raw bytes need not be kept. Only the head and the
                    // unconsumed tail (at most one chunk line) are retained,
                    // so the buffer stays within max_header_bytes plus one
                    // line however the body is chunked.
                    let (h, c) = (self.req.head_len(), self.cs.consumed());
                    if c > h {
                        self.chunk_drop = Some((h, c));
                    }
                    return self.need_body(_t);
                }
            }
        } else {
            let total = self.req.head_len() + self.req.content_length().unwrap_or(0) as usize;
            if data.len() < total {
                return self.need_body(_t);
            }
            total
        };
        Ok(Some(total))
    }

    /// Handles an incomplete body: answers `Expect: 100-continue` once.
    ///
    /// There is no raw-size cap here: a `Content-Length` body is bounded by
    /// the declared length (checked against `max_body_bytes` with the head),
    /// and chunked framing is consumed incrementally, with the decoder itself
    /// bounding the one partial chunk or trailer line it may be holding.
    fn need_body(&mut self, _t: &mut Transport<'_>) -> Result<Option<usize>, ParseError> {
        if self.req.expects_continue() && !self.cont && self.req.minor() == 1 {
            self.cont = true;
            self.out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
        }
        Ok(None)
    }

    /// Runs the handler for the complete request `data` and appends the
    /// response to `self.out`.
    fn serve(&mut self, t: &mut Transport<'_>, data: &[u8]) {
        let HttpConn {
            inner,
            date,
            req,
            resp,
            chunk_body,
            out,
            head_done,
            cont,
            ..
        } = self;
        resp.reset();
        resp.close = if req.minor() == 1 {
            req.conn_close()
        } else {
            req.conn_close() || !req.conn_keep_alive()
        };
        if !inner.keep_alive {
            resp.close = true;
        }
        let body: &[u8] = if req.is_chunked() {
            chunk_body
        } else {
            &data[req.head_len()..]
        };
        {
            let mut ctx = RequestCtx {
                req,
                data,
                body,
                resp,
                t: t.reborrow(),
            };
            let r = catch_unwind(AssertUnwindSafe(|| inner.handler.handle(&mut ctx)));
            if let Err(p) = r {
                let msg = p
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| p.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("<non-string panic>");
                log::error!("twine-http: panic in Handler: {msg}");
                ctx.resp.reset();
                ctx.error("Internal Server Error\n", 500);
                ctx.resp.close = true;
                ctx.resp.hclose = true;
            }
        }
        if inner.keep_alive && req.minor() == 0 && resp.hkeep && !resp.hclose {
            // The handler explicitly asked to keep an HTTP/1.0 connection open.
            resp.close = false;
        }
        let now = t.now();
        write_response(
            out,
            resp,
            req.minor(),
            req.method(data) == b"HEAD",
            &inner.server_line,
            date.borrow_mut().get(now),
            t,
        );
        *head_done = false;
        *cont = false;
        if resp.body.capacity() > MAX_RETAINED {
            resp.body = Vec::new();
        }
        if resp.hdrs.capacity() > MAX_RETAINED {
            resp.hdrs = Vec::new();
        }
        if chunk_body.capacity() > MAX_RETAINED {
            *chunk_body = Vec::new();
        }
    }

    /// Answers a request that could not be parsed and closes.
    fn send_error(&mut self, t: &mut Transport<'_>, code: u16) {
        append_error(&mut self.out, code);
        self.close_conn(t);
    }

    /// Flushes and half-closes. Data the peer still sends is discarded.
    fn close_conn(&mut self, t: &mut Transport<'_>) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.flush_out(t);
        t.lose_connection();
        // Reading may be paused (pipelining yield or producer pause). The core
        // completes a graceful close when it reads the peer's EOF, so reading
        // must be on; data_received discards anything that arrives meanwhile.
        t.resume_reading();
    }

    /// Writes all buffered responses with one `write`.
    fn flush_out(&mut self, t: &mut Transport<'_>) {
        if self.out.is_empty() {
            return;
        }
        t.write(&self.out);
        if self.out.capacity() > MAX_RETAINED {
            self.out = Vec::new();
        } else {
            self.out.clear();
        }
    }
}

impl<H: Handler> Protocol for HttpConn<H> {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        if let Some(d) = self.inner.idle_timeout {
            t.set_idle_timeout(Some(d));
        }
        t.register_producer(Producer::This, true);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.closed {
            return; // closing: discard whatever the peer still sends
        }
        let data: &[u8] = data;
        if !self.buf.is_empty() || self.resume_pending || self.prod_paused {
            self.buf.extend_from_slice(data);
            note_buf(self.buf.len());
            if !self.resume_pending && !self.prod_paused {
                self.process_buf(t);
            }
        } else {
            let n = self.process(t, data);
            if !self.closed && n < data.len() {
                self.buf.clear();
                if let Some((a, b)) = self.chunk_drop.take() {
                    self.buf.extend_from_slice(&data[n..n + a]);
                    self.buf.extend_from_slice(&data[n + b..]);
                    self.cs.rebase(b - a);
                } else {
                    self.buf.extend_from_slice(&data[n..]);
                }
                note_buf(self.buf.len());
                self.reserve();
            }
        }
        self.flush_out(t);
    }

    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {
        self.closed = true;
        self.buf = Vec::new();
        self.chunk_body = Vec::new();
        self.out = Vec::new();
        self.resp.release();
    }

    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        if self.prod_paused || self.closed {
            return;
        }
        self.prod_paused = true;
        t.pause_reading();
    }

    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        if !self.prod_paused {
            return;
        }
        self.prod_paused = false;
        self.schedule(t);
    }

    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        if token == TOKEN_RESUME {
            self.resume(t);
        }
    }

    fn type_name(&self) -> &'static str {
        "twine_http::HttpConn"
    }
}

// --- response formatting ----------------------------------------------------

/// Appends the decimal form of `n` without allocating.
fn push_u64(out: &mut Vec<u8>, mut n: u64) {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(&tmp[i..]);
}

/// The reason phrase for `code`.
pub(crate) fn status_text(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        102 => "Processing",
        103 => "Early Hints",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        207 => "Multi-Status",
        208 => "Already Reported",
        226 => "IM Used",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        305 => "Use Proxy",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Request Entity Too Large",
        414 => "Request URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Requested Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Entity",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        _ => "Status",
    }
}

fn push_status_line(out: &mut Vec<u8>, code: u16) {
    out.extend_from_slice(b"HTTP/1.1 ");
    push_u64(out, code as u64);
    out.push(b' ');
    out.extend_from_slice(status_text(code).as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Appends the canned close-after response for a rejected request.
fn append_error(out: &mut Vec<u8>, code: u16) {
    let code = match code {
        400 | 413 | 431 | 500 | 501 | 505 => code,
        _ => 400,
    };
    let text = status_text(code);
    push_status_line(out, code);
    out.extend_from_slice(DEFAULT_CONTENT_TYPE);
    out.extend_from_slice(b"Content-Length: ");
    push_u64(out, text.len() as u64 + 1);
    out.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    out.extend_from_slice(text.as_bytes());
    out.push(b'\n');
}

/// Appends the response for the current request to `out`; a body above
/// `LARGE_BODY` is sent with `writev` next to the head instead of copied.
fn write_response(
    out: &mut Vec<u8>,
    resp: &Resp,
    minor: u8,
    is_head: bool,
    server_line: &[u8],
    date_line: &[u8],
    t: &mut Transport<'_>,
) {
    let st = if (100..=599).contains(&resp.status) {
        resp.status
    } else {
        500
    };
    push_status_line(out, st);
    if !resp.no_server {
        out.extend_from_slice(server_line);
    }
    if !resp.no_date {
        out.extend_from_slice(date_line);
    }
    let mut body: &[u8] = &resp.body;
    if st >= 200 && st != 204 && st != 304 {
        if !resp.ct.is_empty() {
            out.extend_from_slice(b"Content-Type: ");
            out.extend_from_slice(&resp.ct);
            out.extend_from_slice(b"\r\n");
        } else if !body.is_empty() {
            out.extend_from_slice(DEFAULT_CONTENT_TYPE);
        }
        out.extend_from_slice(b"Content-Length: ");
        push_u64(out, body.len() as u64);
        out.extend_from_slice(b"\r\n");
    } else {
        body = &[];
    }
    out.extend_from_slice(&resp.hdrs);
    if resp.close {
        out.extend_from_slice(b"Connection: close\r\n");
    } else if minor == 0 {
        out.extend_from_slice(b"Connection: keep-alive\r\n");
    }
    out.extend_from_slice(b"\r\n");
    if is_head {
        body = &[];
    }
    if body.len() > LARGE_BODY {
        t.writev(&[out.as_slice(), body]);
        out.clear();
    } else {
        out.extend_from_slice(body);
    }
}
