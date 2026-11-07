//! [`RequestCtx`]: one request and its response, handed to the [`Handler`](crate::Handler).

use std::net::SocketAddr;

use twine::{Loop, Transport};

use crate::parser::{is_token, Request};

/// Response state owned by the connection and reused for every request on it.
pub(crate) struct Resp {
    pub(crate) status: u16,
    /// `Name: value\r\n` lines set by the handler.
    pub(crate) hdrs: Vec<u8>,
    pub(crate) ct: Vec<u8>,
    pub(crate) body: Vec<u8>,
    pub(crate) close: bool,
    /// The handler named `close` in a `Connection` header.
    pub(crate) hclose: bool,
    /// The handler named `keep-alive` in a `Connection` header.
    pub(crate) hkeep: bool,
    pub(crate) no_server: bool,
    pub(crate) no_date: bool,
}

impl Resp {
    pub(crate) fn new() -> Resp {
        Resp {
            status: 200,
            hdrs: Vec::new(),
            ct: Vec::new(),
            body: Vec::new(),
            close: false,
            hclose: false,
            hkeep: false,
            no_server: false,
            no_date: false,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.status = 200;
        self.hdrs.clear();
        self.ct.clear();
        self.body.clear();
        self.close = false;
        self.hclose = false;
        self.hkeep = false;
        self.no_server = false;
        self.no_date = false;
    }

    pub(crate) fn release(&mut self) {
        self.hdrs = Vec::new();
        self.ct = Vec::new();
        self.body = Vec::new();
    }
}

/// Appends the header value `s`: CR and LF become spaces so a value cannot
/// inject extra lines, and every other control byte (below 0x20 except HTAB,
/// and 0x7F) is removed.
fn append_safe(dst: &mut Vec<u8>, s: &[u8]) {
    dst.extend(s.iter().filter_map(|&c| match c {
        b'\r' | b'\n' => Some(b' '),
        b'\t' => Some(c),
        0..=0x1f | 0x7f => None,
        _ => Some(c),
    }));
}

/// One request and its response.
///
/// There is one `RequestCtx` view per request; the buffers behind it belong to
/// the connection and are reused, so nothing is allocated per request in the
/// steady state. Every `&[u8]` returned by a request accessor aliases the
/// loop's read buffer or the connection's reassembly buffer and is valid only
/// while the handler runs. The response is buffered and written when the
/// handler returns.
pub struct RequestCtx<'a> {
    pub(crate) req: &'a Request,
    pub(crate) data: &'a [u8],
    pub(crate) body: &'a [u8],
    pub(crate) resp: &'a mut Resp,
    pub(crate) t: Transport<'a>,
}

impl<'a> RequestCtx<'a> {
    // --- request ---------------------------------------------------------

    /// The request method, e.g. `GET`.
    pub fn method(&self) -> &[u8] {
        self.req.method(self.data)
    }

    /// Whether the method equals `m` (case-sensitive, as methods are).
    pub fn is_method(&self, m: &str) -> bool {
        self.method() == m.as_bytes()
    }

    /// The raw request target as sent, e.g. `/a/b?x=1`.
    pub fn request_uri(&self) -> &[u8] {
        self.req.request_uri(self.data)
    }

    /// The path of the target without the query, still percent-encoded. For an
    /// absolute-form target it is the path part (`/` when empty); for `*` it is `*`.
    pub fn path(&self) -> &[u8] {
        self.req.path(self.data)
    }

    /// The query string without the leading `?`; empty when the target has none.
    pub fn raw_query(&self) -> &[u8] {
        self.req.raw_query(self.data)
    }

    /// The protocol version as sent: `HTTP/1.0` or `HTTP/1.1`.
    pub fn version(&self) -> &[u8] {
        self.req.version(self.data)
    }

    /// Whether the request is HTTP/1.1.
    pub fn is_http11(&self) -> bool {
        self.req.minor() == 1
    }

    /// The request host: the authority of an absolute-form target, else the
    /// `Host` header (empty if absent, which only HTTP/1.0 allows).
    pub fn host(&self) -> &[u8] {
        self.req.host(self.data).unwrap_or(&[])
    }

    /// The value of the first request header named `name` (ASCII
    /// case-insensitive), trimmed of surrounding whitespace. A linear scan of
    /// at most 64 entries; no map is built.
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.header_bytes(name.as_bytes())
    }

    /// [`header`](Self::header) with a byte-slice name.
    pub fn header_bytes(&self, name: &[u8]) -> Option<&[u8]> {
        self.req.header(self.data, name)
    }

    /// Number of request header lines.
    pub fn num_headers(&self) -> usize {
        self.req.num_headers()
    }

    /// Calls `f(name, value)` for every request header in wire order, repeated
    /// names included.
    pub fn visit_headers(&self, mut f: impl FnMut(&[u8], &[u8])) {
        for h in self.req.headers() {
            f(
                &self.data[h.name_start as usize..h.name_end as usize],
                &self.data[h.value_start as usize..h.value_end as usize],
            );
        }
    }

    /// The request's `Content-Length`; `None` when absent (including chunked
    /// requests: use `body().len()`).
    pub fn content_length(&self) -> Option<u64> {
        self.req.content_length()
    }

    /// Whether the request body used chunked transfer coding.
    pub fn is_chunked(&self) -> bool {
        self.req.is_chunked()
    }

    /// The complete request body: the raw `Content-Length` bytes or the decoded
    /// chunked payload.
    pub fn body(&self) -> &[u8] {
        self.body
    }

    /// The peer address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.t.peer()
    }

    /// The local address of the connection.
    pub fn local_addr(&self) -> SocketAddr {
        self.t.host()
    }

    /// The connection's transport (for timers, handles, ...). Do not write to
    /// it: the response is written when the handler returns, and bytes written
    /// here would precede it.
    pub fn transport(&mut self) -> &mut Transport<'a> {
        &mut self.t
    }

    /// The loop that owns the connection.
    pub fn loop_mut(&mut self) -> &mut Loop {
        self.t.loop_mut()
    }

    // --- response --------------------------------------------------------

    /// Sets the response status (default 200). Codes outside 100..=599 are sent as 500.
    pub fn set_status(&mut self, code: u16) {
        self.resp.status = code;
    }

    /// The response status set so far.
    pub fn status(&self) -> u16 {
        self.resp.status
    }

    /// Sets the `Content-Type` header. Without it a non-empty body is sent as
    /// `text/plain; charset=utf-8`.
    pub fn set_content_type(&mut self, ct: &str) {
        self.set_content_type_bytes(ct.as_bytes());
    }

    /// [`set_content_type`](Self::set_content_type) with a byte-slice value.
    pub fn set_content_type_bytes(&mut self, ct: &[u8]) {
        self.resp.ct.clear();
        append_safe(&mut self.resp.ct, ct);
    }

    /// Sets a response header, replacing earlier values of the same name (ASCII
    /// case-insensitive). `Content-Type` is routed to
    /// [`set_content_type`](Self::set_content_type); `Content-Length` and
    /// `Transfer-Encoding` are ignored (the length is computed); `Connection:
    /// close` closes the connection after the response; `Server` and `Date`
    /// replace the automatic values. CR and LF in name or value are replaced
    /// with spaces so a header cannot split the response.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.set_header_bytes(name.as_bytes(), value.as_bytes());
    }

    /// [`set_header`](Self::set_header) with byte-slice arguments.
    pub fn set_header_bytes(&mut self, name: &[u8], value: &[u8]) {
        if self.special(name, value, true) {
            return;
        }
        self.del_header_bytes(name);
        self.push_header(name, value);
    }

    /// Adds a response header line without replacing existing ones (for
    /// `Set-Cookie` and other repeatable headers). Special names behave as in
    /// [`set_header`](Self::set_header).
    pub fn add_header(&mut self, name: &str, value: &str) {
        self.add_header_bytes(name.as_bytes(), value.as_bytes());
    }

    /// [`add_header`](Self::add_header) with byte-slice arguments.
    pub fn add_header_bytes(&mut self, name: &[u8], value: &[u8]) {
        if self.special(name, value, false) {
            return;
        }
        self.push_header(name, value);
    }

    /// Removes response headers previously set with `name`.
    pub fn del_header(&mut self, name: &str) {
        self.del_header_bytes(name.as_bytes());
    }

    /// Closes the connection after this response.
    pub fn set_connection_close(&mut self) {
        self.resp.close = true;
        self.resp.hclose = true;
    }

    /// Replaces the response body with a copy of `b`.
    pub fn set_body(&mut self, b: &[u8]) {
        self.resp.body.clear();
        self.resp.body.extend_from_slice(b);
    }

    /// Replaces the response body with a copy of `s`.
    pub fn set_body_str(&mut self, s: &str) {
        self.set_body(s.as_bytes());
    }

    /// Appends `p` to the response body.
    pub fn write(&mut self, p: &[u8]) {
        self.resp.body.extend_from_slice(p);
    }

    /// Appends `s` to the response body.
    pub fn write_str(&mut self, s: &str) {
        self.resp.body.extend_from_slice(s.as_bytes());
    }

    /// The response body written so far.
    pub fn response_body(&self) -> &[u8] {
        &self.resp.body
    }

    /// Replaces the response with status `code` and `msg` as a plain-text body.
    pub fn error(&mut self, msg: &str, code: u16) {
        self.resp.status = code;
        self.resp.ct.clear();
        self.resp.ct.extend_from_slice(b"text/plain; charset=utf-8");
        self.resp.body.clear();
        self.resp.body.extend_from_slice(msg.as_bytes());
    }

    // --- internals -------------------------------------------------------

    fn push_header(&mut self, name: &[u8], value: &[u8]) {
        if name.is_empty() || !name.iter().all(|&c| is_token(c)) {
            log::debug!("twine-http: dropping response header with invalid name {name:?}");
            return;
        }
        let h = &mut self.resp.hdrs;
        h.extend_from_slice(name);
        h.extend_from_slice(b": ");
        append_safe(h, value);
        h.extend_from_slice(b"\r\n");
    }

    /// Handles header names the server manages itself; reports whether the
    /// header was consumed.
    fn special(&mut self, name: &[u8], value: &[u8], replace: bool) -> bool {
        match name.len() {
            4 if name.eq_ignore_ascii_case(b"date") => {
                if replace {
                    self.del_header_bytes(name);
                }
                self.resp.no_date = true;
                false
            }
            6 if name.eq_ignore_ascii_case(b"server") => {
                if replace {
                    self.del_header_bytes(name);
                }
                self.resp.no_server = true;
                false
            }
            10 if name.eq_ignore_ascii_case(b"connection") => {
                // A comma-separated token list: `close` anywhere closes;
                // `keep-alive` keeps an HTTP/1.0 connection open.
                for tok in value.split(|&c| c == b',') {
                    let tok = tok.trim_ascii();
                    if tok.eq_ignore_ascii_case(b"close") {
                        self.resp.close = true;
                        self.resp.hclose = true;
                    } else if tok.eq_ignore_ascii_case(b"keep-alive") {
                        self.resp.hkeep = true;
                    }
                }
                true
            }
            12 if name.eq_ignore_ascii_case(b"content-type") => {
                self.set_content_type_bytes(value);
                true
            }
            14 => name.eq_ignore_ascii_case(b"content-length"),
            17 => name.eq_ignore_ascii_case(b"transfer-encoding"),
            _ => false,
        }
    }

    /// Removes every line of the response headers whose name folds to `name`.
    fn del_header_bytes(&mut self, name: &[u8]) {
        let h = &mut self.resp.hdrs;
        let mut i = 0;
        while i < h.len() {
            let mut end = i;
            while end < h.len() && h[end] != b'\n' {
                end += 1;
            }
            end += 1; // past '\n'
            let mut colon = i;
            while h[colon] != b':' {
                colon += 1;
            }
            if h[i..colon].eq_ignore_ascii_case(name) {
                h.drain(i..end.min(h.len()));
                continue;
            }
            i = end;
        }
    }
}
