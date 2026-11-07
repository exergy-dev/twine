//! The allocation-free HTTP/1.x request parser.
//!
//! [`parse_request`] parses a request head (request line plus header fields)
//! from a byte slice and records everything as offsets into that slice, so the
//! result stays valid when an incomplete request is moved from the loop's read
//! buffer into a connection's reassembly buffer. [`ChunkState`] decodes a
//! chunked body incrementally. Both are what the server uses, and both are
//! public so a fuzz target can drive them.
//!
//! The parser is deliberately strict (see the crate documentation): bare LF
//! line endings, obs-fold, whitespace before a header colon, a request with
//! both `Content-Length` and `Transfer-Encoding`, and an HTTP/1.1 request
//! without `Host` are all rejected.

use std::fmt;

/// Fixed capacity of the header array; a request with more header lines is
/// rejected with [`ParseError::HeaderFieldsTooLarge`] (431).
pub const MAX_HEADERS: usize = 64;

/// Bounds a chunk-size line (size plus extensions).
const MAX_CHUNK_LINE: usize = 4096;

/// Default for [`Limits::max_header_bytes`].
pub const DEFAULT_MAX_HEADER_BYTES: usize = 8 << 10;
/// Default for [`Limits::max_body_bytes`].
pub const DEFAULT_MAX_BODY_BYTES: usize = 4 << 20;

/// Parser limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Bound on the request line plus headers (default 8 KiB). Exceeding it
    /// yields 431.
    pub max_header_bytes: usize,
    /// Bound on a request body, declared or decoded (default 4 MiB). Exceeding
    /// it yields 413.
    pub max_body_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }
}

/// Why a request was rejected. Each variant maps to the status code the server
/// answers with before closing the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// 400: malformed request.
    BadRequest,
    /// 431: head larger than the limit, or more than [`MAX_HEADERS`] headers.
    HeaderFieldsTooLarge,
    /// 413: body larger than the limit.
    PayloadTooLarge,
    /// 501: unsupported transfer coding.
    NotImplemented,
    /// 505: well-formed but unsupported HTTP version.
    VersionNotSupported,
}

impl ParseError {
    /// The HTTP status code for this error.
    pub fn status(self) -> u16 {
        match self {
            ParseError::BadRequest => 400,
            ParseError::HeaderFieldsTooLarge => 431,
            ParseError::PayloadTooLarge => 413,
            ParseError::NotImplemented => 501,
            ParseError::VersionNotSupported => 505,
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "http parse error ({})", self.status())
    }
}

impl std::error::Error for ParseError {}

/// Location of one request header inside the request bytes (offsets relative
/// to the first byte of the request). Pointer-free and `Copy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeaderIdx {
    /// Start of the field name.
    pub name_start: u32,
    /// End (exclusive) of the field name.
    pub name_end: u32,
    /// Start of the (trimmed) value.
    pub value_start: u32,
    /// End (exclusive) of the (trimmed) value.
    pub value_end: u32,
}

/// A parsed request head. Holds offsets only; pass the request bytes to the
/// accessors. Cheap to reset and reuse; contains no heap data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    method_end: u32,
    uri_start: u32,
    uri_end: u32,
    path_start: u32,
    path_end: u32,
    query_start: u32,
    query_end: u32,
    ver_start: u32,
    minor: u8,
    has_query: bool,
    path_slash: bool,
    abs_host: bool,
    has_host: bool,
    host_start: u32,
    host_end: u32,
    hdrs: [HeaderIdx; MAX_HEADERS],
    nh: usize,
    head_len: usize,
    content_length: i64,
    cl_start: u32,
    cl_end: u32,
    chunked: bool,
    conn_close: bool,
    conn_keep_alive: bool,
    expect_100: bool,
}

impl Default for Request {
    fn default() -> Self {
        Self::new()
    }
}

/// The result of [`parse_request`].
#[derive(Clone, Debug, PartialEq, Eq)]
// Boxing the request would allocate; the whole point of this API is that it does not.
#[allow(clippy::large_enum_variant)]
pub enum Parsed {
    /// A complete head was parsed.
    Complete {
        /// Length of the head including the blank line; the body (if any)
        /// starts here.
        head_len: usize,
        /// The parsed head (offsets into the parsed slice).
        request: Request,
    },
    /// More bytes are needed.
    Incomplete,
}

/// Parses the request head at the start of `buf`. Allocation-free.
///
/// Returns [`Parsed::Incomplete`] when `buf` ends inside the head. Errors are
/// final: no further bytes can make the request valid. A declared
/// `Content-Length` above `limits.max_body_bytes` is reported here as
/// [`ParseError::PayloadTooLarge`], as soon as the head is complete.
pub fn parse_request(buf: &[u8], limits: &Limits) -> Result<Parsed, ParseError> {
    let mut r = Request::new();
    match r.parse(buf, limits)? {
        Some(head_len) => Ok(Parsed::Complete {
            head_len,
            request: r,
        }),
        None => Ok(Parsed::Incomplete),
    }
}

const fn build_token_table() -> [bool; 256] {
    let mut t = [false; 256];
    let mut c = 0usize;
    while c < 256 {
        let b = c as u8;
        t[c] = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        c += 1;
    }
    t
}

/// RFC 9110 tchar bytes.
static TOKEN: [bool; 256] = build_token_table();

#[inline]
pub(crate) fn is_token(c: u8) -> bool {
    TOKEN[c as usize]
}

#[inline]
fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

#[inline]
fn unhex(c: u8) -> i64 {
    match c {
        b'0'..=b'9' => (c - b'0') as i64,
        b'a'..=b'f' => (c - b'a') as i64 + 10,
        _ => (c - b'A') as i64 + 10,
    }
}

/// `Ok(None)` (need more) unless the head already exceeds the limit.
#[inline]
fn more(too_big: bool) -> Result<Option<usize>, ParseError> {
    if too_big {
        Err(ParseError::HeaderFieldsTooLarge)
    } else {
        Ok(None)
    }
}

impl Request {
    /// An empty request (all accessors return empty values).
    pub fn new() -> Request {
        Request {
            method_end: 0,
            uri_start: 0,
            uri_end: 0,
            path_start: 0,
            path_end: 0,
            query_start: 0,
            query_end: 0,
            ver_start: 0,
            minor: 0,
            has_query: false,
            path_slash: false,
            abs_host: false,
            has_host: false,
            host_start: 0,
            host_end: 0,
            hdrs: [HeaderIdx {
                name_start: 0,
                name_end: 0,
                value_start: 0,
                value_end: 0,
            }; MAX_HEADERS],
            nh: 0,
            head_len: 0,
            content_length: -1,
            cl_start: 0,
            cl_end: 0,
            chunked: false,
            conn_close: false,
            conn_keep_alive: false,
            expect_100: false,
        }
    }

    /// Clears the parsed state for a new request.
    pub fn reset(&mut self) {
        // Header slots need not be cleared: `nh` bounds every use.
        self.method_end = 0;
        self.uri_start = 0;
        self.uri_end = 0;
        self.path_start = 0;
        self.path_end = 0;
        self.query_start = 0;
        self.query_end = 0;
        self.ver_start = 0;
        self.minor = 0;
        self.has_query = false;
        self.path_slash = false;
        self.abs_host = false;
        self.has_host = false;
        self.host_start = 0;
        self.host_end = 0;
        self.nh = 0;
        self.head_len = 0;
        self.content_length = -1;
        self.cl_start = 0;
        self.cl_end = 0;
        self.chunked = false;
        self.conn_close = false;
        self.conn_keep_alive = false;
        self.expect_100 = false;
    }

    /// Parses the head at the start of `b` into `self`, enforcing `limits`.
    /// `Ok(Some(head_len))` when complete, `Ok(None)` when more bytes are
    /// needed. On `Ok(None)` or `Err` the contents of `self` are unspecified.
    pub fn parse(&mut self, b: &[u8], limits: &Limits) -> Result<Option<usize>, ParseError> {
        let n = match self.parse_head(b, limits.max_header_bytes)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if self.content_length > limits.max_body_bytes as i64 {
            return Err(ParseError::PayloadTooLarge);
        }
        Ok(Some(n))
    }

    fn parse_head(&mut self, b: &[u8], max_hdr: usize) -> Result<Option<usize>, ParseError> {
        use ParseError::*;
        self.reset();
        let mut p = b;
        let mut too_big = false;
        if p.len() > max_hdr {
            p = &p[..max_hdr];
            too_big = true;
        }
        let mut i = 0usize;

        // Method.
        while i < p.len() && is_token(p[i]) {
            i += 1;
        }
        if i == p.len() {
            return more(too_big);
        }
        if p[i] != b' ' || i == 0 {
            return Err(BadRequest);
        }
        self.method_end = i as u32;
        i += 1;

        // Request target: visible ASCII and obs-text, no controls or spaces.
        let s = i;
        while i < p.len() && p[i] > b' ' && p[i] != 0x7f {
            i += 1;
        }
        if i == p.len() {
            return more(too_big);
        }
        if p[i] != b' ' || i == s {
            return Err(BadRequest);
        }
        self.uri_start = s as u32;
        self.uri_end = i as u32;
        if !self.split_target(p, s, i) {
            return Err(BadRequest);
        }
        i += 1;

        // Version: exactly "HTTP/1.0" or "HTTP/1.1" followed by CRLF.
        self.ver_start = i as u32;
        if p.len() - i < 10 {
            if !version_prefix(&p[i..]) {
                return Err(BadRequest);
            }
            return more(too_big);
        }
        let v = &p[i..i + 10];
        if &v[..7] != b"HTTP/1." || (v[7] != b'0' && v[7] != b'1') || v[8] != b'\r' || v[9] != b'\n'
        {
            if version_prefix(v) {
                return Err(VersionNotSupported); // well formed, unsupported
            }
            return Err(BadRequest);
        }
        self.minor = v[7] - b'0';
        i += 10;

        // Header fields.
        loop {
            if i >= p.len() {
                return more(too_big);
            }
            if p[i] == b'\r' {
                if i + 1 >= p.len() {
                    return more(too_big);
                }
                if p[i + 1] != b'\n' {
                    return Err(BadRequest);
                }
                i += 2;
                break;
            }
            let ns = i;
            while i < p.len() && is_token(p[i]) {
                i += 1;
            }
            if i == p.len() {
                return more(too_big);
            }
            // Rejects obs-fold (a line starting with whitespace), whitespace
            // before the colon, bare LF and any other non-token byte.
            if p[i] != b':' || i == ns {
                return Err(BadRequest);
            }
            let ne = i;
            i += 1;
            while i < p.len() && (p[i] == b' ' || p[i] == b'\t') {
                i += 1;
            }
            let vs = i;
            while i < p.len() && p[i] != b'\r' {
                let c = p[i];
                if (c < b' ' && c != b'\t') || c == 0x7f {
                    return Err(BadRequest);
                }
                i += 1;
            }
            if i + 1 >= p.len() {
                return more(too_big);
            }
            if p[i + 1] != b'\n' {
                return Err(BadRequest);
            }
            let mut ve = i;
            while ve > vs && (p[ve - 1] == b' ' || p[ve - 1] == b'\t') {
                ve -= 1;
            }
            i += 2;
            if self.nh == MAX_HEADERS {
                return Err(HeaderFieldsTooLarge);
            }
            let h = HeaderIdx {
                name_start: ns as u32,
                name_end: ne as u32,
                value_start: vs as u32,
                value_end: ve as u32,
            };
            self.hdrs[self.nh] = h;
            self.nh += 1;
            self.classify(p, h)?;
        }

        // Message framing and mandatory fields.
        if self.chunked && self.content_length >= 0 {
            return Err(BadRequest); // request smuggling vector; refuse rather than guess
        }
        if self.chunked && self.minor == 0 {
            return Err(BadRequest);
        }
        if self.minor == 1 && !self.has_host {
            return Err(BadRequest);
        }
        self.head_len = i;
        Ok(Some(i))
    }

    /// Validates the request target `p[s..e]` and records path and query
    /// ranges. Supported forms: origin-form, asterisk-form and absolute-form
    /// (`http://host/path?q`). Percent escapes in the path must be well formed.
    fn split_target(&mut self, p: &[u8], s: usize, e: usize) -> bool {
        let mut s = s;
        let mut t = &p[s..e];
        if t[0] == b'/' {
            // origin-form
        } else if t.len() == 1 && t[0] == b'*' {
            self.path_start = s as u32;
            self.path_end = e as u32;
            return true;
        } else {
            let mut n = 0usize;
            if t.len() >= 7 && t[..7].eq_ignore_ascii_case(b"http://") {
                n = 7;
            } else if t.len() >= 8 && t[..8].eq_ignore_ascii_case(b"https://") {
                n = 8;
            }
            if n == 0 {
                return false;
            }
            // Authority: a conservative host[:port] (no userinfo, no IP-literal).
            let hs = n;
            while n < t.len() && t[n] != b'/' && t[n] != b'?' {
                n += 1;
            }
            if !valid_authority(&t[hs..n]) {
                return false;
            }
            self.abs_host = true;
            self.host_start = (s + hs) as u32;
            self.host_end = (s + n) as u32;
            if n == t.len() || t[n] == b'?' {
                self.path_slash = true;
                self.path_start = (s + n) as u32;
                self.path_end = (s + n) as u32;
                if n < t.len() {
                    self.has_query = true;
                    self.query_start = (s + n + 1) as u32;
                    self.query_end = e as u32;
                }
                return true;
            }
            s += n;
            t = &t[n..];
        }
        let q = t.iter().position(|&c| c == b'?');
        let mut path = t;
        if let Some(q) = q {
            path = &t[..q];
            self.has_query = true;
            self.query_start = (s + q + 1) as u32;
            self.query_end = e as u32;
        }
        let mut i = 0;
        while i < path.len() {
            if path[i] == b'%' {
                if i + 2 >= path.len() || !is_hex(path[i + 1]) || !is_hex(path[i + 2]) {
                    return false;
                }
                i += 2;
            }
            i += 1;
        }
        self.path_start = s as u32;
        self.path_end = (s + path.len()) as u32;
        true
    }

    /// Records the framing and connection-management headers.
    #[allow(clippy::collapsible_match)]
    fn classify(&mut self, p: &[u8], h: HeaderIdx) -> Result<(), ParseError> {
        use ParseError::*;
        let name = &p[h.name_start as usize..h.name_end as usize];
        let val = &p[h.value_start as usize..h.value_end as usize];
        match name.len() {
            4 => {
                if name.eq_ignore_ascii_case(b"host") {
                    if self.has_host {
                        return Err(BadRequest);
                    }
                    self.has_host = true;
                    if !self.abs_host {
                        self.host_start = h.value_start;
                        self.host_end = h.value_end;
                    }
                }
            }
            6 => {
                if name.eq_ignore_ascii_case(b"expect") && val.eq_ignore_ascii_case(b"100-continue")
                {
                    self.expect_100 = true;
                }
            }
            10 => {
                if name.eq_ignore_ascii_case(b"connection") {
                    self.parse_connection(val);
                }
            }
            14 => {
                if name.eq_ignore_ascii_case(b"content-length") {
                    if val.is_empty() || !val.iter().all(u8::is_ascii_digit) {
                        return Err(BadRequest);
                    }
                    // Leading zeros carry no value: only the significant
                    // digits count towards the overflow check. A syntactically
                    // valid length too big for an i64 exceeds every body
                    // limit, so it is 413 rather than 400.
                    let sig = &val[val.iter().take_while(|&&c| c == b'0').count()..];
                    if sig.len() > 18 {
                        return Err(PayloadTooLarge);
                    }
                    let mut n: i64 = 0;
                    for &c in sig {
                        n = n * 10 + (c - b'0') as i64;
                    }
                    if self.content_length >= 0 {
                        // Repeated Content-Length must be byte-identical.
                        if p[self.cl_start as usize..self.cl_end as usize] != *val {
                            return Err(BadRequest);
                        }
                        return Ok(());
                    }
                    self.content_length = n;
                    self.cl_start = h.value_start;
                    self.cl_end = h.value_end;
                }
            }
            17 => {
                if name.eq_ignore_ascii_case(b"transfer-encoding") {
                    if self.chunked {
                        return Err(BadRequest);
                    }
                    if !val.eq_ignore_ascii_case(b"chunked") {
                        return Err(NotImplemented);
                    }
                    self.chunked = true;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Scans a comma-separated `Connection` token list.
    fn parse_connection(&mut self, v: &[u8]) {
        for tok in v.split(|&c| c == b',') {
            let tok = tok.trim_ascii();
            if tok.eq_ignore_ascii_case(b"close") {
                self.conn_close = true;
            } else if tok.eq_ignore_ascii_case(b"keep-alive") {
                self.conn_keep_alive = true;
            }
        }
    }

    #[inline]
    fn slice(data: &[u8], s: u32, e: u32) -> &[u8] {
        &data[s as usize..e as usize]
    }

    /// The method, e.g. `GET`.
    pub fn method<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        Self::slice(data, 0, self.method_end)
    }
    /// The raw request target as sent.
    pub fn request_uri<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        Self::slice(data, self.uri_start, self.uri_end)
    }
    /// The path of the target without the query (still percent-encoded);
    /// `/` for an absolute-form target without a path, `*` for asterisk-form.
    pub fn path<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        if self.path_slash {
            return b"/";
        }
        Self::slice(data, self.path_start, self.path_end)
    }
    /// The query without the leading `?` (empty when the target has none; see
    /// [`has_query`](Self::has_query)).
    pub fn raw_query<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        if !self.has_query {
            return &[];
        }
        Self::slice(data, self.query_start, self.query_end)
    }
    /// Whether the target carries a `?`.
    pub fn has_query(&self) -> bool {
        self.has_query
    }
    /// `HTTP/1.0` or `HTTP/1.1` as sent.
    pub fn version<'a>(&self, data: &'a [u8]) -> &'a [u8] {
        Self::slice(data, self.ver_start, self.ver_start + 8)
    }
    /// The HTTP minor version (0 or 1).
    pub fn minor(&self) -> u8 {
        self.minor
    }
    /// The host: the authority of an absolute-form target, else the `Host`
    /// header; `None` if absent (only HTTP/1.0 allows that).
    pub fn host<'a>(&self, data: &'a [u8]) -> Option<&'a [u8]> {
        if !self.has_host && !self.abs_host {
            return None;
        }
        Some(Self::slice(data, self.host_start, self.host_end))
    }
    /// Number of header lines.
    pub fn num_headers(&self) -> usize {
        self.nh
    }
    /// The header locations in wire order.
    pub fn headers(&self) -> &[HeaderIdx] {
        &self.hdrs[..self.nh]
    }
    /// The value of the first header named `name` (ASCII case-insensitive).
    pub fn header<'a>(&self, data: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
        for h in &self.hdrs[..self.nh] {
            if (h.name_end - h.name_start) as usize == name.len()
                && Self::slice(data, h.name_start, h.name_end).eq_ignore_ascii_case(name)
            {
                return Some(Self::slice(data, h.value_start, h.value_end));
            }
        }
        None
    }
    /// Length of the head including the blank line (valid after a successful parse).
    pub fn head_len(&self) -> usize {
        self.head_len
    }
    /// The declared `Content-Length`, if any.
    pub fn content_length(&self) -> Option<u64> {
        (self.content_length >= 0).then_some(self.content_length as u64)
    }
    /// Whether the body uses chunked transfer coding.
    pub fn is_chunked(&self) -> bool {
        self.chunked
    }
    /// Whether `Connection` lists `close`.
    pub fn conn_close(&self) -> bool {
        self.conn_close
    }
    /// Whether `Connection` lists `keep-alive`.
    pub fn conn_keep_alive(&self) -> bool {
        self.conn_keep_alive
    }
    /// Whether the request sent `Expect: 100-continue`.
    pub fn expects_continue(&self) -> bool {
        self.expect_100
    }
}

/// Whether `v` is consistent with `HTTP/d.d\r\n` for as many bytes as it has.
fn version_prefix(v: &[u8]) -> bool {
    const PAT: &[u8; 10] = b"HTTP/0.0\r\n";
    for (j, &c) in v.iter().enumerate().take(PAT.len()) {
        if PAT[j] == b'0' {
            if !c.is_ascii_digit() {
                return false;
            }
        } else if c != PAT[j] {
            return false;
        }
    }
    true
}

/// Accepts reg-name hosts made of letters, digits, `.`, `-` and `_` with an
/// optional numeric port.
fn valid_authority(a: &[u8]) -> bool {
    if a.is_empty() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        let c = a[i];
        if c.is_ascii_alphanumeric() || c == b'.' || c == b'-' || c == b'_' {
            i += 1;
            continue;
        }
        break;
    }
    if i == 0 {
        return false;
    }
    if i == a.len() {
        return true;
    }
    if a[i] != b':' {
        return false;
    }
    a[i + 1..].iter().all(|c| c.is_ascii_digit())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Size,
    Data,
    Crlf,
    Trailer,
}

/// Incremental chunked-body decoder. Offsets are relative to the start of the
/// request, so the raw bytes may move between the loop buffer and the
/// connection's reassembly buffer between calls; always pass the request from
/// its first byte.
#[derive(Clone, Debug)]
pub struct ChunkState {
    off: usize,
    phase: Phase,
    remain: u64,
    trailer: usize,
}

impl ChunkState {
    /// A decoder whose chunked body starts at offset `body_start` (the head length).
    pub fn new(body_start: usize) -> ChunkState {
        ChunkState {
            off: body_start,
            phase: Phase::Size,
            remain: 0,
            trailer: 0,
        }
    }

    /// Offset (from the start of the request) of the first byte the decoder has
    /// not consumed yet. Everything in `head_len..consumed()` is decoded and may
    /// be dropped from the buffer; call [`rebase`](Self::rebase) with the
    /// number of bytes removed.
    pub fn consumed(&self) -> usize {
        self.off
    }

    /// Tells the decoder that `n` bytes immediately before
    /// [`consumed`](Self::consumed) were removed from the request buffer, so
    /// later calls to [`decode`](Self::decode) get the request without them.
    /// The returned total length is then relative to the shortened request.
    pub fn rebase(&mut self, n: usize) {
        self.off -= n.min(self.off);
    }

    /// Continues decoding the chunked body of the request held in `data`
    /// (`data[0]` is the first byte of the request) and appends payload bytes
    /// to `body`. Returns `Ok(Some(total_request_len))` when the body is
    /// complete, `Ok(None)` when more bytes are needed.
    pub fn decode(
        &mut self,
        data: &[u8],
        body: &mut Vec<u8>,
        max_body: usize,
        max_hdr: usize,
    ) -> Result<Option<usize>, ParseError> {
        use ParseError::*;
        loop {
            match self.phase {
                Phase::Size => {
                    let rest = &data[self.off..];
                    let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
                        if rest.len() > MAX_CHUNK_LINE {
                            return Err(BadRequest);
                        }
                        return Ok(None);
                    };
                    let line = &rest[..nl];
                    if nl > MAX_CHUNK_LINE || line.is_empty() || line[line.len() - 1] != b'\r' {
                        return Err(BadRequest);
                    }
                    let line = &line[..line.len() - 1];
                    let mut j = 0;
                    while j < line.len() && is_hex(line[j]) {
                        j += 1;
                    }
                    if j == 0 {
                        return Err(BadRequest);
                    }
                    // Leading zeros carry no value; more than 15 significant
                    // hex digits would overflow.
                    let sig = &line[line.iter().take_while(|&&c| c == b'0').count().min(j)..j];
                    if sig.len() > 15 {
                        return Err(BadRequest);
                    }
                    let n = sig.iter().fold(0u64, |a, &c| (a << 4) | unhex(c) as u64);
                    if j < line.len() {
                        if line[j] != b';' {
                            return Err(BadRequest);
                        }
                        for &c in &line[j + 1..] {
                            if (c < b' ' && c != b'\t') || c == 0x7f {
                                return Err(BadRequest);
                            }
                        }
                    }
                    self.off += nl + 1;
                    if n == 0 {
                        self.phase = Phase::Trailer;
                        continue;
                    }
                    // n < 2^60, so the u64 arithmetic cannot overflow.
                    if (body.len() as u64).saturating_add(n) > max_body as u64 {
                        return Err(PayloadTooLarge);
                    }
                    self.remain = n;
                    self.phase = Phase::Data;
                }
                Phase::Data => {
                    let avail = (data.len() - self.off) as u64;
                    if avail == 0 {
                        return Ok(None);
                    }
                    let n = avail.min(self.remain);
                    body.extend_from_slice(&data[self.off..self.off + n as usize]);
                    self.off += n as usize;
                    self.remain -= n;
                    if self.remain > 0 {
                        return Ok(None);
                    }
                    self.phase = Phase::Crlf;
                }
                Phase::Crlf => {
                    let avail = data.len() - self.off;
                    if avail < 2 {
                        if avail == 1 && data[self.off] != b'\r' {
                            return Err(BadRequest);
                        }
                        return Ok(None);
                    }
                    if data[self.off] != b'\r' || data[self.off + 1] != b'\n' {
                        return Err(BadRequest);
                    }
                    self.off += 2;
                    self.phase = Phase::Size;
                }
                Phase::Trailer => {
                    let rest = &data[self.off..];
                    let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
                        // A trailing CR may be the first half of the final
                        // blank line (or of a line whose length is checked
                        // once it completes), so it is not counted yet; this
                        // keeps the result independent of where the input
                        // is split.
                        let pending = rest.strip_suffix(b"\r").unwrap_or(rest).len();
                        if self.trailer + pending > max_hdr {
                            return Err(HeaderFieldsTooLarge);
                        }
                        return Ok(None);
                    };
                    let line = &rest[..nl];
                    // Same pre-check as the partial-line case above so the
                    // error does not depend on where the input was split.
                    if line != b"\r"
                        && self.trailer + line.strip_suffix(b"\r").unwrap_or(line).len() > max_hdr
                    {
                        return Err(HeaderFieldsTooLarge);
                    }
                    if line.is_empty() || line[line.len() - 1] != b'\r' {
                        return Err(BadRequest);
                    }
                    let line = &line[..line.len() - 1];
                    self.off += nl + 1;
                    if line.is_empty() {
                        return Ok(Some(self.off));
                    }
                    self.trailer += nl + 1;
                    if self.trailer > max_hdr {
                        return Err(HeaderFieldsTooLarge);
                    }
                    if !valid_trailer(line) {
                        return Err(BadRequest);
                    }
                }
            }
        }
    }
}

/// Checks `token: value` syntax for a trailer field; trailers are validated
/// and discarded.
fn valid_trailer(line: &[u8]) -> bool {
    let mut i = 0;
    while i < line.len() && is_token(line[i]) {
        i += 1;
    }
    if i == 0 || i == line.len() || line[i] != b':' {
        return false;
    }
    line[i + 1..]
        .iter()
        .all(|&c| !((c < b' ' && c != b'\t') || c == 0x7f))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Outcome of framing one complete message from `raw`.
    #[derive(Debug, PartialEq, Eq)]
    enum Out {
        /// Whole message parsed; total length and decoded body.
        Ok(usize, Vec<u8>),
        Incomplete,
        Err(u16),
    }

    /// Parses head and body (Content-Length or chunked) like the server does.
    fn frame(raw: &[u8], lim: &Limits) -> (Out, Option<Request>) {
        let mut r = Request::new();
        let h = match r.parse(raw, lim) {
            Ok(Some(h)) => h,
            Ok(None) => return (Out::Incomplete, None),
            Err(e) => return (Out::Err(e.status()), None),
        };
        let mut body = Vec::new();
        if r.is_chunked() {
            let mut cs = ChunkState::new(h);
            match cs.decode(raw, &mut body, lim.max_body_bytes, lim.max_header_bytes) {
                Ok(Some(n)) => (Out::Ok(n, body), Some(r)),
                Ok(None) => (Out::Incomplete, Some(r)),
                Err(e) => (Out::Err(e.status()), Some(r)),
            }
        } else {
            let total = h + r.content_length().unwrap_or(0) as usize;
            if raw.len() < total {
                (Out::Incomplete, Some(r))
            } else {
                (Out::Ok(total, raw[h..total].to_vec()), Some(r))
            }
        }
    }

    fn status(raw: &str) -> Out {
        frame(raw.as_bytes(), &Limits::default()).0
    }

    #[test]
    fn status_table() {
        let big = "a".repeat(9 << 10);
        let many = "A: b\r\n".repeat(64);
        // (name, raw, expected: None = incomplete, Some(0) = ok, Some(n) = error status)
        let cases: Vec<(&str, String, Option<u16>)> = vec![
            ("simple", "GET / HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(0)),
            ("http10 no host", "GET / HTTP/1.0\r\n\r\n".into(), Some(0)),
            ("incomplete head", "GET / HTTP/1.1\r\nHost: x\r\n".into(), None),
            ("incomplete body", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nab".into(), None),
            ("missing host", "GET / HTTP/1.1\r\n\r\n".into(), Some(400)),
            ("bad method", "G@T / HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("empty method", " / HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("double space", "GET  / HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("bare LF", "GET / HTTP/1.1\nHost: x\n\n".into(), Some(400)),
            ("bare LF header", "GET / HTTP/1.1\r\nHost: x\n\r\n".into(), Some(400)),
            ("obs-fold", "GET / HTTP/1.1\r\nHost: x\r\nX: a\r\n b\r\n\r\n".into(), Some(400)),
            ("leading ws header", "GET / HTTP/1.1\r\n Host: x\r\n\r\n".into(), Some(400)),
            ("space before colon", "GET / HTTP/1.1\r\nHost : x\r\n\r\n".into(), Some(400)),
            ("bad version", "GET / HTTP/1.x\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("http2 version", "GET / HTTP/2.0\r\nHost: x\r\n\r\n".into(), Some(505)),
            ("http0.9 version", "GET / HTTP/0.9\r\nHost: x\r\n\r\n".into(), Some(505)),
            ("relative target", "GET foo HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("bad escape", "GET /%zz HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("short escape", "GET /%4 HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("absolute form", "GET http://example.com/p?q HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(0)),
            ("absolute userinfo", "GET http://u@example.com/ HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("absolute bad port", "GET http://example.com:8x/ HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(400)),
            ("asterisk", "OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n".into(), Some(0)),
            ("two hosts", "GET / HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n".into(), Some(400)),
            ("cl and te", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n".into(), Some(400)),
            ("te and cl", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\n0\r\n\r\n".into(), Some(400)),
            ("bad cl", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: +1\r\n\r\na".into(), Some(400)),
            ("empty cl", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: \r\n\r\n".into(), Some(400)),
            ("conflicting cl", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab".into(), Some(400)),
            ("same cl twice", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na".into(), Some(0)),
            ("gzip te", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n".into(), Some(501)),
            ("double chunked", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n".into(), Some(400)),
            ("http10 chunked", "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n".into(), Some(400)),
            ("ctl in value", "GET / HTTP/1.1\r\nHost: x\r\nX: a\x01b\r\n\r\n".into(), Some(400)),
            ("del in value", "GET / HTTP/1.1\r\nHost: x\r\nX: a\x7fb\r\n\r\n".into(), Some(400)),
            ("tab in value ok", "GET / HTTP/1.1\r\nHost: x\r\nX: a\tb\r\n\r\n".into(), Some(0)),
            ("big header", format!("GET / HTTP/1.1\r\nHost: x\r\nX: {big}\r\n\r\n"), Some(431)),
            ("big incomplete header", format!("GET / HTTP/1.1\r\nHost: x\r\nX: {big}"), Some(431)),
            ("too many headers", format!("GET / HTTP/1.1\r\nHost: x\r\n{many}\r\n"), Some(431)),
            ("body too big", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4194305\r\n\r\n".into(), Some(413)),
            ("body max ok", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4194304\r\n\r\n".into(), None),
            ("chunk bad size", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n".into(), Some(400)),
            ("chunk bare LF", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\nab\r\n".into(), Some(400)),
            ("chunk missing crlf", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nab\r\n0\r\n\r\n".into(), Some(400)),
            ("chunk ext", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1;a=b;c\r\na\r\n0\r\n\r\n".into(), Some(0)),
            ("chunk bad ext", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1;a\x01\r\na\r\n0\r\n\r\n".into(), Some(400)),
            ("chunk trailer", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nX-T: 1\r\n\r\n".into(), Some(0)),
            ("chunk bad trailer", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nX T: 1\r\n\r\n".into(), Some(400)),
            ("chunk huge size", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nfffffffffffffffff\r\n".into(), Some(400)),
        ];
        for (name, raw, want) in cases {
            let got = status(&raw);
            match (want, &got) {
                (None, Out::Incomplete) => {}
                (Some(0), Out::Ok(..)) => {}
                (Some(c), Out::Err(g)) if c == *g => {}
                _ => panic!("{name}: got {got:?}, want {want:?}"),
            }
        }
    }

    #[test]
    fn keep_alive_flags() {
        let p = |s: &str| {
            let mut r = Request::new();
            r.parse(s.as_bytes(), &Limits::default()).unwrap().unwrap();
            r
        };
        let r = p("GET / HTTP/1.0\r\n\r\n");
        assert_eq!(r.minor(), 0);
        assert!(!r.conn_keep_alive() && !r.conn_close());
        let r = p("GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n");
        assert!(r.conn_keep_alive());
        let r = p("GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade, Close\r\n\r\n");
        assert!(r.conn_close());
        let r = p("GET / HTTP/1.1\r\nHost: x\r\nExpect: 100-Continue\r\n\r\n");
        assert!(r.expects_continue());
    }

    #[test]
    fn accessors() {
        let raw = b"PUT /a%20b/c?x=1&y=2 HTTP/1.1\r\nHost:  example.com:8080 \r\nContent-Type: text/x\r\nContent-Length: 3\r\n\r\nabc";
        let Parsed::Complete {
            head_len,
            request: r,
        } = parse_request(raw, &Limits::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(&raw[head_len..], b"abc");
        assert_eq!(r.method(raw), b"PUT");
        assert_eq!(r.path(raw), b"/a%20b/c");
        assert_eq!(r.raw_query(raw), b"x=1&y=2");
        assert_eq!(r.request_uri(raw), b"/a%20b/c?x=1&y=2");
        assert_eq!(r.version(raw), b"HTTP/1.1");
        assert_eq!(r.host(raw), Some(&b"example.com:8080"[..]));
        assert_eq!(r.header(raw, b"CONTENT-type"), Some(&b"text/x"[..]));
        assert_eq!(r.header(raw, b"x-missing"), None);
        assert_eq!(r.num_headers(), 3);
        assert_eq!(r.content_length(), Some(3));

        let raw = b"GET http://h.example:81?q HTTP/1.1\r\nHost: other\r\n\r\n";
        let Parsed::Complete { request: r, .. } = parse_request(raw, &Limits::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(r.path(raw), b"/");
        assert_eq!(r.raw_query(raw), b"q");
        assert_eq!(r.host(raw), Some(&b"h.example:81"[..]));
    }

    #[test]
    fn custom_limits() {
        let lim = Limits {
            max_header_bytes: 64,
            max_body_bytes: 10,
        };
        let raw = b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(
            parse_request(raw, &lim),
            Err(ParseError::HeaderFieldsTooLarge)
        );
        let raw = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 11\r\n\r\n";
        assert_eq!(parse_request(raw, &lim), Err(ParseError::PayloadTooLarge));
        // Head exactly at the limit still parses.
        let mut raw = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let lim = Limits {
            max_header_bytes: raw.len(),
            max_body_bytes: 10,
        };
        assert!(matches!(
            parse_request(&raw, &lim),
            Ok(Parsed::Complete { .. })
        ));
        raw.extend_from_slice(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"); // pipelined bytes beyond the limit
        assert!(matches!(
            parse_request(&raw, &lim),
            Ok(Parsed::Complete { .. })
        ));
    }

    #[test]
    fn chunked_decoding() {
        let head = b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n";
        let mut raw = head.to_vec();
        raw.extend_from_slice(
            b"3;name=val\r\nabc\r\nA\r\n0123456789\r\n0\r\nX-A: 1\r\nX-B: 2\r\n\r\nGET /next",
        );
        let (o, _) = frame(&raw, &Limits::default());
        let Out::Ok(n, body) = o else { panic!("{o:?}") };
        assert_eq!(body, b"abc0123456789");
        assert_eq!(&raw[n..], b"GET /next");
    }

    // --- property test ----------------------------------------------------

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn word(rng: &mut Rng, max: usize) -> String {
        const A: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.";
        (0..1 + rng.below(max))
            .map(|_| A[rng.below(A.len())] as char)
            .collect()
    }

    /// Generates a valid request; returns (bytes, expected decoded body).
    fn gen_request(rng: &mut Rng) -> (Vec<u8>, Vec<u8>) {
        const METHODS: [&str; 5] = ["GET", "POST", "PUT", "DELETE", "OPTIONS"];
        let m = METHODS[rng.below(METHODS.len())];
        let mut path = String::from("/");
        for _ in 0..rng.below(4) {
            path.push_str(&word(rng, 8));
            path.push('/');
        }
        if rng.below(2) == 0 {
            path.push('?');
            path.push_str(&word(rng, 10));
            path.push('=');
            path.push_str(&word(rng, 10));
        }
        let mut s = format!("{m} {path} HTTP/1.1\r\nHost: {}\r\n", word(rng, 12));
        for _ in 0..rng.below(8) {
            let pad = if rng.below(3) == 0 { "  " } else { "" };
            s.push_str(&format!(
                "X-{}: {pad}{}{pad}\r\n",
                word(rng, 8),
                word(rng, 30)
            ));
        }
        if rng.below(5) == 0 {
            s.push_str("Connection: keep-alive\r\n");
        }
        let mut body = Vec::new();
        let mut out;
        match rng.below(3) {
            0 => {
                s.push_str("\r\n");
                out = s.into_bytes();
            }
            1 => {
                body = (0..rng.below(200)).map(|_| rng.below(256) as u8).collect();
                s.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
                out = s.into_bytes();
                out.extend_from_slice(&body);
            }
            _ => {
                s.push_str("Transfer-Encoding: chunked\r\n\r\n");
                out = s.into_bytes();
                for _ in 0..rng.below(5) {
                    let chunk: Vec<u8> = (0..1 + rng.below(100))
                        .map(|_| rng.below(256) as u8)
                        .collect();
                    out.extend_from_slice(format!("{:x}", chunk.len()).as_bytes());
                    if rng.below(3) == 0 {
                        out.extend_from_slice(format!(";e={}", word(rng, 5)).as_bytes());
                    }
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&chunk);
                    out.extend_from_slice(b"\r\n");
                    body.extend_from_slice(&chunk);
                }
                out.extend_from_slice(b"0\r\n");
                for _ in 0..rng.below(3) {
                    out.extend_from_slice(
                        format!("T-{}: {}\r\n", word(rng, 5), word(rng, 9)).as_bytes(),
                    );
                }
                out.extend_from_slice(b"\r\n");
            }
        }
        (out, body)
    }

    /// Incremental parse: feed `raw` in pieces split at `cuts`, growing a
    /// reassembly buffer exactly like the connection does (head re-parsed from
    /// the start until complete, chunk state kept).
    fn incremental(
        raw: &[u8],
        cuts: &[usize],
        lim: &Limits,
    ) -> (Option<usize>, Vec<u8>, Option<Request>) {
        let mut buf: Vec<u8> = Vec::new();
        let mut req = Request::new();
        let mut head_done = false;
        let mut cs = ChunkState::new(0);
        let mut body = Vec::new();
        let mut prev = 0;
        let mut ends: Vec<usize> = cuts.to_vec();
        ends.push(raw.len());
        for &e in &ends {
            buf.extend_from_slice(&raw[prev..e]);
            prev = e;
            if !head_done {
                match req.parse(&buf, lim).expect("valid request rejected") {
                    None => continue,
                    Some(h) => {
                        head_done = true;
                        if req.is_chunked() {
                            cs = ChunkState::new(h);
                        }
                    }
                }
            }
            let total = if req.is_chunked() {
                match cs
                    .decode(&buf, &mut body, lim.max_body_bytes, lim.max_header_bytes)
                    .expect("valid chunked body rejected")
                {
                    Some(n) => n,
                    None => continue,
                }
            } else {
                let t = req.head_len() + req.content_length().unwrap_or(0) as usize;
                if buf.len() < t {
                    continue;
                }
                body = buf[req.head_len()..t].to_vec();
                t
            };
            return (Some(total), body, Some(req));
        }
        (None, body, None)
    }

    #[test]
    fn property_random_splits_equal_one_shot() {
        let lim = Limits::default();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for iter in 0..3000 {
            let (req_bytes, want_body) = gen_request(&mut rng);
            // Trailing pipelined bytes must not matter.
            let mut raw = req_bytes.clone();
            if rng.below(2) == 0 {
                raw.extend_from_slice(b"GET /pipelined HTTP/1.1\r\nHo");
            }
            let (one, _) = frame(&raw, &lim);
            let Out::Ok(total, body) = one else {
                panic!(
                    "iter {iter}: one-shot failed: {one:?}\n{:?}",
                    String::from_utf8_lossy(&raw)
                );
            };
            assert_eq!(total, req_bytes.len(), "iter {iter}");
            assert_eq!(body, want_body, "iter {iter}");
            let (_, one_req) = frame(&raw, &lim);
            let one_req = one_req.unwrap();

            // Random cut points (sorted, distinct), including 1-byte splits.
            let mut cuts: Vec<usize> = Vec::new();
            match rng.below(3) {
                0 => cuts.extend(1..raw.len()),
                _ => {
                    for _ in 0..rng.below(12) {
                        cuts.push(1 + rng.below(raw.len() - 1));
                    }
                    cuts.sort_unstable();
                    cuts.dedup();
                }
            }
            let (n, got_body, got_req) = incremental(&raw, &cuts, &lim);
            assert_eq!(n, Some(total), "iter {iter} cuts {cuts:?}");
            assert_eq!(got_body, want_body, "iter {iter}");
            assert_eq!(got_req.as_ref(), Some(&one_req), "iter {iter}");

            // Every strict prefix of the request is Incomplete, never an error.
            if iter % 50 == 0 {
                for l in 0..req_bytes.len() {
                    let (o, _) = frame(&req_bytes[..l], &lim);
                    assert_eq!(o, Out::Incomplete, "iter {iter} prefix {l}");
                }
            }
        }
    }

    #[test]
    fn property_mutated_input_never_panics() {
        // Bit-flip and truncate valid requests: the parser must return, not panic.
        let lim = Limits {
            max_header_bytes: 512,
            max_body_bytes: 4096,
        };
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        for _ in 0..3000 {
            let (mut raw, _) = gen_request(&mut rng);
            for _ in 0..1 + rng.below(4) {
                let i = rng.below(raw.len());
                raw[i] = rng.below(256) as u8;
            }
            let cut = rng.below(raw.len() + 1);
            let _ = frame(&raw[..cut], &lim);
            let _ = frame(&raw, &lim);
        }
    }
}

#[cfg(test)]
mod fix_tests {
    use super::*;

    fn cl_status(cl: &str) -> Result<Option<u64>, ParseError> {
        let raw = format!("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {cl}\r\n\r\n");
        let mut r = Request::new();
        r.parse(raw.as_bytes(), &Limits::default())?;
        Ok(r.content_length())
    }

    #[test]
    fn content_length_leading_zeros() {
        assert_eq!(cl_status("000000000000000000000000123"), Ok(Some(123)));
        assert_eq!(cl_status("0000000000000000000"), Ok(Some(0)));
        assert_eq!(cl_status("0"), Ok(Some(0)));
        // 18 significant digits is the maximum accepted; the default limit
        // then rejects it as too large (413), as does any longer number.
        assert_eq!(
            cl_status("999999999999999999"),
            Err(ParseError::PayloadTooLarge)
        );
        assert_eq!(
            cl_status("1000000000000000000"),
            Err(ParseError::PayloadTooLarge)
        );
        assert_eq!(
            cl_status("0001000000000000000000"),
            Err(ParseError::PayloadTooLarge)
        );
        assert_eq!(cl_status("12a"), Err(ParseError::BadRequest));
        assert_eq!(cl_status(""), Err(ParseError::BadRequest));
    }

    fn chunk_one_shot(
        raw: &[u8],
        max_body: usize,
        max_hdr: usize,
    ) -> (Result<Option<usize>, ParseError>, Vec<u8>) {
        let mut cs = ChunkState::new(0);
        let mut body = Vec::new();
        let r = cs.decode(raw, &mut body, max_body, max_hdr);
        (r, body)
    }

    #[test]
    fn chunk_size_leading_zeros() {
        let (r, b) = chunk_one_shot(b"0000000000000000000000005\r\nhello\r\n0\r\n\r\n", 100, 100);
        assert_eq!(r, Ok(Some(39)));
        assert_eq!(b, b"hello");
        let (r, _) = chunk_one_shot(b"000000000000000000000000\r\n\r\n", 100, 100);
        assert_eq!(r, Ok(Some(28)));
        // 16 significant digits overflow the 15-digit limit.
        let (r, _) = chunk_one_shot(b"0010000000000000000\r\n", 100, 100);
        assert_eq!(r, Err(ParseError::BadRequest));
        // 15 significant digits parse and are then rejected by the body limit.
        let (r, _) = chunk_one_shot(b"0ffffffffffffff\r\n", 100, 100);
        assert_eq!(r, Err(ParseError::PayloadTooLarge));
    }

    #[test]
    fn chunk_max_body_usize_max() {
        let (r, b) = chunk_one_shot(b"5\r\nhello\r\n0\r\n\r\n", usize::MAX, 100);
        assert_eq!(r, Ok(Some(15)));
        assert_eq!(b, b"hello");
        let (r, _) = chunk_one_shot(b"fffffffffffffff\r\n", usize::MAX, 100);
        assert_eq!(r, Ok(None));
    }

    #[test]
    fn trailer_limit_split_crlf() {
        // max_hdr 6: the trailer line "X: a\r\n" fills it exactly.
        let raw = b"0\r\nX: a\r\n\r\n";
        let mut cs = ChunkState::new(0);
        let mut body = Vec::new();
        assert_eq!(cs.decode(&raw[..10], &mut body, 100, 6), Ok(None));
        assert_eq!(cs.decode(raw, &mut body, 100, 6), Ok(Some(11)));
    }

    /// Decoding with the input delivered in two parts, at every split point,
    /// gives the one-shot result (also feeding byte by byte).
    #[test]
    fn chunk_decode_split_equivalence() {
        let bodies: &[&[u8]] = &[
            b"0\r\n\r\n",
            b"5\r\nhello\r\n0\r\n\r\n",
            b"5;ext=1\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: 1\r\nY: 2\r\n\r\n",
            b"1\r\na\r\n1\r\nb\r\n0\r\nX: a\r\n\r\n",
            b"0\r\nX: a\r\n\r\n",
            b"3\r\nabc\r\n0\r\nbad trailer\r\n\r\n",
            b"3\r\nabc\r\n0\r\nX: aaaaaaaaaaaaaaaaaaaa\r\n\r\n",
            b"3\r\nabcd\r\n0\r\n\r\n",
        ];
        for max_hdr in [6usize, 12, 100] {
            for raw in bodies {
                let (want, want_body) = chunk_one_shot(raw, 100, max_hdr);
                for k in 0..=raw.len() {
                    let mut cs = ChunkState::new(0);
                    let mut body = Vec::new();
                    let mut got = cs.decode(&raw[..k], &mut body, 100, max_hdr);
                    if matches!(got, Ok(None)) {
                        got = cs.decode(raw, &mut body, 100, max_hdr);
                    }
                    assert_eq!(
                        got,
                        want,
                        "split {k} of {:?} hdr {max_hdr}",
                        String::from_utf8_lossy(raw)
                    );
                    if want.is_ok() {
                        assert_eq!(body, want_body);
                    }
                }
                let mut cs = ChunkState::new(0);
                let mut body = Vec::new();
                let mut got = Ok(None);
                for k in 0..=raw.len() {
                    got = cs.decode(&raw[..k], &mut body, 100, max_hdr);
                    if !matches!(got, Ok(None)) {
                        break;
                    }
                }
                assert_eq!(
                    got,
                    want,
                    "bytewise {:?} hdr {max_hdr}",
                    String::from_utf8_lossy(raw)
                );
            }
        }
    }

    #[test]
    fn chunk_rebase_matches_whole() {
        // Drop consumed raw bytes between calls: same result as keeping them.
        let head = b"HEAD!";
        let tail = b"5\r\nhello\r\n6;e=1\r\n world\r\n0\r\nT: 1\r\n\r\n";
        let mut full = head.to_vec();
        full.extend_from_slice(tail);
        let mut cs = ChunkState::new(head.len());
        let mut body = Vec::new();
        let mut buf = head.to_vec();
        let mut res = Ok(None);
        for &b in tail.iter() {
            buf.push(b);
            res = cs.decode(&buf, &mut body, 100, 100);
            if !matches!(res, Ok(None)) {
                break;
            }
            let (h, c) = (head.len(), cs.consumed());
            buf.drain(h..c);
            cs.rebase(c - h);
        }
        assert_eq!(body, b"hello world");
        let n = res.unwrap().unwrap();
        assert_eq!(n, buf.len());
        let (r, b2) = chunk_one_shot(&full[head.len()..], 100, 100);
        assert_eq!(b2, body);
        assert_eq!(r.unwrap().unwrap(), full.len() - head.len());
    }
}
