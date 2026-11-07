//! twine-http: a zero-allocation HTTP/1.1 server for the twine event loop
//! (design Section 9).
//!
//! A [`Server`]'s factory builds one protocol object per connection. It owns a
//! few byte buffers that grow on demand and are reused for every request on the
//! connection, so the steady state (keep-alive requests, pipelined requests,
//! `Content-Length` and chunked bodies) performs no heap allocation per request:
//!
//! - The request line and headers are parsed by a hand-written state machine
//!   ([`parse_request`]) into a fixed array of 64 offset records
//!   ([`HeaderIdx`]). Offsets refer to the request bytes, which stay in the
//!   loop's read buffer when a request arrives whole and are copied into the
//!   connection's reassembly buffer only when it is incomplete.
//! - The handler builds its response in connection-owned buffers. When it
//!   returns, the status line, `Server`, the cached `Date`, `Content-Length`
//!   and `Connection` headers and the body are appended to one output `Vec`,
//!   and all responses produced from one read are sent with a single
//!   `Transport::write` (bodies above 32 KiB go out with `writev` next to the
//!   head). The `Date` line is cached per loop and regenerated lazily when the
//!   second changes (see the `date` module for why this replaces the
//!   once-per-second timer).
//!
//! Every `&[u8]` a [`RequestCtx`] returns is valid only while the handler runs.
//!
//! # Behaviour
//!
//! HTTP/1.1 connections are persistent unless the request or the handler says
//! `Connection: close`; HTTP/1.0 connections close after the response unless
//! the request asked for keep-alive. `Expect: 100-continue` is answered when
//! the head arrives without its body. HEAD responses carry the `Content-Length`
//! of the body the handler wrote but not the body; 1xx, 204 and 304 responses
//! carry neither.
//!
//! Limits: `max_header_bytes` (8 KiB) or more than 64 header lines yields 431;
//! `max_body_bytes` (4 MiB) yields 413, checked against `Content-Length` as
//! soon as the head arrives. Malformed requests yield 400, unsupported transfer
//! codings 501, unsupported versions 505. Each of these responses carries
//! `Connection: close` and the connection is half-closed after it; anything the
//! peer sends afterwards is discarded. The parser is deliberately strict: bare
//! LF line endings, obs-fold, whitespace before a header colon, a request with
//! both `Content-Length` and `Transfer-Encoding`, and an HTTP/1.1 request
//! without `Host` are all rejected with 400.
//!
//! Pipelining: at most `max_pipelined_requests` (16) requests are served from
//! one read. The remainder stays in the reassembly buffer, reading is paused
//! and serving resumes on the next loop iteration (`call_later(ZERO)`). The
//! connection also registers itself as a streaming producer
//! (`Producer::This`): when the outbound ring passes the engine's
//! `write_high_water`, serving stops and reading pauses until it drains.
//!
//! Panics: a panic in the handler is caught by the server, logged, and
//! answered with 500 followed by a close. Nothing of the failed response has
//! been sent at that point.
//!
//! There is no `net/http` adapter (the Go package's `Adapt`); Rust has no
//! equivalent standard handler type.
#![deny(missing_docs)]

mod ctx;
mod date;
mod parser;
mod server;

pub use ctx::RequestCtx;
pub use date::{format_http_date, HTTP_DATE_LEN};
pub use parser::{
    parse_request, ChunkState, HeaderIdx, Limits, ParseError, Parsed, Request,
    DEFAULT_MAX_BODY_BYTES, DEFAULT_MAX_HEADER_BYTES, MAX_HEADERS,
};
#[doc(hidden)]
pub use server::{buffer_high_water, reset_buffer_high_water};
pub use server::{handler, Handler, Server, DEFAULT_MAX_PIPELINED_REQUESTS};
