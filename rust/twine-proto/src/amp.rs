//! Twisted AMP (Asynchronous Messaging Protocol): request/response multiplexing
//! over "boxes", wire-compatible with `twisted.protocols.amp`.
//!
//! # Wire format
//!
//! A box is a sequence of key/value pairs. Every key and every value is a
//! string with a 2-byte big-endian length prefix, and the box ends with an empty
//! key (two zero bytes). Keys are at most 255 bytes, values at most 65535. A
//! request box carries `_command` and (unless no answer is wanted) `_ask`; the
//! reply carries `_answer` with the same tag, or `_error` with `_error_code` and
//! `_error_description`. Tags are opaque strings; this implementation generates
//! them from a 32-bit counter rendered in hex, as Twisted does.
//!
//! # Layers
//!
//! * [`AmpParser`]: a pure, fuzzable box parser (no loop needed);
//! * [`AmpBox`]: box encoding and one-shot decoding;
//! * [`Amp`]: the [`Protocol`] wrapper that owns the parser, a user
//!   [`AmpHandler`] for incoming commands and the table of pending calls.
//!
//! # Differences from the Go port
//!
//! * [`Args`] is a [`BTreeMap`], so the iteration (and, for [`AmpBox`], the
//!   encoding) order is sorted by key, not wire order. Duplicate keys: the last
//!   one wins.
//! * Values are raw bytes (`Vec<u8>`), as Twisted's `amp.String` is; use
//!   [`arg_str`] for a UTF-8 view and [`args`] to build an [`Args`] from pairs.
//!   Keys are `String`s: a key must be non-empty, at most 255 bytes and valid
//!   UTF-8. A box with a non-UTF-8 key (or more than `max_pairs` pairs) is
//!   *rejected*: if it carries an `_ask` the peer gets an `_error` reply with
//!   code [`MALFORMED_ERROR_CODE`], otherwise it is dropped; the connection
//!   survives because the box was still framed.
//! * A panicking handler is caught, logged locally and answered with `UNKNOWN`
//!   and the fixed description `internal error` (the panic text is not sent to
//!   the peer); the connection survives.
//!
//! # Memory use
//!
//! A box is parsed into a [`BTreeMap`] of owned strings and byte vectors, and
//! the smallest possible pair takes 5 bytes on the wire. Without a cap on the
//! pair count a single 1 MiB box ([`DEFAULT_MAX_BOX_SIZE`]) could therefore
//! expand to some 200 000 map entries and tens of MiB of heap, per connection.
//! [`Amp::max_pairs`] (default [`DEFAULT_MAX_PAIRS`], 4096) bounds the number
//! of pairs stored for one box: pairs beyond it are framed but discarded and
//! the box is rejected as malformed. Size the box limit and pair cap for the
//! number of connections you serve: worst case heap per connection is roughly
//! `max_box_size` plus `max_pairs` times about 100 bytes of map overhead.
//!
//! # Errors
//!
//! A box that cannot be framed (key over 255 bytes, box over the size limit)
//! aborts with [`Error::FrameTooLarge`]; [`AmpParser::feed`] additionally
//! reports a rejected box (see above) as an [`Error::Io`] of kind `InvalidData`
//! ([`crate::is_malformed`]). An `_error` reply rejects the caller's `Deferred`
//! with an [`Error::Io`] wrapping a [`RemoteError`]; recover it with
//! [`remote_error`].

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};

use twine::{ConnId, Deferred, Error, LocalResolver, Loop, Protocol, Transport};

use crate::{recycle, Flow};

/// An AMP box's arguments: key/value pairs, sorted by key (see the module docs).
/// Values are raw bytes, as Twisted's `amp.String`; see [`args`] and [`arg_str`].
pub type Args = BTreeMap<String, Vec<u8>>;

/// Builds [`Args`] from key/value pairs, e.g. `args([("a", "13"), ("b", "29")])`.
/// A repeated key keeps the last value.
pub fn args<K: Into<String>, V: Into<Vec<u8>>>(pairs: impl IntoIterator<Item = (K, V)>) -> Args {
    pairs
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
}

/// The value of `key` as UTF-8 text; `None` if the key is absent or the value is
/// not valid UTF-8.
pub fn arg_str<'a>(args: &'a Args, key: &str) -> Option<&'a str> {
    std::str::from_utf8(args.get(key)?).ok()
}

/// Default limit on one box's encoded size: 1 MiB.
pub const DEFAULT_MAX_BOX_SIZE: usize = 1 << 20;

/// Default limit on the number of key/value pairs stored per box: 4096. See the
/// module docs on memory use.
pub const DEFAULT_MAX_PAIRS: usize = 4096;

/// Error code sent when a handler fails with an error that is not a [`RemoteError`]
/// (and when it panics).
pub const UNKNOWN_ERROR_CODE: &str = "UNKNOWN";
/// Error code sent when no handler accepts a command.
pub const UNHANDLED_ERROR_CODE: &str = "UNHANDLED";
/// Error code sent in reply to a request box that was framed but rejected (a
/// non-UTF-8 key or too many pairs).
pub const MALFORMED_ERROR_CODE: &str = "MALFORMED";

const ASK: &str = "_ask";
const ANSWER: &str = "_answer";
const COMMAND: &str = "_command";
const ERROR: &str = "_error";
const ERROR_CODE: &str = "_error_code";
const ERROR_DESC: &str = "_error_description";
const MAX_KEY: usize = 0xff;
const MAX_VALUE: usize = 0xffff;
/// Description sent to the peer when a handler panics.
const INTERNAL_ERROR: &str = "internal error";

fn is_reserved(k: &str) -> bool {
    matches!(k, ASK | ANSWER | COMMAND | ERROR | ERROR_CODE | ERROR_DESC)
}

fn malformed(what: &str) -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("twine-proto: malformed AMP box: {what}"),
    ))
}

fn bad_args(what: impl fmt::Display) -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("twine-proto: invalid AMP arguments: {what}"),
    ))
}

/// The rejection of a call answered with an `_error` box, and the error a handler
/// sends with [`AmpReply::Error`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteError {
    /// The peer's `_error_code` ([`UNKNOWN_ERROR_CODE`], [`UNHANDLED_ERROR_CODE`]
    /// or application-defined).
    pub code: String,
    /// The peer's `_error_description`.
    pub description: String,
}

impl fmt::Display for RemoteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AMP remote error {}: {}", self.code, self.description)
    }
}

impl std::error::Error for RemoteError {}

impl From<RemoteError> for Error {
    /// `Error::Io(io::Error::new(Other, e))`; recover with [`remote_error`].
    fn from(e: RemoteError) -> Error {
        Error::Io(io::Error::other(e))
    }
}

/// Recovers the [`RemoteError`] carried by `e`, if any.
pub fn remote_error(e: &Error) -> Option<&RemoteError> {
    match e {
        Error::Io(io) => io.get_ref()?.downcast_ref::<RemoteError>(),
        _ => None,
    }
}

fn validate(args: &Args) -> Result<(), Error> {
    for (k, v) in args {
        if k.is_empty() {
            return Err(bad_args("empty key"));
        }
        if k.len() > MAX_KEY {
            return Err(bad_args("key longer than 255 bytes"));
        }
        if v.len() > MAX_VALUE {
            return Err(bad_args(format_args!(
                "value of {k:?} longer than 65535 bytes"
            )));
        }
        if is_reserved(k) {
            return Err(bad_args(format_args!("reserved key {k:?}")));
        }
    }
    Ok(())
}

fn validate_cmd(cmd: &str) -> Result<(), Error> {
    if cmd.is_empty() {
        return Err(bad_args("empty command name"));
    }
    if cmd.len() > MAX_VALUE {
        return Err(bad_args("command name longer than 65535 bytes"));
    }
    Ok(())
}

/// Appends one length-prefixed string. Callers guarantee `s.len() <= 0xffff`.
fn push_str(out: &mut Vec<u8>, s: &[u8]) {
    debug_assert!(s.len() <= MAX_VALUE);
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s);
}

/// An owned box: a set of [`Args`] with encode and decode helpers. Unlike the
/// arguments of [`Amp::call`], a raw box may contain the reserved keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AmpBox {
    /// The box's key/value pairs.
    pub args: Args,
}

impl AmpBox {
    /// An empty box.
    pub fn new() -> AmpBox {
        AmpBox::default()
    }

    /// Wraps existing arguments.
    pub fn from_args(args: Args) -> AmpBox {
        AmpBox { args }
    }

    /// Sets `key` to `value`.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.args.insert(key.into(), value.into());
    }

    /// Appends the wire encoding (pairs in key order, then the empty key) to `out`.
    /// `Error::Io(InvalidInput)` for an empty or over-long key or an over-long
    /// value; nothing is appended then.
    pub fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), Error> {
        for (k, v) in &self.args {
            if k.is_empty() {
                return Err(bad_args("empty key"));
            }
            if k.len() > MAX_KEY {
                return Err(bad_args("key longer than 255 bytes"));
            }
            if v.len() > MAX_VALUE {
                return Err(bad_args(format_args!(
                    "value of {k:?} longer than 65535 bytes"
                )));
            }
        }
        for (k, v) in &self.args {
            push_str(out, k.as_bytes());
            push_str(out, v);
        }
        out.extend_from_slice(&[0, 0]);
        Ok(())
    }

    /// The wire encoding as a new vector.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut v = Vec::new();
        self.encode_into(&mut v)?;
        Ok(v)
    }

    /// Decodes `bytes` that must hold exactly one complete box.
    pub fn decode(bytes: &[u8]) -> Result<AmpBox, Error> {
        let mut p = AmpParser::new();
        let mut got = Vec::new();
        p.feed(bytes, &mut |a| {
            got.push(a);
            Flow::Continue
        })?;
        if got.len() != 1 || p.buffered() != 0 {
            return Err(malformed("expected exactly one complete box"));
        }
        Ok(AmpBox {
            args: got.pop().unwrap_or_default(),
        })
    }
}

/// Pure AMP box parser. Holds only the incomplete trailing string and the
/// partially assembled box (at most `max_pairs` pairs are stored).
#[derive(Debug)]
pub struct AmpParser {
    max_box: usize,
    max_pairs: usize,
    pairs: usize,
    bad: Option<&'static str>,
    size: usize,
    in_body: bool,
    nhdr: usize,
    hdr: [u8; 2],
    n: usize,
    buf: Vec<u8>,
    key: Option<String>,
    args: Args,
    stopped: bool,
}

impl Default for AmpParser {
    fn default() -> Self {
        AmpParser::new()
    }
}

impl AmpParser {
    /// A parser with [`DEFAULT_MAX_BOX_SIZE`].
    pub fn new() -> AmpParser {
        AmpParser::with_max_box_size(DEFAULT_MAX_BOX_SIZE)
    }

    /// A parser that rejects boxes whose encoded size (including the terminating
    /// empty key) exceeds `max` bytes.
    pub fn with_max_box_size(max: usize) -> AmpParser {
        AmpParser::with_limits(max, DEFAULT_MAX_PAIRS)
    }

    /// Like [`with_max_box_size`](Self::with_max_box_size), also storing at most
    /// `max_pairs` key/value pairs per box (more makes the box malformed).
    pub fn with_limits(max: usize, max_pairs: usize) -> AmpParser {
        AmpParser {
            max_box: max,
            max_pairs,
            pairs: 0,
            bad: None,
            size: 0,
            in_body: false,
            nhdr: 0,
            hdr: [0; 2],
            n: 0,
            buf: Vec::new(),
            key: None,
            args: Args::new(),
            stopped: false,
        }
    }

    /// Bytes of the incomplete trailing string plus the partial box's size so far
    /// (zero between boxes).
    pub fn buffered(&self) -> usize {
        self.size + self.nhdr
    }

    /// True once a [`Flow::Close`] or an error stopped the parser.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    fn fail(&mut self, e: Error) -> Error {
        self.stopped = true;
        self.buf = Vec::new();
        self.args.clear();
        e
    }

    fn check_len(&mut self, n: usize) -> Result<(), Error> {
        self.size += 2 + n;
        if self.size > self.max_box || (self.key.is_none() && n > MAX_KEY) {
            return Err(Error::FrameTooLarge);
        }
        Ok(())
    }

    fn string(&mut self, s: &[u8], f: &mut BoxFn<'_>) -> Result<(), Error> {
        if let Some(key) = self.key.take() {
            // An empty key marks a value to discard (the box is already bad).
            if !key.is_empty() {
                self.args.insert(key, s.to_vec());
            }
            return Ok(());
        }
        if s.is_empty() {
            self.size = 0;
            self.pairs = 0;
            let a = std::mem::take(&mut self.args);
            let bad = self.bad.take();
            if f(a, bad) == Flow::Close {
                self.stopped = true;
            }
            return Ok(());
        }
        self.pairs += 1;
        if self.pairs > self.max_pairs && self.bad.is_none() {
            self.bad = Some("too many pairs");
        }
        if self.bad.is_some() {
            self.key = Some(String::new());
            return Ok(());
        }
        match std::str::from_utf8(s) {
            Ok(k) => self.key = Some(k.to_owned()),
            Err(_) => {
                self.bad = Some("key is not UTF-8");
                self.key = Some(String::new());
            }
        }
        Ok(())
    }

    /// Feeds a chunk, calling `f` with each complete, well-formed box. A box
    /// rejected for a non-UTF-8 key or too many pairs stops the parser with an
    /// `InvalidData` error ([`crate::is_malformed`]), as does an oversized box
    /// ([`Error::FrameTooLarge`]); `f` may return [`Flow::Close`] to stop.
    pub fn feed(&mut self, data: &[u8], f: &mut dyn FnMut(Args) -> Flow) -> Result<(), Error> {
        let mut rejected = None;
        let r = self.feed_boxes(data, &mut |a, bad| match bad {
            None => f(a),
            Some(w) => {
                rejected = Some(w);
                Flow::Close
            }
        });
        if let Some(w) = rejected {
            return Err(self.fail(malformed(w)));
        }
        r
    }

    /// Like [`feed`](Self::feed), but a rejected box is delivered to `f` as
    /// `(pairs stored so far, Some(reason))` and parsing continues after it.
    fn feed_boxes(&mut self, mut data: &[u8], f: &mut BoxFn<'_>) -> Result<(), Error> {
        while !data.is_empty() && !self.stopped {
            if self.in_body {
                let k = (self.n - self.buf.len()).min(data.len());
                self.buf.extend_from_slice(&data[..k]);
                data = &data[k..];
                if self.buf.len() < self.n {
                    return Ok(());
                }
                self.in_body = false;
                let s = std::mem::take(&mut self.buf);
                let r = self.string(&s, f);
                self.buf = s;
                recycle(&mut self.buf);
                if let Err(e) = r {
                    return Err(self.fail(e));
                }
                continue;
            }
            let n = if self.nhdr == 0 && data.len() >= 2 {
                let n = u16::from_be_bytes([data[0], data[1]]) as usize;
                data = &data[2..];
                n
            } else {
                self.hdr[self.nhdr] = data[0];
                self.nhdr += 1;
                data = &data[1..];
                if self.nhdr < 2 {
                    continue;
                }
                self.nhdr = 0;
                u16::from_be_bytes(self.hdr) as usize
            };
            if let Err(e) = self.check_len(n) {
                return Err(self.fail(e));
            }
            if data.len() >= n {
                let (s, rest) = data.split_at(n);
                data = rest;
                if let Err(e) = self.string(s, f) {
                    return Err(self.fail(e));
                }
            } else {
                self.n = n;
                self.in_body = true;
                self.buf.clear();
            }
        }
        Ok(())
    }
}

type BoxFn<'a> = dyn FnMut(Args, Option<&'static str>) -> Flow + 'a;

/// What an [`AmpHandler`] answers an incoming command with.
pub enum AmpReply {
    /// Send an `_answer` box carrying these arguments.
    Answer(Args),
    /// Send an `_error` box.
    Error {
        /// The `_error_code`.
        code: String,
        /// The `_error_description` (truncated to 65535 bytes).
        description: String,
    },
    /// The answer is not ready: it is sent when the Deferred settles. A rejection
    /// is sent as an error (the [`RemoteError`]'s code and description, else
    /// `UNKNOWN` and the error text). Connection loss in the meantime drops it.
    Deferred(Deferred<Args>),
}

impl AmpReply {
    /// The conventional reply for a command the handler does not know.
    pub fn unhandled(cmd: &str) -> AmpReply {
        AmpReply::Error {
            code: UNHANDLED_ERROR_CODE.into(),
            description: format!("Unhandled Command: {cmd:?}"),
        }
    }

    /// An error reply with a code and description.
    pub fn error(code: impl Into<String>, description: impl Into<String>) -> AmpReply {
        AmpReply::Error {
            code: code.into(),
            description: description.into(),
        }
    }
}

/// User callbacks for [`Amp`]. All methods run on the loop thread.
pub trait AmpHandler: 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}

    /// Serves an incoming command. `args` excludes all six reserved keys
    /// (`_ask`, `_answer`, `_error`, `_command`, `_error_code`,
    /// `_error_description`). For a
    /// request that asked for no answer the reply is discarded.
    fn handle(&mut self, t: &mut Transport<'_>, cmd: &str, args: Args) -> AmpReply;

    /// The connection closed; pending calls were already rejected. A framing
    /// error is reported as `reason`.
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {}
}

/// AMP as a wrapper protocol: parses boxes, dispatches commands to the handler,
/// multiplexes outgoing calls by ask id.
pub struct Amp<H: AmpHandler> {
    handler: H,
    parser: AmpParser,
    next_id: u32,
    pending: HashMap<String, LocalResolver<Args>>,
    lost: bool,
    err: Option<Error>,
    out: Vec<u8>,
}

impl<H: AmpHandler> Amp<H> {
    /// Wraps `handler` with the default box size limit.
    pub fn new(handler: H) -> Amp<H> {
        Amp::with_max_box_size(handler, DEFAULT_MAX_BOX_SIZE)
    }

    /// Wraps `handler`, limiting incoming boxes to `max` encoded bytes.
    ///
    /// Note that the box limit alone does not bound memory: see the module docs
    /// and [`max_pairs`](Self::max_pairs).
    pub fn with_max_box_size(handler: H, max: usize) -> Amp<H> {
        Amp {
            handler,
            parser: AmpParser::with_max_box_size(max),
            next_id: 0,
            pending: HashMap::new(),
            lost: false,
            err: None,
            out: Vec::new(),
        }
    }

    /// Limits the number of key/value pairs stored per incoming box (default
    /// [`DEFAULT_MAX_PAIRS`]). A box with more is rejected as malformed (an
    /// `_error` reply with [`MALFORMED_ERROR_CODE`] if it asked for one); the
    /// extra pairs are framed but not stored, so this bounds the per-box memory
    /// amplification described in the module docs. Call before the protocol is
    /// installed.
    pub fn max_pairs(mut self, n: usize) -> Amp<H> {
        self.parser.max_pairs = n;
        self
    }

    /// The handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// The handler, mutably.
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    /// The framing error that made the protocol abort, if any.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }

    /// Number of calls awaiting an answer.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    fn next_tag(&mut self) -> String {
        loop {
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                continue;
            }
            let tag = format!("{:x}", self.next_id);
            if !self.pending.contains_key(&tag) {
                return tag;
            }
        }
    }

    /// Sends `cmd` and returns a Deferred for the answer. It rejects with the
    /// peer's [`RemoteError`] (see [`remote_error`]) on an `_error` reply, with
    /// [`Error::Closed`] if the connection is lost first (or already was), and
    /// with `Error::Io(InvalidInput)` for an invalid `cmd` (empty or over 65535
    /// bytes) or invalid `args` (empty, reserved or over-long keys, over-long
    /// values).
    pub fn call(&mut self, t: &mut Transport<'_>, cmd: &str, args: Args) -> Deferred<Args> {
        if let Err(e) = validate_cmd(cmd).and_then(|()| validate(&args)) {
            return Deferred::rejected(t.loop_mut(), e);
        }
        if self.lost || !t.is_open() {
            return Deferred::rejected(t.loop_mut(), Error::Closed);
        }
        let (d, res) = t.loop_mut().deferred_local::<Args>();
        let tag = self.next_tag();
        send_box(&mut self.out, t, &[(ASK, &tag), (COMMAND, cmd)], &args);
        self.pending.insert(tag, res);
        d
    }

    /// Like [`call`](Self::call) for a connection known only by id, run through
    /// `Loop::with_protocol`. Rejects with `Closed` / `Busy` if it cannot be reached.
    pub fn call_on(lp: &mut Loop, id: ConnId, cmd: &str, args: Args) -> Deferred<Args> {
        match lp.with_protocol::<Amp<H>, _>(id, |a, t| a.call(t, cmd, args)) {
            Ok(d) => d,
            Err(e) => Deferred::rejected(lp, e),
        }
    }

    /// Sends `cmd` without `_ask`: the peer runs its handler but does not reply.
    /// `Error::Io(InvalidInput)` for an invalid `cmd` or `args` (see
    /// [`call`](Self::call)), `Error::Closed` if the connection is gone.
    pub fn call_no_answer(
        &mut self,
        t: &mut Transport<'_>,
        cmd: &str,
        args: Args,
    ) -> Result<(), Error> {
        validate_cmd(cmd)?;
        validate(&args)?;
        if self.lost || !t.is_open() {
            return Err(Error::Closed);
        }
        send_box(&mut self.out, t, &[(COMMAND, cmd)], &args);
        Ok(())
    }

    /// Delivers the outcome of a deferred reply.
    fn finish_reply(&mut self, t: &mut Transport<'_>, tag: &str, r: Result<Args, Error>) {
        if self.lost || !t.is_open() {
            return;
        }
        let out = &mut self.out;
        match r {
            Ok(a) => send_answer(out, t, tag, a),
            Err(e) => {
                let (code, description) = match remote_error(&e) {
                    Some(re) => (re.code.clone(), re.description.clone()),
                    None => (UNKNOWN_ERROR_CODE.to_string(), e.to_string()),
                };
                send_error(out, t, tag, &code, &description);
            }
        }
    }
}

/// Encodes one box (the `head` pairs first, then `args`) into `out` and writes it
/// with a single `write`.
fn send_box(out: &mut Vec<u8>, t: &mut Transport<'_>, head: &[(&str, &str)], args: &Args) {
    out.clear();
    for (k, v) in head {
        push_str(out, k.as_bytes());
        push_str(out, v.as_bytes());
    }
    for (k, v) in args {
        push_str(out, k.as_bytes());
        push_str(out, v);
    }
    out.extend_from_slice(&[0, 0]);
    t.write(out);
    recycle(out);
}

fn send_answer(out: &mut Vec<u8>, t: &mut Transport<'_>, tag: &str, a: Args) {
    if let Err(e) = validate(&a) {
        send_error(out, t, tag, UNKNOWN_ERROR_CODE, &e.to_string());
        return;
    }
    send_box(out, t, &[(ANSWER, tag)], &a);
}

fn send_error(out: &mut Vec<u8>, t: &mut Transport<'_>, tag: &str, code: &str, desc: &str) {
    let code = if code.is_empty() {
        UNKNOWN_ERROR_CODE
    } else {
        truncate(code, MAX_VALUE)
    };
    let desc = truncate(desc, MAX_VALUE);
    send_box(
        out,
        t,
        &[(ERROR, tag), (ERROR_CODE, code), (ERROR_DESC, desc)],
        &Args::new(),
    );
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut n = max;
    while !s.is_char_boundary(n) {
        n -= 1;
    }
    &s[..n]
}

fn panic_message(p: &(dyn Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".to_string()
    }
}

fn dispatch<H: AmpHandler>(
    handler: &mut H,
    pending: &mut HashMap<String, LocalResolver<Args>>,
    out: &mut Vec<u8>,
    t: &mut Transport<'_>,
    mut b: Args,
    bad: Option<&'static str>,
) -> Flow {
    // A tag is only usable if it is valid UTF-8.
    let take_tag =
        |b: &mut Args, k: &str| -> Option<String> { String::from_utf8(b.remove(k)?).ok() };
    if let Some(what) = bad {
        // Framed but rejected: answer a request with MALFORMED, fail a pending
        // call whose reply was mangled, drop anything else.
        if let Some(tag) = take_tag(&mut b, ASK) {
            send_error(out, t, &tag, MALFORMED_ERROR_CODE, what);
        } else if let Some(r) = take_tag(&mut b, ANSWER)
            .or_else(|| take_tag(&mut b, ERROR))
            .and_then(|tag| pending.remove(&tag))
        {
            r.reject(malformed(what));
        }
        return Flow::Continue;
    }
    if let Some(tag) = take_tag(&mut b, ANSWER) {
        if let Some(r) = pending.remove(&tag) {
            b.retain(|k, _| !is_reserved(k));
            r.resolve(b);
        }
        return Flow::Continue;
    }
    if let Some(tag) = take_tag(&mut b, ERROR) {
        if let Some(r) = pending.remove(&tag) {
            let text =
                |v: Option<Vec<u8>>| String::from_utf8_lossy(&v.unwrap_or_default()).into_owned();
            r.reject(
                RemoteError {
                    code: text(b.remove(ERROR_CODE)),
                    description: text(b.remove(ERROR_DESC)),
                }
                .into(),
            );
        }
        return Flow::Continue;
    }
    // Not a request, answer or error: ignored, as Twisted logs and drops it.
    let Some(cmd) = b.remove(COMMAND) else {
        return Flow::Continue;
    };
    let ask = take_tag(&mut b, ASK);
    let Ok(cmd) = String::from_utf8(cmd) else {
        if let Some(tag) = ask {
            send_error(
                out,
                t,
                &tag,
                MALFORMED_ERROR_CODE,
                "command name is not UTF-8",
            );
        }
        return Flow::Continue;
    };
    b.retain(|k, _| !is_reserved(k));
    let reply = match catch_unwind(AssertUnwindSafe(|| handler.handle(t, &cmd, b))) {
        Ok(r) => r,
        Err(p) => {
            log::error!(
                "twine-proto: panic in AMP handler for {cmd:?}: {}",
                panic_message(&*p)
            );
            AmpReply::Error {
                code: UNKNOWN_ERROR_CODE.into(),
                description: INTERNAL_ERROR.into(),
            }
        }
    };
    match (reply, ask) {
        (AmpReply::Answer(a), Some(tag)) => send_answer(out, t, &tag, a),
        (AmpReply::Error { code, description }, Some(tag)) => {
            send_error(out, t, &tag, &code, &description)
        }
        (AmpReply::Deferred(d), Some(tag)) => {
            d.on_conn::<Amp<H>>(t.id(), move |amp, t, r| amp.finish_reply(t, &tag, r));
        }
        (AmpReply::Deferred(d), None) => d.finally(|_, _| {}),
        (_, None) => {}
    }
    if t.is_open() {
        Flow::Continue
    } else {
        Flow::Close
    }
}

/// A copy of a framing error for `err()` (twine's `Error` is not `Clone`).
fn dup(e: &Error) -> Error {
    match e {
        Error::FrameTooLarge => Error::FrameTooLarge,
        Error::Io(io) => Error::Io(io::Error::new(io.kind(), io.to_string())),
        _ => Error::Closed,
    }
}

impl<H: AmpHandler> Protocol for Amp<H> {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.handler.connection_made(t);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.err.is_some() {
            return;
        }
        let Amp {
            handler,
            parser,
            pending,
            out,
            ..
        } = self;
        let r = parser.feed_boxes(data, &mut |b, bad| {
            dispatch(handler, pending, out, t, b, bad)
        });
        match r {
            Err(e) => {
                self.err = Some(e);
                t.abort();
            }
            Ok(()) => {
                if self.parser.is_stopped() && !t.is_closing() {
                    t.lose_connection();
                }
            }
        }
    }

    fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
        self.lost = true;
        for (_, r) in std::mem::take(&mut self.pending) {
            r.reject(Error::Closed);
        }
        let reason = self.err.as_ref().map(dup).or(reason);
        self.handler.connection_lost(t, reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Independent encoder mirroring `twisted.protocols.amp.AmpBox.serialize`.
    fn twisted_box(kv: &[&str]) -> Vec<u8> {
        let mut b = Vec::new();
        for s in kv {
            b.extend_from_slice(&(s.len() as u16).to_be_bytes());
            b.extend_from_slice(s.as_bytes());
        }
        b.extend_from_slice(&[0, 0]);
        b
    }

    fn args(kv: &[(&str, &str)]) -> Args {
        super::args(kv.iter().copied())
    }

    fn parse_all(p: &mut AmpParser, chunks: &[&[u8]]) -> (Vec<Args>, Result<(), Error>) {
        let mut out = Vec::new();
        let mut res = Ok(());
        for c in chunks {
            res = p.feed(c, &mut |a| {
                out.push(a);
                Flow::Continue
            });
            if res.is_err() {
                break;
            }
        }
        (out, res)
    }

    #[test]
    fn canonical_bytes() {
        // The documented example from the AMP spec: a Sum request.
        let wire = twisted_box(&["_ask", "1", "_command", "Sum", "a", "13", "b", "29"]);
        let mut want = Vec::new();
        want.extend_from_slice(b"\x00\x04_ask\x00\x011");
        want.extend_from_slice(b"\x00\x08_command\x00\x03Sum");
        want.extend_from_slice(b"\x00\x01a\x00\x0213\x00\x01b\x00\x0229\x00\x00");
        assert_eq!(wire, want);
        let (boxes, r) = parse_all(&mut AmpParser::new(), &[&wire]);
        r.unwrap();
        assert_eq!(
            boxes,
            vec![args(&[
                ("_ask", "1"),
                ("_command", "Sum"),
                ("a", "13"),
                ("b", "29")
            ])]
        );
        // Encoding sorts by key; re-decoding is the identity.
        let b = AmpBox::from_args(boxes[0].clone());
        assert_eq!(AmpBox::decode(&b.encode().unwrap()).unwrap(), b);
        // The empty box is two zero bytes.
        assert_eq!(AmpBox::new().encode().unwrap(), vec![0, 0]);
        assert_eq!(
            AmpBox::from_args(args(&[("k", "v")])).encode().unwrap(),
            b"\x00\x01k\x00\x01v\x00\x00"
        );
    }

    #[test]
    fn bytewise_and_multiple_boxes() {
        let mut wire = twisted_box(&["_answer", "1", "total", "42"]);
        wire.extend(twisted_box(&["_command", "Ping"]));
        wire.extend(twisted_box(&[]));
        let want = vec![
            args(&[("_answer", "1"), ("total", "42")]),
            args(&[("_command", "Ping")]),
            Args::new(),
        ];
        let (got, r) = parse_all(&mut AmpParser::new(), &[&wire]);
        r.unwrap();
        assert_eq!(got, want);
        let chunks: Vec<&[u8]> = wire.chunks(1).collect();
        let mut p = AmpParser::new();
        let (got, r) = parse_all(&mut p, &chunks);
        r.unwrap();
        assert_eq!(got, want);
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn empty_value_allowed() {
        let (got, r) = parse_all(&mut AmpParser::new(), &[&twisted_box(&["k", ""])]);
        r.unwrap();
        assert_eq!(got, vec![args(&[("k", "")])]);
    }

    #[test]
    fn oversize_and_malformed() {
        // Box over the limit.
        let mut p = AmpParser::with_max_box_size(20);
        let wire = twisted_box(&["k", "0123456789abcdef"]);
        let (_, r) = parse_all(&mut p, &[&wire]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
        assert!(p.is_stopped());
        // Exactly at the limit is fine.
        let wire = twisted_box(&["k", "v"]);
        let mut p = AmpParser::with_max_box_size(wire.len());
        parse_all(&mut p, &[&wire]).1.unwrap();
        let mut p = AmpParser::with_max_box_size(wire.len() - 1);
        assert!(matches!(
            parse_all(&mut p, &[&wire]).1,
            Err(Error::FrameTooLarge)
        ));
        // Key longer than 255.
        let long = "k".repeat(256);
        let wire = twisted_box(&[&long, "v"]);
        let (_, r) = parse_all(&mut AmpParser::new(), &[&wire]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
        // Values are raw bytes: invalid UTF-8 is fine.
        let wire = vec![0, 1, b'k', 0, 1, 0xff, 0, 0];
        let (got, r) = parse_all(&mut AmpParser::new(), &[&wire]);
        r.unwrap();
        assert_eq!(got[0]["k"], vec![0xff]);
        assert_eq!(arg_str(&got[0], "k"), None);
        // A non-UTF-8 key rejects the box (feed errors once the box ends).
        let wire = vec![0, 1, 0xff, 0, 1, b'v', 0, 0];
        let (_, r) = parse_all(&mut AmpParser::new(), &[&wire]);
        assert!(crate::is_malformed(&r.unwrap_err()));
        // Truncated box is not an error, but decode() insists on completeness.
        let wire = twisted_box(&["k", "v"]);
        assert!(AmpBox::decode(&wire[..wire.len() - 1]).is_err());
        assert!(AmpBox::decode(&[wire.clone(), wire].concat()).is_err());
    }

    #[test]
    fn bad_box_is_delivered_and_framing_continues() {
        // Box 1: _ask, an invalid key, more pairs; box 2 is fine.
        let mut wire = vec![0, 4];
        wire.extend_from_slice(b"_ask\x00\x011");
        wire.extend_from_slice(&[0, 1, 0xfe, 0, 1, b'v']);
        wire.extend_from_slice(&twisted_box(&["k", "v"]));
        wire.extend(twisted_box(&["ok", "1"]));
        let mut p = AmpParser::new();
        let mut got = Vec::new();
        p.feed_boxes(&wire, &mut |a, bad| {
            got.push((a, bad));
            Flow::Continue
        })
        .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1, Some("key is not UTF-8"));
        assert_eq!(got[0].0["_ask"], b"1");
        assert_eq!(got[1], (args(&[("ok", "1")]), None));
    }

    #[test]
    fn max_pairs_bounds_storage() {
        let kv: Vec<String> = (0..10)
            .flat_map(|i| [format!("k{i}"), "v".into()])
            .collect();
        let refs: Vec<&str> = kv.iter().map(String::as_str).collect();
        let wire = twisted_box(&refs);
        let mut p = AmpParser::with_limits(DEFAULT_MAX_BOX_SIZE, 9);
        let mut got = Vec::new();
        p.feed_boxes(&wire, &mut |a, bad| {
            got.push((a.len(), bad));
            Flow::Continue
        })
        .unwrap();
        assert_eq!(got, vec![(9, Some("too many pairs"))]);
        // Exactly at the cap is fine.
        let mut p = AmpParser::with_limits(DEFAULT_MAX_BOX_SIZE, 10);
        let (b, r) = parse_all(&mut p, &[&wire]);
        r.unwrap();
        assert_eq!(b[0].len(), 10);
        // Through feed() an over-cap box is a malformed error.
        let mut p = AmpParser::with_limits(DEFAULT_MAX_BOX_SIZE, 3);
        assert!(crate::is_malformed(
            &parse_all(&mut p, &[&wire]).1.unwrap_err()
        ));
        // Default cap is 4096.
        assert_eq!(DEFAULT_MAX_PAIRS, 4096);
    }

    #[test]
    fn helpers() {
        let a = args(&[("a", "x")]);
        assert_eq!(arg_str(&a, "a"), Some("x"));
        assert_eq!(arg_str(&a, "b"), None);
        let b = super::args([("k", vec![1u8, 2]), ("k", vec![3])]);
        assert_eq!(b["k"], vec![3]);
    }

    #[test]
    fn encode_limits() {
        let mut b = AmpBox::new();
        b.insert("k", "v".repeat(65536));
        assert!(b.encode().is_err());
        let mut b = AmpBox::new();
        b.insert("k".repeat(256), "v");
        assert!(b.encode().is_err());
        let mut b = AmpBox::new();
        b.insert("", "v");
        assert!(b.encode().is_err());
        let mut b = AmpBox::new();
        b.insert("k".repeat(255), "v".repeat(65535));
        assert_eq!(AmpBox::decode(&b.encode().unwrap()).unwrap(), b);
    }

    #[test]
    fn close_flow_stops() {
        let wire = [twisted_box(&["a", "1"]), twisted_box(&["b", "2"])].concat();
        let mut p = AmpParser::new();
        let mut n = 0;
        p.feed(&wire, &mut |_| {
            n += 1;
            Flow::Close
        })
        .unwrap();
        assert_eq!(n, 1);
        assert!(p.is_stopped());
    }

    #[test]
    fn remote_error_roundtrip() {
        let e: Error = RemoteError {
            code: "C".into(),
            description: "d".into(),
        }
        .into();
        let re = remote_error(&e).unwrap();
        assert_eq!((re.code.as_str(), re.description.as_str()), ("C", "d"));
        assert!(remote_error(&Error::Closed).is_none());
        assert!(remote_error(&Error::Io(io::Error::other("x"))).is_none());
    }
}
