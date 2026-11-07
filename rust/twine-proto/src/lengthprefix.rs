//! Length-prefixed framing (Twisted's `Int8/Int16/Int32StringReceiver`):
//! [`FrameFormat`], [`LengthPrefixParser`] (pure) and the [`LengthPrefixed`]
//! protocol wrapper.
//!
//! Each frame is a 1, 2 or 4 byte unsigned length followed by that many bytes.
//! A body longer than the maximum is [`twine::Error::FrameTooLarge`]; with
//! `length_includes_prefix`, a length smaller than the prefix itself is
//! malformed ([`crate::is_malformed`]).

use twine::{Error, Protocol, Transport};

use crate::{malformed_frame, recycle, Flow};

/// Default maximum frame body: 1 MiB.
pub const DEFAULT_MAX_FRAME: usize = 1 << 20;

/// Byte order of the length prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Endian {
    /// Network order (the default, as in Twisted).
    #[default]
    Big,
    /// Little endian.
    Little,
}

/// The wire format of a length-prefixed stream. `Copy`, so a handler that wants
/// to reply with [`send_frame`](Self::send_frame) can keep its own copy of the
/// format it configured the wrapper with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameFormat {
    prefix_len: u8,
    endian: Endian,
    includes_prefix: bool,
    max_frame: usize,
}

impl Default for FrameFormat {
    fn default() -> Self {
        FrameFormat::new()
    }
}

impl FrameFormat {
    /// A 4-byte big-endian prefix counting only the body, 1 MiB limit.
    pub const fn new() -> Self {
        FrameFormat {
            prefix_len: 4,
            endian: Endian::Big,
            includes_prefix: false,
            max_frame: DEFAULT_MAX_FRAME,
        }
    }

    /// Prefix size in bytes: 1, 2 or 4.
    ///
    /// # Panics
    /// If `n` is not 1, 2 or 4.
    pub const fn with_prefix_len(mut self, n: u8) -> Self {
        assert!(
            n == 1 || n == 2 || n == 4,
            "prefix length must be 1, 2 or 4"
        );
        self.prefix_len = n;
        self
    }

    /// Byte order of the prefix.
    pub const fn with_endian(mut self, e: Endian) -> Self {
        self.endian = e;
        self
    }

    /// Whether the prefix value counts its own bytes as well as the body.
    pub const fn with_length_includes_prefix(mut self, on: bool) -> Self {
        self.includes_prefix = on;
        self
    }

    /// Largest accepted (and sendable) body. A limit of 0 is a real limit, not
    /// "use the default": only empty frames are accepted.
    pub const fn with_max_frame(mut self, n: usize) -> Self {
        self.max_frame = n;
        self
    }

    /// Prefix size in bytes.
    pub const fn prefix_len(&self) -> usize {
        self.prefix_len as usize
    }

    /// Largest body this format can send: the smaller of `max_frame` and what the
    /// prefix can encode.
    pub fn max_sendable(&self) -> usize {
        let p = self.prefix_len as u64;
        let mut lim = (1u64 << (8 * p)) - 1;
        if self.includes_prefix {
            lim -= p;
        }
        lim.min(self.max_frame as u64) as usize
    }

    /// Encodes the prefix for a body of `body_len` bytes into the front of the
    /// returned array (use `prefix_len()` bytes). `FrameTooLarge` if the body does
    /// not fit.
    pub fn encode_prefix(&self, body_len: usize) -> Result<[u8; 4], Error> {
        if body_len > self.max_sendable() {
            return Err(Error::FrameTooLarge);
        }
        let mut v = body_len as u32;
        if self.includes_prefix {
            v += self.prefix_len as u32;
        }
        let mut out = [0u8; 4];
        let p = self.prefix_len as usize;
        match self.endian {
            Endian::Big => out[..p].copy_from_slice(&v.to_be_bytes()[4 - p..]),
            Endian::Little => out[..p].copy_from_slice(&v.to_le_bytes()[..p]),
        }
        Ok(out)
    }

    /// Writes the prefix and `body` with one `writev`. Returns `FrameTooLarge`,
    /// writing nothing, when `body` exceeds [`max_sendable`](Self::max_sendable).
    /// Does not allocate.
    pub fn send_frame(&self, t: &mut Transport<'_>, body: &[u8]) -> Result<(), Error> {
        let pre = self.encode_prefix(body.len())?;
        t.writev(&[&pre[..self.prefix_len as usize], body]);
        Ok(())
    }

    fn decode_prefix(&self, src: &[u8]) -> u64 {
        let mut b = [0u8; 4];
        match self.endian {
            Endian::Big => {
                b[4 - src.len()..].copy_from_slice(src);
                u32::from_be_bytes(b) as u64
            }
            Endian::Little => {
                b[..src.len()].copy_from_slice(src);
                u32::from_le_bytes(b) as u64
            }
        }
    }
}

/// Pure length-prefix parser. Holds only the incomplete trailing frame.
#[derive(Debug)]
pub struct LengthPrefixParser {
    fmt: FrameFormat,
    in_body: bool,
    nhdr: usize,
    hdr: [u8; 4],
    n: usize,
    buf: Vec<u8>,
    stopped: bool,
}

impl LengthPrefixParser {
    /// A parser for `fmt`.
    pub fn new(fmt: FrameFormat) -> Self {
        LengthPrefixParser {
            fmt,
            in_body: false,
            nhdr: 0,
            hdr: [0; 4],
            n: 0,
            buf: Vec::new(),
            stopped: false,
        }
    }

    /// The wire format.
    pub fn format(&self) -> &FrameFormat {
        &self.fmt
    }

    /// Bytes currently buffered (partial header plus partial body).
    pub fn buffered(&self) -> usize {
        self.nhdr + self.buf.len()
    }

    /// True once a [`Flow::Close`] or an error stopped the parser.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    fn fail(&mut self, e: Error) -> Error {
        self.stopped = true;
        self.buf = Vec::new();
        e
    }

    /// Feeds a chunk, calling `f` with each frame body (zero-copy when the body
    /// lies inside `data`). Stops with an error on an oversized or malformed
    /// frame; `f` may return [`Flow::Close`] to stop.
    pub fn feed(&mut self, mut data: &[u8], f: &mut dyn FnMut(&[u8]) -> Flow) -> Result<(), Error> {
        let p = self.fmt.prefix_len();
        while !data.is_empty() && !self.stopped {
            if self.in_body {
                let k = (self.n - self.buf.len()).min(data.len());
                self.buf.extend_from_slice(&data[..k]);
                data = &data[k..];
                if self.buf.len() < self.n {
                    return Ok(());
                }
                self.in_body = false;
                if f(&self.buf) == Flow::Close {
                    self.stopped = true;
                }
                recycle(&mut self.buf);
                continue;
            }
            let v = if self.nhdr == 0 && data.len() >= p {
                let v = self.fmt.decode_prefix(&data[..p]);
                data = &data[p..];
                v
            } else {
                let k = (p - self.nhdr).min(data.len());
                self.hdr[self.nhdr..self.nhdr + k].copy_from_slice(&data[..k]);
                self.nhdr += k;
                data = &data[k..];
                if self.nhdr < p {
                    return Ok(());
                }
                self.nhdr = 0;
                self.fmt.decode_prefix(&self.hdr[..p])
            };
            let mut body = v;
            if self.fmt.includes_prefix {
                if v < p as u64 {
                    return Err(self.fail(malformed_frame()));
                }
                body -= p as u64;
            }
            if body > self.fmt.max_frame as u64 {
                return Err(self.fail(Error::FrameTooLarge));
            }
            let n = body as usize;
            if data.len() >= n {
                let body = &data[..n];
                data = &data[n..];
                if f(body) == Flow::Close {
                    self.stopped = true;
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

/// User callbacks for [`LengthPrefixed`]. All methods run on the loop thread.
pub trait FrameHandler: 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}

    /// Called with each frame body; `f` is valid only for the call.
    fn frame_received(&mut self, t: &mut Transport<'_>, f: &[u8]);

    /// The connection closed. A framing error is reported here as `reason`.
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {}

    /// A connection timer armed with `Transport::call_later` fired.
    fn timer_fired(&mut self, _t: &mut Transport<'_>, _token: u64) {}

    /// Producer hook: stop producing.
    fn pause_producing(&mut self, _t: &mut Transport<'_>) {}

    /// Producer hook: resume producing.
    fn resume_producing(&mut self, _t: &mut Transport<'_>) {}

    /// Producer hook: the consumer is gone.
    fn stop_producing(&mut self, _t: &mut Transport<'_>) {}
}

/// Length-prefixed framing as a wrapper protocol.
pub struct LengthPrefixed<H: FrameHandler> {
    handler: H,
    parser: LengthPrefixParser,
    err: Option<Error>,
}

impl<H: FrameHandler> LengthPrefixed<H> {
    /// Wraps `handler` with the default format (4-byte big-endian prefix).
    pub fn new(handler: H) -> Self {
        Self::with_format(handler, FrameFormat::new())
    }

    /// Wraps `handler` with an explicit [`FrameFormat`].
    pub fn with_format(handler: H, fmt: FrameFormat) -> Self {
        LengthPrefixed {
            handler,
            parser: LengthPrefixParser::new(fmt),
            err: None,
        }
    }

    /// The wire format.
    pub fn format(&self) -> &FrameFormat {
        self.parser.format()
    }

    /// The handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// The handler, mutably.
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    /// Writes `body` with this wrapper's format; see [`FrameFormat::send_frame`].
    pub fn send_frame(&self, t: &mut Transport<'_>, body: &[u8]) -> Result<(), Error> {
        self.parser.format().send_frame(t, body)
    }
}

impl<H: FrameHandler> Protocol for LengthPrefixed<H> {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.handler.connection_made(t);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.err.is_some() {
            return;
        }
        let h = &mut self.handler;
        let r = self.parser.feed(data, &mut |s| {
            h.frame_received(t, s);
            if t.is_open() {
                Flow::Continue
            } else {
                Flow::Close
            }
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
        let reason = self.err.take().or(reason);
        self.handler.connection_lost(t, reason);
    }

    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        self.handler.pause_producing(t);
    }

    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        self.handler.resume_producing(t);
    }

    fn stop_producing(&mut self, t: &mut Transport<'_>) {
        self.handler.stop_producing(t);
    }

    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        self.handler.timer_fired(t, token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(p: &mut LengthPrefixParser, chunks: &[&[u8]]) -> (Vec<Vec<u8>>, Result<(), Error>) {
        let mut out = Vec::new();
        let mut res = Ok(());
        for c in chunks {
            res = p.feed(c, &mut |s| {
                out.push(s.to_vec());
                Flow::Continue
            });
            if res.is_err() {
                break;
            }
        }
        (out, res)
    }

    fn encode(fmt: &FrameFormat, body: &[u8]) -> Vec<u8> {
        let pre = fmt.encode_prefix(body.len()).unwrap();
        let mut v = pre[..fmt.prefix_len()].to_vec();
        v.extend_from_slice(body);
        v
    }

    fn all_formats() -> Vec<FrameFormat> {
        let mut v = Vec::new();
        for p in [1u8, 2, 4] {
            for e in [Endian::Big, Endian::Little] {
                for inc in [false, true] {
                    v.push(
                        FrameFormat::new()
                            .with_prefix_len(p)
                            .with_endian(e)
                            .with_length_includes_prefix(inc),
                    );
                }
            }
        }
        v
    }

    #[test]
    fn wire_format() {
        let f = FrameFormat::new();
        assert_eq!(encode(&f, b"abc"), b"\0\0\0\x03abc");
        let f = FrameFormat::new().with_endian(Endian::Little);
        assert_eq!(encode(&f, b"abc"), b"\x03\0\0\0abc");
        let f = FrameFormat::new()
            .with_prefix_len(2)
            .with_length_includes_prefix(true);
        assert_eq!(encode(&f, b"abc"), b"\0\x05abc");
        let f = FrameFormat::new().with_prefix_len(1);
        assert_eq!(encode(&f, b""), b"\0");
    }

    #[test]
    fn all_combos_whole_and_bytewise() {
        for fmt in all_formats() {
            let bodies: [&[u8]; 4] = [b"hello", b"", b"x", b"a longer frame body"];
            let mut wire = Vec::new();
            for b in bodies {
                wire.extend(encode(&fmt, b));
            }
            let want: Vec<Vec<u8>> = bodies.iter().map(|b| b.to_vec()).collect();
            let mut p = LengthPrefixParser::new(fmt);
            let (got, r) = run(&mut p, &[&wire]);
            r.unwrap();
            assert_eq!(got, want, "{fmt:?}");
            let mut p = LengthPrefixParser::new(fmt);
            let chunks: Vec<&[u8]> = wire.chunks(1).collect();
            let (got, r) = run(&mut p, &chunks);
            r.unwrap();
            assert_eq!(got, want, "bytewise {fmt:?}");
            assert_eq!(p.buffered(), 0);
        }
    }

    #[test]
    fn zero_length_frames() {
        let fmt = FrameFormat::new().with_prefix_len(1);
        let mut p = LengthPrefixParser::new(fmt);
        let (got, _) = run(&mut p, &[b"\0\0\0"]);
        assert_eq!(got, vec![Vec::<u8>::new(); 3]);
    }

    #[test]
    fn incomplete_tail_kept() {
        let fmt = FrameFormat::new().with_prefix_len(2);
        let mut p = LengthPrefixParser::new(fmt);
        let (got, _) = run(&mut p, &[b"\0\x05ab"]);
        assert!(got.is_empty());
        assert_eq!(p.buffered(), 2);
        let (got, _) = run(&mut p, &[b"cde\0"]);
        assert_eq!(got, vec![b"abcde".to_vec()]);
        assert_eq!(p.buffered(), 1, "partial header kept");
    }

    #[test]
    fn frame_too_large() {
        let fmt = FrameFormat::new().with_prefix_len(1).with_max_frame(4);
        let mut p = LengthPrefixParser::new(fmt);
        let (got, r) = run(&mut p, &[b"\x04abcd"]);
        r.unwrap();
        assert_eq!(got.len(), 1);
        let (_, r) = run(&mut p, &[b"\x05"]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
        assert!(p.is_stopped());
        let fmt = FrameFormat::new();
        let mut p = LengthPrefixParser::new(fmt);
        let (_, r) = run(&mut p, &[&[0xff, 0xff, 0xff, 0xff]]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
    }

    #[test]
    fn includes_prefix_smaller_than_prefix_is_malformed() {
        let fmt = FrameFormat::new()
            .with_prefix_len(2)
            .with_length_includes_prefix(true);
        let mut p = LengthPrefixParser::new(fmt);
        let (_, r) = run(&mut p, &[b"\0\x01"]);
        assert!(crate::is_malformed(&r.unwrap_err()));
        let mut p = LengthPrefixParser::new(fmt);
        let (got, r) = run(&mut p, &[b"\0\x02"]);
        r.unwrap();
        assert_eq!(got, vec![Vec::<u8>::new()]);
    }

    #[test]
    fn max_sendable_and_encode_errors() {
        let f = FrameFormat::new().with_prefix_len(1);
        assert_eq!(f.max_sendable(), 255);
        assert!(f.encode_prefix(255).is_ok());
        assert!(matches!(f.encode_prefix(256), Err(Error::FrameTooLarge)));
        let f = f.with_length_includes_prefix(true);
        assert_eq!(f.max_sendable(), 254);
        let f = FrameFormat::new().with_prefix_len(2).with_max_frame(10);
        assert_eq!(f.max_sendable(), 10);
        assert!(matches!(f.encode_prefix(11), Err(Error::FrameTooLarge)));
        assert_eq!(FrameFormat::new().max_sendable(), DEFAULT_MAX_FRAME);
    }

    #[test]
    fn large_buffer_released() {
        let fmt = FrameFormat::new().with_max_frame(1 << 20);
        let mut p = LengthPrefixParser::new(fmt);
        let body = vec![7u8; 100_000];
        let wire = encode(&fmt, &body);
        let (got, _) = run(&mut p, &[&wire[..50_000], &wire[50_000..]]);
        assert_eq!(got[0], body);
        assert_eq!(p.buf.capacity(), 0);
    }
}
