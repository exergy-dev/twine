//! Netstring framing (`len:data,`, <http://cr.yp.to/proto/netstrings.txt>):
//! [`NetstringParser`] (pure) and the [`Netstrings`] protocol wrapper.
//!
//! A declared length above the maximum is [`twine::Error::FrameTooLarge`],
//! reported as soon as the length digits exceed it. Malformed input (an empty
//! length, a non-digit before `:`, a leading zero in a multi-digit length, or a
//! byte other than `,` after the payload) is an error for which
//! [`crate::is_malformed`] is true.

use twine::{Error, Protocol, Transport};

use crate::{malformed_netstring, recycle, Flow};

/// Default maximum netstring payload: 1 MiB.
pub const DEFAULT_MAX_NETSTRING: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// Reading length digits.
    Length,
    /// Collecting the payload into `buf`.
    Data,
    /// Payload complete in `buf`, expecting `,`.
    Comma,
}

/// Pure netstring parser. Holds only the incomplete trailing payload.
#[derive(Debug)]
pub struct NetstringParser {
    max_length: usize,
    state: State,
    digits: usize,
    n: usize,
    buf: Vec<u8>,
    stopped: bool,
}

impl Default for NetstringParser {
    fn default() -> Self {
        Self::new()
    }
}

impl NetstringParser {
    /// A parser with the default 1 MiB limit.
    pub fn new() -> Self {
        NetstringParser {
            max_length: DEFAULT_MAX_NETSTRING,
            state: State::Length,
            digits: 0,
            n: 0,
            buf: Vec::new(),
            stopped: false,
        }
    }

    /// Sets the largest accepted payload. A limit of 0 is a real limit, not
    /// "use the default": only empty netstrings (`0:,`) are accepted.
    pub fn with_max_length(mut self, n: usize) -> Self {
        self.max_length = n;
        self
    }

    /// The configured maximum payload length.
    pub fn max_length(&self) -> usize {
        self.max_length
    }

    /// Payload bytes currently buffered.
    pub fn buffered(&self) -> usize {
        self.buf.len()
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

    /// Feeds a chunk, calling `f` with each payload (zero-copy when payload and
    /// terminator lie inside `data`). Stops with an error on a limit violation or
    /// malformed input; `f` may return [`Flow::Close`] to stop.
    pub fn feed(&mut self, mut data: &[u8], f: &mut dyn FnMut(&[u8]) -> Flow) -> Result<(), Error> {
        while !data.is_empty() && !self.stopped {
            match self.state {
                State::Length => {
                    let c = data[0];
                    data = &data[1..];
                    match c {
                        b'0'..=b'9' => {
                            if self.digits > 0 && self.n == 0 {
                                return Err(self.fail(malformed_netstring()));
                            }
                            // A length that overflows usize exceeds any limit.
                            match self
                                .n
                                .checked_mul(10)
                                .and_then(|n| n.checked_add((c - b'0') as usize))
                            {
                                Some(n) if n <= self.max_length => self.n = n,
                                _ => return Err(self.fail(Error::FrameTooLarge)),
                            }
                            self.digits += 1;
                        }
                        b':' if self.digits > 0 => {
                            self.digits = 0;
                            let n = self.n;
                            if data.len() > n {
                                // Zero-copy: payload and comma are in this chunk.
                                if data[n] != b',' {
                                    return Err(self.fail(malformed_netstring()));
                                }
                                let s = &data[..n];
                                data = &data[n + 1..];
                                self.n = 0;
                                if f(s) == Flow::Close {
                                    self.stopped = true;
                                }
                            } else {
                                self.state = State::Data;
                            }
                        }
                        _ => return Err(self.fail(malformed_netstring())),
                    }
                }
                State::Data => {
                    let k = (self.n - self.buf.len()).min(data.len());
                    self.buf.extend_from_slice(&data[..k]);
                    data = &data[k..];
                    if self.buf.len() == self.n {
                        self.state = State::Comma;
                    }
                }
                State::Comma => {
                    if data[0] != b',' {
                        return Err(self.fail(malformed_netstring()));
                    }
                    data = &data[1..];
                    self.state = State::Length;
                    self.n = 0;
                    if f(&self.buf) == Flow::Close {
                        self.stopped = true;
                    }
                    recycle(&mut self.buf);
                }
            }
        }
        Ok(())
    }
}

/// User callbacks for [`Netstrings`]. All methods run on the loop thread.
pub trait NetstringHandler: 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}

    /// Called with each payload; `s` is valid only for the call.
    fn string_received(&mut self, t: &mut Transport<'_>, s: &[u8]);

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

/// Twisted's `NetstringReceiver` as a wrapper protocol.
pub struct Netstrings<H: NetstringHandler> {
    handler: H,
    parser: NetstringParser,
    err: Option<Error>,
}

impl<H: NetstringHandler> Netstrings<H> {
    /// Wraps `handler` with the default 1 MiB limit.
    pub fn new(handler: H) -> Self {
        Netstrings {
            handler,
            parser: NetstringParser::new(),
            err: None,
        }
    }

    /// Sets the largest accepted payload. A limit of 0 is a real limit, not
    /// "use the default": only empty netstrings (`0:,`) are accepted.
    pub fn with_max_length(mut self, n: usize) -> Self {
        self.parser = self.parser.with_max_length(n);
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

    /// Writes `s` as a netstring with one `writev`; the length digits live in a
    /// stack buffer. Does not allocate.
    pub fn send_string(t: &mut Transport<'_>, s: &[u8]) {
        let mut hdr = [0u8; 21];
        let h = header(&mut hdr, s.len());
        t.writev(&[h, s, b","]);
    }
}

/// Formats `len:` into `buf` and returns the used suffix.
pub(crate) fn header(buf: &mut [u8; 21], len: usize) -> &[u8] {
    let mut i = buf.len();
    i -= 1;
    buf[i] = b':';
    let mut n = len;
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    &buf[i..]
}

impl<H: NetstringHandler> Protocol for Netstrings<H> {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.handler.connection_made(t);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.err.is_some() {
            return;
        }
        let h = &mut self.handler;
        let r = self.parser.feed(data, &mut |s| {
            h.string_received(t, s);
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

    fn run(p: &mut NetstringParser, chunks: &[&[u8]]) -> (Vec<Vec<u8>>, Result<(), Error>) {
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

    #[test]
    fn basic_and_empty() {
        let mut p = NetstringParser::new();
        let (out, r) = run(&mut p, &[b"5:hello,0:,3:a,b,"]);
        r.unwrap();
        assert_eq!(out, vec![b"hello".to_vec(), vec![], b"a,b".to_vec()]);
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn byte_by_byte() {
        let input = b"5:hello,0:,12:abcdefghijkl,";
        let mut p = NetstringParser::new();
        let chunks: Vec<&[u8]> = input.chunks(1).collect();
        let (out, r) = run(&mut p, &chunks);
        r.unwrap();
        assert_eq!(
            out,
            vec![b"hello".to_vec(), vec![], b"abcdefghijkl".to_vec()]
        );
    }

    #[test]
    fn incomplete_tail_kept() {
        let mut p = NetstringParser::new();
        let (out, _) = run(&mut p, &[b"5:hel"]);
        assert!(out.is_empty());
        assert_eq!(p.buffered(), 3);
        let (out, _) = run(&mut p, &[b"lo"]);
        assert!(out.is_empty());
        let (out, _) = run(&mut p, &[b","]);
        assert_eq!(out, vec![b"hello".to_vec()]);
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn malformed() {
        for bad in [
            &b"x:"[..],
            b":",
            b"00:,",
            b"01:a,",
            b"3:abc;",
            b"1:ab",
            b"-1:a,",
            b"5 :hello,",
        ] {
            let mut p = NetstringParser::new();
            let (_, r) = run(&mut p, &[bad]);
            let e = r.expect_err(std::str::from_utf8(bad).unwrap());
            assert!(crate::is_malformed(&e), "{bad:?}: {e:?}");
            assert!(p.is_stopped());
        }
        // Missing comma split across chunks.
        let mut p = NetstringParser::new();
        let (_, r) = run(&mut p, &[b"2:ab", b"x"]);
        assert!(crate::is_malformed(&r.unwrap_err()));
    }

    #[test]
    fn leading_zero_rules() {
        let mut p = NetstringParser::new();
        let (out, r) = run(&mut p, &[b"0:,10:0123456789,"]);
        r.unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn max_length() {
        let mut p = NetstringParser::new().with_max_length(5);
        let (out, r) = run(&mut p, &[b"5:12345,"]);
        r.unwrap();
        assert_eq!(out.len(), 1);
        // Rejected as soon as the digits exceed the limit, before any payload.
        let mut p = NetstringParser::new().with_max_length(5);
        let (_, r) = run(&mut p, &[b"6"]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
        let mut p = NetstringParser::new();
        let (_, r) = run(&mut p, &[b"99999999999999999999999999:"]);
        assert!(matches!(r, Err(Error::FrameTooLarge)));
    }

    #[test]
    fn header_formatting() {
        let mut b = [0u8; 21];
        assert_eq!(header(&mut b, 0), b"0:");
        assert_eq!(header(&mut b, 1234567), b"1234567:");
        assert_eq!(
            header(&mut b, usize::MAX),
            format!("{}:", usize::MAX).as_bytes()
        );
    }

    #[test]
    fn close_stops() {
        let mut p = NetstringParser::new();
        let mut n = 0;
        p.feed(b"1:a,1:b,", &mut |_| {
            n += 1;
            Flow::Close
        })
        .unwrap();
        assert_eq!(n, 1);
        assert!(p.is_stopped());
    }
}
