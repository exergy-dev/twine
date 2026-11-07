//! Line framing: [`LineParser`] (pure) and the [`Lines`] protocol wrapper.
//!
//! Lines are split on the exact delimiter only: with the default `\r\n` a bare
//! `\n` is part of the line. Use `with_delimiter(b"\n")` to accept bare newlines
//! (a trailing `\r` then stays in the line).
//!
//! A line longer than the maximum (excluding the delimiter) is an error
//! ([`twine::Error::LineTooLong`]). The check applies to complete lines and to a
//! pending partial line as soon as it is longer than `max + delimiter.len() - 1`
//! bytes, so an endless line is caught without waiting for its delimiter.

use twine::{Error, Protocol, Transport};

use crate::Flow;

/// Default maximum line length (excluding the delimiter): 16 KiB.
pub const DEFAULT_MAX_LINE_LENGTH: usize = 16 << 10;

const CRLF: &[u8] = b"\r\n";

/// Upper bound on the partial-line capacity kept between lines.
const MAX_RETAIN: usize = 64 << 10;

/// What [`LineParser::feed`] yields to its callback.
#[derive(Debug)]
pub enum Framed<'a> {
    /// A complete line without its delimiter. Zero-copy when the line lies
    /// inside the fed chunk; otherwise it points into the parser's buffer.
    Line(&'a [u8]),
    /// Raw-mode data (the fed chunk, or the part of it after a mode switch).
    Raw(&'a mut [u8]),
}

/// Pure line parser. Holds only the incomplete trailing line.
#[derive(Debug)]
pub struct LineParser {
    buf: Vec<u8>,
    /// Empty means the default `\r\n` (kept empty so `new` does not allocate).
    delimiter: Vec<u8>,
    max_line_length: usize,
    raw: bool,
    stopped: bool,
    reparse: bool,
}

impl Default for LineParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Position of the first occurrence of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    match needle {
        [] => None,
        [b] => hay.iter().position(|x| x == b),
        [first, rest @ ..] => {
            let last = hay.len().checked_sub(needle.len())?;
            let mut i = 0;
            while i <= last {
                let j = hay[i..=last].iter().position(|x| x == first)?;
                i += j;
                if &hay[i + 1..i + needle.len()] == rest {
                    return Some(i);
                }
                i += 1;
            }
            None
        }
    }
}

impl LineParser {
    /// A parser with delimiter `\r\n` and a 16 KiB limit.
    pub fn new() -> Self {
        LineParser {
            buf: Vec::new(),
            delimiter: Vec::new(),
            max_line_length: DEFAULT_MAX_LINE_LENGTH,
            raw: false,
            stopped: false,
            reparse: false,
        }
    }

    /// Sets the delimiter (an empty slice selects the default `\r\n`). Must be
    /// done before data is fed.
    pub fn with_delimiter(mut self, d: &[u8]) -> Self {
        self.delimiter = if d == CRLF { Vec::new() } else { d.to_vec() };
        self
    }

    /// Sets the longest accepted line, excluding the delimiter. A limit of 0
    /// is a real limit, not "use the default": only empty lines are accepted.
    pub fn with_max_line_length(mut self, n: usize) -> Self {
        self.max_line_length = n;
        self
    }

    /// The configured delimiter.
    pub fn delimiter(&self) -> &[u8] {
        if self.delimiter.is_empty() {
            CRLF
        } else {
            &self.delimiter
        }
    }

    /// The configured maximum line length.
    pub fn max_line_length(&self) -> usize {
        self.max_line_length
    }

    /// Switches to raw mode, discarding any buffered partial line.
    pub fn set_raw_mode(&mut self) {
        self.raw = true;
        self.recycle_buf();
    }

    /// Switches back to line mode. `extra`, if non-empty, is parsed as if it had
    /// just been received: its lines are delivered by the next call to
    /// [`feed`](Self::feed) (which may be given an empty chunk to flush).
    pub fn set_line_mode(&mut self, extra: &[u8]) {
        self.raw = false;
        if !extra.is_empty() {
            self.buf.extend_from_slice(extra);
            self.reparse = true;
        }
    }

    /// True in raw mode.
    pub fn raw_mode(&self) -> bool {
        self.raw
    }

    /// True once a [`Flow::Close`] or an error stopped the parser; later input is
    /// ignored.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Capacity of the partial-line buffer (exposed for tests of buffer reuse).
    #[doc(hidden)]
    pub fn buffer_capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Bytes currently buffered (the incomplete trailing line).
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Feeds a chunk, calling `f` for every complete line (or, in raw mode, for
    /// the data). Returns `Err(LineTooLong)` when a line exceeds the limit; the
    /// parser is then stopped. `f`'s return value switches modes or stops, see
    /// [`Flow`].
    pub fn feed(
        &mut self,
        data: &mut [u8],
        f: &mut dyn FnMut(Framed<'_>) -> Flow,
    ) -> Result<(), Error> {
        if self.reparse && !self.stopped {
            self.reparse = false;
            let mut tmp = std::mem::take(&mut self.buf);
            self.feed_inner(&mut tmp, f)?;
            if self.buf.is_empty() && tmp.capacity() <= self.retain_cap() {
                tmp.clear();
                self.buf = tmp;
            }
        }
        self.feed_inner(data, f)
    }

    fn retain_cap(&self) -> usize {
        self.max_line_length
            .saturating_add(self.delimiter().len())
            .clamp(crate::RETAIN, MAX_RETAIN)
    }

    /// Clears the partial-line buffer, keeping its capacity if it is no larger
    /// than a line within the configured limit (bounded by `MAX_RETAIN`), so a
    /// stream of large lines that span chunks does not reallocate per line.
    /// (`recycle`'s fixed 4 KiB would drop the buffer after every such line.)
    fn recycle_buf(&mut self) {
        if self.buf.capacity() > self.retain_cap() {
            self.buf = Vec::new();
        } else {
            self.buf.clear();
        }
    }

    fn fail(&mut self) -> Error {
        self.stopped = true;
        self.buf = Vec::new();
        Error::LineTooLong
    }

    fn feed_inner(
        &mut self,
        data: &mut [u8],
        f: &mut dyn FnMut(Framed<'_>) -> Flow,
    ) -> Result<(), Error> {
        let mut pos = 0;
        let max = self.max_line_length;
        loop {
            if self.stopped || pos >= data.len() {
                return Ok(());
            }
            if self.raw {
                let n = data.len() - pos;
                match f(Framed::Raw(&mut data[pos..])) {
                    Flow::Close => {
                        self.stopped = true;
                        return Ok(());
                    }
                    Flow::LineMode(k) => {
                        self.raw = false;
                        pos = data.len() - k.min(n);
                    }
                    Flow::Continue | Flow::RawMode => return Ok(()),
                }
                continue;
            }
            let d: &[u8] = if self.delimiter.is_empty() {
                CRLF
            } else {
                &self.delimiter
            };
            let rest = &data[pos..];
            // Longest pending partial line that can still turn out to be a
            // valid line (saturating: `max` may be `usize::MAX`).
            let partial_max = max.saturating_add(d.len() - 1);
            let flow;
            if self.buf.is_empty() {
                match find(rest, d) {
                    Some(i) => {
                        if i > max {
                            return Err(self.fail());
                        }
                        flow = f(Framed::Line(&rest[..i]));
                        pos += i + d.len();
                    }
                    None => {
                        if rest.len() > partial_max {
                            return Err(self.fail());
                        }
                        self.buf.extend_from_slice(rest);
                        return Ok(());
                    }
                }
            } else {
                // Complete the buffered partial line. A delimiter that starts in
                // the buffer can only start in its last `d.len() - 1` bytes.
                let tl = self.buf.len().min(d.len() - 1);
                let mut found = None;
                for s in 0..tl {
                    let have = tl - s;
                    let start = self.buf.len() - have;
                    let need = d.len() - have;
                    if self.buf[start..] == d[..have]
                        && rest.len() >= need
                        && rest[..need] == d[have..]
                    {
                        found = Some((start, need));
                        break;
                    }
                }
                if found.is_none() {
                    if let Some(i) = find(rest, d) {
                        if self.buf.len().saturating_add(i) > max {
                            return Err(self.fail());
                        }
                        self.buf.extend_from_slice(&rest[..i]);
                        found = Some((self.buf.len(), i + d.len()));
                    }
                }
                let Some((line_len, consumed)) = found else {
                    if self.buf.len().saturating_add(rest.len()) > partial_max {
                        return Err(self.fail());
                    }
                    self.buf.extend_from_slice(rest);
                    return Ok(());
                };
                if line_len > max {
                    return Err(self.fail());
                }
                flow = f(Framed::Line(&self.buf[..line_len]));
                self.recycle_buf();
                pos += consumed;
            }
            match flow {
                Flow::Close => self.stopped = true,
                Flow::RawMode => self.raw = true,
                Flow::Continue | Flow::LineMode(_) => {}
            }
        }
    }
}

/// User callbacks for [`Lines`]. All methods run on the loop thread.
pub trait LineHandler: 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}

    /// Called for each line, without its delimiter; `line` is valid only for the
    /// call. Return [`Flow::Continue`] normally, [`Flow::RawMode`] to deliver the
    /// rest of the stream (including the remainder of this chunk) to
    /// [`raw_data_received`](Self::raw_data_received), or [`Flow::Close`] to stop
    /// parsing and close the connection gracefully. (Raw mode is requested by
    /// return value because the handler cannot reach the wrapper that owns it.)
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow;

    /// Called with data while in raw mode. Return [`Flow::LineMode`]`(n)` to hand
    /// the last `n` bytes of `data` back to the line parser, [`Flow::Close`] to
    /// close, or [`Flow::Continue`]. Default: discard the data.
    fn raw_data_received(&mut self, _t: &mut Transport<'_>, _data: &mut [u8]) -> Flow {
        Flow::Continue
    }

    /// The connection closed. A framing error (e.g. `LineTooLong`) is reported
    /// here as `reason`.
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

/// Twisted's `LineReceiver` as a wrapper protocol: owns a [`LineParser`] and a
/// [`LineHandler`].
pub struct Lines<H: LineHandler> {
    handler: H,
    parser: LineParser,
    err: Option<Error>,
}

impl<H: LineHandler> Lines<H> {
    /// Wraps `handler` with delimiter `\r\n` and a 16 KiB line limit.
    pub fn new(handler: H) -> Self {
        Lines {
            handler,
            parser: LineParser::new(),
            err: None,
        }
    }

    /// Sets the delimiter (default `\r\n`).
    pub fn with_delimiter(mut self, d: &[u8]) -> Self {
        self.parser = self.parser.with_delimiter(d);
        self
    }

    /// Sets the maximum line length (default [`DEFAULT_MAX_LINE_LENGTH`]). A
    /// limit of 0 is a real limit, not "use the default": only empty lines are
    /// accepted.
    pub fn with_max_line_length(mut self, n: usize) -> Self {
        self.parser = self.parser.with_max_line_length(n);
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

    /// Switches to raw mode (for code that reaches the wrapper, e.g. via
    /// `TransportHandle::call_proto`); handlers use [`Flow::RawMode`].
    pub fn set_raw_mode(&mut self) {
        self.parser.set_raw_mode();
    }

    /// Switches back to line mode; `extra` is parsed before the next received
    /// data.
    pub fn set_line_mode(&mut self, extra: &[u8]) {
        self.parser.set_line_mode(extra);
    }

    /// True in raw mode.
    pub fn raw_mode(&self) -> bool {
        self.parser.raw_mode()
    }

    /// Writes `line` followed by `\r\n` with one `writev`. Does not allocate.
    pub fn send_line(t: &mut Transport<'_>, line: &[u8]) {
        t.writev(&[line, CRLF]);
    }

    /// Writes `line` followed by this wrapper's configured delimiter.
    pub fn send_line_with(&self, t: &mut Transport<'_>, line: &[u8]) {
        t.writev(&[line, self.parser.delimiter()]);
    }
}

impl<H: LineHandler> Protocol for Lines<H> {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.handler.connection_made(t);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.err.is_some() {
            return;
        }
        let h = &mut self.handler;
        let r = self.parser.feed(data, &mut |fr| {
            let fl = match fr {
                Framed::Line(l) => h.line_received(t, l),
                Framed::Raw(d) => h.raw_data_received(t, d),
            };
            // A handler that aborted must not see the rest of the chunk.
            if fl != Flow::Close && !t.is_open() {
                Flow::Close
            } else {
                fl
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

    fn run(p: &mut LineParser, chunks: &[&[u8]]) -> (Vec<Vec<u8>>, Vec<u8>, Result<(), Error>) {
        let mut lines = Vec::new();
        let mut raw = Vec::new();
        let mut res = Ok(());
        for c in chunks {
            let mut v = c.to_vec();
            res = p.feed(&mut v, &mut |fr| {
                match fr {
                    Framed::Line(l) => lines.push(l.to_vec()),
                    Framed::Raw(r) => raw.extend_from_slice(r),
                }
                Flow::Continue
            });
            if res.is_err() {
                break;
            }
        }
        (lines, raw, res)
    }

    fn l(s: &[&str]) -> Vec<Vec<u8>> {
        s.iter().map(|x| x.as_bytes().to_vec()).collect()
    }

    #[test]
    fn basic_and_empty_lines() {
        let mut p = LineParser::new();
        let (lines, _, r) = run(&mut p, &[b"a\r\n\r\nbc\r\nd"]);
        r.unwrap();
        assert_eq!(lines, l(&["a", "", "bc"]));
        assert_eq!(p.buffered(), 1);
    }

    #[test]
    fn bare_lf_is_part_of_line_with_default_delimiter() {
        let mut p = LineParser::new();
        let (lines, _, _) = run(&mut p, &[b"a\nb\r\n"]);
        assert_eq!(lines, l(&["a\nb"]));
    }

    #[test]
    fn byte_by_byte_equals_one_chunk() {
        let input = b"one\r\ntwo two\r\n\r\nthree\r\n";
        let mut p = LineParser::new();
        let (whole, _, _) = run(&mut p, &[input]);
        let mut p = LineParser::new();
        let singles: Vec<&[u8]> = input.chunks(1).collect();
        let (bytewise, _, _) = run(&mut p, &singles);
        assert_eq!(whole, l(&["one", "two two", "", "three"]));
        assert_eq!(whole, bytewise);
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn custom_delimiters() {
        let mut p = LineParser::new().with_delimiter(b"\n");
        let (lines, _, _) = run(&mut p, &[b"a\r\nb\n"]);
        assert_eq!(lines, l(&["a\r", "b"]));
        let mut p = LineParser::new().with_delimiter(b"<>!");
        let (lines, _, _) = run(&mut p, &[b"x<>!y<><>!<>!z"]);
        assert_eq!(lines, l(&["x", "y<>", ""]));
        assert_eq!(p.buffered(), 1);
    }

    #[test]
    fn delimiter_split_across_chunks() {
        for split in 1..4 {
            let mut p = LineParser::new().with_delimiter(b"<>!");
            let input = b"ab<>!cd<>!";
            let (a, b) = input.split_at(2 + split);
            let (lines, _, r) = run(&mut p, &[a, b]);
            r.unwrap();
            assert_eq!(lines, l(&["ab", "cd"]), "split {split}");
        }
        // Self-overlapping delimiter.
        let mut p = LineParser::new().with_delimiter(b"aa");
        let (lines, _, _) = run(&mut p, &[b"xa", b"aya", b"a"]);
        assert_eq!(lines, l(&["x", "y"]));
    }

    #[test]
    fn crlf_split_between_cr_and_lf() {
        let mut p = LineParser::new();
        let (lines, _, _) = run(&mut p, &[b"abc\r", b"\ndef\r\n"]);
        assert_eq!(lines, l(&["abc", "def"]));
    }

    #[test]
    fn max_length_complete_line() {
        let mut p = LineParser::new().with_max_line_length(5);
        let (lines, _, r) = run(&mut p, &[b"12345\r\n"]);
        r.unwrap();
        assert_eq!(lines, l(&["12345"]));
        let mut p = LineParser::new().with_max_line_length(5);
        let (_, _, r) = run(&mut p, &[b"123456\r\n"]);
        assert!(matches!(r, Err(Error::LineTooLong)));
        assert!(p.is_stopped());
        // Later input is ignored.
        let (lines, _, r) = run(&mut p, &[b"ok\r\n"]);
        assert!(r.is_ok() && lines.is_empty());
    }

    #[test]
    fn endless_line_detected_without_delimiter() {
        // max 5, delimiter 2 bytes: 6 buffered bytes (max + len(d) - 1) are still
        // legal (5 bytes + a lone '\r'); the 7th is not.
        let mut p = LineParser::new().with_max_line_length(5);
        let (_, _, r) = run(&mut p, &[b"12345\r"]);
        r.unwrap();
        assert_eq!(p.buffered(), 6);
        let (_, _, r) = run(&mut p, &[b"x"]);
        assert!(matches!(r, Err(Error::LineTooLong)));
        assert_eq!(p.buffered(), 0, "buffer released on error");
        // Unbuffered path: one big chunk with no delimiter.
        let mut p = LineParser::new().with_max_line_length(5);
        let (_, _, r) = run(&mut p, &[b"1234567"]);
        assert!(matches!(r, Err(Error::LineTooLong)));
        // Buffered path: line completes in the second chunk but is too long.
        let mut p = LineParser::new().with_max_line_length(5);
        let (_, _, r) = run(&mut p, &[b"123", b"456\r\n"]);
        assert!(matches!(r, Err(Error::LineTooLong)));
    }

    #[test]
    fn raw_mode_switch_mid_chunk() {
        let mut p = LineParser::new();
        let mut lines = Vec::new();
        let mut raw = Vec::new();
        let mut data = b"a\r\nRAW\r\nxyz\r\n12".to_vec();
        p.feed(&mut data, &mut |fr| match fr {
            Framed::Line(l) => {
                lines.push(l.to_vec());
                if l == b"RAW" {
                    Flow::RawMode
                } else {
                    Flow::Continue
                }
            }
            Framed::Raw(r) => {
                raw.extend_from_slice(r);
                Flow::Continue
            }
        })
        .unwrap();
        assert_eq!(lines, l(&["a", "RAW"]));
        assert_eq!(raw, b"xyz\r\n12");
        assert!(p.raw_mode());
        let (_, raw2, _) = run(&mut p, &[b"more"]);
        assert_eq!(raw2, b"more");
    }

    #[test]
    fn raw_mode_from_buffered_line() {
        let mut p = LineParser::new();
        let mut raw = Vec::new();
        let mut data = b"RA".to_vec();
        p.feed(&mut data, &mut |_| Flow::Continue).unwrap();
        let mut data = b"W\r\nrest".to_vec();
        p.feed(&mut data, &mut |fr| match fr {
            Framed::Line(_) => Flow::RawMode,
            Framed::Raw(r) => {
                raw.extend_from_slice(r);
                Flow::Continue
            }
        })
        .unwrap();
        assert_eq!(raw, b"rest");
        assert_eq!(p.buffered(), 0);
    }

    #[test]
    fn raw_handler_returns_unconsumed_to_line_mode() {
        let mut p = LineParser::new();
        p.set_raw_mode();
        let mut lines = Vec::new();
        let mut data = b"1234a\r\nb\r\nc".to_vec();
        p.feed(&mut data, &mut |fr| match fr {
            Framed::Raw(_) => Flow::LineMode(7),
            Framed::Line(l) => {
                lines.push(l.to_vec());
                Flow::Continue
            }
        })
        .unwrap();
        assert!(!p.raw_mode());
        assert_eq!(lines, l(&["a", "b"]));
        assert_eq!(p.buffered(), 1);
    }

    #[test]
    fn set_line_mode_with_extra_reparses() {
        let mut p = LineParser::new();
        p.set_raw_mode();
        p.set_line_mode(b"x\r\ny\r\npart");
        let (lines, _, r) = run(&mut p, &[b"ial\r\n"]);
        r.unwrap();
        assert_eq!(lines, l(&["x", "y", "partial"]));
        // Flush with an empty chunk.
        p.set_line_mode(b"z\r\n");
        let (lines, _, _) = run(&mut p, &[b""]);
        assert_eq!(lines, l(&["z"]));
    }

    #[test]
    fn close_stops_parsing() {
        let mut p = LineParser::new();
        let mut n = 0;
        let mut data = b"a\r\nb\r\nc\r\n".to_vec();
        p.feed(&mut data, &mut |_| {
            n += 1;
            Flow::Close
        })
        .unwrap();
        assert_eq!(n, 1);
        assert!(p.is_stopped());
    }

    #[test]
    fn buffer_is_released_after_large_line() {
        // Retention is bounded (64 KiB) however high the limit is.
        let mut p = LineParser::new().with_max_line_length(1 << 20);
        let big = vec![b'x'; 100_000];
        let (_, _, _) = run(&mut p, &[&big]);
        assert_eq!(p.buffered(), 100_000);
        let (lines, _, _) = run(&mut p, &[b"\r\n"]);
        assert_eq!(lines[0].len(), 100_000);
        assert_eq!(p.buffered(), 0);
        assert_eq!(p.buf.capacity(), 0);
    }
}
