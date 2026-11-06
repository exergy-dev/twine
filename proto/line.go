package proto

import (
	"bytes"

	"github.com/robert-malhotra/twine"
)

// defaultDelimiter is the LineReceiver delimiter used when Delimiter is
// empty. Unexported so it cannot be mutated.
var defaultDelimiter = []byte("\r\n")

// DefaultMaxLineLength is the LineReceiver line limit used when
// MaxLineLength is zero.
const DefaultMaxLineLength = 16 << 10

// LineReceiver is Twisted's LineReceiver: it splits the byte stream into
// lines and calls LineReceived for each one. Embed it and set the callback
// fields.
//
// Lines are split on the exact Delimiter only: with the default "\r\n" a bare
// "\n" is part of the line. Set Delimiter to "\n" to accept bare newlines
// (a trailing '\r' then stays in the line).
//
// A line longer than MaxLineLength (excluding the delimiter) aborts the
// connection and ConnectionLost reports twine.ErrLineTooLong. The check is
// applied to complete lines and to a pending partial line once it is longer
// than MaxLineLength+len(Delimiter)-1 bytes, so an endless line is caught
// without waiting for its delimiter.
//
// Lines already received continue to be delivered from the current chunk
// after the callback calls LoseConnection; they stop after an Abort by the
// receiver itself. Changing Delimiter while data is buffered is undefined.
type LineReceiver struct {
	// Delimiter separates lines. Default "\r\n".
	Delimiter []byte
	// MaxLineLength is the longest accepted line, excluding the delimiter.
	// Default 16 KiB.
	MaxLineLength int

	// LineReceived is called for each line, without its delimiter. line is
	// valid only during the call.
	LineReceived func(line []byte)
	// RawDataReceived receives data in raw mode (see SetRawMode). b is valid
	// only during the call. When nil, raw data is discarded.
	RawDataReceived func(b []byte)
	// OnConnectionMade, if set, is called from ConnectionMade after the
	// transport has been stored.
	OnConnectionMade func(t twine.Transport)
	// OnConnectionLost, if set, is called from ConnectionLost with the
	// framing error recorded before an abort, or the core's error otherwise.
	OnConnectionLost func(err error)

	t   twine.Transport
	buf []byte // incomplete trailing line (may end with a delimiter prefix)
	win []byte // scratch for delimiter-spanning search
	err error
	raw bool
	iov [2][]byte
}

var _ twine.Protocol = (*LineReceiver)(nil)

// ConnectionMade implements twine.Protocol.
func (lr *LineReceiver) ConnectionMade(t twine.Transport) {
	lr.t = t
	if lr.OnConnectionMade != nil {
		lr.OnConnectionMade(t)
	}
}

// ConnectionLost implements twine.Protocol. It substitutes the recorded
// framing error, if any, and calls OnConnectionLost.
func (lr *LineReceiver) ConnectionLost(err error) {
	if lr.err != nil {
		err = lr.err
	}
	lr.buf = nil
	if lr.OnConnectionLost != nil {
		lr.OnConnectionLost(err)
	}
}

// Transport returns the transport stored by ConnectionMade.
func (lr *LineReceiver) Transport() twine.Transport { return lr.t }

// Err returns the framing error that made the receiver abort, or nil.
func (lr *LineReceiver) Err() error { return lr.err }

// SetRawMode switches to raw mode: subsequent data, including the rest of
// the current chunk when called from LineReceived, goes to RawDataReceived.
func (lr *LineReceiver) SetRawMode() { lr.raw = true }

// SetLineMode switches back to line mode. extra, if non-empty, is parsed as
// if it had just been received (Twisted semantics: a RawDataReceived
// callback hands back the bytes it did not consume).
func (lr *LineReceiver) SetLineMode(extra []byte) {
	lr.raw = false
	if len(extra) > 0 {
		lr.DataReceived(extra)
	}
}

// RawMode reports whether the receiver is in raw mode.
func (lr *LineReceiver) RawMode() bool { return lr.raw }

// SendLine writes line followed by the delimiter with a single Writev. When
// t is nil the stored transport is used. It does not allocate.
func (lr *LineReceiver) SendLine(t twine.Transport, line []byte) {
	if t == nil {
		t = lr.t
	}
	lr.iov[0], lr.iov[1] = line, lr.delim()
	t.Writev(lr.iov[:]...)
	lr.iov = [2][]byte{}
}

func (lr *LineReceiver) delim() []byte {
	if len(lr.Delimiter) == 0 {
		return defaultDelimiter
	}
	return lr.Delimiter
}

func (lr *LineReceiver) maxLen() int {
	if lr.MaxLineLength <= 0 {
		return DefaultMaxLineLength
	}
	return lr.MaxLineLength
}

// fail records err and aborts the transport.
func (lr *LineReceiver) fail(err error) {
	lr.err = err
	lr.buf = lr.buf[:0]
	if lr.t != nil {
		lr.t.Abort()
	}
}

// DataReceived implements twine.Protocol.
func (lr *LineReceiver) DataReceived(b []byte) {
	if lr.err != nil {
		return
	}
	if lr.raw {
		if lr.RawDataReceived != nil {
			lr.RawDataReceived(b)
		}
		return
	}
	d := lr.delim()
	max := lr.maxLen()

	if len(lr.buf) > 0 {
		// Complete the buffered partial line first.
		var line []byte
		pos := -1
		m := min(len(lr.buf), len(d)-1)
		if m > 0 {
			lr.win = append(append(lr.win[:0], lr.buf[len(lr.buf)-m:]...), b[:min(len(b), len(d)-1)]...)
			if i := bytes.Index(lr.win, d); i >= 0 && i < m {
				// Delimiter starts in the buffer and ends in b.
				line = lr.buf[:len(lr.buf)-(m-i)]
				pos = len(d) - (m - i)
			}
		}
		if pos < 0 {
			i := bytes.Index(b, d)
			if i < 0 {
				lr.buf = append(lr.buf, b...)
				if len(lr.buf) > max+len(d)-1 {
					lr.fail(twine.ErrLineTooLong)
				}
				return
			}
			lr.buf = append(lr.buf, b[:i]...)
			line = lr.buf
			pos = i + len(d)
		}
		if len(line) > max {
			lr.fail(twine.ErrLineTooLong)
			return
		}
		// Detach the buffer before the callback so a re-entrant
		// SetLineMode(extra) cannot overwrite line; reclaim the storage
		// afterwards when nothing re-entered.
		saved := lr.buf[:0]
		lr.buf = nil
		if lr.LineReceived != nil {
			lr.LineReceived(line)
		}
		if lr.buf == nil {
			lr.buf = saved
		}
		b = b[pos:]
		if lr.err != nil {
			return
		}
		if lr.raw {
			if len(b) > 0 && lr.RawDataReceived != nil {
				lr.RawDataReceived(b)
			}
			return
		}
		if len(lr.buf) > 0 {
			// A re-entrant SetLineMode left a partial; keep stream order.
			lr.buf = append(lr.buf, b...)
			lr.reparse()
			return
		}
	}
	lr.parse(b, d, max)
}

// parse delivers every complete line of b in place and buffers the tail.
// The partial buffer must be empty.
func (lr *LineReceiver) parse(b, d []byte, max int) {
	for len(b) > 0 {
		i := bytes.Index(b, d)
		if i < 0 {
			if len(b) > max+len(d)-1 {
				lr.fail(twine.ErrLineTooLong)
				return
			}
			lr.buf = append(lr.buf, b...)
			return
		}
		if i > max {
			lr.fail(twine.ErrLineTooLong)
			return
		}
		line := b[:i]
		b = b[i+len(d):]
		if lr.LineReceived != nil {
			lr.LineReceived(line)
		}
		if lr.err != nil {
			return
		}
		if lr.raw {
			if len(b) > 0 && lr.RawDataReceived != nil {
				lr.RawDataReceived(b)
			}
			return
		}
		if len(lr.buf) > 0 {
			lr.buf = append(lr.buf, b...)
			lr.reparse()
			return
		}
	}
}

// reparse handles the rare case where a callback re-entered DataReceived
// (via SetLineMode) and left a partial line: the buffered bytes are taken
// out and parsed again as a fresh chunk.
func (lr *LineReceiver) reparse() {
	pending := lr.buf
	lr.buf = nil
	lr.DataReceived(pending)
}
