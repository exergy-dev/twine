package proto

import (
	"strconv"

	"github.com/robert-malhotra/twine"
)

// DefaultMaxNetstring is the NetstringReceiver limit used when MaxLength is
// zero.
const DefaultMaxNetstring = 1 << 20

// NetstringReceiver is Twisted's NetstringReceiver: it parses
// "len:data," framing (http://cr.yp.to/proto/netstrings.txt) and calls
// StringReceived for each payload. Embed it and set the callback fields.
//
// A declared length above MaxLength aborts with twine.ErrFrameTooLarge as
// soon as the length digits exceed it. Malformed input (an empty length, a
// non-digit before ':', a leading zero in a multi-digit length, or a byte
// other than ',' after the payload) aborts with ErrMalformedNetstring. In
// both cases ConnectionLost reports the error (see the package doc).
type NetstringReceiver struct {
	// MaxLength is the largest accepted payload. Default 1 MiB.
	MaxLength int

	// StringReceived is called with each payload. s is valid only during
	// the call.
	StringReceived func(s []byte)
	// OnConnectionMade, if set, is called from ConnectionMade after the
	// transport has been stored.
	OnConnectionMade func(t twine.Transport)
	// OnConnectionLost, if set, is called from ConnectionLost with the
	// framing error recorded before an abort, or the core's error otherwise.
	OnConnectionLost func(err error)

	t      twine.Transport
	err    error
	state  uint8
	digits int // length digits seen in the current header
	n      int // declared payload length
	buf    []byte
	hdr    [24]byte
	iov    [3][]byte
}

const (
	nsLength uint8 = iota // reading length digits
	nsData                // reading payload into buf
	nsComma               // payload complete, expecting ','
)

var _ twine.Protocol = (*NetstringReceiver)(nil)

var comma = []byte{','}

// ConnectionMade implements twine.Protocol.
func (ns *NetstringReceiver) ConnectionMade(t twine.Transport) {
	ns.t = t
	if ns.OnConnectionMade != nil {
		ns.OnConnectionMade(t)
	}
}

// ConnectionLost implements twine.Protocol. It substitutes the recorded
// framing error, if any, and calls OnConnectionLost.
func (ns *NetstringReceiver) ConnectionLost(err error) {
	if ns.err != nil {
		err = ns.err
	}
	ns.buf = nil
	if ns.OnConnectionLost != nil {
		ns.OnConnectionLost(err)
	}
}

// Transport returns the transport stored by ConnectionMade.
func (ns *NetstringReceiver) Transport() twine.Transport { return ns.t }

// Err returns the framing error that made the receiver abort, or nil.
func (ns *NetstringReceiver) Err() error { return ns.err }

// SendString writes s as a netstring with a single Writev. When t is nil the
// stored transport is used. It does not allocate.
func (ns *NetstringReceiver) SendString(t twine.Transport, s []byte) {
	if t == nil {
		t = ns.t
	}
	h := append(strconv.AppendInt(ns.hdr[:0], int64(len(s)), 10), ':')
	ns.iov[0], ns.iov[1], ns.iov[2] = h, s, comma
	t.Writev(ns.iov[:]...)
	ns.iov = [3][]byte{}
}

func (ns *NetstringReceiver) maxLen() int {
	if ns.MaxLength <= 0 {
		return DefaultMaxNetstring
	}
	return ns.MaxLength
}

func (ns *NetstringReceiver) fail(err error) {
	ns.err = err
	ns.buf = ns.buf[:0]
	if ns.t != nil {
		ns.t.Abort()
	}
}

// DataReceived implements twine.Protocol.
func (ns *NetstringReceiver) DataReceived(b []byte) {
	max := ns.maxLen()
	for len(b) > 0 && ns.err == nil {
		switch ns.state {
		case nsLength:
			c := b[0]
			b = b[1:]
			switch {
			case c >= '0' && c <= '9':
				if ns.digits > 0 && ns.n == 0 {
					ns.fail(ErrMalformedNetstring) // leading zero
					return
				}
				ns.n = ns.n*10 + int(c-'0')
				ns.digits++
				if ns.n > max {
					ns.fail(twine.ErrFrameTooLarge)
					return
				}
			case c == ':' && ns.digits > 0:
				ns.digits = 0
				// Fast path: whole payload and comma in this chunk.
				if len(b) > ns.n {
					if b[ns.n] != ',' {
						ns.fail(ErrMalformedNetstring)
						return
					}
					s := b[:ns.n]
					b = b[ns.n+1:]
					ns.n = 0
					if ns.StringReceived != nil {
						ns.StringReceived(s)
					}
					continue
				}
				ns.state = nsData
				if cap(ns.buf) < ns.n {
					ns.buf = make([]byte, 0, ns.n)
				}
				ns.buf = ns.buf[:0]
			default:
				ns.fail(ErrMalformedNetstring)
				return
			}
		case nsData:
			k := min(ns.n-len(ns.buf), len(b))
			ns.buf = append(ns.buf, b[:k]...)
			b = b[k:]
			if len(ns.buf) == ns.n {
				ns.state = nsComma
			}
		case nsComma:
			if b[0] != ',' {
				ns.fail(ErrMalformedNetstring)
				return
			}
			b = b[1:]
			ns.state = nsLength
			ns.n = 0
			s := ns.buf
			if ns.StringReceived != nil {
				ns.StringReceived(s)
			}
			ns.buf = ns.buf[:0]
		}
	}
}
