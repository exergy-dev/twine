package proto

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/exergy-dev/twine"
)

// DefaultMaxFrame is the LengthPrefixed limit used when MaxFrame is zero.
const DefaultMaxFrame = 1 << 20

// LengthPrefixed replaces Twisted's Int8/Int16/Int32StringReceiver: each
// frame is a 1, 2 or 4 byte unsigned length followed by that many bytes.
// Embed it and set the callback fields.
//
// The zero value uses a 4-byte big-endian prefix that counts only the body.
// Because the zero value of a bool is false, byte order is selected with
// LittleEndian rather than a BigEndian field. Configure the fields before
// the first byte is received.
//
// A body longer than MaxFrame aborts with ErrFrameTooLarge; with
// LengthIncludesPrefix a length smaller than the prefix aborts with
// ErrMalformedFrame. ConnectionLost reports the error (see the package doc).
type LengthPrefixed struct {
	// PrefixLen is the prefix size in bytes: 1, 2 or 4. Default (0) is 4.
	// Any other value makes ConnectionMade panic; the core reports that to
	// ConnectionLost as a *twine.PanicError and closes the connection.
	PrefixLen int
	// LittleEndian selects little-endian prefixes. Default big-endian
	// (network order), matching Twisted.
	LittleEndian bool
	// LengthIncludesPrefix makes the prefix count its own bytes as well as
	// the body.
	LengthIncludesPrefix bool
	// MaxFrame is the largest accepted body. Default 1 MiB.
	MaxFrame int

	// FrameReceived is called with each frame body. f is valid only during
	// the call.
	FrameReceived func(f []byte)
	// OnConnectionMade, if set, is called from ConnectionMade after the
	// transport has been stored.
	OnConnectionMade func(t twine.Transport)
	// OnConnectionLost, if set, is called from ConnectionLost with the
	// framing error recorded before an abort, or the core's error otherwise.
	OnConnectionLost func(err error)

	stream // buf holds a partial body
	inBody bool
	nhdr   int       // header bytes buffered in hdr
	hdr    [4]byte   // partial header on receive
	n      int       // current body length
	out    [4]byte   // prefix scratch for SendFrame
	iov    [2][]byte // Writev argument; a literal through the interface would allocate
}

var _ twine.Protocol = (*LengthPrefixed)(nil)

// ConnectionMade implements twine.Protocol.
func (lp *LengthPrefixed) ConnectionMade(t twine.Transport) {
	switch lp.PrefixLen {
	case 0, 1, 2, 4:
	default:
		panic(fmt.Sprintf("proto: LengthPrefixed.PrefixLen is %d; want 1, 2 or 4 (0 means 4)", lp.PrefixLen))
	}
	lp.made(t, lp.OnConnectionMade)
}

// ConnectionLost implements twine.Protocol. It substitutes the recorded
// framing error, if any, and calls OnConnectionLost.
func (lp *LengthPrefixed) ConnectionLost(err error) {
	lp.lost(err, lp.OnConnectionLost)
}

// Transport returns the transport stored by ConnectionMade.
func (lp *LengthPrefixed) Transport() twine.Transport { return lp.t }

// Err returns the framing error that made the receiver abort, or nil.
func (lp *LengthPrefixed) Err() error { return lp.err }

func (lp *LengthPrefixed) prefixLen() int {
	switch lp.PrefixLen {
	case 1, 2:
		return lp.PrefixLen
	}
	return 4
}

func (lp *LengthPrefixed) maxFrame() int {
	if lp.MaxFrame <= 0 {
		return DefaultMaxFrame
	}
	return lp.MaxFrame
}

// MaxSendable returns the largest body SendFrame can encode with the
// configured prefix.
func (lp *LengthPrefixed) MaxSendable() int {
	p := lp.prefixLen()
	limit := uint64(1)<<(8*p) - 1
	if lp.LengthIncludesPrefix {
		limit -= uint64(p)
	}
	if limit > math.MaxInt {
		return math.MaxInt
	}
	return int(limit)
}

// SendFrame writes the prefix and b to the transport stored by
// ConnectionMade, with a single Writev. It returns ErrFrameTooLarge,
// writing nothing, when b does not fit the prefix (see MaxSendable). It
// does not allocate.
func (lp *LengthPrefixed) SendFrame(b []byte) error {
	if len(b) > lp.MaxSendable() {
		return ErrFrameTooLarge
	}
	p := lp.prefixLen()
	n := uint32(len(b))
	if lp.LengthIncludesPrefix {
		n += uint32(p)
	}
	lp.putPrefix(lp.out[:p], n)
	lp.iov[0], lp.iov[1] = lp.out[:p], b
	lp.t.Writev(lp.iov[:]...)
	lp.iov = [2][]byte{}
	return nil
}

func (lp *LengthPrefixed) putPrefix(dst []byte, n uint32) {
	switch len(dst) {
	case 1:
		dst[0] = byte(n)
	case 2:
		if lp.LittleEndian {
			binary.LittleEndian.PutUint16(dst, uint16(n))
		} else {
			binary.BigEndian.PutUint16(dst, uint16(n))
		}
	default:
		if lp.LittleEndian {
			binary.LittleEndian.PutUint32(dst, n)
		} else {
			binary.BigEndian.PutUint32(dst, n)
		}
	}
}

func (lp *LengthPrefixed) getPrefix(src []byte) uint64 {
	switch len(src) {
	case 1:
		return uint64(src[0])
	case 2:
		if lp.LittleEndian {
			return uint64(binary.LittleEndian.Uint16(src))
		}
		return uint64(binary.BigEndian.Uint16(src))
	default:
		if lp.LittleEndian {
			return uint64(binary.LittleEndian.Uint32(src))
		}
		return uint64(binary.BigEndian.Uint32(src))
	}
}

// bodyLen validates a decoded prefix and returns the body length.
func (lp *LengthPrefixed) bodyLen(v uint64, p int) (int, bool) {
	if lp.LengthIncludesPrefix {
		if v < uint64(p) {
			lp.fail(ErrMalformedFrame)
			return 0, false
		}
		v -= uint64(p)
	}
	if v > uint64(lp.maxFrame()) {
		lp.fail(ErrFrameTooLarge)
		return 0, false
	}
	return int(v), true
}

// DataReceived implements twine.Protocol.
func (lp *LengthPrefixed) DataReceived(b []byte) {
	p := lp.prefixLen()
	for len(b) > 0 && lp.err == nil {
		if lp.inBody {
			k := min(lp.n-len(lp.buf), len(b))
			lp.buf = append(lp.buf, b[:k]...)
			b = b[k:]
			if len(lp.buf) < lp.n {
				return
			}
			lp.inBody = false
			if lp.FrameReceived != nil {
				lp.FrameReceived(lp.buf)
			}
			lp.buf.reset()
			continue
		}
		var v uint64
		if lp.nhdr == 0 && len(b) >= p {
			v = lp.getPrefix(b[:p])
			b = b[p:]
		} else {
			k := copy(lp.hdr[lp.nhdr:p], b)
			lp.nhdr += k
			b = b[k:]
			if lp.nhdr < p {
				return
			}
			lp.nhdr = 0
			v = lp.getPrefix(lp.hdr[:p])
		}
		n, ok := lp.bodyLen(v, p)
		if !ok {
			return
		}
		if len(b) >= n {
			// Fast path: whole body in this chunk, delivered in place.
			f := b[:n]
			b = b[n:]
			if lp.FrameReceived != nil {
				lp.FrameReceived(f)
			}
			continue
		}
		lp.n = n
		lp.inBody = true
		lp.buf.reserve(n)
	}
}
