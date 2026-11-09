package proto

import "github.com/exergy-dev/twine"

// retainLimit is the partial-frame buffer capacity a framer keeps between
// frames, and the most it allocates up front for a declared frame size;
// larger buffers are released after use.
const retainLimit = 64 << 10

// stream is the connection state LineReceiver, NetstringReceiver and
// LengthPrefixed share: the stored transport, the recorded framing error
// and the partial-frame buffer. Each framer keeps its own exported
// OnConnectionMade and OnConnectionLost fields and passes them in.
type stream struct {
	t   twine.Transport
	err error
	buf accum
}

// made stores t and calls the optional OnConnectionMade hook.
func (s *stream) made(t twine.Transport, hook func(twine.Transport)) {
	s.t = t
	if hook != nil {
		hook(t)
	}
}

// lost releases the buffer, substitutes the recorded framing error for err
// and calls the optional OnConnectionLost hook.
func (s *stream) lost(err error, hook func(error)) {
	if s.err != nil {
		err = s.err
	}
	s.buf = nil
	if hook != nil {
		hook(err)
	}
}

// fail records err and aborts the transport.
func (s *stream) fail(err error) {
	s.err = err
	s.buf = s.buf[:0]
	if s.t != nil {
		s.t.Abort()
	}
}

// accum is a framer's partial-frame buffer.
type accum []byte

// reserve empties a for a frame of n bytes. A peer declares n, so at most
// retainLimit is allocated up front and append grows the rest as bytes
// arrive.
func (a *accum) reserve(n int) {
	if cap(*a) < min(n, retainLimit) {
		*a = make(accum, 0, min(n, retainLimit))
		return
	}
	*a = (*a)[:0]
}

// reset empties a after a frame was delivered, releasing storage above
// retainLimit so a large frame does not stay pinned to the connection.
func (a *accum) reset() {
	if cap(*a) > retainLimit {
		*a = nil
		return
	}
	*a = (*a)[:0]
}
