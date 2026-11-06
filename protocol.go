package twine

import (
	"net"
	"time"
)

// Protocol is implemented by user code. All methods are called on the owning
// Loop.
type Protocol interface {
	// ConnectionMade is called once, after the Transport is ready.
	ConnectionMade(t Transport)
	// DataReceived is called with a slice of the loop's shared read buffer.
	// b is valid ONLY for the duration of the call. Copy what you keep.
	DataReceived(b []byte)
	// ConnectionLost is called exactly once. err is nil on clean close.
	ConnectionLost(err error)
}

// Factory builds a Protocol per accepted or connected transport.
type Factory interface {
	BuildProtocol(peer net.Addr) Protocol
}

// FactoryFunc adapts a function to Factory.
type FactoryFunc func(peer net.Addr) Protocol

// BuildProtocol implements Factory.
func (f FactoryFunc) BuildProtocol(peer net.Addr) Protocol { return f(peer) }

// Transport is the loop-affine handle to a connection. Every method must be
// called from the owning Loop (see design Section 8) except Loop().
type Transport interface {
	// Write copies b to the outbound ring only if an immediate write would
	// block. The caller may reuse b as soon as Write returns.
	Write(b []byte)
	// Writev writes several slices with one syscall when possible.
	Writev(bs ...[]byte)
	// LoseConnection half-closes after flushing pending writes.
	LoseConnection()
	// Abort closes immediately and discards pending writes.
	Abort()
	GetPeer() net.Addr
	GetHost() net.Addr
	Loop() *Loop
	// SetIdleTimeout arms an idle timer on the timing wheel; 0 disables.
	// Any read or write resets it. Expiry aborts with ErrIdleTimeout.
	SetIdleTimeout(d time.Duration)
	// SetContext stores a per-connection user value. Storing a non-pointer
	// value allocates; store pointers on hot paths.
	SetContext(v any)
	Context() any

	// Backpressure (Twisted IPushProducer/IConsumer):

	// PauseReading drops read interest; DataReceived stops being called.
	PauseReading()
	// ResumeReading restores read interest and performs one immediate read
	// so that an edge missed while paused is not lost.
	ResumeReading()
	RegisterProducer(p Producer, streaming bool)
	UnregisterProducer()
	// PendingWrite returns the bytes buffered in the outbound ring.
	PendingWrite() int
}

// Producer is told to pause/resume when the transport's ring crosses its
// high/low watermarks. A non-streaming (pull) producer has ResumeProducing
// called whenever the ring drains and it should produce more.
type Producer interface {
	PauseProducing()
	ResumeProducing()
	StopProducing()
}

// Consumer is implemented by Transport; exposed so protocols can chain
// producers.
type Consumer interface {
	RegisterProducer(p Producer, streaming bool)
	UnregisterProducer()
	Write(b []byte)
}

// DatagramProtocol is the UDP counterpart of Protocol.
type DatagramProtocol interface {
	// StartProtocol is called once with the transport before any datagram.
	StartProtocol(t DatagramTransport)
	// DatagramReceived is called per datagram. b is valid only for the call.
	DatagramReceived(b []byte, from net.Addr)
	// StopProtocol is called once when the listener closes.
	StopProtocol()
}

// DatagramTransport sends datagrams. Outbound datagrams are batched and
// flushed with sendmmsg at the end of the loop iteration (Linux).
type DatagramTransport interface {
	WriteTo(b []byte, to net.Addr)
	GetHost() net.Addr
	Loop() *Loop
	Close()
}

// Listener is a bound TCP or UDP socket. Close stops accepting; existing
// connections are unaffected.
type Listener interface {
	Addr() net.Addr
	Close() error
}
