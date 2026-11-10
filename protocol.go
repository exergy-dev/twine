package twine

import (
	"net"
	"time"
)

// Protocol is implemented by user code, one value per connection. All
// methods are called on the owning Loop, never concurrently.
type Protocol interface {
	// ConnectionMade is called once, after the Transport is ready and
	// before any data.
	ConnectionMade(t Transport)
	// DataReceived is called with a slice of the loop's shared read buffer.
	// b is valid ONLY for the duration of the call. Copy what you keep.
	DataReceived(b []byte)
	// ConnectionLost is called exactly once. err is nil on a clean close
	// (including Abort and Close), otherwise it says why.
	ConnectionLost(err error)
}

// Factory builds the Protocol for a new connection from (or to) peer, one
// per accepted or connected transport. It runs on the loop that will own the
// connection. Returning nil refuses the connection, as in Twisted: an
// accepted socket is closed at once with no callbacks, and a ConnectTCP or
// Dial Future rejects with ErrNilProtocol.
type Factory func(peer net.Addr) Protocol

// Transport is the loop-affine handle to a connection. Every method must be
// called on the owning Loop, except Loop, which is safe from any goroutine;
// debug builds (TWINE_DEBUG=1) panic with ErrNotInLoop otherwise. Another
// goroutine reaches a connection through Loop().CallFromThread.
//
// A Transport handle stays safe to hold after ConnectionLost: the core
// recycles connection state, and a stale handle becomes inert (writes are
// counted in MetricsSnapshot.WriteAfterClose) instead of touching the next
// connection.
//
// A Transport carries no user data. Per-connection state belongs in the
// Protocol struct, which exists once per connection and whose methods are
// the only code that runs for it.
//
// The Transport the core hands to ConnectionMade also has an FD() int
// method, reachable by a type assertion to interface{ FD() int }. It returns
// the socket descriptor (or -1 once closed) for kernel offload setup such as
// kTLS setsockopt calls, and must not be used for I/O: reading or writing
// the descriptor behind the loop's back corrupts its buffering and
// readiness state.
type Transport interface {
	Consumer
	// Writev writes the slices in order, with a single writev(2) when
	// nothing is buffered. Like Write, it never retains the slices.
	Writev(bs ...[]byte)
	// Close closes the connection gracefully: unlike net.Conn.Close it
	// first flushes pending writes, then half-closes (SHUT_WR) and closes
	// once the peer closes its side or CloseLinger elapses;
	// ConnectionLost(nil) follows. Later writes are dropped. Abort closes
	// at once instead.
	Close()
	// Abort closes immediately and discards pending writes;
	// ConnectionLost(nil) follows at the end of the loop iteration.
	Abort()
	// RemoteAddr returns the remote address (a *net.TCPAddr for TCP).
	RemoteAddr() net.Addr
	// LocalAddr returns the local address (a *net.TCPAddr for TCP).
	LocalAddr() net.Addr
	// Loop returns the owning Loop. Safe from any goroutine.
	Loop() *Loop
	// SetIdleTimeout arms an idle timer on the timing wheel; 0 disables.
	// Any read or write resets it. Expiry aborts with ErrIdleTimeout.
	SetIdleTimeout(d time.Duration)
	// PauseReading drops read interest; DataReceived stops being called
	// until ResumeReading (Twisted's pauseProducing on the transport).
	PauseReading()
	// ResumeReading restores read interest and performs one immediate read
	// so that an edge missed while paused is not lost.
	ResumeReading()
	// PendingWrite returns the bytes buffered in the outbound ring, that is,
	// accepted by Write but not yet taken by the kernel.
	PendingWrite() int
}

// Producer is a source of data written to a Consumer (Twisted's
// IPushProducer / IPullProducer). Its methods are called on the loop.
type Producer interface {
	// PauseProducing asks a streaming producer to stop writing: the
	// consumer's pending writes passed WriteHighWater.
	PauseProducing()
	// ResumeProducing asks the producer to write more: a paused streaming
	// producer when pending writes fall to WriteLowWater, a pull producer
	// each time the outbound buffer drains.
	ResumeProducing()
	// StopProducing is called once when the connection closes, before
	// ConnectionLost; the producer must not write again.
	StopProducing()
}

// Consumer is the write side of a connection that a Producer feeds
// (Twisted's IConsumer). Transport embeds it, so every Transport is a
// Consumer; wrappers such as the twtls package's Transport implement it too.
type Consumer interface {
	// RegisterProducer attaches p. With streaming true (a push producer) p
	// writes on its own and is paused and resumed around the WriteHighWater
	// and WriteLowWater marks. With streaming false (a pull producer) p
	// writes only when asked: ResumeProducing is called now if nothing is
	// buffered, and again each time the outbound buffer drains. Registering
	// a second producer panics; on a closed connection p.StopProducing is
	// called at once.
	RegisterProducer(p Producer, streaming bool)
	// UnregisterProducer detaches the producer without stopping it.
	UnregisterProducer()
	// Write sends b, copying it into the outbound ring only if the kernel
	// does not take it all immediately. It never retains b: the caller may
	// reuse b as soon as Write returns.
	Write(b []byte)
}

// DatagramProtocol is the UDP counterpart of Protocol. Its methods are
// called on the owning Loop.
type DatagramProtocol interface {
	// StartProtocol is called once with the transport before any datagram.
	StartProtocol(t DatagramTransport)
	// DatagramReceived is called per datagram. Both b and from are valid
	// only for the call: from is a *net.UDPAddr the loop reuses for the next
	// datagram, so keep a copy such as from.(*net.UDPAddr).AddrPort(), not
	// the pointer (debug builds empty it after each call). Passing from
	// straight to WriteTo inside the call is fine.
	DatagramReceived(b []byte, from net.Addr)
	// StopProtocol is called once when the socket closes.
	StopProtocol()
}

// DatagramTransport sends datagrams. Its methods except Loop are
// loop-affine.
type DatagramTransport interface {
	// WriteTo queues b (copied) for the peer to, which must be a
	// *net.UDPAddr; any other address type drops the datagram. Datagrams up
	// to 8 KiB are batched and sent at the end of the loop iteration
	// (sendmmsg on Linux), larger ones at once. UDP is best effort: a
	// datagram the kernel refuses is dropped.
	WriteTo(b []byte, to net.Addr)
	// LocalAddr returns the bound address.
	LocalAddr() net.Addr
	// Loop returns the owning Loop. Safe from any goroutine.
	Loop() *Loop
	// Close flushes queued datagrams, closes the socket and calls
	// StopProtocol.
	Close()
}

// Listener is a bound TCP or UDP socket.
type Listener interface {
	// Addr returns the bound address, with port 0 resolved.
	Addr() net.Addr
	// Close stops accepting; existing connections are unaffected. It is
	// safe from any goroutine, always returns nil, and completes
	// asynchronously on each loop, so a connection the loop is accepting at
	// that moment may still be delivered. For UDP it closes the socket and
	// calls StopProtocol.
	Close() error
}
