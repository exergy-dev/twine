package tls

import (
	"time"

	"github.com/robert-malhotra/twine"
)

// DefaultHandshakeTimeout bounds a handshake when HandshakeTimeout is not
// given. A handshake occupies a worker goroutine while it waits for the
// peer, so an unbounded handshake would let idle peers exhaust the pool.
const DefaultHandshakeTimeout = 10 * time.Second

// Option configures Server, Client, Listen and Connect.
type Option func(*options)

type options struct {
	kernelOffload    bool
	onHandshakeError func(err error)
	handshakeTimeout time.Duration
	listenOpts       []twine.ListenOption
	dialOpts         []twine.DialOption
}

func buildOptions(opts []Option) options {
	o := options{handshakeTimeout: DefaultHandshakeTimeout}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// KernelOffload requests kTLS offload after the handshake. It is accepted for
// forward compatibility but currently always falls back, silently, to the
// memory BIO: Go's crypto/tls does not expose the record-layer traffic keys
// that setsockopt(SOL_TLS) needs (see the package documentation,
// KTLSSupported and KTLSAvailable). Transport.KernelOffloaded reports the
// mode actually in use.
func KernelOffload(on bool) Option { return func(o *options) { o.kernelOffload = on } }

// OnHandshakeError registers fn to be called, on the connection's loop, when
// a handshake fails (including timeouts and connections lost mid-handshake).
// The inner protocol never sees such a connection: its ConnectionMade and
// ConnectionLost are not called, and the underlying transport is aborted.
func OnHandshakeError(fn func(err error)) Option {
	return func(o *options) { o.onHandshakeError = fn }
}

// HandshakeTimeout bounds the handshake; on expiry the connection is aborted
// and the handshake fails with ErrHandshakeTimeout. Default
// DefaultHandshakeTimeout; 0 disables the timeout.
func HandshakeTimeout(d time.Duration) Option {
	return func(o *options) { o.handshakeTimeout = d }
}

// WithListenOptions passes core listener options through Listen to
// twine.Engine.ListenTCP. It is ignored by Server, Client and Connect.
func WithListenOptions(lo ...twine.ListenOption) Option {
	return func(o *options) { o.listenOpts = append(o.listenOpts, lo...) }
}

// WithDialOptions passes core dial options through Connect to
// twine.Loop.ConnectTCP. It is ignored by Server, Client and Listen.
func WithDialOptions(do ...twine.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, do...) }
}
