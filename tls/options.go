package tls

import (
	"time"

	"github.com/exergy-dev/twine"
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

// KernelOffload requests kTLS offload after the handshake (Linux 5.x+ with
// the tls module; TLS 1.3 with AES-128-GCM or AES-256-GCM only:
// ChaCha20-Poly1305 and TLS 1.2 always stay on the memory BIO). When any
// precondition fails the connection silently stays on the memory BIO. A
// server offloads both directions, a client transmit only.
// Transport.KernelOffload reports the mode in use; see the package
// documentation, "Kernel TLS", for the exact rules and limitations.
func KernelOffload(on bool) Option { return func(o *options) { o.kernelOffload = on } }

// OnHandshakeError registers fn to be called, on the connection's loop, when
// a handshake fails (including timeouts and connections lost mid-handshake).
// err is a *HandshakeError carrying the peer address. The inner protocol
// never sees such a connection: its ConnectionMade and ConnectionLost are
// not called, and the underlying transport is aborted. Without a hook each
// failure is logged once at Warn level through the engine's logger, with the
// peer address.
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
