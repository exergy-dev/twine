package tls

import (
	ctls "crypto/tls"
	"errors"
	"net"
	"strings"

	"github.com/exergy-dev/twine"
)

// ErrHandshakeTimeout is the handshake error when HandshakeTimeout expires
// before the handshake completes.
var ErrHandshakeTimeout = errors.New("tls: handshake timeout")

// HandshakeError is the error passed to the OnHandshakeError hook, and the
// rejection of a Connect Future, when a handshake fails: a certificate that
// does not verify, no shared version or cipher suite, an alert from the
// peer, a connection lost mid-handshake, or HandshakeTimeout expiring (Err
// is then ErrHandshakeTimeout). errors.Is and errors.As see through it to
// Err.
type HandshakeError struct {
	// Peer is the remote address of the connection.
	Peer net.Addr
	// Err is the cause: crypto/tls's handshake error, ErrHandshakeTimeout,
	// io.EOF or the connection's error when it was lost mid-handshake.
	Err error
}

// Error implements error.
func (e *HandshakeError) Error() string {
	// crypto/tls errors and ErrHandshakeTimeout carry their own "tls: ".
	cause := strings.TrimPrefix(e.Err.Error(), "tls: ")
	if e.Peer == nil {
		return "tls: handshake failed: " + cause
	}
	return "tls: handshake with " + e.Peer.String() + " failed: " + cause
}

// Unwrap returns Err.
func (e *HandshakeError) Unwrap() error { return e.Err }

// errNoCertificate is crypto/tls.Listen's error for a Config that cannot
// serve a handshake.
var errNoCertificate = errors.New("tls: neither Certificates, GetCertificate, nor GetConfigForClient set in Config")

// canServe reports whether cfg can complete a server handshake, by the same
// test as crypto/tls.Listen.
func canServe(cfg *ctls.Config) bool {
	return cfg != nil && (len(cfg.Certificates) > 0 || cfg.GetCertificate != nil || cfg.GetConfigForClient != nil)
}

// factory wraps an inner twine.Factory with TLS.
type factory struct {
	cfg    *ctls.Config
	inner  twine.Factory
	client bool
	opts   options
	// res, when set, is resolved with the first connection's *Transport
	// (Connect).
	res twine.Resolver[twine.Transport]
}

// BuildProtocol implements twine.Factory.
func (f *factory) BuildProtocol(peer net.Addr) twine.Protocol {
	inner := f.inner.BuildProtocol(peer)
	if inner == nil {
		// Refuse the connection, as the core does for a plain listener: the
		// loop closes the socket without a handshake.
		return nil
	}
	t := &Transport{f: f, inner: inner, peer: peer}
	return (*wrapper)(t)
}

// newFactory builds a factory. With KernelOffload(true) it also runs the
// kTLS capability probes, once per process, on the calling goroutine rather
// than on a loop at the first handshake.
func newFactory(cfg *ctls.Config, inner twine.Factory, client bool, opts []Option) *factory {
	f := &factory{cfg: cfg, inner: inner, client: client, opts: buildOptions(opts)}
	if f.opts.kernelOffload {
		warmKTLS()
	}
	return f
}

// Server wraps inner so that every connection it serves speaks TLS as the
// server side, using cfg. cfg must be able to serve a handshake: it must set
// Certificates, GetCertificate or GetConfigForClient, and Server panics
// otherwise (Listen returns that condition as an error instead).
// inner.ConnectionMade is called with a *Transport once the handshake has
// completed; DataReceived then carries plaintext. The returned Factory is
// used with twine.Engine.ListenTCP.
func Server(cfg *ctls.Config, inner twine.Factory, opts ...Option) twine.Factory {
	if !canServe(cfg) {
		panic(errNoCertificate)
	}
	return newFactory(cfg, inner, false, opts)
}

// Client wraps inner so that every connection it builds speaks TLS as the
// client side, using cfg (nil means a zero Config, as with crypto/tls.Dial).
// Unless cfg.InsecureSkipVerify is set, cfg must name the server in
// ServerName; Client does not fill it in.
//
// Most callers want Connect instead: its Future resolves with the TLS
// *Transport once the handshake has completed. Client is for dialers that
// take a twine.Factory, such as twine.Loop.ConnectTCP, twine.Engine.Dial or
// proto.ReconnectingClient. What those dialers hand back is the raw socket
// transport, resolved as soon as TCP connects and before the handshake: do
// not write to it (that would put plaintext on the wire mid-handshake). The
// TLS Transport is the one passed to inner's ConnectionMade, after the
// handshake; keep and use that one.
func Client(cfg *ctls.Config, inner twine.Factory, opts ...Option) twine.Factory {
	if cfg == nil {
		cfg = &ctls.Config{}
	}
	return newFactory(cfg, inner, true, opts)
}

// Listen is ListenTCP with a Server factory. Core listener options are passed
// with WithListenOptions. Like crypto/tls.Listen it returns an error when cfg
// is nil or sets none of Certificates, GetCertificate and GetConfigForClient.
func Listen(e *twine.Engine, addr string, cfg *ctls.Config, inner twine.Factory, opts ...Option) (twine.Listener, error) {
	if !canServe(cfg) {
		return nil, errNoCertificate
	}
	f := Server(cfg, inner, opts...).(*factory)
	return e.ListenTCP(addr, f, f.opts.listenOpts...)
}

// Connect dials addr from loop l and runs a TLS client handshake. The Future
// resolves with the *Transport (as a twine.Transport) after the handshake has
// completed and inner.ConnectionMade has returned; it rejects with the dial
// error or a *HandshakeError (whose Err is ErrHandshakeTimeout when
// HandshakeTimeout expired). A nil cfg means a zero
// Config. When cfg.ServerName is empty and verification is enabled, a clone
// of cfg is used with ServerName set to the host part of addr. Core dial
// options are passed with WithDialOptions. Safe to call from any goroutine.
func Connect(l *twine.Loop, addr string, cfg *ctls.Config, inner twine.Factory, opts ...Option) *twine.Future[twine.Transport] {
	if cfg == nil {
		cfg = &ctls.Config{}
	}
	if cfg.ServerName == "" && !cfg.InsecureSkipVerify {
		if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
	}
	fut, res := twine.NewFuture[twine.Transport](l)
	f := Client(cfg, inner, opts...).(*factory)
	f.res = res
	l.ConnectTCP(addr, f, f.opts.dialOpts...).Catch(res.Reject)
	return fut
}
