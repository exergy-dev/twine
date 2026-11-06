package tls

import (
	ctls "crypto/tls"
	"errors"
	"net"

	"github.com/robert-malhotra/twine"
)

// ErrHandshakeTimeout is the handshake error when HandshakeTimeout expires
// before the handshake completes.
var ErrHandshakeTimeout = errors.New("tls: handshake timeout")

// errNilConfig is returned by Listen when cfg is nil.
var errNilConfig = errors.New("tls: nil *crypto/tls.Config")

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
		inner = nopProtocol{}
	}
	t := &Transport{f: f, inner: inner}
	return (*wrapper)(t)
}

type nopProtocol struct{}

func (nopProtocol) ConnectionMade(twine.Transport) {}
func (nopProtocol) DataReceived([]byte)            {}
func (nopProtocol) ConnectionLost(error)           {}

// Server wraps inner so that every connection it serves speaks TLS as the
// server side, using cfg (which must provide a certificate, for example via
// Certificates or GetCertificate). inner.ConnectionMade is called with a
// *Transport once the handshake has completed; DataReceived then carries
// plaintext. The returned Factory is used with twine.Engine.ListenTCP.
func Server(cfg *ctls.Config, inner twine.Factory, opts ...Option) twine.Factory {
	if cfg == nil {
		cfg = &ctls.Config{} // every handshake fails: no certificate
	}
	return &factory{cfg: cfg, inner: inner, opts: buildOptions(opts)}
}

// Client wraps inner so that every connection it builds speaks TLS as the
// client side, using cfg. Unless cfg.InsecureSkipVerify is set, cfg must name
// the server (ServerName); Connect fills it in from the dialled address. The
// returned Factory is used with twine.Loop.ConnectTCP.
func Client(cfg *ctls.Config, inner twine.Factory, opts ...Option) twine.Factory {
	if cfg == nil {
		cfg = &ctls.Config{}
	}
	return &factory{cfg: cfg, inner: inner, client: true, opts: buildOptions(opts)}
}

// Listen is ListenTCP with a Server factory. Core listener options are passed
// with WithListenOptions.
func Listen(e *twine.Engine, addr string, cfg *ctls.Config, inner twine.Factory, opts ...Option) (twine.Listener, error) {
	if cfg == nil {
		return nil, errNilConfig
	}
	f := &factory{cfg: cfg, inner: inner, opts: buildOptions(opts)}
	return e.ListenTCP(addr, f, f.opts.listenOpts...)
}

// Connect dials addr from loop l and runs a TLS client handshake. The Future
// resolves with the *Transport (as a twine.Transport) after the handshake has
// completed and inner.ConnectionMade has returned; it rejects with the dial
// error, the handshake error, or ErrHandshakeTimeout. When cfg.ServerName is
// empty and verification is enabled, a clone of cfg is used with ServerName
// set to the host part of addr. Core dial options are passed with
// WithDialOptions. Safe to call from any goroutine.
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
	f := &factory{cfg: cfg, inner: inner, client: true, opts: buildOptions(opts), res: res}
	l.ConnectTCP(addr, f, f.opts.dialOpts...).Catch(res.Reject)
	return fut
}
