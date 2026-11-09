package tls

import (
	ctls "crypto/tls"
	"errors"
	"net"

	"github.com/exergy-dev/twine"
)

// ErrHandshakeTimeout is the handshake error when HandshakeTimeout expires
// before the handshake completes.
var ErrHandshakeTimeout = errors.New("tls: handshake timeout")

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
	return &factory{cfg: cfg, inner: inner, opts: buildOptions(opts)}
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
	return &factory{cfg: cfg, inner: inner, client: true, opts: buildOptions(opts)}
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
// error, the handshake error, or ErrHandshakeTimeout. A nil cfg means a zero
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
