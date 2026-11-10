package twtls_test

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/twtls"
)

// lineEcho is a plaintext protocol: it writes back what it receives. Under
// TLS it is unchanged; the TLS Transport encrypts and decrypts around it.
type lineEcho struct{ t twine.Transport }

func (p *lineEcho) ConnectionMade(t twine.Transport) { p.t = t }
func (p *lineEcho) DataReceived(b []byte)            { p.t.Write(b) }
func (p *lineEcho) ConnectionLost(error)             {}

// Serve TLS on :8443. Listen rejects a Config without a certificate, like
// crypto/tls.Listen. Core listener options go through WithListenOptions.
func ExampleListen() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}}
	_, err = twtls.Listen(eng, ":8443", cfg,
		twine.FactoryFunc(func(net.Addr) twine.Protocol { return &lineEcho{} }),
		twtls.WithListenOptions(twine.Backlog(4096)),
		twtls.HandshakeTimeout(5*time.Second),
		twtls.OnHandshakeError(func(err error) {
			var he *twtls.HandshakeError
			if errors.As(err, &he) {
				log.Printf("TLS handshake with %v failed: %v", he.Peer, he.Err)
			}
		}))
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// Server wraps a factory for use with Engine.ListenTCP directly; core
// listener options are then passed to ListenTCP itself.
func ExampleServer() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	f := twtls.Server(&tls.Config{Certificates: []tls.Certificate{cert}},
		twine.FactoryFunc(func(net.Addr) twine.Protocol { return &lineEcho{} }),
		twtls.KernelOffload(true)) // kTLS where the kernel supports it
	if _, err := eng.ListenTCP(":8443", f, twine.TCPNoDelay(true), twine.Backlog(4096)); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// Connect dials, completes the handshake, and resolves with the TLS
// Transport once the inner protocol's ConnectionMade has run.
func ExampleConnect() {
	eng := twine.New()
	go func() { _ = eng.Run(context.Background()) }()
	defer eng.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// ServerName defaults to the host part of the address.
	t, err := twtls.Connect(eng.NextLoop(), "example.com:443", &tls.Config{},
		twine.FactoryFunc(func(net.Addr) twine.Protocol { return &lineEcho{} })).Await(ctx)
	if err != nil {
		log.Fatal(err) // a dial error or a *twtls.HandshakeError
	}
	t.Loop().CallFromThread(func() { // Transports are loop-affine
		st := t.(*twtls.Transport).ConnectionState()
		log.Printf("connected with %s", tls.VersionName(st.Version))
		t.LoseConnection()
	})
}

// The README's TLS snippet: the plaintext echo Protocol (echo, in
// tls_test.go) served over TLS on :8443. It runs until killed, so the Example
// that compiles it has no Output and is not run.
func main() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	factory := twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} })
	if _, err := twtls.Listen(eng, ":8443", cfg, factory); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// The smallest TLS listener: a plaintext Protocol, unchanged, behind Listen.
func ExampleListen_echo() { main() }

// alpnProto inspects the negotiated parameters when the handshake is done.
type alpnProto struct{ lineEcho }

func (p *alpnProto) ConnectionMade(t twine.Transport) {
	p.t = t
	tt := t.(*twtls.Transport)
	st := tt.ConnectionState()
	tx, rx := tt.KernelOffload()
	log.Printf("%s %s ALPN=%q SNI=%q kTLS tx=%v rx=%v",
		tls.VersionName(st.Version), tls.CipherSuiteName(st.CipherSuite),
		st.NegotiatedProtocol, st.ServerName, tx, rx)
	if st.NegotiatedProtocol != "twine/1" {
		t.LoseConnection()
	}
}

// The inner protocol reaches the TLS parameters (version, cipher suite,
// ALPN, SNI, peer certificates) through a type assertion on the Transport
// passed to ConnectionMade.
func ExampleTransport_ConnectionState() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"twine/1"}}
	if _, err := twtls.Listen(eng, ":8443", cfg,
		twine.FactoryFunc(func(net.Addr) twine.Protocol { return &alpnProto{} })); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}
