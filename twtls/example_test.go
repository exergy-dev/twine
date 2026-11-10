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

// Serve TLS on :8443: Server wraps the plaintext factory and Engine.ListenTCP
// binds the port, with the TLS options on Server and the listener options on
// ListenTCP. Server panics on a Config without a certificate, the Config
// crypto/tls.Listen rejects.
func ExampleServer() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}}
	f := twtls.Server(cfg, func(net.Addr) twine.Protocol { return &lineEcho{} },
		twtls.HandshakeTimeout(5*time.Second),
		twtls.OnHandshakeError(func(err error) {
			var he *twtls.HandshakeError
			if errors.As(err, &he) {
				log.Printf("TLS handshake with %v failed: %v", he.Peer, he.Err)
			}
		}))
	if _, err := eng.ListenTCP(":8443", f, twine.Backlog(4096)); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// KernelOffload asks for kTLS where the kernel supports it; connections it
// cannot offload stay on the memory BIO.
func ExampleKernelOffload() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	f := twtls.Server(&tls.Config{Certificates: []tls.Certificate{cert}},
		func(net.Addr) twine.Protocol { return &lineEcho{} },
		twtls.KernelOffload(true))
	if _, err := eng.ListenTCP(":8443", f, twine.TCPNoDelay(true)); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// Dial dials from one of the engine's loops, completes the handshake, and
// resolves with the TLS Transport once the inner protocol's ConnectionMade
// has run. Connect does the same on a loop of your choice.
func ExampleDial() {
	eng := twine.New()
	go func() { _ = eng.Run(context.Background()) }()
	defer eng.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// ServerName defaults to the host part of the address.
	t, err := twtls.Dial(eng, "example.com:443", &tls.Config{},
		func(net.Addr) twine.Protocol { return &lineEcho{} }).Await(ctx)
	if err != nil {
		log.Fatal(err) // a dial error or a *twtls.HandshakeError
	}
	t.Loop().CallFromThread(func() { // Transports are loop-affine
		st := t.(*twtls.Transport).ConnectionState()
		log.Printf("connected with %s", tls.VersionName(st.Version))
		t.Close()
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
	factory := func(net.Addr) twine.Protocol { return &echo{} }
	if _, err := eng.ListenTCP(":8443", twtls.Server(cfg, factory)); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}

// The smallest TLS listener: a plaintext Protocol, unchanged, behind Server.
func ExampleServer_echo() { main() }

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
		t.Close()
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
	f := twtls.Server(cfg, func(net.Addr) twine.Protocol { return &alpnProto{} })
	if _, err := eng.ListenTCP(":8443", f); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}
