// Package tls adds TLS to Twine connections by running crypto/tls over an
// in-memory net.Conn (a "memory BIO"), so that encryption fits the
// non-blocking event loop without a goroutine per connection.
//
// # Usage
//
//	cfg := &ctls.Config{Certificates: []ctls.Certificate{cert}, NextProtos: []string{"h2", "http/1.1"}}
//	lst, err := tls.Listen(eng, ":8443", cfg, myFactory, tls.OnHandshakeError(logIt))
//
//	fut := tls.Connect(eng.NextLoop(), "example.com:443", &ctls.Config{}, clientFactory)
//
// Server and Client wrap any twine.Factory. The inner protocol's
// ConnectionMade is called only after the handshake has completed, with a
// *Transport (a twine.Transport); DataReceived carries plaintext and Write
// takes plaintext. Negotiated parameters (version, cipher suite, ALPN, SNI,
// peer certificates) come from t.(*tls.Transport).ConnectionState().
// LoseConnection sends close_notify before half-closing the socket, so a
// peer sees a clean io.EOF. A failed handshake never reaches the inner
// protocol: the socket is aborted and the OnHandshakeError hook is called.
//
// # Modes (design Section 10)
//
//   - Memory BIO (default, the only mode implemented). tls.Server/tls.Client
//     run over a bio whose Read takes from an inbound ciphertext queue and
//     whose Write produces outbound ciphertext. The handshake, which is where
//     crypto/tls allocates, runs off the loop on a worker goroutine whose
//     result is a twine.Future settled on the loop (the DeferToThread
//     pattern; see "Worker goroutines" below): in this "handshake mode"
//     bio.Read blocks on a
//     sync.Cond until the loop feeds ciphertext from DataReceived (or the
//     connection is lost), and bio.Write queues bytes and asks the loop,
//     through CallFromThread, to ship them. When the handshake Future
//     resolves on the loop the bio switches to "loop mode": Read never blocks
//     and returns an internal would-block net.Error (Timeout and Temporary
//     both true, so crypto/tls keeps the connection usable) when the queue is
//     empty, and Write hands each record directly to the underlying
//     transport, which copies only if the socket would block. Record
//     decryption and encryption happen on the loop.
//   - kTLS offload. Requested with KernelOffload(true); see "Kernel TLS"
//     below. It currently always falls back to the memory BIO.
//   - Goroutine adapter. Not part of this package: wrap a factory with
//     twine.Blocking and run crypto/tls over a real net.Conn. That is the
//     migration path, at goroutine-per-connection cost.
//
// # Concurrency rule
//
// A *crypto/tls.Conn is never touched from two goroutines. During the
// handshake only the worker goroutine running Handshake touches it; the loop
// touches only the bio's queues, under the bio mutex. The worker returns
// before the handshake Future settles and settlement is routed through
// CallFromThread, which orders everything the worker did before every loop
// callback that follows. From then on only the loop touches the tls.Conn,
// and the bio's loop-mode data needs no locking (bio.Read still takes the
// then uncontended mutex to check the mode). Transport, like every
// twine.Transport, is loop-affine.
//
// # Worker goroutines
//
// A handshake blocks for whole network round trips while it waits for the
// peer, so each one runs on its own short-lived goroutine rather than on the
// engine's bounded DeferToThread pool: on the pool, as many silent peers as
// there are workers would hold every worker for HandshakeTimeout and stall
// all other handshakes and DeferToThread users. The goroutine exists only for
// the handshake; the steady state has none. The HandshakeTimeout option
// (default DefaultHandshakeTimeout) bounds how long one can be held.
//
// # Memory
//
// crypto/tls must see a whole record before it returns plaintext, so a
// partially received record (up to 16 KiB plus overhead) is buffered inside
// the tls.Conn; this, and the 16 KiB plaintext buffer that tls.Conn.Read
// decrypts into (allocated when the handshake completes), are the per
// connection buffers TLS cannot avoid. Ciphertext from DataReceived is lent
// to crypto/tls without copying in loop mode; only a remainder left when the
// inner protocol pauses reading mid-chunk is copied. During the handshake,
// ciphertext is copied because the worker reads it after DataReceived
// returns.
//
// # Kernel TLS
//
// kTLS lets the kernel encrypt and decrypt records after a user-space
// handshake, so the loop could use plain read/write. KTLSAvailable probes the
// kernel once (setsockopt(SOL_TCP, TCP_ULP, "tls") on a throwaway loopback
// socket; always false off Linux). Installing the offload additionally needs
// the record-layer traffic keys, IVs and sequence numbers for
// setsockopt(SOL_TLS, TLS_TX/TLS_RX). Go's crypto/tls (verified against Go
// 1.25) keeps those in unexported fields and exports no API for them;
// ExportKeyingMaterial yields RFC 5705 exporter secrets, not record keys.
// This package refuses to reach into crypto/tls with unsafe or go:linkname,
// so the data-path offload cannot be completed: KTLSSupported is false,
// KernelOffload(true) silently uses the memory BIO, and
// Transport.KernelOffloaded reports false. Should crypto/tls gain a key
// export hook, only installKTLS needs to change.
//
// # Limitations
//
// Engine shutdown loses the underlying connections directly, so no
// close_notify is sent for connections still open at Stop; call
// LoseConnection on them first (for example from an Engine.OnShutdown hook
// that hops to each loop) when a clean TLS close matters. Writev encrypts
// each slice into its own record(s).
package tls
