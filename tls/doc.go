// Package tls adds TLS to Twine connections by running crypto/tls over an
// in-memory net.Conn (a "memory BIO"), so that encryption fits the
// non-blocking event loop without a goroutine per connection.
//
// # Usage
//
// The package is named tls, like crypto/tls, which programs usually import
// too. The convention is to keep crypto/tls unaliased and import this
// package as twtls:
//
//	import (
//		"crypto/tls"
//
//		twtls "github.com/exergy-dev/twine/tls"
//	)
//
//	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2", "http/1.1"}}
//	lst, err := twtls.Listen(eng, ":8443", cfg, myFactory, twtls.OnHandshakeError(logIt))
//
//	fut := twtls.Connect(eng.NextLoop(), "example.com:443", &tls.Config{}, clientFactory)
//
// Server and Client wrap any twine.Factory. The inner protocol's
// ConnectionMade is called only after the handshake has completed, with a
// *Transport (a twine.Transport); DataReceived carries plaintext and Write
// takes plaintext. Negotiated parameters (version, cipher suite, ALPN, SNI,
// peer certificates) come from t.(*twtls.Transport).ConnectionState().
// LoseConnection sends close_notify before half-closing the socket, so a
// peer sees a clean io.EOF. A failed handshake never reaches the inner
// protocol: the socket is aborted, and the OnHandshakeError hook is called
// with a *HandshakeError (without a hook, the failure is logged).
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
//     below. Engages for TLS 1.3 AES-GCM connections on Linux and falls back
//     to the memory BIO otherwise.
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
// the tls.Conn: that is the per-connection buffer TLS cannot avoid. The
// 16 KiB plaintext buffer that tls.Conn.Read decrypts into is shared by all
// connections of a loop, because plaintext is only lent to the inner
// protocol for the duration of DataReceived; a connection allocates one of
// its own only when it is drained while that buffer is in use (TLS nested
// inside TLS on the same loop). Ciphertext from DataReceived is lent to
// crypto/tls without copying in loop mode; only a remainder left when the
// inner protocol pauses reading mid-chunk is copied. During the handshake,
// ciphertext is copied because the handshake goroutine reads it after
// DataReceived returns.
//
// # Kernel TLS
//
// With KernelOffload(true), after the memory-BIO handshake the record layer
// moves into the kernel: setsockopt(SOL_TCP, TCP_ULP, "tls") followed by
// SOL_TLS TLS_TX (and TLS_RX). The loop then does plain read/write, the
// kernel encrypts and decrypts, and Transport.Write goes straight to the
// socket. KTLSAvailable probes the ULP once on a throwaway loopback socket;
// Transport.KernelOffload reports what a connection got (tx, rx).
//
// Key extraction. crypto/tls exports no record keys, so the package reads the
// TLS 1.3 traffic secrets and sequence numbers of both directions out of the
// unexported fields of *crypto/tls.Conn (in/out halfConn: trafficSecret, seq,
// cipher; Conn.vers, cipherSuite, and the rawInput/input/hand buffers) with
// reflect and unsafe, then derives key and IV with HKDF-Expand-Label (RFC 8446
// section 7.1) exactly as crypto/tls does. This is fragile by nature: it
// depends on Go internals (checked against Go 1.25). Every field is looked up
// by name and checked for kind and length at run time; any mismatch, a
// missing field or a panic simply means "no offload" and the connection
// continues on the memory BIO. No Go version can make offload misbehave
// silently except by changing the meaning of those fields while keeping
// their names, types and sizes.
//
// Engagement rules (anything else falls back to the memory BIO):
//
//   - Linux, KTLSAvailable, TLS 1.3 negotiated, cipher suite
//     TLS_AES_128_GCM_SHA256 or TLS_AES_256_GCM_SHA384 (AES-GCM only:
//     TLS_CHACHA20_POLY1305_SHA256 always stays on the memory BIO), and the
//     kernel accepted TX and RX keys for that cipher on a one-time probe
//     (dummy keys, loopback socket).
//   - No ciphertext queued for sending (core ring, handshake output).
//   - Server: TX and RX are offloaded provided crypto/tls has consumed no
//     application data and nothing from the peer is queued (neither inside
//     crypto/tls nor in the bio). This is the normal case: it is decided
//     when the handshake Future settles, before the first plaintext
//     DataReceived. Go's server sends its NewSessionTicket records inside
//     Handshake, so its outbound side is clean. If the client pipelined data
//     right behind its Finished, only TX is offloaded and RX continues on
//     the memory BIO (still correct).
//   - Client: TX only. After the handshake the server's NewSessionTicket
//     records arrive encrypted, and the kernel delivers non-data records only
//     through recvmsg control messages, which the core does not use. RX
//     therefore stays on the memory BIO.
//
// Once TX is installed crypto/tls must never write again (the kernel would
// encrypt its already-encrypted records): the bio refuses writes. The one
// place crypto/tls would write on the read path is the reply to a peer
// KeyUpdate that requests one; that fails and aborts the connection.
//
// Close. LoseConnection sends close_notify with sendmsg and a
// TLS_SET_RECORD_TYPE control message (alert record, {warning,
// close_notify}), then half-closes. The core exposes no ordered "control
// record" write, so if application bytes are still buffered in the core's
// write ring the alert waits (short poll) until the ring has drained, so
// that it cannot overtake data.
//
// Residual risks. TX keys cannot be uninstalled: if TX succeeds and RX then
// fails, the connection continues TX-offloaded with RX on the memory BIO.
// The per-cipher probe makes that effectively impossible in practice. With
// RX offloaded, a non-data record from the peer (close_notify, another
// alert, KeyUpdate) makes the kernel fail read with EIO, so the inner
// protocol sees ConnectionLost with an error wrapping syscall.EIO (a peer
// that closes cleanly with close_notify is thus indistinguishable from a
// fatal record). KeyUpdate is not supported under RX offload.
//
// # Limitations
//
// Engine shutdown loses the underlying connections directly, so no
// close_notify is sent for connections still open at Stop; call
// LoseConnection on them first (for example from an Engine.OnShutdown hook
// that hops to each loop) when a clean TLS close matters. Writev encrypts
// each slice into its own record(s).
package tls
