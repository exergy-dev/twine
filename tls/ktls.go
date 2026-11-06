package tls

import "sync"

// KTLSSupported reports whether this package can install kernel TLS (kTLS)
// offload on a connection. It is always false.
//
// Installing kTLS requires the record-layer traffic secrets (key, IV and
// sequence number for each direction) to be passed to
// setsockopt(SOL_TLS, TLS_TX/TLS_RX). As of Go 1.25, crypto/tls keeps them in
// unexported fields of tls.Conn and offers no API to export them
// (ConnectionState.ExportKeyingMaterial derives RFC 5705 exporter secrets,
// which are not the record keys). Reaching them with unsafe or go:linkname
// would break on any crypto/tls change, so this package does not. The
// KernelOffload option is accepted and silently falls back to the memory
// BIO.
func KTLSSupported() bool { return false }

var (
	ktlsOnce  sync.Once
	ktlsAvail bool
)

// KTLSAvailable reports whether the running kernel accepts the "tls" upper
// layer protocol (setsockopt(SOL_TCP, TCP_ULP, "tls")), probed once on a
// throwaway socket and cached. It is always false on non-Linux systems. A
// true result does not mean offload is used: see KTLSSupported.
func KTLSAvailable() bool {
	ktlsOnce.Do(func() { ktlsAvail = probeKTLS() })
	return ktlsAvail
}

// installKTLS would move record encryption into the kernel after the
// handshake. Because the traffic secrets are not reachable (KTLSSupported),
// it never probes the kernel and always reports false, leaving the memory
// BIO in use.
func installKTLS(*Transport) bool { return KTLSSupported() }
