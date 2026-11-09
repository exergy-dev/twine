package tls

import (
	"errors"
	"sync"
	"syscall"
)

var (
	ktlsOnce  sync.Once
	ktlsAvail bool
)

// KTLSAvailable reports whether the running kernel accepts the "tls" upper
// layer protocol (setsockopt(SOL_TCP, TCP_ULP, "tls")), probed once on a
// throwaway socket and cached. It is always false on non-Linux systems. A
// true result does not by itself mean a connection will be offloaded: see
// KTLSSupported and the package documentation.
func KTLSAvailable() bool {
	ktlsOnce.Do(func() { ktlsAvail = probeKTLS() })
	return ktlsAvail
}

var (
	// errOffloadWrite is returned by the bio once record protection moved to
	// the kernel: crypto/tls must not write (it would double-encrypt), so
	// anything it tries to send, such as the reply to a peer KeyUpdate that
	// requests one, fails and aborts the connection.
	errOffloadWrite = errors.New("tls: kTLS offload does not support crypto/tls writes (peer KeyUpdate?)")
)

// isRetry reports whether a non-blocking send failed only because the socket
// buffer is full or the call was interrupted.
func isRetry(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR)
}
