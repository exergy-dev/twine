//go:build !linux

package tls

import "errors"

var errNoKTLS = errors.New("tls: kTLS not available")

// KTLSSupported reports whether this package can install kernel TLS offload.
// It is false off Linux.
func KTLSSupported() bool { return false }

// probeKTLS reports false: kTLS is Linux-only.
func probeKTLS() bool { return false }

// installKTLS never offloads off Linux.
func installKTLS(*Transport) bool { return false }

// sendCloseNotify is unreachable off Linux (no offload is ever installed).
func sendCloseNotify(int) error { return errNoKTLS }
