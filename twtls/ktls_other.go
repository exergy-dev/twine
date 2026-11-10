//go:build !linux

package twtls

import "errors"

var errNoKTLS = errors.New("tls: kTLS not available")

// probeKTLS reports false: kTLS is Linux-only.
func probeKTLS() bool { return false }

// warmKTLS has nothing to probe off Linux.
func warmKTLS() {}

// installKTLS never offloads off Linux.
func installKTLS(*Transport) bool { return false }

// sendCloseNotify is unreachable off Linux (no offload is ever installed).
func sendCloseNotify(int) error { return errNoKTLS }
