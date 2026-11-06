//go:build !linux

package tls

// probeKTLS reports false: kTLS is Linux-only.
func probeKTLS() bool { return false }
