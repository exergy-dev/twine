package tls

// RXOffloaded reports whether kTLS RX was installed (test hook).
func (t *Transport) RXOffloaded() bool { return t.rxOffloaded }

// CipherUsable reports whether the kernel accepted kTLS keys for the TLS 1.3
// suite on a probe socket (test hook).
func CipherUsable(suite uint16) bool {
	k, _, _, ok := kindFor(suite)
	return ok && cipherUsable(k)
}

// FeedCiphertext hands b to the connection as if the core had read it from
// the socket (test hook for the record-path benchmarks). Loop-only.
func (t *Transport) FeedCiphertext(b []byte) { (*wrapper)(t).DataReceived(b) }
