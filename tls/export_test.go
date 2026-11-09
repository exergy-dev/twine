package tls

// RXOffloaded reports whether kTLS RX was installed (test hook).
func (t *Transport) RXOffloaded() bool { return t.rxOffloaded }

// FeedCiphertext hands b to the connection as if the core had read it from
// the socket (test hook for the record-path benchmarks). Loop-only.
func (t *Transport) FeedCiphertext(b []byte) { (*wrapper)(t).DataReceived(b) }
