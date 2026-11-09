package tls

// RXOffloaded reports whether kTLS RX was installed (test hook).
func (t *Transport) RXOffloaded() bool { return t.rxOffloaded }

// FeedCiphertext hands b to the connection as if the core had read it from
// the socket (test hook for the record-path benchmarks). Loop-only.
func (t *Transport) FeedCiphertext(b []byte) { (*wrapper)(t).DataReceived(b) }

// ErrOffloadWrite is errOffloadWrite (test hook).
var ErrOffloadWrite = errOffloadWrite

// ForceRefusedWrite puts the connection in the state a peer KeyUpdate leaves
// it in under kTLS TX offload: offloaded, with crypto/tls having attempted
// a write that the bio refused (test hook). Loop-only.
func (t *Transport) ForceRefusedWrite() {
	t.offloaded = true
	t.bio.mu.Lock()
	t.bio.noWrite = true
	t.bio.mu.Unlock()
	if _, err := t.bio.Write([]byte{0}); err != errOffloadWrite {
		panic("bio accepted a write after noWrite")
	}
}
