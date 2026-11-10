package twtls

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

// SetScratchBusy marks the loop's shared plaintext buffer as in use, as a
// drain further up the stack would (test hook). Loop-only.
func (t *Transport) SetScratchBusy(busy bool) { t.scratch.busy = busy }

// ScratchBusy reports whether the loop's shared plaintext buffer is marked
// in use (test hook). Loop-only.
func (t *Transport) ScratchBusy() bool { return t.scratch.busy }

// OwnPlainBuffer reports whether the connection allocated a plaintext buffer
// of its own (test hook). Loop-only.
func (t *Transport) OwnPlainBuffer() bool { return t.plain != nil }
