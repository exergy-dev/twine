package twtls

import "testing"

// TestBioRecordsRefusedWrite: once kTLS TX is installed the bio refuses
// crypto/tls writes and remembers that it did, because crypto/tls swallows
// the error on its KeyUpdate reply path.
func TestBioRecordsRefusedWrite(t *testing.T) {
	b := &bio{}
	b.cond.L = &b.mu
	b.switchToLoopMode()
	b.noWrite = true
	if b.refused {
		t.Fatal("refused before any write")
	}
	if n, err := b.Write([]byte("keyupdate")); n != 0 || err != errOffloadWrite {
		t.Fatalf("Write = %d, %v; want 0, errOffloadWrite", n, err)
	}
	if !b.refused {
		t.Fatal("refused not set after a refused write")
	}
}
