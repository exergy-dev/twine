package tunpkt

import (
	"bytes"
	"testing"
)

func TestParseKey(t *testing.T) {
	k, err := ParseKey(DefaultKeyHex())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k, DefaultKey()) {
		t.Fatalf("ParseKey(DefaultKeyHex()) = %x", k)
	}
	if DefaultKeyHex() != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("DefaultKeyHex = %s", DefaultKeyHex())
	}
	for _, bad := range []string{"", "00", DefaultKeyHex()[:62] + "zz", DefaultKeyHex() + "00"} {
		if _, err := ParseKey(bad); err == nil {
			t.Errorf("ParseKey(%q): want error", bad)
		}
	}
}

func TestSealOpen(t *testing.T) {
	gcm, err := NewAEAD(DefaultKey())
	if err != nil {
		t.Fatal(err)
	}
	pkt := IPv4UDP([4]byte{10, 0, 0, 1}, [4]byte{10, 0, 0, 2}, 4000, 5000, 512)
	nonce := make([]byte, 12)
	nonce[0] = 7
	body := Seal(gcm, nonce, pkt)
	if len(body) != len(pkt)+AESOverhead {
		t.Fatalf("len(body) = %d, want %d", len(body), len(pkt)+AESOverhead)
	}
	if !bytes.Equal(body[:12], nonce) {
		t.Fatalf("body does not start with the nonce")
	}

	// Round trip.
	pt, ok := Open(gcm, nil, body)
	if !ok || !bytes.Equal(pt, pkt) {
		t.Fatalf("round trip failed: ok=%v", ok)
	}

	// Empty plaintext is a valid 28-byte frame.
	if pt, ok := Open(gcm, nil, Seal(gcm, nonce, nil)); !ok || len(pt) != 0 {
		t.Fatalf("empty plaintext: ok=%v len=%d", ok, len(pt))
	}

	// Wrong key.
	other := DefaultKey()
	other[0] ^= 0xff
	gcm2, err := NewAEAD(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := Open(gcm2, nil, body); ok {
		t.Fatal("wrong key: Open succeeded")
	}

	// Tampered ciphertext and tag.
	for _, i := range []int{0, 12, len(body) - 1} {
		bad := append([]byte(nil), body...)
		bad[i] ^= 1
		if _, ok := Open(gcm, nil, bad); ok {
			t.Fatalf("tampered byte %d: Open succeeded", i)
		}
	}

	// Short bodies.
	for n := 0; n < AESOverhead; n++ {
		if _, ok := Open(gcm, nil, body[:n]); ok {
			t.Fatalf("short body %d: Open succeeded", n)
		}
	}
}

func TestOpenNoAlloc(t *testing.T) {
	gcm, err := NewAEAD(DefaultKey())
	if err != nil {
		t.Fatal(err)
	}
	pkt := IPv4UDP([4]byte{10, 0, 0, 1}, [4]byte{10, 0, 0, 2}, 4000, 5000, 1400)
	body := Seal(gcm, make([]byte, 12), pkt)
	dst := make([]byte, 0, 65535)
	allocs := testing.AllocsPerRun(100, func() {
		if _, ok := Open(gcm, dst, body); !ok {
			t.Fatal("Open failed")
		}
	})
	if allocs != 0 {
		t.Fatalf("Open allocated %.1f times per call", allocs)
	}
}
