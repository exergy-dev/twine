package tunpkt

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
)

// AESOverhead is the per-frame AES-256-GCM overhead: a 12-byte nonce in
// front of the ciphertext and a 16-byte tag behind it.
const AESOverhead = 12 + 16

// DefaultKey returns the fixed benchmark key shared by the load generator
// and the servers: 32 bytes 0x00, 0x01, ..., 0x1f.
func DefaultKey() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i)
	}
	return k
}

// DefaultKeyHex is DefaultKey hex encoded, the default of the -key flags.
func DefaultKeyHex() string { return hex.EncodeToString(DefaultKey()) }

// ParseKey decodes a 64-character hex string into a 32-byte AES-256 key.
func ParseKey(s string) ([]byte, error) {
	if len(s) != 64 {
		return nil, fmt.Errorf("aes key: want 64 hex characters, got %d", len(s))
	}
	k, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("aes key: %w", err)
	}
	return k, nil
}

// NewAEAD returns an AES-256-GCM AEAD (12-byte nonce, 16-byte tag). The
// result is safe for concurrent use.
func NewAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("aes key: want 32 bytes, got %d", len(key))
	}
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Seal encrypts plaintext with the 12-byte nonce and returns the frame body
// nonce || ciphertext || tag (len(plaintext) + AESOverhead bytes).
func Seal(gcm cipher.AEAD, nonce, plaintext []byte) []byte {
	out := make([]byte, 12, len(plaintext)+AESOverhead)
	copy(out, nonce)
	return gcm.Seal(out, out[:12], plaintext, nil)
}

// Open decrypts the frame body nonce || ciphertext || tag into dst[:0] and
// returns the plaintext. ok is false when body is shorter than AESOverhead
// or the tag does not verify. When cap(dst) >= len(body)-AESOverhead it does
// not allocate. dst must not overlap body.
func Open(gcm cipher.AEAD, dst, body []byte) ([]byte, bool) {
	if len(body) < AESOverhead {
		return nil, false
	}
	pt, err := gcm.Open(dst[:0], body[:12], body[12:], nil)
	if err != nil {
		return nil, false
	}
	return pt, true
}
