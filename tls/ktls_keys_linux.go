//go:build linux

package tls

import (
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	ctls "crypto/tls"
	"encoding/binary"
	"hash"
	"reflect"
	"sync"
	"unsafe"
)

// kTLS record-protection kinds this package can install, in the order of the
// kernel's cipher_type numbering (linux/tls.h).
type ktlsKind uint8

const (
	kindAES128 ktlsKind = iota
	kindAES256
	kindChaCha
	numKinds
)

// Kernel constants (linux/tls.h). Declared here, not taken from x/sys, so
// they are platform independent and explicit.
const (
	tlsTX              = 1
	tlsRX              = 2
	tlsSetRecordType   = 1 // cmsg type for sendmsg
	tls13Version       = 0x0304
	cipherAESGCM128    = 51
	cipherAESGCM256    = 52
	cipherChaCha20Poly = 54
	recordTypeAlert    = 21
)

// kindFor maps a TLS 1.3 cipher suite to its kernel cipher kind, key length
// and transcript hash.
func kindFor(suite uint16) (k ktlsKind, keyLen int, h func() hash.Hash, ok bool) {
	switch suite {
	case ctls.TLS_AES_128_GCM_SHA256:
		return kindAES128, 16, sha256.New, true
	case ctls.TLS_AES_256_GCM_SHA384:
		return kindAES256, 32, sha512.New384, true
	case ctls.TLS_CHACHA20_POLY1305_SHA256:
		return kindChaCha, 32, sha256.New, true
	}
	return 0, 0, nil, false
}

// expandLabel is HKDF-Expand-Label (RFC 8446 section 7.1) with an empty
// context.
func expandLabel(h func() hash.Hash, secret []byte, label string, n int) ([]byte, error) {
	full := "tls13 " + label
	info := make([]byte, 0, 4+len(full))
	info = binary.BigEndian.AppendUint16(info, uint16(n))
	info = append(info, byte(len(full)))
	info = append(info, full...)
	info = append(info, 0)
	return hkdf.Expand(h, secret, string(info), n)
}

// ktlsKeys is the per-direction key material for setsockopt(SOL_TLS).
type ktlsKeys struct {
	key, iv []byte // iv is the full 12-byte static IV
	seq     [8]byte
}

// cryptoInfo serialises the keys as the kernel's tls12_crypto_info_* struct
// for TLS 1.3 (linux/tls.h): {version, cipher_type, iv, key, salt, rec_seq}.
// For AES-GCM the 12-byte IV splits into salt (first 4 bytes) and iv (last 8);
// ChaCha20-Poly1305 has a 12-byte iv and no salt.
func (k *ktlsKeys) cryptoInfo(kind ktlsKind) []byte {
	var typ uint16
	switch kind {
	case kindAES128:
		typ = cipherAESGCM128
	case kindAES256:
		typ = cipherAESGCM256
	default:
		typ = cipherChaCha20Poly
	}
	b := make([]byte, 0, 80)
	b = binary.NativeEndian.AppendUint16(b, tls13Version)
	b = binary.NativeEndian.AppendUint16(b, typ)
	if kind == kindChaCha {
		b = append(b, k.iv...)
		b = append(b, k.key...)
	} else {
		b = append(b, k.iv[4:]...)
		b = append(b, k.key...)
		b = append(b, k.iv[:4]...)
	}
	return append(b, k.seq[:]...)
}

// deriveKeys expands a TLS 1.3 traffic secret exactly as crypto/tls does.
func deriveKeys(h func() hash.Hash, secret []byte, keyLen int, seq [8]byte) (*ktlsKeys, error) {
	key, err := expandLabel(h, secret, "key", keyLen)
	if err != nil {
		return nil, err
	}
	iv, err := expandLabel(h, secret, "iv", 12)
	if err != nil {
		return nil, err
	}
	return &ktlsKeys{key: key, iv: iv, seq: seq}, nil
}

// --- reflection into crypto/tls ---------------------------------------------

// peek returns a readable view of the (possibly unexported) field name of
// the addressable struct v.
func peek(v reflect.Value, name string) (reflect.Value, bool) {
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	f := v.FieldByName(name)
	if !f.IsValid() || !f.CanAddr() {
		return reflect.Value{}, false
	}
	return reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem(), true
}

// halfState is what is read from a crypto/tls halfConn.
type halfState struct {
	secret []byte
	seq    [8]byte
}

// halfOf reads trafficSecret and seq of c.<dir> ("in" or "out"). It returns
// ok=false on any mismatch with the layout this package knows.
func halfOf(c reflect.Value, dir string) (hs halfState, ok bool) {
	h, ok := peek(c, dir)
	if !ok || h.Kind() != reflect.Struct {
		return hs, false
	}
	sec, ok := peek(h, "trafficSecret")
	if !ok || sec.Kind() != reflect.Slice || sec.Type().Elem().Kind() != reflect.Uint8 {
		return hs, false
	}
	seq, ok := peek(h, "seq")
	if !ok || seq.Kind() != reflect.Array || seq.Len() != 8 || seq.Type().Elem().Kind() != reflect.Uint8 {
		return hs, false
	}
	if ci, ok := peek(h, "cipher"); !ok || ci.Kind() != reflect.Interface || ci.IsNil() {
		return hs, false
	}
	hs.secret = append([]byte(nil), sec.Bytes()...)
	reflect.Copy(reflect.ValueOf(&hs.seq).Elem(), seq)
	return hs, true
}

// bufLen returns Len() of the bytes.Buffer / bytes.Reader field name of c.
func bufLen(c reflect.Value, name string) (int, bool) {
	f, ok := peek(c, name)
	if !ok || !f.CanAddr() {
		return 0, false
	}
	l, ok := f.Addr().Interface().(interface{ Len() int })
	if !ok {
		return 0, false
	}
	return l.Len(), true
}

// connSnapshot is everything installKTLS needs from a *crypto/tls.Conn.
type connSnapshot struct {
	out, in    halfState
	cleanInput bool // crypto/tls holds no buffered, undelivered input
}

// snapshot reads the record-layer state of tc. ok=false means the layout is
// not the expected one (a different Go version) or the connection is not a
// finished TLS 1.3 session: the caller falls back. wantSuite and wantVers
// are cross-checked against the unexported fields.
func snapshot(tc *ctls.Conn, wantSuite, wantVers uint16) (s connSnapshot, ok bool) {
	defer func() {
		if recover() != nil {
			s, ok = connSnapshot{}, false
		}
	}()
	c := reflect.ValueOf(tc).Elem()
	if c.Kind() != reflect.Struct || c.Type().PkgPath() != "crypto/tls" {
		return s, false
	}
	suite, ok1 := peek(c, "cipherSuite")
	vers, ok2 := peek(c, "vers")
	if !ok1 || !ok2 || suite.Kind() != reflect.Uint16 || vers.Kind() != reflect.Uint16 {
		return s, false
	}
	if uint16(suite.Uint()) != wantSuite || uint16(vers.Uint()) != wantVers {
		return s, false
	}
	if s.out, ok = halfOf(c, "out"); !ok {
		return s, false
	}
	if s.in, ok = halfOf(c, "in"); !ok {
		return s, false
	}
	clean := true
	for _, name := range []string{"rawInput", "input", "hand"} {
		n, ok := bufLen(c, name)
		if !ok {
			return s, false
		}
		if n != 0 {
			clean = false
		}
	}
	s.cleanInput = clean
	return s, true
}

// seqSane reports whether a sequence number is plausible for the start of a
// session (below 1<<32), a guard against reading garbage.
func seqSane(seq [8]byte) bool { return binary.BigEndian.Uint64(seq[:]) < 1<<32 }

// --- capability ---------------------------------------------------------------

var (
	cipherOnce [numKinds]sync.Once
	cipherOK   [numKinds]bool
)

// cipherUsable reports whether the kernel accepts both TLS_TX and TLS_RX for
// kind, probed once per kind on a throwaway loopback socket with dummy keys.
func cipherUsable(kind ktlsKind) bool {
	cipherOnce[kind].Do(func() {
		if !KTLSAvailable() {
			return
		}
		keyLen := 16
		if kind != kindAES128 {
			keyLen = 32
		}
		k := &ktlsKeys{key: make([]byte, keyLen), iv: make([]byte, 12)}
		cipherOK[kind] = probeCipher(k.cryptoInfo(kind))
	})
	return cipherOK[kind]
}
