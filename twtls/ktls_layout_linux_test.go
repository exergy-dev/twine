package twtls

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"math/big"
	"net"
	"runtime"
	"testing"
	"time"
)

// recordCapture passes the handshake through and then keeps each Write (one
// TLS record per Write) instead of sending it.
type recordCapture struct {
	net.Conn
	capture bool
	recs    [][]byte
}

func (c *recordCapture) Write(p []byte) (int, error) {
	if c.capture {
		c.recs = append(c.recs, bytes.Clone(p))
		return len(p), nil
	}
	return c.Conn.Write(p)
}

// tls13Pair runs a TLS 1.3 handshake between a crypto/tls client and server
// over loopback TCP (net.Pipe would deadlock: the server writes its session
// tickets inside Handshake) and returns both ends, idle.
func tls13Pair(t *testing.T) (client, server *tls.Conn, cc *recordCapture) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "layout"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srvCh := make(chan *tls.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		s := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
		if err := s.Handshake(); err != nil {
			errCh <- err
			return
		}
		srvCh <- s
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cc = &recordCapture{Conn: raw}
	client = tls.Client(cc, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
	if err := client.Handshake(); err != nil {
		t.Fatal(err)
	}
	select {
	case server = <-srvCh: // the server goroutine is done with it
	case err := <-errCh:
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server, cc
}

// TestKTLSLayout is the kernel-independent guard for kTLS key extraction:
// on a live crypto/tls connection the reflection probe must resolve every
// field it reads, the traffic secrets must have the suite's hash length and
// pair up across the two ends, and the keys derived from them must decrypt a
// record crypto/tls actually wrote. A Go release that renames, retypes or
// repurposes one of those unexported fields fails here, instead of silently
// turning offload off.
func TestKTLSLayout(t *testing.T) {
	client, server, cc := tls13Pair(t)
	st := client.ConnectionState()
	if st.Version != tls.VersionTLS13 {
		t.Fatalf("negotiated %x, want TLS 1.3", st.Version)
	}
	kind, keyLen, h, ok := kindFor(st.CipherSuite)
	if !ok {
		// Go picks ChaCha20-Poly1305 only without AES hardware.
		t.Skipf("negotiated %s, which this package does not offload", tls.CipherSuiteName(st.CipherSuite))
	}
	cs, ok := snapshot(client, st.CipherSuite, tls.VersionTLS13)
	if !ok {
		t.Fatalf("snapshot failed on the client: the crypto/tls layout changed in %s", runtime.Version())
	}
	ss, ok := snapshot(server, st.CipherSuite, tls.VersionTLS13)
	if !ok {
		t.Fatalf("snapshot failed on the server: the crypto/tls layout changed in %s", runtime.Version())
	}
	for name, hs := range map[string]halfState{"client out": cs.out, "client in": cs.in, "server out": ss.out, "server in": ss.in} {
		if len(hs.secret) != h().Size() {
			t.Errorf("%s: secret is %d bytes, want %d", name, len(hs.secret), h().Size())
		}
		if !seqSane(hs.seq) {
			t.Errorf("%s: implausible sequence number %x", name, hs.seq)
		}
	}
	if !bytes.Equal(cs.out.secret, ss.in.secret) || !bytes.Equal(cs.in.secret, ss.out.secret) {
		t.Fatal("client and server traffic secrets do not pair up: in/out or trafficSecret changed meaning")
	}

	// Decrypt a record the client writes with the keys derived from the
	// client's out secret and sequence number, as the kernel would.
	keys, err := deriveKeys(h, cs.out.secret, keyLen, cs.out.seq)
	if err != nil {
		t.Fatal(err)
	}
	if info := keys.cryptoInfo(kind); len(info) != 4+8+keyLen+4+8 {
		t.Fatalf("crypto_info is %d bytes", len(info))
	}
	cc.capture = true
	msg := []byte("layout check")
	if _, err := client.Write(msg); err != nil {
		t.Fatal(err)
	}
	if len(cc.recs) != 1 {
		t.Fatalf("captured %d records, want 1", len(cc.recs))
	}
	rec := cc.recs[0]
	block, err := aes.NewCipher(keys.key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Clone(keys.iv)
	seq := binary.BigEndian.Uint64(keys.seq[:])
	for i := 0; i < 8; i++ {
		nonce[4+i] ^= byte(seq >> (56 - 8*i))
	}
	pt, err := aead.Open(nil, nonce, rec[5:], rec[:5])
	if err != nil {
		t.Fatalf("derived keys do not decrypt crypto/tls's record: %v", err)
	}
	if want := append(bytes.Clone(msg), 23); !bytes.Equal(pt, want) { // inner type application_data
		t.Fatalf("decrypted %q, want %q", pt, want)
	}
}
