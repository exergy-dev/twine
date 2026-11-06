package tls_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	ctls "crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/robert-malhotra/twine"
	twtls "github.com/robert-malhotra/twine/tls"
)

// kinfo is what a server-side protocol reports about its connection.
type kinfo struct {
	offloaded, rx bool
	suite         uint16
	version       uint16
}

// kProto echoes and reports the offload mode once.
type kProto struct {
	echo
	info chan kinfo
	lost chan error
}

func (p *kProto) ConnectionMade(t twine.Transport) {
	p.t = t
	tt := t.(*twtls.Transport)
	st := tt.ConnectionState()
	p.info <- kinfo{tt.KernelOffloaded(), tt.RXOffloaded(), st.CipherSuite, st.Version}
}
func (p *kProto) ConnectionLost(err error) {
	if p.lost != nil {
		p.lost <- err
	}
}

func kFactory(info chan kinfo, lost chan error) twine.Factory {
	return twine.FactoryFunc(func(net.Addr) twine.Protocol { return &kProto{info: info, lost: lost} })
}

func requireKTLS(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" || !twtls.KTLSAvailable() {
		t.Skip("kTLS unavailable on this host")
	}
}

// settle gives the server loop time to finish the handshake before the client
// sends data. Offload only engages when nothing from the peer has been
// consumed yet; a client that pipelines data right behind its Finished can
// get TX-only offload, which is also correct but would make assertions racy.
func settle() { time.Sleep(150 * time.Millisecond) }

func TestKTLSGoClient(t *testing.T) {
	requireKTLS(t)
	e := startEngine(t)
	info := make(chan kinfo, 1)
	lost := make(chan error, 1)
	addr := listen(t, e, serverConfig(t), kFactory(info, lost), twtls.KernelOffload(true))
	c := dial(t, addr, &ctls.Config{InsecureSkipVerify: true, MinVersion: ctls.VersionTLS13})
	settle()
	ki := <-info
	t.Logf("suite=%s offloaded=%v rx=%v", ctls.CipherSuiteName(ki.suite), ki.offloaded, ki.rx)
	if !ki.offloaded || !ki.rx {
		t.Fatalf("kTLS did not engage: %+v", ki)
	}
	buf := make([]byte, 64)
	for i := 0; i < 1000; i++ {
		msg := []byte("message " + strings.Repeat("x", i%40))
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(c, buf[:len(msg)]); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(buf[:len(msg)], msg) {
			t.Fatalf("echo %d mismatch", i)
		}
	}
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)
	errCh := make(chan error, 1)
	go func() { _, err := c.Write(data); errCh <- err }()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("stream read: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("stream write: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("1 MiB stream corrupted")
	}
	// The peer's close_notify is a non-data record: the kernel fails the
	// read with EIO, which surfaces as ConnectionLost (documented).
	c.Close()
	select {
	case err := <-lost:
		t.Logf("ConnectionLost after client close: %v", err)
		if err != nil && !errors.Is(err, syscall.EIO) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("unexpected error %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
}

func TestKTLSOpenSSLSuites(t *testing.T) {
	requireKTLS(t)
	bin, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("openssl not on PATH")
	}
	e := startEngine(t)
	info := make(chan kinfo, 4)
	addr := listen(t, e, serverConfig(t), kFactory(info, nil), twtls.KernelOffload(true))
	for _, suite := range []struct {
		name string
		id   uint16
	}{
		{"TLS_AES_128_GCM_SHA256", ctls.TLS_AES_128_GCM_SHA256},
		{"TLS_AES_256_GCM_SHA384", ctls.TLS_AES_256_GCM_SHA384},
		{"TLS_CHACHA20_POLY1305_SHA256", ctls.TLS_CHACHA20_POLY1305_SHA256},
	} {
		t.Run(suite.name, func(t *testing.T) {
			usable := twtls.CipherUsable(suite.id)
			if !usable {
				t.Logf("kernel rejects kTLS keys for %s: expecting the memory-BIO fallback", suite.name)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, "s_client", "-connect", addr, "-quiet",
				"-tls1_3", "-ciphersuites", suite.name)
			stdin, _ := cmd.StdinPipe()
			stdout, _ := cmd.StdoutPipe()
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = stdin.Close()
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}()
			time.Sleep(500 * time.Millisecond) // let the handshake finish first
			var want bytes.Buffer
			rnd := make([]byte, 30)
			for i := 0; i < 3000; i++ {
				_, _ = rand.Read(rnd)
				want.WriteString(hex.EncodeToString(rnd) + "\n")
			}
			go func() { _, _ = stdin.Write(want.Bytes()) }()
			rd := bufio.NewReader(stdout)
			got := make([]byte, want.Len())
			done := make(chan error, 1)
			go func() { _, err := io.ReadFull(rd, got); done <- err }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("read: %v; stderr:\n%s", err, stderr.String())
				}
			case <-ctx.Done():
				t.Fatalf("timeout; stderr:\n%s", stderr.String())
			}
			if !bytes.Equal(got, want.Bytes()) {
				t.Fatal("echo mismatch")
			}
			ki := <-info
			t.Logf("negotiated %s offloaded=%v rx=%v", ctls.CipherSuiteName(ki.suite), ki.offloaded, ki.rx)
			if ki.suite != suite.id {
				t.Fatalf("negotiated %x, want %x", ki.suite, suite.id)
			}
			if ki.offloaded != usable || ki.rx != usable {
				t.Fatalf("offload=%+v, kernel support=%v", ki, usable)
			}
		})
	}
}

func TestKTLSLoseConnectionCloseNotify(t *testing.T) {
	requireKTLS(t)
	e := startEngine(t)
	lost := make(chan error, 1)
	offl := make(chan bool, 1)
	big := bytes.Repeat([]byte("x"), 2<<20) // far beyond the socket buffer: ring-buffered
	addr := listen(t, e, serverConfig(t), twine.FactoryFunc(func(net.Addr) twine.Protocol {
		return &offByeProto{byeProto: byeProto{lost: lost, big: big}, ch: offl}
	}), twtls.KernelOffload(true))
	c := dial(t, addr, &ctls.Config{InsecureSkipVerify: true})
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("ReadAll: %v (want clean close_notify EOF)", err)
	}
	if want := append([]byte("bye"), big...); !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	if !<-offl {
		t.Fatal("not offloaded")
	}
	c.Close()
	select {
	case <-lost:
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
}

type offByeProto struct {
	byeProto
	ch chan bool
}

func (p *offByeProto) ConnectionMade(t twine.Transport) {
	p.ch <- t.(*twtls.Transport).KernelOffloaded()
	p.byeProto.ConnectionMade(t)
}

func TestKTLSFallbacks(t *testing.T) {
	requireKTLS(t)
	e := startEngine(t)
	for _, tc := range []struct {
		name string
		opt  bool
		max  uint16
	}{
		{"TLS12", true, ctls.VersionTLS12},
		{"OffloadOff", false, ctls.VersionTLS13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := make(chan kinfo, 1)
			addr := listen(t, e, serverConfig(t), kFactory(info, nil), twtls.KernelOffload(tc.opt))
			c := dial(t, addr, &ctls.Config{InsecureSkipVerify: true, MaxVersion: tc.max})
			defer c.Close()
			settle()
			if _, err := c.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 5)
			if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hello" {
				t.Fatalf("echo: %q %v", buf, err)
			}
			if ki := <-info; ki.offloaded || ki.rx {
				t.Fatalf("unexpected offload: %+v", ki)
			}
		})
	}
}

// TestKTLSEarlyDataIsNotLost: data queued right behind the
// client's Finished must not be lost; RX stays in user space.
func TestKTLSEarlyDataIsNotLost(t *testing.T) {
	requireKTLS(t)
	e := startEngine(t)
	info := make(chan kinfo, 20)
	addr := listen(t, e, serverConfig(t), kFactory(info, nil), twtls.KernelOffload(true))
	for i := 0; i < 20; i++ {
		c := dial(t, addr, &ctls.Config{InsecureSkipVerify: true, MinVersion: ctls.VersionTLS13})
		// No settle: write immediately.
		if _, err := c.Write([]byte("early")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "early" {
			t.Fatalf("echo: %q %v", buf, err)
		}
		ki := <-info
		if !ki.offloaded {
			t.Fatalf("TX offload expected: %+v", ki)
		}
		c.Close()
	}
}

type kClient struct {
	clientProto
	info chan kinfo
}

func (p *kClient) ConnectionMade(t twine.Transport) {
	tt := t.(*twtls.Transport)
	p.info <- kinfo{offloaded: tt.KernelOffloaded(), rx: tt.RXOffloaded()}
	p.clientProto.ConnectionMade(t)
}

// A client offloads TX only: the server's post-handshake records stay in
// user space.
func TestKTLSClientTXOnly(t *testing.T) {
	requireKTLS(t)
	cert, pool := selfSigned(t)
	addr := goEchoServer(t, &ctls.Config{Certificates: []ctls.Certificate{cert}, MinVersion: ctls.VersionTLS13})
	e := startEngine(t)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 40000) // 640 KB
	p := &kClient{clientProto: clientProto{want: len(payload), done: make(chan []byte, 1), payload: payload},
		info: make(chan kinfo, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := twtls.Connect(e.NextLoop(), addr, &ctls.Config{RootCAs: pool},
		twine.FactoryFunc(func(net.Addr) twine.Protocol { return p }), twtls.KernelOffload(true)).Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ki := <-p.info; !ki.offloaded || ki.rx {
		t.Fatalf("want TX-only offload, got %+v", ki)
	}
	select {
	case got := <-p.done:
		if !bytes.Equal(got, payload) {
			t.Fatal("echo mismatch")
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}
