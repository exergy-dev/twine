package proto

import (
	"bytes"
	"testing"
)

// The steady state of each parser is a complete frame in one chunk: it must
// be delivered in place with no heap allocation (design Section 7.3).

func TestLineReceiverZeroAlloc(t *testing.T) {
	lr := &LineReceiver{}
	n := 0
	lr.LineReceived = func(line []byte) { n += len(line) }
	ft := newFake()
	lr.ConnectionMade(ft)
	chunk := []byte("GET /index.html HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if a := testing.AllocsPerRun(1000, func() { lr.DataReceived(chunk) }); a != 0 {
		t.Fatalf("LineReceiver.DataReceived allocates %v per run", a)
	}
	if a := testing.AllocsPerRun(1000, func() { lr.SendLine(chunk[:10]); ft.reset() }); a != 0 {
		t.Fatalf("LineReceiver.SendLine allocates %v per run", a)
	}
	if n == 0 {
		t.Fatal("no lines delivered")
	}
}

func TestNetstringZeroAlloc(t *testing.T) {
	ns := &NetstringReceiver{}
	n := 0
	ns.StringReceived = func(s []byte) { n += len(s) }
	ft := newFake()
	ns.ConnectionMade(ft)
	chunk := []byte("12:hello world!,0:,5:abcde,")
	if a := testing.AllocsPerRun(1000, func() { ns.DataReceived(chunk) }); a != 0 {
		t.Fatalf("NetstringReceiver.DataReceived allocates %v per run", a)
	}
	if a := testing.AllocsPerRun(1000, func() { ns.SendString(chunk); ft.reset() }); a != 0 {
		t.Fatalf("NetstringReceiver.SendString allocates %v per run", a)
	}
	if n == 0 {
		t.Fatal("no strings delivered")
	}
}

// TestPartialFrameBufferBounded: a header declaring a 1 MiB frame followed
// by a stall must not make the framer allocate the declared size up front;
// the buffer grows only as body bytes arrive, and the frame is still
// delivered intact.
func TestPartialFrameBufferBounded(t *testing.T) {
	const n = 1 << 20
	body := bytes.Repeat([]byte("x"), n)

	lp := &LengthPrefixed{}
	var gotLP []byte
	lp.FrameReceived = func(f []byte) { gotLP = append([]byte(nil), f...) }
	lp.ConnectionMade(newFake())
	lp.DataReceived([]byte{0, 0x10, 0, 0, 'x'})
	if c := cap(lp.buf); c > retainLimit {
		t.Fatalf("LengthPrefixed holds %d bytes for a 1-byte partial frame", c)
	}
	lp.DataReceived(body[1:])
	if !bytes.Equal(gotLP, body) {
		t.Fatalf("LengthPrefixed delivered %d bytes, want %d", len(gotLP), n)
	}

	ns := &NetstringReceiver{}
	var gotNS []byte
	ns.StringReceived = func(s []byte) { gotNS = append([]byte(nil), s...) }
	ns.ConnectionMade(newFake())
	ns.DataReceived([]byte("1048576:x"))
	if c := cap(ns.buf); c > retainLimit {
		t.Fatalf("NetstringReceiver holds %d bytes for a 1-byte partial frame", c)
	}
	ns.DataReceived(append(body[1:], ','))
	if !bytes.Equal(gotNS, body) {
		t.Fatalf("NetstringReceiver delivered %d bytes, want %d", len(gotNS), n)
	}
}

func TestLengthPrefixedZeroAlloc(t *testing.T) {
	for _, p := range []int{1, 2, 4} {
		lp := &LengthPrefixed{PrefixLen: p}
		n := 0
		lp.FrameReceived = func(f []byte) { n += len(f) }
		ft := newFake()
		lp.ConnectionMade(ft)
		body := []byte("0123456789abcdef")
		for i := 0; i < 3; i++ {
			_ = lp.SendFrame(body)
		}
		chunk := append([]byte(nil), ft.out...)
		ft.reset()
		if a := testing.AllocsPerRun(1000, func() { lp.DataReceived(chunk) }); a != 0 {
			t.Fatalf("prefix %d: LengthPrefixed.DataReceived allocates %v per run", p, a)
		}
		if a := testing.AllocsPerRun(1000, func() { _ = lp.SendFrame(body); ft.reset() }); a != 0 {
			t.Fatalf("prefix %d: LengthPrefixed.SendFrame allocates %v per run", p, a)
		}
		if n != 1001*3*len(body) {
			t.Fatalf("prefix %d: delivered %d bytes", p, n)
		}
	}
}
