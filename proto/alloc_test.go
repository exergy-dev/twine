package proto

import (
	"bytes"
	"testing"
)

// The steady state of each parser is a complete frame in one chunk: it must
// be delivered in place with no heap allocation.

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

// TestSplitFrameZeroAlloc: a frame split across two chunks goes through the
// partial-frame buffer, which is reused, so the steady state does not
// allocate either.
func TestSplitFrameZeroAlloc(t *testing.T) {
	for _, tc := range []struct {
		name  string
		p     interface{ DataReceived([]byte) }
		frame []byte
	}{
		{"LineReceiver", newBenchLine(), benchLine},
		{"NetstringReceiver", newBenchNS(), benchNSFrame},
		{"LengthPrefixed", newBenchLP(), benchLPFrame},
	} {
		for _, split := range []int{1, len(tc.frame) / 2, len(tc.frame) - 1} {
			a, b := tc.frame[:split], tc.frame[split:]
			tc.p.DataReceived(a) // warm the buffer
			tc.p.DataReceived(b)
			if n := testing.AllocsPerRun(1000, func() { tc.p.DataReceived(a); tc.p.DataReceived(b) }); n != 0 {
				t.Fatalf("%s split at %d allocates %v per frame", tc.name, split, n)
			}
		}
	}
}

// TestAMPAllocs bounds the allocations of one request/answer exchange and
// of serving one request. AMP allocates by design (a map and strings per
// box, a Future per call); this gate catches regressions.
func TestAMPAllocs(t *testing.T) {
	if debugBuild {
		t.Skip("twine_debug adds allocations")
	}
	a := &AMP{}
	ft := newFake()
	a.ConnectionMade(ft)
	args := Args{"a": "13", "b": "29"}
	answer := twistedBox("_answer", "1", "total", "42")
	call := testing.AllocsPerRun(1000, func() {
		ft.reset()
		a.next = 0
		a.Call("Sum", args)
		a.DataReceived(answer)
	})
	if a.Pending() != 0 {
		t.Fatal("calls left pending")
	}

	s := &AMP{Handle: func(_ string, args Args, reply func(Args, error)) {
		reply(Args{"total": args["a"]}, nil)
	}}
	s.ConnectionMade(ft)
	req := twistedBox("_ask", "1", "_command", "Sum", "a", "13", "b", "29")
	serve := testing.AllocsPerRun(1000, func() { ft.reset(); s.DataReceived(req) })

	// Measured with Go 1.25.3 (7 and 9); if a Go release moves them,
	// re-measure with BenchmarkAMPCall/BenchmarkAMPServe before changing the
	// bounds.
	const maxCall, maxServe = 7, 9
	if call > maxCall || serve > maxServe {
		t.Fatalf("AMP allocations: call+answer %v (max %d), serve %v (max %d)", call, maxCall, serve, maxServe)
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
