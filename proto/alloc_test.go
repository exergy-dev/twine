package proto

import (
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
