package proto

import (
	"testing"

	"github.com/exergy-dev/twine"
)

// Receive-path benchmarks. "Whole" delivers one frame per chunk (the fast
// path: the frame is handed over in place); "Split" delivers the same frame
// in two chunks cut mid-body, so the partial-frame buffer is used. The
// framers are fed directly, with a fake transport and no engine.

var benchLine = []byte("PRIVMSG #twine :a line of chat that is long enough to matter\r\n")

func benchChunks(b *testing.B, p interface{ DataReceived([]byte) }, frame []byte, split int) {
	b.Helper()
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	if split <= 0 {
		for b.Loop() {
			p.DataReceived(frame)
		}
		return
	}
	for b.Loop() {
		p.DataReceived(frame[:split])
		p.DataReceived(frame[split:])
	}
}

func newBenchLine() *LineReceiver {
	lr := &LineReceiver{}
	lr.LineReceived = func([]byte) {}
	lr.ConnectionMade(newFake())
	return lr
}

func BenchmarkLineWhole(b *testing.B) { benchChunks(b, newBenchLine(), benchLine, 0) }
func BenchmarkLineSplit(b *testing.B) {
	benchChunks(b, newBenchLine(), benchLine, len(benchLine)/2)
}

// benchLPFrame is a 4-byte big-endian prefix and a 60-byte body.
var benchLPFrame = append([]byte{0, 0, 0, 60}, benchLine[:60]...)

func newBenchLP() *LengthPrefixed {
	lp := &LengthPrefixed{}
	lp.FrameReceived = func([]byte) {}
	lp.ConnectionMade(newFake())
	return lp
}

func BenchmarkLPWhole(b *testing.B) { benchChunks(b, newBenchLP(), benchLPFrame, 0) }
func BenchmarkLPSplit(b *testing.B) {
	benchChunks(b, newBenchLP(), benchLPFrame, len(benchLPFrame)/2)
}

var benchNSFrame = append(append([]byte("60:"), benchLine[:60]...), ',')

func newBenchNS() *NetstringReceiver {
	ns := &NetstringReceiver{}
	ns.StringReceived = func([]byte) {}
	ns.ConnectionMade(newFake())
	return ns
}

func BenchmarkNSWhole(b *testing.B) { benchChunks(b, newBenchNS(), benchNSFrame, 0) }
func BenchmarkNSSplit(b *testing.B) {
	benchChunks(b, newBenchNS(), benchNSFrame, len(benchNSFrame)/2)
}

// The Huge benchmarks model a peer that sends a header declaring a 1 MiB
// frame plus one body byte and then stalls: B/op is what one such
// connection makes the framer hold.

func BenchmarkLPPartialHuge(b *testing.B) {
	ft := newFake()
	in := []byte{0, 0x10, 0, 0, 'x'}
	b.ReportAllocs()
	for b.Loop() {
		lp := &LengthPrefixed{}
		lp.ConnectionMade(ft)
		lp.DataReceived(in)
	}
}

func BenchmarkNSPartialHuge(b *testing.B) {
	ft := newFake()
	in := []byte("1048576:x")
	b.ReportAllocs()
	for b.Loop() {
		ns := &NetstringReceiver{}
		ns.ConnectionMade(ft)
		ns.DataReceived(in)
	}
}

// BenchmarkAMPCall is one request/answer exchange over a fake transport:
// Call encodes and writes the request, then the peer's answer box is parsed
// and resolves the Future.
func BenchmarkAMPCall(b *testing.B) {
	a := &AMP{}
	ft := newFake()
	a.ConnectionMade(ft)
	args := Args{"a": "13", "b": "29"}
	answer := twistedBox("_answer", "1", "total", "42")
	b.ReportAllocs()
	for b.Loop() {
		ft.reset()
		a.next = 0 // every call gets tag 1, matching the canned answer
		f := a.Call("Sum", args)
		a.DataReceived(answer)
		if !f.Done() {
			b.Fatal("call not answered")
		}
	}
}

// BenchmarkAMPServe is one incoming request answered by a synchronous
// handler.
func BenchmarkAMPServe(b *testing.B) {
	a := &AMP{Handle: func(_ string, args Args) *twine.Future[Args] {
		return twine.Resolved[Args](nil, Args{"total": args["a"]})
	}}
	ft := newFake()
	a.ConnectionMade(ft)
	req := twistedBox("_ask", "1", "_command", "Sum", "a", "13", "b", "29")
	b.ReportAllocs()
	for b.Loop() {
		ft.reset()
		a.DataReceived(req)
	}
	if ft.writes != 1 {
		b.Fatal("no answer written")
	}
}
