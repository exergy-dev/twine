package proto

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/robert-malhotra/twine"
)

func newLR(t *testing.T, lr *LineReceiver) (*fakeTransport, *collector) {
	t.Helper()
	ft := newFake()
	c := &collector{}
	lr.LineReceived = c.add
	lr.ConnectionMade(ft)
	return ft, c
}

func TestLineReceiverBasic(t *testing.T) {
	lr := &LineReceiver{}
	_, c := newLR(t, lr)
	feed(lr, []byte("hello\r\nworld\r\n\r\nbare\nnl\r\npartial"))
	want := [][]byte{[]byte("hello"), []byte("world"), {}, []byte("bare\nnl")}
	if !framesEqual(c.frames, want) {
		t.Fatalf("got %q want %q", c.frames, want)
	}
	feed(lr, []byte(" done\r\n"))
	if got := c.frames[len(c.frames)-1]; string(got) != "partial done" {
		t.Fatalf("got %q", got)
	}
}

func TestLineReceiverSplits(t *testing.T) {
	delims := []string{"\r\n", "\n", "ab", "\r\n\r\n", "aab", "|"}
	rng := rand.New(rand.NewPCG(1, 2))
	for _, d := range delims {
		for iter := 0; iter < 300; iter++ {
			// Random text over a small alphabet that collides with the
			// delimiters, so they appear often and overlap.
			data := make([]byte, rng.IntN(200))
			for i := range data {
				data[i] = "ab\r\n|x"[rng.IntN(6)]
			}
			max := 1 + rng.IntN(40)
			want, wantErr := refLines(data, []byte(d), max)
			for _, sizes := range [][]int{{1}, {2}, {3}, {7}, {1000}, {1, 5, 2, 9}} {
				lr := &LineReceiver{Delimiter: []byte(d), MaxLineLength: max}
				ft, c := newLR(t, lr)
				feed(lr, data, sizes...)
				if !framesEqual(c.frames, want) {
					t.Fatalf("delim %q sizes %v data %q: got %q want %q", d, sizes, data, c.frames, want)
				}
				if !errors.Is(lr.Err(), wantErr) || (wantErr == nil) != (lr.Err() == nil) || ft.aborted != (wantErr != nil) {
					t.Fatalf("delim %q sizes %v data %q: err %v aborted %v, want %v", d, sizes, data, lr.Err(), ft.aborted, wantErr)
				}
			}
		}
	}
}

func TestLineReceiverDelimiterSpansChunks(t *testing.T) {
	lr := &LineReceiver{Delimiter: []byte("\r\n\r\n")}
	_, c := newLR(t, lr)
	feed(lr, []byte("head\r"))
	feed(lr, []byte("\n\r"))
	if len(c.frames) != 0 {
		t.Fatalf("early line %q", c.frames)
	}
	feed(lr, []byte("\nbody\r\n\r\n"))
	want := [][]byte{[]byte("head"), []byte("body")}
	if !framesEqual(c.frames, want) {
		t.Fatalf("got %q want %q", c.frames, want)
	}
}

func TestLineReceiverMaxLength(t *testing.T) {
	var lost error
	lr := &LineReceiver{MaxLineLength: 4, OnConnectionLost: func(err error) { lost = err }}
	ft, c := newLR(t, lr)
	feed(lr, []byte("1234\r\n12345\r\nnever\r\n"))
	if len(c.frames) != 1 || string(c.frames[0]) != "1234" {
		t.Fatalf("frames %q", c.frames)
	}
	if !ft.aborted || !errors.Is(lr.Err(), twine.ErrLineTooLong) {
		t.Fatalf("aborted %v err %v", ft.aborted, lr.Err())
	}
	// The core reports nil after Abort; the receiver substitutes its error.
	lr.ConnectionLost(nil)
	if !errors.Is(lost, twine.ErrLineTooLong) {
		t.Fatalf("OnConnectionLost got %v", lost)
	}
	feed(lr, []byte("x\r\n"))
	if len(c.frames) != 1 {
		t.Fatal("data delivered after abort")
	}
}

func TestLineReceiverEndlessPartial(t *testing.T) {
	lr := &LineReceiver{MaxLineLength: 10}
	ft, _ := newLR(t, lr)
	// 11 bytes may still be 10 + '\r'; 12 cannot be a valid line.
	feed(lr, bytes.Repeat([]byte("x"), 11), 1)
	if ft.aborted {
		t.Fatal("aborted at max+len(delim)-1")
	}
	feed(lr, []byte("x"))
	if !ft.aborted || !errors.Is(lr.Err(), twine.ErrLineTooLong) {
		t.Fatalf("aborted %v err %v", ft.aborted, lr.Err())
	}
}

func TestLineReceiverRawModeSwitch(t *testing.T) {
	for _, sizes := range [][]int{{1000}, {1}, {3}, {6, 1}} {
		lr := &LineReceiver{}
		var raw []byte
		var lines []string
		lr.LineReceived = func(line []byte) {
			lines = append(lines, string(line))
			if string(line) == "RAW" {
				lr.SetRawMode()
			}
		}
		lr.RawDataReceived = func(b []byte) {
			// Consume up to and including '!' and hand the rest back.
			if i := bytes.IndexByte(b, '!'); i >= 0 {
				raw = append(raw, b[:i]...)
				lr.SetLineMode(b[i+1:])
				return
			}
			raw = append(raw, b...)
		}
		lr.ConnectionMade(newFake())
		feed(lr, []byte("one\r\nRAW\r\nraw bytes\r\n!two\r\nthree\r\n"), sizes...)
		if string(raw) != "raw bytes\r\n" {
			t.Fatalf("sizes %v: raw %q", sizes, raw)
		}
		want := []string{"one", "RAW", "two", "three"}
		if len(lines) != len(want) {
			t.Fatalf("sizes %v: lines %q", sizes, lines)
		}
		for i := range want {
			if lines[i] != want[i] {
				t.Fatalf("sizes %v: lines %q", sizes, lines)
			}
		}
	}
}

func TestLineReceiverRawFromBufferedLine(t *testing.T) {
	// The line that switches modes completes from the partial buffer; the
	// remainder of that chunk must go to RawDataReceived.
	lr := &LineReceiver{}
	var raw []byte
	lr.LineReceived = func([]byte) { lr.SetRawMode() }
	lr.RawDataReceived = func(b []byte) { raw = append(raw, b...) }
	lr.ConnectionMade(newFake())
	feed(lr, []byte("GO"))
	feed(lr, []byte("\r\nleftover"))
	if string(raw) != "leftover" || !lr.RawMode() {
		t.Fatalf("raw %q", raw)
	}
}

func TestLineReceiverSetLineModeInsideLine(t *testing.T) {
	// Re-entrant SetLineMode(extra) from LineReceived: extra is parsed
	// before the rest of the current chunk.
	lr := &LineReceiver{}
	var lines []string
	lr.LineReceived = func(line []byte) {
		lines = append(lines, string(line))
		if string(line) == "inject" {
			lr.SetLineMode([]byte("x\r\npart"))
		}
	}
	lr.ConnectionMade(newFake())
	feed(lr, []byte("inject\r\nial\r\nlast\r\n"))
	want := []string{"inject", "x", "partial", "last"}
	if len(lines) != len(want) {
		t.Fatalf("lines %q", lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("lines %q", lines)
		}
	}
}

func TestLineReceiverSendLine(t *testing.T) {
	lr := &LineReceiver{Delimiter: []byte("\n")}
	ft, _ := newLR(t, lr)
	lr.SendLine(nil, []byte("hi"))
	lr.SendLine(ft, []byte("there"))
	if string(ft.out) != "hi\nthere\n" || ft.writes != 2 {
		t.Fatalf("out %q writes %d", ft.out, ft.writes)
	}
	if lr.iov[0] != nil || lr.iov[1] != nil {
		t.Fatal("SendLine retains the caller's slice")
	}
}

func TestLineReceiverConnectionHooks(t *testing.T) {
	var made twine.Transport
	var lost error = errors.New("unset")
	lr := &LineReceiver{
		OnConnectionMade: func(t twine.Transport) { made = t },
		OnConnectionLost: func(err error) { lost = err },
	}
	ft := newFake()
	lr.ConnectionMade(ft)
	if made != ft || lr.Transport() != ft {
		t.Fatal("transport not stored")
	}
	lr.ConnectionLost(nil)
	if lost != nil {
		t.Fatalf("clean close reported %v", lost)
	}
}
