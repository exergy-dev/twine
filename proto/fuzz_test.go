package proto

import (
	"errors"
	"testing"
)

// splitSizes turns fuzzer-chosen bytes into chunk sizes for feed.
func splitSizes(s []byte) []int {
	if len(s) == 0 {
		return []int{1 << 30} // one chunk
	}
	sizes := make([]int, len(s))
	for i, b := range s {
		sizes[i] = int(b) + 1
	}
	return sizes
}

var fuzzDelims = [][]byte{[]byte("\r\n"), []byte("\n"), []byte("\r\n\r\n"), []byte("aab"), []byte("|")}

func FuzzLineReceiver(f *testing.F) {
	f.Add([]byte("hello\r\nworld\r\n"), uint8(0), uint16(16), []byte{1})
	f.Add([]byte("a\nb\nc"), uint8(1), uint16(1), []byte{0, 2})
	f.Add([]byte("x\r\n\r\ny\r\n\r\n\r"), uint8(2), uint16(4), []byte{3})
	f.Add([]byte("aaab aab aaaab"), uint8(3), uint16(3), []byte{0})
	f.Add([]byte("too long line\r\n"), uint8(0), uint16(4), []byte{})
	f.Add([]byte("|||x|yy|zzz"), uint8(4), uint16(2), []byte{1, 0, 5})
	f.Fuzz(func(t *testing.T, data []byte, delimSel uint8, maxLen uint16, split []byte) {
		d := fuzzDelims[int(delimSel)%len(fuzzDelims)]
		max := int(maxLen%1024) + 1
		want, wantErr := refLines(data, d, max)
		lr := &LineReceiver{Delimiter: d, MaxLineLength: max}
		ft := newFake()
		c := &collector{}
		lr.LineReceived = c.add
		lr.ConnectionMade(ft)
		feed(lr, data, splitSizes(split)...)
		if !framesEqual(c.frames, want) {
			t.Fatalf("lines %q, want %q", c.frames, want)
		}
		if ft.aborted != (wantErr != nil) || !errors.Is(lr.Err(), wantErr) {
			t.Fatalf("aborted %v err %v, want %v", ft.aborted, lr.Err(), wantErr)
		}
		if lr.Err() == nil && len(lr.buf) > max+len(d)-1 {
			t.Fatalf("partial buffer %d exceeds bound", len(lr.buf))
		}
	})
}

func FuzzNetstring(f *testing.F) {
	f.Add([]byte("5:hello,0:,"), uint16(100), []byte{1})
	f.Add([]byte("3:abc,3:de"), uint16(3), []byte{0, 4})
	f.Add([]byte("01:a,"), uint16(10), []byte{})
	f.Add([]byte("10:0123456789,"), uint16(5), []byte{2})
	f.Add([]byte("2:ab;"), uint16(10), []byte{0})
	f.Add([]byte(":x,"), uint16(10), []byte{})
	f.Add([]byte("999999999999999999999999:"), uint16(1000), []byte{7})
	f.Fuzz(func(t *testing.T, data []byte, maxLen uint16, split []byte) {
		max := int(maxLen%4096) + 1
		want, wantErr := refNetstrings(data, max)
		ns := &NetstringReceiver{MaxLength: max}
		ft := newFake()
		c := &collector{}
		ns.StringReceived = c.add
		ns.ConnectionMade(ft)
		feed(ns, data, splitSizes(split)...)
		if !framesEqual(c.frames, want) {
			t.Fatalf("strings %q, want %q", c.frames, want)
		}
		if ft.aborted != (wantErr != nil) || !errors.Is(ns.Err(), wantErr) {
			t.Fatalf("aborted %v err %v, want %v", ft.aborted, ns.Err(), wantErr)
		}
	})
}

func FuzzLengthPrefixed(f *testing.F) {
	f.Add([]byte{0, 0, 0, 3, 'a', 'b', 'c'}, uint8(4), uint8(0), uint16(100), []byte{1})
	f.Add([]byte{3, 'a', 'b', 'c', 0}, uint8(1), uint8(0), uint16(100), []byte{})
	f.Add([]byte{0, 5, 'a', 'b', 'c'}, uint8(2), uint8(2), uint16(100), []byte{0, 1})
	f.Add([]byte{3, 0, 'x', 0, 0}, uint8(2), uint8(1), uint16(100), []byte{2})
	f.Add([]byte{0, 0, 1, 0}, uint8(4), uint8(0), uint16(10), []byte{})
	f.Add([]byte{0, 0, 0, 1}, uint8(4), uint8(2), uint16(10), []byte{0})
	f.Fuzz(func(t *testing.T, data []byte, prefix, flags uint8, maxFrame uint16, split []byte) {
		p := []int{1, 2, 4}[int(prefix)%3]
		le, incl := flags&1 != 0, flags&2 != 0
		max := int(maxFrame) + 1
		want, wantErr := refFrames(data, p, le, incl, max)
		lp := &LengthPrefixed{PrefixLen: p, LittleEndian: le, LengthIncludesPrefix: incl, MaxFrame: max}
		ft := newFake()
		c := &collector{}
		lp.FrameReceived = c.add
		lp.ConnectionMade(ft)
		feed(lp, data, splitSizes(split)...)
		if !framesEqual(c.frames, want) {
			t.Fatalf("frames %q, want %q", c.frames, want)
		}
		if ft.aborted != (wantErr != nil) || !errors.Is(lp.Err(), wantErr) {
			t.Fatalf("aborted %v err %v, want %v", ft.aborted, lp.Err(), wantErr)
		}
		// Round trip: re-encoding the frames and parsing them reproduces them.
		enc := &LengthPrefixed{PrefixLen: p, LittleEndian: le, LengthIncludesPrefix: incl}
		et := newFake()
		enc.ConnectionMade(et)
		for _, fr := range c.frames {
			if err := enc.SendFrame(nil, fr); err != nil {
				t.Fatal(err)
			}
		}
		back, err := refFrames(et.out, p, le, incl, max)
		if err != nil || !framesEqual(back, c.frames) {
			t.Fatalf("round trip mismatch: %v", err)
		}
	})
}
