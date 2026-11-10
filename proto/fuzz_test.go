package proto

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
			if err := enc.SendFrame(fr); err != nil {
				t.Fatal(err)
			}
		}
		back, err := refFrames(et.out, p, le, incl, max)
		if err != nil || !framesEqual(back, c.frames) {
			t.Fatalf("round trip mismatch: %v", err)
		}
	})
}

// ampFuzzHandler picks the handler outcome from the command's first byte:
// 0 unhandled, 1 failed with a *RemoteError, 2 failed with a plain error,
// 3 answered with the request's own args.
func ampFuzzHandler(cmd string, args Args, reply func(Args, error)) {
	switch ampFuzzMode(cmd) {
	case 1:
		reply(nil, &RemoteError{Code: "FUZZ", Description: cmd})
	case 2:
		reply(nil, errors.New(cmd))
	case 3:
		reply(args, nil)
	default:
		reply(nil, ErrUnhandledCommand)
	}
}

func ampFuzzMode(cmd string) int {
	if cmd == "" {
		return 0
	}
	return int(cmd[0]) % 4
}

// refAMP is the oracle: it reframes data as 2-byte-prefixed strings,
// assembles boxes with the same limits AMP applies, and returns the reply
// each complete request should produce (requests with _ask only; no calls
// are pending, so answer and error boxes are dropped) and the error the
// connection must abort with.
func refAMP(data []byte, maxBox int) ([]map[string]string, error) {
	frames, err := refFrames(data, 2, false, false, ampMaxValueLen)
	if err != nil {
		panic(err) // a 2-byte prefix cannot exceed 65535
	}
	var replies []map[string]string
	box := map[string]string{}
	key, haveKey, size := "", false, 0
	for _, f := range frames {
		size += 2 + len(f)
		if size > maxBox {
			return replies, ErrFrameTooLarge
		}
		switch {
		case haveKey:
			box[key] = string(f)
			haveKey = false
		case len(f) == 0:
			if r := refReply(box); r != nil {
				replies = append(replies, r)
			}
			box, size = map[string]string{}, 0
		case len(f) > ampMaxKeyLen:
			return replies, ErrFrameTooLarge
		default:
			key, haveKey = string(f), true
		}
	}
	return replies, nil
}

func refReply(box map[string]string) map[string]string {
	if _, ok := box["_answer"]; ok {
		return nil
	}
	if _, ok := box["_error"]; ok {
		return nil
	}
	cmd, ok := box["_command"]
	if !ok {
		return nil
	}
	tag, ok := box["_ask"]
	if !ok {
		return nil
	}
	errBox := func(code, desc string) map[string]string {
		return map[string]string{"_error": tag, "_error_code": code, "_error_description": desc}
	}
	switch ampFuzzMode(cmd) {
	case 1:
		return errBox("FUZZ", cmd)
	case 2:
		return errBox(UnknownErrorCode, cmd)
	case 3:
		r := map[string]string{"_answer": tag}
		for k, v := range box {
			switch k {
			case "_command", "_ask":
			case "_error_code", "_error_description":
				// Echoing a reserved key is invalid: the reply is an
				// UNKNOWN error naming one of them (map order picks which).
				return errBox(UnknownErrorCode, "reserved key")
			default:
				r[k] = v
			}
		}
		return r
	}
	return errBox(UnhandledErrorCode, "Unhandled Command: "+strconv.Quote(cmd))
}

func FuzzAMP(f *testing.F) {
	f.Add(twistedBox("_ask", "1", "_command", "\x03echo", "a", "1"), uint16(0), []byte{1})
	f.Add(append(twistedBox("_ask", "a", "_command", "\x00x"), twistedBox("_ask", "b", "_command", "\x01y")...), uint16(0), []byte{0, 3})
	f.Add(twistedBox("_command", "\x02none", "k", "v"), uint16(0), []byte{})
	f.Add(twistedBox("_answer", "1", "x", "y"), uint16(0), []byte{2})
	f.Add(twistedBox("_ask", "1", "_command", "\x03", "_error_code", "x"), uint16(0), []byte{})
	f.Add(twistedBox("_ask", "1", "_command", "Sum", "big", strings.Repeat("v", 200)), uint16(64), []byte{5})
	f.Add(twistedBox(strings.Repeat("k", 256), "v"), uint16(0), []byte{})
	f.Add([]byte{0, 3, 'a', 'b'}, uint16(0), []byte{0})
	f.Fuzz(func(t *testing.T, data []byte, maxBox uint16, split []byte) {
		limit := int(maxBox)
		if limit == 0 {
			limit = DefaultMaxBoxSize
		}
		want, wantErr := refAMP(data, limit)

		var lost error
		a := &AMP{Handle: ampFuzzHandler, MaxBoxSize: int(maxBox), OnConnectionLost: func(err error) { lost = err }}
		ft := newFake()
		a.ConnectionMade(ft)
		feed(a, data, splitSizes(split)...)

		if ft.aborted != (a.Err() != nil) {
			t.Fatalf("aborted %v but Err %v", ft.aborted, a.Err())
		}
		if a.Err() != nil && !errors.Is(a.Err(), ErrFrameTooLarge) {
			t.Fatalf("Err %v, want nil or ErrFrameTooLarge", a.Err())
		}
		if a.Err() != wantErr {
			t.Fatalf("Err %v, want %v", a.Err(), wantErr)
		}
		got := decodeBoxes(t, ft.out)
		if len(got) != len(want) {
			t.Fatalf("%d replies, want %d:\n got %v\nwant %v", len(got), len(want), got, want)
		}
		for i := range want {
			g, w := got[i], want[i]
			if w["_error_description"] == "reserved key" {
				if g["_error"] != w["_error"] || g["_error_code"] != UnknownErrorCode ||
					!strings.Contains(g["_error_description"], "reserved key") || len(g) != 3 {
					t.Fatalf("reply %d: %v, want a reserved-key error", i, g)
				}
				continue
			}
			if fmt.Sprint(g) != fmt.Sprint(w) {
				t.Fatalf("reply %d:\n got %v\nwant %v", i, g, w)
			}
		}
		a.ConnectionLost(nil)
		if lost != a.Err() {
			t.Fatalf("OnConnectionLost(%v), want %v", lost, a.Err())
		}
	})
}

// rawEvent is a delivered line, or the raw bytes received between a line
// "R" (which switches to raw mode) and the next '!' (which switches back).
type rawEvent struct {
	raw bool
	b   []byte
}

func (e rawEvent) String() string {
	if e.raw {
		return fmt.Sprintf("raw%q", e.b)
	}
	return fmt.Sprintf("line%q", e.b)
}

// refLinesRaw is the whole-buffer oracle for FuzzLineReceiverRaw.
func refLinesRaw(data, d []byte, max int) ([]rawEvent, error) {
	var ev []rawEvent
	for {
		i := bytes.Index(data, d)
		if i < 0 {
			if len(data) > max+len(d)-1 {
				return ev, ErrLineTooLong
			}
			return ev, nil
		}
		if i > max {
			return ev, ErrLineTooLong
		}
		line := data[:i]
		data = data[i+len(d):]
		ev = append(ev, rawEvent{b: line})
		if string(line) != "R" {
			continue
		}
		j := bytes.IndexByte(data, '!')
		if j < 0 {
			return append(ev, rawEvent{raw: true, b: data}), nil
		}
		ev = append(ev, rawEvent{raw: true, b: data[:j]})
		data = data[j+1:]
	}
}

func FuzzLineReceiverRaw(f *testing.F) {
	f.Add([]byte("a\r\nR\r\nraw bytes\r\n!b\r\n"), uint8(0), uint16(16), []byte{1})
	f.Add([]byte("R\nxx!R\n!y\nR\nz"), uint8(1), uint16(4), []byte{0, 2})
	f.Add([]byte("R|!!|R|abc"), uint8(4), uint16(1), []byte{3})
	f.Add([]byte("Raab12!longline toolong aab"), uint8(3), uint16(4), []byte{})
	f.Add([]byte("R\r\n\r\n!\r\n\r\nR\r\n\r\nx!"), uint8(2), uint16(8), []byte{0, 1, 5})
	f.Fuzz(func(t *testing.T, data []byte, delimSel uint8, maxLen uint16, split []byte) {
		d := fuzzDelims[int(delimSel)%len(fuzzDelims)]
		max := int(maxLen%1024) + 1
		want, wantErr := refLinesRaw(data, d, max)

		lr := &LineReceiver{Delimiter: d, MaxLineLength: max}
		var got []rawEvent
		lr.LineReceived = func(line []byte) {
			got = append(got, rawEvent{b: bytes.Clone(line)})
			if string(line) == "R" {
				got = append(got, rawEvent{raw: true, b: []byte{}})
				lr.SetRawMode()
			}
		}
		lr.RawDataReceived = func(b []byte) {
			last := &got[len(got)-1]
			if j := bytes.IndexByte(b, '!'); j >= 0 {
				last.b = append(last.b, b[:j]...)
				lr.SetLineMode(b[j+1:])
				return
			}
			last.b = append(last.b, b...)
		}
		ft := newFake()
		lr.ConnectionMade(ft)
		feed(lr, data, splitSizes(split)...)

		if len(got) != len(want) {
			t.Fatalf("events %v, want %v", got, want)
		}
		for i := range want {
			if got[i].raw != want[i].raw || !bytes.Equal(got[i].b, want[i].b) {
				t.Fatalf("events %v, want %v", got, want)
			}
		}
		if ft.aborted != (wantErr != nil) || !errors.Is(lr.Err(), wantErr) {
			t.Fatalf("aborted %v err %v, want %v", ft.aborted, lr.Err(), wantErr)
		}
	})
}
