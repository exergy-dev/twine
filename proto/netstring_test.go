package proto

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/exergy-dev/twine"
)

func newNS(ns *NetstringReceiver) (*fakeTransport, *collector) {
	ft := newFake()
	c := &collector{}
	ns.StringReceived = c.add
	ns.ConnectionMade(ft)
	return ft, c
}

func TestNetstringBasic(t *testing.T) {
	in := []byte("5:hello,0:,12:hello world!,")
	want := [][]byte{[]byte("hello"), {}, []byte("hello world!")}
	for _, sizes := range [][]int{{1000}, {1}, {2}, {3}, {5, 1}} {
		ns := &NetstringReceiver{}
		ft, c := newNS(ns)
		feed(ns, in, sizes...)
		if !framesEqual(c.frames, want) || ft.aborted {
			t.Fatalf("sizes %v: got %q aborted %v", sizes, c.frames, ft.aborted)
		}
	}
}

func TestNetstringMalformed(t *testing.T) {
	cases := []struct {
		in   string
		want error
		good int
	}{
		{":", ErrMalformedNetstring, 0},
		{"a:", ErrMalformedNetstring, 0},
		{"3x", ErrMalformedNetstring, 0},
		{"01:a,", ErrMalformedNetstring, 0},
		{"00:,", ErrMalformedNetstring, 0},
		{"3:abc;", ErrMalformedNetstring, 0},
		{"1:a,2:abc,", ErrMalformedNetstring, 1},
		{"-1:", ErrMalformedNetstring, 0},
		{"11:", twine.ErrFrameTooLarge, 0},
		{"99999999999999999999999999:", twine.ErrFrameTooLarge, 0},
	}
	for _, tc := range cases {
		for _, sizes := range [][]int{{1000}, {1}} {
			var lost error
			ns := &NetstringReceiver{MaxLength: 10, OnConnectionLost: func(err error) { lost = err }}
			ft, c := newNS(ns)
			feed(ns, []byte(tc.in), sizes...)
			if !ft.aborted || !errors.Is(ns.Err(), tc.want) || len(c.frames) != tc.good {
				t.Fatalf("%q sizes %v: aborted %v err %v frames %q", tc.in, sizes, ft.aborted, ns.Err(), c.frames)
			}
			ns.ConnectionLost(nil)
			if !errors.Is(lost, tc.want) {
				t.Fatalf("%q: OnConnectionLost got %v", tc.in, lost)
			}
		}
	}
}

func TestNetstringRandomSplits(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for iter := 0; iter < 500; iter++ {
		var data []byte
		for k := rng.IntN(6); k > 0; k-- {
			p := make([]byte, rng.IntN(30))
			for i := range p {
				p[i] = byte(rng.IntN(256))
			}
			data = strconv.AppendInt(data, int64(len(p)), 10)
			data = append(data, ':')
			data = append(data, p...)
			data = append(data, ',')
		}
		if rng.IntN(3) == 0 && len(data) > 0 {
			data[rng.IntN(len(data))] = "0:,x9"[rng.IntN(5)] // corrupt
		}
		data = data[:rng.IntN(len(data)+1)]
		want, wantErr := refNetstrings(data, 20)
		for _, sizes := range [][]int{{1}, {2}, {7}, {1000}} {
			ns := &NetstringReceiver{MaxLength: 20}
			ft, c := newNS(ns)
			feed(ns, data, sizes...)
			if !framesEqual(c.frames, want) || ft.aborted != (wantErr != nil) || !errors.Is(ns.Err(), wantErr) {
				t.Fatalf("data %q sizes %v: got %q err %v; want %q err %v", data, sizes, c.frames, ns.Err(), want, wantErr)
			}
		}
	}
}

func TestNetstringSendString(t *testing.T) {
	ns := &NetstringReceiver{}
	ft, _ := newNS(ns)
	ns.SendString(nil, []byte("hello"))
	ns.SendString(ft, nil)
	if string(ft.out) != "5:hello,0:," || ft.writes != 2 {
		t.Fatalf("out %q writes %d", ft.out, ft.writes)
	}
}
