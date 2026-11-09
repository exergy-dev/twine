package proto

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/exergy-dev/twine"
)

func newLP(lp *LengthPrefixed) (*fakeTransport, *collector) {
	ft := newFake()
	c := &collector{}
	lp.FrameReceived = c.add
	lp.ConnectionMade(ft)
	return ft, c
}

func TestLengthPrefixedConfigs(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	for _, prefix := range []int{0, 1, 2, 4} {
		for _, le := range []bool{false, true} {
			for _, incl := range []bool{false, true} {
				// Encode random frames with SendFrame, then parse them back.
				enc := &LengthPrefixed{PrefixLen: prefix, LittleEndian: le, LengthIncludesPrefix: incl}
				encT, _ := newLP(enc)
				var want [][]byte
				for k := 0; k < 20; k++ {
					f := make([]byte, rng.IntN(min(enc.MaxSendable(), 300)+1))
					for i := range f {
						f[i] = byte(rng.IntN(256))
					}
					if err := enc.SendFrame(f); err != nil {
						t.Fatal(err)
					}
					want = append(want, f)
				}
				p := enc.prefixLen()
				if ref, err := refFrames(encT.out, p, le, incl, DefaultMaxFrame); err != nil || !framesEqual(ref, want) {
					t.Fatalf("prefix %d le %v incl %v: encoding disagrees with reference (%v)", prefix, le, incl, err)
				}
				for _, sizes := range [][]int{{1}, {2}, {3}, {5, 1}, {100000}} {
					lp := &LengthPrefixed{PrefixLen: prefix, LittleEndian: le, LengthIncludesPrefix: incl}
					ft, c := newLP(lp)
					feed(lp, encT.out, sizes...)
					if ft.aborted || !framesEqual(c.frames, want) {
						t.Fatalf("prefix %d le %v incl %v sizes %v: got %d frames, aborted %v", prefix, le, incl, sizes, len(c.frames), ft.aborted)
					}
				}
			}
		}
	}
}

func TestLengthPrefixedWireFormat(t *testing.T) {
	lp := &LengthPrefixed{PrefixLen: 2}
	ft, _ := newLP(lp)
	_ = lp.SendFrame([]byte("abc"))
	if !bytes.Equal(ft.out, []byte{0, 3, 'a', 'b', 'c'}) || ft.writes != 1 {
		t.Fatalf("BE out %v", ft.out)
	}
	lp = &LengthPrefixed{LittleEndian: true, LengthIncludesPrefix: true}
	ft, _ = newLP(lp)
	_ = lp.SendFrame([]byte("abc"))
	if !bytes.Equal(ft.out, []byte{7, 0, 0, 0, 'a', 'b', 'c'}) {
		t.Fatalf("LE incl out %v", ft.out)
	}
}

func TestLengthPrefixedLimits(t *testing.T) {
	var lost error
	lp := &LengthPrefixed{MaxFrame: 3, OnConnectionLost: func(err error) { lost = err }}
	ft, c := newLP(lp)
	feed(lp, []byte{0, 0, 0, 3, 'a', 'b', 'c', 0, 0, 0, 4, 'x'}, 1)
	if len(c.frames) != 1 || !ft.aborted || !errors.Is(lp.Err(), twine.ErrFrameTooLarge) {
		t.Fatalf("frames %q aborted %v err %v", c.frames, ft.aborted, lp.Err())
	}
	lp.ConnectionLost(nil)
	if !errors.Is(lost, twine.ErrFrameTooLarge) {
		t.Fatalf("OnConnectionLost got %v", lost)
	}

	lp = &LengthPrefixed{PrefixLen: 2, LengthIncludesPrefix: true}
	ft, _ = newLP(lp)
	feed(lp, []byte{0, 1})
	if !ft.aborted || !errors.Is(lp.Err(), ErrMalformedFrame) {
		t.Fatalf("aborted %v err %v", ft.aborted, lp.Err())
	}

	lp = &LengthPrefixed{PrefixLen: 1}
	ft, _ = newLP(lp)
	if err := lp.SendFrame(make([]byte, 256)); !errors.Is(err, twine.ErrFrameTooLarge) || len(ft.out) != 0 {
		t.Fatalf("oversized SendFrame: %v, wrote %d", err, len(ft.out))
	}
	if err := lp.SendFrame(make([]byte, 255)); err != nil {
		t.Fatal(err)
	}
}

func TestLengthPrefixedBadPrefixLen(t *testing.T) {
	for _, p := range []int{-1, 3, 5, 8} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("PrefixLen %d: ConnectionMade did not panic", p)
				}
			}()
			lp := &LengthPrefixed{PrefixLen: p}
			lp.ConnectionMade(newFake())
		}()
	}
	for _, p := range []int{0, 1, 2, 4} {
		lp := &LengthPrefixed{PrefixLen: p}
		lp.ConnectionMade(newFake())
	}
}

func TestLengthPrefixedRandomSplits(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	for iter := 0; iter < 500; iter++ {
		data := make([]byte, rng.IntN(120))
		for i := range data {
			data[i] = byte(rng.IntN(12)) // small values keep lengths plausible
		}
		prefix := []int{1, 2, 4}[rng.IntN(3)]
		le, incl := rng.IntN(2) == 0, rng.IntN(2) == 0
		want, wantErr := refFrames(data, prefix, le, incl, 9)
		for _, sizes := range [][]int{{1}, {3}, {1000}} {
			lp := &LengthPrefixed{PrefixLen: prefix, LittleEndian: le, LengthIncludesPrefix: incl, MaxFrame: 9}
			ft, c := newLP(lp)
			feed(lp, data, sizes...)
			if !framesEqual(c.frames, want) || ft.aborted != (wantErr != nil) || !errors.Is(lp.Err(), wantErr) {
				t.Fatalf("data %v p%d le %v incl %v sizes %v: got %q err %v; want %q err %v", data, prefix, le, incl, sizes, c.frames, lp.Err(), want, wantErr)
			}
		}
	}
}
