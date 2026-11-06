//go:build linux && !twine_uring

package poll

import (
	"math"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDurationToMs(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want int
	}{
		{-1, -1},
		{-time.Hour, -1},
		{0, 0},
		{1, 1},
		{time.Microsecond, 1},
		{time.Millisecond, 1},
		{time.Millisecond + 1, 2},
		{1500 * time.Microsecond, 2},
		{time.Second, 1000},
		{math.MaxInt64, math.MaxInt32},
		{time.Duration(math.MaxInt32+1) * time.Millisecond, math.MaxInt32},
	}
	for _, c := range cases {
		if got := durationToMs(c.d); got != c.want {
			t.Errorf("durationToMs(%v) = %d, want %d", c.d, got, c.want)
		}
	}
}

func TestEventfdDrained(t *testing.T) {
	p := newPoller(t)
	for i := 0; i < 5; i++ {
		p.Wake()
	}
	var evs [4]Event
	if n, err := p.Wait(evs[:], time.Second); n != 0 || err != nil {
		t.Fatalf("Wait = %d, %v", n, err)
	}
	var buf [8]byte
	if _, err := unix.Read(p.w.fd, buf[:]); err != unix.EAGAIN {
		t.Fatalf("eventfd read after Wait: err = %v, want EAGAIN (drained)", err)
	}
}

func TestRDHupWithoutReadInterest(t *testing.T) {
	// A half-close is reported even when read interest is paused.
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Shutdown(b, unix.SHUT_WR); err != nil {
		t.Fatal(err)
	}
	f, _ := waitFor(t, p, a, time.Second)
	if f&Hangup == 0 {
		t.Fatalf("flags = %#x, want Hangup", f)
	}
}
