package poll

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// socketpair returns a connected, non-blocking AF_UNIX stream pair that is
// closed at the end of the test.
func socketpair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	for _, fd := range fds {
		if err := unix.SetNonblock(fd, true); err != nil {
			t.Fatalf("setnonblock: %v", err)
		}
	}
	t.Cleanup(func() {
		unix.Close(fds[0])
		unix.Close(fds[1])
	})
	return fds[0], fds[1]
}

func newPoller(t *testing.T) *Poller {
	t.Helper()
	p, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func write(t *testing.T, fd int, s string) {
	t.Helper()
	if _, err := unix.Write(fd, []byte(s)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// drainFD reads fd until EAGAIN.
func drainFD(fd int) {
	var buf [4096]byte
	for {
		n, err := unix.Read(fd, buf[:])
		if n <= 0 || err != nil {
			return
		}
	}
}

// waitFor waits up to 1s and returns the merged flags reported for fd and
// the total number of events returned.
func waitFor(t *testing.T, p *Poller, fd int, timeout time.Duration) (uint32, int) {
	t.Helper()
	var evs [16]Event
	n, err := p.Wait(evs[:], timeout)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	var f uint32
	for _, ev := range evs[:n] {
		if ev.FD == int32(fd) {
			f |= ev.Flags
		}
	}
	return f, n
}

func TestReadable(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if f, n := waitFor(t, p, a, 0); n != 0 {
		t.Fatalf("idle Wait(0) = %d events (flags %#x), want 0", n, f)
	}
	write(t, b, "hello")
	f, _ := waitFor(t, p, a, time.Second)
	if f&Readable == 0 {
		t.Fatalf("flags = %#x, want Readable", f)
	}
	if f&(Hangup|Error) != 0 {
		t.Fatalf("flags = %#x, unexpected Hangup/Error", f)
	}
}

func TestWritable(t *testing.T) {
	p := newPoller(t)
	a, _ := socketpair(t)
	if err := p.Add(a, Readable|Writable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	f, _ := waitFor(t, p, a, time.Second)
	if f != Writable {
		t.Fatalf("flags = %#x, want Writable only", f)
	}
}

func TestEdgeTriggered(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	write(t, b, "one")
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("first Wait flags = %#x, want Readable", f)
	}
	// Data is still unread, but edge-triggered means no new event.
	for i := 0; i < 3; i++ {
		if _, n := waitFor(t, p, a, 0); n != 0 {
			t.Fatalf("Wait(0) after edge = %d events, want 0", n)
		}
	}
	write(t, b, "two")
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("Wait after new data flags = %#x, want Readable", f)
	}
	if _, n := waitFor(t, p, a, 0); n != 0 {
		t.Fatalf("Wait(0) after second edge = %d events, want 0", n)
	}
}

func TestModTogglesReadable(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, 0); err != nil {
		t.Fatalf("Add: %v", err)
	}
	write(t, b, "x")
	if _, n := waitFor(t, p, a, 0); n != 0 {
		t.Fatalf("no interest: Wait(0) = %d events, want 0", n)
	}
	// Enabling Readable while data is already queued must report it.
	if err := p.Mod(a, Readable); err != nil {
		t.Fatalf("Mod(Readable): %v", err)
	}
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("after Mod(Readable) flags = %#x, want Readable", f)
	}
	drainFD(a)
	// Disable again: new data must not be reported.
	if err := p.Mod(a, 0); err != nil {
		t.Fatalf("Mod(0): %v", err)
	}
	write(t, b, "y")
	if f, n := waitFor(t, p, a, 0); n != 0 {
		t.Fatalf("after Mod(0) Wait = %d events (flags %#x), want 0", n, f)
	}
	// Mod to Writable only: Writable reported, Readable not.
	if err := p.Mod(a, Writable); err != nil {
		t.Fatalf("Mod(Writable): %v", err)
	}
	if f, _ := waitFor(t, p, a, time.Second); f != Writable {
		t.Fatalf("after Mod(Writable) flags = %#x, want Writable", f)
	}
	// Mod twice with the same set is fine.
	if err := p.Mod(a, Readable|Writable); err != nil {
		t.Fatalf("Mod(R|W): %v", err)
	}
	if err := p.Mod(a, Readable|Writable); err != nil {
		t.Fatalf("Mod(R|W) again: %v", err)
	}
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("after Mod(R|W) flags = %#x, want Readable", f)
	}
}

func TestDel(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable|Writable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := p.Del(a); err != nil {
		t.Fatalf("Del: %v", err)
	}
	write(t, b, "x")
	if f, n := waitFor(t, p, a, 0); n != 0 {
		t.Fatalf("after Del Wait = %d events (flags %#x), want 0", n, f)
	}
	// Re-adding after Del works.
	if err := p.Add(a, Readable); err != nil {
		t.Fatalf("re-Add: %v", err)
	}
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("after re-Add flags = %#x, want Readable", f)
	}
}

func TestHangup(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	unix.Close(b)
	f, _ := waitFor(t, p, a, time.Second)
	if f&Hangup == 0 || f&Readable == 0 {
		t.Fatalf("after peer close flags = %#x, want Readable|Hangup", f)
	}
}

func TestWakeUnblocksWait(t *testing.T) {
	p := newPoller(t)
	var woke time.Time
	var mu sync.Mutex
	go func() {
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		woke = time.Now()
		mu.Unlock()
		if err := p.Wake(); err != nil {
			t.Errorf("Wake: %v", err)
		}
	}()
	var evs [8]Event
	n, err := p.Wait(evs[:], 5*time.Second)
	returned := time.Now()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 0 {
		t.Fatalf("Wait after Wake = %d events, want 0", n)
	}
	mu.Lock()
	lat := returned.Sub(woke)
	mu.Unlock()
	if woke.IsZero() {
		t.Fatalf("Wait returned before Wake")
	}
	if lat > 10*time.Millisecond {
		t.Fatalf("Wait returned %v after Wake, want <= 10ms", lat)
	}
	// The wakeup was consumed: the next poll sees nothing.
	if n, err := p.Wait(evs[:], 0); n != 0 || err != nil {
		t.Fatalf("Wait(0) after consumed wake = %d, %v; want 0, nil", n, err)
	}
}

func TestWakeBlockingForever(t *testing.T) {
	p := newPoller(t)
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Wake()
	}()
	var evs [8]Event
	if n, err := p.Wait(evs[:], -1); n != 0 || err != nil {
		t.Fatalf("Wait(-1) = %d, %v; want 0, nil", n, err)
	}
}

func TestWakeCoalesces(t *testing.T) {
	p := newPoller(t)
	for i := 0; i < 1000; i++ {
		if err := p.Wake(); err != nil {
			t.Fatalf("Wake %d: %v", i, err)
		}
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if err := p.Wake(); err != nil {
					t.Errorf("concurrent Wake: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	var evs [8]Event
	if n, err := p.Wait(evs[:], time.Second); n != 0 || err != nil {
		t.Fatalf("Wait after 9000 wakes = %d, %v; want 0, nil", n, err)
	}
	if n, err := p.Wait(evs[:], 0); n != 0 || err != nil {
		t.Fatalf("second Wait(0) = %d, %v; want 0, nil", n, err)
	}
	// A wakeup still works after the burst was drained.
	if err := p.Wake(); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	start := time.Now()
	if n, err := p.Wait(evs[:], 5*time.Second); n != 0 || err != nil {
		t.Fatalf("Wait = %d, %v; want 0, nil", n, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Wait after Wake took %v", d)
	}
}

func TestWakeWithEvents(t *testing.T) {
	// The wake fd must not be reported even alongside real events.
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatalf("Add: %v", err)
	}
	write(t, b, "x")
	p.Wake()
	var evs [8]Event
	n, err := p.Wait(evs[:], time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n != 1 || evs[0].FD != int32(a) || evs[0].Flags&Readable == 0 {
		t.Fatalf("Wait = %d %+v; want exactly fd %d Readable", n, evs[:n], a)
	}
}

func TestWaitTimeout(t *testing.T) {
	p := newPoller(t)
	var evs [8]Event
	start := time.Now()
	n, err := p.Wait(evs[:], 30*time.Millisecond)
	d := time.Since(start)
	if n != 0 || err != nil {
		t.Fatalf("Wait = %d, %v; want 0, nil", n, err)
	}
	if d < 25*time.Millisecond {
		t.Fatalf("Wait(30ms) returned after %v", d)
	}
	// A sub-millisecond timeout must still return promptly.
	start = time.Now()
	if n, err := p.Wait(evs[:], time.Nanosecond); n != 0 || err != nil {
		t.Fatalf("Wait(1ns) = %d, %v", n, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Wait(1ns) took %v", d)
	}
}

func TestWaitEmptySlice(t *testing.T) {
	p := newPoller(t)
	if _, err := p.Wait(nil, 0); err == nil {
		t.Fatal("Wait(nil) succeeded, want EINVAL")
	}
}

func TestManyFDs(t *testing.T) {
	// More ready fds than the evs slice: every fd is reported across Waits.
	p := newPoller(t)
	const nfd = 40
	want := map[int32]bool{}
	for i := 0; i < nfd; i++ {
		a, b := socketpair(t)
		if err := p.Add(a, Readable); err != nil {
			t.Fatalf("Add: %v", err)
		}
		write(t, b, "x")
		want[int32(a)] = true
	}
	var evs [7]Event
	for tries := 0; len(want) > 0 && tries < 100; tries++ {
		n, err := p.Wait(evs[:], time.Second)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if n > len(evs) {
			t.Fatalf("Wait returned %d > len(evs)", n)
		}
		for _, ev := range evs[:n] {
			if ev.Flags&Readable != 0 {
				delete(want, ev.FD)
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("%d fds never reported", len(want))
	}
}

func TestCloseIdempotent(t *testing.T) {
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestBackend(t *testing.T) {
	switch b := newPoller(t).Backend(); b {
	case "epoll", "kqueue", "io_uring", "epoll (io_uring unavailable)":
		t.Logf("backend: %s", b)
	default:
		t.Fatalf("Backend() = %q", b)
	}
}

func TestAllocs(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatal(err)
	}
	evs := make([]Event, 64)
	check := func(name string, f func()) {
		t.Helper()
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s: %v allocs/op, want 0", name, n)
		}
	}
	check("Wait(0)", func() { p.Wait(evs, 0) })
	check("Wake", func() { p.Wake() })
	check("Wake+Wait(-1)", func() {
		p.Wake()
		p.Wait(evs, -1)
	})
	check("Wake+Wait(1s)", func() {
		p.Wake()
		p.Wait(evs, time.Second)
	})
	check("Wait(1ns)", func() { p.Wait(evs, time.Nanosecond) })
	buf := []byte("x")
	check("readable Wait", func() {
		unix.Write(b, buf)
		p.Wait(evs, time.Second)
	})
	check("Mod", func() { p.Mod(a, Readable|Writable) })
	check("Del+Add", func() {
		p.Del(a)
		p.Add(a, Readable)
	})
}
