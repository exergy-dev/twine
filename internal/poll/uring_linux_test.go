//go:build linux && twine_uring

package poll

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// envForceEpoll makes the test binary run the whole suite against the epoll
// fallback (see TestEpollFallbackSuite).
const envForceEpoll = "TWINE_POLL_TEST_FORCE_EPOLL"

func TestMain(m *testing.M) {
	if os.Getenv(envForceEpoll) == "1" {
		forceEpoll = true
	}
	os.Exit(m.Run())
}

// requireURing skips the test when the Poller fell back to epoll.
func requireURing(t testing.TB, p *Poller) {
	t.Helper()
	if p.ring == nil {
		if forceEpoll {
			t.Skip("epoll fallback forced")
		}
		t.Skipf("io_uring unavailable, using epoll fallback: %v", p.uringErr)
	}
}

func TestURingBackendSelected(t *testing.T) {
	p := newPoller(t)
	if forceEpoll {
		if p.ring != nil || p.ep == nil || p.Backend() != "epoll" {
			t.Fatalf("forceEpoll set but backend is %q", p.Backend())
		}
		return
	}
	if p.ring == nil {
		if got := p.Backend(); got != "epoll (io_uring unavailable)" {
			t.Errorf("fallback Backend() = %q", got)
		}
		b, _ := os.ReadFile("/proc/sys/kernel/io_uring_disabled")
		t.Logf("io_uring_disabled=%q", strings.TrimSpace(string(b)))
		t.Skipf("io_uring unavailable, epoll fallback in use: %v", p.uringErr)
	}
	if got := p.Backend(); got != "io_uring" {
		t.Fatalf("Backend() = %q, want io_uring", got)
	}
	// open(false) is the fallback path New takes when io_uring fails.
	q, err := open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.uringErr = unix.ENOSYS
	if got := q.Backend(); got != "epoll (io_uring unavailable)" {
		t.Fatalf("fallback Backend() = %q", got)
	}
}

// TestEpollFallbackSuite re-runs every test in this package in a child
// process with the epoll fallback forced, so the fallback path (the epoller
// behind the io_uring Poller) is exercised by the same suite.
func TestEpollFallbackSuite(t *testing.T) {
	if forceEpoll {
		t.Skip("already running against the fallback")
	}
	if testing.Short() {
		t.Skip("skipped in -short mode")
	}
	cmd := exec.Command(os.Args[0], "-test.run=.", "-test.count=1")
	cmd.Env = append(os.Environ(), envForceEpoll+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fallback suite failed: %v\n%s", err, out)
	}
}

func TestURingRegistrationErrors(t *testing.T) {
	p := newPoller(t)
	a, _ := socketpair(t)
	if err := p.Mod(a, Readable); err != unix.ENOENT {
		t.Fatalf("Mod of unregistered fd: %v, want ENOENT", err)
	}
	if err := p.Del(a); err != unix.ENOENT {
		t.Fatalf("Del of unregistered fd: %v, want ENOENT", err)
	}
	if err := p.Add(a, Readable); err != nil {
		t.Fatal(err)
	}
	if err := p.Add(a, Readable); err != unix.EEXIST {
		t.Fatalf("second Add: %v, want EEXIST", err)
	}
	if err := p.Add(-1, Readable); err != unix.EBADF {
		t.Fatalf("Add(-1): %v, want EBADF", err)
	}
}

// TestURingStaleEventsDropped checks that readiness posted for a
// registration that was deleted (and whose fd number is then reused) is never
// reported for the new registration.
func TestURingStaleEventsDropped(t *testing.T) {
	p := newPoller(t)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	a, b := fds[0], fds[1]
	// Ready for writing and hung up: the arm posts Writable|Hangup at once.
	if err := p.Add(a, Readable|Writable); err != nil {
		t.Fatal(err)
	}
	unix.Close(b)
	var evs [8]Event
	if _, err := p.Wait(evs[:], 0); err != nil { // submit; CQEs now queued
		t.Fatal(err)
	}
	if err := p.Del(a); err != nil {
		t.Fatal(err)
	}
	unix.Close(a)
	c, d := socketpair(t) // very likely reuses a's number
	if err := p.Add(c, Readable); err != nil {
		t.Fatal(err)
	}
	if n, err := p.Wait(evs[:], 10*time.Millisecond); n != 0 || err != nil {
		t.Fatalf("Wait after Del+reuse = %d %+v, %v; want nothing (c=%d)", n, evs[:n], err, c)
	}
	write(t, d, "x")
	if f, _ := waitFor(t, p, c, time.Second); f != Readable {
		t.Fatalf("flags = %#x, want Readable only", f)
	}
}

// TestURingCoalesce checks that several CQEs for one fd become one Event.
func TestURingCoalesce(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Readable|Writable); err != nil {
		t.Fatal(err)
	}
	var evs [8]Event
	if _, err := p.Wait(evs[:], 0); err != nil { // submit the arm
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		write(t, b, "x")
	}
	time.Sleep(5 * time.Millisecond)
	n, err := p.Wait(evs[:], time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// epoll reports Readable|Writable here; io_uring reports the wakeup key
	// (Readable) only. Either is valid edge-triggered behaviour.
	if n != 1 || evs[0].FD != int32(a) || evs[0].Flags&Readable == 0 {
		t.Fatalf("Wait = %d %+v; want one Readable event for fd %d", n, evs[:n], a)
	}
}

// TestURingRearmAfterCancel cancels the live multishot poll behind the
// Poller's back, which is what the kernel does on CQ overflow or when the
// submitting thread exits; the Poller must re-arm it transparently.
func TestURingRearmAfterCancel(t *testing.T) {
	p := newPoller(t)
	requireURing(t, p)
	a, b := socketpair(t)
	if err := p.Add(a, Readable); err != nil {
		t.Fatal(err)
	}
	var evs [8]Event
	if _, err := p.Wait(evs[:], 0); err != nil {
		t.Fatal(err)
	}
	r := p.ring
	for _, fd := range []int{a, p.w.fd} {
		if err := r.queuePollRemove(pollUD(fd, r.fds[fd].gen)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.submit(); err != nil {
		t.Fatal(err)
	}
	// Reaps the -ECANCELED CQEs and queues the re-arms. The cancelled wake
	// poll counts as a (spurious) wakeup.
	if n, err := p.Wait(evs[:], 10*time.Millisecond); n != 0 || err != nil {
		t.Fatalf("Wait = %d, %v", n, err)
	}
	write(t, b, "x")
	if f, _ := waitFor(t, p, a, time.Second); f&Readable == 0 {
		t.Fatalf("after re-arm flags = %#x, want Readable", f)
	}
	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		p.Wake()
	}()
	if n, err := p.Wait(evs[:], 5*time.Second); n != 0 || err != nil {
		t.Fatalf("Wait = %d, %v", n, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("wake after re-arm took %v", d)
	}
}

// TestURingAsyncAddError checks that a registration the kernel rejects is
// reported as Error|Hangup by Wait.
func TestURingAsyncAddError(t *testing.T) {
	p := newPoller(t)
	requireURing(t, p)
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	unix.Close(fd) // Add of a closed fd: EBADF arrives as a CQE
	if err := p.Add(fd, Readable); err != nil {
		t.Fatal(err)
	}
	f, _ := waitFor(t, p, fd, time.Second)
	if f != Error|Hangup {
		t.Fatalf("flags = %#x, want Error|Hangup", f)
	}
	if err := p.Del(fd); err != nil {
		t.Fatalf("Del after failed registration: %v", err)
	}
}

// TestURingConnectRefused checks the documented failed-connect mapping.
func TestURingConnectRefused(t *testing.T) {
	// Find a closed port: bind, read the port, close.
	l, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Bind(l, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	sa, _ := unix.Getsockname(l)
	unix.Close(l)

	p := newPoller(t)
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Connect(fd, sa); err != unix.EINPROGRESS && err != unix.ECONNREFUSED {
		t.Fatalf("connect: %v", err)
	}
	if err := p.Add(fd, Writable); err != nil {
		t.Fatal(err)
	}
	f, _ := waitFor(t, p, fd, time.Second)
	if f != Writable|Hangup|Error {
		t.Fatalf("flags = %#x, want Writable|Hangup|Error", f)
	}
}

// TestURingSQOverflowFlush queues more changes than the SQ holds between
// Waits and checks every fd still works.
func TestURingSQOverflowFlush(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3*sqEntries; i++ {
		if err := p.Mod(a, uint32(i&1)*Readable); err != nil {
			t.Fatalf("Mod %d: %v", i, err)
		}
	}
	if err := p.Mod(a, Readable); err != nil {
		t.Fatal(err)
	}
	write(t, b, "x")
	if f, _ := waitFor(t, p, a, time.Second); f != Readable {
		t.Fatalf("flags = %#x, want Readable", f)
	}
}

func TestURingFDTableGrowth(t *testing.T) {
	p := newPoller(t)
	requireURing(t, p)
	a, b := socketpair(t)
	high := initialFDs*2 + 7
	if err := unix.Dup2(a, high); err != nil {
		t.Skipf("dup2 to %d: %v", high, err)
	}
	defer unix.Close(high)
	if err := p.Add(high, Readable); err != nil {
		t.Fatal(err)
	}
	write(t, b, "x")
	if f, _ := waitFor(t, p, high, time.Second); f != Readable {
		t.Fatalf("flags = %#x, want Readable", f)
	}
	if err := p.Del(high); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkWaitWake compares the io_uring backend with the epoll fallback
// (the same code as epoll_linux.go); the sub-benchmarks are described at
// benchWaitWake.
//
// Measured on this host (Linux 6.18 WSL2, i9-14900KF, go1.24,
// -benchtime=1s, several runs; ranges are run-to-run spread, all variants
// 0 allocs/op):
//
//	                     uring              epoll
//	wait0                9-16 ns/op         115-250 ns/op
//	wake+wait            0.7-1.8 µs/op      0.55-1.1 µs/op
//	pingpong             4.8-7 µs/op *      3.6-3.8 µs/op *
//	pingpong-locked      3.9-4.2 µs/op *    3.4-4.5 µs/op *
//	mod+wait             0.44-1.0 µs/op     0.35-0.75 µs/op
//
// wait0 is a memory-only CQ check on io_uring. Same-thread wake+wait and
// mod+wait cost slightly more than epoll because the readiness is delivered
// through task_work and a CQE. Cross-thread wakeup latency (pingpong) is on
// par with epoll when the waiter is the thread that armed the poll (the real
// loop, which calls LockOSThread before New); otherwise the completion is
// posted by task_work on the arming thread, which costs an extra thread
// wakeup. (*) The pingpong numbers are quiet-host figures: while other
// workloads ran on this shared WSL2 host, both backends measured 23-44 µs/op.
func BenchmarkWaitWake(b *testing.B) {
	for _, backend := range []struct {
		name  string
		uring bool
	}{{"uring", true}, {"epoll", false}} {
		benchWaitWake(b, backend.name, func(b *testing.B) *Poller {
			p, err := open(backend.uring)
			if err != nil {
				b.Fatal(err)
			}
			if backend.uring && p.ring == nil {
				p.Close()
				b.Skipf("io_uring unavailable: %v", p.uringErr)
			}
			b.Cleanup(func() { p.Close() })
			return p
		}, true)
	}
}

// tcpPair returns a non-blocking accepted server fd and its client fd.
func tcpPair(t *testing.T) (srv, cli int) {
	t.Helper()
	l, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(l)
	if err := unix.Bind(l, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Listen(l, 1); err != nil {
		t.Fatal(err)
	}
	sa, _ := unix.Getsockname(l)
	cli, err = unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Connect(cli, sa); err != nil {
		t.Fatal(err)
	}
	srv, _, err = unix.Accept4(l, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(cli) })
	return srv, cli
}

// TestURingDelReleasesFile checks that after Del and close(2) the socket is
// really closed (the peer sees EOF) without another Wait, even when the
// registration was still queued in the SQ.
func TestURingDelReleasesFile(t *testing.T) {
	for _, name := range []string{"queued", "submitted"} {
		t.Run(name, func(t *testing.T) {
			p := newPoller(t)
			srv, cli := tcpPair(t)
			if err := p.Add(srv, Readable); err != nil {
				t.Fatal(err)
			}
			if err := p.Mod(srv, Readable|Writable); err != nil {
				t.Fatal(err)
			}
			if name == "submitted" {
				var evs [4]Event
				p.Wait(evs[:], 10*time.Millisecond)
			}
			if err := p.Del(srv); err != nil {
				t.Fatal(err)
			}
			unix.Close(srv)
			tv := unix.NsecToTimeval(int64(2 * time.Second))
			unix.SetsockoptTimeval(cli, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
			start := time.Now()
			var buf [16]byte
			n, err := unix.Read(cli, buf[:])
			if n != 0 || err != nil {
				t.Fatalf("peer read = %d, %v after %v; want EOF", n, err, time.Since(start))
			}
		})
	}
}

// TestURingRemoveEALREADYRetried submits an immediately-ready arm and its
// POLL_REMOVE in one batch (bypassing Mod/Del's in-place elision). The
// kernel answers the remove with -EALREADY and leaves the poll armed; the
// retry (synchronous in Del's submit path, or in the next Wait) must still
// release the file.
func TestURingRemoveEALREADYRetried(t *testing.T) {
	for _, name := range []string{"del", "wait"} {
		t.Run(name, func(t *testing.T) {
			p := newPoller(t)
			requireURing(t, p)
			r := p.ring
			srv, cli := tcpPair(t)
			if err := p.Add(srv, Readable|Writable); err != nil { // writable now
				t.Fatal(err)
			}
			st := &r.fds[srv]
			if err := r.queuePollRemove(pollUD(srv, st.gen)); err != nil {
				t.Fatal(err)
			}
			st.gen = (st.gen + 1) & genMask
			st.state = fdFree
			if name == "del" {
				if err := r.submitRemoves(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := r.submit(); err != nil {
					t.Fatal(err)
				}
				var evs [4]Event
				for i := 0; i < 3; i++ {
					p.Wait(evs[:], 0)
					p.Wait(evs[:], time.Millisecond)
				}
			}
			unix.Close(srv)
			tv := unix.NsecToTimeval(int64(2 * time.Second))
			unix.SetsockoptTimeval(cli, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
			var buf [16]byte
			if n, err := unix.Read(cli, buf[:]); n != 0 || err != nil {
				t.Fatalf("peer read = %d, %v; want EOF (poll still holds the file)", n, err)
			}
		})
	}
}
