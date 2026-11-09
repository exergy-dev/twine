package twine

import (
	"testing"

	"golang.org/x/sys/unix"
)

func iovecsOf(bs ...[]byte) []unix.Iovec {
	iov := make([]unix.Iovec, 0, len(bs))
	for _, b := range bs {
		var v unix.Iovec
		if len(b) > 0 {
			v.Base = &b[0]
		}
		v.SetLen(len(b))
		iov = append(iov, v)
	}
	return iov
}

// writevImpls are the platform's rawWritev and the per-segment fallback
// used on darwin and OpenBSD; both must honour the writev(2) contract the
// write path relies on.
var writevImpls = []struct {
	name string
	fn   func(int, []unix.Iovec) (int, error)
}{{"rawWritev", rawWritev}, {"writevSegments", writevSegments}}

func TestRawWritev(t *testing.T) {
	for _, impl := range writevImpls {
		t.Run(impl.name, func(t *testing.T) { testWritev(t, impl.fn) })
	}
}

func testWritev(t *testing.T, rawWritev func(int, []unix.Iovec) (int, error)) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	if err := unix.SetNonblock(fds[0], true); err != nil {
		t.Fatal(err)
	}

	if n, err := rawWritev(fds[0], nil); n != 0 || err != nil {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
	n, err := rawWritev(fds[0], iovecsOf([]byte("ab"), nil, []byte("cde")))
	if n != 5 || err != nil {
		t.Fatalf("n=%d err=%v, want 5 <nil>", n, err)
	}
	buf := make([]byte, 16)
	m, _ := unix.Read(fds[1], buf)
	if string(buf[:m]) != "abcde" {
		t.Fatalf("peer read %q", buf[:m])
	}

	// Fill the socket: a partial write reports the bytes taken, and a write
	// on a full socket reports -1 and EAGAIN.
	big := make([]byte, 1<<20)
	sawPartial := false
	for i := 0; i < 64; i++ {
		n, err := rawWritev(fds[0], iovecsOf(big, big))
		if n < 0 {
			if err != unix.EAGAIN {
				t.Fatalf("full socket: n=%d err=%v, want -1 EAGAIN", n, err)
			}
			if !sawPartial {
				t.Log("socket filled without a partial write")
			}
			return
		}
		if err != nil {
			t.Fatalf("n=%d with err=%v", n, err)
		}
		if n < 2*len(big) {
			sawPartial = true
		}
	}
	t.Fatal("socket never filled")
}

func TestRawWritevZeroAlloc(t *testing.T) {
	for _, impl := range writevImpls {
		t.Run(impl.name, func(t *testing.T) { testWritevZeroAlloc(t, impl.fn) })
	}
}

func testWritevZeroAlloc(t *testing.T, rawWritev func(int, []unix.Iovec) (int, error)) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	iov := iovecsOf([]byte("hello "), []byte("world"))
	buf := make([]byte, 64)
	allocs := testing.AllocsPerRun(100, func() {
		if n, _ := rawWritev(fds[0], iov); n != 11 {
			t.Fatalf("n=%d", n)
		}
		_, _ = unix.Read(fds[1], buf)
	})
	if allocs != 0 {
		t.Errorf("rawWritev allocates %v", allocs)
	}
}
