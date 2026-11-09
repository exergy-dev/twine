package sock

import (
	"runtime"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestMmsghdrSize(t *testing.T) {
	want := uintptr(64)
	if unsafe.Sizeof(uintptr(0)) == 4 {
		want = 32
	}
	if got := unsafe.Sizeof(mmsghdr{}); got != want {
		t.Fatalf("sizeof(mmsghdr) = %d on %s, want %d", got, runtime.GOARCH, want)
	}
}

func TestLinuxOptions(t *testing.T) {
	lfd, _ := listen(t, "tcp4", "127.0.0.1:0")
	if err := SetDeferAccept(lfd, 1500*time.Millisecond); err != nil {
		t.Fatalf("SetDeferAccept: %v", err)
	}
	// The kernel converts seconds to retransmit counts and back, so only
	// check that the option is enabled.
	if v, err := unix.GetsockoptInt(lfd, unix.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT); err != nil || v == 0 {
		t.Fatalf("TCP_DEFER_ACCEPT = %d, %v; want enabled", v, err)
	}
	if err := SetDeferAccept(lfd, 0); err != nil {
		t.Fatalf("SetDeferAccept(0): %v", err)
	}
	if err := SetBusyPoll(lfd, 0); err != nil {
		t.Fatalf("SetBusyPoll(0): %v", err)
	}
	if err := SetBusyPoll(lfd, 50*time.Microsecond); err != nil && err != unix.EPERM {
		t.Fatalf("SetBusyPoll(50us): %v", err)
	}
	if err := SetNoDelay(lfd, true); err != nil {
		t.Fatal(err)
	}
	if v, _ := unix.GetsockoptInt(lfd, unix.IPPROTO_TCP, unix.TCP_NODELAY); v == 0 {
		t.Fatal("TCP_NODELAY not set")
	}
	if v, _ := unix.GetsockoptInt(lfd, unix.SOL_SOCKET, unix.SO_REUSEADDR); v == 0 {
		t.Fatal("SO_REUSEADDR not set on listener")
	}
}

func TestMmsgAllocs(t *testing.T) {
	afd, _ := listenUDP(t)
	bfd, baddr := listenUDP(t)
	sb := NewBatch(8, 32)
	for i := range sb.Msgs() {
		m := &sb.Msgs()[i]
		m.Buf = m.Buf[:16]
		m.SetAddr(baddr)
	}
	rb := NewBatch(8, 32)
	if n := testing.AllocsPerRun(100, func() { rb.RecvMmsg(bfd) }); n != 0 {
		t.Errorf("RecvMmsg EAGAIN: %v allocs/op, want 0", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		sb.SendMmsg(afd, 8)
		for {
			if _, err := rb.RecvMmsg(bfd); err != nil {
				break
			}
		}
	}); n != 0 {
		t.Errorf("SendMmsg+RecvMmsg: %v allocs/op, want 0", n)
	}
	if n := testing.AllocsPerRun(100, func() { sb.sendLoop(afd, 8) }); n != 0 {
		t.Errorf("sendLoop: %v allocs/op, want 0", n)
	}
}
