package twine

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// currentCPU returns the CPU the calling thread is running on.
func currentCPU() int {
	var cpu, node uint32
	if _, _, e := unix.Syscall(unix.SYS_GETCPU, uintptr(unsafe.Pointer(&cpu)), uintptr(unsafe.Pointer(&node)), 0); e != 0 {
		return -1
	}
	return int(cpu)
}

// checkDeferAccept asserts that TCP_DEFER_ACCEPT is set on the listener fd.
func checkDeferAccept(t *testing.T, fd int) {
	t.Helper()
	v, err := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT)
	if err != nil {
		t.Fatal(err)
	}
	if v <= 0 {
		t.Errorf("TCP_DEFER_ACCEPT = %d, want > 0", v)
	}
}

// TestPinLoops checks that PinLoops lands each loop on the requested CPU.
func TestPinLoops(t *testing.T) {
	e := startEngine(t, Loops(2), PinLoops(0, 1))
	for i, l := range e.loops {
		var cpu int
		onLoop(t, l, func() { cpu = currentCPU() })
		if cpu != i {
			t.Errorf("loop %d runs on cpu %d, want %d", i, cpu, i)
		}
	}
}
