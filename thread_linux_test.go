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
