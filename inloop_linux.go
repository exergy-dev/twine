package twine

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// threadID returns the kernel thread id of the calling goroutine's thread.
// Loops lock their goroutine to a thread, so this identifies the loop.
func threadID() int64 { return int64(unix.Gettid()) }

const haveThreadID = true

// pinToCPU restricts the calling thread to one CPU.
func pinToCPU(cpu int) error {
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	return unix.SchedSetaffinity(0, &set)
}

// currentCPU returns the CPU the calling thread is running on (tests).
func currentCPU() int {
	var cpu, node uint32
	if _, _, e := unix.Syscall(unix.SYS_GETCPU, uintptr(unsafe.Pointer(&cpu)), uintptr(unsafe.Pointer(&node)), 0); e != 0 {
		return -1
	}
	return int(cpu)
}
