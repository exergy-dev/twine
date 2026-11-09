package twine

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// threadID returns the kernel id of the calling thread (thr_self(2)). Loops
// lock their goroutine to one thread, so this identifies the loop.
func threadID() int64 {
	var id int64
	_, _, _ = unix.RawSyscall(unix.SYS_THR_SELF, uintptr(unsafe.Pointer(&id)), 0, 0)
	return id
}

const haveThreadID = true
