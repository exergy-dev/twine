package twine

import "golang.org/x/sys/unix"

// threadID returns the id of the calling LWP (_lwp_self(2)). Loops lock their
// goroutine to one thread, so this identifies the loop.
func threadID() int64 {
	id, _, _ := unix.RawSyscall(unix.SYS__LWP_SELF, 0, 0, 0)
	return int64(id)
}

const haveThreadID = true
