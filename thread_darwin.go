package twine

import "golang.org/x/sys/unix"

// threadID returns the kernel id of the calling thread (thread_selfid(2),
// the syscall behind pthread_threadid_np). Loops lock their goroutine to one
// thread, so this identifies the loop. x/sys has no libSystem wrapper for it;
// the syscall number has been stable since OS X 10.6.
func threadID() int64 {
	//lint:ignore SA1019 x/sys has no libSystem wrapper for thread_selfid
	id, _, _ := unix.RawSyscall(unix.SYS_THREAD_SELFID, 0, 0, 0) //nolint:staticcheck // as above
	return int64(id)
}

const haveThreadID = true
