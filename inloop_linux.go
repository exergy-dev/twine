package twine

import "golang.org/x/sys/unix"

// threadID returns the kernel thread id of the calling goroutine's thread.
// Loops lock their goroutine to a thread, so this identifies the loop.
func threadID() int64 { return int64(unix.Gettid()) }

const haveThreadID = true
