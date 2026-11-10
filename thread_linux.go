package twine

import "golang.org/x/sys/unix"

// threadID returns the kernel id of the calling thread (gettid(2)). Loops
// lock their goroutine to one thread, so this identifies the loop.
func threadID() int64 { return int64(unix.Gettid()) }

const haveThreadID = true

// pinToCPU restricts the calling thread to one CPU.
func pinToCPU(cpu int) error {
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	return unix.SchedSetaffinity(0, &set)
}
