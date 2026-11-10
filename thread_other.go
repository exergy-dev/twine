//go:build !linux && !darwin && !freebsd && !netbsd

package twine

// threadID is not used on this platform (OpenBSD allows system calls only
// through libc, and DragonFly is untested): InLoop falls back to goroutine id
// parsing (see goid.go), which costs about a microsecond per call.
func threadID() int64 { return 0 }

const haveThreadID = false
