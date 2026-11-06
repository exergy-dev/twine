package twine

import (
	"runtime"
	"strconv"
	"strings"
)

// goid returns the current goroutine's id by parsing runtime.Stack. It is
// slow (~1µs) and allocates; it is used only where threadID is unavailable
// (non-Linux, or LockOSThread(false)).
func goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	s = strings.TrimPrefix(s, "goroutine ")
	if i := strings.IndexByte(s, ' '); i > 0 {
		s = s[:i]
	}
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}
