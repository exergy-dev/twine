package twine

import (
	"runtime"
	"sync"
)

// goidBufs recycles the runtime.Stack scratch buffer. runtime.Stack stores
// the slice it is given in the goroutine's g0 (g0.writebuf), so a stack
// array always escapes to the heap; pooling it keeps goid allocation-free in
// steady state.
var goidBufs = sync.Pool{New: func() any { return new([64]byte) }}

// goid returns the current goroutine's id by parsing the "goroutine N "
// header that runtime.Stack writes. It costs ~1µs and does not allocate in
// steady state. The watchdog uses it to find the loop goroutine in a full
// stack dump. It is also InLoop's fallback where threadID is unavailable
// (OpenBSD, DragonFly), and there it sits on the CallFromThread path, so it
// must stay allocation-free (TestGoidZeroAlloc).
func goid() int64 {
	buf := goidBufs.Get().(*[64]byte)
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	var id int64
	if n > len(prefix) && string(buf[:len(prefix)]) == prefix {
		for _, c := range buf[len(prefix):n] {
			if c < '0' || c > '9' {
				break
			}
			id = id*10 + int64(c-'0')
		}
	}
	goidBufs.Put(buf)
	return id
}
