package twine

import (
	"os"
	"runtime"
)

// debugEnabled is true when TWINE_DEBUG=1 is set at startup or the binary
// was built with -tags twine_debug. Debug builds assert loop affinity on
// every Transport/Timer method, poison the read buffer after each
// DataReceived, and capture Future creation stacks.
var debugEnabled = debugBuildTag || os.Getenv("TWINE_DEBUG") == "1"

// debugFutures enables Future creation-stack capture and unhandled-rejection
// logging independently of the other debug checks.
var debugFutures = debugEnabled || os.Getenv("TWINE_DEBUG_FUTURES") == "1"

// assertInLoop panics with the caller's stack when called off the loop in a
// debug build. It is small enough to inline, so release builds pay one load
// and branch per Transport method instead of a call.
func (l *Loop) assertInLoop() {
	if debugEnabled {
		l.checkInLoop()
	}
}

// checkInLoop is the out-of-line half of assertInLoop.
func (l *Loop) checkInLoop() {
	if l == nil || l.InLoop() {
		return
	}
	buf := make([]byte, 8192)
	n := runtime.Stack(buf, false)
	panic(&PanicError{Value: ErrNotInLoop, Stack: buf[:n]})
}

// poison overwrites b so that a retained read-buffer slice is noticed.
func poison(b []byte) {
	for i := range b {
		b[i] = 0xDE
	}
}
