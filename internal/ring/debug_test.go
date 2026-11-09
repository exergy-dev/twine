//go:build twine_debug

package ring

import (
	"strings"
	"testing"
)

// mustPanicRing runs f and fails unless it panics with a "ring: " message.
func mustPanicRing(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		msg, _ := recover().(string)
		if !strings.HasPrefix(msg, "ring: ") {
			t.Fatalf("%s: recover() = %q, want a ring invariant panic", what, msg)
		}
	}()
	f()
}

func TestRingInvariantsDebug(t *testing.T) {
	r, _ := newRing()
	r.Write(pattern(0, 100))
	r.Consume(40) // consistent rings pass the checks
	r.length++    // corrupt: Len no longer matches the segment
	mustPanicRing(t, "Write", func() { r.Write([]byte("x")) })

	r, _ = newRing()
	r.Write(pattern(0, 100))
	r.head.n -= 10 // corrupt the segment instead
	mustPanicRing(t, "Consume", func() { r.Consume(1) })
}
