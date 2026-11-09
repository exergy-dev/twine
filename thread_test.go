package twine

import (
	"runtime"
	"testing"
)

// threadID must identify the locked loop thread on every platform that has
// one, and differ for another locked thread.
func TestThreadID(t *testing.T) {
	if !haveThreadID {
		t.Skip("no kernel thread id on this platform; InLoop uses goid")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	a := threadID()
	if a <= 0 {
		t.Fatalf("threadID() = %d", a)
	}
	if b := threadID(); b != a {
		t.Fatalf("threadID() changed on a locked thread: %d then %d", a, b)
	}
	other := make(chan int64)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		other <- threadID()
	}()
	if b := <-other; b == a {
		t.Fatalf("two locked threads share threadID %d", a)
	}
}

// InLoop is true on the loop and false elsewhere, whichever identity the
// platform uses.
func TestInLoop(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	if l.InLoop() {
		t.Fatal("InLoop true on the test goroutine")
	}
	var in bool
	onLoop(t, l, func() { in = l.InLoop() })
	if !in {
		t.Fatal("InLoop false on the loop")
	}
}
