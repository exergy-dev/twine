//go:build twine_debug

package slab

import "testing"

// mustPanic runs f and fails unless it panics with want.
func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if got := recover(); got != want {
			t.Fatalf("panic = %v, want %q", got, want)
		}
	}()
	f()
}

func TestPutTwicePanics(t *testing.T) {
	a := New()
	b := a.Get(100)
	other := a.Get(100)
	a.Put(b)
	mustPanic(t, "slab: double Put", func() { a.Put(b) })
	// A short reslice of the same slab is the same slab.
	mustPanic(t, "slab: double Put", func() { a.Put(b[:10]) })
	a.Put(other) // a different slab of the same class is fine
	if s := a.Stats(); s.Free != 2*class4K || s.InUse != 0 {
		t.Fatalf("stats after rejected double Puts: %+v", s)
	}
}

func TestGetOversizePanicsDebug(t *testing.T) {
	mustPanic(t, "slab: Get larger than MaxClass", func() { New().Get(MaxClass + 1) })
}
