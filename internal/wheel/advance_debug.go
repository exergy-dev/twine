//go:build twine_debug

package wheel

// debugState records that Advance is running, so a re-entrant call from a
// timer's Fn panics instead of corrupting the firing list. A Wheel belongs to
// one loop, so the flag needs no synchronisation.
type debugState struct{ advancing bool }

// Advance moves the wheel to now and fires every expired timer; see advance.
// In the twine_debug build it panics when called from inside a timer's Fn.
func (w *Wheel) Advance(now int64) int {
	if w.dbg.advancing {
		panic("wheel: Advance called from inside a timer's Fn")
	}
	w.dbg.advancing = true
	defer func() { w.dbg.advancing = false }() // also clears after a panicking Fn
	return w.advance(now)
}
