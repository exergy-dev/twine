//go:build twine_debug

package wheel

// advancing records that Advance is running, so a re-entrant call from a
// timer's Fn panics instead of corrupting the firing list. It lives outside
// the Wheel struct so the release layout is unchanged.
var advancing = map[*Wheel]bool{}

// Advance moves the wheel to now and fires every expired timer; see advance.
// In the twine_debug build it panics when called from inside a timer's Fn.
func (w *Wheel) Advance(now int64) int {
	if advancing[w] {
		panic("wheel: Advance called from inside a timer's Fn")
	}
	advancing[w] = true
	defer delete(advancing, w) // also clears after a panicking Fn
	return w.advance(now)
}
