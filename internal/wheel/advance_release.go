//go:build !twine_debug

package wheel

// debugState carries nothing in release builds, so the Wheel's layout is
// identical to a build without the re-entrancy check.
type debugState struct{}

// Advance moves the wheel to now and fires every expired timer; see advance.
func (w *Wheel) Advance(now int64) int {
	_ = w.dbg // the field exists for the twine_debug build; this keeps lint quiet here
	return w.advance(now)
}
