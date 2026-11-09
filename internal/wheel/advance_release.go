//go:build !twine_debug

package wheel

// Advance moves the wheel to now and fires every expired timer; see advance.
func (w *Wheel) Advance(now int64) int { return w.advance(now) }
