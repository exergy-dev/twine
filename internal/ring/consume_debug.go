//go:build twine_debug

package ring

// Consume drops n bytes from the front; see consume. In the twine_debug build
// it verifies the ring's invariants afterwards, including after a panic.
func (r *Ring) Consume(n int) {
	defer r.mustVerify()
	r.consume(n)
}
