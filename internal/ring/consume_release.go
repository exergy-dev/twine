//go:build !twine_debug

package ring

// Consume drops n bytes from the front; see consume.
func (r *Ring) Consume(n int) { r.consume(n) }
