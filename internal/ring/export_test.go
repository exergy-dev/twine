package ring

// Peek returns the first contiguous readable region (nil when empty). Test
// helper.
func (r *Ring) Peek() []byte {
	s := r.head
	if s == nil || s.n == 0 {
		return nil
	}
	end := min(s.r+s.n, len(s.buf))
	return s.buf[s.r:end:end]
}
