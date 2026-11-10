//go:build linux && !twine_uring

package poll

import "testing"

// BenchmarkWaitWake measures the epoll backend (see benchWaitWake). The
// twine_uring build defines it instead, comparing io_uring with the epoll
// fallback under the same sub-benchmark names.
func BenchmarkWaitWake(b *testing.B) {
	benchWaitWake(b, "epoll", func(b *testing.B) *Poller {
		p, err := New()
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { p.Close() })
		return p
	}, false)
}
