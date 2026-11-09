package tls

import (
	"testing"
	"time"
)

// TestHandshakeTimeoutDefault: a handshake is never unbounded; d <= 0 means
// DefaultHandshakeTimeout.
func TestHandshakeTimeoutDefault(t *testing.T) {
	for _, tc := range []struct {
		opts []Option
		want time.Duration
	}{
		{nil, DefaultHandshakeTimeout},
		{[]Option{HandshakeTimeout(0)}, DefaultHandshakeTimeout},
		{[]Option{HandshakeTimeout(-time.Second)}, DefaultHandshakeTimeout},
		{[]Option{HandshakeTimeout(3 * time.Second)}, 3 * time.Second},
	} {
		if got := buildOptions(tc.opts).handshakeTimeout; got != tc.want {
			t.Errorf("handshakeTimeout = %v, want %v", got, tc.want)
		}
	}
}
