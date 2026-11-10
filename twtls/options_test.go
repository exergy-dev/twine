package twtls

import (
	"errors"
	"net"
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

func TestHandshakeErrorMessage(t *testing.T) {
	peer := &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
	for _, tc := range []struct {
		err  *HandshakeError
		want string
	}{
		{&HandshakeError{Peer: peer, Err: ErrHandshakeTimeout}, "tls: handshake with 192.0.2.1:443 failed: handshake timeout"},
		{&HandshakeError{Err: errors.New("boom")}, "tls: handshake failed: boom"},
	} {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
		if !errors.Is(tc.err, tc.err.Err) {
			t.Errorf("errors.Is(%v, its Err) = false", tc.err)
		}
	}
}
