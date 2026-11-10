//go:build !linux

package twine

import "testing"

// checkDeferAccept is a no-op: TCP_DEFER_ACCEPT exists only on Linux.
func checkDeferAccept(*testing.T, int) {}
