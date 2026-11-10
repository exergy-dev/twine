//go:build race

package tls_test

// raceEnabled reports whether the race detector is on; allocation counts are
// not stable under it.
const raceEnabled = true
