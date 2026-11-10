//go:build race

package twtls_test

// raceEnabled reports whether the race detector is on; allocation counts are
// not stable under it.
const raceEnabled = true
