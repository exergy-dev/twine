//go:build !linux

package twine

// threadID is unavailable on this platform; InLoop falls back to goroutine
// id parsing (see goid.go).
func threadID() int64 { return 0 }

const haveThreadID = false

// pinToCPU is unsupported on this platform.
func pinToCPU(int) error { return nil }

func currentCPU() int { return -1 }
