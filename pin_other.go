//go:build !linux

package twine

// pinToCPU is unsupported on this platform; PinLoops is ignored.
func pinToCPU(int) error { return nil }
