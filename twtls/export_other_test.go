//go:build !linux

package twtls

// CipherUsable is false off Linux: there is no kTLS (test hook).
func CipherUsable(uint16) bool { return false }
