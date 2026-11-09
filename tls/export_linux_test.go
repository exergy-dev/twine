package tls

// CipherUsable reports whether the kernel accepted kTLS keys for the TLS 1.3
// suite on a probe socket (test hook).
func CipherUsable(suite uint16) bool {
	k, _, _, ok := kindFor(suite)
	return ok && cipherUsable(k)
}
