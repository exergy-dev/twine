//go:build !go1.27

package twtls_test

// maxReadAllocs is the per-record allocation budget of the read path. Before
// Go 1.27 crypto/tls allocates in Conn.Read (two allocations per record).
const maxReadAllocs = 2
