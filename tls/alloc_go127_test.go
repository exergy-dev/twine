//go:build go1.27

package tls_test

// maxReadAllocs is the per-record allocation budget of the read path. From
// Go 1.27 crypto/tls decrypts a record without allocating.
const maxReadAllocs = 0
