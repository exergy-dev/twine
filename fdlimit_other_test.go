//go:build !(darwin || dragonfly || freebsd || netbsd || openbsd)

package twine

// kernelFileLimit reports no cap beyond RLIMIT_NOFILE (Linux enforces the
// rlimit itself; fs.nr_open bounds the hard limit, not the soft one).
func kernelFileLimit() (uint64, string) { return 0, "kernel cap" }
