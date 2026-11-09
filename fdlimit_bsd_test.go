//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package twine

import "golang.org/x/sys/unix"

// kernelFileLimit returns the per-process open-file cap the kernel enforces
// on top of RLIMIT_NOFILE (kern.maxfilesperproc on macOS, FreeBSD and
// DragonFly), or 0 where there is none or it cannot be read.
func kernelFileLimit() (uint64, string) {
	const name = "kern.maxfilesperproc"
	n, err := unix.SysctlUint32(name)
	if err != nil {
		return 0, name
	}
	return uint64(n), name
}
