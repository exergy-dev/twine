package twine

import (
	"testing"

	"golang.org/x/sys/unix"
)

// raiseNoFile raises the RLIMIT_NOFILE soft limit to the hard limit where
// the system allows it and returns the resulting soft limit.
//
// The Go runtime already does this at startup (capped on macOS at
// kern.maxfilesperproc, typically 10240), so on most systems this is a
// no-op; it matters when the test binary inherited a soft limit the runtime
// could not raise. Setrlimit fails on macOS when the hard limit is
// RLIM_INFINITY; the limit the runtime set is then the best available.
func raiseNoFile(t testing.TB) uint64 {
	t.Helper()
	var r unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
		t.Fatalf("getrlimit(RLIMIT_NOFILE): %v", err)
	}
	if r.Cur < r.Max {
		want := unix.Rlimit{Cur: r.Max, Max: r.Max}
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &want); err != nil {
			t.Logf("setrlimit(RLIMIT_NOFILE, %d): %v; keeping %d", r.Max, err, r.Cur)
		} else if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
			t.Fatalf("getrlimit(RLIMIT_NOFILE): %v", err)
		}
	}
	t.Logf("RLIMIT_NOFILE: soft %d, hard %d", r.Cur, r.Max)
	return uint64(r.Cur) // int64 on FreeBSD
}

// fdBudgetConns returns how many loopback TCP connections (two fds each,
// client and server side, both in this process) fit under the open-file
// limit after raising it, capped at want. reserve fds are left for the
// engine, the test binary and other tests' leftovers. The test is skipped
// when fewer than atLeast connections fit.
func fdBudgetConns(t testing.TB, want, atLeast int) int {
	t.Helper()
	const reserve = 512
	limit := raiseNoFile(t)
	fit := 0
	if limit > reserve {
		fit = int(min(limit-reserve, uint64(2*want)) / 2)
	}
	if fit < atLeast {
		t.Skipf("RLIMIT_NOFILE soft limit %d fits %d loopback connections, need at least %d", limit, fit, atLeast)
	}
	if fit < want {
		t.Logf("RLIMIT_NOFILE soft limit %d: using %d connections instead of %d", limit, fit, want)
	}
	return fit
}
