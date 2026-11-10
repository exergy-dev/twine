package twine

import (
	"testing"

	"golang.org/x/sys/unix"
)

// raiseNoFile raises the RLIMIT_NOFILE soft limit as far as the system
// allows and returns the number of fds this process can actually open.
//
// The Go runtime already raises the soft limit to the hard limit at startup,
// so this is usually a no-op; it matters when the test binary inherited a
// soft limit the runtime could not raise. On macOS getrlimit can report
// RLIM_INFINITY while the kernel enforces kern.maxfilesperproc (the first
// macOS CI run with this helper logged soft = hard = 2^63-1 and still hit
// EMFILE), so the sysctl cap, where there is one, bounds the result.
func raiseNoFile(t testing.TB) uint64 {
	t.Helper()
	var r unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
		t.Fatalf("getrlimit(RLIMIT_NOFILE): %v", err)
	}
	soft, hard := uint64(r.Cur), uint64(r.Max) // int64 on FreeBSD
	kcap, kname := kernelFileLimit()
	target := hard
	if kcap > 0 && target > kcap {
		target = kcap
	}
	if soft < target {
		want := r
		want.Cur = rlimValue(r.Cur, target)
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &want); err != nil {
			t.Logf("setrlimit(RLIMIT_NOFILE, %d): %v; keeping %d", target, err, soft)
		} else if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
			t.Fatalf("getrlimit(RLIMIT_NOFILE): %v", err)
		}
	}
	eff := uint64(r.Cur)
	if kcap > 0 && eff > kcap {
		eff = kcap
	}
	t.Logf("RLIMIT_NOFILE: soft %d, hard %d (at start: soft %d); %s=%d; usable fds %d",
		uint64(r.Cur), uint64(r.Max), soft, kname, kcap, eff)
	return eff
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
		t.Skipf("open-file limit %d fits %d loopback connections, need at least %d", limit, fit, atLeast)
	}
	if fit < want {
		t.Logf("open-file limit %d: using %d connections instead of %d", limit, fit, want)
	}
	return fit
}

// rlimValue converts v to the platform's rlimit field type (uint64 on Linux
// and macOS, int64 on FreeBSD); the first argument only selects the type.
func rlimValue[T ~uint64 | ~int64](_ T, v uint64) T { return T(v) }
