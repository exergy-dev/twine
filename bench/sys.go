//go:build linux

package bench

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ParseCPUs parses a CPU list such as "0,2,4" or "16-30:2,31" ("a-b:step"
// ranges are allowed).
func ParseCPUs(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		step := 1
		if i := strings.IndexByte(part, ':'); i >= 0 {
			v, err := strconv.Atoi(part[i+1:])
			if err != nil || v < 1 {
				return nil, fmt.Errorf("bad step in %q", part)
			}
			step, part = v, part[:i]
		}
		lo, hi := part, part
		if i := strings.IndexByte(part, '-'); i >= 0 {
			lo, hi = part[:i], part[i+1:]
		}
		a, err1 := strconv.Atoi(lo)
		b, err2 := strconv.Atoi(hi)
		if err1 != nil || err2 != nil || a < 0 || b < a {
			return nil, fmt.Errorf("bad cpu range %q", part)
		}
		for c := a; c <= b; c += step {
			out = append(out, c)
		}
	}
	return out, nil
}

// SetAffinity restricts every thread the process creates from now on, and
// the calling thread, to cpus (sched_setaffinity on the calling thread; new
// threads inherit it). Call it first thing in main, before the Go runtime
// has started many threads, or use taskset on the whole process instead.
func SetAffinity(cpus []int) error {
	var set unix.CPUSet
	set.Zero()
	for _, c := range cpus {
		set.Set(c)
	}
	return unix.SchedSetaffinity(0, &set)
}

// ProcCPU returns user+system CPU time consumed so far by process pid
// (utime+stime from /proc/<pid>/stat, 100 ticks per second).
func ProcCPU(pid int) (time.Duration, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, fmt.Errorf("bad /proc stat")
	}
	f := bytes.Fields(b[i+1:])
	// f[0] is field 3 (state); utime is field 14, stime field 15.
	if len(f) < 13 {
		return 0, fmt.Errorf("short /proc stat")
	}
	ut, err1 := strconv.ParseUint(string(f[11]), 10, 64)
	st, err2 := strconv.ParseUint(string(f[12]), 10, 64)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("bad utime/stime")
	}
	return time.Duration(ut+st) * 10 * time.Millisecond, nil
}

// RaiseNoFile raises RLIMIT_NOFILE's soft limit to the hard limit and
// returns the new limit.
func RaiseNoFile() (uint64, error) {
	var r unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
		return 0, err
	}
	r.Cur = r.Max
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &r); err != nil {
		return 0, err
	}
	return r.Cur, nil
}

// MemAvailableKB returns MemAvailable from /proc/meminfo, in KiB.
func MemAvailableKB() uint64 { return meminfoKB("MemAvailable:") }

func meminfoKB(key string) uint64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, key) {
			f := strings.Fields(line[len(key):])
			if len(f) > 0 {
				v, _ := strconv.ParseUint(f[0], 10, 64)
				return v
			}
		}
	}
	return 0
}

// StatusKB returns a "Key: N kB" field of /proc/<pid>/status in bytes.
func StatusKB(pid int, key string) uint64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, key+":") {
			f := strings.Fields(line[len(key)+1:])
			if len(f) > 0 {
				v, _ := strconv.ParseUint(f[0], 10, 64)
				return v << 10
			}
		}
	}
	return 0
}
