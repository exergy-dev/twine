//go:build linux

// Command idle1m opens N idle TCP connections (default 1,000,000) to the
// twine echo server in bench/idle1m/server and holds them, sampling the
// server's RSS and the host's free memory every -sample. It aborts (closing
// everything) if MemAvailable drops below -min-free-gb.
//
//	go run ./idle1m/server -loops 8 -ports 40 > server.log &
//	go run ./idle1m -n 1000000 -ports 40 -hold 10m -server-pid $!
//
// Connections use blocking raw sockets from -workers goroutines, bound to
// 127.0.0.2.. (round-robin over -srcips addresses) with
// IP_BIND_ADDRESS_NO_PORT, paced to -rate connects/s, retrying
// ECONNREFUSED/ETIMEDOUT/EAGAIN/EADDRNOTAVAIL with backoff.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine/bench"
	"golang.org/x/sys/unix"
)

var (
	opened   atomic.Int64
	retries  atomic.Int64
	fatalErr atomic.Value // string
	errCount sync.Map     // errno string -> *atomic.Int64
	abort    atomic.Bool
)

func countErr(err error) {
	v, _ := errCount.LoadOrStore(err.Error(), new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func dial(dst *unix.SockaddrInet4, srcIdx int) (int, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT, 1)
	tv := unix.NsecToTimeval(int64(3 * time.Second))
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, &tv)
	ip := srcIdx + 2
	src := &unix.SockaddrInet4{Addr: [4]byte{127, 0, byte(ip >> 8), byte(ip)}}
	if err := unix.Bind(fd, src); err != nil {
		unix.Close(fd)
		return -1, err
	}
	for {
		err = unix.Connect(fd, dst)
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	// Idle from here on: restore blocking-free semantic by clearing the timeout.
	return fd, nil
}

func sockstat() string {
	b, err := os.ReadFile("/proc/net/sockstat")
	if err != nil {
		return ""
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "TCP:") {
			return strings.TrimSpace(strings.TrimPrefix(l, "TCP:"))
		}
	}
	return ""
}

func main() {
	n := flag.Int("n", 1_000_000, "connections to open")
	host := flag.String("host", "127.0.0.1", "server host")
	base := flag.Int("base", 20000, "first server port")
	ports := flag.Int("ports", 40, "server ports")
	srcIPs := flag.Int("srcips", 50, "loopback source addresses 127.0.0.2..")
	workers := flag.Int("workers", 64, "connecting goroutines")
	rate := flag.Int("rate", 40000, "max connects per second (0 = unlimited)")
	hold := flag.Duration("hold", 10*time.Minute, "hold time after the target (or the maximum) is reached")
	sample := flag.Duration("sample", 10*time.Second, "sampling interval")
	serverPID := flag.Int("server-pid", 0, "server pid, for RSS sampling")
	minFree := flag.Float64("min-free-gb", 4, "abort if MemAvailable drops below this")
	stall := flag.Duration("stall", 60*time.Second, "give up dialing after this long without a new connection")
	flag.Parse()

	lim, err := bench.RaiseNoFile()
	if err != nil {
		fmt.Println("setrlimit:", err)
		os.Exit(1)
	}
	fmt.Printf("# idle1m start %s target=%d ports=%d..%d srcips=%d workers=%d rate=%d/s nofile=%d hold=%v\n",
		time.Now().Format(time.RFC3339), *n, *base, *base+*ports-1, *srcIPs, *workers, *rate, lim, *hold)
	rss0 := bench.StatusKB(*serverPID, "VmRSS")
	fmt.Printf("# baseline server_rss_mb=%.1f mem_available_gb=%.1f sockstat: %s\n",
		float64(rss0)/1e6, float64(bench.MemAvailableKB())/1e6, sockstat())

	dsts := make([]*unix.SockaddrInet4, *ports)
	for i := range dsts {
		d := &unix.SockaddrInet4{Port: *base + i}
		copy(d.Addr[:], []byte{127, 0, 0, 1})
		_ = *host
		dsts[i] = d
	}
	fds := make([]int32, *n)
	for i := range fds {
		fds[i] = -1
	}

	start := time.Now()
	var next atomic.Int64
	var lastProgress atomic.Int64 // unix nanos
	lastProgress.Store(time.Now().UnixNano())
	var wg sync.WaitGroup
	// Token bucket: connection i may not start before start + i/rate.
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !abort.Load() {
				i := int(next.Add(1) - 1)
				if i >= *n {
					return
				}
				if *rate > 0 {
					at := start.Add(time.Duration(float64(i) / float64(*rate) * float64(time.Second)))
					if d := time.Until(at); d > 0 {
						time.Sleep(d)
					}
				}
				backoff := 20 * time.Millisecond
				for !abort.Load() {
					fd, err := dial(dsts[i%*ports], i%*srcIPs)
					if err == nil {
						fds[i] = int32(fd)
						opened.Add(1)
						lastProgress.Store(time.Now().UnixNano())
						break
					}
					countErr(err)
					switch err {
					case unix.ECONNREFUSED, unix.ETIMEDOUT, unix.EAGAIN, unix.EADDRNOTAVAIL, unix.EINPROGRESS, unix.ECONNRESET:
						retries.Add(1)
						time.Sleep(backoff)
						if backoff < time.Second {
							backoff *= 2
						}
						if time.Since(time.Unix(0, lastProgress.Load())) > *stall {
							fatalErr.CompareAndSwap(nil, fmt.Sprintf("no new connection for %v (last error %v)", *stall, err))
							abort.Store(true)
						}
					default: // EMFILE, ENFILE, ENOMEM, ENOBUFS, ...
						fatalErr.CompareAndSwap(nil, fmt.Sprintf("non-retryable error: %v", err))
						abort.Store(true)
					}
				}
				if abort.Load() {
					return
				}
			}
		}()
	}
	dialed := make(chan struct{})
	go func() { wg.Wait(); close(dialed) }()

	intr := make(chan os.Signal, 1)
	signal.Notify(intr, os.Interrupt)
	var (
		peakRSS, rssAtMin2, rssEnd uint64
		allUpAt                    time.Duration
		holdStart                  time.Time
		reason                     string
		minAvail                   = ^uint64(0)
		dialing                    = true
	)
	line := func(tag string) {
		rss := bench.StatusKB(*serverPID, "VmRSS")
		avail := bench.MemAvailableKB()
		if avail < minAvail {
			minAvail = avail
		}
		if rss > peakRSS {
			peakRSS = rss
		}
		rssEnd = rss
		fmt.Printf("%s t=%5.0fs opened=%d retries=%d server_rss_mb=%.1f server_hwm_mb=%.1f mem_avail_gb=%.2f mem_free_gb=%.2f sockstat: %s\n",
			tag, time.Since(start).Seconds(), opened.Load(), retries.Load(), float64(rss)/1e6,
			float64(bench.StatusKB(*serverPID, "VmHWM"))/1e6, float64(avail)/1e6, float64(bench.MemAvailableKB())/1e6, sockstat())
		if float64(avail)/1e6 < *minFree {
			reason = fmt.Sprintf("ABORT: MemAvailable %.2f GB < %.1f GB", float64(avail)/1e6, *minFree)
			abort.Store(true)
		}
	}
	tk := time.NewTicker(*sample)
	defer tk.Stop()
	var holdEnd time.Time
loop:
	for {
		select {
		case <-tk.C:
			if dialing {
				line("dial")
			} else {
				line("hold")
				if rssAtMin2 == 0 && time.Since(holdStart) >= 2*time.Minute {
					rssAtMin2 = rssEnd
				}
			}
			if abort.Load() && reason == "" {
				if v := fatalErr.Load(); v != nil {
					reason = v.(string)
				}
			}
			if reason != "" && strings.HasPrefix(reason, "ABORT") {
				break loop
			}
			if !dialing && time.Now().After(holdEnd) {
				break loop
			}
		case <-dialed:
			dialed = nil
			dialing = false
			allUpAt = time.Since(start)
			if v := fatalErr.Load(); v != nil {
				reason = v.(string)
			}
			line("dialdone")
			fmt.Printf("# dialing finished at %.1fs with %d/%d connections; reason=%q\n", allUpAt.Seconds(), opened.Load(), *n, reason)
			h := *hold
			if opened.Load() < int64(*n) && h > 3*time.Minute {
				h = 3 * time.Minute
			}
			holdStart = time.Now()
			holdEnd = holdStart.Add(h)
			fmt.Printf("# holding %v\n", h)
		case <-intr:
			reason = "interrupted"
			break loop
		}
	}
	abort.Store(true)
	if !dialing {
		line("final")
	}

	// Liveness: echo one byte on up to 2000 evenly spaced connections.
	live, tried := 0, 0
	if reason == "" || !strings.HasPrefix(reason, "ABORT") {
		step := max(1, int(opened.Load())/2000)
		buf := make([]byte, 1)
		for i := 0; i < len(fds); i += step {
			if fds[i] < 0 {
				continue
			}
			tried++
			fd := int(fds[i])
			tv := unix.NsecToTimeval(int64(5 * time.Second))
			_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
			if _, err := unix.Write(fd, []byte{'x'}); err != nil {
				continue
			}
			if n, err := unix.Read(fd, buf); err == nil && n == 1 && buf[0] == 'x' {
				live++
			}
		}
	}
	errs := ""
	errCount.Range(func(k, v any) bool { errs += fmt.Sprintf(" %v=%d", k, v.(*atomic.Int64).Load()); return true })
	got := opened.Load()
	fmt.Printf("# SUMMARY target=%d opened=%d time_to_open=%.1fs (%.0f conn/s) retries=%d dial_errors:%s\n",
		*n, got, allUpAt.Seconds(), float64(got)/max(allUpAt.Seconds(), 1e-9), retries.Load(), errs)
	fmt.Printf("# SUMMARY stop_reason=%q liveness_echo=%d/%d min_mem_avail_gb=%.2f\n", reason, live, tried, float64(minAvail)/1e6)
	fmt.Printf("# SUMMARY server_rss_baseline_mb=%.1f peak_rss_mb=%.1f rss_at_hold_min2_mb=%.1f rss_end_mb=%.1f\n",
		float64(rss0)/1e6, float64(peakRSS)/1e6, float64(rssAtMin2)/1e6, float64(rssEnd)/1e6)
	if got > 0 {
		fmt.Printf("# SUMMARY rss_bytes_per_conn=%.0f (end-baseline)/opened, peak %.0f\n",
			float64(rssEnd-rss0)/float64(got), float64(peakRSS-rss0)/float64(got))
	}
	if rssAtMin2 > 0 {
		fmt.Printf("# SUMMARY drift_min2_to_end_mb=%+.2f (%.3f%%)\n",
			(float64(rssEnd)-float64(rssAtMin2))/1e6, 100*(float64(rssEnd)-float64(rssAtMin2))/float64(rssAtMin2))
	}
	for _, fd := range fds {
		if fd >= 0 {
			_ = unix.SetsockoptLinger(int(fd), unix.SOL_SOCKET, unix.SO_LINGER, &unix.Linger{Onoff: 1, Linger: 0})
			unix.Close(int(fd))
		}
	}
	if strings.HasPrefix(reason, "ABORT") {
		os.Exit(2) //nolint:gocritic // the deferred ticker stop does not matter at exit
	}
}
