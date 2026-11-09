//go:build linux

// Command scaling is the server-bound scaling runner (BENCHMARKS.md, "pinned
// methodology"). For every combination of server, server loop count,
// connection count and client configuration it
//
//  1. starts the server as its own process under taskset on the first N
//     physical cores (even CPUs 0,2,4,...; the sibling threads stay idle),
//     with twine.PinLoops for twine (GOMAXPROCS=N for net/http),
//  2. runs the load generator as another process under taskset on CPUs 16-31,
//  3. reads the server's CPU time from /proc/<pid>/stat over the measured
//     window (done by the client via -server-pid),
//
// and prints one line per run plus summary tables with linearity
// (rate at N loops) / (N x rate at 1 loop).
//
// Build the binaries first (the runner does it into -bin if they are missing):
//
//	go run ./scaling -mode echo -servers twine,gnet -loops 1,2,4,8 -conns 1000,4000
//	go run ./scaling -mode http -servers twine@8,nethttp@8,nethttp@16@0-15 -conns 256,10000 -clients gor,tl,spin
//
// Client configurations: gor = bench/echoclient or bench/httpclient
// (goroutine per connection); tl = bench/twineload, 8 loops on 16,18,..,30;
// spin = tl with SpinIterations(5000) (client threads stay awake; results
// recorded before the rename print client=tls); tlm / tlms
// = twineload with as many loops as the server has (default spin / spin 5000).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type job struct {
	kind  string // twine, gnet, nethttp
	n     int
	cpus  string
	conns int
	cli   string
	fixed bool // single explicit configuration (kind@loops[@cpus])
}

type result struct {
	j                  job
	rate, p50, p99     float64
	srvCores, cliCores float64
	raw                string
	err                error
}

func main() {
	mode := flag.String("mode", "echo", "echo or http")
	servers := flag.String("servers", "twine", "comma list of kind[@loops[@cpus]]: twine, gnet, nethttp; loops/cpus given = a fixed configuration, otherwise swept over -loops on the first N even CPUs")
	loopsFlag := flag.String("loops", "1,2,4,8", "server loop counts to sweep")
	connsFlag := flag.String("conns", "1000", "connection counts")
	clients := flag.String("clients", "gor,tl,spin,tlm,tlms", "client configurations (see package doc)")
	dur := flag.Duration("duration", 10*time.Second, "measured window per run")
	cliCPUs := flag.String("client-cpus", "16-31", "taskset CPU list for the load generator")
	cliPin := flag.String("client-pin", "16,18,20,22,24,26,28,30", "PinLoops list for twineload")
	out := flag.String("out", "", "append raw run output to this file")
	binDir := flag.String("bin", "", "directory for built binaries (default: temp dir)")
	size := flag.Int("size", 64, "echo message size")
	flag.Parse()

	bd := *binDir
	if bd == "" {
		d, err := os.MkdirTemp("", "twine-scaling")
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = os.RemoveAll(d) }()
		bd = d
	}
	for _, p := range []string{"twineload", "echo_twine", "echo_gnet", "echoclient", "http_twine", "http_nethttp", "httpclient"} {
		if _, err := os.Stat(filepath.Join(bd, p)); err == nil {
			continue
		}
		c := exec.Command("go", "build", "-o", filepath.Join(bd, p), "./"+p)
		c.Env = append(os.Environ(), "GOTOOLCHAIN=local")
		c.Stderr = os.Stderr
		if err := c.Run(); err != nil {
			log.Fatalf("build %s: %v (run from the bench/ directory)", p, err) //nolint:gocritic // leaves the temp dir behind on a build failure
		}
	}

	var outF = io.Discard
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		outF = f
	}
	logf := func(format string, a ...any) {
		s := fmt.Sprintf(format, a...)
		fmt.Print(s)
		_, _ = io.WriteString(outF, s)
	}
	logf("# scaling run %s mode=%s duration=%v host=%s\n", time.Now().Format(time.RFC3339), *mode, *dur, hostInfo())

	var results []result
	port := 9200
	for _, sv := range strings.Split(*servers, ",") {
		parts := strings.Split(sv, "@")
		kind := parts[0]
		var loopList []int
		cpusOverride := ""
		if len(parts) > 1 {
			n, _ := strconv.Atoi(parts[1])
			loopList = []int{n}
		} else {
			for _, s := range strings.Split(*loopsFlag, ",") {
				n, _ := strconv.Atoi(s)
				loopList = append(loopList, n)
			}
		}
		if len(parts) > 2 {
			cpusOverride = parts[2]
		}
		for _, cs := range strings.Split(*connsFlag, ",") {
			conns, _ := strconv.Atoi(cs)
			for _, cli := range strings.Split(*clients, ",") {
				for _, n := range loopList {
					cpus := cpusOverride
					if cpus == "" {
						var l []string
						for i := 0; i < n; i++ {
							l = append(l, strconv.Itoa(2*i))
						}
						cpus = strings.Join(l, ",")
					}
					j := job{kind, n, cpus, conns, cli, len(parts) > 1}
					port++
					r := run(j, *mode, bd, port, *dur, *cliCPUs, *cliPin, *size)
					results = append(results, r)
					if r.err != nil {
						logf("FAIL %s: %v\n%s\n", describe(j), r.err, r.raw)
						continue
					}
					logf("%-58s rate=%9.0f p50=%7.0fus p99=%7.0fus server_cores=%5.2f client_cores=%5.2f\n",
						describe(j), r.rate, r.p50, r.p99, r.srvCores, r.cliCores)
				}
			}
		}
	}
	logf("\n")
	summary(logf, results)
}

func describe(j job) string {
	return fmt.Sprintf("%s loops=%d cpus=%s conns=%d client=%s", j.kind, j.n, j.cpus, j.conns, j.cli)
}

func hostInfo() string {
	h, _ := os.Hostname()
	b, _ := os.ReadFile("/proc/loadavg")
	return h + " loadavg=" + strings.TrimSpace(string(b))
}

func run(j job, mode, bd string, port int, dur time.Duration, cliCPUs, cliPin string, size int) result {
	r := result{j: j}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var srv *exec.Cmd
	switch {
	case mode == "echo" && j.kind == "twine":
		srv = exec.Command("taskset", "-c", j.cpus, filepath.Join(bd, "echo_twine"), "-addr", addr, "-loops", strconv.Itoa(j.n), "-pin", j.cpus)
	case mode == "echo" && j.kind == "gnet":
		srv = exec.Command("taskset", "-c", j.cpus, filepath.Join(bd, "echo_gnet"), "-addr", addr, "-loops", strconv.Itoa(j.n))
	case mode == "http" && j.kind == "twine":
		srv = exec.Command("taskset", "-c", j.cpus, filepath.Join(bd, "http_twine"), "-addr", addr, "-loops", strconv.Itoa(j.n), "-pin", j.cpus)
	case mode == "http" && j.kind == "nethttp":
		srv = exec.Command("taskset", "-c", j.cpus, filepath.Join(bd, "http_nethttp"), "-addr", addr, "-loops", strconv.Itoa(j.n))
		srv.Env = append(os.Environ(), "GOMAXPROCS="+strconv.Itoa(j.n))
	default:
		r.err = fmt.Errorf("unsupported server %s in mode %s", j.kind, mode)
		return r
	}
	srv.Stderr = io.Discard
	srv.Stdout = io.Discard
	if err := srv.Start(); err != nil {
		r.err = err
		return r
	}
	defer func() {
		_ = srv.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = srv.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = srv.Process.Kill()
			<-done
		}
		time.Sleep(300 * time.Millisecond)
	}()
	up := false
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			up = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !up {
		r.err = fmt.Errorf("server did not come up")
		return r
	}
	time.Sleep(300 * time.Millisecond)

	pid := strconv.Itoa(srv.Process.Pid)
	var args []string
	cd := fmt.Sprintf("%ds", int(dur.Seconds()))
	if j.cli == "gor" {
		if mode == "echo" {
			args = []string{filepath.Join(bd, "echoclient"), "-addr", addr, "-conns", strconv.Itoa(j.conns), "-size", strconv.Itoa(size), "-duration", cd, "-server-pid", pid}
		} else {
			args = []string{filepath.Join(bd, "httpclient"), "-addr", addr, "-conns", strconv.Itoa(j.conns), "-path", "/", "-duration", cd, "-server-pid", pid}
		}
	} else {
		loops, spin := 8, -1
		switch j.cli {
		case "tl":
		case "spin":
			spin = 5000
		case "tlm":
			loops = j.n
		case "tlms":
			loops, spin = j.n, 5000
		default:
			r.err = fmt.Errorf("unknown client %q", j.cli)
			return r
		}
		pins := strings.Split(cliPin, ",")
		if loops < len(pins) {
			pins = pins[:loops]
		}
		args = []string{filepath.Join(bd, "twineload"), "-proto", mode, "-addr", addr, "-conns", strconv.Itoa(j.conns),
			"-size", strconv.Itoa(size), "-loops", strconv.Itoa(loops), "-pin", strings.Join(pins, ","),
			"-duration", cd, "-server-pid", pid}
		if spin >= 0 {
			args = append(args, "-spin", strconv.Itoa(spin))
		}
	}
	cmd := exec.Command("taskset", append([]string{"-c", cliCPUs}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	outB, err := cmd.CombinedOutput()
	r.raw = string(outB)
	if err != nil {
		r.err = err
		return r
	}
	sc := bufio.NewScanner(strings.NewReader(r.raw))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "RESULT ") {
			continue
		}
		for _, f := range strings.Fields(line)[1:] {
			k, v, _ := strings.Cut(f, "=")
			x, _ := strconv.ParseFloat(v, 64)
			switch k {
			case "rate":
				r.rate = x
			case "p50_us":
				r.p50 = x
			case "p99_us":
				r.p99 = x
			case "server_cores":
				r.srvCores = x
			case "client_cores":
				r.cliCores = x
			}
		}
		return r
	}
	r.err = fmt.Errorf("no RESULT line")
	return r
}

func summary(logf func(string, ...any), rs []result) {
	type key struct {
		kind  string
		conns int
		cli   string
		cpus  string
	}
	groups := map[key][]result{}
	var order []key
	for _, r := range rs {
		if r.err != nil {
			continue
		}
		k := key{r.j.kind, r.j.conns, r.j.cli, ""}
		if r.j.fixed {
			k.kind = fmt.Sprintf("%s (GOMAXPROCS/loops=%d, cpus %s)", r.j.kind, r.j.n, r.j.cpus)
		}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	logf("| server | conns | client | loops | rate/s | per server core | server cores used | linearity vs 1 loop | p50 us | p99 us | client cores used |\n")
	logf("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, k := range order {
		g := groups[k]
		base := 0.0
		for _, r := range g {
			if r.j.n == 1 {
				base = r.rate
			}
		}
		for _, r := range g {
			lin := "-"
			if base > 0 {
				lin = fmt.Sprintf("%.2f", r.rate/(float64(r.j.n)*base))
			}
			logf("| %s | %d | %s | %d | %.0f | %.0f | %.2f | %s | %.0f | %.0f | %.2f |\n",
				k.kind, k.conns, k.cli, r.j.n, r.rate, r.rate/float64(r.j.n), r.srvCores, lin, r.p50, r.p99, r.cliCores)
		}
	}
}
