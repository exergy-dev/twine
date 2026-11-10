# bench

Benchmark harnesses behind BENCHMARKS.md. This is a separate Go module so the
third-party comparison targets (gnet) never enter the root module.
Linux only: the harness pins CPUs and reads `/proc`, and every file carries
`//go:build linux`.

Run everything with `make bench` from the repository root; results land in
`bench/results/<git-sha>.txt`. Interpreted numbers and caveats are in
`../BENCHMARKS.md`; the raw baseline is `results/baseline.txt`.

| Path | What |
| --- | --- |
| `servers.go`, `http_servers.go` | the echo and HTTP servers under test (twine, goroutine-per-conn, gnet, net/http, raw-accept memory baseline) |
| `loadgen.go` | ping-pong, stream, HTTP and idle-connection load generators, latency histogram |
| `harness_test.go` | `BenchmarkEchoSmall`, `BenchmarkEchoScaling` (+`Gnet` control), `BenchmarkEchoStream`, `BenchmarkHTTPPlaintext`, `BenchmarkEchoIdle` |
| `echo_twine`, `echo_goroutine`, `echo_gnet` | standalone echo servers (`-addr`, `-loops`; `echo_twine` and `http_twine` also `-pin`) |
| `http_twine`, `http_nethttp` | standalone plaintext HTTP servers (`-addr`, `-loops`) |
| `echoclient`, `httpclient` | standalone goroutine-per-connection load generators (`-addr -conns -size/-path -duration -server-pid`) |
| `twineload` | event-loop load generator on twine (`-proto echo\|http`, `-loops -pin -spin -server-pid`) |
| `scaling` | server-bound runner: separate pinned server and client processes, reports server CPU from `/proc` (see BENCHMARKS.md); output in `results/pinned.txt` |
| `idle1m/`, `idle1m/server` | 1M idle-connection soak client and twine server (`idle1m/soak.sh -netns`); output in `results/idle1m.txt` |
| `sys.go` | CPU list parsing, affinity, `/proc` CPU and memory readers, RLIMIT_NOFILE |

Harness flags (pass after the package, e.g. `go test -run "^$" -bench EchoIdle . -idle-conns=100000`):
`-idle-conns` (default 10000), `-idle-settle` (1s), `-echo-loops` (8, twine and gnet
loops), `-scaling-spin` (twine `SpinIterations` in EchoScaling, -1 = default).
