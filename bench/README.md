# bench

Benchmark harnesses (design Section 12). This is a separate Go module so the
third-party comparison targets (gnet) never enter the root module.

Run everything with `make bench` from the repository root; results land in
`bench/results/<git-sha>.txt`. Interpreted numbers and caveats are in
`../BENCHMARKS.md`; the raw baseline is `results/baseline.txt`.

| Path | What |
| --- | --- |
| `servers.go`, `http_servers.go` | the echo and HTTP servers under test (twine, goroutine-per-conn, gnet, net/http, raw-accept memory baseline) |
| `loadgen.go` | ping-pong, stream, HTTP and idle-connection load generators, latency histogram |
| `harness_test.go` | `BenchmarkEchoSmall`, `BenchmarkEchoScaling` (+`Gnet` control), `BenchmarkEchoStream`, `BenchmarkHTTPPlaintext`, `BenchmarkEchoIdle` |
| `echo_twine`, `echo_goroutine`, `echo_gnet` | standalone echo servers (`-addr`, `-loops`) |
| `http_twine`, `http_nethttp` | standalone plaintext HTTP servers (`-addr`, `-loops`) |
| `echoclient`, `httpclient` | standalone load generators (`-addr -conns -size/-path -duration`) |

Harness flags (pass after the package, e.g. `go test -run "^$" -bench EchoIdle . -idle-conns=100000`):
`-idle-conns` (default 10000), `-idle-settle` (1s), `-echo-loops` (8, twine and gnet
loops), `-scaling-spin` (twine `SpinIterations` in EchoScaling, -1 = default).
