# Wave 2 ownership matrix

Each agent owns exactly the paths below and creates no files elsewhere. Core
(non-test) files in the root package are owned by the orchestrator; agents
report bugs there with a diagnosis instead of editing.

| Agent | Owns | Test / fuzz names |
| --- | --- | --- |
| proto | `proto/**`, `examples/chat/`, `examples/client_reconnect/` | `FuzzLineReceiver`, `FuzzNetstring`, `FuzzLengthPrefixed`, corpora in `proto/testdata/fuzz/` |
| http | `http/**`, `examples/http_hello/` | `FuzzParseRequest`, corpus in `http/testdata/fuzz/` |
| tls | `tls/**` | — |
| bench | `bench/**` (whole module incl. go.mod/go.sum), `examples/echo/`, `examples/udp_echo/`, root `benchmarks_test.go`, `BENCHMARKS.md` | `BenchmarkTimers1M`, `BenchmarkCallFromThread` |
| tests | root `integration_test.go`, `future_test.go`, `connect_test.go`, `udp_test.go`, `shutdown_test.go`, `debug_test.go`, `blocking_test.go` | — |
| uring | `internal/poll/uring_linux.go`, `internal/poll/uring_linux_test.go` (tag `twine_uring`) | — |
| core-review | nothing (read-only review of root package) | — |

Public API frozen in: `protocol.go`, `engine.go`, `loop.go`, `future.go`,
`timer.go`, `options.go`, `errors.go`. Read `design.md` Sections 6–9 for the
contract and `docs/INTERNALS.md` for the internal packages.
