# Contributing

Bug reports, fixes and measurements are welcome. Twine is v0.x: the API can
still change, and every change that breaks it goes in
[CHANGELOG.md](CHANGELOG.md).

## Layout

The repository holds three Go modules:

| Module | Path | Notes |
| --- | --- | --- |
| `github.com/exergy-dev/twine` | `/` | the library: root package, `proto`, `http`, `tls`, `internal/*`, and the `examples/` except tunsink. Standard library plus `golang.org/x/sys` only; minimum Go 1.24 |
| `github.com/exergy-dev/twine/bench` | `bench/` | comparison harness (gnet, goroutine-per-connection, net/http). Linux only |
| `github.com/exergy-dev/twine/examples/tunsink` | `examples/tunsink/` | TCP → TUN + Kafka example (franz-go). Linux only |

The nested modules use a `replace` directive to build against the checkout.
Third-party dependencies never go into the root module.

## Make targets

CI runs these same targets, so a clean local run predicts a clean CI run.

```
make test          # unit + integration tests
make test-race     # the same under -race (CI adds TESTFLAGS=-short)
make test-debug    # with -tags twine_debug (assertions, buffer poisoning)
make test-uring    # io_uring backend (Linux)
make vet           # vet, also under the twine_debug and twine_uring tags
make crossbuild    # build and vet for darwin, freebsd, netbsd, openbsd, dragonfly
make tunsink       # vet and build examples/tunsink
make lint          # vet, crossbuild, tunsink, golangci-lint (root, darwin, twine_uring, bench, tunsink), staticcheck
make fuzz          # every parser fuzzer for FUZZTIME (default 30s)
make api           # regenerate docs/API.md
make tidy          # go mod tidy in all three modules
make vuln          # govulncheck in all three modules
make bench         # benchmarks into bench/results/<sha>.txt
make bench-compare # benchstat against BASE (default bench/results/main.txt)
```

The library needs Go 1.24; `make lint` and `make tunsink` need Go 1.25 or
later because examples/tunsink does. `make lint` skips golangci-lint or
staticcheck if it is not installed; set
`LINT_STRICT=1` (as CI does) to make that an error. CI pins the versions in
`.github/workflows/ci.yml`.

## Before sending a change

- `gofmt` clean (`make fmt`), and `make lint test test-race` pass.
- If the exported API changed, run `make api` and commit `docs/API.md`; CI
  fails when it is stale. Add a line to the Unreleased section of
  `CHANGELOG.md`, under "Changed (breaking)" if callers must change.
- Changes to the hot path (`loop.go`, `conn.go`, `transport.go`,
  `internal/*`, the framers, the TLS record path, the HTTP request path)
  need numbers: `make bench` on the base commit, copy its result to
  `bench/results/main.txt`, then `make bench bench-compare` on yours and
  include the benchstat output. The zero-allocation tests must keep passing.
- Keep each commit to one change, with a message that says what and why.

The benchmark harness and tunsink need Linux; the library itself is tested
on Linux and macOS in CI.

## Reporting a bug

Open an issue at <https://github.com/exergy-dev/twine/issues> with:

- OS and kernel version (`uname -a`), and the distribution or macOS version
- `go version` and `go env GOOS GOARCH`
- build tags in use (`twine_debug`, `twine_uring`) and options that differ
  from the defaults
- a minimal program or test that reproduces it
- the output with `TWINE_DEBUG=1` set (or built with `-tags twine_debug`),
  which turns on loop-affinity assertions, read-buffer poisoning and Future
  creation stacks; many misuse bugs report themselves this way

For security issues, follow [SECURITY.md](SECURITY.md) instead of opening a
public issue.
