# bench

Benchmark harnesses (design Section 12). This is a separate Go module so the
third-party comparison targets (gnet) never enter the root module.

Run everything with `make bench` from the repository root; results land in
`bench/results/<git-sha>.txt`.
