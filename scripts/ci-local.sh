#!/bin/sh
# Replays the steps of .github/workflows/ci.yml on this machine (Linux job,
# lint job, and the nightly job's fuzz step with FUZZTIME configurable) so the
# workflow's commands are exercised even without a GitHub remote.
set -e
export GOTOOLCHAIN=${GOTOOLCHAIN:-local}
FUZZTIME=${FUZZTIME:-10m}
step() { echo "== $*"; }
step "test job: go vet ./..." ;            go vet ./...
step "test job: go vet -tags twine_debug"; go vet -tags twine_debug ./...
step "test job: go test ./..." ;           go test -count=1 ./...
step "test job: go test -race -short" ;    go test -race -short -count=1 ./...
step "test job: go test -tags twine_debug -short"; go test -tags twine_debug -short -count=1 ./...
step "test job (Linux): go vet -tags twine_uring"; go vet -tags twine_uring . ./internal/...
step "lint job: golangci-lint" ;           "$(go env GOPATH)/bin/golangci-lint" run ./...
step "lint job: cross-build darwin/freebsd"; GOOS=darwin GOARCH=arm64 go build ./... && GOOS=freebsd GOARCH=amd64 go build ./...
step "nightly: mpsc stress under -race (full)"; go test -race -count=1 ./internal/mpsc
step "nightly: fuzz FUZZTIME=$FUZZTIME per target (4 targets in parallel)"
go test ./proto -run='^$' -fuzz=FuzzLineReceiver   -fuzztime="$FUZZTIME" -parallel 4 > /tmp/fuzz-line.txt 2>&1 &
go test ./proto -run='^$' -fuzz=FuzzNetstring      -fuzztime="$FUZZTIME" -parallel 4 > /tmp/fuzz-netstring.txt 2>&1 &
go test ./proto -run='^$' -fuzz=FuzzLengthPrefixed -fuzztime="$FUZZTIME" -parallel 4 > /tmp/fuzz-lp.txt 2>&1 &
go test ./http  -run='^$' -fuzz=FuzzParseRequest   -fuzztime="$FUZZTIME" -parallel 4 > /tmp/fuzz-http.txt 2>&1 &
wait
for f in /tmp/fuzz-line.txt /tmp/fuzz-netstring.txt /tmp/fuzz-lp.txt /tmp/fuzz-http.txt; do
  echo "-- $f"; grep -E "^(ok|FAIL|---)|elapsed" "$f" | tail -3
done
echo "CI replay complete"
