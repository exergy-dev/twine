GO      ?= go
GOBIN   ?= $(shell $(GO) env GOPATH)/bin
PKGS    := ./...
SHA     := $(shell git rev-parse --short HEAD 2>/dev/null || echo nogit)

# Extra flags for the test targets, e.g. `make test-race TESTFLAGS=-short`
# (CI shortens the race and debug runs this way).
TESTFLAGS ?=

# LINT_STRICT=1 makes a missing golangci-lint or staticcheck an error instead
# of a skip. CI sets it.
LINT_STRICT ?=

.PHONY: all test test-short test-race test-debug test-uring bench bench-compare \
	lint vet fuzz fmt tidy crossbuild api vuln

all: lint test

test:
	$(GO) test -count=1 $(TESTFLAGS) $(PKGS)

test-short:
	$(GO) test -short -count=1 $(TESTFLAGS) $(PKGS)

test-race:
	$(GO) test -race -count=1 $(TESTFLAGS) $(PKGS)

# Debug build tag enables loop-affinity assertions and buffer poisoning.
test-debug:
	$(GO) test -tags twine_debug -count=1 $(TESTFLAGS) $(PKGS)

# io_uring readiness backend (Linux; falls back to epoll where io_uring is
# unavailable, so this also passes elsewhere but tests nothing new).
test-uring:
	$(GO) test -tags twine_uring -count=1 $(TESTFLAGS) . ./internal/poll

vet:
	$(GO) vet $(PKGS)
	$(GO) vet -tags twine_debug $(PKGS)
	$(GO) vet -tags twine_uring . ./internal/...

# Cross-compile and vet the kqueue and BSD code paths. Only Linux and macOS
# run in CI; the rest must at least build.
CROSS := darwin/arm64 freebsd/amd64 netbsd/amd64 netbsd/386 openbsd/amd64 openbsd/386 dragonfly/amd64
crossbuild:
	@set -e; for p in $(CROSS); do \
		echo "GOOS=$${p%/*} GOARCH=$${p#*/} $(GO) build && vet $(PKGS)"; \
		GOOS=$${p%/*} GOARCH=$${p#*/} $(GO) build $(PKGS); \
		GOOS=$${p%/*} GOARCH=$${p#*/} $(GO) vet $(PKGS); \
	done

# Tools are looked up in GOBIN, then PATH.
tool = $(shell test -x $(GOBIN)/$(1) && echo $(GOBIN)/$(1) || command -v $(1) 2>/dev/null)
GOLANGCI    := $(call tool,golangci-lint)
STATICCHECK := $(call tool,staticcheck)
BENCHSTAT   := $(call tool,benchstat)
GOVULNCHECK := $(call tool,govulncheck)
missing = if [ -n "$(LINT_STRICT)" ]; then echo "$(1) not installed and LINT_STRICT is set" >&2; exit 1; fi; echo "$(1) not installed; skipped"

# Root module on Linux, darwin and the io_uring tag; the bench module for
# Linux (it is Linux-only), including gofmt.
lint: vet crossbuild
ifneq ($(GOLANGCI),)
	$(GOLANGCI) run $(PKGS)
	GOOS=darwin GOARCH=arm64 $(GOLANGCI) run $(PKGS)
	$(GOLANGCI) run --build-tags twine_uring ./internal/...
	cd bench && GOOS=linux $(GOLANGCI) run ./...
else
	@$(call missing,golangci-lint)
endif
ifneq ($(STATICCHECK),)
	$(STATICCHECK) $(PKGS)
else
	@$(call missing,staticcheck)
endif
	cd bench && GOOS=linux $(GO) vet ./...
	@out=$$(gofmt -l bench); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

fmt:
	gofmt -l -w $$(git ls-files '*.go')

# Regenerate docs/API.md (the public surface). CI fails when it is stale.
api:
	GO=$(GO) sh scripts/apidump.sh

tidy:
	$(GO) mod tidy
	cd bench && $(GO) mod tidy

# Known vulnerabilities in all three modules (nightly CI runs this).
vuln:
ifeq ($(GOVULNCHECK),)
	@echo "govulncheck not installed: go install golang.org/x/vuln/cmd/govulncheck@latest" >&2; exit 1
endif
	$(GOVULNCHECK) ./...
	cd bench && GOOS=linux $(GOVULNCHECK) ./...

# Fuzz every parser for a short time (nightly CI runs 10 minutes each). The
# patterns are anchored because -fuzz must match exactly one target.
FUZZTIME ?= 30s
fuzz:
	$(GO) test ./proto -run='^$$' -fuzz='^FuzzLineReceiver$$'    -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run='^$$' -fuzz='^FuzzLineReceiverRaw$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run='^$$' -fuzz='^FuzzNetstring$$'       -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run='^$$' -fuzz='^FuzzLengthPrefixed$$'  -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run='^$$' -fuzz='^FuzzAMP$$'             -fuzztime=$(FUZZTIME)
	$(GO) test ./twhttp -run='^$$' -fuzz='^FuzzParseRequest$$'   -fuzztime=$(FUZZTIME)

# Benchmarks: root module micro-benchmarks plus the bench/ harness module
# (Linux only).
bench:
	mkdir -p bench/results
	$(GO) test -p 1 -run=^$$ -bench=. -benchmem -count=5 $(PKGS) | tee bench/results/$(SHA).txt
	cd bench && $(GO) test -p 1 -run=^$$ -bench=. -benchmem -count=5 ./... | tee -a ../bench/results/$(SHA).txt

# Compare this commit's `make bench` output against a baseline. To make one,
# run `make bench` on the base commit and copy its bench/results/<sha>.txt.
BASE ?= bench/results/main.txt
bench-compare:
	@test -f "$(BASE)" || { echo "bench-compare: baseline $(BASE) not found; run 'make bench' on the base commit and copy its bench/results/<sha>.txt there, or pass BASE=file" >&2; exit 1; }
	@test -f bench/results/$(SHA).txt || { echo "bench-compare: bench/results/$(SHA).txt not found; run 'make bench' first" >&2; exit 1; }
	@test -n "$(BENCHSTAT)" || { echo "benchstat not installed: go install golang.org/x/perf/cmd/benchstat@latest" >&2; exit 1; }
	$(BENCHSTAT) $(BASE) bench/results/$(SHA).txt
