GO      ?= go
GOBIN   ?= $(shell $(GO) env GOPATH)/bin
PKGS    := ./...
SHA     := $(shell git rev-parse --short HEAD 2>/dev/null || echo nogit)

.PHONY: all test test-short test-race bench bench-compare lint vet fuzz fmt tidy crossbuild api tunsink

all: lint test

test:
	$(GO) test -count=1 $(PKGS)

test-short:
	$(GO) test -short -count=1 $(PKGS)

test-race:
	$(GO) test -race -count=1 $(PKGS)

# Debug build tag enables loop-affinity assertions and buffer poisoning.
test-debug:
	$(GO) test -tags twine_debug -count=1 $(PKGS)

vet:
	$(GO) vet $(PKGS)
	$(GO) vet -tags twine_debug $(PKGS)
	$(GO) vet -tags twine_uring . ./internal/...

# examples/tunsink is its own module (it depends on franz-go); vet and build it,
# plus the Rust twin (rust/examples/tunsink) when cmake is available.
tunsink:
	cd examples/tunsink && $(GO) vet ./... && $(GO) build -o /dev/null . && $(GO) build ./cmd/...
	@if PATH="$$HOME/.local/bin:$$PATH" command -v cmake >/dev/null 2>&1; then \
		PATH="$$HOME/.local/bin:$$PATH" cargo build --release --manifest-path rust/examples/tunsink/Cargo.toml; \
	else echo "tunsink: cmake not found, skipping Rust crate build"; fi

# Cross-compile the kqueue and BSD code paths; they cannot run on Linux CI
# but they must build.
crossbuild:
	GOOS=darwin  GOARCH=arm64 $(GO) build $(PKGS)
	GOOS=freebsd GOARCH=amd64 $(GO) build $(PKGS)
	GOOS=darwin  GOARCH=arm64 $(GO) vet $(PKGS)

# golangci-lint and staticcheck are looked up in GOBIN then PATH; a missing
# tool is reported and skipped, but findings from an installed tool fail.
GOLANGCI := $(shell test -x $(GOBIN)/golangci-lint && echo $(GOBIN)/golangci-lint || which golangci-lint 2>/dev/null)
STATICCHECK := $(shell test -x $(GOBIN)/staticcheck && echo $(GOBIN)/staticcheck || which staticcheck 2>/dev/null)

lint: vet crossbuild
	@if [ -n "$(GOLANGCI)" ]; then $(GOLANGCI) run $(PKGS); else echo "golangci-lint not installed; skipped"; fi
	@if [ -n "$(STATICCHECK)" ]; then $(STATICCHECK) $(PKGS); else echo "staticcheck not installed; skipped"; fi
	@cd bench && $(GO) vet ./...

fmt:
	gofmt -l -w .

# Regenerate docs/API.md (the frozen public surface).
api:
	GOTOOLCHAIN=local sh scripts/apidump.sh

tidy:
	$(GO) mod tidy
	cd bench && $(GO) mod tidy

# Fuzz every framing parser for a short time (nightly CI runs 10 minutes each).
FUZZTIME ?= 30s
fuzz:
	$(GO) test ./proto -run=^$$ -fuzz=FuzzLineReceiver   -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run=^$$ -fuzz=FuzzNetstring      -fuzztime=$(FUZZTIME)
	$(GO) test ./proto -run=^$$ -fuzz=FuzzLengthPrefixed -fuzztime=$(FUZZTIME)
	$(GO) test ./http  -run=^$$ -fuzz=FuzzParseRequest   -fuzztime=$(FUZZTIME)

# Benchmarks: root module micro-benchmarks plus the bench/ harness module.
bench:
	mkdir -p bench/results
	$(GO) test -run=^$$ -bench=. -benchmem -count=5 $(PKGS) | tee bench/results/$(SHA).txt
	cd bench && $(GO) test -run=^$$ -bench=. -benchmem -count=5 ./... | tee -a ../bench/results/$(SHA).txt

bench-compare:
	@test -f bench/results/main.txt || (echo "no bench/results/main.txt baseline"; exit 1)
	$(GOBIN)/benchstat bench/results/main.txt bench/results/$(SHA).txt
