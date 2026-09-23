.PHONY: build test check-test e2e lint fmt fmt-check vet check clean docker run help

# Build variables
BINARY_NAME := costfluent-k8s-agent
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

# Go variables
GOBIN := $(shell go env GOPATH)/bin
GOLANGCI_LINT_VERSION := v2.4.0
GOLANGCI_LINT_TOOLCHAIN := go1.24.10
GOLANGCI_LINT := $(GOBIN)/golangci-lint-$(GOLANGCI_LINT_VERSION)

# Docker variables
DOCKER_REGISTRY ?= ghcr.io/costfluent
DOCKER_IMAGE := $(DOCKER_REGISTRY)/k8s-agent
DOCKER_TAG ?= $(VERSION)

## build: Build the binary
build:
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/agent

## build-linux: Build for Linux (for Docker)
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/$(BINARY_NAME)-linux-amd64 ./cmd/agent

## test: Run tests
test:
	go test -v -race -coverprofile=coverage.out ./...

## test-short: Run tests without race detector
test-short:
	go test -v -coverprofile=coverage.out ./...

## coverage: Show test coverage
coverage: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## lint: Run linter
lint: $(GOLANGCI_LINT)
	GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) $(GOLANGCI_LINT) run ./...

$(GOLANGCI_LINT):
	@mkdir -p $(dir $(GOLANGCI_LINT)); \
		tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
		GOTOOLCHAIN=$(GOLANGCI_LINT_TOOLCHAIN) GOBIN="$$tmp" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
		mv "$$tmp/golangci-lint" $(GOLANGCI_LINT)

## fmt: Format code
fmt:
	go fmt ./...
	gofmt -s -w .

## fmt-check: Fail when Go source is not formatted, without rewriting it
fmt-check:
	@test -z "$$(gofmt -s -l .)" || { gofmt -s -l .; exit 1; }

## vet: Run go vet
vet:
	go vet ./...

## tidy: Tidy and verify dependencies
tidy:
	go mod tidy
	go mod verify

## clean: Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

## docker: Build Docker image
docker:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_IMAGE):latest

## docker-push: Push Docker image
docker-push: docker
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

## run: Run locally (requires kubeconfig)
run: build
	./bin/$(BINARY_NAME)

## run-debug: Run with debug logging
run-debug: build
	COSTFLUENT_LOG_LEVEL=debug COSTFLUENT_LOG_FORMAT=console ./bin/$(BINARY_NAME)

## deps: Download dependencies
deps:
	go mod download

## generate: Run go generate
generate:
	go generate ./...

## check-test: Run tests without leaving a coverage file
check-test:
	go test -race ./...

## e2e: Install the chart into a kind cluster and assert a report reaches a stub receiver (needs docker, kind, kubectl, helm; not part of check)
e2e:
	go test -tags e2e -count=1 -timeout 15m -v ./test/e2e/

## check: Run non-mutating build, format, vet, lint, and tests
check: fmt-check vet lint check-test
	go build ./...

## version: Print version info
version:
	@echo "Version:    $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"

## help: Show this help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
