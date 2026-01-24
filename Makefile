.PHONY: help build test test-short test-integration test-coverage lint clean

# Default target
help:
	@echo "Available targets:"
	@echo "  build            - Build the pgok binary"
	@echo "  test             - Run all tests (unit + integration)"
	@echo "  test-short       - Run only unit tests (skip integration)"
	@echo "  test-integration - Run only integration tests"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  lint             - Run golangci-lint"
	@echo "  clean            - Clean build artifacts and test cache"

# Build the binary
build:
	go build -o pgok main.go

# Run all tests
test:
	go test -v ./...

# Run only unit tests (skip integration tests that require Docker)
test-short:
	go test -v -short ./...

# Run only integration tests
test-integration:
	go test -v -run Integration ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run linter
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	rm -f pgok
	rm -f coverage.out coverage.html
	go clean -testcache
