# Cross-platform build Makefile for to-pgsql

# Binary and paths
BIN_NAME := to-pgsql
MAIN_PKG := ./cmd/$(BIN_NAME)
DIST_DIR := dist

# Build options
CGO_ENABLED ?= 0
GOFLAGS ?=
LDFLAGS ?= -s -w

# Default target builds for current platform
.PHONY: all
all: build

# Ensure dist directory exists
$(DIST_DIR):
	@mkdir -p $(DIST_DIR)

# Build for host platform into dist/
.PHONY: build
build: $(DIST_DIR)
	@echo "Building $(BIN_NAME) for host platform"
	CGO_ENABLED=$(CGO_ENABLED) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(BIN_NAME) $(MAIN_PKG)

# Linux x86_64 (amd64)
.PHONY: build-linux-amd64
build-linux-amd64: $(DIST_DIR)
	@echo "Building $(BIN_NAME) for linux/amd64"
	GOOS=linux GOARCH=amd64 CGO_ENABLED=$(CGO_ENABLED) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(BIN_NAME)-linux-amd64 $(MAIN_PKG)

# Linux arm64 (aarch64)
.PHONY: build-linux-arm64
build-linux-arm64: $(DIST_DIR)
	@echo "Building $(BIN_NAME) for linux/arm64"
	GOOS=linux GOARCH=arm64 CGO_ENABLED=$(CGO_ENABLED) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(BIN_NAME)-linux-arm64 $(MAIN_PKG)

# Build both Linux targets
.PHONY: build-linux
build-linux: build-linux-amd64 build-linux-arm64

# Windows x86_64 (amd64)
.PHONY: build-windows-amd64
build-windows-amd64: $(DIST_DIR)
	@echo "Building $(BIN_NAME) for windows/amd64"
	GOOS=windows GOARCH=amd64 CGO_ENABLED=$(CGO_ENABLED) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(BIN_NAME)-windows-amd64.exe $(MAIN_PKG)

# Windows arm64 (aarch64)
.PHONY: build-windows-arm64
build-windows-arm64: $(DIST_DIR)
	@echo "Building $(BIN_NAME) for windows/arm64"
	GOOS=windows GOARCH=arm64 CGO_ENABLED=$(CGO_ENABLED) go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(DIST_DIR)/$(BIN_NAME)-windows-arm64.exe $(MAIN_PKG)

# Build both Windows targets
.PHONY: build-windows
build-windows: build-windows-amd64 build-windows-arm64

# Build all targets
.PHONY: build-all
build-all: build-linux build-windows
	@echo "Built all targets into $(DIST_DIR)"

# Clean artifacts
.PHONY: clean
clean:
	@echo "Cleaning $(DIST_DIR)"
	rm -rf $(DIST_DIR)
