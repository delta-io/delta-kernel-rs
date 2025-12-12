.PHONY: all build example clean test rust-ffi

# Detect OS
UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	LIB_EXT := dylib
	LIB_PATH_VAR := DYLD_LIBRARY_PATH
else
	LIB_EXT := so
	LIB_PATH_VAR := LD_LIBRARY_PATH
endif

# Paths
PROJECT_ROOT := ../..
RUST_TARGET := $(PROJECT_ROOT)/target/release
LIB_PATH := $(RUST_TARGET)

all: rust-ffi build example

# Build the Rust FFI library
rust-ffi:
	cd $(PROJECT_ROOT) && cargo build -p delta_kernel_ffi --all-features --release

# Build Go bindings
build: rust-ffi
	go build ./delta

# Build examples
example: rust-ffi
	go build -o examples/describe_schema ./examples/describe_schema.go
	go build -o examples/read_table ./examples/read_table.go

# Run example (requires TABLE_PATH environment variable)
run-example: example
	@if [ -z "$(TABLE_PATH)" ]; then \
		echo "Error: TABLE_PATH not set. Usage: make run-example TABLE_PATH=/path/to/table"; \
		exit 1; \
	fi
	$(LIB_PATH_VAR)=$(LIB_PATH):$$$(LIB_PATH_VAR) ./examples/describe_schema $(TABLE_PATH)

# Clean build artifacts
clean:
	rm -f examples/describe_schema examples/read_table
	go clean

# Run tests
test: rust-ffi
	$(LIB_PATH_VAR)=$(LIB_PATH):$$$(LIB_PATH_VAR) go test -v ./delta/...

# Run acceptance tests only
test-acceptance: rust-ffi
	$(LIB_PATH_VAR)=$(LIB_PATH):$$$(LIB_PATH_VAR) go test -v -run TestAcceptance ./delta

# Run quick tests (short mode)
test-quick: rust-ffi
	$(LIB_PATH_VAR)=$(LIB_PATH):$$$(LIB_PATH_VAR) go test -short ./...

# Download Go dependencies
deps:
	go mod download
	go mod tidy

# Format Go code
fmt:
	go fmt ./delta

# Run Go vet
vet:
	go vet ./delta
