KERNEL_PATH := ../..
KERNEL_FEATURES := default-engine-rustls,arrow,internal-api,prettyprint

.PHONY: test serve build clean

# Build the book
build:
	mdbook build

# Serve locally with hot reload
serve:
	mdbook serve

# Test that code examples compile
test:
	cargo clean --manifest-path $(KERNEL_PATH)/kernel/Cargo.toml -p delta_kernel
	cargo build --locked --manifest-path $(KERNEL_PATH)/kernel/Cargo.toml --features "$(KERNEL_FEATURES)"
	mdbook test -L $(KERNEL_PATH)/target/debug/deps

# Clean build artifacts
clean:
	mdbook clean
