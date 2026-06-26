KERNEL_PATH := ../..

.PHONY: test serve build clean

# Build the book
build:
	mdbook build

# Serve locally with hot reload
serve:
	mdbook serve

# Test that code examples compile. Build both crates in one cargo invocation so the
# resulting target/debug/deps contains a single delta_kernel rmeta with the unified
# feature set. Two separate cargo builds would leave two rmetas behind and mdbook test
# cannot pick one (E0464).
test:
	cargo clean --manifest-path $(KERNEL_PATH)/kernel/Cargo.toml
	cargo build --locked --manifest-path $(KERNEL_PATH)/Cargo.toml \
	  -p delta_kernel -p delta_kernel_default_engine \
	  --features "delta_kernel/internal-api,delta_kernel/prettyprint,delta_kernel_default_engine/rustls,delta_kernel_default_engine/arrow"
	mdbook test -L $(KERNEL_PATH)/target/debug/deps

# Clean build artifacts
clean:
	mdbook clean
