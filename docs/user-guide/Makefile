KERNEL_PATH := ../..
KERNEL_FEATURES := delta_kernel/internal-api,delta_kernel/prettyprint
DEFAULT_ENGINE_FEATURES := delta_kernel_default_engine/rustls,delta_kernel_default_engine/arrow

.PHONY: test serve build clean

# Build the book
build:
	mdbook build

# Serve locally with hot reload
serve:
	mdbook serve

# Test that code examples compile. Build the default engine as the single root package so
# target/debug/deps contains one delta_kernel rmeta with the unified feature set. Selecting
# delta_kernel as another root creates a second rmeta that mdbook cannot disambiguate (E0464).
test:
	cargo clean --manifest-path $(KERNEL_PATH)/kernel/Cargo.toml
	cargo build --locked --manifest-path $(KERNEL_PATH)/Cargo.toml \
	  -p delta_kernel_default_engine \
	  --features "$(KERNEL_FEATURES),$(DEFAULT_ENGINE_FEATURES)"
	mdbook test -L $(KERNEL_PATH)/target/debug/deps

# Clean build artifacts
clean:
	mdbook clean
