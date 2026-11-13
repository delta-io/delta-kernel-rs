KERNEL_PATH := ../..
USER_GUIDE_TARGET_DIR ?= $(KERNEL_PATH)/target/user-guide

.PHONY: test check-version serve build clean

# Build the book
build:
	mdbook build

# Serve locally with hot reload
serve:
	mdbook serve

# Test that code examples compile. Build both crates in one cargo invocation so the
# resulting $(USER_GUIDE_TARGET_DIR)/debug/deps contains a single delta_kernel rmeta with the
# unified feature set. Two separate cargo builds would leave two rmetas behind and mdbook test
# cannot pick one (E0464). The clean enforces the single rmeta. It and the build use a private
# directory, passed as --target-dir (which overrides any CARGO_TARGET_DIR), so that the clean
# cannot delete other builds that share the workspace target directory.
test: check-version
	cargo clean --manifest-path $(KERNEL_PATH)/kernel/Cargo.toml --target-dir $(USER_GUIDE_TARGET_DIR)
	cargo build --locked --manifest-path $(KERNEL_PATH)/Cargo.toml --target-dir $(USER_GUIDE_TARGET_DIR) \
	  -p delta_kernel -p delta_kernel_default_engine \
	  --features "delta_kernel/internal-api,delta_kernel/prettyprint,delta_kernel/check-constraints-in-dev,delta_kernel_default_engine/rustls,delta_kernel_default_engine/arrow"
	mdbook test -L $(USER_GUIDE_TARGET_DIR)/debug/deps

# Keep published dependency snippets aligned with the workspace release version.
check-version:
	@workspace_version=$$(awk '/^\[workspace.package\]/{in_package=1; next} in_package && /^version =/{gsub(/"/, "", $$3); print $$3; exit}' $(KERNEL_PATH)/Cargo.toml); \
	mismatches=$$(grep -R -n -E '^delta_kernel(_default_engine)? = ' src \
	  | grep -v '"\.\.\."' \
	  | grep -v -E " = (\\{ version = )?\"$${workspace_version}\"([,}]|$$)" || true); \
	if [ -n "$${mismatches}" ]; then \
	  echo "Dependency snippets must use workspace version $${workspace_version}:"; \
	  echo "$${mismatches}"; \
	  exit 1; \
	fi

# Clean build artifacts
clean:
	mdbook clean
