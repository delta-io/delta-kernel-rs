# Installation

`delta_kernel` is available on [crates.io](https://crates.io/crates/delta_kernel) and uses
Cargo feature flags to keep the core dependency-light.

## Requirements

- **Rust edition**: 2021
- **Minimum Rust version**: 1.88

## Adding the dependency

Add `delta_kernel` to your `Cargo.toml`:

```toml
[dependencies]
delta_kernel = { version = "0.21", features = ["default-engine-rustls", "arrow"] }
```

This gives you the default engine (which handles all I/O and expression evaluation for you) backed
by Arrow, with `rustls` for TLS. This is the recommended starting point for most users.

## Feature flags

You only pay for what you enable.

### Engine features

These enable the built-in `DefaultEngine`, which provides out-of-the-box support for reading and
writing Delta tables using Arrow and the `object_store` crate.

| Feature | Description |
|---------|-------------|
| `default-engine-rustls` | Default engine with `rustls` for TLS. **Recommended for most users.** |
| `default-engine-native-tls` | Default engine using your platform's native TLS (OpenSSL on Linux, Schannel on Windows, Secure Transport on macOS). Use this if `rustls` doesn't work in your environment. |
| `arrow` | Re-exports Arrow types at the version the kernel was built against. Enables `arrow-conversion` and `arrow-expression` implicitly via the default engine features. Currently maps to Arrow 58. |

You need exactly one of `default-engine-rustls` or `default-engine-native-tls` to use the default
engine. If you're building a custom engine, you may not need either. See
[Building a Connector](../connector/overview.md) for details.

### Arrow version pinning

If you need a specific Arrow version (e.g. to match your existing Arrow dependency), you can pin it
explicitly:

| Feature | Arrow version |
|---------|---------------|
| `arrow-58` | Arrow 58 (current default) |
| `arrow-57` | Arrow 57 |

For more details on managing Arrow version compatibility, see [Feature Flags](../concepts/feature_flags.md).

### Data features

| Feature | Description |
|---------|-------------|
| `arrow-conversion` | Enables converting between kernel types and Arrow types |
| `arrow-expression` | Enables evaluating kernel expressions over Arrow data |

These are pulled in automatically by the default engine features. You typically only need to specify
them directly if you're building a custom engine that still uses Arrow.

### Advanced features

| Feature | Description |
|---------|-------------|
| `internal-api` | Exposes additional APIs that are not yet fully stabilized. Some examples in this guide require this feature. |
| `schema-diff` | Enables experimental schema diffing functionality. |

## Example `Cargo.toml`

A typical project using delta kernel:

```toml
[package]
name = "my-delta-reader"
version = "0.1.0"
edition = "2021"

[dependencies]
delta_kernel = { version = "0.21", features = ["default-engine-rustls", "arrow"] }

# The kernel re-exports arrow, but you can also depend on it directly
# arrow = "58"
```

## What's next

With the dependency added, head to [Quick Start: Reading a Table](./quick_start_read.md) to read
your first Delta table.
