# Installation

The Delta Kernel ships as two crates on crates.io:

- [`delta_kernel`](https://crates.io/crates/delta_kernel): the core library. No I/O, no Arrow.
- [`delta_kernel_default_engine`](https://crates.io/crates/delta_kernel_default_engine): the default
  Arrow + Tokio implementation of the `Engine` trait.

Cargo feature flags keep both crates dependency-light.

## Requirements

- **Rust edition**: 2021
- **Minimum Rust version**: 1.88

## Adding the dependency

For the common case (use the default engine), add both crates:

```toml
[dependencies]
delta_kernel = "0.28.0"
delta_kernel_default_engine = { version = "0.28.0", features = ["rustls"] }
```

That gives you Kernel plus a default engine that handles I/O and expression evaluation for you,
backed by Arrow with `rustls` for TLS.

If you're building a custom engine and don't need the default, depend on just `delta_kernel`
and enable whatever Arrow interop flags you want:

```toml
[dependencies]
delta_kernel = { version = "0.28.0", features = ["arrow-conversion", "arrow-expression"] }
```

## Choosing features

The default engine needs a TLS backend for HTTPS object stores. Use `rustls` for a portable default
or `native-tls` when the connector must use its platform TLS stack.

If your connector already exposes Arrow types, select the matching `arrow-N` feature to avoid two
incompatible Arrow versions at the boundary. Otherwise, let the default `arrow` feature track the
newest version supported by your Kernel release.

[Feature flags](../concepts/feature_flags.md) explains these choices and links to the exhaustive
feature inventories generated from the published crate manifests.

## Example `Cargo.toml`

A typical project using Kernel:

```toml
[package]
name = "my-delta-reader"
version = "0.1.0"
edition = "2021"

[dependencies]
delta_kernel = "0.28.0"
delta_kernel_default_engine = { version = "0.28.0", features = ["rustls"] }
```

## What's next

With the dependencies added, head to [Quick Start: Reading a Table](./quick_start_read.md) to
read your first Delta table.
