# Feature flags

Cargo feature flags choose how Kernel integrates with your connector. Pick an Engine, a TLS
backend, and an Arrow compatibility level. Enable experimental Kernel APIs only when the workflow
requires them.

For exhaustive, release-specific inventories, use the generated feature lists for
[`delta_kernel`] and [`delta_kernel_default_engine`]. Those lists come from the published crate
manifests and stay aligned with each release.

[`delta_kernel`]: https://docs.rs/crate/delta_kernel/latest/features
[`delta_kernel_default_engine`]: https://docs.rs/crate/delta_kernel_default_engine/latest/features

## Choose an Engine

Most connectors should start with the default engine:

```toml
[dependencies]
delta_kernel = "0.28.0"
delta_kernel_default_engine = { version = "0.28.0", features = ["rustls"] }
```

The default engine supplies Arrow data, Tokio execution, expression evaluation, Parquet and JSON
handling, and `object_store` integration. Its dependencies activate the matching Kernel support
automatically.

If your connector implements every Engine capability in its own data format, depend on Kernel
without Arrow features:

```toml
[dependencies]
delta_kernel = "0.28.0"
```

For a custom Engine that still uses Kernel's Arrow conversion and expression modules, enable those
capabilities directly:

```toml
[dependencies]
delta_kernel = { version = "0.28.0", features = ["arrow-conversion", "arrow-expression"] }
```

## Choose a TLS backend

The default engine needs one TLS backend for HTTPS object stores. Use `rustls` for a portable
default without native TLS dependencies. Use `native-tls` when your deployment must use the
platform's TLS library or certificate integration.

Choose one backend explicitly when you disable default features. Enabling both adds dependencies
without giving the connector a useful second transport path.

## Align Arrow versions

The `arrow` feature tracks the newest Arrow version supported by that Kernel release. Use it when
your connector does not expose Arrow types in its own public API.

If your connector already depends on Arrow, select the matching `arrow-N` feature on the default
engine. It activates the same version in Kernel. This prevents duplicate Arrow types from crossing
the connector boundary.

Check the generated crate feature lists for the Arrow versions supported by the release you use.
Do not infer the current versions from examples written for another release.

## Use unstable and development features

The `internal-api` feature exposes APIs that have not reached Kernel's public stability boundary.
Code that depends on it must expect source changes between minor releases.

Development and test features exist for in-progress protocol work, generated plans, test helpers,
and heavyweight integration environments. Treat their manifest descriptions and feature
dependencies as authoritative. They are intended for contributors or targeted adopters rather
than as a default connector configuration.

## What's next

- [Installation](../getting_started/installation.md) shows the complete dependency setup.
- [The Engine trait](./engine_trait.md) helps you choose between the default and custom Engines.
- [Implementing the Engine trait](../connector/implementing_engine.md) covers custom integration.
