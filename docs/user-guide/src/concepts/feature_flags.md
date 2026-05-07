# Feature flags

Delta Kernel uses Cargo feature flags to keep the core library lightweight. The core crate
has no required runtime dependencies beyond the Rust standard library. Everything else is
opt-in.

## Recommended starting point

For most connectors that use the built-in engine with Arrow:

```toml
[dependencies]
delta_kernel = { version = "0.21", features = ["default-engine-rustls", "arrow"] }
```

## Complete feature reference

### Default engine

These features enable the built-in `DefaultEngine`, which provides out-of-the-box support for
reading and writing Delta tables.

| Feature | Description |
|---------|-------------|
| `default-engine-rustls` | Default engine using `rustls` for TLS. Recommended for most users because it requires no native dependency. |
| `default-engine-native-tls` | Default engine using your platform's native TLS library (OpenSSL on Linux, Schannel on Windows, Secure Transport on macOS). |

Pick exactly one. Both pull in `default-engine-base` plus `reqwest` (for fetching pre-signed URLs),
which together enable:
- `arrow-conversion` and `arrow-expression`
- `tokio` async runtime
- `futures`
- `reqwest` HTTP client (TLS backend selected by the feature you choose)

### Arrow

| Feature | Description |
|---------|-------------|
| `arrow` | Re-exports Arrow types at the latest supported version (currently Arrow 58). Use this unless you need a specific version. |
| `arrow-58` | Pins to Arrow 58 (with `parquet` 58 and `object_store` 0.13). |
| `arrow-57` | Pins to Arrow 57 (with `parquet` 57 and `object_store` 0.12). |
| `arrow-conversion` | Enables converting between Kernel schema types and Arrow types (`TryIntoArrow`, `TryFromArrow`). |
| `arrow-expression` | Enables evaluating Kernel expressions over Arrow data. |

`arrow-conversion` and `arrow-expression` are pulled in automatically by the default engine.
You only need to specify them directly if you're building a custom engine that still uses
Arrow.

> [!TIP]
> Each `arrow-*` version feature also pulls in the matching `parquet` and `object_store`
> crate versions. If your connector already depends on a specific Arrow version, pin the
> matching feature to avoid duplicate transitive dependencies.

### Experimental features

These features are under active development. Their APIs may change between releases.

| Feature | Description |
|---------|-------------|
| `schema-diff` | Schema diffing functionality for comparing table schemas. |

### Development features

| Feature | Description |
|---------|-------------|
| `internal-api` | Exposes additional APIs not yet stabilized (marked with `#[cfg(feature = "internal-api")]`). Some examples in this guide use this feature. |
| `prettyprint` | Enables Arrow pretty-print helpers. Useful for debugging and examples. Automatically enabled by `test-utils`. |
| `test-utils` | Exposes `delta_kernel::test_utils::*` helpers for downstream crate tests. Pulls in `default-engine-rustls`, `internal-api`, `prettyprint`, and tarball/temp-dir/rstest deps. Not intended for production use. |
| `integration-test` | Enables heavy integration tests (e.g., HDFS via `hdfs-native-object-store`). |

## Common combinations

**Read and write with the default engine:**
```toml
delta_kernel = { version = "0.21", features = ["default-engine-rustls", "arrow"] }
```

**Custom engine using Arrow (no default engine):**
```toml
delta_kernel = { version = "0.21", features = ["arrow-conversion", "arrow-expression"] }
```

**Minimal custom engine with no Arrow dependency at all:**
```toml
delta_kernel = { version = "0.21" }
```

This gives you only the core Kernel types and traits. You implement `Engine` and `EngineData`
entirely in your own data format.
