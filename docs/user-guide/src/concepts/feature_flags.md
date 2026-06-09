# Feature flags

The Delta Kernel ships as two crates with Cargo feature flags to keep each lightweight. The
`delta_kernel` crate has no required runtime dependencies beyond the Rust standard library.
The `delta_kernel_default_engine` crate adds Arrow, Tokio, and `object_store`. Everything else
is opt-in.

## Recommended starting point

For most connectors that use the built-in engine with Arrow:

```toml
[dependencies]
delta_kernel = "0.23"
delta_kernel_default_engine = { version = "0.23", features = ["rustls"] }
```

## Complete feature reference

### `delta_kernel_default_engine` features

`delta_kernel_default_engine` provides out-of-the-box support for reading and writing Delta
tables on local filesystems and cloud object stores.

| Feature | Description |
|---------|-------------|
| `rustls` | TLS via `rustls`. Recommended for most users because it requires no native dependency. |
| `native-tls` | TLS via your platform's native library (OpenSSL on Linux, Schannel on Windows, Secure Transport on macOS). |
| `arrow` | Build against the latest supported Arrow version (currently 58). |
| `arrow-58` | Pin to Arrow 58 (with `parquet` 58 and `object_store` 0.13). |
| `arrow-57` | Pin to Arrow 57 (with `parquet` 57 and `object_store` 0.12). |

Pick exactly one of `rustls` or `native-tls`. Picking an `arrow-*` version on the default
engine automatically activates the same version on `delta_kernel` (the two crates must agree).

### `delta_kernel` features

| Feature | Description |
|---------|-------------|
| `arrow-conversion` | Convert between Kernel schema types and Arrow types (`TryIntoArrow`, `TryFromArrow`). |
| `arrow-expression` | Evaluate Kernel expressions over Arrow data. |
| `default-engine-base` | Shared Arrow modules used by the default engine. Pulled in automatically by `delta_kernel_default_engine`. |
| `arrow-58` / `arrow-57` | Pin the Arrow version used by Kernel's arrow modules. |
| `schema-diff` | Experimental schema diffing. |
| `internal-api` | Expose additional APIs that aren't yet stabilized. Some examples in this guide need this. |
| `prettyprint` | Arrow pretty-print helpers. Useful for debugging and examples. |
| `test-utils` | Test-only constructors for downstream crate tests. Pulls in `prettyprint`. Not for production use. |
| `integration-test` | Heavy integration tests (e.g., HDFS via `hdfs-native-object-store`). |

> [!TIP]
> Each `arrow-*` version feature pulls in the matching `parquet` and `object_store` crate
> versions. If your connector already depends on a specific Arrow version, pin the matching
> feature on both crates to avoid duplicate transitive dependencies.

## Common combinations

**Read and write with the default engine:**

```toml
delta_kernel = "0.23"
delta_kernel_default_engine = { version = "0.23", features = ["rustls"] }
```

**Custom engine using Arrow (no default engine):**

```toml
delta_kernel = { version = "0.23", features = ["arrow-conversion", "arrow-expression"] }
```

**Minimal custom engine with no Arrow dependency at all:**

```toml
delta_kernel = "0.23"
```

That gives you only the core Kernel types and traits. You implement `Engine` and `EngineData`
entirely in your own data format.
