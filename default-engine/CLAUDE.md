# default-engine

## Overview

The `delta_kernel_default_engine` crate provides the default `Engine` implementation for
`delta_kernel`. It uses Arrow for in-memory data representation, `object_store` for storage
I/O, and Tokio for async task execution.

## Crate Structure

| File | Description |
|------|-------------|
| `lib.rs` | `DefaultEngine`, `DefaultEngineBuilder` -- engine construction and trait impl |
| `executor.rs` | `TaskExecutor` trait, `TokioBackgroundExecutor`, `TokioMultiThreadExecutor` |
| `filesystem.rs` | `ObjectStoreStorageHandler` -- `StorageHandler` impl via `object_store` |
| `json.rs` | `DefaultJsonHandler` -- JSON log file reading/writing |
| `parquet.rs` | `DefaultParquetHandler` -- Parquet data file I/O, pre-signed URL support |
| `stats.rs` | `collect_stats` -- per-file column statistics collection |
| `file_stream.rs` | Async file reading utilities |
| `storage.rs` | `store_from_url`, `store_from_url_opts` -- ObjectStore construction helpers |

## Dependency on kernel

This crate depends on `delta_kernel` with the `internal-api` feature, which exposes
`pub(crate)` items needed by the engine (e.g. `arrow_utils`, `column_trie`,
`ensure_data_types`, `parquet_row_group_skipping`). These APIs are unstable.

## Feature Flags

- `rustls` / `native-tls` -- TLS backend for `reqwest` (pick one)
- `arrow` -- defaults to latest Arrow version (`arrow-57`)
- `arrow-56`, `arrow-57` -- explicit Arrow version selection

## Build & Test

```bash
cargo nextest run -p delta_kernel_default_engine --all-features
cargo clippy -p delta_kernel_default_engine --all-features -- -D warnings
```
