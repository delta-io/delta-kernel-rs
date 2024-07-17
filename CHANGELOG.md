# Changelog

## [v0.2.0](https://github.com/delta-incubator/delta-kernel-rs/tree/v0.2.0/) (2024-07-17)

[Full Changelog](https://github.com/delta-incubator/delta-kernel-rs/compare/v0.1.1...v0.2.0)

**API Changes**

*Breaking*

1. The scan callback if using `visit_scan_files` now takes an extra `Option<Stats>` argument, holding top level
   stats for associated scan file. You will need to add this argument to your callback.

    Likewise, the callback in the ffi code also needs to take a new argument which is a pointer to a
   `Stats` struct, and which can be null if no stats are present.

*Additions*

1. You can call `scan_builder()` directly on a snapshot, for more convenience.
2. You can pass a `URL` starting with `"hdfs"` or `"viewfs"` to the default client to read using `hdfs_native_store`

**Implemented enhancements:**

- Handle nested structs in `schemaString` (allows reading iceberg compat tables) [\#257](https://github.com/delta-incubator/delta-kernel-rs/pull/257)
- Expose top level stats in scans [\#227](https://github.com/delta-incubator/delta-kernel-rs/pull/227)
- Hugely expanded C-FFI example [\#203](https://github.com/delta-incubator/delta-kernel-rs/pull/203)
- Add `scan_builder` function to `Snapshot` [\#273](https://github.com/delta-incubator/delta-kernel-rs/pull/273)
- Add `hdfs_native_store` support [\#273](https://github.com/delta-incubator/delta-kernel-rs/pull/274)
- Proper reading of Parquet files, including only reading requested leaves, type casting, and reordering [\#271](https://github.com/delta-incubator/delta-kernel-rs/pull/271)
- Allow building the package if you are behind an https proxy [\#282](https://github.com/delta-incubator/delta-kernel-rs/pull/282)

**Fixed bugs:**

- Don't error if more fields exist than expected in a struct expression [\#267](https://github.com/delta-incubator/delta-kernel-rs/pull/267)
- Handle cases where the deletion vector length is less than the total number of rows in the chunk [\#276](https://github.com/delta-incubator/delta-kernel-rs/pull/276)
- Fix partition map indexing if column mapping is in effect [\#278](https://github.com/delta-incubator/delta-kernel-rs/pull/278)


## [v0.1.1](https://github.com/delta-incubator/delta-kernel-rs/tree/v0.1.0/) (2024-06-03)

[Full Changelog](https://github.com/delta-incubator/delta-kernel-rs/compare/v0.1.0...v0.1.1)

**Implemented enhancements:**

- Support unary `NOT` and `IsNull` for data skipping [\#231](https://github.com/delta-incubator/delta-kernel-rs/pull/231)
- Add unary visitors to c ffi [\#247](https://github.com/delta-incubator/delta-kernel-rs/pull/247)
- Minor other QOL improvements


## [v0.1.0](https://github.com/delta-incubator/delta-kernel-rs/tree/v0.1.0/) (2024-06-12)

Initial public release
