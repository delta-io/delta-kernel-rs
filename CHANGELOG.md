# Changelog

## [v0.3.0](https://github.com/delta-incubator/delta-kernel-rs/tree/v0.3.0/) (2024-08-07)

[Full Changelog](https://github.com/delta-incubator/delta-kernel-rs/compare/v0.2.0...v0.3.0)

**API Changes**

*Breaking*

1. `delta_kernel::column_mapping` module moved to `delta_kernel::features::column_mapping` [\#222](https://github.com/delta-incubator/delta-kernel-rs/pull/297)


*Additions*

1. New deletion vector API `row_indexes` (and accompanying FFI) to get row indexes instead of seletion vector of deleted rows. This can be more efficient for sparse DVs. [\#215](https://github.com/delta-incubator/delta-kernel-rs/pull/215)
2. Typed table features: `ReaderFeatures`, `WriterFeatures` enums and `has_reader_feature`/`has_writer_feature` API [\#222](https://github.com/delta-incubator/delta-kernel-rs/pull/297)

**Implemented enhancements:**

- Add `--limit` option to example `read-table-multi-threaded` [\#297](https://github.com/delta-incubator/delta-kernel-rs/pull/297)
- FFI now built with cmake. Move to using the read-test example as an ffi-test. And building on macos. [\#288](https://github.com/delta-incubator/delta-kernel-rs/pull/288)
- Golden table tests migrated from delta-spark/delta-kernel java [\#295](https://github.com/delta-incubator/delta-kernel-rs/pull/295)
- Code coverage implemented via [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) and reported with [codecov](https://app.codecov.io/github/delta-incubator/delta-kernel-rs) [\#287](https://github.com/delta-incubator/delta-kernel-rs/pull/287)
- All tests enabled to run in CI [\#284](https://github.com/delta-incubator/delta-kernel-rs/pull/284)
- Updated DAT to 0.3 [\#290](https://github.com/delta-incubator/delta-kernel-rs/pull/290)

**Fixed bugs:**

- Evaluate timestamps as "UTC" instead of "+00:00" for timezone [\#295](https://github.com/delta-incubator/delta-kernel-rs/pull/295)
- Make Map arrow type field naming consistent with parquet field naming [\#299](https://github.com/delta-incubator/delta-kernel-rs/pull/299)


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