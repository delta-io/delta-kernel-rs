# Changelog

## [v0.22.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.22.1/) (2026-01-20)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.22.0...v0.22.1)

### 🐛 Bug Fixes

1. fix: accumulate warnings so reporter no longer has deadlock risk. Previous the `warn!` in some
   reporter code could cause deadlocks. (#2550)

[#2550]: https://github.com/delta-io/delta-kernel-rs/pull/2550

## [v0.22.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.22.0/) (2026-04-29)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.21.0...v0.22.0)


### 🏗️ Breaking changes

1. Add delta.parquet.format.version table property ([#2369])
   - Adds `parquet_format_version: Option<String>` field to `TableProperties`. Callers using
     exhaustive struct construction must add `parquet_format_version: None`.
2. Robust partitioned-write APIs ([#2356])
   - Replaces the old `WriteContext` API with partition-aware variants: use
     `txn.partitioned_write_context(partition_values)` or `txn.unpartitioned_write_context()`.
     Partition values are now passed as `Map<String, Scalar>` (kernel handles serialization per
     the Delta spec) instead of `Map<String, String>`.
     `DefaultParquetHandler::write_parquet_file` signature changed from
     `(path, data, partition_values, stats_columns)` to `(data, &WriteContext)`. See
     `kernel/tests/integration/write/partitioned.rs` and the updated
     `kernel/examples/write-table` for migration examples.
3. Add typed null literal support to FFI expression visitors ([#2375])
   - FFI `visit_expression_literal_null` and `visit_literal_null` now accept `type_tag` (plus
     `precision`/`scale` for decimal). FFI engines must provide type information when emitting
     null literals; see the new `NullTypeTag` enum for the contract.
4. Change get_create_table_builder to accept EngineSchema visitor ([#2378])
   - FFI `get_create_table_builder` now takes `&EngineSchema` instead of `Handle<SharedSchema>`.
     Engines must implement the visitor pattern (`visit_field_*` callbacks) to populate the
     schema, matching the existing `scan_builder_with_schema` pattern. See the
     `ffi/examples/create-table` example for a reference implementation.
5. Transform stats parsed for remove actions ([#2061])
   - Adds `fn has_field(&self, name: &ColumnName) -> bool` to the `EngineData` trait. Custom
     `EngineData` implementors must add this method.
6. Default to writing relative paths in `add.path` ([#2410])
   - Kernel now writes **relative** paths in `add.path` (e.g. `abc.parquet`) instead of absolute
     URLs (e.g. `s3://bucket/table/abc.parquet`), matching Delta Spark.
7. Add tests for histograms and expose stats and histogram ([#2373])
   - New public types `FileStats` and `FileSizeHistogram`, plus `Snapshot::get_or_load_file_stats`
     and `Snapshot::get_file_stats_if_loaded` accessors. Connectors can read CRC file-stats
     histogram data through these public APIs.
8. Separate read state from effective state in Transaction ([#2385])
   - Internal `Transaction` refactor: splits the held snapshot into
     `read_snapshot_opt: Option<SnapshotRef>` (pre-commit state) and
     `effective_table_config: TableConfiguration` (state this commit will produce).
9. Make scan_table_changes_next return *mut ArrowFFIData ([#2430])
   - The Ok variant of FFI `scan_table_changes_next`'s `ExternResult` is now a heap-allocated
     `*mut ArrowFFIData` instead of an inline `ArrowFFIData` value. FFI consumers must switch
     from value-style access (`res.ok`) to pointer-style and call the new `free_arrow_ffi_data`
     on non-null results.
10. Update `CheckpointWriter::finalize` to accept `LastCheckpointHintStats` ([#2400])
    - `CheckpointWriter::finalize` now takes a `LastCheckpointHintStats` struct instead of
      `(FileMeta, ActionReconciliationIteratorState)`. Construct the new struct to correctly
      populate `_last_checkpoint` (including `sizeInBytes` and `size`) for V2 checkpoints with
      sidecars.
11. Replace existing metrics reporting with tracing ([#1822])
    - `MetricsReporter` is no longer part of the `Engine` APIs. Instead you can register a
      reporter as a tracing layer. See `kernel/examples/inspect-table` for an example of how
      to migrate.
12. Add CheckpointRowGroupFilter for checkpoint data skipping ([#1893])
    - `ParquetStatsProvider::get_parquet_rowcount_stat` return type changed from `i64` to
      `Option<i64>`. Engines implementing `ParquetStatsProvider` must wrap their existing return
      in `Some(num_rows)`.

### 🚀 Features / new APIs

1. Add infrastructures for sidecar splitting support ([#2271])
2. Add `extract_primitive_scalar` utility for Arrow-to-Scalar conversion ([#2368])
3. `ParquetHandler` auto-creates the target directory if it does not exist ([#2287])
4. Add schema validation for CREATE TABLE ([#2309])
5. Add row tracking support for create table ([#2317])
6. *(tests)* Add read-path integration tests for row tracking ([#2316])
7. Add Arrow batch-mode scan metadata FFI ([#2395])
8. Reject non-null columns in CREATE TABLE unconditionally ([#2404])
9. Auto-enable invariants writer feature for non-null columns in CREATE TABLE ([#2418])
10. Add ffi examples for cdf, create-table, write-table ([#2431])
11. Add high level api for timestamp conversion ([#900])
12. Collect nullCount statistics for array, map, and variant columns ([#2442])
13. Add AlterTable framework with add_column support ([#2387])
14. Add set_nullable support for ALTER TABLE ([#2388])
15. Allow materializePartitionColumns feature signal in CREATE TABLE ([#2481])

### 🐛 Bug Fixes

1. Acceptance test framework should reject negative snapshot version ([#2364])
2. Exclude partition columns from write-path stats collection ([#2362])
3. Add BYTE/SHORT support to stats verifier and GetData trait ([#2382])
4. Support presigned DB URLs ([#2398])
5. Ensure doctests are run in GitHub Actions ([#2412])
6. Missing PR link in the v0.21.0 CHANGELOG ([#2428])
7. Drop partitionValues_parsed in build_remove_transform ([#2429])
8. URI-encode Hive partition path for partitioned writes ([#2424])
9. Propagate null bitmap in evaluate_map_to_struct ([#2419])
10. Correct inaccuracies in ffi examples ([#2432])
11. Restore the Snapshot::new internal API ([#2425])
12. Clear stale CRC file in LogSegment::try_new_with_checkpoint ([#2457])
13. Keep name-based validation for column expressions with struct ([#2440])
14. Reuse LazyCrc in checkpoint early-return during incremental update ([#2329])
15. Add deserialization alias for file histogram ([#2489])

### 📚 Documentation

1. Remove references to default-engine feature ([#2417])

### 🚜 Refactor

1. Extract prerequisite schema constructions for `CheckpointWriter` ([#2313])
2. Change inconsistent kernel modules to use mod convention ([#2408])
3. Enforce line width and import ordering with nightly rustfmt ([#2383])
4. Add more test setup utils for partitioned write tests ([#2422])
5. Move data file methods behind SupportsDataFiles trait ([#2386])

### 🧪 Testing

1. Add CountingReporter integration tests across different scenarios ([#2194])
2. Predicate parser on acceptance workload harness ([#2215])
3. Add partition support for TestTableBuilder ([#2321])
4. Add FeatureSet methods for table builder ([#2283])
5. Split write.rs into topic-focused files ([#2460])
6. Consolidate write tests into single [[test]] binary ([#2472])
7. Collapse integration tests into a single binary ([#2477])
8. Migrate a test to use the create table builder ([#2482])

### ⚙️ Chores/CI

1. Don't generate unused Arrow schema. ([#2107])
2. Validate ascii only in PR body via CI job ([#2405])
3. Skip invalid handle code tests for coverage ([#2414])
4. PathMode ([#2360], [#2410]) and `WriteContext::partition_group_key` ([#2392], [#2403]) were
   added and reverted within this release. No net user-visible change.


[#2369]: https://github.com/delta-io/delta-kernel-rs/pull/2369
[#2364]: https://github.com/delta-io/delta-kernel-rs/pull/2364
[#2271]: https://github.com/delta-io/delta-kernel-rs/pull/2271
[#2194]: https://github.com/delta-io/delta-kernel-rs/pull/2194
[#2368]: https://github.com/delta-io/delta-kernel-rs/pull/2368
[#2362]: https://github.com/delta-io/delta-kernel-rs/pull/2362
[#2356]: https://github.com/delta-io/delta-kernel-rs/pull/2356
[#2287]: https://github.com/delta-io/delta-kernel-rs/pull/2287
[#2215]: https://github.com/delta-io/delta-kernel-rs/pull/2215
[#2382]: https://github.com/delta-io/delta-kernel-rs/pull/2382
[#2375]: https://github.com/delta-io/delta-kernel-rs/pull/2375
[#2309]: https://github.com/delta-io/delta-kernel-rs/pull/2309
[#2392]: https://github.com/delta-io/delta-kernel-rs/pull/2392
[#2317]: https://github.com/delta-io/delta-kernel-rs/pull/2317
[#2378]: https://github.com/delta-io/delta-kernel-rs/pull/2378
[#2398]: https://github.com/delta-io/delta-kernel-rs/pull/2398
[#2316]: https://github.com/delta-io/delta-kernel-rs/pull/2316
[#2403]: https://github.com/delta-io/delta-kernel-rs/pull/2403
[#2107]: https://github.com/delta-io/delta-kernel-rs/pull/2107
[#2061]: https://github.com/delta-io/delta-kernel-rs/pull/2061
[#2395]: https://github.com/delta-io/delta-kernel-rs/pull/2395
[#2360]: https://github.com/delta-io/delta-kernel-rs/pull/2360
[#2373]: https://github.com/delta-io/delta-kernel-rs/pull/2373
[#2313]: https://github.com/delta-io/delta-kernel-rs/pull/2313
[#2408]: https://github.com/delta-io/delta-kernel-rs/pull/2408
[#2410]: https://github.com/delta-io/delta-kernel-rs/pull/2410
[#2383]: https://github.com/delta-io/delta-kernel-rs/pull/2383
[#2412]: https://github.com/delta-io/delta-kernel-rs/pull/2412
[#2405]: https://github.com/delta-io/delta-kernel-rs/pull/2405
[#2404]: https://github.com/delta-io/delta-kernel-rs/pull/2404
[#2414]: https://github.com/delta-io/delta-kernel-rs/pull/2414
[#2385]: https://github.com/delta-io/delta-kernel-rs/pull/2385
[#2422]: https://github.com/delta-io/delta-kernel-rs/pull/2422
[#2417]: https://github.com/delta-io/delta-kernel-rs/pull/2417
[#2428]: https://github.com/delta-io/delta-kernel-rs/pull/2428
[#2429]: https://github.com/delta-io/delta-kernel-rs/pull/2429
[#2321]: https://github.com/delta-io/delta-kernel-rs/pull/2321
[#2424]: https://github.com/delta-io/delta-kernel-rs/pull/2424
[#2418]: https://github.com/delta-io/delta-kernel-rs/pull/2418
[#2419]: https://github.com/delta-io/delta-kernel-rs/pull/2419
[#2432]: https://github.com/delta-io/delta-kernel-rs/pull/2432
[#2430]: https://github.com/delta-io/delta-kernel-rs/pull/2430
[#2400]: https://github.com/delta-io/delta-kernel-rs/pull/2400
[#2425]: https://github.com/delta-io/delta-kernel-rs/pull/2425
[#2431]: https://github.com/delta-io/delta-kernel-rs/pull/2431
[#1822]: https://github.com/delta-io/delta-kernel-rs/pull/1822
[#900]: https://github.com/delta-io/delta-kernel-rs/pull/900
[#2442]: https://github.com/delta-io/delta-kernel-rs/pull/2442
[#2386]: https://github.com/delta-io/delta-kernel-rs/pull/2386
[#2283]: https://github.com/delta-io/delta-kernel-rs/pull/2283
[#2457]: https://github.com/delta-io/delta-kernel-rs/pull/2457
[#1893]: https://github.com/delta-io/delta-kernel-rs/pull/1893
[#2440]: https://github.com/delta-io/delta-kernel-rs/pull/2440
[#2460]: https://github.com/delta-io/delta-kernel-rs/pull/2460
[#2387]: https://github.com/delta-io/delta-kernel-rs/pull/2387
[#2472]: https://github.com/delta-io/delta-kernel-rs/pull/2472
[#2388]: https://github.com/delta-io/delta-kernel-rs/pull/2388
[#2481]: https://github.com/delta-io/delta-kernel-rs/pull/2481
[#2477]: https://github.com/delta-io/delta-kernel-rs/pull/2477
[#2329]: https://github.com/delta-io/delta-kernel-rs/pull/2329
[#2482]: https://github.com/delta-io/delta-kernel-rs/pull/2482
[#2489]: https://github.com/delta-io/delta-kernel-rs/pull/2489


## [v0.21.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.21.0/) (2026-04-10)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.20.0...v0.21.0)


### 🏗️ Breaking changes

1. Add partitioned variant to DataLayout enum ([#2145])
   - Adds `Partitioned` variant to `DataLayout` enum. Update match statements to handle the new variant.
2. Add create many API to engine ([#2070])
   - Adds `create_many` method to `ParquetHandler` trait. Implementors must add this method. See the trait rustdocs for details.
3. Rename uc-catalog and uc-client crates ([#2136])
   - `delta-kernel-uc-catalog` renamed to `delta-kernel-unity-catalog`. `delta-kernel-uc-client` renamed to `unity-catalog-delta-rest-client`. Update `Cargo.toml` dependencies accordingly.
4. Checksum and checkpoint APIs return updated Snapshot ([#2182])
   - `Snapshot::checkpoint()` and checksum APIs now return the updated `Snapshot`. Callers must handle the returned value.
5. Add P&M to CommitMetadata and enforce committer/table type matching ([#2250])
   - Enforces that committer type matches table type (catalog-managed vs path-based). Use appropriate committer for your table type.
6. Add UCCommitter validation for catalog-managed tables ([#2254])
   - `UCCommitter` now rejects commits to non-catalog-managed tables. Use `FileSystemCommitter` for path-based tables.
7. Refactor snapshot FFI to use builder pattern and enable snapshot reuse ([#2255])
   - FFI snapshot creation now uses builder pattern. Update FFI callers to use the new builder APIs.
8. Make tags and remove partition values allow null values in map ([#2281])
   - `tags` and `partitionValues` map values are now nullable. Update code that assumes non-null values.
9. Better naming style for column mapping related functions/variables ([#2290])
   - Renamed: `make_physical` to `to_physical_name`, `make_physical_struct` to `to_physical_schema`, `transform_struct_for_projection` to `projection_transform`. Update call sites.
10. Remove the catalog-managed feature flag ([#2310])
    - The `catalog-managed` feature flag is removed. Catalog-managed table support is now always available.
11. Update snapshot.checkpoint API to return a CheckpointResult ([#2314])
    - `Snapshot::checkpoint()` now returns `CheckpointResult` instead of `Snapshot`. Access the snapshot via `CheckpointResult::snapshot`.
12. Remove old non-builder snapshot FFI functions ([#2318])
    - Removed legacy FFI snapshot functions. Use the new builder-pattern FFI functions instead.
13. Support version 0 (table creation) commits in UCCommitter ([#2247])
    - Connectors using `UCCommitter` for table creation must now handle post-commit finalization via the UC create table API.
14. Pass computed ICT to CommitMetadata instead of wall-clock time ([#2319])
    - `CommitMetadata` now uses computed in-commit timestamp instead of wall-clock time. Callers relying on wall-clock timing should update accordingly.
15. Upgrade to arrow-58 and object_store-13, drop arrow-56 support ([#2116])
    - Minimum supported Arrow version is now arrow-57. Update your `Cargo.toml` if using `arrow-56` feature.
16. Crc File Histogram Read and Write Support ([#2235])
    - Adds `AddedHistogram` and `RemovedHistogram` fields to `FileStatsDelta` struct.
17. Add ScanMetadataCompleted metric event ([#2236])
    - Adds `ScanMetadataCompleted` variant to `MetricEvent` enum. Update metric reporters to handle the new variant.
18. Instrument JSON and Parquet handler reads with MetricsReporter ([#2169])
    - Adds `JsonReadCompleted` and `ParquetReadCompleted` variants to `MetricEvent` enum. Update metric reporters to handle new variants.
19. New transform helpers for unary and binary children ([#2150])
    - Removes public `CowExt` trait. Remove any usages of this trait.
20. New mod transforms for expression and schema transforms ([#2077])
    - Moves `SchemaTransform` and `ExpressionTransform` to new `transforms` module. Update import paths.
21. Introduce object_store compat shim ([#2111])
    - Renames `object_store` dependency to `object_store_12`. Update any direct references.
22. Consolidate domain metadata reads through Snapshot ([#2065])
    - Domain metadata reads now go through `Snapshot` methods. Update callers using old free functions.
23. Don't read or write arrow schema in parquet files ([#2025])
    - Parquet files no longer include arrow schema metadata. Code relying on this metadata must be updated.
24. Rename include_stats_columns to include_all_stats_columns ([#1996])
    - Renames `ScanBuilder::include_stats_columns()` to `ScanBuilder::include_all_stats_columns()`. Update call sites.

### 🚀 Features / new APIs

1. Add SQL -> Kernel predicate parser to benchmark framework ([#2099])
2. Add observability metrics for scan log replay ([#1866])
3. Filtered engine data visitor ([#1942])
4. Trigger benchmarking with comments ([#2089])
5. Unify data stats and partition values in DataSkippingFilter ([#1948])
6. Download benchmark workloads from DAT release ([#2163])
7. Add partitioned variant to DataLayout enum ([#2145])
8. Expose table_properties in FFI via visit_table_properties ([#2196])
9. Allow checkpoint stats properties in CREATE TABLE ([#2210])
10. Add crc file histogram initial struct and methods ([#2212])
11. BinaryPredicate evaluate expression with ArrowViewType. ([#2052])
12. Add acceptance workloads testing harness ([#2092])
13. Enable DeletionVectors table feature in CREATE TABLE ([#2245])
14. Checksum and checkpoint APIs return updated Snapshot ([#2182])
15. Adding ScanBuilder FFI functions for Scans ([#2237])
16. Add CountingReporter and fix metrics forwarding ([#2166])
17. Instrument JSON and Parquet handler reads with MetricsReporter ([#2169])
18. Wire CountingReporter into workload benchmarks ([#2171])
19. Add create many API to engine ([#2070])
20. Add ScanMetadataCompleted metric event ([#2236])
21. Allow AppendOnly, ChangeDataFeed, and TypeWidening in CREATE TABLE ([#2279])
22. Support max timestamp stats for data skipping ([#2249])
23. Add list with backward checkpoint scan ([#2174])
24. Add Snapshot::get_timestamp ([#2266])
25. Make tags  and remove partition values allow null values in map ([#2281])
26. Support UC credential vending and S3 benchmarks ([#2109])
27. Add catalogManaged to allowed features in CREATE TABLE ([#2293])
28. Add catalog-managed table creation utilities ([#2203])
29. Support version 0 (table creation) commits in UCCommitter ([#2247])
30. Update snapshot.checkpoint API to return a CheckpointResult ([#2314])
31. Cached checkpoint output schema ([#2270])
32. Refactor snapshot FFI to use builder pattern and enable snapshot reuse ([#2255])
33. Add P&M to CommitMetadata and enforce committer/table type matching ([#2250])
34. Add UCCommitter validation for catalog-managed tables ([#2254])
35. Crc File Histogram Read and Write Support ([#2235])
36. Add FFI function to expose snapshot's timestamp ([#2274])
37. Add FFI create table DDL functions ([#2296])
38. Add FFI remove files DML functions ([#2297])
39. Expose Protocol and Metadata as opaque FFI handle types ([#2260])
40. Add FFI bindings for domain metadata write operations ([#2327])

### 🐛 Bug Fixes

1. Treat null literal as unknown in meta-predicate evaluation ([#2097])
2. Update TokioBackgroundExecutor to join thread instead of detaching ([#2126])
3. Use thread pools and multi-thread tokio executor in read metadata benchmark runner ([#2044])
4. Emit null stats for all-null columns instead of omitting them ([#2187])
5. Allow Date/Timestamp casting for stats_parsed compatibility ([#2074])
6. Filter evaluator input schema ([#2195])
7. SnapshotCompleted.total_duration now includes log segment loading ([#2183])
8. Avoid creating empty stats schemas ([#2199])
9. Prevent dual TLS crypto backends from reqwest default features ([#2178])
10. Vendor and pin homebrew actions ([#2243])
11. Validate min_reader/writer_version are at least 1 ([#2202])
12. Preserve loaded LazyCrc during incremental snapshot updates ([#2211])
13. Detect stats_parsed in multi-part V1 checkpoints ([#2214])
14. Downgrade per-batch data skipping log from info to debug ([#2219])
15. Unknown table features in feature list are "supported" ([#2159])
16. Remove debug_assert_eq before require in scan evaluator row count checks ([#2262])
17. Adopt checkpoint written later for same-version snapshot refresh ([#2143])
18. Return error when parquet handler returns empty data for scan files ([#2261])
19. Refactor benchmarking workflow to not require criterion compare action ([#2264])
20. Skip name-based validation for struct columns in expression evaluator ([#2160])
21. Handle missing leaf columns in nested struct during parquet projection ([#2170])
22. Pass computed ICT to CommitMetadata instead of wall-clock time ([#2319])
23. Detect and handle empty (0-byte) log files during listing ([#2336])

### 📚 Documentation

1. Update claude readme to include github actions safety note ([#2190])
2. Add line width and comment divider style rules to CLAUDE.md ([#2277])
3. Add documentation for current tags ([#2234])
4. Document benchmarking in CI accuracy ([#2302])

### ⚡ Performance

1. Pre-size dedup HashSet in ScanLogReplayProcessor ([#2186])
2. Pre-size HashMap in ArrowEngineData::visit_rows ([#2185])
3. Remove dead schema conversions in expression evaluators ([#2184])

### 🚜 Refactor

1. Finalized benchmark table names and added new tables ([#2072])
2. New transform helpers for unary and binary children ([#2150])
3. Remove legacy row-level partition filter path ([#2158])
4. Restructured list log files function ([#2173])
5. Consolidate and add testing for set transaction expiration ([#2176])
6. Rename uc-catalog and uc-client crates ([#2136])
7. Better naming style for column mapping related functions/variables ([#2290])
8. Centralize computation for physical schema without partition columns ([#2142])
9. Consolidate FFI test setup helpers into ffi_test_utils ([#2307])
10. *(action_reconciliation)* Combine getter index and field name constants ([#1717]) ([#1774])
11. Extract shared stat helpers from RowGroupFilter ([#2324])
12. Extract WriteContext to its own file ([#2349])

### ⚙️ Chores/CI

1. Clean up arrow deps in cargo files ([#2115])
2. Commit Cargo.lock and enforce --locked in all CI workflows ([#2240])
3. Harden pr-title-validator a bit ([#2246])
4. Renable semver ([#2248])
5. Attempt fixup of semver-label job ([#2253])
6. Use artifacts for semver label ([#2258])
7. Remove old non-builder snapshot FFI functions ([#2318])
8. Remove the catalog-managed feature flag ([#2310])
9. Upgrade to arrow-58 and object_store-13, drop arrow-56 support ([#2116])

### Other

[#2097]: https://github.com/delta-io/delta-kernel-rs/pull/2097
[#2099]: https://github.com/delta-io/delta-kernel-rs/pull/2099
[#2126]: https://github.com/delta-io/delta-kernel-rs/pull/2126
[#2115]: https://github.com/delta-io/delta-kernel-rs/pull/2115
[#1866]: https://github.com/delta-io/delta-kernel-rs/pull/1866
[#2044]: https://github.com/delta-io/delta-kernel-rs/pull/2044
[#1942]: https://github.com/delta-io/delta-kernel-rs/pull/1942
[#2072]: https://github.com/delta-io/delta-kernel-rs/pull/2072
[#2089]: https://github.com/delta-io/delta-kernel-rs/pull/2089
[#2187]: https://github.com/delta-io/delta-kernel-rs/pull/2187
[#2190]: https://github.com/delta-io/delta-kernel-rs/pull/2190
[#1948]: https://github.com/delta-io/delta-kernel-rs/pull/1948
[#2150]: https://github.com/delta-io/delta-kernel-rs/pull/2150
[#2074]: https://github.com/delta-io/delta-kernel-rs/pull/2074
[#2195]: https://github.com/delta-io/delta-kernel-rs/pull/2195
[#2158]: https://github.com/delta-io/delta-kernel-rs/pull/2158
[#2186]: https://github.com/delta-io/delta-kernel-rs/pull/2186
[#2185]: https://github.com/delta-io/delta-kernel-rs/pull/2185
[#2173]: https://github.com/delta-io/delta-kernel-rs/pull/2173
[#2163]: https://github.com/delta-io/delta-kernel-rs/pull/2163
[#2145]: https://github.com/delta-io/delta-kernel-rs/pull/2145
[#2184]: https://github.com/delta-io/delta-kernel-rs/pull/2184
[#2183]: https://github.com/delta-io/delta-kernel-rs/pull/2183
[#2199]: https://github.com/delta-io/delta-kernel-rs/pull/2199
[#2196]: https://github.com/delta-io/delta-kernel-rs/pull/2196
[#2210]: https://github.com/delta-io/delta-kernel-rs/pull/2210
[#2178]: https://github.com/delta-io/delta-kernel-rs/pull/2178
[#2240]: https://github.com/delta-io/delta-kernel-rs/pull/2240
[#2243]: https://github.com/delta-io/delta-kernel-rs/pull/2243
[#2202]: https://github.com/delta-io/delta-kernel-rs/pull/2202
[#2211]: https://github.com/delta-io/delta-kernel-rs/pull/2211
[#2214]: https://github.com/delta-io/delta-kernel-rs/pull/2214
[#2246]: https://github.com/delta-io/delta-kernel-rs/pull/2246
[#2219]: https://github.com/delta-io/delta-kernel-rs/pull/2219
[#2212]: https://github.com/delta-io/delta-kernel-rs/pull/2212
[#2176]: https://github.com/delta-io/delta-kernel-rs/pull/2176
[#2159]: https://github.com/delta-io/delta-kernel-rs/pull/2159
[#2248]: https://github.com/delta-io/delta-kernel-rs/pull/2248
[#2253]: https://github.com/delta-io/delta-kernel-rs/pull/2253
[#2052]: https://github.com/delta-io/delta-kernel-rs/pull/2052
[#2092]: https://github.com/delta-io/delta-kernel-rs/pull/2092
[#2258]: https://github.com/delta-io/delta-kernel-rs/pull/2258
[#2136]: https://github.com/delta-io/delta-kernel-rs/pull/2136
[#2245]: https://github.com/delta-io/delta-kernel-rs/pull/2245
[#2182]: https://github.com/delta-io/delta-kernel-rs/pull/2182
[#2262]: https://github.com/delta-io/delta-kernel-rs/pull/2262
[#2237]: https://github.com/delta-io/delta-kernel-rs/pull/2237
[#2166]: https://github.com/delta-io/delta-kernel-rs/pull/2166
[#2169]: https://github.com/delta-io/delta-kernel-rs/pull/2169
[#2171]: https://github.com/delta-io/delta-kernel-rs/pull/2171
[#2143]: https://github.com/delta-io/delta-kernel-rs/pull/2143
[#2070]: https://github.com/delta-io/delta-kernel-rs/pull/2070
[#2261]: https://github.com/delta-io/delta-kernel-rs/pull/2261
[#2277]: https://github.com/delta-io/delta-kernel-rs/pull/2277
[#2236]: https://github.com/delta-io/delta-kernel-rs/pull/2236
[#2279]: https://github.com/delta-io/delta-kernel-rs/pull/2279
[#2249]: https://github.com/delta-io/delta-kernel-rs/pull/2249
[#2290]: https://github.com/delta-io/delta-kernel-rs/pull/2290
[#2174]: https://github.com/delta-io/delta-kernel-rs/pull/2174
[#2264]: https://github.com/delta-io/delta-kernel-rs/pull/2264
[#2234]: https://github.com/delta-io/delta-kernel-rs/pull/2234
[#2302]: https://github.com/delta-io/delta-kernel-rs/pull/2302
[#2142]: https://github.com/delta-io/delta-kernel-rs/pull/2142
[#2266]: https://github.com/delta-io/delta-kernel-rs/pull/2266
[#2281]: https://github.com/delta-io/delta-kernel-rs/pull/2281
[#2109]: https://github.com/delta-io/delta-kernel-rs/pull/2109
[#2293]: https://github.com/delta-io/delta-kernel-rs/pull/2293
[#2203]: https://github.com/delta-io/delta-kernel-rs/pull/2203
[#2247]: https://github.com/delta-io/delta-kernel-rs/pull/2247
[#2160]: https://github.com/delta-io/delta-kernel-rs/pull/2160
[#2314]: https://github.com/delta-io/delta-kernel-rs/pull/2314
[#2270]: https://github.com/delta-io/delta-kernel-rs/pull/2270
[#2255]: https://github.com/delta-io/delta-kernel-rs/pull/2255
[#2250]: https://github.com/delta-io/delta-kernel-rs/pull/2250
[#2254]: https://github.com/delta-io/delta-kernel-rs/pull/2254
[#2307]: https://github.com/delta-io/delta-kernel-rs/pull/2307
[#2170]: https://github.com/delta-io/delta-kernel-rs/pull/2170
[#2235]: https://github.com/delta-io/delta-kernel-rs/pull/2235
[#2274]: https://github.com/delta-io/delta-kernel-rs/pull/2274
[#1774]: https://github.com/delta-io/delta-kernel-rs/pull/1774
[#2296]: https://github.com/delta-io/delta-kernel-rs/pull/2296
[#2318]: https://github.com/delta-io/delta-kernel-rs/pull/2318
[#2310]: https://github.com/delta-io/delta-kernel-rs/pull/2310
[#2297]: https://github.com/delta-io/delta-kernel-rs/pull/2297
[#2324]: https://github.com/delta-io/delta-kernel-rs/pull/2324
[#2260]: https://github.com/delta-io/delta-kernel-rs/pull/2260
[#2327]: https://github.com/delta-io/delta-kernel-rs/pull/2327
[#2319]: https://github.com/delta-io/delta-kernel-rs/pull/2319
[#2116]: https://github.com/delta-io/delta-kernel-rs/pull/2116
[#2349]: https://github.com/delta-io/delta-kernel-rs/pull/2349
[#2336]: https://github.com/delta-io/delta-kernel-rs/pull/2336
[#2077]: https://github.com/delta-io/delta-kernel-rs/pull/2077                                                                                               
[#2111]: https://github.com/delta-io/delta-kernel-rs/pull/2111                                                                                                 
[#2065]: https://github.com/delta-io/delta-kernel-rs/pull/2065                                                                                               
[#2025]: https://github.com/delta-io/delta-kernel-rs/pull/2025                                                                                               
[#1996]: https://github.com/delta-io/delta-kernel-rs/pull/1996
[#1717]: https://github.com/delta-io/delta-kernel-rs/pull/1717
[#1922]: https://github.com/delta-io/delta-kernel-rs/pull/1922

## [v0.20.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.20.0/) (2026-02-26)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.19.2...v0.20.0)

### 🏗️ Breaking changes
1. Remove `DefaultEngine::new` ([#1583])
   - Use `DefaultEngineBuilder` instead like: `DefaultEngineBuilder::new(store).build()`
2. Add ParseJson expression  ([#1586])
   - Implementors of the ExpressionHandler trait now need to handle this expression
3. Change CommitResponse::Committed to return a FileMeta ([#1599])
   - Committer implementations must now return a FileMeta of the written file after each commit, instead of only returning the committed version
4. Add stats_columns to ParquetHandler ([#1668])
    - Add stat_columns to `write_parquet_file` engine implementation, which specifies the columns to collect Delta stats on
5. Add StatisticsCollector core with numRecords ([#1662])
    - Renames `_stat_columns` above to `stat_columns`
6. Return updated Snapshot from `Snapshot::publish` ([#1694])
    - Snapshot::publish now takes self: Arc<Self> and returns DeltaResult<SnapshotRef> instead of ()
7. Pass engine to Snapshot::transaction() for domain metadata access ([#1707])
    - Snapshot::transaction() now requires an engine: &dyn Engine parameter to read domain metadata
8. Add tracing instrumentation to transaction and snapshot operations ([#1772])
    - snapshot and transaction have both stopped implementing auto traits UnwindSafe and RefUnwindSafe due to storing new instrumentation span fields
9. Use physical stats column names in `WriteContext` ([#1836])
    - `WriteContext.stats_columns` now uses _physical_ column names per column mapping. Ref: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping
10. Generate `physical_schema` in `WriteContext` w.r.t column mapping and `materializePartitionColumns` ([#1837])
    - `WriteContext.physical_schema` now respects column mapping, and retains partition columns when `materializePartitionColumns` is enabled. Ref: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping
11. Fix get_app_id_version to take &self ([#1770])
    - If you are calling `get_app_id` pass a reference to the `Snapshot` not `Arc<Snapshot>`
12. Add ability to  'enter' the runtime to the default engine ([#1847])
    - Implementors of the `TaskExecutor` trait now need to support this

### 🚀 Features / new APIs
1. Add doctests for `IntoEngineData` derive macro ([#1580])
2. Create `DefaultEngineBuilder` to build `DefaultEngine` ([#1582])
3. Implement `Scalar::From<HashMap<K, V>>` ([#1541])
4. Add `logSegment.new_with_commit_appended` API ([#1602])
5. `snapshot.new_post_commit` ([#1604])
   - Creates a new Snapshot reflecting a just-committed transaction without re-reading the log
6. Enable Arrow to convert nullable StructArray to RecordBatch ([#1635])
7. Add `snapshot.checkpoint()` for all-in-one checkpointing ([#1600])
8. Add a tracing statement to print table configuration for each version ([#1634])
9. Add CheckpointDeduplicator for checkpoint phase of distributed log replay ([#1538])
10. Add CreateTable API with simplified single-stage flow ([#1629])
11. Add with_table_properties method to CreateTableTransactionBuilder ([#1649])
12. Add post-commit Snapshot to txn ([#1633])
13. Add CDF tracing for Phase 1 of Change Data feed ([#1654])
14. Make Sequential phase schema only contain add and remove actions ([#1679])
15. Add executor for distributed log replay  ([#1539])
16. Transaction stats API ([#1658])
17. `Snapshot::publish` API with e2e in-memory UC test ([#1628])
18. Expose a `Snapshot::get_domain_metadata_internal` API, guarded by `internal-api` feature flag ([#1692])
19. Add nullCount support to StatisticsCollector ([#1663])
20. Add minValues and maxValues support to StatisticsCollector ([#1664])
21. Enable NullCount collection for complex data types ([#1706])
22. Implement schema diffing for flat schemas (2/5]) ([#1478])
23. Add API on Scan to perform 2-phase log replay  ([#1547])
24. Enable distributed log replay serde serialization for serializable scan state ([#1549])
25. Add InCommitTimestamp support to ChangeDataFeed ([#1670])
26. Add include_stats_columns API and output_stats_schema field ([#1728])
27. Add write support for clustered tables behind feature flag ([#1704])
28. Add snapshot load instrumentation ([#1750])
29. Create table builder and domain metadata handling ([#1762])
30. Add crc module with schema, visitor, reader, and lazy loader ([#1780])
31. Add clustering support for CREATE TABLE ([#1763])
32. Support owned runtime in `TokioMultiThreadExecutor` ([#1719])
33. *(transaction)* Support blind append commit metadata ([#1783])
    - Adds `set_is_blind_append()` API to `Transaction`, includes `isBlindAppend` in generated `CommitInfo`, and validates blind-append semantics (add-only, no removals/DV updates, `dataChange` must be true) before commit.
34. Add stats transform module for checkpoint stats population ([#1646])
35. Refactor data skipping to use stats_parsed directly ([#1715])
36. Support using stats_columns and predicate together in scans ([#1691])
38. Support creation of `DefaultEngine` with `TokioMultiThreadExecutor` in FFI ([#1755])
39. Add column mapping support for CREATE TABLE ([#1764])
40. Write parsed stats in checkpoints ([#1643])
41. Implement ReadConfig for Benchmark Framework ([#1758])
42. Implement TableInfo Deserialization for Benchmark Framework ([#1759])
43. Implement Read Spec Deserialization for Benchmark Framework ([#1760])
44. Allow visitors to visit REE Arrow columns. ([#1829])
45. *(committer)* Add tracing instrumentation to FileSystemCommitter::commit ([#1811])
46. Try and cache brew packages to speed up CI ([#1909])
47. Extend GetData with float, double, date, timestamp, decimal types ([#1901])
48. Define and use constants for protocol (3,7]) ([#1917])
49. Generate transform in `WriteContext` w.r.t column mapping ([#1862])
50. Support v2 checkpoints in create_table API ([#1864])
51. Expand add files schema to include all stats fields ([#1748])
52. Support write with both partition columns and column mapping in `DefaultEngine` ([#1870])
53. Feat: support scanning for multiple specific domains in domain metadata replay ([#1881])
    - Allows callers to request multiple domain names in a single metadata replay pass, with early termination once all requested domains are found. Includes optimized skip of domain metadata fields when a domain has already been seen in a newer commit.
54. Allow ffi for uc_catalog stuff ([#1711])
55. Support column mapping on writes ([#1863])
56. Coerce parquet read nullability to match table schema ([#1903])
57. Relax clustering column constraints to align with Delta protocol ([#1913])
58. Auto-enable variantType feature during CREATE TABLE ([#1922]) ([#1949])
59. Add type validation for `evaluate_expression` ([#1575])
60. Use ReaderBuilder::with_coerce_primitive when parsing JSON ([#1651])
61. Allow to change tracing level and callback more than once ([#1111])
62. Simplify checkpoint-table with Snapshot::checkpoint ([#1813])
63. Add size metadata to the CdfScanFile ([#1935])
64. Add deletion vector APIs to transaction ([#1430])
65. Include max known published commit version inside of `LogSegment` ([#1587])
66. Use CRC for In-Commit-Timestamp reading ([#1806])
67. Refactor `ListedLogFiles::try_new` to be more extensible and with default values by using builder pattern ([#1585])
68. Implement the read metadata workload runner ([#1919])
69. Provide expected stats schema ([#1592])
70. Add checkpoint schema discovery for stats_parsed detection ([#1550])
71. Add function to check if schema supports parsed stats ([#1573])
72. Read parsed-stats from checkpoint ([#1638])
73. feat: add get clustering columns in transactions ([#1693])
74. Change expected_stats_schema to return logical schema + physical schema ([#1749])
75. Add support for outputting parsed file statistics to scan batches ([#1720])
76. Checkpoint and sidecar row group skipping via stats_parsed ([#1853])
77. Add serialization/deserialization support for Predicates and Expressions ([#1543])
78. Distributed Log Replay serialization/deserialization ([#1503])
79. Introduce Deduplicator trait to unify mutable and immutable deduplication ([#1537])
80. Add ffi api to perform a checkpoint ([#1619])

### 🐛 Bug Fixes

1. Make parquet read actually use the executor ([#1596])
2. Deadlock for `TokioMultiThreadExecutor` ([#1606])
3. Remove `breaking-change` tag after semver passes ([#1621])
4. Enable arrow conversion from Int96 ([#1653])
5. Preserve null bitmap in nested transform expressions ([#1645])
6. Include domain metadata in checkpoints ([#1718])
- Domain metadata was not being written to checkpoint files, causing it to be lost after checkpoints
7. Propagate struct-level nulls when computing nested column stats ([#1745])
8. Express One Zone URLs do not support lexicographical ordering ([#1753])
9. Preserve non-commit files (CRC, checkpoints, compactions) at log tail versions ([#1817])
    - Fixes `list_log_files` to no longer discard CRC, checkpoint, and compaction files at the log tail boundary, ensuring these auxiliary files are preserved alongside their commit files.
10. Fix Miri CI failure by cleaning stale Miri artifacts before test run ([#1845])
11. Strip parquet field IDs from physical stats schema for checkpoint reading ([#1839])
12. Unify v2 checkpoint batch schemas ([#1833])
13. Improve performance and correctness of EngineMap implementation in default engine ([#1785])
14. Parquet footer skipping cannot trust nullcount=0 stat ([#1914])
15. Column extraction for visitors should not rely on schema order ([#1818])
16. Ensure consistent usage of parquet.field.id and conversion to PARQUET:field_id in kernel/default engine ([#1850])
17. Make log segment merging in `Snapshot::try_new_from` deduplicate compaction files ([#1954])

### ⚡ Performance

1. Pre-allocate Vecs and HashSets when size is known ([#1676])
2. Add skip_stats option to skip reading file statistics ([#1738])
3. Use CRC in Protocol + Metadata log replay ([#1790])

### 🚜 Refactor

1. Move doctest into mods ([#1574])
2. Deny panics in ffi crate ([#1576])
3. Extract shared HTTP utilities to http.rs ([#1590])
4. Rename `Snapshot.checkpoint` ([#1608])
5. Extract stats from `ActionReconciliationIterator` ([#1618])
6. Cleanup repeated schema definitions in `kernel/tests/write.rs` ([#1637])
7. Split `committer.rs` into multiple files ([#1622])
8. Consolidate nullable stat transforms ([#1636])
9. Add Expression::coalesce helper method ([#1648])
10. Add checkpoint info to ScanLogReplayProcessor ([#1752])
11. Extract protocol & metadata replay into log_segment submodule ([#1782])
12. Define constants for table property keys ([#1797])
    - Replaces scattered string literals for Delta table property keys (e.g. `delta.appendOnly`, `delta.enableChangeDataFeed`) with named constants, improving maintainability and preventing typos.
13. Update metadata schema to be a SchemaRef and add appropriate Arcs ([#1802])
14. Rename `set_is_blind_append` to `with_blind_append`, returning `Self` ([#1838])
    - Adopts builder-style API for the blind append flag, allowing method chaining (e.g. `txn.with_blind_append(true).commit(...)`).
15. Extract clustering tests into sub-module ([#1828])
16. Split `UCCommitsClient` into `UCCommitClient` and `UCGetCommitsClient` ([#1854])
    - Separates the Unity Catalog commits client into two focused traits — one for committing and one for reading commits — enabling cleaner dependency boundaries and testability.
17. Use type-state pattern for `CreateTableTransaction` compile-time API safety ([#1842])
    - Encodes the create-table workflow states (building → ready → committed) in the type system, so invalid transitions (e.g. committing before setting schema) are caught at compile time. Reorganizes create-table code and moves tests to integration tests.
18. Simplify table feature parsing ([#1878])
19. Define and use new TableConfiguration methods ([#1905])
20. Improve Protocol::try_new and make tests call it reliably ([#1907])
21. Simplify GetData impls with bool::then() ([#1918])
22. Split transaction module into `mod.rs` and `update.rs` ([#1877])
    - Breaks the growing transaction module into separate files: core transaction logic in `mod.rs` and update/DV-related logic in `update.rs`, improving navigability.
23. Rename FeatureType::Writer as WriterOnly ([#1934])
24. Clean up TableConfiguration validation and unit tests ([#1947])
26. StructType modification method and stat_transform schema boilerplate code refactor. ([#1872])

### 🧪 Testing

1. In-Memory UC-Commits-Client ([#1644])
2. Add test for post_commit_snapshot with create table API ([#1680])
3. Add rs-test support ([#1708])
4. Add test validating collect_stats() output against Spark ([#1778])
5. Add test for parquet id when CM enabled ([#1946])
6. [Test Only] Minor refactor to log_segment tests ([#1581]
7. Add file size to the unit test of Engine's ParquetReader ([#1921])

### ⚙️ Chores/CI
1. Remove unnecessary spaces in PR description ([#1598])
2. Upgrade to reqwest 0.13 and rustls as default ([#1588])
3. Stats-schema improvements ([#1642])
4. Add Rust caching to build and test jobs ([#1672])
5. Use cargo-nextest for parallel test execution ([#1673])
- ~19x faster locally via per-test process isolation
6. Fix ffi_test cache miss by using consistent toolchain action ([#1702])
7. Add caching and optimize tool installation across all jobs ([#1674])
8. Remove unused remove metadata ([#1732])
9. Prefer `append_value_n` over `append_value` ([#1868])
10. Pin native-tls to 0.2.16 due to upstream breakage ([#1880])
11. Fix unit tests with bad protocol versions ([#1879])
12. Add nextest support for miri tests ([#1685])
13. Unpin Miri nightly toolchain ([#1900])
14. Bring 0.19.1 changes into main ([#1632])
15. Remove comfy-table dependency declaration ([#1860])
16. Update review policy in CONTRIBUTING.md ([#1945])
17. Revert "chore: pin native-tls to 0.2.16 due to upstream breakage" ([#1915])

### Other
4. Remove comments and text from `pull_request_template.md` ([#1589])

[#1581]: https://github.com/delta-io/delta-kernel-rs/pull/1581
[#1585]: https://github.com/delta-io/delta-kernel-rs/pull/1585
[#1575]: https://github.com/delta-io/delta-kernel-rs/pull/1575
[#1574]: https://github.com/delta-io/delta-kernel-rs/pull/1574
[#1550]: https://github.com/delta-io/delta-kernel-rs/pull/1550
[#1576]: https://github.com/delta-io/delta-kernel-rs/pull/1576
[#1589]: https://github.com/delta-io/delta-kernel-rs/pull/1589
[#1430]: https://github.com/delta-io/delta-kernel-rs/pull/1430
[#1580]: https://github.com/delta-io/delta-kernel-rs/pull/1580
[#1582]: https://github.com/delta-io/delta-kernel-rs/pull/1582
[#1590]: https://github.com/delta-io/delta-kernel-rs/pull/1590
[#1598]: https://github.com/delta-io/delta-kernel-rs/pull/1598
[#1583]: https://github.com/delta-io/delta-kernel-rs/pull/1583
[#1591]: https://github.com/delta-io/delta-kernel-rs/pull/1591
[#1587]: https://github.com/delta-io/delta-kernel-rs/pull/1587
[#1586]: https://github.com/delta-io/delta-kernel-rs/pull/1586
[#1596]: https://github.com/delta-io/delta-kernel-rs/pull/1596
[#1541]: https://github.com/delta-io/delta-kernel-rs/pull/1541
[#1588]: https://github.com/delta-io/delta-kernel-rs/pull/1588
[#1599]: https://github.com/delta-io/delta-kernel-rs/pull/1599
[#1573]: https://github.com/delta-io/delta-kernel-rs/pull/1573
[#1609]: https://github.com/delta-io/delta-kernel-rs/pull/1609
[#1606]: https://github.com/delta-io/delta-kernel-rs/pull/1606
[#1608]: https://github.com/delta-io/delta-kernel-rs/pull/1608
[#1543]: https://github.com/delta-io/delta-kernel-rs/pull/1543
[#1592]: https://github.com/delta-io/delta-kernel-rs/pull/1592
[#1503]: https://github.com/delta-io/delta-kernel-rs/pull/1503
[#1602]: https://github.com/delta-io/delta-kernel-rs/pull/1602
[#1537]: https://github.com/delta-io/delta-kernel-rs/pull/1537
[#1621]: https://github.com/delta-io/delta-kernel-rs/pull/1621
[#1618]: https://github.com/delta-io/delta-kernel-rs/pull/1618
[#1604]: https://github.com/delta-io/delta-kernel-rs/pull/1604
[#1637]: https://github.com/delta-io/delta-kernel-rs/pull/1637
[#1622]: https://github.com/delta-io/delta-kernel-rs/pull/1622
[#1651]: https://github.com/delta-io/delta-kernel-rs/pull/1651
[#1635]: https://github.com/delta-io/delta-kernel-rs/pull/1635
[#1636]: https://github.com/delta-io/delta-kernel-rs/pull/1636
[#1600]: https://github.com/delta-io/delta-kernel-rs/pull/1600
[#1619]: https://github.com/delta-io/delta-kernel-rs/pull/1619
[#1653]: https://github.com/delta-io/delta-kernel-rs/pull/1653
[#1642]: https://github.com/delta-io/delta-kernel-rs/pull/1642
[#1645]: https://github.com/delta-io/delta-kernel-rs/pull/1645
[#1648]: https://github.com/delta-io/delta-kernel-rs/pull/1648
[#1634]: https://github.com/delta-io/delta-kernel-rs/pull/1634
[#1625]: https://github.com/delta-io/delta-kernel-rs/pull/1625
[#1538]: https://github.com/delta-io/delta-kernel-rs/pull/1538
[#1626]: https://github.com/delta-io/delta-kernel-rs/pull/1626
[#1672]: https://github.com/delta-io/delta-kernel-rs/pull/1672
[#1673]: https://github.com/delta-io/delta-kernel-rs/pull/1673
[#1629]: https://github.com/delta-io/delta-kernel-rs/pull/1629
[#1649]: https://github.com/delta-io/delta-kernel-rs/pull/1649
[#1633]: https://github.com/delta-io/delta-kernel-rs/pull/1633
[#1654]: https://github.com/delta-io/delta-kernel-rs/pull/1654
[#1679]: https://github.com/delta-io/delta-kernel-rs/pull/1679
[#1644]: https://github.com/delta-io/delta-kernel-rs/pull/1644
[#1680]: https://github.com/delta-io/delta-kernel-rs/pull/1680
[#1539]: https://github.com/delta-io/delta-kernel-rs/pull/1539
[#1658]: https://github.com/delta-io/delta-kernel-rs/pull/1658
[#1668]: https://github.com/delta-io/delta-kernel-rs/pull/1668
[#1662]: https://github.com/delta-io/delta-kernel-rs/pull/1662
[#1628]: https://github.com/delta-io/delta-kernel-rs/pull/1628
[#1692]: https://github.com/delta-io/delta-kernel-rs/pull/1692
[#1111]: https://github.com/delta-io/delta-kernel-rs/pull/1111
[#1702]: https://github.com/delta-io/delta-kernel-rs/pull/1702
[#1674]: https://github.com/delta-io/delta-kernel-rs/pull/1674
[#1663]: https://github.com/delta-io/delta-kernel-rs/pull/1663
[#1694]: https://github.com/delta-io/delta-kernel-rs/pull/1694
[#1707]: https://github.com/delta-io/delta-kernel-rs/pull/1707
[#1664]: https://github.com/delta-io/delta-kernel-rs/pull/1664
[#1706]: https://github.com/delta-io/delta-kernel-rs/pull/1706
[#1478]: https://github.com/delta-io/delta-kernel-rs/pull/1478
[#1547]: https://github.com/delta-io/delta-kernel-rs/pull/1547
[#1718]: https://github.com/delta-io/delta-kernel-rs/pull/1718
[#1549]: https://github.com/delta-io/delta-kernel-rs/pull/1549
[#1638]: https://github.com/delta-io/delta-kernel-rs/pull/1638
[#1693]: https://github.com/delta-io/delta-kernel-rs/pull/1693
[#1732]: https://github.com/delta-io/delta-kernel-rs/pull/1732
[#1745]: https://github.com/delta-io/delta-kernel-rs/pull/1745
[#1670]: https://github.com/delta-io/delta-kernel-rs/pull/1670
[#1749]: https://github.com/delta-io/delta-kernel-rs/pull/1749
[#1728]: https://github.com/delta-io/delta-kernel-rs/pull/1728
[#1752]: https://github.com/delta-io/delta-kernel-rs/pull/1752
[#1753]: https://github.com/delta-io/delta-kernel-rs/pull/1753
[#1704]: https://github.com/delta-io/delta-kernel-rs/pull/1704
[#1720]: https://github.com/delta-io/delta-kernel-rs/pull/1720
[#1750]: https://github.com/delta-io/delta-kernel-rs/pull/1750
[#1772]: https://github.com/delta-io/delta-kernel-rs/pull/1772
[#1708]: https://github.com/delta-io/delta-kernel-rs/pull/1708
[#1762]: https://github.com/delta-io/delta-kernel-rs/pull/1762
[#1782]: https://github.com/delta-io/delta-kernel-rs/pull/1782
[#1780]: https://github.com/delta-io/delta-kernel-rs/pull/1780
[#1770]: https://github.com/delta-io/delta-kernel-rs/pull/1770
[#1797]: https://github.com/delta-io/delta-kernel-rs/pull/1797
[#1763]: https://github.com/delta-io/delta-kernel-rs/pull/1763
[#1719]: https://github.com/delta-io/delta-kernel-rs/pull/1719
[#1783]: https://github.com/delta-io/delta-kernel-rs/pull/1783
[#1802]: https://github.com/delta-io/delta-kernel-rs/pull/1802
[#1778]: https://github.com/delta-io/delta-kernel-rs/pull/1778
[#1646]: https://github.com/delta-io/delta-kernel-rs/pull/1646
[#1817]: https://github.com/delta-io/delta-kernel-rs/pull/1817
[#1715]: https://github.com/delta-io/delta-kernel-rs/pull/1715
[#1691]: https://github.com/delta-io/delta-kernel-rs/pull/1691
[#1838]: https://github.com/delta-io/delta-kernel-rs/pull/1838
[#1790]: https://github.com/delta-io/delta-kernel-rs/pull/1790
[#1845]: https://github.com/delta-io/delta-kernel-rs/pull/1845
[#1839]: https://github.com/delta-io/delta-kernel-rs/pull/1839
[#1833]: https://github.com/delta-io/delta-kernel-rs/pull/1833
[#1785]: https://github.com/delta-io/delta-kernel-rs/pull/1785
[#1755]: https://github.com/delta-io/delta-kernel-rs/pull/1755
[#1764]: https://github.com/delta-io/delta-kernel-rs/pull/1764
[#1828]: https://github.com/delta-io/delta-kernel-rs/pull/1828
[#1643]: https://github.com/delta-io/delta-kernel-rs/pull/1643
[#1758]: https://github.com/delta-io/delta-kernel-rs/pull/1758
[#1854]: https://github.com/delta-io/delta-kernel-rs/pull/1854
[#1853]: https://github.com/delta-io/delta-kernel-rs/pull/1853
[#1868]: https://github.com/delta-io/delta-kernel-rs/pull/1868
[#1880]: https://github.com/delta-io/delta-kernel-rs/pull/1880
[#1759]: https://github.com/delta-io/delta-kernel-rs/pull/1759
[#1760]: https://github.com/delta-io/delta-kernel-rs/pull/1760
[#1842]: https://github.com/delta-io/delta-kernel-rs/pull/1842
[#1878]: https://github.com/delta-io/delta-kernel-rs/pull/1878
[#1879]: https://github.com/delta-io/delta-kernel-rs/pull/1879
[#1685]: https://github.com/delta-io/delta-kernel-rs/pull/1685
[#1847]: https://github.com/delta-io/delta-kernel-rs/pull/1847
[#1900]: https://github.com/delta-io/delta-kernel-rs/pull/1900
[#1829]: https://github.com/delta-io/delta-kernel-rs/pull/1829
[#1811]: https://github.com/delta-io/delta-kernel-rs/pull/1811
[#1632]: https://github.com/delta-io/delta-kernel-rs/pull/1632
[#1813]: https://github.com/delta-io/delta-kernel-rs/pull/1813
[#1836]: https://github.com/delta-io/delta-kernel-rs/pull/1836
[#1837]: https://github.com/delta-io/delta-kernel-rs/pull/1837
[#1905]: https://github.com/delta-io/delta-kernel-rs/pull/1905
[#1909]: https://github.com/delta-io/delta-kernel-rs/pull/1909
[#1914]: https://github.com/delta-io/delta-kernel-rs/pull/1914
[#1676]: https://github.com/delta-io/delta-kernel-rs/pull/1676
[#1915]: https://github.com/delta-io/delta-kernel-rs/pull/1915
[#1907]: https://github.com/delta-io/delta-kernel-rs/pull/1907
[#1901]: https://github.com/delta-io/delta-kernel-rs/pull/1901
[#1918]: https://github.com/delta-io/delta-kernel-rs/pull/1918
[#1917]: https://github.com/delta-io/delta-kernel-rs/pull/1917
[#1818]: https://github.com/delta-io/delta-kernel-rs/pull/1818
[#1862]: https://github.com/delta-io/delta-kernel-rs/pull/1862
[#1872]: https://github.com/delta-io/delta-kernel-rs/pull/1872
[#1738]: https://github.com/delta-io/delta-kernel-rs/pull/1738
[#1860]: https://github.com/delta-io/delta-kernel-rs/pull/1860
[#1864]: https://github.com/delta-io/delta-kernel-rs/pull/1864
[#1877]: https://github.com/delta-io/delta-kernel-rs/pull/1877
[#1748]: https://github.com/delta-io/delta-kernel-rs/pull/1748
[#1870]: https://github.com/delta-io/delta-kernel-rs/pull/1870
[#1881]: https://github.com/delta-io/delta-kernel-rs/pull/1881
[#1806]: https://github.com/delta-io/delta-kernel-rs/pull/1806
[#1711]: https://github.com/delta-io/delta-kernel-rs/pull/1711
[#1850]: https://github.com/delta-io/delta-kernel-rs/pull/1850
[#1934]: https://github.com/delta-io/delta-kernel-rs/pull/1934
[#1863]: https://github.com/delta-io/delta-kernel-rs/pull/1863
[#1919]: https://github.com/delta-io/delta-kernel-rs/pull/1919
[#1903]: https://github.com/delta-io/delta-kernel-rs/pull/1903
[#1921]: https://github.com/delta-io/delta-kernel-rs/pull/1921
[#1935]: https://github.com/delta-io/delta-kernel-rs/pull/1935
[#1913]: https://github.com/delta-io/delta-kernel-rs/pull/1913
[#1946]: https://github.com/delta-io/delta-kernel-rs/pull/1946
[#1945]: https://github.com/delta-io/delta-kernel-rs/pull/1945
[#1954]: https://github.com/delta-io/delta-kernel-rs/pull/1954
[#1947]: https://github.com/delta-io/delta-kernel-rs/pull/1947
[#1949]: https://github.com/delta-io/delta-kernel-rs/pull/1949


## [v0.19.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.19.0/) (2026-01-20)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.19.0...v0.19.1)

### 🐛 Bug Fixes

1. fix: deadlock for `TokioMultiThreadExecutor` ([#1606]) (see [#1605] for a description of the issue)

[#1606]: https://github.com/delta-io/delta-kernel-rs/pull/1606
[#1605]: https://github.com/delta-io/delta-kernel-rs/issues/1605

## [v0.19.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.19.0/) (2025-12-19)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.18.2...v0.19.0)

### 🏗️ Breaking changes
1. Error on surplus columns in output schema ([#1528])
2. Remove `arrow-55` support (upgrate to arrow 56 or 57 required) ([#1507])
3. Add a new `read_parquet_schema` function to the `ParquetHandler` trait ([#1498])
4. Add a new `write_parquet_file` function to the `ParquetHandler` trait ([#1392])
5. Make PartialEq for Scalar a physical comparison ([#1554])
> [!CAUTION]
> Note this is a **breaking** behavior change. Code that previously relied on `PartialEq` as a
> logical comparison will still compile, but its runtime behavior will silently change to perform
> structural comparisons.
>
> This change moves the current definition of `PartialEq` for `Scalar` to a new `Scalar::logical_eq`
> method, and derives `PartialEq` (= physical comparison).
>
> We also remove PartialOrd for Scalar because it, too, would become physical (required to match
> PartialEq), and the result would be largely nonsensical. The logical comparison moves to
> Scalar::logical_partial_cmp instead.
>
> These changes are needed because today there's no reliable way to physically compare two scalars,
> and most comparisons are physical in practice. Only predicate evaluation needs logical
> comparisons, and that code already has a narrow waist.
6. Expose mod time in scan metadata callbacks: users must change the scan callback function to take
a struct which has all the previous arguments as members (and the mod time). See an example of the
needed change [here][change1]. For FFI code, your callback function needs an extra argument. See an
example of the change needed [here][change2]. ([#1565])

[change1]: https://github.com/delta-io/delta-kernel-rs/pull/1565/files#diff-3f44d0b7f8cfbe763cbe0cdbb2e2450a84833065de32fc102aef9d38b21b3daaR62
[change2]: https://github.com/delta-io/delta-kernel-rs/pull/1565/files#diff-60493959e34a593831a075fbf2cc7a03a45d8f423f98e2d6b6a4a6ce479dd25bR54

### 🚀 Features / new APIs

1. Initial Metrics implementation ([#1448])
2. Build TableConfiguration for each version of change data feed ([#1531])
3. Add ability for engines to specify a scan schema ([#1463])
4. Add bidirectional expression round-trip test with visitor functions ([#1467])
5. Add support for the materializePartitionColumns writer feature ([#1476])
6. Allow comfy-table 7.2.x ([#1545])
7. Rustls for uc-client ([#1533])
8. Add file name metadata column to parquet reading. ([#1512])
9. Add checkpoint example ([#1544])
10. Commit Reader for processing commit actions ([#1499])
11. Add CheckpointManifestReader to process sidecar files ([#1500])
12. Distributed Log Replay Sequential Phase ([#1502])
13. Passing schema from C, plus example/tests in C ([#1535])
14. Support sidecar in inspect-table ([#1566])
15. short-circuit coalesce evaluation when array has no nulls ([#1568])

### 🐛 Bug Fixes

1. Force usage of ListedLogFiles::try_new() ([#1562])
2. Improve parse_json performance by removing line-by-line parsing ([#1561])

### 🚜 Refactor

1. Move ensure_read_support/ensure_write_support to operation entry points ([#1518])
2. Migrate custom feature functions to generic is_feature_enabled/is_feature_supported ([#1519])
3. Separate async handler logic from sync bridge logic ([#1435])

### 🧪 Testing

1. Migrated protocol validation tests to table_configuration ([#1517])
2. Move scan/mod.rs to scan/tests.rs and scan/test_utils.rs ([#1485])

### ⚙️ Chores/CI

1. Remove macOS metadata from test data tarballs ([#1534])
2. Make tests async if they rely on async ([#1438])
3. Cleanup scalar eq workaround ([#1560])

### Other
1. Remove architecture.md from readme ([#1551])


[#1517]: https://github.com/delta-io/delta-kernel-rs/pull/1517
[#1448]: https://github.com/delta-io/delta-kernel-rs/pull/1448
[#1518]: https://github.com/delta-io/delta-kernel-rs/pull/1518
[#1528]: https://github.com/delta-io/delta-kernel-rs/pull/1528
[#1507]: https://github.com/delta-io/delta-kernel-rs/pull/1507
[#1531]: https://github.com/delta-io/delta-kernel-rs/pull/1531
[#1463]: https://github.com/delta-io/delta-kernel-rs/pull/1463
[#1485]: https://github.com/delta-io/delta-kernel-rs/pull/1485
[#1534]: https://github.com/delta-io/delta-kernel-rs/pull/1534
[#1438]: https://github.com/delta-io/delta-kernel-rs/pull/1438
[#1467]: https://github.com/delta-io/delta-kernel-rs/pull/1467
[#1519]: https://github.com/delta-io/delta-kernel-rs/pull/1519
[#1435]: https://github.com/delta-io/delta-kernel-rs/pull/1435
[#1476]: https://github.com/delta-io/delta-kernel-rs/pull/1476
[#1498]: https://github.com/delta-io/delta-kernel-rs/pull/1498
[#1545]: https://github.com/delta-io/delta-kernel-rs/pull/1545
[#1392]: https://github.com/delta-io/delta-kernel-rs/pull/1392
[#1551]: https://github.com/delta-io/delta-kernel-rs/pull/1551
[#1533]: https://github.com/delta-io/delta-kernel-rs/pull/1533
[#1512]: https://github.com/delta-io/delta-kernel-rs/pull/1512
[#1544]: https://github.com/delta-io/delta-kernel-rs/pull/1544
[#1554]: https://github.com/delta-io/delta-kernel-rs/pull/1554
[#1499]: https://github.com/delta-io/delta-kernel-rs/pull/1499
[#1560]: https://github.com/delta-io/delta-kernel-rs/pull/1560
[#1500]: https://github.com/delta-io/delta-kernel-rs/pull/1500
[#1502]: https://github.com/delta-io/delta-kernel-rs/pull/1502
[#1535]: https://github.com/delta-io/delta-kernel-rs/pull/1535
[#1565]: https://github.com/delta-io/delta-kernel-rs/pull/1565
[#1566]: https://github.com/delta-io/delta-kernel-rs/pull/1566
[#1562]: https://github.com/delta-io/delta-kernel-rs/pull/1562
[#1561]: https://github.com/delta-io/delta-kernel-rs/pull/1561
[#1568]: https://github.com/delta-io/delta-kernel-rs/pull/1568


## [v0.18.2](https://github.com/delta-io/delta-kernel-rs/tree/v0.18.2/) (2025-12-03)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.18.1...v0.18.2)


### 🐛 Bug Fixes

1. Address column mapping edge case in protocol validation ([#1513])

### 🧪 Testing
1. Remove arrow error message dependency from test ([#1529])


[#1513]: https://github.com/delta-io/delta-kernel-rs/pull/1513
[#1529]: https://github.com/delta-io/delta-kernel-rs/pull/1529


## [v0.18.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.18.1/) (2025-11-24)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.18.0...v0.18.1)


### 🚀 Features / new APIs

1. Scan::execute no longer requires lifetime bound  ([#1515])
2. Migrate protocol validation to table_configuration ([#1411])
3. Add Display for StructType, StructField, and MetadataColumnSpec ([#1494])
5. Add EngineDataArrowExt and use it everywhere ([#1516])
6. Implement builder for StructType ([#1492])
7. Enable CDF for column-mapped tables ([#1510])

### 🧪 Testing

1. Extract File Action tests ([#1365])


[#1515]: https://github.com/delta-io/delta-kernel-rs/pull/1515
[#1365]: https://github.com/delta-io/delta-kernel-rs/pull/1365
[#1411]: https://github.com/delta-io/delta-kernel-rs/pull/1411
[#1494]: https://github.com/delta-io/delta-kernel-rs/pull/1494
[#1516]: https://github.com/delta-io/delta-kernel-rs/pull/1516
[#1492]: https://github.com/delta-io/delta-kernel-rs/pull/1492
[#1510]: https://github.com/delta-io/delta-kernel-rs/pull/1510


## [v0.18.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.18.0/) (2025-11-19)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.17.1...v0.18.0)

### 🏗️ Breaking changes
1. New Engine StorageHandler head API ([#1465])
   - Engine API implementers must add the `head` API to StorageHandler which fetches metadata about a file in storage
2. Add remove_files API ([#1353])
   - The schema for scan rows (from `Scan::scan_metadata`) has been updated to include two
     new fields: `fileConstantValues.tags` and `fileConstantValues.defaultRowCommitVersion`.

### 🚀 Features / new APIs

1. Add parser for iceberg compat properties ([#1466])
2. Pass ColumnMappingMode to physical_name ([#1403])
3. Allow visiting entire domain metadata ([#1384])
4. Add Table Feature Info ([#1462])
5. *(FFI)* Snapshot log tail FFI ([#1379])
6. Add generic is_feature_supported and is_feature_enabled methods to TableConfiguration ([#1405])
7. Un-deprecate ArrayData.array_elements() ([#1493])
8. Allow writes to CDF tables for add-only, remove-only, and non-data-change transactions ([#1490])
9. *(catalog-managed)* UCCommitter ([#1418])

### 🐛 Bug Fixes

1. Eliminate endless busy looping in read_json_files on failed read ([#1489])
2. Handle array/map types in ffi schema example and test ([#1497])

### 📚 Documentation

1. Fix docs for rustc 1.92+ ([#1470])

### 🚜 Refactor

1. Harmonize checkpoint and log compaction iterators ([#1436])
2. Avoid overly complex itertools methods in log listing code ([#1434])
3. Simplify creation of default engine in tests ([#1437])

### 🧪 Testing

1. Add tests for StructField.physical_name ([#1469])

[#1466]: https://github.com/delta-io/delta-kernel-rs/pull/1466
[#1403]: https://github.com/delta-io/delta-kernel-rs/pull/1403
[#1465]: https://github.com/delta-io/delta-kernel-rs/pull/1465
[#1436]: https://github.com/delta-io/delta-kernel-rs/pull/1436
[#1470]: https://github.com/delta-io/delta-kernel-rs/pull/1470
[#1384]: https://github.com/delta-io/delta-kernel-rs/pull/1384
[#1462]: https://github.com/delta-io/delta-kernel-rs/pull/1462
[#1474]: https://github.com/delta-io/delta-kernel-rs/pull/1474
[#1379]: https://github.com/delta-io/delta-kernel-rs/pull/1379
[#1434]: https://github.com/delta-io/delta-kernel-rs/pull/1434
[#1437]: https://github.com/delta-io/delta-kernel-rs/pull/1437
[#1353]: https://github.com/delta-io/delta-kernel-rs/pull/1353
[#1489]: https://github.com/delta-io/delta-kernel-rs/pull/1489
[#1405]: https://github.com/delta-io/delta-kernel-rs/pull/1405
[#1469]: https://github.com/delta-io/delta-kernel-rs/pull/1469
[#1493]: https://github.com/delta-io/delta-kernel-rs/pull/1493
[#1497]: https://github.com/delta-io/delta-kernel-rs/pull/1497
[#1490]: https://github.com/delta-io/delta-kernel-rs/pull/1490
[#1418]: https://github.com/delta-io/delta-kernel-rs/pull/1418


## [v0.17.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.17.1/) (2025-11-13)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.17.0...v0.17.1)


### 📚 Documentation

1. Fix docs for rustc 1.92+ ([#1470])


[#1470]: https://github.com/delta-io/delta-kernel-rs/pull/1470


## [v0.17.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.17.0/) (2025-11-10)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.16.0...v0.17.0)

### 🏗️ Breaking changes
1. (catalog-managed): New copy_atomic StorageHandler method ([#1400])
   - StorageHandler implementers must implement the copy_atomic method.
2. Make expression and predicate evaluator constructors fallible ([#1452])
   - Predicate and expression evaluator constructors return DeltaResult.
3. (catalog-managed): add `log_tail` to `SnapshotBuilder` ([#1290])
   - `into_scan_builder()` no longer exists on `Snapshot`. Must create an `Arc<Snapshot>`
4. Arrow 57, MSRV 1.85+ ([#1424])
   - The Minimum Required Rust Version to use kernel-rs is now 1.85.
5. Add ffi for idempotent write primitives ([#1191])
   - get_transform_for_row now returns new FFI-safe OptionalValue instead of Option
6. Rearchitect `CommitResult` ([#1343])
   - CommitResult is now an enum containing CommittedTransaction, ConflictedTransaction,
   and RetryableTransaction
7. Add with_data_change to transaction ([#1281])
   - Engines must use with_data_change on the transaction level instead of
   passing it to the method. add_files_schema is moved to be scoped on a the
   transaction.
8. *(catalog-managed)* Introduce Committer (with FileSystemCommitter) ([#1349])
   - Constructing a transaction now requires a committer. Ex: FileSystemCommitter
9. Switch scan.execute to return pre-filtered data ([#1429])
   - Connectors no longer need to filter data that is returned from `scan.execute()`


### 🚀 Features / new APIs

1. Add visit_string_map to the ffi ([#1342])
2. Add tags field to LastCheckpointHint ([#1455])
3. Support writing domain metadata (1/2]) ([#1274])
4. Change input to write_json_file to be FilteredEngineData ([#1312])
5. Convert DV `storage_type` to enum ([#1366])
6. Add latest_commit_file field to LogSegment ([#1364])
7. No staged commits in checkpoint/compaction ([#1374])
8. Generate In Commit Timestamp on write ([#1314])
9. *(catalog-managed)* Add `uc-catalog` crate with load_table ([#1324])
10. Snapshot should not expose delta implementation details ([#1339])
11. *(catalog-managed)* Uc-client commit API ([#1399])
12. Add row tracking support ([#1375])
13. Support writing domain metadata (2/2]) ([#1275])
14. Add parser for enableTypeWidening table property ([#1456])
15. Implement `From` trait `EngineData` into `FilteredEngineData` ([#1397])
16. Unify TableFeatures followups ([#1404])
18. Accept nullable values in "tags" HashMap in `Add` action ([#1395])
19. Enable writes to CDF enabled tables only if append only is supported ([#1449])
20. Add deletion vector file writer ([#1425])
21. Allow converting `bytes::Bytes` into a Binary Scalar ([#1373])
22. CDF API for FFI ([#1335])
23. Add optional stats field to remove action ([#1390])
24. Modify read_actions to not require callers to know details about checkpoints. ([#1407])
25. Add Accessor for `Binary` data ([#1383])

### 🐛 Bug Fixes

1. Change InCommitTimestamp enablement getter function  ([#1357])
2. Be adaptive to the log schema changing in inspect-table ([#1368])
3. Typo on variable name for ScanTransformFieldClassifierieldClassifier ([#1394])
4. Pin cbindgen to 0.29.0 ([#1412])
5. Unpin cbindgen ([#1414])
6. Don't return errors from ParsedLogPath::try_from ([#1433])
7. Doc issue, stray ' ([#1445])
8. Replace todo!() with proper error handling in deletion vector ([#1447])

### 📚 Documentation

1. Fix scan_metadata docs ([#1450])

### 🚜 Refactor

1. Pull out transform spec utils and definitions ([#1326])
2. Use expression transforms in change data feed  ([#1330])
3. Remove raw pointer indexing and add unit tests for RowIndexBuilder ([#1334])
4. Make `Metadata` fields private ([#1347])
5. Remove storing UUID in LogPathFileType::UuidCheckpoint ([#1317])
6. Consolidate physical/logical info into StateInfo ([#1350])
7. Consolidate regular scan and CDF scan field handling  ([#1359])
8. Make get_cdf_transform_expr return Option<ExpressionRef> ([#1401])
9. Separate domain metadata additions and removals ([#1421])
10. Unify Reader/WriterFeature into a single TableFeature ([#1345])
11. Put `DataFileMetadata::as_record_batch` under `#[internal_api]` ([#1409])
12. Create static variables for magic values in deletion vector ([#1446])

### 🧪 Testing

1. E2e test for log compaction ([#1308])
2. Tombstone expiration e2e test for log compaction ([#1341])
3. Add memory tests (via DHAT) ([#1009])
4. One liner to skip read_table_version_hdfs ([#1428])

### ⚙️ Chores/CI

1. Add CI for examples ([#1393])
2. Small typo's in `log_segment.rs` ([#1396])
3. Reduce log verbosity when encountering non-standard files in _delta_log ([#1416])
4. Follow up on TODO in `log_replay.rs` ([#1408])
5. Remove a stray comment in the kernel visitor ([#1457])
6. Allow passing more on the command line for all the cli examples ([#1352])
7. add back arrow-55 support ([#1458])
8. Rename log_schema to commit_schema ([#1419])

[#1326]: https://github.com/delta-io/delta-kernel-rs/pull/1326
[#1308]: https://github.com/delta-io/delta-kernel-rs/pull/1308
[#1342]: https://github.com/delta-io/delta-kernel-rs/pull/1342
[#1290]: https://github.com/delta-io/delta-kernel-rs/pull/1290
[#1274]: https://github.com/delta-io/delta-kernel-rs/pull/1274
[#1330]: https://github.com/delta-io/delta-kernel-rs/pull/1330
[#1334]: https://github.com/delta-io/delta-kernel-rs/pull/1334
[#1347]: https://github.com/delta-io/delta-kernel-rs/pull/1347
[#1312]: https://github.com/delta-io/delta-kernel-rs/pull/1312
[#1352]: https://github.com/delta-io/delta-kernel-rs/pull/1352
[#1357]: https://github.com/delta-io/delta-kernel-rs/pull/1357
[#1317]: https://github.com/delta-io/delta-kernel-rs/pull/1317
[#1341]: https://github.com/delta-io/delta-kernel-rs/pull/1341
[#1350]: https://github.com/delta-io/delta-kernel-rs/pull/1350
[#1009]: https://github.com/delta-io/delta-kernel-rs/pull/1009
[#1366]: https://github.com/delta-io/delta-kernel-rs/pull/1366
[#1364]: https://github.com/delta-io/delta-kernel-rs/pull/1364
[#1368]: https://github.com/delta-io/delta-kernel-rs/pull/1368
[#1339]: https://github.com/delta-io/delta-kernel-rs/pull/1339
[#1373]: https://github.com/delta-io/delta-kernel-rs/pull/1373
[#1359]: https://github.com/delta-io/delta-kernel-rs/pull/1359
[#1343]: https://github.com/delta-io/delta-kernel-rs/pull/1343
[#1374]: https://github.com/delta-io/delta-kernel-rs/pull/1374
[#1314]: https://github.com/delta-io/delta-kernel-rs/pull/1314
[#1394]: https://github.com/delta-io/delta-kernel-rs/pull/1394
[#1393]: https://github.com/delta-io/delta-kernel-rs/pull/1393
[#1396]: https://github.com/delta-io/delta-kernel-rs/pull/1396
[#1281]: https://github.com/delta-io/delta-kernel-rs/pull/1281
[#1324]: https://github.com/delta-io/delta-kernel-rs/pull/1324
[#1401]: https://github.com/delta-io/delta-kernel-rs/pull/1401
[#1412]: https://github.com/delta-io/delta-kernel-rs/pull/1412
[#1349]: https://github.com/delta-io/delta-kernel-rs/pull/1349
[#1407]: https://github.com/delta-io/delta-kernel-rs/pull/1407
[#1414]: https://github.com/delta-io/delta-kernel-rs/pull/1414
[#1416]: https://github.com/delta-io/delta-kernel-rs/pull/1416
[#1191]: https://github.com/delta-io/delta-kernel-rs/pull/1191
[#1399]: https://github.com/delta-io/delta-kernel-rs/pull/1399
[#1375]: https://github.com/delta-io/delta-kernel-rs/pull/1375
[#1419]: https://github.com/delta-io/delta-kernel-rs/pull/1419
[#1275]: https://github.com/delta-io/delta-kernel-rs/pull/1275
[#1400]: https://github.com/delta-io/delta-kernel-rs/pull/1400
[#1335]: https://github.com/delta-io/delta-kernel-rs/pull/1335
[#1397]: https://github.com/delta-io/delta-kernel-rs/pull/1397
[#1421]: https://github.com/delta-io/delta-kernel-rs/pull/1421
[#1345]: https://github.com/delta-io/delta-kernel-rs/pull/1345
[#1428]: https://github.com/delta-io/delta-kernel-rs/pull/1428
[#1404]: https://github.com/delta-io/delta-kernel-rs/pull/1404
[#1433]: https://github.com/delta-io/delta-kernel-rs/pull/1433
[#1445]: https://github.com/delta-io/delta-kernel-rs/pull/1445
[#1408]: https://github.com/delta-io/delta-kernel-rs/pull/1408
[#1429]: https://github.com/delta-io/delta-kernel-rs/pull/1429
[#1450]: https://github.com/delta-io/delta-kernel-rs/pull/1450
[#1395]: https://github.com/delta-io/delta-kernel-rs/pull/1395
[#1390]: https://github.com/delta-io/delta-kernel-rs/pull/1390
[#1449]: https://github.com/delta-io/delta-kernel-rs/pull/1449
[#1425]: https://github.com/delta-io/delta-kernel-rs/pull/1425
[#1409]: https://github.com/delta-io/delta-kernel-rs/pull/1409
[#1452]: https://github.com/delta-io/delta-kernel-rs/pull/1452
[#1424]: https://github.com/delta-io/delta-kernel-rs/pull/1424
[#1447]: https://github.com/delta-io/delta-kernel-rs/pull/1447
[#1456]: https://github.com/delta-io/delta-kernel-rs/pull/1456
[#1455]: https://github.com/delta-io/delta-kernel-rs/pull/1455
[#1457]: https://github.com/delta-io/delta-kernel-rs/pull/1457
[#1458]: https://github.com/delta-io/delta-kernel-rs/pull/1458
[#1383]: https://github.com/delta-io/delta-kernel-rs/pull/1383
[#1446]: https://github.com/delta-io/delta-kernel-rs/pull/1446


## [v0.16.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.16.0/) (2025-09-19)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.15.2...v0.16.0)

### 🏗️ Breaking changes
1. New expression variants: `UnaryExpression` and `ToJson` expression ([#1192])
2. New SnapshotBuilder API: `Snapshot::try_new(...)` replaced with `Snapshot::builder(...)` and its
   associated methods. TLDR, you make a builder and call `build` to construct a `Snapshot`.  ([#1189])
3. Simplify the `Expr::Transform` API, add FFI support:
   - Reworks the pub members of Transform used by Expr::Transform and introduce a new FieldTransform struct.
   Also, rework Transform::new (constructor) and Transform::with_input_path (method) into a pair of
   constructors, new_top_level and new_nested.
   - Adds two new members to the FFI EngineExpressionVisitor struct -- visit_transform_expression and
   visit_field_transform, which also changes the ordering of existing fields. ([#1243])
4. Add `numRecords` to `ADD_FILES_SCHEMA` ([#1235])
5. New `EngineData` trait required method: `try_append_columns` ([#1190])
6. Make ColumnType private ([#1258])
7. Add row tracking writer feature: updates `ADD_FILES_SCHEMA` (see PR for details) ([#1239])
8. Migrate `Snapshot::try_new_from` into `SnapshotBuilder::new_from` ([#1289])
9. (FFI) Add CDvInfo struct: The `CScanCallback` now takes a `&CDvInfo` and not a `&DvInfo`. ([#1286])
10. (FFI) Add explicit numbers for each `KernelError` enum variants. (see PR for details) ([#1313])
11. (more) new expression variants: `Expression::Variadic` and `Coalesce` expressions ([#1198])
12. All new/modified `StructType` constructors, see PR for details ([#1278])
13. Introduce metadata column API: `StructType` has new private field ([#1266])
14. (FFI) `engine_data::get_engine_data` now takes an `AllocateErrorFn` instead of an engine. ([#1325])
15. `StructType::into_fields` returns `DoubleEndedIterator + FusedIterator` ([#1327])

### 🚀 Features / new APIs

1. *(catalog-managed)* Add log_tail to list_log_files ([#1194])
2. CommitInfo sets a txnId ([#1262])
3. Allow LargeUTF8 -> String and LargeBinary -> Binary in arrow conversion ([#1294])
4. Implement log compaction ([#1234])
5. Disallow equal version in log compaction ([#1309])
6. Add `Iterable` to `StructType` ([#1287])
7. ParsedLogPath for staged commits ([#1305])
8. Default expression eval supports nested transforms ([#1247])
9. Introduce row index metadata column ([#1272])

### 📚 Documentation

1. Update README.md to enhance FFI documentation ([#1237])

### ⚡ Performance

1. Make checkpoint visitor more efficient using short circuiting ([#1203])

### 🚜 Refactor

1. Factor out a method for LastCheckpointHint path generation ([#1228])
2. Do not guess Vec size for checkpoints ([#1263])
3. Introduce current_time_ms() helper ([#1256])
4. Retention calculation into a new trait ([#1264])
5. Minor Refactoring in Log Compaction ([#1301])
6. Rename SnapshotBuilder::new to new_for ([#1306])
7. Move log replay into the action reconciliation module ([#1295])
8. Introduce SnapshotRef type alias ([#1299])
9. Row tracking write cleanup ([#1291])

### 🧪 Testing

1. Update invalid-handle tests for rustc 1.90 ([#1321])
2. Create expression benchmark for default engine ([#1220])

### ⚙️ Chores/CI

1. Update changelog for 0.15.1 release ([#1227])
2. Sync changelog for 0.15.2 ([#1251])
3. Update data types test to validate full Arrow error message ([#1259])
4. Add better panic message when not OK ([#1293])
5. Add test for empty commits and clean up test error types ([#1252])
6. Update contributing.md ([#1206])


[#1192]: https://github.com/delta-io/delta-kernel-rs/pull/1192
[#1189]: https://github.com/delta-io/delta-kernel-rs/pull/1189
[#1227]: https://github.com/delta-io/delta-kernel-rs/pull/1227
[#1228]: https://github.com/delta-io/delta-kernel-rs/pull/1228
[#1203]: https://github.com/delta-io/delta-kernel-rs/pull/1203
[#1243]: https://github.com/delta-io/delta-kernel-rs/pull/1243
[#1251]: https://github.com/delta-io/delta-kernel-rs/pull/1251
[#1235]: https://github.com/delta-io/delta-kernel-rs/pull/1235
[#1190]: https://github.com/delta-io/delta-kernel-rs/pull/1190
[#1194]: https://github.com/delta-io/delta-kernel-rs/pull/1194
[#1258]: https://github.com/delta-io/delta-kernel-rs/pull/1258
[#1259]: https://github.com/delta-io/delta-kernel-rs/pull/1259
[#1263]: https://github.com/delta-io/delta-kernel-rs/pull/1263
[#1256]: https://github.com/delta-io/delta-kernel-rs/pull/1256
[#1262]: https://github.com/delta-io/delta-kernel-rs/pull/1262
[#1264]: https://github.com/delta-io/delta-kernel-rs/pull/1264
[#1239]: https://github.com/delta-io/delta-kernel-rs/pull/1239
[#1237]: https://github.com/delta-io/delta-kernel-rs/pull/1237
[#1294]: https://github.com/delta-io/delta-kernel-rs/pull/1294
[#1234]: https://github.com/delta-io/delta-kernel-rs/pull/1234
[#1220]: https://github.com/delta-io/delta-kernel-rs/pull/1220
[#1289]: https://github.com/delta-io/delta-kernel-rs/pull/1289
[#1301]: https://github.com/delta-io/delta-kernel-rs/pull/1301
[#1293]: https://github.com/delta-io/delta-kernel-rs/pull/1293
[#1306]: https://github.com/delta-io/delta-kernel-rs/pull/1306
[#1295]: https://github.com/delta-io/delta-kernel-rs/pull/1295
[#1286]: https://github.com/delta-io/delta-kernel-rs/pull/1286
[#1313]: https://github.com/delta-io/delta-kernel-rs/pull/1313
[#1299]: https://github.com/delta-io/delta-kernel-rs/pull/1299
[#1309]: https://github.com/delta-io/delta-kernel-rs/pull/1309
[#1198]: https://github.com/delta-io/delta-kernel-rs/pull/1198
[#1287]: https://github.com/delta-io/delta-kernel-rs/pull/1287
[#1321]: https://github.com/delta-io/delta-kernel-rs/pull/1321
[#1278]: https://github.com/delta-io/delta-kernel-rs/pull/1278
[#1252]: https://github.com/delta-io/delta-kernel-rs/pull/1252
[#1291]: https://github.com/delta-io/delta-kernel-rs/pull/1291
[#1266]: https://github.com/delta-io/delta-kernel-rs/pull/1266
[#1305]: https://github.com/delta-io/delta-kernel-rs/pull/1305
[#1325]: https://github.com/delta-io/delta-kernel-rs/pull/1325
[#1247]: https://github.com/delta-io/delta-kernel-rs/pull/1247
[#1327]: https://github.com/delta-io/delta-kernel-rs/pull/1327
[#1206]: https://github.com/delta-io/delta-kernel-rs/pull/1206
[#1272]: https://github.com/delta-io/delta-kernel-rs/pull/1272


## [v0.15.2](https://github.com/delta-io/delta-kernel-rs/tree/v0.15.2/) (2025-09-03)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.15.1...v0.15.2)


### 🐛 Bug Fixes

1. pin `comfy-table` at `7.1.4` to restore kernel MSRV ([#1231])
2. Arrow json decoder fix for breakage on long json string ([#1244])


[#1231]: https://github.com/delta-io/delta-kernel-rs/pull/1231
[#1244]: https://github.com/delta-io/delta-kernel-rs/pull/1244


## [v0.15.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.15.1/) (2025-08-28)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.15.0...v0.15.1)


### 🐛 Bug Fixes

1. Make ListedLogFiles::try_new internal-api (again) ([#1226])


[#1226]: https://github.com/delta-io/delta-kernel-rs/pull/1226


## [v0.15.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.15.0/) (2025-08-28)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.14.0...v0.15.0)

### 🏗️ Breaking changes
1. Rename `default-engine` feature to `default-engine-native-tls` ([#1100])
2. Add arrow 56 support, drop arrow 54 ([#1141])
3. Add `catalogManaged` (and `catalogOwned-preview`) table features + `catalog-managed`
   _experimental_ feature flag ([#1165])
4. `ExpressionRef` instead of owned `Expression` for transforms ([#1171]): `Expression::Struct` now
   takes a `Vec<ExpressionRef>` instead of `Vec<Expression>`
5. Add support for Column Mapping Id Mode ([#1056]): significantly changes the semantics (`Engine`
   trait requirements) of the parquet handler in column mapping id mode. See
   `ParquetHandler::read_parquet_files` docs for details.
6. `StructField.physical_name` is no longer public (internal-api) ([#1186])
7. Add support for sparse transform expressions ([#1199]): adds a new `Expression::Transform`
   variant.
8. Expression evaluators take `ExpressionRef` as input ([#1221]):
   - `EvaluationHandler::new_expression_evaluator` and `EvaluationHandler::new_predicate_evaluator`
   take Arc instead of owned expression/predicate.
   - `scan::state::transform_to_logical` takes owned `Option<ExpressionRef>` instead of a borrowed
   reference.
   - `transaction::WriteContext::logical_to_physical` returns an Arc instead of a borrowed reference

### 🚀 Features / new APIs

1. Impl IntoEngineData for Protocol action ([#1136])
2. Add txnId to commit info ([#1148])
3. *(catalog-managed)* Experimental uc client ([#1164])
4. Implement `IntoEngineData` for `DomainMetadata` ([#1169])
5. Add example for table writes ([#1119])
6. *(ffi)* Add `visit_expression_literal_date` ([#1096])

### 🐛 Bug Fixes

1. Match arrow versions in examples ([#1166])
2. Support arrow views in ensure_data_types ([#1028])
3. Make `ListedLogFiles` internal-api again ([#1209])
4. Provide accurate error when evaluating a different type in LiteralExpressionTransform ([#1207])
5. Fix failing test and improve indentation test error message ([#1135])

### 🚜 Refactor

1. Contiguous commit file checking inside `ListedLogFiles::try_new()` ([#1107])
2. New listed_log_files module ([#1150])
3. Move LastCheckpointHint to separate module ([#1154])
4. *(catalog-managed)* Push down _last_checkpoint read into LogSegment ([#1204])

### 🧪 Testing

1. Add metadata-only regression test ([#1183])
2. Parameterize column mapping tests to check different modes ([#1176])
3. Add apply_schema mismatch test ([#1210])

### ⚙️ Chores/CI

1. Appease clippy in rustc 1.89 ([#1151])
2. Bump MSRV to 1.84 ([#1142])
3. Remove object store versioning ([#1161])
4. Remove unused deps from examples ([#1175])
5. Update deps ([#1181])


[#1135]: https://github.com/delta-io/delta-kernel-rs/pull/1135
[#1136]: https://github.com/delta-io/delta-kernel-rs/pull/1136
[#1107]: https://github.com/delta-io/delta-kernel-rs/pull/1107
[#1148]: https://github.com/delta-io/delta-kernel-rs/pull/1148
[#1151]: https://github.com/delta-io/delta-kernel-rs/pull/1151
[#1142]: https://github.com/delta-io/delta-kernel-rs/pull/1142
[#1100]: https://github.com/delta-io/delta-kernel-rs/pull/1100
[#1150]: https://github.com/delta-io/delta-kernel-rs/pull/1150
[#1154]: https://github.com/delta-io/delta-kernel-rs/pull/1154
[#1141]: https://github.com/delta-io/delta-kernel-rs/pull/1141
[#1161]: https://github.com/delta-io/delta-kernel-rs/pull/1161
[#1166]: https://github.com/delta-io/delta-kernel-rs/pull/1166
[#1164]: https://github.com/delta-io/delta-kernel-rs/pull/1164
[#1165]: https://github.com/delta-io/delta-kernel-rs/pull/1165
[#1171]: https://github.com/delta-io/delta-kernel-rs/pull/1171
[#1175]: https://github.com/delta-io/delta-kernel-rs/pull/1175
[#1028]: https://github.com/delta-io/delta-kernel-rs/pull/1028
[#1183]: https://github.com/delta-io/delta-kernel-rs/pull/1183
[#1181]: https://github.com/delta-io/delta-kernel-rs/pull/1181
[#1176]: https://github.com/delta-io/delta-kernel-rs/pull/1176
[#1169]: https://github.com/delta-io/delta-kernel-rs/pull/1169
[#1056]: https://github.com/delta-io/delta-kernel-rs/pull/1056
[#1186]: https://github.com/delta-io/delta-kernel-rs/pull/1186
[#1209]: https://github.com/delta-io/delta-kernel-rs/pull/1209
[#1119]: https://github.com/delta-io/delta-kernel-rs/pull/1119
[#1096]: https://github.com/delta-io/delta-kernel-rs/pull/1096
[#1204]: https://github.com/delta-io/delta-kernel-rs/pull/1204
[#1199]: https://github.com/delta-io/delta-kernel-rs/pull/1199
[#1210]: https://github.com/delta-io/delta-kernel-rs/pull/1210
[#1207]: https://github.com/delta-io/delta-kernel-rs/pull/1207
[#1221]: https://github.com/delta-io/delta-kernel-rs/pull/1221


## [v0.14.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.14.0/) (2025-08-01)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.13.0...v0.14.0)

### 🏗️ Breaking changes
1. Removed Table APIs: instead use `Snapshot` and `Transaction` directly. ([#976])
2. Add support for Variant type and the variantType table feature (new `DataType::Variant` enum
   variant and new `variantType-preview` and `variantShredding` Reader/Writer features) ([#1015])
3. Expose post commit stats. Now, in `Transaction::commit` the `Committed` variant of the enum
   includes a `post_commit_stats` field with info about the commits since checkpoint and log
   compaction. ([#1079])
4. Replace `Transaction::with_commit_info()` API with `with_engine_info()` API ([#997])
5. Removed `DataType::decimal_unchecked` API ([#1087])
6. `make_physical` takes column mapping and sets parquet field ids. breaking: (1)
    `StructField::make_physical` is now an internal_api instead of a public function. Its signature
    has also changed. And (2) If `ColumnMappingMode` is `None`, then the physical schema's name is
    the logical name. Previously, kernel would unconditionally use the column mapping physical name,
    even if column mapping mode is none. ([#1082])

### 🚀 Features / new APIs

1. *(ffi)* Added default-engine-rustls feature and extern "C" for .h file ([#1023])
2. Add log segment constructor for timestamp to version conversion ([#895])
3. Expose unshredded variant type as `DataType::unshredded_variant()` ([#1086])
4. New ffi API for `get_domain_metadata()` ([#1041])
5. Add append functions to ffi ([#962])
6. Add try_new and `IntoEngineData` for Metadata action ([#1122])

### 🐛 Bug Fixes

1. Rename object_store PutMultipartOpts ([#1071], [#1090])
2. Use object_store >= 0.12.3 for arrow 55 feature ([#1117])
3. VARIANT follow-ups for SchemaTransform etc ([#1106])

### 🚜 Refactor

1. Downgrade stale `_last_checkpoint` log from `warn!` to `info!` ([#777])
2. Exclude `tests/data` from release ([#1092])
3. Deny panics in prod code ([#1113])

### 🧪 Testing

1. Add derive macro tests ([#514])
2. Add unshredded variant read test ([#1088])
3. *(ffi)* `AllocateErrorFn` should be able to allocate a nullptr ([#1105])
4. Assert tests on error message instead of `is_err()` ([#1110])

### ⚙️ Chores/CI

1. Expose Snapshot and ListedLogFiles constructors behind internal api flag ([#1076])
2. Only semver check released crates ([#1101])

### Other

1. Fix typos in README ([#1093])
2. Fix typos in docstrings ([#1118])


[#1023]: https://github.com/delta-io/delta-kernel-rs/pull/1023
[#976]: https://github.com/delta-io/delta-kernel-rs/pull/976
[#1071]: https://github.com/delta-io/delta-kernel-rs/pull/1071
[#1076]: https://github.com/delta-io/delta-kernel-rs/pull/1076
[#1015]: https://github.com/delta-io/delta-kernel-rs/pull/1015
[#1079]: https://github.com/delta-io/delta-kernel-rs/pull/1079
[#514]: https://github.com/delta-io/delta-kernel-rs/pull/514
[#777]: https://github.com/delta-io/delta-kernel-rs/pull/777
[#895]: https://github.com/delta-io/delta-kernel-rs/pull/895
[#997]: https://github.com/delta-io/delta-kernel-rs/pull/997
[#1086]: https://github.com/delta-io/delta-kernel-rs/pull/1086
[#1090]: https://github.com/delta-io/delta-kernel-rs/pull/1090
[#1088]: https://github.com/delta-io/delta-kernel-rs/pull/1088
[#1093]: https://github.com/delta-io/delta-kernel-rs/pull/1093
[#1092]: https://github.com/delta-io/delta-kernel-rs/pull/1092
[#1087]: https://github.com/delta-io/delta-kernel-rs/pull/1087
[#1041]: https://github.com/delta-io/delta-kernel-rs/pull/1041
[#1101]: https://github.com/delta-io/delta-kernel-rs/pull/1101
[#1113]: https://github.com/delta-io/delta-kernel-rs/pull/1113
[#1105]: https://github.com/delta-io/delta-kernel-rs/pull/1105
[#1117]: https://github.com/delta-io/delta-kernel-rs/pull/1117
[#1118]: https://github.com/delta-io/delta-kernel-rs/pull/1118
[#1106]: https://github.com/delta-io/delta-kernel-rs/pull/1106
[#962]: https://github.com/delta-io/delta-kernel-rs/pull/962
[#1122]: https://github.com/delta-io/delta-kernel-rs/pull/1122
[#1110]: https://github.com/delta-io/delta-kernel-rs/pull/1110
[#1082]: https://github.com/delta-io/delta-kernel-rs/pull/1082


## [v0.13.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.13.0/) (2025-07-11)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.12.1...v0.13.0)

### 🏗️ Breaking changes
1. Add support for opaque engine expressions. Includes a number of changes: new `ExpressionType`s
   (`OpaqueExpression`, `OpaquePredicate`, `Unknown`) and `Expression`/`Predicate` variants
   (`Opaque`, `Unknown`), and visitors, transforms, and evaluators changed to support
   opaque/unknown expressions/predicate. ([#686])
2. Rename `Transaction::add_write_metadata` to `Transaction::add_files` ([#1019])

### 🚀 Features / new APIs

1. add ability to only retain SetTransaction actions <= SetTransactionRetentionDuration ([#1013])
2. *(ffi)* Add timetravel by version number ([#1044])
3. Introduce a crate for args that are common between examples ([#1046])
4. Support reordering structs that are inside maps in default parquet reader ([#1060])
5. Add default engine support for arrow eval of opaque expressions ([#980])
5. Expose descriptive fields on Metadata action ([#1051])

### 🐛 Bug Fixes

1. Clippy fmt cleanup ([#1042])
2. Examples: move logic into the thread::scope call so examples don't hang ([#1040])
3. Remove panic from read_last_checkpoint ([#1022])
4. Always write `_last_checkpoint` with parts = None ([#1053])
5. Don't release `common` crate (used only by example programs)  ([#1065])

### 🚜 Refactor

1. Move various test util functions to test-utils crate ([#985])
2. Define and use a cow helper for transforms ([#1057])
3. Expand capability and usage of `Cow` helper for transforms ([#1061])


[#985]: https://github.com/delta-io/delta-kernel-rs/pull/985
[#1013]: https://github.com/delta-io/delta-kernel-rs/pull/1013
[#1042]: https://github.com/delta-io/delta-kernel-rs/pull/1042
[#1040]: https://github.com/delta-io/delta-kernel-rs/pull/1040
[#1022]: https://github.com/delta-io/delta-kernel-rs/pull/1022
[#1044]: https://github.com/delta-io/delta-kernel-rs/pull/1044
[#1019]: https://github.com/delta-io/delta-kernel-rs/pull/1019
[#1053]: https://github.com/delta-io/delta-kernel-rs/pull/1053
[#1046]: https://github.com/delta-io/delta-kernel-rs/pull/1046
[#1057]: https://github.com/delta-io/delta-kernel-rs/pull/1057
[#1061]: https://github.com/delta-io/delta-kernel-rs/pull/1061
[#1065]: https://github.com/delta-io/delta-kernel-rs/pull/1065
[#686]: https://github.com/delta-io/delta-kernel-rs/pull/686
[#1060]: https://github.com/delta-io/delta-kernel-rs/pull/1060
[#980]: https://github.com/delta-io/delta-kernel-rs/pull/980
[#1051]: https://github.com/delta-io/delta-kernel-rs/pull/1051


## [v0.12.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.12.1/) (2025-06-05)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.12.0...v0.12.1)


### 🐛 Bug Fixes

1. Remove azure suffix range request ([#1006])


[#1006]: https://github.com/delta-io/delta-kernel-rs/pull/1006


## [v0.12.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.12.0/) (2025-06-04)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.11.0...v0.12.0)

### 🏗️ Breaking changes
1. Remove `GlobalScanState`: instead use new `Scan` APIs directly (`logical_schema`, `physical_schema`, etc.) ([#947])
2. table feature enums are now `internal_api` (not public, unless `internal-api` flag is set) ([#998])

### 🚀 Features / new APIs

1. Use compacted log files in log-replay ([#950])
2. New `#[derive(IntoEngineData)]` proc macro ([#830])
3. Add support for kernel default expression evaluation ([#979])
4. **New: panic in debug builds if ListedLogFiles breaks invariants** ([#986])
5. Create visitor for getting In-commit Timestamp ([#897])
6. Binary searching utility function for timestamp to version conversion ([#896])
7. Enable "TimestampWithoutTimezone" table feature and add protocol validation for it ([#988])
8. add missing reader/writer features (variantType/clustered) ([#998])

### 🐛 Bug Fixes

1. Disable timestamp column's `maxValues` for data skipping ([#1003])

### 🚜 Refactor

1. Make KernelPredicateEvaluator trait dyn-compatible ([#994])


[#950]: https://github.com/delta-io/delta-kernel-rs/pull/950
[#830]: https://github.com/delta-io/delta-kernel-rs/pull/830
[#979]: https://github.com/delta-io/delta-kernel-rs/pull/979
[#994]: https://github.com/delta-io/delta-kernel-rs/pull/994
[#986]: https://github.com/delta-io/delta-kernel-rs/pull/986
[#897]: https://github.com/delta-io/delta-kernel-rs/pull/897
[#896]: https://github.com/delta-io/delta-kernel-rs/pull/896
[#988]: https://github.com/delta-io/delta-kernel-rs/pull/988
[#947]: https://github.com/delta-io/delta-kernel-rs/pull/947
[#1003]: https://github.com/delta-io/delta-kernel-rs/pull/1003
[#998]: https://github.com/delta-io/delta-kernel-rs/pull/998


## [v0.11.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.11.0/) (2025-05-27)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.10.0...v0.11.0)

### 🏗️ Breaking changes
1. Add in-commit timestamp table feature ([#894])
2. Make `Error` non_exhaustive (will reduce future breaking changes!) ([#913])
3. `Scalar::Map` support ([#881])
   - New `Scalar::Map(MapData)` variant and `MapData` struct to describe `Scalar` maps.
   - New `visit_literal_map` FFI
4. Split out predicates as different from expressions ([#775]): pervasive change which moves some
   expressions to new predicate type.
5. Bump MSRV from 1.81 to 1.82 ([#942])
6. `DataSkippingPredicateEvaluator`'s associated types `TypedStat` and `IntStat` combined into one
   `ColumnStat` type ([#939])
7. Code movement in FFI crate ([#940]):
   - Rename `ffi::expressions::engine` mod as `kernel_visitor`
   - Rename `ffi::expressions::kernel` mod as `engine_visitor`
   - Move the `free_kernel_[expression|predicate]` functions to the `expressions` mod
   - Move the `EnginePredicate` struct to the `ffi::scan` module
8. Fix timestamp ntz in physical to logical cdf ([#948]): now `TableChangesScan::execute` returns
   a schema with `_commit_timestamp` of type `Timestamp` (UTC) instead of `TimestampNtz`.
9. Add TryIntoKernel/Arrow traits ([#946]): Removes old `From`/`Into` implementations for kernel
   schema types, replaces with `TryFromKernel`/`TryIntoKernel`/`TryFromArrow`/`TryIntoArrow`.
   Migration should be as simple as changing a `.try_into()` to a `.try_into_kernel()` or
   `.try_into_arrow()`.
10. Remove `SyncEngine` (now test-only), use `DefaultEngine` everywhere else ([#957])

### 🚀 Features / new APIs

1. Add `Snapshot::checkpoint()` & `Table::checkpoint()` API ([#797])
2. Add CRC ParsedLogPath ([#889])
3. Use arrow array builders in Scalar::to_array ([#905])
4. Add `domainMetadata` read support ([#875])
5. Support maps and arrays in literal_expression_transform ([#882])
6. Add `CheckpointWriter::finalize()` API ([#851])
7. `DataSkippingPredicate` dyn compatible ([#939]): `finish_eval_pred_junction` now takes `&dyn Iterator`
8. Store compacted log files in LogSegment ([#936])
9. Add CRC, FileSizeHistogram, and DeletedRecordCountsHistogram schemas ([#917])
10. Scan from previous result ([#829])
11. Include latest CRC in LogSegment ([#964])
12. CRC protocol+metadata visitor ([#972])
13. Make several types/function pub and fix their doc comments ([#977])
    - `KernelPredicateEvaluator` and `KernelPredicateEvaluatorDefaults` are now pub.
    - `DataSkippingPredicateEvaluator` is now pub.
    - add new type aliases `DirectDataSkippingPredicateEvaluator` and `IndirectDataSkippingPredicateEvaluator`
    - Arrow engine `evaluate_expression` and `evaluate_predicate` are now pub.
    - `Expression::predicate` renamed to `Expression::from_pred`

### 🐛 Bug Fixes

1. Fix incorrect results for `Scalar::Array::to_array` ([#905])
2. Use object_store::Path::from_url_path when appropriate ([#924])
3. Don't include modules via a macro ([#935])
4. Rustc 1.87 clippy fixes ([#955])
5. Allow CheckpointDataIterator to be used across await ([#961])
6. Remove `target-cpu=native` rustflags ([#960])
7. Rename `drop_null_container_values` to `allow_null_container_values` ([#965])
8. Make `ActionsBatch` fields pub for `internal-api` ([#983])

### 📚 Documentation

1. Add readme badges ([#904])

### 🚜 Refactor

1. Combine actions counts in `CheckpointVisitor` ([#883])
2. Simplify Display for Expression and Predicate ([#938])
3. Macro traits cleanup ([#967])
4. Remove redundant binary predicate operations ([#949])
5. Make arrow predicate eval directly invertible ([#956])
6. Add `ActionsBatch` ([#974])

### ⚙️ Chores/CI

1. Remove abs_diff since we have rust 1.81 ([#909])
2. Conditional compilation instead of suppressing clippy warnings ([#945])
3. Expose some more arrow utils via `internal-api` ([#971])
4. Use consistent naming of kernel data type in arrow eval tests ([#978])
5. Cargo doc workspace + all-features ([#981])


[#797]: https://github.com/delta-io/delta-kernel-rs/pull/797
[#909]: https://github.com/delta-io/delta-kernel-rs/pull/909
[#889]: https://github.com/delta-io/delta-kernel-rs/pull/889
[#904]: https://github.com/delta-io/delta-kernel-rs/pull/904
[#894]: https://github.com/delta-io/delta-kernel-rs/pull/894
[#905]: https://github.com/delta-io/delta-kernel-rs/pull/905
[#913]: https://github.com/delta-io/delta-kernel-rs/pull/913
[#881]: https://github.com/delta-io/delta-kernel-rs/pull/881
[#883]: https://github.com/delta-io/delta-kernel-rs/pull/883
[#875]: https://github.com/delta-io/delta-kernel-rs/pull/875
[#775]: https://github.com/delta-io/delta-kernel-rs/pull/775
[#924]: https://github.com/delta-io/delta-kernel-rs/pull/924
[#882]: https://github.com/delta-io/delta-kernel-rs/pull/882
[#851]: https://github.com/delta-io/delta-kernel-rs/pull/851
[#935]: https://github.com/delta-io/delta-kernel-rs/pull/935
[#942]: https://github.com/delta-io/delta-kernel-rs/pull/942
[#938]: https://github.com/delta-io/delta-kernel-rs/pull/938
[#939]: https://github.com/delta-io/delta-kernel-rs/pull/939
[#940]: https://github.com/delta-io/delta-kernel-rs/pull/940
[#945]: https://github.com/delta-io/delta-kernel-rs/pull/945
[#936]: https://github.com/delta-io/delta-kernel-rs/pull/936
[#955]: https://github.com/delta-io/delta-kernel-rs/pull/955
[#917]: https://github.com/delta-io/delta-kernel-rs/pull/917
[#961]: https://github.com/delta-io/delta-kernel-rs/pull/961
[#948]: https://github.com/delta-io/delta-kernel-rs/pull/948
[#960]: https://github.com/delta-io/delta-kernel-rs/pull/960
[#965]: https://github.com/delta-io/delta-kernel-rs/pull/965
[#967]: https://github.com/delta-io/delta-kernel-rs/pull/967
[#829]: https://github.com/delta-io/delta-kernel-rs/pull/829
[#949]: https://github.com/delta-io/delta-kernel-rs/pull/949
[#956]: https://github.com/delta-io/delta-kernel-rs/pull/956
[#946]: https://github.com/delta-io/delta-kernel-rs/pull/946
[#957]: https://github.com/delta-io/delta-kernel-rs/pull/957
[#964]: https://github.com/delta-io/delta-kernel-rs/pull/964
[#972]: https://github.com/delta-io/delta-kernel-rs/pull/972
[#974]: https://github.com/delta-io/delta-kernel-rs/pull/974
[#971]: https://github.com/delta-io/delta-kernel-rs/pull/971
[#978]: https://github.com/delta-io/delta-kernel-rs/pull/978
[#977]: https://github.com/delta-io/delta-kernel-rs/pull/977
[#981]: https://github.com/delta-io/delta-kernel-rs/pull/981
[#983]: https://github.com/delta-io/delta-kernel-rs/pull/983


## [v0.10.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.10.0/) (2025-04-28)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.9.0...v0.10.0)


### 🏗️ Breaking changes
1. Updated dependencies, breaking updates: `itertools 0.14`, `thiserror 2`, and `strum 0.27` ([#814])
2. Rename `developer-visibility` feature flag to `internal-api` ([#834])
3. Tidy up AND/OR/NOT API and usage ([#842])
4. Rename VariadicExpression to JunctionExpression ([#841])
5. Enforce precision/scale correctness of Decimal types and values ([#857])
6. Expression system refactors
   - Make literal expressions more strict (removed `Into` trait impl) ([#867])
   - Remove nearly-unused expression `lt_eq`/`gt_eq` overloads ([#871])
   - Move expression transforms (`ExpressionTransform` and `ExpressionDepthChecker`) to own module ([#878])
   - Code movement in expression-related code (Reordered variants of the `BinaryExpressionOp` enum) ([#879])
7. Introduce the ability for consumers to add ObjectStore url handlers ([#873])
8. Update to arrow 55, drop arrow 53 support ([#885], [#903])

### 🚀 Features / new APIs

1. Add `CheckpointVisitor` in new `checkpoint` mod ([#738])
2. Add `CheckpointLogReplayProcessor` in new `checkpoints` mod ([#744])
3. Add `transaction.with_transaction_id()` API ([#824])
4. Add `snapshot.get_app_id_version(app_id, engine)` ([#862])
5. Overwrite logic in `write_json_file` for default & sync engine  ([#849])

### 🐛 Bug Fixes

1. default engine: Sort list results based on URL scheme ([#820])
2. `impl AllocateError for T: ExternEngine` ([#856])
3. Disable predicate pushdown in `Scan::execute` ([#861])

### 📚 Documentation

1. Correct docstring for `DefaultEngine::new` ([#821])
2. Remove `acceptance` from `rust-analyzer.cargo.features` in README ([#858])

### 🚜 Refactor

1. Rename `predicates` mod to `kernel_predicates` ([#822])
2. Code movement to tidy up ffi ([#840])
3. Grab bag of cosmetic tweaks and comment updates ([#848])
4. New `#[internal_api]` macro instead of `visibility` crate ([#835])
5. Expression transforms use new recurse_into_children helper ([#869])
6. Minor test improvements ([#872])

### ⚙️ Chores/CI

1. Remove unused dependencies ([#863])
2. Test code uses Expr shorthand for Expression ([#866])
3. Arrow DefaultExpressionEvaluator need not box its inner expression ([#868])


[#738]: https://github.com/delta-io/delta-kernel-rs/pull/738
[#821]: https://github.com/delta-io/delta-kernel-rs/pull/821
[#822]: https://github.com/delta-io/delta-kernel-rs/pull/822
[#820]: https://github.com/delta-io/delta-kernel-rs/pull/820
[#814]: https://github.com/delta-io/delta-kernel-rs/pull/814
[#744]: https://github.com/delta-io/delta-kernel-rs/pull/744
[#840]: https://github.com/delta-io/delta-kernel-rs/pull/840
[#834]: https://github.com/delta-io/delta-kernel-rs/pull/834
[#842]: https://github.com/delta-io/delta-kernel-rs/pull/842
[#841]: https://github.com/delta-io/delta-kernel-rs/pull/841
[#848]: https://github.com/delta-io/delta-kernel-rs/pull/848
[#835]: https://github.com/delta-io/delta-kernel-rs/pull/835
[#856]: https://github.com/delta-io/delta-kernel-rs/pull/856
[#824]: https://github.com/delta-io/delta-kernel-rs/pull/824
[#849]: https://github.com/delta-io/delta-kernel-rs/pull/849
[#863]: https://github.com/delta-io/delta-kernel-rs/pull/863
[#858]: https://github.com/delta-io/delta-kernel-rs/pull/858
[#862]: https://github.com/delta-io/delta-kernel-rs/pull/862
[#866]: https://github.com/delta-io/delta-kernel-rs/pull/866
[#857]: https://github.com/delta-io/delta-kernel-rs/pull/857
[#861]: https://github.com/delta-io/delta-kernel-rs/pull/861
[#867]: https://github.com/delta-io/delta-kernel-rs/pull/867
[#868]: https://github.com/delta-io/delta-kernel-rs/pull/868
[#869]: https://github.com/delta-io/delta-kernel-rs/pull/869
[#871]: https://github.com/delta-io/delta-kernel-rs/pull/871
[#872]: https://github.com/delta-io/delta-kernel-rs/pull/872
[#878]: https://github.com/delta-io/delta-kernel-rs/pull/878
[#873]: https://github.com/delta-io/delta-kernel-rs/pull/873
[#879]: https://github.com/delta-io/delta-kernel-rs/pull/879
[#885]: https://github.com/delta-io/delta-kernel-rs/pull/885
[#903]: https://github.com/delta-io/delta-kernel-rs/pull/903


## [v0.9.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.9.0/) (2025-04-08)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.8.0...v0.9.0)

### 🏗️ Breaking changes
1. Change `MetadataValue::Number(i32)` to `MetadataValue::Number(i64)` ([#733])
2. Get prefix from offset path: `DefaultEngine::new` no longer requires a `table_root` parameter
   and `list_from` consistently returns keys greater than the offset ([#699])
3. Make `snapshot.schema()` return a `SchemaRef` ([#751])
4. Make `visit_expression_internal` private, and `unwrap_kernel_expression` pub(crate) ([#767])
5. Make actions types `pub(crate)` instead of `pub` ([#405])
6. New `null_row` ExpressionHandler API ([#662])
7. Rename enums `ReaderFeatures` -> `ReaderFeature` and `WriterFeatures` -> `WriterFeature` ([#802])
8. Remove `get_` prefix from engine getters ([#804])
9. Rename `FileSystemClient` to `StorageHandler` ([#805])
10. Adopt types for table features (New `ReadFeature::Unknown(String)` and
    (`WriterFeature::Unknown(String)`) ([#684])
11. Renamed `ScanData` to `ScanMetadata` ([#817])
    - rename `ScanData` to `ScanMetadata`
    - rename `Scan::scan_data()` to `Scan::scan_metadata()`
    - (ffi) rename `free_kernel_scan_data()` to `free_scan_metadata_iter()`
    - (ffi) rename `kernel_scan_data_next()` to `scan_metadata_next()`
    - (ffi) rename `visit_scan_data()` to `visit_scan_metadata()`
    - (ffi) rename `kernel_scan_data_init()` to `scan_metadata_iter_init()`
    - (ffi) rename `KernelScanDataIterator` to `ScanMetadataIterator`
    - (ffi) rename `SharedScanDataIterator` to `SharedScanMetadataIterator`
12. `ScanMetadata` is now a struct (instead of tuple) with new `FiltereEngineData` type  ([#768])

### 🚀 Features / new APIs

1. (`v2Checkpoint`) Extract & insert sidecar batches in `replay`'s action iterator ([#679])
2. Support the `v2Checkpoint` reader/writer feature ([#685])
3. Add check for whether `appendOnly` table feature is supported or enabled  ([#664])
4. Add basic partition pruning support ([#713])
5. Add `DeletionVectors` to supported writer features ([#735])
6. Add writer version 2/invariant table feature support ([#734])
7. Improved pre-signed URL checks ([#760])
8. Add `CheckpointMetadata` action ([#781])
9. Add classic and uuid parquet checkpoint path generation ([#782])
10. New `Snapshot::try_new_from()` API ([#549])

### 🐛 Bug Fixes

1. Return `Error::unsupported` instead of panic in `Scalar::to_array(MapType)` ([#757])
2. Remove 'default-members' in workspace, default to all crates ([#752])
3. Update compilation error and clippy lints for rustc 1.86 ([#800])

### 🚜 Refactor

1. Split up `arrow_expression` module ([#750])
2. Flatten deeply nested match statement ([#756])
3. Simplify predicate evaluation by supporting inversion ([#761])
4. Rename `LogSegment::replay` to `LogSegment::read_actions` ([#766])
5. Extract deduplication logic from `AddRemoveDedupVisitor` into embeddable `FileActionsDeduplicator` ([#769])
6. Move testing helper function to `test_utils` mod ([#794])
7. Rename `_last_checkpoint` from `CheckpointMetadata` to `LastCheckpointHint` ([#789])
8. Use ExpressionTransform instead of adhoc expression traversals ([#803])
9. Extract log replay processing structure into `LogReplayProcessor` trait ([#774])

### 🧪 Testing

1. Add V2 checkpoint read support integration tests ([#690])

### ⚙️ Chores/CI

1. Use maintained action to setup rust toolchain ([#585])

### Other

1. Update HDFS dependencies ([#689])
2. Add .cargo/config.toml with native instruction codegen ([#772])


[#679]: https://github.com/delta-io/delta-kernel-rs/pull/679
[#685]: https://github.com/delta-io/delta-kernel-rs/pull/685
[#689]: https://github.com/delta-io/delta-kernel-rs/pull/689
[#664]: https://github.com/delta-io/delta-kernel-rs/pull/664
[#690]: https://github.com/delta-io/delta-kernel-rs/pull/690
[#713]: https://github.com/delta-io/delta-kernel-rs/pull/713
[#735]: https://github.com/delta-io/delta-kernel-rs/pull/735
[#734]: https://github.com/delta-io/delta-kernel-rs/pull/734
[#733]: https://github.com/delta-io/delta-kernel-rs/pull/733
[#585]: https://github.com/delta-io/delta-kernel-rs/pull/585
[#750]: https://github.com/delta-io/delta-kernel-rs/pull/750
[#756]: https://github.com/delta-io/delta-kernel-rs/pull/756
[#757]: https://github.com/delta-io/delta-kernel-rs/pull/757
[#699]: https://github.com/delta-io/delta-kernel-rs/pull/699
[#752]: https://github.com/delta-io/delta-kernel-rs/pull/752
[#751]: https://github.com/delta-io/delta-kernel-rs/pull/751
[#761]: https://github.com/delta-io/delta-kernel-rs/pull/761
[#760]: https://github.com/delta-io/delta-kernel-rs/pull/760
[#766]: https://github.com/delta-io/delta-kernel-rs/pull/766
[#767]: https://github.com/delta-io/delta-kernel-rs/pull/767
[#405]: https://github.com/delta-io/delta-kernel-rs/pull/405
[#772]: https://github.com/delta-io/delta-kernel-rs/pull/772
[#662]: https://github.com/delta-io/delta-kernel-rs/pull/662
[#769]: https://github.com/delta-io/delta-kernel-rs/pull/769
[#794]: https://github.com/delta-io/delta-kernel-rs/pull/794
[#781]: https://github.com/delta-io/delta-kernel-rs/pull/781
[#789]: https://github.com/delta-io/delta-kernel-rs/pull/789
[#800]: https://github.com/delta-io/delta-kernel-rs/pull/800
[#802]: https://github.com/delta-io/delta-kernel-rs/pull/802
[#803]: https://github.com/delta-io/delta-kernel-rs/pull/803
[#774]: https://github.com/delta-io/delta-kernel-rs/pull/774
[#804]: https://github.com/delta-io/delta-kernel-rs/pull/804
[#782]: https://github.com/delta-io/delta-kernel-rs/pull/782
[#805]: https://github.com/delta-io/delta-kernel-rs/pull/805
[#549]: https://github.com/delta-io/delta-kernel-rs/pull/549
[#684]: https://github.com/delta-io/delta-kernel-rs/pull/684
[#817]: https://github.com/delta-io/delta-kernel-rs/pull/817
[#768]: https://github.com/delta-io/delta-kernel-rs/pull/768


## [v0.8.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.8.0/) (2025-03-04)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.7.0...v0.8.0)

### 🏗️ Breaking changes

1. ffi: `get_partition_column_count` and `get_partition_columns` now take a `Snapshot` instead of a
   `Scan` ([#697])
2. ffi: expression visitor callback `visit_literal_decimal` now takes `i64` for the upper half of a 128-bit int value  ([#724])
3. - `DefaultJsonHandler::with_readahead()` renamed to `DefaultJsonHandler::with_buffer_size()` ([#711])
4. DefaultJsonHandler's defaults changed:
  - default buffer size: 10 => 1000 requests/files
  - default batch size: 1024 => 1000 rows
5. Bump MSRV to rustc 1.81 ([#725])

### 🐛 Bug Fixes

1. Pin `chrono` version to fix arrow compilation failure ([#719])

### ⚡ Performance

1. Replace default engine JSON reader's `FileStream` with concurrent futures ([#711])


[#719]: https://github.com/delta-io/delta-kernel-rs/pull/719
[#724]: https://github.com/delta-io/delta-kernel-rs/pull/724
[#697]: https://github.com/delta-io/delta-kernel-rs/pull/697
[#725]: https://github.com/delta-io/delta-kernel-rs/pull/725
[#711]: https://github.com/delta-io/delta-kernel-rs/pull/711


## [v0.7.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.7.0/) (2025-02-24)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.6.1...v0.7.0)

### 🏗️ Breaking changes
1. Read transforms are now communicated via expressions ([#607], [#612], [#613], [#614]) This includes:
    - `ScanData` now includes a third tuple field: a row-indexed vector of transforms to apply to the `EngineData`.
    - Adds a new `scan::state::transform_to_logical` function that encapsulates the boilerplate of applying the transform expression
    - Removes `scan_action_iter` API and `logical_to_physical` API
    - Removes `column_mapping_mode` from `GlobalScanState`
    - ffi: exposes methods to get an expression evaluator and evaluate an expression from c
    - read-table example: Removes `add_partition_columns` in arrow.c
    - read-table example: adds an `apply_transform` function in arrow.c
2. ffi: support field nullability in schema visitor ([#656])
3. ffi: expose metadata in SchemaEngineVisitor ffi api ([#659])
4. ffi: new `visit_schema` FFI now operates on a `Schema` instead of a `Snapshot` ([#683], [#709])
5. Introduced feature flags (`arrow_54` and `arrow_53`) to select major arrow versions ([#654], [#708], [#717])

### 🚀 Features / new APIs

1. Read `partition_values` in `RemoveVisitor` and remove `break` in `RowVisitor` for `RemoveVisitor` ([#633])
2. Add the in-commit timestamp field to CommitInfo ([#581])
3. Support NOT and column expressions in eval_sql_where ([#653])
4. Add check for schema read compatibility ([#554])
5. Introduce `TableConfiguration` to jointly manage metadata, protocol, and table properties ([#644])
6. Add visitor `SidecarVisitor` and `Sidecar` action struct  ([#673])
7. Add in-commit timestamps table properties ([#558])
8. Support writing to writer version 1 ([#693])
9. ffi: new `logical_schema` FFI to get the logical schema of a snapshot ([#709])

### 🐛 Bug Fixes

1. Incomplete multi-part checkpoint handling when no hint is provided ([#641])
2. Consistent PartialEq for Scalar ([#677])
3. Cargo fmt does not handle mods defined in macros ([#676])
4. Ensure properly nested null masks for parquet reads ([#692])
5. Handle predicates on non-nullable columns without stats ([#700])

### 📚 Documentation

1. Update readme to reflect tracing feature is needed for read-table ([#619])
2. Clarify `JsonHandler` semantics on EngineData ordering ([#635])

### 🚜 Refactor

1. Make [non] nullable struct fields easier to create ([#646])
2. Make eval_sql_where available to DefaultPredicateEvaluator ([#627])

### 🧪 Testing

1. Port cdf tests from delta-spark to kernel ([#611])

### ⚙️ Chores/CI

1. Fix some typos ([#643])
2. Release script publishing fixes ([#638])

[#638]: https://github.com/delta-io/delta-kernel-rs/pull/638
[#643]: https://github.com/delta-io/delta-kernel-rs/pull/643
[#619]: https://github.com/delta-io/delta-kernel-rs/pull/619
[#635]: https://github.com/delta-io/delta-kernel-rs/pull/635
[#633]: https://github.com/delta-io/delta-kernel-rs/pull/633
[#611]: https://github.com/delta-io/delta-kernel-rs/pull/611
[#581]: https://github.com/delta-io/delta-kernel-rs/pull/581
[#646]: https://github.com/delta-io/delta-kernel-rs/pull/646
[#627]: https://github.com/delta-io/delta-kernel-rs/pull/627
[#641]: https://github.com/delta-io/delta-kernel-rs/pull/641
[#653]: https://github.com/delta-io/delta-kernel-rs/pull/653
[#607]: https://github.com/delta-io/delta-kernel-rs/pull/607
[#656]: https://github.com/delta-io/delta-kernel-rs/pull/656
[#554]: https://github.com/delta-io/delta-kernel-rs/pull/554
[#644]: https://github.com/delta-io/delta-kernel-rs/pull/644
[#659]: https://github.com/delta-io/delta-kernel-rs/pull/659
[#612]: https://github.com/delta-io/delta-kernel-rs/pull/612
[#677]: https://github.com/delta-io/delta-kernel-rs/pull/677
[#676]: https://github.com/delta-io/delta-kernel-rs/pull/676
[#673]: https://github.com/delta-io/delta-kernel-rs/pull/673
[#613]: https://github.com/delta-io/delta-kernel-rs/pull/613
[#558]: https://github.com/delta-io/delta-kernel-rs/pull/558
[#692]: https://github.com/delta-io/delta-kernel-rs/pull/692
[#700]: https://github.com/delta-io/delta-kernel-rs/pull/700
[#683]: https://github.com/delta-io/delta-kernel-rs/pull/683
[#654]: https://github.com/delta-io/delta-kernel-rs/pull/654
[#693]: https://github.com/delta-io/delta-kernel-rs/pull/693
[#614]: https://github.com/delta-io/delta-kernel-rs/pull/614
[#709]: https://github.com/delta-io/delta-kernel-rs/pull/709
[#708]: https://github.com/delta-io/delta-kernel-rs/pull/708
[#717]: https://github.com/delta-io/delta-kernel-rs/pull/717


## [v0.6.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.6.1/) (2025-01-10)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.6.0...v0.6.1)


### 🚀 Features / new APIs

1. New feature flag `default-engine-rustls` ([#572])

### 🐛 Bug Fixes

1. Allow partition value timestamp to be ISO8601 formatted string ([#622])
2. Fix stderr output for handle tests ([#630])

### ⚙️ Chores/CI

1. Expand the arrow version range to allow arrow v54 ([#616])
2. Update to CodeCov @v5 ([#608])

### Other

1. Fix msrv check by pinning `home` dependency ([#605])
2. Add release script ([#636])


[#605]: https://github.com/delta-io/delta-kernel-rs/pull/605
[#608]: https://github.com/delta-io/delta-kernel-rs/pull/608
[#622]: https://github.com/delta-io/delta-kernel-rs/pull/622
[#630]: https://github.com/delta-io/delta-kernel-rs/pull/630
[#572]: https://github.com/delta-io/delta-kernel-rs/pull/572
[#616]: https://github.com/delta-io/delta-kernel-rs/pull/616
[#636]: https://github.com/delta-io/delta-kernel-rs/pull/636


## [v0.6.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.6.0/) (2024-12-17)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.5.0...v0.6.0)

**API Changes**

*Breaking*
1. `Scan::execute` takes an `Arc<dyn EngineData>` now ([#553])
2. `StructField::physical_name` no longer takes a `ColumnMapping` argument ([#543])
3. removed `ColumnMappingMode` `Default` implementation ([#562])
4. Remove lifetime requirement on `Scan::execute` ([#588])
5. `scan::Scan::predicate` renamed as `physical_predicate` to eliminate ambiguity ([#512])
6. `scan::log_replay::scan_action_iter` now takes fewer (and different) params. ([#512])
7. `Expression::Unary`, `Expression::Binary`, and `Expression::Variadic` now wrap a struct of the
   same name containing their fields ([#530])
8. Moved `delta_kernel::engine::parquet_stats_skipping` module to
   `delta_kernel::predicate::parquet_stats_skipping` ([#602])
9. New `Error` variants `Error::ChangeDataFeedIncompatibleSchema` and `Error::InvalidCheckpoint`
   ([#593])

*Additions*
1. Ability to read a table's change data feed with new TableChanges API! See new `table_changes`
   module as well as the 'read-table-changes' example ([#597]). Changes include:
  - Implement Log Replay for Change Data Feed ([#540])
  - `ScanFile` expression and visitor for CDF ([#546])
  - Resolve deletion vectors to find inserted and removed rows for CDF ([#568])
  - Helper methods for CDF Physical to Logical Transformation ([#579])
  - `TableChangesScan::execute` and end to end testing for CDF ([#580])
  - `TableChangesScan::schema` method to get logical schema ([#589])
2. Enable relaying log events via FFI ([#542])

**Implemented enhancements:**
- Define an ExpressionTransform trait ([#530])
- [chore] appease clippy in rustc 1.83 ([#557])
- Simplify column mapping mode handling ([#543])
- Adding some more miri tests ([#503])
- Data skipping correctly handles nested columns and column mapping ([#512])
- Engines now return FileMeta with correct millisecond timestamps ([#565])

**Fixed bugs:**
- don't use std abs_diff, put it in test_utils instead, run tests with msrv in action ([#596])
- (CDF) Add fix for sv extension ([#591])
- minimal CI fixes in arrow integration test and semver check ([#548])

[#503]: https://github.com/delta-io/delta-kernel-rs/pull/503
[#512]: https://github.com/delta-io/delta-kernel-rs/pull/512
[#530]: https://github.com/delta-io/delta-kernel-rs/pull/530
[#540]: https://github.com/delta-io/delta-kernel-rs/pull/540
[#542]: https://github.com/delta-io/delta-kernel-rs/pull/542
[#543]: https://github.com/delta-io/delta-kernel-rs/pull/543
[#546]: https://github.com/delta-io/delta-kernel-rs/pull/546
[#548]: https://github.com/delta-io/delta-kernel-rs/pull/548
[#553]: https://github.com/delta-io/delta-kernel-rs/pull/553
[#557]: https://github.com/delta-io/delta-kernel-rs/pull/557
[#562]: https://github.com/delta-io/delta-kernel-rs/pull/562
[#565]: https://github.com/delta-io/delta-kernel-rs/pull/565
[#568]: https://github.com/delta-io/delta-kernel-rs/pull/568
[#579]: https://github.com/delta-io/delta-kernel-rs/pull/579
[#580]: https://github.com/delta-io/delta-kernel-rs/pull/580
[#588]: https://github.com/delta-io/delta-kernel-rs/pull/588
[#589]: https://github.com/delta-io/delta-kernel-rs/pull/589
[#591]: https://github.com/delta-io/delta-kernel-rs/pull/591
[#593]: https://github.com/delta-io/delta-kernel-rs/pull/593
[#596]: https://github.com/delta-io/delta-kernel-rs/pull/596
[#597]: https://github.com/delta-io/delta-kernel-rs/pull/597
[#602]: https://github.com/delta-io/delta-kernel-rs/pull/602


## [v0.5.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.5.0/) (2024-11-26)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.4.0...0.5.0)

**API Changes**

*Breaking*

1. `Expression::Column(String)` is now `Expression::Column(ColumnName)` [\#400]
2. delta_kernel_ffi::expressions moved into two modules:
   `delta_kernel_ffi::expressions::engine` and `delta_kernel_ffi::expressions::kernel` [\#363]
3. FFI: removed (hazardous) `impl From` for `KernelStringSlize` and added `unsafe` constructor
   instead [\#441]
4. Moved `LogSegment` into its own module (`log_segment::LogSegment`) [\#438]
5. Renamed `EngineData::length` as `EngineData::len` [\#471]
6. New `AsAny` trait: `AsAny: Any + Send + Sync` required bound on all engine traits [\#450]
7. Rename `mod features` to `mod table_features` [\#454]
8. LogSegment fields renamed: `commit_files` -> `ascending_commit_files` and `checkpoint_files` ->
    `checkpoint_parts` [\#495]
9. Added minimum-supported rust version: currenly rust 1.80 [\#504]
10. Improved row visitor API: renamed `EngineData::extract` as `EngineData::visit_rows`, and
    `DataVisitor` trait renamed as `RowVisitor` [\#481]
11. FFI: New `mod engine_data` and `mod error` (moved `Error` to `error::Error`) [\#537]
12. new error types: `InvalidProtocol`, `InvalidCommitInfo`, `MissingCommitInfo`,
    `FileAlreadyExists`, `Unsupported`, `ParseIntervalError`, `ChangeDataFeedUnsupported`

*Additions*

1. New `ColumnName`, `column_name!`, `column_expr!` for structured column name parsing. [\#400]
   [\#467]
2. New `Engine` API `write_json_file()` for atomically writing JSON [\#370]
3. New `Transaction` API for creating transactions, adding commit info and write metadata, and
   commiting the transaction to the table. Includes `Table.new_transaction()`,
   `Transaction.write_context()`, `Transaction.with_commit_info`, `Transaction.with_operation()`,
   `Transaction.with_write_metadata()`, and `Transaction.commit()` [\#370] [\#393]
4. FFI: Visitor for converting kernel expressions to engine expressions. See the new example at
   `ffi/examples/visit-expression/` [\#363]
5. FFI: New `TryFromStringSlice` trait and `kernel_string_slice` macro [\#441]
6. New `DefaultEngine` engine implementation for writing parquet: `write_parquet_file()` [\#393]
7. Added support for parsing comma-separated column name lists:
   `ColumnName::parse_column_name_list()` [\#458]
9. New `VacuumProtocolCheck` table feature [\#454]
10. `DvInfo` now implements `Clone`, `PartialEq`, and `Eq` [\#468]
11. `Stats` now implements `Debug`, `Clone`, `PartialEq`, and `Eq` [\#468]
12. Added `Cdc` action support [\#506]
13. (early CDF read support) New `TableChanges` type to read CDF from a table between versions
    [\#505]
14. (early CDF read support) Builder for scans on `TableChanges` [\#521]
15. New `TableProperties` struct which can parse tables' `metadata.configuration` [\#453] [\#536]

**Implemented enhancements:**
- FFI examples now use AddressSanitizer [\#447]
- `ColumnName` now tracks a path of field names instead of a simple string [\#445]
- use `ParsedLogPaths` for files in `LogSegment` [\#472]
- FFI: added Miri support for tests [\#470]
- check table URI has trailing slash [\#432]
- build `cargo docs` in CI [\#479]
- new `test-utils` crate [\#477]
- added proper protocol validation (both parsing correctness and semantic correctness) [\#454]
  [\#493]
- harmonize predicate evaluation between delta stats and parquet footer stats [\#420]
- more log path tests [\#485]
- `ensure_read_supported` and `ensure_write_supported` APIs [\#518]
- include NOTICE and LICENSE in published crates [\#520]
- FFI: factored out read_table kernel utils into `kernel_utils.h/c` [\#539]
- simplified log replay visitor and avoid materializing Add/Remove actions [\#494]
- simplified schema transform API [\#531]
- support arrow view types in conversion from `ArrowDataType` to kernel's `DataType` [\#533]

**Fixed bugs:**

- **Disabled missing-column row group skipping**: The optimization to treat a physically missing
  column as all-null is unsound, if the schema was not already verified to prove that the table's
  logical schema actually includes the missing column. We disable it until we can add the necessary
  validation. [\#435]
- fixed leaks in read_table FFI example [\#449]
- fixed read_table compilation on windows [\#455]
- fixed various predicate eval bugs [\#420]

[\#400]: https://github.com/delta-io/delta-kernel-rs/pull/400
[\#370]: https://github.com/delta-io/delta-kernel-rs/pull/370
[\#363]: https://github.com/delta-io/delta-kernel-rs/pull/363
[\#435]: https://github.com/delta-io/delta-kernel-rs/pull/435
[\#447]: https://github.com/delta-io/delta-kernel-rs/pull/447
[\#449]: https://github.com/delta-io/delta-kernel-rs/pull/449
[\#441]: https://github.com/delta-io/delta-kernel-rs/pull/441
[\#455]: https://github.com/delta-io/delta-kernel-rs/pull/455
[\#445]: https://github.com/delta-io/delta-kernel-rs/pull/445
[\#393]: https://github.com/delta-io/delta-kernel-rs/pull/393
[\#458]: https://github.com/delta-io/delta-kernel-rs/pull/458
[\#438]: https://github.com/delta-io/delta-kernel-rs/pull/438
[\#468]: https://github.com/delta-io/delta-kernel-rs/pull/468
[\#472]: https://github.com/delta-io/delta-kernel-rs/pull/472
[\#470]: https://github.com/delta-io/delta-kernel-rs/pull/470
[\#471]: https://github.com/delta-io/delta-kernel-rs/pull/471
[\#432]: https://github.com/delta-io/delta-kernel-rs/pull/432
[\#479]: https://github.com/delta-io/delta-kernel-rs/pull/479
[\#477]: https://github.com/delta-io/delta-kernel-rs/pull/477
[\#450]: https://github.com/delta-io/delta-kernel-rs/pull/450
[\#454]: https://github.com/delta-io/delta-kernel-rs/pull/454
[\#467]: https://github.com/delta-io/delta-kernel-rs/pull/467
[\#493]: https://github.com/delta-io/delta-kernel-rs/pull/493
[\#495]: https://github.com/delta-io/delta-kernel-rs/pull/495
[\#420]: https://github.com/delta-io/delta-kernel-rs/pull/420
[\#485]: https://github.com/delta-io/delta-kernel-rs/pull/485
[\#504]: https://github.com/delta-io/delta-kernel-rs/pull/504
[\#506]: https://github.com/delta-io/delta-kernel-rs/pull/506
[\#518]: https://github.com/delta-io/delta-kernel-rs/pull/518
[\#520]: https://github.com/delta-io/delta-kernel-rs/pull/520
[\#505]: https://github.com/delta-io/delta-kernel-rs/pull/505
[\#481]: https://github.com/delta-io/delta-kernel-rs/pull/481
[\#521]: https://github.com/delta-io/delta-kernel-rs/pull/521
[\#453]: https://github.com/delta-io/delta-kernel-rs/pull/453
[\#536]: https://github.com/delta-io/delta-kernel-rs/pull/536
[\#539]: https://github.com/delta-io/delta-kernel-rs/pull/539
[\#537]: https://github.com/delta-io/delta-kernel-rs/pull/537
[\#494]: https://github.com/delta-io/delta-kernel-rs/pull/494
[\#533]: https://github.com/delta-io/delta-kernel-rs/pull/533
[\#531]: https://github.com/delta-io/delta-kernel-rs/pull/531


## [v0.4.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.4.1/) (2024-10-28)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.4.0...v0.4.1)

**API Changes**

None.

**Fixed bugs:**

- **Disabled missing-column row group skipping**: The optimization to treat a physically missing
column as all-null is unsound, if the schema was not already verified to prove that the table's
logical schema actually includes the missing column. We disable it until we can add the necessary
validation. [\#435]

[\#435]: https://github.com/delta-io/delta-kernel-rs/pull/435

## [v0.4.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.4.0/) (2024-10-23)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.3.1...v0.4.0)

**API Changes**

*Breaking*

1. `pub ScanResult.mask` field made private and only accessible as `ScanResult.raw_mask()` method [\#374]
2. new `ReaderFeatures` enum variant: `TypeWidening` and `TypeWideningPreview` [\#335]
3. new `WriterFeatures` enum variant: `TypeWidening` and `TypeWideningPreview` [\#335]
4. new `Error` enum variant: `InvalidLogPath` when kernel is unable to parse the name of a log path [\#347]
5. Module moved: `mod delta_kernel::transaction` -> `mod delta_kernel::actions::set_transaction` [\#386]
6. change `default-feature` to be none (removed `sync-engine` by default. If downstream users relied on this, turn on `sync-engine` feature or specific arrow-related feature flags to pull in the pieces needed) [\#339]
7. `Scan`'s `execute(..)` method now returns a lazy iterator instead of materializing a `Vec<ScanResult>`. You can trivially migrate to the new API (and force eager materialization by using `.collect()` or the like on the returned iterator) [\#340]
8. schema and expression FFI moved to their own `mod delta_kernel_ffi::schema` and `mod delta_kernel_ffi::expressions` [\#360]
9. Parquet and JSON readers in `Engine` trait now take `Arc<Expression>` (aliased to `ExpressionRef`) instead of `Expression` [\#364]
10. `StructType::new(..)` now takes an `impl IntoIterator<Item = StructField>` instead of `Vec<StructField>` [\#385]
11. `DataType::struct_type(..)` now takes an `impl IntoIterator<Item = StructField>` instead of `Vec<StructField>` [\#385]
12. removed `DataType::array_type(..)` API: there is already an `impl From<ArrayType> for DataType` [\#385]
13. `Expression::struct_expr(..)` renamed to `Expression::struct_from(..)` [\#399]
14. lots of expressions take `impl Into<Self>` or `impl Into<Expression>` instead of just `Self`/`Expression` now [\#399]
15. remove `log_replay_iter` and `process_batch` APIs in `scan::log_replay` [\#402]

*Additions*

1. remove feature flag requirement for `impl GetData` on `()` [\#334]
2. new `full_mask()` method on `ScanResult` [\#374]
3. `StructType::try_new(fields: impl IntoIterator<Item = StructField>)` [\#385]
4. `DataType::try_struct_type(fields: impl IntoIterator<Item = StructField>)` [\#385]
5. `StructField.metadata_with_string_values(&self) -> HashMap<String, String>` to materialize and return our metadata into a hashmap [\#331]

**Implemented enhancements:**

- support reading tables with type widening in default engine [\#335]
- add predicate to protocol and metadata log replay for pushdown [\#336] and [\#343]
- support annotation (macro) for nullable values in a container (for `#[derive(Schema)]`) [\#342]
- new `ParsedLogPath` type for better log path parsing [\#347]
- implemented row group skipping for default engine parquet readers and new utility trait for stats-based skipping logic [\#357], [\#362], [\#381]
- depend on wider arrow versions and add arrow integration testing [\#366] and [\#413]
- added semver testing to CI [\#369], [\#383], [\#384]
- new `SchemaTransform` trait and usage in column mapping and data skipping [\#395] and [\#398]
- arrow expression evaluation improvements [\#401]
- replace panics with `to_compiler_error` in macros [\#409]

**Fixed bugs:**

- output of arrow expression evaluation now applies/validates output schema in default arrow expression handler [\#331]
- add `arrow-buffer` to `arrow-expression` feature [\#332]
- fix bug with out-of-date last checkpoint [\#354]
- fixed broken sync engine json parsing and harmonized sync/async json parsing [\#373]
- filesystem client now always returns a sorted list [\#344]

[\#331]: https://github.com/delta-io/delta-kernel-rs/pull/331
[\#332]: https://github.com/delta-io/delta-kernel-rs/pull/332
[\#334]: https://github.com/delta-io/delta-kernel-rs/pull/334
[\#335]: https://github.com/delta-io/delta-kernel-rs/pull/335
[\#336]: https://github.com/delta-io/delta-kernel-rs/pull/336
[\#337]: https://github.com/delta-io/delta-kernel-rs/pull/337
[\#339]: https://github.com/delta-io/delta-kernel-rs/pull/339
[\#340]: https://github.com/delta-io/delta-kernel-rs/pull/340
[\#342]: https://github.com/delta-io/delta-kernel-rs/pull/342
[\#343]: https://github.com/delta-io/delta-kernel-rs/pull/343
[\#344]: https://github.com/delta-io/delta-kernel-rs/pull/344
[\#347]: https://github.com/delta-io/delta-kernel-rs/pull/347
[\#354]: https://github.com/delta-io/delta-kernel-rs/pull/354
[\#357]: https://github.com/delta-io/delta-kernel-rs/pull/357
[\#360]: https://github.com/delta-io/delta-kernel-rs/pull/360
[\#362]: https://github.com/delta-io/delta-kernel-rs/pull/362
[\#364]: https://github.com/delta-io/delta-kernel-rs/pull/364
[\#366]: https://github.com/delta-io/delta-kernel-rs/pull/366
[\#369]: https://github.com/delta-io/delta-kernel-rs/pull/369
[\#373]: https://github.com/delta-io/delta-kernel-rs/pull/373
[\#374]: https://github.com/delta-io/delta-kernel-rs/pull/374
[\#381]: https://github.com/delta-io/delta-kernel-rs/pull/381
[\#383]: https://github.com/delta-io/delta-kernel-rs/pull/383
[\#384]: https://github.com/delta-io/delta-kernel-rs/pull/384
[\#385]: https://github.com/delta-io/delta-kernel-rs/pull/385
[\#386]: https://github.com/delta-io/delta-kernel-rs/pull/386
[\#395]: https://github.com/delta-io/delta-kernel-rs/pull/395
[\#398]: https://github.com/delta-io/delta-kernel-rs/pull/398
[\#399]: https://github.com/delta-io/delta-kernel-rs/pull/399
[\#401]: https://github.com/delta-io/delta-kernel-rs/pull/401
[\#402]: https://github.com/delta-io/delta-kernel-rs/pull/402
[\#409]: https://github.com/delta-io/delta-kernel-rs/pull/409
[\#413]: https://github.com/delta-io/delta-kernel-rs/pull/413


## [v0.3.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.3.1/) (2024-09-10)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.3.0...v0.3.1)

**API Changes**

*Additions*

1. Two new binary expressions: `In` and `NotIn`, as well as a new `Scalar::Array` variant to represent arrays in the expression framework [\#270](https://github.com/delta-io/delta-kernel-rs/pull/270) NOTE: exact API for these expressions is still evolving.

**Implemented enhancements:**

- Enabled more golden table tests [\#301](https://github.com/delta-io/delta-kernel-rs/pull/301)

**Fixed bugs:**

- Allow kernel to read tables with invalid `_last_checkpoint` [\#311](https://github.com/delta-io/delta-kernel-rs/pull/311)
- List log files with checkpoint hint when constructing latest snapshot (when version requested is `None`) [\#312](https://github.com/delta-io/delta-kernel-rs/pull/312)
- Fix incorrect offset value when computing list offsets [\#327](https://github.com/delta-io/delta-kernel-rs/pull/327)
- Fix metadata string conversion in default engine arrow conversion [\#328](https://github.com/delta-io/delta-kernel-rs/pull/328)

## [v0.3.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.3.0/) (2024-08-07)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.2.0...v0.3.0)

**API Changes**

*Breaking*

1. `delta_kernel::column_mapping` module moved to `delta_kernel::features::column_mapping` [\#222](https://github.com/delta-io/delta-kernel-rs/pull/297)


*Additions*

1. New deletion vector API `row_indexes` (and accompanying FFI) to get row indexes instead of seletion vector of deleted rows. This can be more efficient for sparse DVs. [\#215](https://github.com/delta-io/delta-kernel-rs/pull/215)
2. Typed table features: `ReaderFeatures`, `WriterFeatures` enums and `has_reader_feature`/`has_writer_feature` API [\#222](https://github.com/delta-io/delta-kernel-rs/pull/297)

**Implemented enhancements:**

- Add `--limit` option to example `read-table-multi-threaded` [\#297](https://github.com/delta-io/delta-kernel-rs/pull/297)
- FFI now built with cmake. Move to using the read-test example as an ffi-test. And building on macos. [\#288](https://github.com/delta-io/delta-kernel-rs/pull/288)
- Golden table tests migrated from delta-spark/delta-kernel java [\#295](https://github.com/delta-io/delta-kernel-rs/pull/295)
- Code coverage implemented via [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) and reported with [codecov](https://app.codecov.io/github/delta-io/delta-kernel-rs) [\#287](https://github.com/delta-io/delta-kernel-rs/pull/287)
- All tests enabled to run in CI [\#284](https://github.com/delta-io/delta-kernel-rs/pull/284)
- Updated DAT to 0.3 [\#290](https://github.com/delta-io/delta-kernel-rs/pull/290)

**Fixed bugs:**

- Evaluate timestamps as "UTC" instead of "+00:00" for timezone [\#295](https://github.com/delta-io/delta-kernel-rs/pull/295)
- Make Map arrow type field naming consistent with parquet field naming [\#299](https://github.com/delta-io/delta-kernel-rs/pull/299)


## [v0.2.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.2.0/) (2024-07-17)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.1.1...v0.2.0)

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

- Handle nested structs in `schemaString` (allows reading iceberg compat tables) [\#257](https://github.com/delta-io/delta-kernel-rs/pull/257)
- Expose top level stats in scans [\#227](https://github.com/delta-io/delta-kernel-rs/pull/227)
- Hugely expanded C-FFI example [\#203](https://github.com/delta-io/delta-kernel-rs/pull/203)
- Add `scan_builder` function to `Snapshot` [\#273](https://github.com/delta-io/delta-kernel-rs/pull/273)
- Add `hdfs_native_store` support [\#273](https://github.com/delta-io/delta-kernel-rs/pull/274)
- Proper reading of Parquet files, including only reading requested leaves, type casting, and reordering [\#271](https://github.com/delta-io/delta-kernel-rs/pull/271)
- Allow building the package if you are behind an https proxy [\#282](https://github.com/delta-io/delta-kernel-rs/pull/282)

**Fixed bugs:**

- Don't error if more fields exist than expected in a struct expression [\#267](https://github.com/delta-io/delta-kernel-rs/pull/267)
- Handle cases where the deletion vector length is less than the total number of rows in the chunk [\#276](https://github.com/delta-io/delta-kernel-rs/pull/276)
- Fix partition map indexing if column mapping is in effect [\#278](https://github.com/delta-io/delta-kernel-rs/pull/278)


## [v0.1.1](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0/) (2024-06-03)

[Full Changelog](https://github.com/delta-io/delta-kernel-rs/compare/v0.1.0...v0.1.1)

**Implemented enhancements:**

- Support unary `NOT` and `IsNull` for data skipping [\#231](https://github.com/delta-io/delta-kernel-rs/pull/231)
- Add unary visitors to c ffi [\#247](https://github.com/delta-io/delta-kernel-rs/pull/247)
- Minor other QOL improvements


## [v0.1.0](https://github.com/delta-io/delta-kernel-rs/tree/v0.1.0/) (2024-06-12)

Initial public release
