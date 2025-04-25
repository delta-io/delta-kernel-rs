# Changelog

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