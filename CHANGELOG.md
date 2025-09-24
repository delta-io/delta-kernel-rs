# Changelog

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