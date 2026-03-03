# Test Tables Reference

Test tables organized by feature area. Tables live in two locations:

- **`data/`** -- Directory-based tables and `.tar.zst` archives
- **`golden_data/`** -- `.tar.zst` archives loaded by `golden_test!`, `negative_test!`, or `skip_test!` macros in `golden_tables.rs`

## Deletion Vectors

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `table-with-dv-small` | data/ | `value: int` | v3/v7 | r:`deletionVectors` w:`deletionVectors` | 10 rows, 2 soft-deleted by DV, 8 visible. Most heavily referenced test table. | `dv.rs::test_table_scan(with_dv)`, `write.rs::test_remove_files_adds_expected_entries`, `write.rs::test_update_deletion_vectors_adds_expected_entries`, `read.rs::with_predicate_and_removes`, `path.rs::test_to_uri/test_child/test_child_escapes`, `snapshot.rs::test_snapshot_read_metadata/test_new_snapshot/test_snapshot_new_from/test_read_table_with_missing_last_checkpoint/test_log_compaction_writer`, `deletion_vector.rs` tests, `transaction/mod.rs::setup_dv_enabled_table/test_add_files_schema/test_new_deletion_vector_path`, `default/parquet.rs` read test, `default/json.rs` read test, `log_compaction/tests.rs::create_mock_snapshot`, `resolve_dvs.rs` tests |
| `table-without-dv-small` | data/ | `value: long` | v1/v2 | | 10 rows, all visible. Companion to table-with-dv-small. | `dv.rs::test_table_scan(without_dv)`, `transaction/mod.rs::setup_non_dv_table/create_existing_table_txn/test_commit_io_error_returns_retryable_transaction`, `sequential_phase.rs::test_sequential_v2_with_commits_only/test_sequential_finish_before_exhaustion_error`, `parallel_phase.rs` tests, `scan/tests.rs::test_scan_metadata_paths/test_scan_metadata/test_scan_metadata_from_same_version` |
| `with-short-dv` | data/ | `id: long, value: string, timestamp: timestamp, rand: double` | v3/v7 | r:`deletionVectors` w:`deletionVectors` | 2 files x 5 rows. First file has inline DV (`storageType="u"`) deleting 3 rows. | `read.rs::short_dv` |
| `dv-partitioned-with-checkpoint` | golden_data/ | `value: int, part: int` partitioned by `part` | v3/v7 | r:`deletionVectors` w:`deletionVectors` | DVs on a partitioned table with a checkpoint | `golden_tables.rs::golden_test!` |
| `dv-with-columnmapping` | golden_data/ | `value: int` | v3/v7 | r:`deletionVectors,columnMapping` w:`deletionVectors,columnMapping`, `columnMapping.mode=name` | DVs combined with column mapping | `golden_tables.rs::golden_test!` |
| `log-replay-dv-key-cases` | golden_data/ | `value: int` | v3/v7 | r:`deletionVectors` w:`deletionVectors` | Edge cases in DV key handling during log replay | `golden_tables.rs::golden_test!` |

## Change Data Feed (CDF)

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `table-with-cdf` | data/ | `part: int, id: int` | v1/v4 | `enableChangeDataFeed=true` | Fake file paths for CDF validation logic | `table_changes/mod.rs::test_table_changes_errors/test_cdf_start_end_version/test_cdf_no_end_version`, `table_changes/scan.rs::test_table_changes_scan_errors/test_create_cdf_scan_batches_errors` |
| `cdf-table` | data/ .tar.zst | `id: int, name: string, birthday: date` partitioned by `birthday` | v1/v4 | `enableChangeDataFeed=true` | Base CDF table | `cdf.rs::basic_cdf` |
| `cdf-table-simple` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Version range queries and validation | `cdf.rs::simple_cdf_version_ranges/invalid_range_end_before_start/invalid_range_start_after_last_version_of_table` |
| `cdf-table-non-partitioned` | data/ .tar.zst | `id: int, name: string, birthday: date, long_field: long, boolean_field: boolean, double_field: double, smallint_field: short` | v1/v4 | `enableChangeDataFeed=true` | Non-partitioned CDF table | `cdf.rs::cdf_non_partitioned` |
| `cdf-table-partitioned` | data/ .tar.zst | `id: long, text: string, part: long` partitioned by `part` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Partitioned CDF table | `cdf.rs::partition_table` |
| `cdf-table-delete-unconditional` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Unconditional DELETE | `cdf.rs::unconditional_delete` |
| `cdf-table-delete-conditional-all-rows` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Conditional DELETE removing all rows | `cdf.rs::conditional_delete_all_rows` |
| `cdf-table-delete-conditional-two-rows` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Conditional DELETE removing two rows | `cdf.rs::conditional_delete_two_rows` |
| `cdf-table-update-ops` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | UPDATE operations | `cdf.rs::update_operations` |
| `cdf-table-data-change` | data/ .tar.zst | `id: long` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Operations with `dataChange=false` | `cdf.rs::false_data_change_is_ignored` |
| `cdf-table-with-dv` | data/ .tar.zst | `value: int` | v3/v7 | r:`deletionVectors` w:`deletionVectors,changeDataFeed`, `enableChangeDataFeed=true` | CDF with deletion vectors | `cdf.rs::cdf_with_deletion_vector` |
| `cdf-table-with-cdc-and-dvs` | data/ .tar.zst | `id: int, comment: string` | v3/v7 | r:`deletionVectors` w:`deletionVectors,changeDataFeed`, `enableChangeDataFeed=true` | CDF with both CDC files and DVs | `cdf.rs::cdf_with_cdc_and_dvs` |
| `cdf-table-backtick-column-names` | data/ .tar.zst | `` `id.num`: int, `id.num\`s`: int, struct_col: struct{field: int, `field.one`: int} `` | v1/v7 | w:`changeDataFeed,appendOnly,invariants`, `enableChangeDataFeed=true` | Backtick-escaped column names | `cdf.rs::backtick_column_names` |
| `cdf-column-mapping-name-mode` | data/ .tar.zst | `id: long, name: string, value: double` | v2/v5 | Column mapping (name) | CDF + column mapping name mode | `cdf.rs::cdf_with_column_mapping_name_mode` |
| `cdf-column-mapping-name-mode-3-7` | data/ .tar.zst | `id: long, name: string, value: double` | v3/v7 | r:`columnMapping,deletionVectors` w:`columnMapping,deletionVectors` | CDF + column mapping name mode (v3/v7) | `cdf.rs::cdf_with_column_mapping_name_mode` |
| `cdf-column-mapping-id-mode` | data/ .tar.zst | `id: long, name: string, value: double` | v2/v5 | Column mapping (id) | CDF + column mapping id mode | `cdf.rs::cdf_with_column_mapping_id_mode` |

## Column Mapping

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `partition_cm/none` | data/ | `value: int, category: string` partitioned by `category` | v1/v1 | `columnMapping.mode=none` | Partitioned write with CM disabled | `write.rs::test_column_mapping_partitioned_write(cm_none)` |
| `partition_cm/id` | data/ | `value: int, category: string` partitioned by `category` | v3/v7 | r:`columnMapping` w:`columnMapping`, `columnMapping.mode=id` | Partitioned write with CM id mode | `write.rs::test_column_mapping_partitioned_write(cm_id)` |
| `partition_cm/name` | data/ | `value: int, category: string` partitioned by `category` | v3/v7 | r:`columnMapping` w:`columnMapping`, `columnMapping.mode=name` | Partitioned write with CM name mode | `write.rs::test_column_mapping_partitioned_write(cm_name)` |
| `table-with-columnmapping-mode-name` | golden_data/ | `ByteType: byte, ShortType: short, IntegerType: int, LongType: long, FloatType: float, DoubleType: double, decimal: decimal(10,2), BooleanType: boolean, StringType: string, BinaryType: binary, DateType: date, TimestampType: timestamp, nested_struct: struct{aa: string, ac: struct{aca: int}}, array_of_prims: array<int>, array_of_arrays: array<array<int>>, array_of_structs: array<struct{ab: long}>, map_of_prims: map<int,long>, map_of_rows: map<int,struct{ab: long}>, map_of_arrays: map<long,array<int>>` | v2/v5 | `columnMapping.mode=name` | Column mapping name mode | `golden_tables.rs::golden_test!` |
| `table-with-columnmapping-mode-id` | golden_data/ | `ByteType: byte, ShortType: short, IntegerType: int, LongType: long, FloatType: float, DoubleType: double, decimal: decimal(10,2), BooleanType: boolean, StringType: string, BinaryType: binary, DateType: date, TimestampType: timestamp, nested_struct: struct{aa: string, ac: struct{aca: int}}, array_of_prims: array<int>, array_of_arrays: array<array<int>>, array_of_structs: array<struct{ab: long}>, map_of_prims: map<int,long>, map_of_rows: map<int,struct{ab: long}>, map_of_arrays: map<long,array<int>>` | v2/v5 | `columnMapping.mode=id` | Column mapping id mode | `golden_tables.rs::golden_test!` |

## Checkpoints

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `with_checkpoint_no_last_checkpoint` | data/ | `letter: string, int: long, date: date` | v1/v2 | `checkpointInterval=2` | Checkpoint at v2 but missing `_last_checkpoint` hint file | `snapshot.rs::test_read_table_with_checkpoint`, `scan/tests.rs::test_scan_with_checkpoint`, `sequential_phase.rs::test_sequential_checkpoint_no_commits`, `checkpoint_manifest.rs` tests, `sync/parquet.rs` test, `default/parquet.rs` test |
| `external-table-different-nullability` | data/ | `i: int` | v1/v2 | `checkpointInterval=2` | Parquet files have different nullability than Delta schema; includes checkpoint | `write.rs::test_checkpoint_non_kernel_written_table` |
| `checkpoint` | golden_data/ | `intCol: int` | v1/v2 | | Basic checkpoint read | `golden_tables.rs::golden_test!(checkpoint_test)` |
| `corrupted-last-checkpoint-kernel` | golden_data/ | `id: long` | v1/v2 | | Corrupted `_last_checkpoint` file | `golden_tables.rs::golden_test!` |
| `multi-part-checkpoint` | golden_data/ | `id: long` | v1/v2 | `checkpointInterval=1` | Multi-part checkpoint files | `golden_tables.rs::golden_test!` |
| `only-checkpoint-files` | golden_data/ | `id: long` | v1/v2 | `checkpointInterval=1` | Table with only checkpoint files, no JSON commits | `golden_tables.rs::golden_test!` |

## V2 Checkpoints

Tests V2 checkpoints across format, sidecar, and `_last_checkpoint` combinations.

All `data/` V2 checkpoint tables share: schema=`id: long`, protocol v3/v7, r:`v2Checkpoint` w:`v2Checkpoint,appendOnly,invariants`, `checkpointPolicy=v2`.

| Table | Location | Format | Sidecars | `_last_checkpoint` | Tests |
|-------|----------|--------|----------|--------------------|-------|
| `v2-checkpoints-json-with-sidecars` | data/ .tar.zst | JSON | Yes | No | `v2_checkpoints.rs::v2_checkpoints_json_with_sidecars`, `checkpoint_manifest.rs` test, `sequential_phase.rs::test_sequential_v2_with_sidecars`, `parallel_phase.rs` tests |
| `v2-checkpoints-json-without-sidecars` | data/ .tar.zst | JSON | No | No | `v2_checkpoints.rs::v2_checkpoints_json_without_sidecars`, `sequential_phase.rs::test_sequential_checkpoint_without_sidecars` |
| `v2-checkpoints-json-with-last-checkpoint` | data/ .tar.zst | JSON | Yes | Yes | `v2_checkpoints.rs::v2_checkpoints_json_with_last_checkpoint` |
| `v2-checkpoints-parquet-with-sidecars` | data/ .tar.zst | Parquet | Yes | No | `v2_checkpoints.rs::v2_checkpoints_parquet_with_sidecars`, `checkpoint_manifest.rs` test, `sequential_phase.rs::test_sequential_parquet_checkpoint_with_sidecars`, `parallel_phase.rs` test |
| `v2-checkpoints-parquet-without-sidecars` | data/ .tar.zst | Parquet | No | No | `v2_checkpoints.rs::v2_checkpoints_parquet_without_sidecars` |
| `v2-checkpoints-parquet-with-last-checkpoint` | data/ .tar.zst | Parquet | Yes | Yes | `v2_checkpoints.rs::v2_checkpoints_parquet_with_last_checkpoint` |
| `v2-classic-checkpoint-json` | data/ .tar.zst | JSON | Classic compat | No | `v2_checkpoints.rs::v2_classic_checkpoint_json` |
| `v2-classic-checkpoint-parquet` | data/ .tar.zst | Parquet | Classic compat | No | `v2_checkpoints.rs::v2_classic_checkpoint_parquet` |

The golden V2 checkpoint tables have a different protocol (v1/v2, no table features):

| Table | Location | Schema | Protocol | Features | Tests |
|-------|----------|--------|----------|----------|-------|
| `v2-checkpoint-json` | golden_data/ | `id: long` | v1/v2 | `checkpointInterval=2` | `golden_tables.rs::golden_test!` |
| `v2-checkpoint-parquet` | golden_data/ | `id: long` | v1/v2 | `checkpointInterval=2` | `golden_tables.rs::golden_test!` |

## Data Skipping & Statistics

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `parsed-stats` | data/ | `id: long, name: string, age: long, salary: long` | v1/v2 | `checkpointInterval=3` | 100 rows. Checkpoint at v3 with `stats_parsed` columns. | `scan/tests.rs::test_scan_metadata_with_stats_columns/test_data_skipping_with_parsed_stats/test_scan_metadata_stats_columns_with_predicate/test_prefix_columns_simple/test_build_actions_meta_predicate_with_predicate/test_build_actions_meta_predicate_no_predicate/test_build_actions_meta_predicate_static_skip_all/test_skip_stats_disables_data_skipping/test_skip_stats_and_include_stats_columns_errors` |
| `parquet_row_group_skipping` | data/ | `bool, chrono{date32,timestamp,timestamp_ntz}, numeric{decimals{...},floats{...},ints{...}}, varlen{binary,utf8}` | v3/v7 | r:`timestampNtz` w:`timestampNtz`, `dataSkippingNumIndexedCols=0` | Deep nested structs, all types. Delta stats disabled; tests parquet-level row group skipping. | `read.rs::predicate_references_invalid_missing_column`, `scan/tests.rs::test_replay_for_scan_metadata/test_data_row_group_skipping/test_missing_column_row_group_skipping`, `set_transaction.rs::test_replay_for_app_ids`, `parquet_row_group_skipping/tests.rs`, `protocol_metadata_replay.rs` |
| `mixed-nulls` | data/ | `id: long, part: long, value: string, n: string` partitioned by `part` | v1/v2 | | part=0: `n` all null. part=1: no nulls. part=2: mixed nulls. | `read.rs::mixed_null/mixed_not_null` |
| `stats-writing-all-types` | data/ (under `delta/`) | 16 columns: all primitives + array, map, nested struct | v3/v7 | r:`timestampNtz,columnMapping` w:`timestampNtz,columnMapping`, `columnMapping.mode=name` | Verifies stats collection matches Spark output | `default/stats.rs::test_collect_stats_matches_spark` |
| `data-skipping-basic-stats-all-types` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_float: float, as_double: double, as_string: string, as_date: date, as_timestamp: timestamp, as_big_decimal: decimal(1,0)` | v1/v2 | | Basic stats for all types | `golden_tables.rs::golden_test!` |
| `data-skipping-basic-stats-all-types-checkpoint` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_float: float, as_double: double, as_string: string, as_date: date, as_timestamp: timestamp, as_big_decimal: decimal(1,0)` | v1/v2 | `checkpointInterval=1` | Stats from checkpoint | `golden_tables.rs::golden_test!` |
| `data-skipping-basic-stats-all-types-columnmapping-id` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_float: float, as_double: double, as_string: string, as_date: date, as_timestamp: timestamp, as_big_decimal: decimal(1,0)` | v2/v5 | `columnMapping.mode=id` | Stats with CM id mode (not supported) | `golden_tables.rs::skip_test!` |
| `data-skipping-basic-stats-all-types-columnmapping-name` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_float: float, as_double: double, as_string: string, as_date: date, as_timestamp: timestamp, as_big_decimal: decimal(1,0)` | v2/v5 | `columnMapping.mode=name` | Stats with CM name mode | `golden_tables.rs::golden_test!` |
| `data-skipping-change-stats-collected-across-versions` | golden_data/ | `col1: int, col2: int` | v1/v2 | | Stats that change across versions | `golden_tables.rs::golden_test!` |
| `data-skipping-partition-and-data-column` | golden_data/ | `part: int, id: int` | v1/v2 | | Skipping with partition + data predicates | `golden_tables.rs::golden_test!` |

## Type Handling

### Decimal

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `basic-decimal-table` | data/ | `part: decimal(12,5), col1: decimal(5,2), col2: decimal(10,5), col3: decimal(20,10)` partitioned by `part` | v1/v2 | Decimal partitions with negative, small, and large values | `read.rs::basic_decimal`, `golden_tables.rs::golden_basic_decimal_table` |
| `124-decimal-decode-bug` | golden_data/ | `large_decimal: decimal(10,0)` | v1/v2 | Regression for decimal decode bug #124 | `golden_tables.rs::golden_test!` |
| `125-iterator-bug` | golden_data/ | `col1: int` | v1/v2 | Regression for iterator bug #125 | `golden_tables.rs::golden_test!` |
| `basic-decimal-table-legacy` | golden_data/ | `part: decimal(12,5), col1: decimal(5,2), col2: decimal(10,5), col3: decimal(20,10)` partitioned by `part` | v1/v2 | Decimal types in legacy parquet format | `golden_tables.rs::golden_test!` |
| `decimal-various-scale-precision` | golden_data/ | 29 decimal columns with varying precisions (4,0 through 38,36) | v1/v2 | Various scale/precision combinations | `golden_tables.rs::golden_test!` |
| `parquet-decimal-dictionaries` | golden_data/ | `id: int, col1: decimal(9,0), col2: decimal(12,0), col3: decimal(25,0)` | v1/v2 | Dictionary-encoded decimals | `golden_tables.rs::golden_test!` |
| `parquet-decimal-dictionaries-v1` | golden_data/ | `id: int, col1: decimal(9,0), col2: decimal(12,0), col3: decimal(25,0)` | v1/v2 | Dictionary-encoded decimals (parquet v1) | `golden_tables.rs::golden_test!` |
| `parquet-decimal-dictionaries-v2` | golden_data/ | `id: int, col1: decimal(9,0), col2: decimal(12,0), col3: decimal(25,0)` | v1/v2 | Dictionary-encoded decimals (parquet v2) | `golden_tables.rs::golden_test!` |
| `parquet-decimal-type` | golden_data/ | `id: int, col1: decimal(5,1), col2: decimal(10,5), col3: decimal(20,5)` | v1/v2 | Parquet decimal type handling | `golden_tables.rs::golden_test!` |

### Timestamps

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `data-reader-timestamp_ntz` | data/ | `id: int, tsNtz: timestamp_ntz, tsNtzPartition: timestamp_ntz` partitioned by `tsNtzPartition` | v3/v7, r:`timestampNtz` | Timestamp without timezone as data + partition column | `read.rs::timestamp_ntz`, `golden_tables.rs::golden_data_reader_timestamp_ntz` |
| `timestamp-partitioned-table` | data/ .tar.zst | `id: long, x: double, s: string, time: timestamp` partitioned by `time` | v3/v7 | r:`deletionVectors` w:`deletionVectors,invariants,appendOnly`, `enableDeletionVectors=true` | Table partitioned by timestamp column | `read.rs::timestamp_partitioned_table` |
| `kernel-timestamp-int96` | golden_data/ | `id: long, tsNtz: timestamp` | v1/v2 | Timestamps stored as INT96 | `golden_tables.rs::golden_test!` |
| `kernel-timestamp-pst` | golden_data/ | `id: long, tsNtz: timestamp` | v1/v2 | Timestamps in PST timezone | `golden_tables.rs::golden_test!` |
| `kernel-timestamp-timestamp_micros` | golden_data/ | `id: long, tsNtz: timestamp` | v1/v2 | TIMESTAMP_MICROS encoding | `golden_tables.rs::golden_test!` |
| `kernel-timestamp-timestamp_millis` | golden_data/ | `id: long, tsNtz: timestamp` | v1/v2 | TIMESTAMP_MILLIS encoding | `golden_tables.rs::golden_test!` |
| `data-reader-timestamp_ntz-id-mode` | golden_data/ | `id: int, tsNtz: timestamp_ntz, tsNtzPartition: timestamp_ntz` partitioned by `tsNtzPartition` | v3/v7 | r:`timestampNtz,columnMapping` w:`timestampNtz,columnMapping`, `columnMapping.mode=id` | Timestamp NTZ with CM id mode | `golden_tables.rs::golden_test!` |
| `data-reader-timestamp_ntz-name-mode` | golden_data/ | `id: int, tsNtz: timestamp_ntz, tsNtzPartition: timestamp_ntz` partitioned by `tsNtzPartition` | v3/v7 | r:`timestampNtz,columnMapping` w:`timestampNtz,columnMapping`, `columnMapping.mode=name` | Timestamp NTZ with CM name mode | `golden_tables.rs::golden_test!` |

### Type Widening & Variant

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `type-widening` | data/ | 13 columns named by widening path (e.g. `byte_long`, `float_double`, `date_timestamp_ntz`) | v1/v2 (at v0) | Commit 0 = narrow types, commit 1 = widened | `read.rs::type_widening_basic/type_widening_decimal` |
| `unshredded-variant` | data/ .tar.zst | `id: long, v: variant, array_of_variants: array<variant>, struct_of_variants: struct{v: variant}, map_of_variants: map<string,variant>, array_of_struct_of_variants: array<struct{v: variant}>, struct_of_array_of_variants: struct{v: array<variant>}` | v3/v7 | r:`variantType-preview` w:`variantType-preview,appendOnly,invariants`, `checkpointInterval=2` | Unshredded variant type column | `read.rs::unshredded_variant_table` |

## Application Transactions

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `app-txn-checkpoint` | data/ | `id: string, value: int, modified: string` partitioned by `modified` | v1/v2 | | `txn` for `appId="my-app"`. Two partitions. Has checkpoint. | `hdfs.rs::read_table_version_hdfs`, `set_transaction.rs::test_txn` |
| `app-txn-no-checkpoint` | data/ | `id: string, value: int, modified: string` partitioned by `modified` | v1/v2 | | Same as above but no checkpoint | `commit.rs::test_commit_phase_processes_commits`, `checkpoint/mod.rs` (doc example), `set_transaction.rs::test_txn` |
| `app-txn-with-last-updated` | data/ | `a: long, b: long` | v1/v2 | `setTransactionRetentionDuration="interval 1 days"` | `txn` with `lastUpdated` timestamps and retention config | `set_transaction.rs::test_txn_retention_filtering` |

## CRC (Checksum) Files

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `crc-full` | data/ | `id: long` | v3/v7 | r:`deletionVectors` w:`domainMetadata,deletionVectors,rowTracking` | Full CRC with `allFiles` (10 files), 2 `setTransactions`, `domainMetadata`, `fileSizeHistogram` | `crc/reader.rs::test_read_crc`, `crc/lazy.rs::test_lazy_crc` |
| `crc-malformed` | data/ | N/A | N/A | | CRC file contains only `"malformed"` | `crc/lazy.rs::test_lazy_crc_malformed_file`, `crc/reader.rs` error test |

## Partitioning & Write Path

| Table | Location | Schema | Protocol | Features | Description | Tests |
|-------|----------|--------|----------|----------|-------------|-------|
| `basic_partitioned` | data/ | `letter: string, number: long, a_float: double` partitioned by `letter` | v1/v2 | | Partitions: a, b, c, e, `__HIVE_DEFAULT_PARTITION__`. Second most referenced table. | `read.rs::data/column_ordering/column_ordering_and_projection/predicate_on_number/predicate_on_letter/predicate_on_letter_and_number/predicate_on_number_not/predicate_on_number_with_not_null/predicate_null/and_or_predicates/not_and_or_predicates/invalid_skips_none_predicates/predicate_references_invalid_missing_column`, `benchmarks/runners.rs`, `snapshot.rs::test_new_post_commit_simple`, `log_compaction/tests.rs::create_multi_version_snapshot`, `transaction/mod.rs::test_physical_schema_excludes_partition_columns/test_materialize_partition_columns_in_write_context/test_partition_column_in_eval_output`, `set_transaction.rs::test_txn`, `scan/tests.rs::test_scan_metadata_from_with_update` |
| `partitioned_with_materialize_feature` | data/ | `letter: string, number: long, a_float: double` partitioned by `letter` | v3/v7 | w:`materializePartitionColumns` | Same data as basic_partitioned but partition columns materialized in write output | `transaction/mod.rs::test_materialize_partition_columns_in_write_context/test_physical_schema_includes_partition_columns_when_materialized/test_partition_column_in_eval_output` |

## Log Replay & State Reconstruction

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `compacted-log-files-table` | data/ .tar.zst | `id: int, comment: string` | v3/v7, r:`deletionVectors` w:`deletionVectors,changeDataFeed` | Table with compacted (merged) log files | `read.rs::compacted_log_files_table` |
| `log-replay-latest-metadata-protocol` | golden_data/ | `id: int, val: string` | v1/v2 (at v0, changes later) | Log replay picks latest metadata/protocol | `golden_tables.rs::golden_test!` |
| `log-replay-special-characters` | golden_data/ | `id: int` | v1/v2 | File paths with special characters | `golden_tables.rs::golden_test!` |
| `log-replay-special-characters-a` | golden_data/ | `id: int` | v1/v2 | Special characters variant (a) | `golden_tables.rs::golden_test!` |
| `log-replay-special-characters-b` | golden_data/ | `id: int` | v1/v2 | Special characters variant (b) | `golden_tables.rs::skip_test!` (not yet implemented) |
| `deltalog-getChanges` | golden_data/ | `part: int, id: int` | v1/v2 | Reading changes from delta log | `golden_tables.rs::golden_test!` |
| `deltalog-invalid-protocol-version` | golden_data/ | `id: int` | v99/v7 | Invalid protocol version | `golden_tables.rs::negative_test!` |
| `deltalog-state-reconstruction-from-checkpoint-missing-metadata` | golden_data/ | N/A | v1/v2 | Checkpoint missing metadata action | `golden_tables.rs::negative_test!` |
| `deltalog-state-reconstruction-from-checkpoint-missing-protocol` | golden_data/ | N/A | v1/v2 | Checkpoint missing protocol action | `golden_tables.rs::negative_test!` |
| `deltalog-state-reconstruction-without-metadata` | golden_data/ | N/A | v3/v7 | Log without metadata action | `golden_tables.rs::negative_test!` |
| `deltalog-state-reconstruction-without-protocol` | golden_data/ | `intCol: int` | N/A | Log without protocol action | `golden_tables.rs::negative_test!` |
| `no-delta-log-folder` | golden_data/ | N/A | N/A | Missing `_delta_log` directory | `golden_tables.rs::negative_test!` |
| `versions-not-contiguous` | golden_data/ | N/A | v1/v2 | Non-contiguous version numbers | `golden_tables.rs::negative_test!` |

## Snapshots & Time Travel

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `snapshot-data0` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot at initial version | `golden_tables.rs::golden_test!` |
| `snapshot-data1` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after first insert | `golden_tables.rs::golden_test!` |
| `snapshot-data2` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after second insert | `golden_tables.rs::golden_test!` |
| `snapshot-data2-deleted` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after delete operation | `golden_tables.rs::golden_test!` |
| `snapshot-data3` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after third insert | `golden_tables.rs::golden_test!` |
| `snapshot-repartitioned` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after repartitioning | `golden_tables.rs::golden_test!` |
| `snapshot-vacuumed` | golden_data/ | `col1: int, col2: string` | v1/v2 | Snapshot after vacuum | `golden_tables.rs::golden_test!` |
| `time-travel-start` | golden_data/ | `id: long` | v1/v2 | Time travel to initial version | `golden_tables.rs::golden_test!` |
| `time-travel-start-start20` | golden_data/ | `id: long` | v1/v2 | Time travel across 20 versions | `golden_tables.rs::golden_test!` |
| `time-travel-start-start20-start40` | golden_data/ | `id: long` | v1/v2 | Time travel across 40 versions | `golden_tables.rs::golden_test!` |
| `time-travel-partition-changes-a` | golden_data/ | `id: long, part5: long` partitioned by `part5` | v1/v2 | Time travel with partition changes (a) | `golden_tables.rs::golden_test!` |
| `time-travel-partition-changes-b` | golden_data/ | `id: long, part5: long` partitioned by `part5` | v1/v2 | Time travel with partition changes (b) | `golden_tables.rs::golden_test!` |
| `time-travel-schema-changes-a` | golden_data/ | `id: long` | v1/v2 | Time travel with schema changes (a) | `golden_tables.rs::golden_test!` |
| `time-travel-schema-changes-b` | golden_data/ | `id: long` | v1/v2 | Time travel with schema changes (b) | `golden_tables.rs::golden_test!` |

## Data Readers (Golden)

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `data-reader-array-complex-objects` | golden_data/ | `i: int, 3d_int_list: array<array<array<int>>>, 4d_int_list: array<array<array<array<int>>>>, list_of_maps: array<map<string,long>>, list_of_records: array<struct{val: int}>` | v1/v2 | Arrays of complex objects | `golden_tables.rs::golden_test!` |
| `data-reader-array-primitives` | golden_data/ | `as_array_int: array<int>, as_array_long: array<long>, as_array_byte: array<byte>, as_array_short: array<short>, as_array_boolean: array<boolean>, as_array_float: array<float>, as_array_double: array<double>, as_array_string: array<string>, as_array_binary: array<binary>, as_array_big_decimal: array<decimal(1,0)>` | v1/v2 | Arrays of primitive types | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-America` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in America timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-Asia` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in Asia timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-Etc` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in Etc timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-Iceland` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in Iceland timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-Jst` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in JST timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-Pst` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in PST timezone | `golden_tables.rs::golden_test!` |
| `data-reader-date-types-utc` | golden_data/ | `timestamp: timestamp, date: date` | v1/v2 | Date types in UTC timezone | `golden_tables.rs::golden_test!` |
| `data-reader-escaped-chars` | golden_data/ | `_1: string, _2: string` partitioned by `_2` | v1/v2 | File paths with escaped characters | `golden_tables.rs::golden_test!` |
| `data-reader-map` | golden_data/ | `i: int, a: map<int,int>, b: map<long,byte>, c: map<short,boolean>, d: map<float,double>, e: map<string,decimal(1,0)>, f: map<int,array<struct{val: int}>>` | v1/v2 | Map type columns | `golden_tables.rs::golden_test!` |
| `data-reader-nested-struct` | golden_data/ | `a: struct{aa: string, ab: string, ac: struct{aca: int, acb: long}}, b: int` | v1/v2 | Nested struct columns | `golden_tables.rs::golden_test!` |
| `data-reader-nullable-field-invalid-schema-key` | golden_data/ | `array_can_contain_null: array<string>` | v1/v2 | Nullable fields with invalid schema keys | `golden_tables.rs::golden_test!` |
| `data-reader-partition-values` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_boolean: boolean, as_float: float, as_double: double, as_string: string, as_string_lit_null: string, as_date: date, as_timestamp: timestamp, as_big_decimal: decimal(1,0), as_list_of_records: array<struct{val: int}>, as_nested_struct: struct{aa: string, ab: string, ac: struct{aca: int, acb: long}}, value: string` partitioned by first 12 columns | v1/v2 | Partition values | `golden_tables.rs::skip_test!` (needs timestamp fix) |
| `data-reader-primitives` | golden_data/ | `as_int: int, as_long: long, as_byte: byte, as_short: short, as_boolean: boolean, as_float: float, as_double: double, as_string: string, as_binary: binary, as_big_decimal: decimal(1,0)` | v1/v2 | All primitive types | `golden_tables.rs::golden_test!` |
| `parquet-all-types` | golden_data/ | `ByteType: byte, ShortType: short, IntegerType: int, LongType: long, FloatType: float, DoubleType: double, decimal: decimal(10,2), BooleanType: boolean, StringType: string, BinaryType: binary, DateType: date, TimestampType: timestamp, TimestampNTZType: timestamp_ntz, nested_struct: struct{aa: string, ac: struct{aca: int}}, array_of_prims: array<int>, array_of_arrays: array<array<int>>, array_of_structs: array<struct{ab: long}>, map_of_prims: map<int,long>, map_of_rows: map<int,struct{ab: long}>, map_of_arrays: map<long,array<int>>` | v3/v7, r:`timestampNtz` w:`timestampNtz` | All parquet types | `golden_tables.rs::skip_test!` (nullability disagreement) |
| `parquet-all-types-legacy-format` | golden_data/ | `ByteType: byte, ShortType: short, IntegerType: int, LongType: long, FloatType: float, DoubleType: double, decimal: decimal(10,2), BooleanType: boolean, StringType: string, BinaryType: binary, DateType: date, TimestampType: timestamp, TimestampNTZType: timestamp_ntz, nested_struct: struct{aa: string, ac: struct{aca: int}}, array_of_prims: array<int>, array_of_arrays: array<array<int>>, array_of_structs: array<struct{ab: long}>, map_of_prims: map<int,long>, map_of_rows: map<int,struct{ab: long}>, map_of_arrays: map<long,array<int>>` | v3/v7, r:`timestampNtz` w:`timestampNtz` | Legacy parquet format | `golden_tables.rs::skip_test!` (legacy name issue) |

## Insert / Update / Delete Operations (Golden)

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `basic-with-inserts-deletes-checkpoint` | golden_data/ | `id: long` | v1/v2 | Inserts and deletes with checkpoint | `golden_tables.rs::golden_test!` |
| `basic-with-inserts-merge` | golden_data/ | `id: int, str: string` | v1/v2 | Inserts via MERGE operation | `golden_tables.rs::golden_test!` |
| `basic-with-inserts-overwrite-restore` | golden_data/ | `id: long` | v1/v2 | Inserts with overwrite and RESTORE | `golden_tables.rs::golden_test!` |
| `basic-with-inserts-updates` | golden_data/ | `id: int, str: string` | v1/v2 | Inserts and updates | `golden_tables.rs::golden_test!` |
| `basic-with-vacuum-protocol-check-feature` | golden_data/ | `id: int, str: string` | v1/v2 | Table with vacuum protocol check feature | `golden_tables.rs::golden_test!` |

## Benchmarks

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `300k-add-files-100-col-partitioned` | data/ .tar.zst | `partition_col: string, col_0..col_98: long` (100 data columns) partitioned by `partition_col` | v1/v2 | 300k add actions, 100 partition columns | `kernel/benches/metadata_bench.rs` |

## Miscellaneous / Edge Cases

| Table | Location | Schema | Protocol | Description | Tests |
|-------|----------|--------|----------|-------------|-------|
| `hive` | golden_data/ | N/A | N/A | Hive-format table | `golden_tables.rs::skip_test!` (not yet implemented) |
| `canonicalized-paths-normal-a` | golden_data/ | `id: int` | v1/v2 | Path canonicalization | `golden_tables.rs::skip_test!` (canonicalization bug) |
| `canonicalized-paths-normal-b` | golden_data/ | `id: int` | v1/v2 | Path canonicalization | `golden_tables.rs::skip_test!` (canonicalization bug) |
| `canonicalized-paths-special-a` | golden_data/ | `id: int` | v1/v2 | Path canonicalization with special chars | `golden_tables.rs::skip_test!` (canonicalization bug) |
| `canonicalized-paths-special-b` | golden_data/ | `id: int` | v1/v2 | Path canonicalization with special chars | `golden_tables.rs::skip_test!` (canonicalization bug) |
| `delete-re-add-same-file-different-transactions` | golden_data/ | `intCol: int` | v1/v2 | Delete and re-add same file | `golden_tables.rs::skip_test!` (not yet implemented) |

## Unreferenced Tables

| Table | Location | Schema | Protocol |
|-------|----------|--------|----------|
| `data-reader-absolute-paths-escaped-chars` | golden_data/ | N/A (no metadata in commit) | N/A |
| `deltalog-commit-info` | golden_data/ | N/A | v1/v2 |
| `update-deleted-directory` | golden_data/ | N/A | v1/v2 |
