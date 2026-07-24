//! Integration tests for parsed partition-value output (`partitionValues_parsed`).

use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{col, lit, ColumnName, Predicate, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::PartitionValuesOptions;
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{partitioned, version_latest, FeatureSet, LogState, TableConfig};
use test_utils::{
    add_commit, create_default_engine_mt_executor, get_column,
    install_thread_local_metrics_reporter, test_context, CountingReporter,
};
use url::Url;

/// Requesting the typed struct via `with_struct()` on a partitioned table must emit a
/// `partitionValues_parsed` column with one field per partition column, keyed by physical name.
///
/// Run across every column mapping mode and both log-segment shapes:
/// - `native_checkpoint = false`: no checkpoint, so the struct is synthesized from the
///   `partitionValues` string map via `MAP_TO_STRUCT` on a JSON commit.
/// - `native_checkpoint = true`: a checkpoint with `writeStatsAsStruct=true`, so the read takes the
///   checkpoint's native `partitionValues_parsed` column directly. For the non-null values this
///   table writes, that column matches what `MAP_TO_STRUCT` would reconstruct, so both sources
///   yield the same struct.
///
/// The struct keys on physical names, so a logical-vs-physical mismatch under `Id`/`Name` mapping
/// would surface every partition value as null. The builder writes well-defined (non-null)
/// partition values, so the non-null assertion guards physical-name matching in all combinations.
#[rstest]
fn scan_metadata_emits_partition_values_parsed_across_column_mapping(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Id,
        ColumnMappingMode::Name
    )]
    cm_mode: ColumnMappingMode,
    #[values(false, true)] native_checkpoint: bool,
) {
    let cm_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    // A native `partitionValues_parsed` checkpoint column is only written when
    // `writeStatsAsStruct=true`; otherwise the struct is synthesized from the string map on read.
    let log_state = if native_checkpoint {
        LogState::with_latest_version(1).with_checkpoint_at([1])
    } else {
        LogState::with_latest_version(1)
    };
    let table_config = if native_checkpoint {
        TableConfig::new().write_stats_as_struct(true)
    } else {
        TableConfig::new()
    };
    let (engine, snapshot, _table) = test_context!(
        log_state,
        FeatureSet::empty().column_mapping(cm_str),
        partitioned(),
        table_config,
        version_latest(),
    );

    let schema = snapshot.schema();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()
        .unwrap();

    // Resolve the physical name kernel uses for a partition column under the active mapping mode.
    let physical_name = |logical: &str| -> String {
        get_any_level_column_physical_name(schema.as_ref(), &ColumnName::new([logical]), cm_mode)
            .unwrap()
            .into_inner()
            .into_iter()
            .next()
            .unwrap()
    };

    let scan_metadata_results: Vec<_> = scan
        .scan_metadata(&engine)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        !scan_metadata_results.is_empty(),
        "Should have scan metadata"
    );

    let mut file_count = 0;
    for scan_metadata in scan_metadata_results {
        let (underlying_data, selection_vector) = scan_metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(underlying_data)
            .unwrap()
            .into();
        let filtered_batch =
            filter_record_batch(&batch, &BooleanArray::from(selection_vector)).unwrap();
        if filtered_batch.num_rows() == 0 {
            continue;
        }

        let pv_parsed = get_column!(filtered_batch, "partitionValues_parsed", StructArray);

        // `partitioned()` partitions by all 13 primitive types in `partitioned_schema()`.
        assert_eq!(
            pv_parsed.num_columns(),
            13,
            "expected one parsed field per partition column (cm={cm_str}, native_checkpoint={native_checkpoint})"
        );

        // Every partition value the builder writes is well-defined, so each field must be
        // non-null. A logical-vs-physical name mismatch under column mapping would surface nulls.
        for field in pv_parsed.columns() {
            assert_eq!(
                field.null_count(),
                0,
                "partition value unexpectedly null (cm={cm_str}, native_checkpoint={native_checkpoint})"
            );
        }

        // The struct must be keyed by physical name. Under Id/Name mapping the physical name
        // differs from the logical name, so a logical-keyed struct would fail this lookup.
        for logical in ["part_int", "part_string"] {
            let phys = physical_name(logical);
            assert!(
                pv_parsed.column_by_name(&phys).is_some(),
                "partitionValues_parsed should key {logical} by physical name {phys} \
                 (cm={cm_str}, native_checkpoint={native_checkpoint})"
            );
        }

        file_count += filtered_batch.num_rows();
    }

    assert_eq!(file_count, 1, "Should have processed exactly one file");
}

// === Foreign-writer literal empty-string partition values ===
//
// The kernel never persists a literal "" partition value (it serializes its own empty and null
// partition values to JSON null on write), so these tests stand in a raw-JSON foreign writer that
// did, then assert the kernel reconstructs `partitionValues_parsed` with the empty-string cast: ""
// stays "" for string, becomes empty bytes for binary, and becomes null for every other type.

/// Writes a foreign-writer table under `table_path`: protocol + metadata declaring string, binary,
/// and integer partition columns (with `writeStatsAsStruct` enabled so a checkpoint writes its own
/// `partitionValues_parsed` column), followed by one `add` commit holding `add_actions`.
/// Returns the table URL.
async fn write_foreign_partition_table(
    table_path: &std::path::Path,
    add_actions: &[String],
) -> Url {
    std::fs::create_dir_all(table_path).unwrap();
    let url = Url::from_directory_path(table_path).unwrap();
    let table_root = url.to_string();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());

    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "p_str", "type": "string", "nullable": true, "metadata": {}},
            {"name": "p_bin", "type": "binary", "nullable": true, "metadata": {}},
            {"name": "p_int", "type": "integer", "nullable": true, "metadata": {}},
            {"name": "value", "type": "integer", "nullable": true, "metadata": {}},
        ],
    })
    .to_string();
    let protocol = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    let metadata = serde_json::json!({
        "metaData": {
            "id": "00000000-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": ["p_str", "p_bin", "p_int"],
            "configuration": {"delta.checkpoint.writeStatsAsStruct": "true"},
            "createdTime": 1700000000000_i64,
        },
    })
    .to_string();

    add_commit(
        &table_root,
        store.as_ref(),
        0,
        format!("{protocol}\n{metadata}"),
    )
    .await
    .unwrap();
    add_commit(&table_root, store.as_ref(), 1, add_actions.join("\n"))
        .await
        .unwrap();
    url
}

/// Builds an `add` action whose `partitionValues` map holds the given raw strings.
fn add_action(path: &str, p_str: &str, p_bin: &str, p_int: &str) -> String {
    serde_json::json!({
        "add": {
            "path": path,
            "partitionValues": {"p_str": p_str, "p_bin": p_bin, "p_int": p_int},
            "size": 100,
            "modificationTime": 1700000000000_i64,
            "dataChange": true,
            "stats": "{\"numRecords\":1}",
        },
    })
    .to_string()
}

/// A foreign writer can persist a literal "" in the `partitionValues` map. On read, kernel
/// reconstructs `partitionValues_parsed` with the empty-string cast: "" stays "" for string,
/// becomes empty bytes for binary, and becomes null for every other type.
///
/// The result is identical whether the value is reconstructed from the `partitionValues` map (JSON
/// commit) or read from a kernel-written checkpoint's native `partitionValues_parsed` column: the
/// checkpoint reconstructs that column with the same cast, so a checkpoint never changes the value
/// a scan surfaces. The `native_checkpoint` axis exercises both sources.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parsed_partition_values_read_foreign_empty_string(
    #[values(false, true)] native_checkpoint: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("foreign-empty-string");
    let url = write_foreign_partition_table(
        &table_path,
        &[add_action(
            "p_str=/p_bin=/p_int=/part-0.parquet",
            "",
            "",
            "",
        )],
    )
    .await;
    let engine = create_default_engine_mt_executor(&url).unwrap();

    if native_checkpoint {
        let snapshot = Snapshot::builder_for(url.clone())
            .build(engine.as_ref())
            .unwrap();
        snapshot.checkpoint(engine.as_ref(), None).unwrap();
    }

    // Confirm the scan reads from the intended source: the checkpoint axis must actually place a
    // checkpoint in the snapshot's log segment (and the non-checkpoint axis must not), otherwise a
    // silently-skipped checkpoint would re-test the JSON-commit path twice.
    let reporter = Arc::new(CountingReporter::new());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    assert_eq!(
        reporter.checkpoint_files.get(),
        u64::from(native_checkpoint),
        "log segment checkpoint parts must match native_checkpoint={native_checkpoint}"
    );
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()
        .unwrap();

    let mut asserted_rows = 0;
    for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (data, selection) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        let pv = get_column!(batch, "partitionValues_parsed", StructArray);

        let p_str = pv.column_by_name("p_str").unwrap();
        let p_str = p_str.as_any().downcast_ref::<StringArray>().unwrap();
        let p_bin = pv.column_by_name("p_bin").unwrap();
        let p_bin = p_bin.as_any().downcast_ref::<BinaryArray>().unwrap();
        let p_int = pv.column_by_name("p_int").unwrap();
        let p_int = p_int.as_any().downcast_ref::<Int32Array>().unwrap();

        // Identical whether read from the map (JSON commit) or the checkpoint's native column.
        assert!(!p_str.is_null(0), "string \"\" reconstructs as \"\"");
        assert_eq!(p_str.value(0), "");
        assert!(!p_bin.is_null(0), "binary \"\" reconstructs as empty bytes");
        assert_eq!(p_bin.value(0), b"");
        assert!(p_int.is_null(0), "non-string \"\" must be null");

        asserted_rows += batch.num_rows();
    }
    assert_eq!(
        asserted_rows, 1,
        "expected exactly one file (native_checkpoint={native_checkpoint})"
    );
}

fn collect_path(paths: &mut Vec<String>, scan_file: ScanFile) {
    paths.push(scan_file.path);
}

/// A file whose partition value is a foreign literal "" is a real empty value, not null, so
/// partition skipping treats it accordingly:
/// - `p_str = ''` keeps it and `p_str = 'other'` prunes it.
/// - `p_str IS NULL` prunes it and `p_str IS NOT NULL` keeps it (the value is "", not null).
/// - the same holds for the binary column (`p_bin`), whose "" reconstructs as empty bytes.
///
/// The `native_checkpoint` axis exercises that pruning is identical before and after a kernel
/// checkpoint: the checkpoint reconstructs the same "" into its native `partitionValues_parsed`
/// column, so skipping keeps and prunes the same files a scan of the JSON commit would.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_string_partition_pruning(#[values(false, true)] native_checkpoint: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("empty-string-pruning");
    let url = write_foreign_partition_table(
        &table_path,
        &[
            add_action("p_str=/empty.parquet", "", "", ""),
            add_action("p_str=other/other.parquet", "other", "other", "7"),
        ],
    )
    .await;
    let engine = create_default_engine_mt_executor(&url).unwrap();

    if native_checkpoint {
        let snapshot = Snapshot::builder_for(url.clone())
            .build(engine.as_ref())
            .unwrap();
        snapshot.checkpoint(engine.as_ref(), None).unwrap();
    }

    let surviving = |predicate: Predicate| -> Vec<String> {
        let snapshot = Snapshot::builder_for(url.clone())
            .build(engine.as_ref())
            .unwrap();
        let scan = snapshot
            .scan_builder()
            .with_predicate(Arc::new(predicate))
            .build()
            .unwrap();
        let mut paths = Vec::new();
        for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
            paths = scan_metadata
                .unwrap()
                .visit_scan_files(paths, collect_path)
                .unwrap();
        }
        paths.sort();
        paths
    };

    let empty = "p_str=/empty.parquet".to_string();
    let other = "p_str=other/other.parquet".to_string();
    let both = vec![empty.clone(), other.clone()];

    // The empty-string value is a real "", so equality and null predicates treat it as such.
    assert_eq!(
        surviving(Predicate::eq(col!("p_str"), lit(""))),
        vec![empty.clone()],
        "empty-string file must be kept under p_str = ''"
    );
    assert_eq!(
        surviving(Predicate::eq(col!("p_str"), lit("other"))),
        vec![other.clone()],
        "empty-string file must be pruned under p_str = 'other'"
    );
    // Both partition values are non-null ("" and "other"), so IS NULL prunes both and IS NOT NULL
    // keeps both.
    assert!(
        surviving(Predicate::is_null(col!("p_str"))).is_empty(),
        "no file has a null p_str, so IS NULL prunes both (the empty file's value is \"\", not null)"
    );
    assert_eq!(
        surviving(Predicate::is_not_null(col!("p_str"))),
        both,
        "both files must be kept under p_str IS NOT NULL"
    );

    // The binary column reconstructs "" as empty bytes, pruned the same way.
    assert_eq!(
        surviving(Predicate::eq(col!("p_bin"), lit(b"other".as_slice()))),
        vec![other.clone()],
        "empty-bytes file must be pruned under p_bin = X'6f74686572'"
    );
}

// === Engine-supplied session timezone (Option 6) ===
//
// The engine's `ArrowEvaluationHandler` carries the session zone. When it evaluates the
// `MAP_TO_STRUCT` that reconstructs `partitionValues_parsed`, offset-less TIMESTAMP values resolve
// in that zone. Kernel emits a generic `MAP_TO_STRUCT` and never sees the timezone; a
// `DefaultEngine` built via `DefaultEngineBuilder::with_session_timezone` supplies it. This is the
// end-to-end counterpart to the unit tests in `evaluate_expression`.

/// Writes a `p_ts`-TIMESTAMP-partition foreign table, one `add` per `(path, raw_ts)`, and returns
/// its URL. With `write_stats_as_struct = false` a JSON commit reconstructs
/// `partitionValues_parsed` from the string map on read; with `true` a later kernel checkpoint
/// writes a native `partitionValues_parsed` column, exercising the reparse-vs-passthrough branch.
async fn write_ts_partition_table(
    table_path: &std::path::Path,
    files: &[(&str, &str)],
    write_stats_as_struct: bool,
) -> Url {
    std::fs::create_dir_all(table_path).unwrap();
    let url = Url::from_directory_path(table_path).unwrap();
    let table_root = url.to_string();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());

    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "p_ts", "type": "timestamp", "nullable": true, "metadata": {}},
            {"name": "value", "type": "integer", "nullable": true, "metadata": {}},
        ],
    })
    .to_string();
    let configuration = if write_stats_as_struct {
        serde_json::json!({"delta.checkpoint.writeStatsAsStruct": "true"})
    } else {
        serde_json::json!({})
    };
    let protocol = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    let metadata = serde_json::json!({
        "metaData": {
            "id": "00000000-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": ["p_ts"],
            "configuration": configuration,
            "createdTime": 1700000000000_i64,
        },
    })
    .to_string();
    let adds = files
        .iter()
        .map(|(path, raw_ts)| {
            serde_json::json!({
                "add": {
                    "path": path,
                    "partitionValues": {"p_ts": raw_ts},
                    "size": 100,
                    "modificationTime": 1700000000000_i64,
                    "dataChange": true,
                    "stats": "{\"numRecords\":1}",
                },
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");

    add_commit(
        &table_root,
        store.as_ref(),
        0,
        format!("{protocol}\n{metadata}"),
    )
    .await
    .unwrap();
    add_commit(&table_root, store.as_ref(), 1, adds)
        .await
        .unwrap();
    url
}

/// Builds a multi-threaded `DefaultEngine` over `url`, optionally carrying a session zone.
/// `session_tz = None` resolves offset-less TIMESTAMP partition values at UTC.
fn zoned_engine(
    url: &Url,
    session_tz: Option<&str>,
) -> test_utils::delta_kernel_default_engine::DefaultEngine<
    test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor,
> {
    let store = test_utils::delta_kernel_default_engine::storage::store_from_url(url).unwrap();
    let task_executor = Arc::new(
        test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ),
    );
    let mut builder = DefaultEngineBuilder::new(store).with_task_executor(task_executor);
    if let Some(tz) = session_tz {
        builder = builder.with_session_timezone(tz).unwrap();
    }
    builder.build()
}

/// An engine built with a session zone resolves an offset-less TIMESTAMP partition value in that
/// zone; a default (zoneless) engine resolves it at UTC. This exercises the full Option-6 path:
/// `DefaultEngineBuilder::with_session_timezone` -> zoned `ArrowEvaluationHandler` ->
/// `evaluate_map_to_struct`.
#[rstest]
// Offset-less `2024-01-15 16:00:00` under America/New_York (UTC-5 in January) is 21:00Z.
#[case::in_zone(Some("America/New_York"), 1_705_352_400_000_000)]
// A zoneless engine resolves the same string at UTC: 16:00Z.
#[case::no_zone(None, 1_705_334_400_000_000)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_session_timezone_resolves_offset_less_timestamp_partition(
    #[case] session_tz: Option<&str>,
    #[case] expected_micros: i64,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("engine-tz-offset-less");
    let url =
        write_ts_partition_table(&table_path, &[("f.parquet", "2024-01-15 16:00:00")], false).await;
    let engine = zoned_engine(&url, session_tz);

    let snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()
        .unwrap();

    let mut found = false;
    for scan_metadata in scan.scan_metadata(&engine).unwrap() {
        let (data, selection) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        if batch.num_rows() == 0 {
            continue;
        }
        let pv = get_column!(batch, "partitionValues_parsed", StructArray);
        let p_ts = pv
            .column_by_name("p_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            p_ts.value(0),
            expected_micros,
            "offset-less TIMESTAMP must resolve in the engine's session zone"
        );
        found = true;
    }
    assert!(found, "expected one surviving file");
}

/// The engine's session zone actually eliminates files: a partition predicate written as an
/// absolute instant keeps only the file whose offset-less TIMESTAMP resolves (in that zone) to the
/// same instant, and prunes the other. Under UTC the same predicate would keep the other file, so
/// this proves the pruning acts on the zone-resolved value, not a fixed-UTC one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_session_timezone_prunes_offset_less_timestamp_partition() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("engine-tz-pruning");
    // Two offset-less values one hour apart. Under America/New_York (UTC-5) they denote
    // 2024-01-15T21:00:00Z (a.parquet) and 2024-01-15T22:00:00Z (b.parquet).
    let url = write_ts_partition_table(
        &table_path,
        &[
            ("a.parquet", "2024-01-15 16:00:00"),
            ("b.parquet", "2024-01-15 17:00:00"),
        ],
        false,
    )
    .await;
    let engine = zoned_engine(&url, Some("America/New_York"));

    let snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
    // 16:00 in America/New_York is 21:00Z. The predicate written as that instant keeps a.parquet
    // (21:00Z) and prunes b.parquet (22:00Z). Under UTC, a.parquet would resolve to 16:00Z and be
    // pruned instead, so a surviving `a.parquet` proves the zone was applied.
    let target = lit(Scalar::Timestamp(1_705_352_400_000_000)); // 2024-01-15T21:00:00Z
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .with_predicate(Arc::new(Predicate::eq(col!("p_ts"), target)))
        .build()
        .unwrap();

    let mut paths = Vec::new();
    for scan_metadata in scan.scan_metadata(&engine).unwrap() {
        paths = scan_metadata
            .unwrap()
            .visit_scan_files(paths, collect_path)
            .unwrap();
    }
    assert_eq!(
        paths,
        vec!["a.parquet".to_string()],
        "only the file whose session-zone instant equals the predicate survives"
    );
}

/// A native checkpoint's frozen `partitionValues_parsed` for a TIMESTAMP partition column is always
/// bypassed and reparsed from the raw map, even with NO session zone. A foreign writer froze
/// `2024-01-15 16:00:00` at some non-UTC instant, but a zoneless kernel read must reconstruct it
/// from the raw string at UTC (16:00Z), proving the frozen column was not trusted. This is the
/// commit-vs-checkpoint consistency guarantee: the same table reads identically before and after a
/// checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_checkpoint_timestamp_partition_always_reparses_without_zone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let table_path = temp_dir.path().join("forced-reparse-no-zone");
    // `writeStatsAsStruct=true` so the kernel checkpoint writes a native `partitionValues_parsed`
    // column, exercising the reparse-vs-passthrough branch.
    let url =
        write_ts_partition_table(&table_path, &[("f.parquet", "2024-01-15 16:00:00")], true).await;
    let engine = create_default_engine_mt_executor(&url).unwrap();

    // Force a checkpoint so the read sees a native frozen column, not a JSON commit.
    Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap()
        .checkpoint(engine.as_ref(), None)
        .unwrap();

    // Zoneless engine (no session timezone). The frozen column must still be bypassed and the raw
    // map reparsed at UTC: "2024-01-15 16:00:00" -> 16:00Z.
    let utc_micros = 1_705_334_400_000_000_i64;
    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()
        .unwrap();

    let mut found = false;
    for scan_metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (data, selection) = scan_metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        if batch.num_rows() == 0 {
            continue;
        }
        let pv = get_column!(batch, "partitionValues_parsed", StructArray);
        let p_ts = pv
            .column_by_name("p_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            p_ts.value(0),
            utc_micros,
            "TIMESTAMP partition must reparse from the raw map even with no session zone"
        );
        found = true;
    }
    assert!(found, "expected one surviving file");
}
