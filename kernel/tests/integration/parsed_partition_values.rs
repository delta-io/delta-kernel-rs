//! Integration tests for parsed partition-value output (`partitionValues_parsed`).

use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{col, lit, ColumnName, Predicate};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::arrow::ArrowWriter;
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

async fn write_timezone_partition_table(table_path: &std::path::Path, timestamp: &str) -> Url {
    std::fs::create_dir_all(table_path).unwrap();
    let values: ArrayRef = Arc::new(Int32Array::from(vec![42]));
    let batch = RecordBatch::try_from_iter([("value", values)]).unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(table_path.join("part.parquet")).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let file_size = std::fs::metadata(table_path.join("part.parquet"))
        .unwrap()
        .len();
    let url = Url::from_directory_path(table_path).unwrap();
    let table_root = url.to_string();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    let schema_string = serde_json::json!({
        "type": "struct",
        "fields": [
            {"name": "p_ts", "type": "timestamp", "nullable": true, "metadata": {}},
            {"name": "p_ntz", "type": "timestamp_ntz", "nullable": true, "metadata": {}},
            {"name": "p_int", "type": "integer", "nullable": true, "metadata": {}},
            {"name": "value", "type": "integer", "nullable": true, "metadata": {}},
        ],
    })
    .to_string();
    let protocol = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;
    let metadata = serde_json::json!({
        "metaData": {
            "id": "00000000-0000-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": ["p_ts", "p_ntz", "p_int"],
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
    let add = serde_json::json!({
        "add": {
            "path": "part.parquet",
            "partitionValues": {
                "p_ts": timestamp,
                "p_ntz": "2024-01-15 12:30:45.123456",
                "p_int": "7",
            },
            "size": file_size,
            "modificationTime": 1700000000000_i64,
            "dataChange": true,
            "stats": "{\"numRecords\":1}",
        },
    });
    add_commit(&table_root, store.as_ref(), 1, add.to_string())
        .await
        .unwrap();
    url
}

#[rstest]
#[case::utc(None, "2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
#[case::los_angeles_winter(
    Some("America/Los_Angeles"),
    "2024-01-15 12:30:45.123456",
    "2024-01-15T20:30:45.123456Z"
)]
#[case::los_angeles_summer(
    Some("America/Los_Angeles"),
    "2024-06-15 08:00:00.500500",
    "2024-06-15T15:00:00.500500Z"
)]
#[case::fixed_offset_seconds(
    Some("+12:45:30"),
    "2024-01-15 12:30:45.123456",
    "2024-01-14T23:45:15.123456Z"
)]
#[case::explicit_offset_wins(
    Some("America/Los_Angeles"),
    "2024-01-15 12:30:45+02:00",
    "2024-01-15T10:30:45Z"
)]
#[case::dst_overlap_uses_earlier_instant(
    Some("America/Los_Angeles"),
    "2024-11-03 01:30:00",
    "2024-11-03T08:30:00Z"
)]
#[case::dst_gap_uses_pre_transition_offset(
    Some("America/Los_Angeles"),
    "2024-03-10 02:30:00",
    "2024-03-10T10:30:00Z"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timezone_aware_parsed_partition_values_across_json_and_native_checkpoint(
    #[case] timestamp_timezone: Option<&str>,
    #[case] raw_timestamp: &str,
    #[case] expected_timestamp: &str,
    #[values(false, true)] native_checkpoint: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let url = write_timezone_partition_table(temp_dir.path(), raw_timestamp).await;
    let engine = create_default_engine_mt_executor(&url).unwrap();
    if native_checkpoint {
        let snapshot = Snapshot::builder_for(url.clone())
            .build(engine.as_ref())
            .unwrap();
        snapshot.checkpoint(engine.as_ref(), None).unwrap();
    }

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let partition_values = timestamp_timezone
        .map_or_else(PartitionValuesOptions::with_struct, |timezone| {
            PartitionValuesOptions::with_struct().with_timestamp_timezone(timezone)
        });
    let scan = snapshot
        .scan_builder()
        .with_partition_values(partition_values)
        .build()
        .unwrap();
    let mut rows = 0;
    for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (data, selection) = metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        if batch.num_rows() == 0 {
            continue;
        }
        let parsed = get_column!(batch, "partitionValues_parsed", StructArray);
        let timestamp = parsed
            .column_by_name("p_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let timestamp_ntz = parsed
            .column_by_name("p_ntz")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let integer = parsed
            .column_by_name("p_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            timestamp.value(0),
            chrono::DateTime::parse_from_rfc3339(expected_timestamp)
                .unwrap()
                .timestamp_micros()
        );
        assert_eq!(
            timestamp_ntz.value(0),
            chrono::DateTime::parse_from_rfc3339("2024-01-15T12:30:45.123456Z")
                .unwrap()
                .timestamp_micros(),
            "reader timezone must not affect TIMESTAMP_NTZ"
        );
        assert_eq!(integer.value(0), 7);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 1);
}

#[rstest]
#[case::reader_timezone(
    "2024-01-15 12:30:45.123456",
    "America/Los_Angeles",
    "2024-01-15T20:30:45.123456Z"
)]
#[case::explicit_offset_wins(
    "2024-01-15 12:30:45+02:00",
    "America/Los_Angeles",
    "2024-01-15T10:30:45Z"
)]
#[case::dst_overlap_uses_earlier_instant(
    "2024-11-03 01:30:00",
    "America/Los_Angeles",
    "2024-11-03T08:30:00Z"
)]
#[case::dst_gap_uses_pre_transition_offset(
    "2024-03-10 02:30:00",
    "America/Los_Angeles",
    "2024-03-10T10:30:00Z"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_execute_partition_row_transform_respects_reader_timezone(
    #[case] raw_timestamp: &str,
    #[case] timestamp_timezone: &str,
    #[case] expected_timestamp: &str,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let url = write_timezone_partition_table(temp_dir.path(), raw_timestamp).await;
    let engine = create_default_engine_mt_executor(&url).unwrap();
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(
            PartitionValuesOptions::string_map_only().with_timestamp_timezone(timestamp_timezone),
        )
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = scan
        .execute(engine)
        .unwrap()
        .map(|result| {
            ArrowEngineData::try_from_engine_data(result.unwrap())
                .unwrap()
                .into()
        })
        .collect();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    let timestamp = get_column!(batch, "p_ts", TimestampMicrosecondArray);
    let timestamp_ntz = get_column!(batch, "p_ntz", TimestampMicrosecondArray);
    let integer = get_column!(batch, "p_int", Int32Array);
    assert_eq!(
        timestamp.value(0),
        chrono::DateTime::parse_from_rfc3339(expected_timestamp)
            .unwrap()
            .timestamp_micros()
    );
    assert_eq!(
        timestamp_ntz.value(0),
        chrono::DateTime::parse_from_rfc3339("2024-01-15T12:30:45.123456Z")
            .unwrap()
            .timestamp_micros(),
        "reader timezone must not affect TIMESTAMP_NTZ"
    );
    assert_eq!(integer.value(0), 7);
}

#[rstest]
#[case::invalid_zone("2024-01-15 12:30:45", "Not/AZone", "Not/AZone")]
#[case::invalid_timestamp("not a timestamp", "America/Los_Angeles", "not a timestamp")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timezone_aware_parsed_partition_values_report_invalid_input(
    #[case] raw_timestamp: &str,
    #[case] timestamp_timezone: &str,
    #[case] expected_error: &str,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let url = write_timezone_partition_table(temp_dir.path(), raw_timestamp).await;
    let engine = create_default_engine_mt_executor(&url).unwrap();
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(
            PartitionValuesOptions::with_struct().with_timestamp_timezone(timestamp_timezone),
        )
        .build()
        .unwrap();
    let result = scan
        .scan_metadata(engine.as_ref())
        .unwrap()
        .next()
        .expect("one metadata result");
    let Err(error) = result else {
        panic!("invalid timestamp input must fail map parsing");
    };
    assert!(error.to_string().contains(expected_error), "{error}");
}
