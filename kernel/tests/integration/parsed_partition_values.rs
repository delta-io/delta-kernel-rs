//! Integration tests for parsed partition-value output (`partitionValues_parsed`).

use std::fs::File;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime};
use delta_kernel::arrow::array::{
    Array, BinaryArray, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{col, lit, ColumnName, Predicate, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::arrow::ArrowWriter;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::PartitionValuesOptions;
use delta_kernel::schema::{schema_ref, DataType, StructField};
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{Engine, Snapshot, SnapshotRef};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{
    partitioned, unpartitioned, version_latest, FeatureSet, LogState, TableConfig, TestTableBuilder,
};
use test_utils::{
    add_commit, assert_result_error_with_message, create_default_engine_mt_executor, get_column,
    install_thread_local_metrics_reporter, test_context, test_table_setup_mt, CountingReporter,
};
use url::Url;

/// Typed partition values use physical names across column mapping modes, with or without a
/// checkpoint.
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
/// The checkpoint axis verifies that native parsed checkpoint values preserve these semantics.
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

#[rstest]
#[case::default_utc(None, "2024-01-15 12:30:45.123456", "2024-01-15T12:30:45.123456Z")]
#[case::explicit_utc(
    Some("UTC"),
    "2024-01-15 12:30:45.123456",
    "2024-01-15T12:30:45.123456Z"
)]
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
#[case::fixed_offset(
    Some("+12:45"),
    "2024-01-15 12:30:45.123456",
    "2024-01-14T23:45:45.123456Z"
)]
#[case::explicit_offset(
    Some("America/Los_Angeles"),
    "2024-01-15T12:30:45+02:00",
    "2024-01-15T10:30:45Z"
)]
#[case::dst_overlap(
    Some("America/Los_Angeles"),
    "2024-11-03 01:30:00",
    "2024-11-03T08:30:00Z"
)]
#[case::dst_gap(
    Some("America/Los_Angeles"),
    "2024-03-10 02:30:00",
    "2024-03-10T10:30:00Z"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_less_timestamp_partition_read_with_json_and_checkpoint(
    #[case] tz: Option<&str>,
    #[case] timestamp: &str,
    #[values("none", "id", "name")] cm_mode: &str,
    #[case] expected_ts: &str,
    #[values(false, true)] write_parsed_partition: bool,
) {
    // === Init a table with one file in a checkpoint and one in JSON ===
    let (_temp_dir, table_path, engine) = test_table_setup_mt().unwrap();
    let snapshot = create_timestamp_partition_table_with_parsed_partition(
        &table_path,
        engine.as_ref(),
        cm_mode,
        write_parsed_partition,
    );

    insert_raw_timestamp_partitions(
        &snapshot,
        &[TimestampPartitionFile {
            filename: "file1.parquet",
            timestamp,
        }],
    )
    .await;
    let snapshot = Snapshot::builder_for(&table_path)
        .build(engine.as_ref())
        .unwrap();
    snapshot.checkpoint(engine.as_ref(), None).unwrap();

    insert_raw_timestamp_partitions(
        &snapshot,
        &[TimestampPartitionFile {
            filename: "file2.parquet",
            timestamp,
        }],
    )
    .await;
    let snapshot = Snapshot::builder_for(&table_path)
        .build(engine.as_ref())
        .unwrap();
    let physical_names = physical_timestamp_partition_names(&snapshot);

    // === Build a scan with the timezone ===
    let options = tz.map_or_else(PartitionValuesOptions::with_struct, |tz| {
        PartitionValuesOptions::with_struct().with_timestamp_timezone(tz)
    });
    let scan = snapshot
        .scan_builder()
        .with_partition_values(options)
        .build()
        .unwrap();

    // === Assert results ===
    // Checkpoint writes interpret offset-less timestamps in UTC.
    let checkpoint_parsed_ts = DateTime::parse_from_rfc3339(timestamp)
        .map(|value| value.timestamp_micros())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc().timestamp_micros())
        })
        .unwrap();
    let expected_ts = timestamp_micros(expected_ts);
    let mut paths = Vec::new();
    for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let metadata = metadata.unwrap();
        if tz.is_some() {
            assert!(metadata.scan_file_transforms.iter().all(Option::is_none));
        }
        let (data, selection) = metadata.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        if batch.num_rows() == 0 {
            continue;
        }
        let parsed = get_column!(batch, "partitionValues_parsed", StructArray);
        let timestamps = get_column!(parsed, &physical_names[0], TimestampMicrosecondArray);
        let timestamps_ntz = get_column!(parsed, &physical_names[1], TimestampMicrosecondArray);
        let integers = get_column!(parsed, &physical_names[2], Int32Array);
        let file_paths = get_column!(batch, "path", StringArray);
        for field in parsed.columns() {
            assert_eq!(field.null_count(), 0);
        }
        for row in 0..batch.num_rows() {
            let path = file_paths.value(row);
            let expected = match path {
                "file1.parquet" if write_parsed_partition => checkpoint_parsed_ts,
                "file1.parquet" | "file2.parquet" => expected_ts,
                _ => panic!("unexpected file: {path}"),
            };
            assert_eq!(timestamps.value(row), expected, "{path}");
            assert_eq!(
                timestamps_ntz.value(row),
                timestamp_micros("2024-01-15T12:30:45.123456Z"),
                "timezone must not affect TIMESTAMP_NTZ"
            );
            assert_eq!(integers.value(row), 7);
            paths.push(path.to_string());
        }
    }
    paths.sort();
    assert_eq!(paths, ["file1.parquet", "file2.parquet"]);
}

#[rstest]
#[case::utc_before(
    col!("p_ts").lt(lit(Scalar::Timestamp(timestamp_micros("2024-01-15T16:00:00Z")))),
    None,
    &[
        ("offset-less-in-checkpoint.parquet", "2024-01-15T12:30:45.123456Z"),
        ("offset-less-in-json.parquet", "2024-01-15T12:30:45.123456Z"),
    ],
    true /* write_parsed_partition */
)]
#[case::utc_after(
    col!("p_ts").gt(lit(Scalar::Timestamp(timestamp_micros("2024-01-15T16:00:00Z")))),
    None,
    &[
        ("utc-in-checkpoint.parquet", "2024-01-15T18:30:45.123456Z"),
        ("utc-in-json.parquet", "2024-01-15T18:30:45.123456Z"),
    ],
    true /* write_parsed_partition */
)]
#[case::los_angeles_before(
    col!("p_ts").lt(lit(Scalar::Timestamp(timestamp_micros("2024-01-15T16:00:00Z")))),
    Some("America/Los_Angeles"),
    &[("offset-less-in-checkpoint.parquet", "2024-01-15T12:30:45.123456Z")],
    true /* write_parsed_partition */
)]
#[case::los_angeles_after(
    col!("p_ts").gt(lit(Scalar::Timestamp(timestamp_micros("2024-01-15T16:00:00Z")))),
    Some("America/Los_Angeles"),
    &[
        ("offset-less-in-json.parquet", "2024-01-15T20:30:45.123456Z"),
        ("utc-in-checkpoint.parquet", "2024-01-15T18:30:45.123456Z"),
        ("utc-in-json.parquet", "2024-01-15T18:30:45.123456Z"),
    ],
    true /* write_parsed_partition */
)]
#[case::los_angeles_before_without_parsed(
    col!("p_ts").lt(lit(Scalar::Timestamp(timestamp_micros("2024-01-15T16:00:00Z")))),
    Some("America/Los_Angeles"),
    &[],
    false /* write_parsed_partition */
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_less_timestamp_partition_predicates_with_json_and_checkpoint(
    #[case] predicate: Predicate,
    #[case] tz: Option<&str>,
    #[case] expected_files_and_timestamps: &[(&str, &str)],
    #[case] write_parsed_partition: bool,
    #[values(false, true)] parsed_struct_on_scan: bool,
    #[values("none", "id", "name")] cm_mode: &str,
) {
    let (_temp_dir, table_path, engine) = test_table_setup_mt().unwrap();
    let snapshot = create_timestamp_partition_table_with_parsed_partition(
        &table_path,
        engine.as_ref(),
        cm_mode,
        write_parsed_partition,
    );

    insert_raw_timestamp_partitions(
        &snapshot,
        &[
            TimestampPartitionFile {
                filename: "offset-less-in-checkpoint.parquet",
                timestamp: "2024-01-15 12:30:45.123456",
            },
            TimestampPartitionFile {
                filename: "utc-in-checkpoint.parquet",
                timestamp: "2024-01-15T18:30:45.123456Z",
            },
        ],
    )
    .await;
    let snapshot = Snapshot::builder_for(&table_path)
        .build(engine.as_ref())
        .unwrap();
    snapshot.checkpoint(engine.as_ref(), None).unwrap();

    insert_raw_timestamp_partitions(
        &snapshot,
        &[
            TimestampPartitionFile {
                filename: "offset-less-in-json.parquet",
                timestamp: "2024-01-15 12:30:45.123456",
            },
            TimestampPartitionFile {
                filename: "utc-in-json.parquet",
                timestamp: "2024-01-15T18:30:45.123456Z",
            },
        ],
    )
    .await;
    let snapshot = Snapshot::builder_for(&table_path)
        .build(engine.as_ref())
        .unwrap();
    let physical_names = physical_timestamp_partition_names(&snapshot);
    let mut options = if parsed_struct_on_scan {
        PartitionValuesOptions::with_struct()
    } else {
        PartitionValuesOptions::string_map_only()
    };
    if let Some(tz) = tz {
        options = options.with_timestamp_timezone(tz);
    }
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(predicate.clone()))
        .with_partition_values(options)
        .build()
        .unwrap();

    let mut actual = Vec::new();
    for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (data, selection) = metadata.unwrap().scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data).unwrap().into();
        let batch = filter_record_batch(&batch, &BooleanArray::from(selection)).unwrap();
        let paths = get_column!(batch, "path", StringArray);
        let timestamps = parsed_struct_on_scan.then(|| {
            let parsed = get_column!(batch, "partitionValues_parsed", StructArray);
            get_column!(parsed, &physical_names[0], TimestampMicrosecondArray)
        });
        for row in 0..batch.num_rows() {
            actual.push((
                paths.value(row).to_string(),
                timestamps.map(|timestamps| {
                    assert!(!timestamps.is_null(row));
                    timestamps.value(row)
                }),
            ));
        }
    }
    actual.sort();
    let expected: Vec<_> = expected_files_and_timestamps
        .iter()
        .map(|(path, timestamp)| {
            (
                path.to_string(),
                parsed_struct_on_scan.then(|| timestamp_micros(timestamp)),
            )
        })
        .collect();
    assert_eq!(
        actual, expected,
        "tz={tz:?}, cm={cm_mode}, parsed={parsed_struct_on_scan}"
    );
}

// Parallel scan, declarative scan, and scan.execute() require timezone support when the table is
// timestamp partitioned.
#[rstest]
#[case::unpartitioned(vec![])]
#[case::timestamp(vec![("ts", DataType::TIMESTAMP)])]
#[case::timestamp_ntz(vec![("ts_ntz", DataType::TIMESTAMP_NTZ)])]
#[case::integer(vec![("id", DataType::INTEGER)])]
#[case::string(vec![("category", DataType::STRING)])]
fn different_scans_require_timezone_support_only_for_timestamp_partitions(
    #[values("UTC", "America/Los_Angeles", "+05:30")] tz: &str,
    #[values(false, true)] parsed_struct: bool,
    #[case] partition_columns: Vec<(&str, DataType)>,
) {
    let timestamp_partitioned = partition_columns
        .iter()
        .any(|(_, data_type)| data_type == &DataType::TIMESTAMP);
    let (partition_names, partition_fields): (Vec<_>, Vec<_>) = partition_columns
        .into_iter()
        .map(|(name, data_type)| (name, StructField::nullable(name, data_type)))
        .unzip();
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1).with_checkpoint_at([1]))
        .with_table_config(TableConfig::new().write_stats_as_struct(true))
        .with_schema(schema_ref! {
            ..(partition_fields),
            nullable "value": INTEGER,
        })
        .with_partition_columns(partition_names)
        .build()
        .unwrap();
    let engine = Arc::new(table.engine());
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(engine.as_ref())
        .unwrap();
    let options = if parsed_struct {
        PartitionValuesOptions::with_struct()
    } else {
        PartitionValuesOptions::string_map_only()
    };
    let scan = snapshot
        .scan_builder()
        .with_partition_values(options.with_timestamp_timezone(tz))
        .build()
        .unwrap();

    #[cfg(feature = "declarative-plans")]
    assert_result_error_with_message(
        scan.declarative_metadata_scan_plan(engine.as_ref()),
        if timestamp_partitioned {
            "with_timestamp_timezone in declarative_metadata_scan_plan"
        } else {
            "this engine does not provide a PlanExecutor"
        },
    );
    let parallel_result = scan.parallel_scan_metadata(engine.clone());
    let execute_result = scan.execute(engine);
    if timestamp_partitioned {
        assert_result_error_with_message(
            parallel_result,
            "with_timestamp_timezone in parallel_scan_metadata",
        );
        assert_result_error_with_message(execute_result, "row transforms are disabled");
    } else {
        parallel_result.unwrap();
        let batches = execute_result
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(!batches.is_empty());
    }
}

#[rstest]
#[case::unpartitioned(vec![], false /* expect_transform */)]
#[case::not_timestamp_partition(vec!["category", "ts_ntz"], true /* expect_transform */)]
#[case::timestamp_partition(vec!["ts"], false /* expect_transform */)]
fn timestamp_timezone_controls_row_transforms(
    #[case] partition_columns: Vec<&str>,
    #[case] expect_transform: bool,
    #[values("UTC", "America/Los_Angeles", "+05:30")] timezone: &str,
) {
    let timestamp_partition = partition_columns.contains(&"ts");
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_schema(schema_ref! {
            nullable "ts": TIMESTAMP,
            nullable "ts_ntz": TIMESTAMP_NTZ,
            nullable "category": STRING,
            nullable "value": INTEGER,
        })
        .with_partition_columns(partition_columns)
        .with_data(1, 3)
        .build()
        .unwrap();
    let engine = Arc::new(table.engine());
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::default().with_timestamp_timezone(timezone))
        .build()
        .unwrap();

    let mut file_count = 0;
    for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
        let (_, count) = metadata
            .unwrap()
            .visit_scan_files((expect_transform, file_count), |(expected, count), file| {
                assert_eq!(file.transform.is_some(), *expected);
                *count += 1;
            })
            .unwrap();
        file_count = count;
    }
    assert!(file_count > 0);

    let result = scan.execute(engine);
    if timestamp_partition {
        assert_result_error_with_message(result, "row transforms are disabled");
    } else {
        let row_count: usize = result.unwrap().map(|batch| batch.unwrap().len()).sum();
        assert_eq!(row_count, 3);
    }
}

#[test]
fn scan_metadata_ignores_invalid_timestamp_timezone_for_unpartitioned_table() {
    let (engine, snapshot, _table) = test_context!(
        LogState::with_latest_version(1),
        FeatureSet::empty(),
        unpartitioned(),
        TableConfig::new(),
        version_latest(),
    );
    let scan = snapshot
        .scan_builder()
        .with_partition_values(
            PartitionValuesOptions::with_struct().with_timestamp_timezone("Not/AZone"),
        )
        .build()
        .unwrap();

    let mut file_count = 0;
    for metadata in scan.scan_metadata(&engine).unwrap() {
        file_count = metadata
            .unwrap()
            .visit_scan_files(file_count, |count, _| *count += 1)
            .unwrap();
    }
    assert_eq!(file_count, 1);
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
    let (_temp_dir, table_path, engine) = test_table_setup_mt().unwrap();
    let snapshot = create_timestamp_partition_table(&table_path, engine.as_ref(), "none");
    insert_raw_timestamp_partitions(
        &snapshot,
        &[TimestampPartitionFile {
            filename: "part.parquet",
            timestamp: raw_timestamp,
        }],
    )
    .await;
    let snapshot = Snapshot::builder_for(&table_path)
        .build(engine.as_ref())
        .unwrap();
    let scan = snapshot
        .scan_builder()
        .with_partition_values(
            PartitionValuesOptions::with_struct().with_timestamp_timezone(timestamp_timezone),
        )
        .build();
    let error = match scan {
        Err(error) => error,
        Ok(scan) => match scan.scan_metadata(engine.as_ref()) {
            Err(error) => error,
            Ok(mut metadata) => {
                let result = metadata.next().expect("one metadata result");
                let Err(error) = result else {
                    panic!("invalid timestamp input must fail map parsing");
                };
                error
            }
        },
    };
    assert!(error.to_string().contains(expected_error), "{error}");
}

fn create_timestamp_partition_table(
    table_path: &str,
    engine: &dyn Engine,
    cm_mode: &str,
) -> SnapshotRef {
    create_timestamp_partition_table_with_parsed_partition(
        table_path, engine, cm_mode, true, /* write_parsed_partition */
    )
}

fn create_timestamp_partition_table_with_parsed_partition(
    table_path: &str,
    engine: &dyn Engine,
    cm_mode: &str,
    write_parsed_partition: bool,
) -> SnapshotRef {
    let schema = schema_ref! {
        nullable "p_ts": TIMESTAMP,
        nullable "p_ntz": TIMESTAMP_NTZ,
        nullable "p_int": INTEGER,
        nullable "value": INTEGER,
    };
    create_table(table_path, schema, "Test/1.0", true)
        .with_data_layout(DataLayout::partitioned(["p_ts", "p_ntz", "p_int"]))
        .with_table_properties([
            ("delta.columnMapping.mode", cm_mode),
            (
                "delta.checkpoint.writeStatsAsStruct",
                if write_parsed_partition {
                    "true"
                } else {
                    "false"
                },
            ),
        ])
        .build_with_filesystem_committer(engine)
        .unwrap()
        .commit(engine)
        .unwrap()
        .unwrap_post_commit_snapshot()
}

struct TimestampPartitionFile<'a> {
    filename: &'a str,
    timestamp: &'a str,
}

// Write Parquet and commit Add actions with the specified serialized timestamp in partitionValues.
// This allows offset-less timestamps in addFile.partitionValues which kernel doesn't write.
async fn insert_raw_timestamp_partitions(
    snapshot: &SnapshotRef,
    files: &[TimestampPartitionFile<'_>],
) {
    let physical_names = physical_timestamp_partition_names(snapshot);
    let scan = snapshot.clone().scan_builder().build().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(scan.physical_schema().as_ref().try_into_arrow().unwrap()),
        vec![Arc::new(Int32Array::from(vec![42]))],
    )
    .unwrap();
    let table_path = snapshot.table_root().to_file_path().unwrap();
    let actions: Vec<_> = files
        .iter()
        .map(|file| {
            let path = table_path.join(file.filename);
            let mut writer =
                ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let file_size = std::fs::metadata(path).unwrap().len();
            let partition_values: serde_json::Map<_, _> = physical_names
                .iter()
                .zip([file.timestamp, "2024-01-15 12:30:45.123456", "7"])
                .map(|(name, value)| (name.clone(), serde_json::Value::String(value.to_string())))
                .collect();
            serde_json::json!({
                "add": {
                    "path": file.filename,
                    "partitionValues": partition_values,
                    "size": file_size,
                    "modificationTime": 1700000000000_i64,
                    "dataChange": true,
                    "stats": "{\"numRecords\":1}",
                },
            })
            .to_string()
        })
        .collect();
    add_commit(
        snapshot.table_root().as_str(),
        &LocalFileSystem::new(),
        snapshot.version() + 1,
        actions.join("\n"),
    )
    .await
    .unwrap();
}

fn physical_timestamp_partition_names(snapshot: &Snapshot) -> [String; 3] {
    ["p_ts", "p_ntz", "p_int"].map(|name| {
        get_any_level_column_physical_name(
            snapshot.schema().as_ref(),
            &ColumnName::new([name]),
            snapshot.table_configuration().column_mapping_mode(),
        )
        .unwrap()
        .into_inner()
        .pop()
        .unwrap()
    })
}

fn timestamp_micros(value: &str) -> i64 {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_micros()
}
