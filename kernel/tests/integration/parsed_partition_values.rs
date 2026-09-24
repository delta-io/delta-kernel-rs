//! Integration tests for parsed partition-value output (`partitionValues_parsed`).

use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray as _, BinaryArray, BooleanArray, Datum, RecordBatch, StringArray,
    StructArray,
};
use delta_kernel::arrow::compute::kernels::zip::zip;
use delta_kernel::arrow::compute::{concat_batches, filter_record_batch};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{col, lit, ColumnName, Predicate};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::parquet::file::properties::WriterProperties;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::PartitionValuesOptions;
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::Snapshot;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{partitioned, version_latest, FeatureSet, LogState, TableConfig};
use test_utils::{
    add_commit, create_default_engine_mt_executor, get_column,
    install_thread_local_metrics_reporter, record_batch_to_bytes_with_props, test_context,
    CountingReporter,
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
// did, then assert the kernel reads "" as null for every type, as the Delta protocol requires.

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

/// Where a scan of a foreign-writer table reads its partition values from.
#[derive(Clone, Copy, Debug, PartialEq)]
enum LogSource {
    Commits,
    /// A kernel-written checkpoint, whose native `partitionValues_parsed` holds null for "".
    KernelCheckpoint,
    /// A checkpoint whose native STRING and BINARY partition values hold a foreign writer's
    /// literal "", with one action per row group so checkpoint row-group skipping sees each value.
    ForeignCheckpoint,
}

/// Checkpoints the table at `table_path` according to `source`.
fn checkpoint_table(table_path: &std::path::Path, url: &Url, source: LogSource) {
    if source == LogSource::Commits {
        return;
    }
    let engine = create_default_engine_mt_executor(url).unwrap();
    let snapshot = Snapshot::builder_for(url.clone())
        .build(engine.as_ref())
        .unwrap();
    snapshot.checkpoint(engine.as_ref(), None).unwrap();
    if source == LogSource::KernelCheckpoint {
        return;
    }

    let log_dir = table_path.join("_delta_log");
    let checkpoint_path = log_dir.join(format!("{:020}.checkpoint.parquet", snapshot.version()));
    let file = std::fs::File::open(&checkpoint_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    let batch = concat_batches(&batches[0].schema(), &batches).unwrap();

    // Replace every null native STRING and BINARY value of an Add with "".
    let actions = StructArray::from(batch);
    let add = actions.column_by_name("add").unwrap().as_struct();
    let parsed = add
        .column_by_name("partitionValues_parsed")
        .unwrap()
        .as_struct();
    let fill = |name: &str, empty: &dyn Datum| {
        let values = parsed.column_by_name(name).unwrap();
        let is_empty = BooleanArray::from_iter(
            (0..add.len()).map(|row| Some(add.is_valid(row) && values.is_null(row))),
        );
        zip(&is_empty, empty, values).unwrap()
    };
    let parsed = with_field(parsed, "p_str", fill("p_str", &StringArray::new_scalar("")));
    let parsed = with_field(
        &parsed,
        "p_bin",
        fill("p_bin", &BinaryArray::new_scalar(b"")),
    );
    let add = with_field(add, "partitionValues_parsed", Arc::new(parsed));
    let batch = RecordBatch::from(with_field(&actions, "add", Arc::new(add)));

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    std::fs::write(
        &checkpoint_path,
        record_batch_to_bytes_with_props(&batch, props),
    )
    .unwrap();
    std::fs::remove_file(log_dir.join("_last_checkpoint")).unwrap();
}

/// Returns `array` with its `name` child replaced by `value`.
fn with_field(array: &StructArray, name: &str, value: ArrayRef) -> StructArray {
    let (fields, mut columns, nulls) = array.clone().into_parts();
    columns[fields.find(name).unwrap().0] = value;
    StructArray::new(fields, columns, nulls)
}

/// A foreign writer can persist a literal "" in the `partitionValues` map. On read, kernel
/// reconstructs every such `partitionValues_parsed` field as null from every [`LogSource`].
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parsed_partition_values_read_foreign_empty_string(
    #[values(
        LogSource::Commits,
        LogSource::KernelCheckpoint,
        LogSource::ForeignCheckpoint
    )]
    source: LogSource,
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
    checkpoint_table(&table_path, &url, source);
    let engine = create_default_engine_mt_executor(&url).unwrap();

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
        u64::from(source != LogSource::Commits),
        "log segment checkpoint parts must match {source:?}"
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
        for name in ["p_str", "p_bin", "p_int"] {
            assert!(
                pv.column_by_name(name).unwrap().is_null(0),
                "{name} \"\" must be null"
            );
        }

        asserted_rows += batch.num_rows();
    }
    assert_eq!(asserted_rows, 1, "expected exactly one file ({source:?})");
}

fn collect_path(paths: &mut Vec<String>, scan_file: ScanFile) {
    paths.push(scan_file.path);
}

/// A file whose partition value is a foreign literal "" has a null partition value, so partition
/// skipping prunes it under an equality with any literal (including `''`) and under `IS NOT NULL`,
/// and keeps it under `IS NULL`, from every [`LogSource`].
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_string_partition_pruning(
    #[values(
        LogSource::Commits,
        LogSource::KernelCheckpoint,
        LogSource::ForeignCheckpoint
    )]
    source: LogSource,
) {
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
    checkpoint_table(&table_path, &url, source);
    let engine = create_default_engine_mt_executor(&url).unwrap();

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

    let empty = vec!["p_str=/empty.parquet".to_string()];
    let other = vec!["p_str=other/other.parquet".to_string()];

    assert!(
        surviving(Predicate::eq(col!("p_str"), lit(""))).is_empty(),
        "null partition file must be pruned under p_str = ''"
    );
    assert_eq!(
        surviving(Predicate::eq(col!("p_str"), lit("other"))),
        other,
        "null partition file must be pruned under p_str = 'other'"
    );
    assert_eq!(
        surviving(Predicate::is_null(col!("p_str"))),
        empty,
        "null partition file must be kept under p_str IS NULL"
    );
    assert_eq!(
        surviving(Predicate::is_not_null(col!("p_str"))),
        other,
        "null partition file must be pruned under p_str IS NOT NULL"
    );
    assert_eq!(
        surviving(Predicate::is_null(col!("p_bin"))),
        empty,
        "null partition file must be kept under p_bin IS NULL"
    );
}
