use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::expressions::Scalar;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Engine, Snapshot};

mod common;

use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use itertools::Itertools;
use test_utils::{
    insert_data, load_test_data, read_scan, test_table_setup_mt, write_batch_to_table,
};

fn read_v2_checkpoint_table(test_name: impl AsRef<str>) -> DeltaResult<Vec<RecordBatch>> {
    let test_dir = load_test_data("tests/data", test_name.as_ref()).unwrap();
    let test_path = test_dir.path().join(test_name.as_ref());

    let url =
        delta_kernel::try_parse_uri(test_path.to_str().expect("table path to string")).unwrap();
    let engine = test_utils::create_default_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref()).unwrap();
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;

    Ok(batches)
}

fn test_v2_checkpoint_with_table(
    table_name: &str,
    mut expected_table: Vec<String>,
) -> DeltaResult<()> {
    let batches = read_v2_checkpoint_table(table_name)?;

    sort_lines!(expected_table);
    assert_batches_sorted_eq!(expected_table, &batches);
    Ok(())
}

/// Helper function to convert string slice vectors to String vectors
fn to_string_vec(string_slice_vec: Vec<&str>) -> Vec<String> {
    string_slice_vec
        .into_iter()
        .map(|s| s.to_string())
        .collect()
}

fn generate_sidecar_expected_data() -> Vec<String> {
    let header = vec![
        "+-----+".to_string(),
        "| id  |".to_string(),
        "+-----+".to_string(),
    ];

    // Generate rows for different ranges
    let generate_rows = |count: usize| -> Vec<String> {
        (0..count)
            .map(|id| format!("| {:<max_width$} |", id, max_width = 3))
            .collect()
    };

    [
        header,
        vec!["| 0   |".to_string(); 3],
        generate_rows(30),
        generate_rows(100),
        generate_rows(100),
        generate_rows(1000),
        vec!["+-----+".to_string()],
    ]
    .into_iter()
    .flatten()
    .collect_vec()
}

// Rustfmt is disabled to maintain the readability of the expected table
#[rustfmt::skip]
fn get_simple_id_table() -> Vec<String> {
    to_string_vec(vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "+----+",
    ])
}

// Rustfmt is disabled to maintain the readability of the expected table
#[rustfmt::skip]
fn get_classic_checkpoint_table() -> Vec<String> {
    to_string_vec(vec![
        "+----+",
        "| id |",
        "+----+",
        "| 0  |",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "| 4  |",
        "| 5  |",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "| 10 |",
        "| 11 |",
        "| 12 |",
        "| 13 |",
        "| 14 |",
        "| 15 |",
        "| 16 |",
        "| 17 |",
        "| 18 |",
        "| 19 |",
        "+----+",
    ])
}

// Rustfmt is disabled to maintain the readability of the expected table
#[rustfmt::skip]
fn get_without_sidecars_table() -> Vec<String> {
    to_string_vec(vec![
        "+------+",
        "| id   |",
        "+------+",
        "| 0    |",
        "| 1    |",
        "| 2    |",
        "| 3    |",
        "| 4    |",
        "| 5    |",
        "| 6    |",
        "| 7    |",
        "| 8    |",
        "| 9    |",
        "| 2718 |",
        "+------+",
    ])
}

/// The test cases below are derived from delta-spark's `CheckpointSuite`.
///
/// These tests are converted from delta-spark using the following process:
/// 1. Specific test cases of interest in `delta-spark` were modified to persist their generated
///    tables
/// 2. These tables were compressed into `.tar.zst` archives and copied to delta-kernel-rs
/// 3. Each test loads a stored table, scans it, and asserts that the returned table state matches
///    the expected state derived from the corresponding table insertions in `delta-spark`
///
/// The following is the ported list of `delta-spark` tests -> `delta-kernel-rs` tests:
///
/// - `multipart v2 checkpoint` -> `v2_checkpoints_json_with_sidecars`
/// - `multipart v2 checkpoint` -> `v2_checkpoints_parquet_with_sidecars`
/// - `All actions in V2 manifest` -> `v2_checkpoints_json_without_sidecars`
/// - `All actions in V2 manifest` -> `v2_checkpoints_parquet_without_sidecars`
/// - `V2 Checkpoint compat file equivalency to normal V2 Checkpoint` ->
///   `v2_classic_checkpoint_json`
/// - `V2 Checkpoint compat file equivalency to normal V2 Checkpoint` ->
///   `v2_classic_checkpoint_parquet`
/// - `last checkpoint contains correct schema for v1/v2 Checkpoints` ->
///   `v2_checkpoints_json_with_last_checkpoint`
/// - `last checkpoint contains correct schema for v1/v2 Checkpoints` ->
///   `v2_checkpoints_parquet_with_last_checkpoint`
#[test]
fn v2_checkpoints_json_with_sidecars() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-json-with-sidecars",
        generate_sidecar_expected_data(),
    )
}

#[test]
fn v2_checkpoints_parquet_with_sidecars() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-parquet-with-sidecars",
        generate_sidecar_expected_data(),
    )
}

#[test]
fn v2_checkpoints_json_without_sidecars() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-json-without-sidecars",
        get_without_sidecars_table(),
    )
}

#[test]
fn v2_checkpoints_parquet_without_sidecars() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-parquet-without-sidecars",
        get_without_sidecars_table(),
    )
}

#[test]
fn v2_classic_checkpoint_json() -> DeltaResult<()> {
    test_v2_checkpoint_with_table("v2-classic-checkpoint-json", get_classic_checkpoint_table())
}

#[test]
fn v2_classic_checkpoint_parquet() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-classic-checkpoint-parquet",
        get_classic_checkpoint_table(),
    )
}

#[test]
fn v2_checkpoints_json_with_last_checkpoint() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-json-with-last-checkpoint",
        get_simple_id_table(),
    )
}

#[test]
fn v2_checkpoints_parquet_with_last_checkpoint() -> DeltaResult<()> {
    test_v2_checkpoint_with_table(
        "v2-checkpoints-parquet-with-last-checkpoint",
        get_simple_id_table(),
    )
}

/// Tests that writing a V2 checkpoint to parquet succeeds.
///
/// V2 checkpoints include a checkpointMetadata batch in addition to the regular action
/// batches. All batches in a parquet file must share the same schema. This test verifies
/// that `snapshot.checkpoint()` can write a V2 checkpoint without schema mismatch errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_parquet_write() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);
    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([("delta.feature.v2Checkpoint", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Commit an add action via the transaction API so the checkpoint has action batches
    let snapshot0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let result = insert_data(
        snapshot0,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?;

    let CommitResult::CommittedTransaction(committed) = result else {
        panic!("Expected CommittedTransaction");
    };

    let snapshot = committed
        .post_commit_snapshot()
        .expect("expected post-commit snapshot");

    // This writes to parquet — will fail if the checkpointMetadata batch has a different
    // schema than the action batches.
    snapshot.checkpoint(engine.as_ref())?;

    // Verify the checkpoint was written and is used by a fresh snapshot
    let snapshot2 = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot2.version(), 1);
    let log_segment = snapshot2.log_segment();
    assert!(
        !log_segment.listed.checkpoint_parts.is_empty(),
        "expected snapshot to use the written checkpoint, but checkpoint_parts is empty"
    );
    assert_eq!(
        log_segment.checkpoint_version,
        Some(1),
        "expected checkpoint at version 1"
    );
    assert!(
        log_segment.listed.ascending_commit_files.is_empty(),
        "expected no commit files after checkpoint, but found: {:?}",
        log_segment.listed.ascending_commit_files
    );

    // Verify reading data from the checkpointed snapshot returns the expected rows
    let scan = snapshot2.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone() as Arc<dyn delta_kernel::Engine>)?;
    assert_batches_sorted_eq!(
        vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ],
        &batches
    );

    Ok(())
}

/// Reads all parquet record batches from a file, concatenating them into a single batch.
fn read_parquet_file(path: &std::path::Path) -> RecordBatch {
    let bytes = std::fs::read(path).expect("failed to read parquet file");
    let bytes = bytes::Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("failed to create parquet reader")
        .build()
        .expect("failed to build reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("failed to concat batches")
}

/// Finds the checkpoint parquet file in the `_delta_log` directory for a given version.
fn load_checkpoint_path(table_path: &str, version: u64) -> std::path::PathBuf {
    let log_dir = std::path::Path::new(table_path).join("_delta_log");
    let prefix = format!("{version:020}.checkpoint");
    for entry in std::fs::read_dir(&log_dir).expect("failed to read _delta_log") {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && name.ends_with(".parquet") {
            return entry.path();
        }
    }
    panic!("checkpoint parquet file not found for version {version} in {log_dir:?}");
}

/// Reads the `_last_checkpoint` JSON file from the table's `_delta_log` directory.
fn read_last_checkpoint(table_path: &str) -> serde_json::Value {
    let path = std::path::Path::new(table_path).join("_delta_log/_last_checkpoint");
    let content = std::fs::read_to_string(&path).expect("failed to read _last_checkpoint");
    serde_json::from_str(&content).expect("failed to parse _last_checkpoint JSON")
}

/// Creates a v2Checkpoint + domainMetadata table with a nested schema (`id: int`,
/// `info: struct { name: string }`). Inserts 8 files across several commits, removes all of
/// them, adds domain metadata (with a update for same domain), commits `txn` set-transactions
/// (with a reconciliation case for `app1`), then inserts 8 fresh files. Returns the final
/// snapshot. The table should have 8 live adds, 8 remove tombstones, 3 domain metadata
/// entries, and 2 reconciled `txn` actions (`app1@3`, `app2@5`).
///
/// Exercises every action in the V2 checkpoint schema except `sidecar`/`checkpointMetadata`,
/// which are produced by the checkpoint writer itself.
async fn v2_table_with_domain_metadata_and_txn<
    E: delta_kernel::engine::default::executor::TaskExecutor,
>(
    table_path: &str,
    table_url: &url::Url,
    engine: &Arc<delta_kernel::engine::default::DefaultEngine<E>>,
) -> DeltaResult<Arc<Snapshot>> {
    fn make_info_array(names: &[&str]) -> ArrayRef {
        let name_array: ArrayRef = Arc::new(StringArray::from(
            names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
        ));
        let field = Arc::new(ArrowField::new("name", ArrowDataType::Utf8, true));
        Arc::new(StructArray::from(vec![(field, name_array)]))
    }

    fn make_columns(id: i32, name: &str) -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(vec![id])) as ArrayRef,
            make_info_array(&[name]),
        ]
    }

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable(
            "info",
            DataType::try_struct_type([StructField::nullable("name", DataType::STRING)])?,
        ),
    ])?);

    let _ = create_table(table_path, schema.clone(), "Test/1.0")
        .with_table_properties([
            ("delta.feature.v2Checkpoint", "supported"),
            ("delta.feature.domainMetadata", "supported"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Insert 8 files (one per commit) -> ids 1..=8
    let mut snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let names = [
        "alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi",
    ];
    for (i, name) in names.iter().enumerate() {
        snapshot = insert_data(snapshot, engine, make_columns(i as i32 + 1, name))
            .await?
            .unwrap_committed()
            .post_commit_snapshot()
            .expect("expected post-commit snapshot")
            .clone();
    }

    // Domain metadata commit (no data) -- exercises the empty-file-batch skip path in the
    // sidecar splitter. Sets two domains initially.
    snapshot = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_domain_metadata("app.settings".to_string(), r#"{"version":1}"#.to_string())
        .with_domain_metadata(
            "app.feature_flags".to_string(),
            r#"{"dark_mode":true}"#.to_string(),
        )
        .commit(engine.as_ref())?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // Another domain metadata commit -- updates "app.settings" to verify reconciliation
    // picks the latest value, and adds a new domain.
    snapshot = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_domain_metadata(
            "app.analytics".to_string(),
            r#"{"tracking":false}"#.to_string(),
        )
        .with_domain_metadata("app.settings".to_string(), r#"{"version":2}"#.to_string())
        .commit(engine.as_ref())?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // SetTransaction commits -- exercise `txn` actions in checkpoint. Two distinct app_ids
    // plus a second update to `app1` to verify reconciliation picks the latest version.
    for (app_id, version) in [("app1", 1i64), ("app2", 5), ("app1", 3)] {
        snapshot = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_transaction_id(app_id.to_string(), version)
            .commit(engine.as_ref())?
            .unwrap_committed()
            .post_commit_snapshot()
            .expect("expected post-commit snapshot")
            .clone();
    }

    // Remove all 8 files -> 8 remove tombstones
    let scan = snapshot.clone().scan_builder().build()?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("DELETE".to_string())
        .with_data_change(true);
    for sm in scan.scan_metadata(engine.as_ref())? {
        txn.remove_files(sm?.scan_files);
    }
    snapshot = txn
        .commit(engine.as_ref())?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // Insert 8 fresh files (one per commit) -> ids 9..=16
    let names = [
        "ivan", "judy", "karl", "lena", "mike", "nina", "omar", "pat",
    ];
    for (i, name) in names.iter().enumerate() {
        snapshot = insert_data(snapshot, engine, make_columns(i as i32 + 9, name))
            .await?
            .unwrap_committed()
            .post_commit_snapshot()
            .expect("expected post-commit snapshot")
            .clone();
    }

    Ok(snapshot)
}

/// Extracts a named struct column from a `RecordBatch`, panicking if missing or wrong type.
fn get_struct_column_from_record_batch<'a>(batch: &'a RecordBatch, name: &str) -> &'a StructArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("batch should have column '{name}'"))
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| panic!("column '{name}' should be a StructArray"))
}

/// Returns indices of non-null (valid) rows for a column in a batch.
fn valid_row_indices(col: &dyn Array, num_rows: usize) -> Vec<usize> {
    (0..num_rows).filter(|&i| col.is_valid(i)).collect()
}

/// Extracts a named struct sub-column from a `StructArray`, panicking if missing or wrong type.
fn get_struct_column_from_struct_array<'a>(parent: &'a StructArray, name: &str) -> &'a StructArray {
    parent
        .column_by_name(name)
        .unwrap_or_else(|| panic!("struct should have field '{name}'"))
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| panic!("field '{name}' should be a StructArray"))
}

/// Lists all sidecar parquet files in the `_sidecars` directory.
fn list_sidecar_parquet_files(sidecars_dir: &std::path::Path) -> Vec<std::fs::DirEntry> {
    std::fs::read_dir(sidecars_dir)
        .expect("failed to list sidecars dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .collect()
}

/// Validates that sidecar action rows in the main checkpoint have correct path, sizeInBytes,
/// and modificationTime matching actual files on disk.
fn assert_sidecar_actions_match_disk(
    sidecar_col: &StructArray,
    sidecar_rows: &[usize],
    table_path: &str,
    engine: &dyn Engine,
) {
    let sidecar_path_col = sidecar_col
        .column_by_name("path")
        .expect("sidecar should have path field");
    let sidecar_size_col = sidecar_col
        .column_by_name("sizeInBytes")
        .expect("sidecar should have sizeInBytes field");
    let sidecar_mtime_col = sidecar_col
        .column_by_name("modificationTime")
        .expect("sidecar should have modificationTime field");
    let sidecar_tags_col = sidecar_col
        .column_by_name("tags")
        .expect("sidecar should have tags field");
    let sidecars_base = delta_kernel::try_parse_uri(table_path)
        .unwrap()
        .join("_delta_log/_sidecars/")
        .unwrap();

    for &row in sidecar_rows {
        let path = sidecar_path_col.as_string::<i32>().value(row);
        assert!(
            path.ends_with(".parquet"),
            "sidecar path should be a parquet file, got: {path}"
        );

        let sidecar_url = sidecars_base.join(path).unwrap();
        let file_meta = engine
            .storage_handler()
            .head(&sidecar_url)
            .unwrap_or_else(|e| panic!("sidecar file {path} should exist: {e}"));

        let recorded_size = sidecar_size_col
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
            .value(row);
        assert_eq!(
            recorded_size, file_meta.size as i64,
            "sidecar sizeInBytes should match actual file size for {path}"
        );

        let recorded_mtime = sidecar_mtime_col
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
            .value(row);
        assert_eq!(
            recorded_mtime, file_meta.last_modified,
            "sidecar modificationTime should match actual file mtime for {path}"
        );

        assert!(
            sidecar_tags_col.is_null(row),
            "sidecar tags should be null for {path}"
        );
    }
}

/// Reads all sidecar parquet files from the `_sidecars` directory. Asserts each sidecar
/// contains only add/remove file actions (no non-file actions). Returns a sorted list of
/// `(add_count, remove_count)` per sidecar.
fn assert_sidecars_contain_only_file_actions(
    sidecars_dir: &std::path::Path,
) -> Vec<(usize, usize)> {
    let sidecar_files = list_sidecar_parquet_files(sidecars_dir);
    assert!(
        !sidecar_files.is_empty(),
        "should have at least one sidecar parquet file in _delta_log/_sidecars/"
    );

    let mut per_sidecar_counts: Vec<(usize, usize)> = Vec::new();
    for sidecar_entry in &sidecar_files {
        let sidecar_batch = read_parquet_file(&sidecar_entry.path());
        let sidecar_schema = sidecar_batch.schema();

        // Sidecar must have add and/or remove columns
        assert!(
            sidecar_schema.field_with_name("add").is_ok()
                || sidecar_schema.field_with_name("remove").is_ok(),
            "sidecar file should contain add and/or remove columns"
        );

        // Sidecar must NOT contain non-file actions
        for forbidden in &[
            "protocol",
            "metaData",
            "checkpointMetadata",
            "domainMetadata",
            "txn",
            "sidecar",
        ] {
            if let Ok(field) = sidecar_schema.field_with_name(forbidden) {
                let col = sidecar_batch.column_by_name(forbidden).unwrap();
                let non_null = valid_row_indices(col, sidecar_batch.num_rows()).len();
                assert_eq!(
                    non_null, 0,
                    "sidecar should not contain non-null {forbidden} actions, \
                     but found {non_null} (field type: {field:?})"
                );
            }
        }

        let adds = sidecar_batch
            .column_by_name("add")
            .map(|c| valid_row_indices(c, sidecar_batch.num_rows()).len())
            .unwrap_or(0);
        let removes = sidecar_batch
            .column_by_name("remove")
            .map(|c| valid_row_indices(c, sidecar_batch.num_rows()).len())
            .unwrap_or(0);
        per_sidecar_counts.push((adds, removes));
    }
    per_sidecar_counts.sort();
    per_sidecar_counts
}

/// E2e test for V2 sidecar checkpoint writing and reading:
///
/// 1. Create a V2 table with adds, removes, domain metadata, and set-transactions
/// 2. Write a V2 checkpoint with sidecars (hint=2)
/// 3. Add post-checkpoint commits (insert + domain metadata update)
/// 4. Validate `_last_checkpoint` (version, size, sizeInBytes, numOfAddFiles)
/// 5. Read the raw checkpoint parquet and validate:
///    - Main checkpoint has sidecar, protocol, metadata, checkpointMetadata, domainMetadata, txn
///    - Main checkpoint has NO add/remove actions (they live in sidecars)
///    - 5 sidecar references match the actual sidecar files on disk
///    - Sidecar files contain only add/remove actions
///    - Per-sidecar distribution: 1 sidecar with 8 removes, 4 sidecars with 2 adds each
///    - checkpointMetadata version matches checkpoint version
///    - 3 reconciled domain metadata actions
///    - 2 reconciled txn actions (`app1@3`, `app2@5`)
/// 6. Load a fresh snapshot from the checkpoint + post-checkpoint commits
/// 7. Verify domain metadata (including post-checkpoint update)
/// 8. Scan and verify data correctness (9 rows: ids 9-17)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_with_sidecars() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let snapshot = v2_table_with_domain_metadata_and_txn(&table_path, &table_url, &engine).await?;
    let version = snapshot.version() as i64;

    // Write V2 checkpoint with sidecars (hint=2 so file actions split across sidecars)
    let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(2),
    });
    snapshot.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&checkpoint_spec))?;

    // Post-checkpoint: insert one more row (id=17) and update existing domain metadata
    let info_field = Arc::new(ArrowField::new("name", ArrowDataType::Utf8, true));
    let info_array: ArrayRef = Arc::new(StructArray::from(vec![(
        info_field,
        Arc::new(StringArray::from(vec![Some("quinn")])) as ArrayRef,
    )]));
    let post_ckpt_snapshot = insert_data(
        Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?,
        &engine,
        vec![Arc::new(Int32Array::from(vec![17])) as ArrayRef, info_array],
    )
    .await?
    .unwrap_committed()
    .post_commit_snapshot()
    .expect("expected post-commit snapshot")
    .clone();

    let post_ckpt_snapshot = post_ckpt_snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_domain_metadata("app.settings".to_string(), r#"{"version":3}"#.to_string())
        .commit(engine.as_ref())?
        .unwrap_committed()
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // === Validate _last_checkpoint ===
    let last_ckpt = read_last_checkpoint(&table_path);
    let ckpt_file = load_checkpoint_path(&table_path, version as _);
    let ckpt_file_size = std::fs::metadata(&ckpt_file).unwrap().len() as i64;
    let sidecars_dir_for_size = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    let sidecars_total_size: i64 = list_sidecar_parquet_files(&sidecars_dir_for_size)
        .iter()
        .map(|e| e.metadata().unwrap().len() as i64)
        .sum();

    assert_eq!(last_ckpt["version"], version);
    // size = 1 protocol + 1 metadata + 8 adds + 8 removes + 3 domain metadata
    //       + 2 set-transactions (app1 reconciled to the latest) + 1 checkpointMetadata
    //       + 5 sidecar references = 29
    assert_eq!(last_ckpt["size"].as_i64().unwrap(), 29);
    assert_eq!(
        last_ckpt["sizeInBytes"].as_i64().unwrap(),
        ckpt_file_size + sidecars_total_size
    );
    assert_eq!(last_ckpt["numOfAddFiles"], 8);

    // === Read raw checkpoint parquet and validate fields ===
    let ckpt_batch = read_parquet_file(&ckpt_file);
    let ckpt_schema = ckpt_batch.schema();

    for field_name in [
        "protocol",
        "metaData",
        "checkpointMetadata",
        "domainMetadata",
        "sidecar",
        "txn",
    ] {
        assert!(
            ckpt_schema.field_with_name(field_name).is_ok(),
            "checkpoint should have {field_name}"
        );
    }

    // Validate sidecar actions in the main checkpoint
    let sidecar_col = get_struct_column_from_record_batch(&ckpt_batch, "sidecar");
    let sidecar_rows = valid_row_indices(sidecar_col, ckpt_batch.num_rows());
    // 1 sidecar for the bulk-remove batch (8 removes) + 4 sidecars for adds (2 each)
    assert_eq!(
        sidecar_rows.len(),
        5,
        "checkpoint should contain 5 sidecar action rows"
    );

    assert_sidecar_actions_match_disk(sidecar_col, &sidecar_rows, &table_path, engine.as_ref());

    // Main checkpoint must NOT contain add/remove actions (they live in sidecars)
    for action in ["add", "remove"] {
        let col = get_struct_column_from_record_batch(&ckpt_batch, action);
        let rows = valid_row_indices(col, ckpt_batch.num_rows());
        assert!(
            rows.is_empty(),
            "main checkpoint should not contain {action} actions when sidecars are used, \
             but found {} rows",
            rows.len()
        );
    }

    // Validate sidecar files contain ONLY add/remove file actions
    let sidecars_dir = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    let per_sidecar = assert_sidecars_contain_only_file_actions(&sidecars_dir);
    // 1 sidecar has 8 removes (bulk delete can't be split), 4 sidecars have 2 adds each
    assert_eq!(
        per_sidecar,
        vec![(0, 8), (2, 0), (2, 0), (2, 0), (2, 0)],
        "per-sidecar (adds, removes) distribution"
    );

    // Validate checkpointMetadata action
    let ckpt_meta_col = get_struct_column_from_record_batch(&ckpt_batch, "checkpointMetadata");
    let ckpt_meta_rows = valid_row_indices(ckpt_meta_col, ckpt_batch.num_rows());
    assert_eq!(
        ckpt_meta_rows.len(),
        1,
        "should have exactly one checkpointMetadata action"
    );
    let version_col = ckpt_meta_col
        .column_by_name("version")
        .expect("checkpointMetadata should have version field");
    assert_eq!(
        version_col
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
            .value(ckpt_meta_rows[0]),
        version,
        "checkpointMetadata version should match checkpoint version"
    );

    // Validate domainMetadata actions: exactly 3 (app.settings, app.feature_flags, app.analytics)
    let dm_col = get_struct_column_from_record_batch(&ckpt_batch, "domainMetadata");
    let dm_rows = valid_row_indices(dm_col, ckpt_batch.num_rows());
    assert_eq!(
        dm_rows.len(),
        3,
        "checkpoint should contain exactly 3 domainMetadata actions"
    );

    // Validate txn actions: exactly 2 after reconciliation (app1@3 and app2@5).
    let txn_col = get_struct_column_from_record_batch(&ckpt_batch, "txn");
    let txn_rows = valid_row_indices(txn_col, ckpt_batch.num_rows());
    assert_eq!(
        txn_rows.len(),
        2,
        "checkpoint should contain exactly 2 txn actions after reconciliation"
    );
    let txn_app_id_col = txn_col
        .column_by_name("appId")
        .expect("txn should have appId field")
        .as_string::<i32>();
    let txn_version_col = txn_col
        .column_by_name("version")
        .expect("txn should have version field")
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let mut txn_pairs: Vec<(String, i64)> = txn_rows
        .iter()
        .map(|&r| {
            (
                txn_app_id_col.value(r).to_string(),
                txn_version_col.value(r),
            )
        })
        .collect();
    txn_pairs.sort();
    assert_eq!(
        txn_pairs,
        vec![("app1".to_string(), 3), ("app2".to_string(), 5)],
        "txn actions should reflect reconciliation (app1 kept at latest version)"
    );

    // === Scan from fresh snapshot to verify data correctness ===
    let fresh_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(fresh_snapshot.version(), post_ckpt_snapshot.version());
    // Assert that we are reading from the checkpoint + post-checkpoint commits
    let log_segment = fresh_snapshot.log_segment();
    assert!(
        !log_segment.listed.checkpoint_parts.is_empty(),
        "fresh snapshot should load from checkpoint"
    );
    assert_eq!(log_segment.checkpoint_version, Some(version as u64));
    assert_eq!(
        log_segment.listed.ascending_commit_files.len(),
        2,
        "expected 2 commit files after checkpoint (insert + domain metadata update)"
    );

    // Verify domain metadata: app.settings updated post-checkpoint to {"version":3}
    assert_eq!(
        fresh_snapshot.get_domain_metadata("app.settings", engine.as_ref())?,
        Some(r#"{"version":3}"#.to_string()),
        "app.settings domain metadata should reflect the post-checkpoint update"
    );
    assert_eq!(
        fresh_snapshot.get_domain_metadata("app.feature_flags", engine.as_ref())?,
        Some(r#"{"dark_mode":true}"#.to_string()),
        "app.feature_flags domain metadata should be preserved across checkpoint"
    );
    assert_eq!(
        fresh_snapshot.get_domain_metadata("app.analytics", engine.as_ref())?,
        Some(r#"{"tracking":false}"#.to_string()),
        "app.analytics domain metadata should be preserved across checkpoint"
    );

    let scan = fresh_snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone() as Arc<dyn delta_kernel::Engine>)?;
    assert_batches_sorted_eq!(
        vec![
            "+----+---------------+",
            "| id | info          |",
            "+----+---------------+",
            "| 10 | {name: judy}  |",
            "| 11 | {name: karl}  |",
            "| 12 | {name: lena}  |",
            "| 13 | {name: mike}  |",
            "| 14 | {name: nina}  |",
            "| 15 | {name: omar}  |",
            "| 16 | {name: pat}   |",
            "| 17 | {name: quinn} |",
            "| 9  | {name: ivan}  |",
            "+----+---------------+",
        ],
        &batches
    );

    Ok(())
}

/// Creates a partitioned table with stats (`id: long`, `name: string`, partition `part_key`),
/// writes two commits (partition "a" with 2 rows, partition "b" with 3 rows), and returns
/// the snapshot after both commits.
async fn create_partitioned_stats_table<
    E: delta_kernel::engine::default::executor::TaskExecutor,
>(
    table_path: &str,
    table_url: &url::Url,
    engine: &Arc<delta_kernel::engine::default::DefaultEngine<E>>,
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("part_key", DataType::STRING),
    ])?);

    let _ = create_table(table_path, schema.clone(), "Test/1.0")
        .with_table_properties([
            ("delta.feature.v2Checkpoint", "supported"),
            ("delta.checkpoint.writeStatsAsStruct", "true"),
        ])
        .with_data_layout(DataLayout::partitioned(["part_key"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let data_schema = StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?;
    let arrow_schema = Arc::new(ArrowSchema::try_from_kernel(&data_schema)?);

    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
        ],
    )
    .unwrap();
    let snapshot0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let snapshot1 = write_batch_to_table(
        &snapshot0,
        engine.as_ref(),
        batch1,
        HashMap::from([("part_key".to_string(), Scalar::from("a"))]),
    )
    .await?;

    let batch2 = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("charlie"),
                Some("dave"),
                Some("eve"),
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let snapshot2 = write_batch_to_table(
        &snapshot1,
        engine.as_ref(),
        batch2,
        HashMap::from([("part_key".to_string(), Scalar::from("b"))]),
    )
    .await?;

    Ok(snapshot2)
}

/// E2e test: create a partitioned table with stats, write several commits, checkpoint,
/// then read the raw checkpoint parquet to verify `stats_parsed` and `partitionValues_parsed`
/// fields are correctly populated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_stats_parsed_and_partition_values_parsed(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let snapshot2 = create_partitioned_stats_table(&table_path, &table_url, &engine).await?;

    // Write V2 checkpoint with sidecars at version 2
    let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1),
    });
    snapshot2.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&checkpoint_spec))?;

    // === Validate sidecar structure ===
    let sidecars_dir = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    let per_sidecar = assert_sidecars_contain_only_file_actions(&sidecars_dir);
    // 2 adds with hint=1 -> 2 sidecars with 1 add each
    assert_eq!(
        per_sidecar,
        vec![(1, 0), (1, 0)],
        "per-sidecar (adds, removes) distribution"
    );

    // === Read sidecar files and validate stats_parsed / partitionValues_parsed ===
    let sidecar_files = list_sidecar_parquet_files(&sidecars_dir);

    let mut all_record_counts = Vec::new();
    let mut all_part_values = Vec::new();
    let mut all_min_ids = Vec::new();
    let mut all_max_ids = Vec::new();

    for sidecar_entry in &sidecar_files {
        let sidecar_batch = read_parquet_file(&sidecar_entry.path());
        let add_col = get_struct_column_from_record_batch(&sidecar_batch, "add");
        let add_rows = valid_row_indices(add_col, sidecar_batch.num_rows());
        if add_rows.is_empty() {
            continue;
        }

        let stats_parsed = get_struct_column_from_struct_array(add_col, "stats_parsed");

        let num_records_col = stats_parsed
            .column_by_name("numRecords")
            .expect("stats_parsed should have numRecords");
        for &row in &add_rows {
            all_record_counts.push(
                num_records_col
                    .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
                    .value(row),
            );
        }

        let min_values = get_struct_column_from_struct_array(stats_parsed, "minValues");
        let min_id_col = min_values
            .column_by_name("id")
            .expect("minValues should have id");
        for &row in &add_rows {
            all_min_ids.push(
                min_id_col
                    .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
                    .value(row),
            );
        }

        let max_values = get_struct_column_from_struct_array(stats_parsed, "maxValues");
        let max_id_col = max_values
            .column_by_name("id")
            .expect("maxValues should have id");
        for &row in &add_rows {
            all_max_ids.push(
                max_id_col
                    .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
                    .value(row),
            );
        }

        let pv_parsed = get_struct_column_from_struct_array(add_col, "partitionValues_parsed");
        let part_key_col = pv_parsed
            .column_by_name("part_key")
            .expect("partitionValues_parsed should have part_key field");
        for &row in &add_rows {
            all_part_values.push(part_key_col.as_string::<i32>().value(row).to_string());
        }
    }

    all_record_counts.sort();
    assert_eq!(
        all_record_counts,
        vec![2, 3],
        "stats_parsed.numRecords should be [2, 3] (one per partition)"
    );

    all_min_ids.sort();
    assert_eq!(
        all_min_ids,
        vec![1, 3],
        "stats_parsed.minValues.id should be [1, 3] (min id per partition)"
    );

    all_max_ids.sort();
    assert_eq!(
        all_max_ids,
        vec![2, 5],
        "stats_parsed.maxValues.id should be [2, 5] (max id per partition)"
    );

    all_part_values.sort();
    assert_eq!(
        all_part_values,
        vec!["a", "b"],
        "partitionValues_parsed.part_key should be ['a', 'b']"
    );

    // === Verify scan reads all data correctly after sidecar checkpoint ===
    let snapshot3 = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let scan = snapshot3.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone() as Arc<dyn delta_kernel::Engine>)?;
    assert_batches_sorted_eq!(
        vec![
            "+----+---------+----------+",
            "| id | name    | part_key |",
            "+----+---------+----------+",
            "| 1  | alice   | a        |",
            "| 2  | bob     | a        |",
            "| 3  | charlie | b        |",
            "| 4  | dave    | b        |",
            "| 5  | eve     | b        |",
            "+----+---------+----------+",
        ],
        &batches
    );

    Ok(())
}

/// Parameterized negative tests for writing v2 checkpoint with sidecars.
#[rstest::rstest]
#[case::v2_spec_requires_v2checkpoint_feature(
    false,
    CheckpointSpec::V2(V2CheckpointConfig::NoSidecar),
    "v2Checkpoint"
)]
#[case::v1_rejected_on_v2_table(true, CheckpointSpec::V1, "V1")]
#[case::sidecar_hint_zero_rejected(
    true,
    CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(0),
    }),
    "greater than 0"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_spec_rejected(
    #[case] enable_v2checkpoint: bool,
    #[case] spec: CheckpointSpec,
    #[case] err_substring: &str,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    let mut builder = create_table(&table_path, schema, "Test/1.0");
    if enable_v2checkpoint {
        builder = builder.with_table_properties([("delta.feature.v2Checkpoint", "supported")]);
    }
    let _ = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let result = snapshot.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&spec));
    let err_msg = result.expect_err("spec should be rejected").to_string();
    assert!(
        err_msg.contains(err_substring),
        "error should mention {err_substring:?}, got: {err_msg}"
    );

    Ok(())
}

/// V2 checkpoint with sidecar on a table that has no file actions: the loop writes zero
/// sidecar files, the main checkpoint contains no `sidecar` action rows, and `_last_checkpoint`
/// reports `numOfAddFiles = 0` and `sizeInBytes` equal to the main checkpoint file size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_sidecar_checkpoint_with_no_file_actions() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    // v2 table, no data commits -> only protocol + metadata at version 0.
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.v2Checkpoint", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let version = snapshot.version();

    let spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(2),
    });
    snapshot.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&spec))?;

    // No sidecar files on disk: when there are no file actions, our writer should not even
    // create the `_sidecars` directory.
    let sidecars_dir = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    assert!(
        !sidecars_dir.exists(),
        "_sidecars directory should not exist when there are no file actions, found: {}",
        sidecars_dir.display()
    );

    // Main checkpoint contains no `sidecar` action rows.
    let ckpt_file = load_checkpoint_path(&table_path, version);
    let ckpt_batch = read_parquet_file(&ckpt_file);
    let sidecar_col = get_struct_column_from_record_batch(&ckpt_batch, "sidecar");
    let sidecar_rows = valid_row_indices(sidecar_col, ckpt_batch.num_rows());
    assert!(
        sidecar_rows.is_empty(),
        "main checkpoint should contain no sidecar action rows, found {}",
        sidecar_rows.len()
    );

    // `_last_checkpoint` reflects zero adds and a single-file size.
    let last_ckpt = read_last_checkpoint(&table_path);
    assert_eq!(last_ckpt["numOfAddFiles"], 0);
    let ckpt_file_size = std::fs::metadata(&ckpt_file).unwrap().len() as i64;
    assert_eq!(
        last_ckpt["sizeInBytes"].as_i64().unwrap(),
        ckpt_file_size,
        "sizeInBytes should equal main checkpoint size when there are no sidecars"
    );

    Ok(())
}
