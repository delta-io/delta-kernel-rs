use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch};
use delta_kernel::arrow::array::{StringArray, StructArray};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryFromKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

mod common;

use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
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
/// 1. Specific test cases of interest in `delta-spark` were modified to persist their generated tables
/// 2. These tables were compressed into `.tar.zst` archives and copied to delta-kernel-rs
/// 3. Each test loads a stored table, scans it, and asserts that the returned table state
///    matches the expected state derived from the corresponding table insertions in `delta-spark`
///
/// The following is the ported list of `delta-spark` tests -> `delta-kernel-rs` tests:
///
/// - `multipart v2 checkpoint` -> `v2_checkpoints_json_with_sidecars`
/// - `multipart v2 checkpoint` -> `v2_checkpoints_parquet_with_sidecars`
/// - `All actions in V2 manifest` -> `v2_checkpoints_json_without_sidecars`
/// - `All actions in V2 manifest` -> `v2_checkpoints_parquet_without_sidecars`
/// - `V2 Checkpoint compat file equivalency to normal V2 Checkpoint` -> `v2_classic_checkpoint_json`
/// - `V2 Checkpoint compat file equivalency to normal V2 Checkpoint` -> `v2_classic_checkpoint_parquet`
/// - `last checkpoint contains correct schema for v1/v2 Checkpoints` -> `v2_checkpoints_json_with_last_checkpoint`
/// - `last checkpoint contains correct schema for v1/v2 Checkpoints` -> `v2_checkpoints_parquet_with_last_checkpoint`
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
fn find_checkpoint_file(table_path: &str, version: u64) -> std::path::PathBuf {
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

/// E2e test: create a v2 table with domain metadata, write checkpoint with sidecars,
/// validate `_last_checkpoint`, read the raw checkpoint parquet to verify sidecar action
/// rows and non-file actions (protocol, metadata, domainMetadata, checkpointMetadata),
/// then scan to verify data correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_with_domain_metadata_and_sidecars() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    // Create table with v2Checkpoint + domainMetadata features
    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([
            ("delta.feature.v2Checkpoint", "supported"),
            ("delta.feature.domainMetadata", "supported"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Commit 1: insert data
    let snapshot0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let result = insert_data(
        snapshot0,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?;
    let CommitResult::CommittedTransaction(committed) = result else {
        panic!("Expected CommittedTransaction");
    };

    // Commit 2: insert more data with domain metadata
    let snapshot1 = committed
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();
    let mut txn = snapshot1
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_data_change(true);
    txn = txn
        .with_domain_metadata("app.settings".to_string(), r#"{"version":1}"#.to_string())
        .with_domain_metadata(
            "app.feature_flags".to_string(),
            r#"{"dark_mode":true}"#.to_string(),
        );
    let write_context = txn.get_write_context();
    let arrow_schema = ArrowSchema::try_from_kernel(schema.as_ref()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int32Array::from(vec![4, 5])) as ArrayRef],
    )
    .unwrap();
    let add_meta = engine
        .write_parquet(&ArrowEngineData::new(batch), &write_context, HashMap::new())
        .await?;
    txn.add_files(add_meta);
    let result2 = txn.commit(engine.as_ref())?;
    let CommitResult::CommittedTransaction(committed2) = result2 else {
        panic!("Expected CommittedTransaction");
    };
    let snapshot2 = committed2
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // Commit 3: domain metadata only (no data). During reconciliation this batch has no
    // file actions, exercising the zero-selected / empty-file-batch skip path in the
    // sidecar splitter.
    let txn3 = snapshot2
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_domain_metadata(
            "app.analytics".to_string(),
            r#"{"tracking":false}"#.to_string(),
        );
    let result3 = txn3.commit(engine.as_ref())?;
    let CommitResult::CommittedTransaction(committed3) = result3 else {
        panic!("Expected CommittedTransaction");
    };
    let snapshot3 = committed3
        .post_commit_snapshot()
        .expect("expected post-commit snapshot")
        .clone();

    // Write V2 checkpoint with sidecars at version 3
    let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1), // force small sidecars
    });
    snapshot3.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&checkpoint_spec))?;

    // === Validate _last_checkpoint (all kernel-supported fields) ===
    let last_ckpt = read_last_checkpoint(&table_path);
    assert_eq!(
        last_ckpt["version"], 3,
        "_last_checkpoint should be at version 3"
    );
    assert!(
        last_ckpt["size"].as_i64().unwrap() > 0,
        "_last_checkpoint size (action count) should be positive"
    );
    assert!(
        last_ckpt["parts"].is_null(),
        "_last_checkpoint parts should be null for single-file checkpoints"
    );
    // sizeInBytes should match the actual checkpoint file size on disk
    let ckpt_file = find_checkpoint_file(&table_path, 3);
    let ckpt_file_size = std::fs::metadata(&ckpt_file).unwrap().len() as i64;
    assert_eq!(
        last_ckpt["sizeInBytes"].as_i64().unwrap(),
        ckpt_file_size,
        "_last_checkpoint sizeInBytes should match actual checkpoint file size"
    );
    assert_eq!(
        last_ckpt["numOfAddFiles"].as_i64().unwrap(),
        2,
        "_last_checkpoint numOfAddFiles should be 2 (commit 3 has no data, only domain metadata)"
    );

    // === Read raw checkpoint parquet and validate fields ===
    let ckpt_batch = read_parquet_file(&ckpt_file);
    let ckpt_schema = ckpt_batch.schema();

    // Checkpoint should have these top-level action columns
    assert!(
        ckpt_schema.field_with_name("protocol").is_ok(),
        "should have protocol"
    );
    assert!(
        ckpt_schema.field_with_name("metaData").is_ok(),
        "should have metaData"
    );
    assert!(
        ckpt_schema.field_with_name("checkpointMetadata").is_ok(),
        "should have checkpointMetadata (V2)"
    );
    assert!(
        ckpt_schema.field_with_name("domainMetadata").is_ok(),
        "should have domainMetadata"
    );
    assert!(
        ckpt_schema.field_with_name("sidecar").is_ok(),
        "should have sidecar"
    );

    // Validate sidecar actions exist in the main checkpoint (file actions are in sidecars)
    let sidecar_col = ckpt_batch
        .column_by_name("sidecar")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let sidecar_rows: Vec<usize> = (0..ckpt_batch.num_rows())
        .filter(|&i| sidecar_col.is_valid(i))
        .collect();
    assert!(
        !sidecar_rows.is_empty(),
        "checkpoint should contain sidecar action rows referencing sidecar files"
    );

    // Validate sidecar action has path field
    let sidecar_path_col = sidecar_col
        .column_by_name("path")
        .expect("sidecar should have path field");
    for &row in &sidecar_rows {
        let path = sidecar_path_col.as_string::<i32>().value(row);
        assert!(
            path.ends_with(".parquet"),
            "sidecar path should be a parquet file, got: {path}"
        );
    }

    // Validate sidecar action sizeInBytes and modificationTime match actual files on disk
    let sidecar_size_col = sidecar_col
        .column_by_name("sizeInBytes")
        .expect("sidecar should have sizeInBytes field");
    let sidecar_mtime_col = sidecar_col
        .column_by_name("modificationTime")
        .expect("sidecar should have modificationTime field");
    let sidecars_dir = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    for &row in &sidecar_rows {
        let sidecar_path_str = sidecar_path_col.as_string::<i32>().value(row);
        let sidecar_file_path = sidecars_dir.join(sidecar_path_str);
        let file_meta = std::fs::metadata(&sidecar_file_path).unwrap_or_else(|e| {
            panic!("sidecar file {sidecar_path_str} should exist on disk: {e}")
        });

        let recorded_size = sidecar_size_col
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
            .value(row);
        assert_eq!(
            recorded_size,
            file_meta.len() as i64,
            "sidecar sizeInBytes should match actual file size for {sidecar_path_str}"
        );

        let recorded_mtime = sidecar_mtime_col
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
            .value(row);
        let file_mtime_ms = file_meta
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert_eq!(
            recorded_mtime, file_mtime_ms,
            "sidecar modificationTime should match actual file mtime for {sidecar_path_str}"
        );
    }

    // Main checkpoint must NOT contain add/remove actions (they live in sidecars)
    let add_col = ckpt_batch
        .column_by_name("add")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let add_rows_in_main: Vec<usize> = (0..ckpt_batch.num_rows())
        .filter(|&i| add_col.is_valid(i))
        .collect();
    assert!(
        add_rows_in_main.is_empty(),
        "main checkpoint should not contain add actions when sidecars are used, \
         but found {} add rows",
        add_rows_in_main.len()
    );
    let remove_col = ckpt_batch
        .column_by_name("remove")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let remove_rows_in_main: Vec<usize> = (0..ckpt_batch.num_rows())
        .filter(|&i| remove_col.is_valid(i))
        .collect();
    assert!(
        remove_rows_in_main.is_empty(),
        "main checkpoint should not contain remove actions when sidecars are used, \
         but found {} remove rows",
        remove_rows_in_main.len()
    );

    // Validate sidecar files contain ONLY add/remove file actions
    let sidecar_files: Vec<_> = std::fs::read_dir(&sidecars_dir)
        .expect("failed to list sidecars dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .collect();
    assert!(
        !sidecar_files.is_empty(),
        "should have at least one sidecar parquet file in _delta_log/_sidecars/"
    );

    // Read each sidecar parquet and verify it contains file actions
    let mut total_add_rows_in_sidecars = 0usize;
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
                // Field may exist in schema but all rows must be null
                let col = sidecar_batch.column_by_name(forbidden).unwrap();
                let non_null_count = (0..sidecar_batch.num_rows())
                    .filter(|&i| col.is_valid(i))
                    .count();
                assert_eq!(
                    non_null_count, 0,
                    "sidecar should not contain non-null {forbidden} actions, \
                     but found {non_null_count} (field type: {field:?})"
                );
            }
        }

        // Count add rows
        if let Some(sidecar_add_col) = sidecar_batch.column_by_name("add") {
            let sidecar_add = sidecar_add_col
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            total_add_rows_in_sidecars += (0..sidecar_batch.num_rows())
                .filter(|&i| sidecar_add.is_valid(i))
                .count();
        }
    }
    assert!(
        total_add_rows_in_sidecars > 0,
        "sidecar files should contain add actions (the file actions from the table)"
    );

    // Validate checkpointMetadata action
    let ckpt_meta_col = ckpt_batch
        .column_by_name("checkpointMetadata")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let ckpt_meta_rows: Vec<usize> = (0..ckpt_batch.num_rows())
        .filter(|&i| ckpt_meta_col.is_valid(i))
        .collect();
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
        3,
        "checkpointMetadata version should match checkpoint version"
    );

    // Validate domainMetadata actions: exactly 3 (app.settings, app.feature_flags, app.analytics)
    let dm_col = ckpt_batch
        .column_by_name("domainMetadata")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let dm_rows: Vec<usize> = (0..ckpt_batch.num_rows())
        .filter(|&i| dm_col.is_valid(i))
        .collect();
    assert_eq!(
        dm_rows.len(),
        3,
        "checkpoint should contain exactly 3 domainMetadata actions"
    );

    // === Scan from fresh snapshot to verify data correctness ===
    let fresh_snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(fresh_snapshot.version(), 3);

    // Verify all 3 domain metadata are retrievable after checkpoint
    assert_eq!(
        fresh_snapshot.get_domain_metadata("app.settings", engine.as_ref())?,
        Some(r#"{"version":1}"#.to_string()),
        "app.settings domain metadata should be preserved across checkpoint"
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
            "+-------+",
            "| value |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "| 4     |",
            "| 5     |",
            "+-------+",
        ],
        &batches
    );

    Ok(())
}

/// E2e test: create a partitioned table with stats, write several commits, checkpoint,
/// then read the raw checkpoint parquet to verify `stats_parsed` and `partitionValues_parsed`
/// fields are correctly populated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_stats_parsed_and_partition_values_parsed(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
        StructField::nullable("part_key", DataType::STRING),
    ])?);

    // Create table with v2Checkpoint, partition column, and writeStatsAsStruct=true
    let _ = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([
            ("delta.feature.v2Checkpoint", "supported"),
            ("delta.checkpoint.writeStatsAsStruct", "true"),
        ])
        .with_data_layout(DataLayout::partitioned(["part_key"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Commit 1: write data with partition key "a"
    let snapshot0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let data_schema = StructType::try_new(vec![
        StructField::not_null("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?;
    let arrow_schema = ArrowSchema::try_from_kernel(&data_schema)?;
    let batch1 = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
        ],
    )
    .unwrap();
    let snapshot1 = write_batch_to_table(
        &snapshot0,
        engine.as_ref(),
        batch1,
        HashMap::from([("part_key".to_string(), "a".to_string())]),
    )
    .await?;

    // Commit 2: write data with partition key "b"
    let batch2 = RecordBatch::try_new(
        Arc::new(arrow_schema),
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
        HashMap::from([("part_key".to_string(), "b".to_string())]),
    )
    .await?;

    // Write V2 checkpoint with sidecars at version 2
    let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(1),
    });
    snapshot2.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&checkpoint_spec))?;

    // === Read sidecar files and validate stats_parsed / partitionValues_parsed ===
    let sidecars_dir = std::path::Path::new(&table_path).join("_delta_log/_sidecars");
    let sidecar_files: Vec<_> = std::fs::read_dir(&sidecars_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .collect();
    assert!(
        !sidecar_files.is_empty(),
        "should have sidecar files for stats/partition validation"
    );

    // Collect add actions across all sidecar files
    let mut all_record_counts = Vec::new();
    let mut all_part_values = Vec::new();
    let mut found_min_values = false;
    let mut found_max_values = false;

    for sidecar_entry in &sidecar_files {
        let sidecar_batch = read_parquet_file(&sidecar_entry.path());
        let Some(add_array) = sidecar_batch.column_by_name("add") else {
            continue;
        };
        let add_col = add_array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add should be a struct");
        let add_rows: Vec<usize> = (0..sidecar_batch.num_rows())
            .filter(|&i| add_col.is_valid(i))
            .collect();
        if add_rows.is_empty() {
            continue;
        }

        // Validate stats_parsed in sidecar
        let stats_parsed = add_col
            .column_by_name("stats_parsed")
            .expect("sidecar add should have stats_parsed when writeStatsAsStruct=true")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("stats_parsed should be a struct");

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
        if stats_parsed.column_by_name("minValues").is_some() {
            found_min_values = true;
        }
        if stats_parsed.column_by_name("maxValues").is_some() {
            found_max_values = true;
        }

        // Validate partitionValues_parsed in sidecar
        let pv_parsed = add_col
            .column_by_name("partitionValues_parsed")
            .expect("sidecar add should have partitionValues_parsed")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("partitionValues_parsed should be a struct");

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
        "sidecar stats_parsed.numRecords should be [2, 3] (one per partition)"
    );
    assert!(
        found_min_values,
        "sidecar stats_parsed should have minValues"
    );
    assert!(
        found_max_values,
        "sidecar stats_parsed should have maxValues"
    );

    all_part_values.sort();
    assert_eq!(
        all_part_values,
        vec!["a", "b"],
        "sidecar partitionValues_parsed.part_key should be ['a', 'b']"
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

/// V2 checkpoint spec requires the v2Checkpoint table feature to be enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v2_checkpoint_spec_requires_v2checkpoint_feature() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    // Create table WITHOUT v2Checkpoint feature
    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // Attempting V2 checkpoint on a table without the feature should fail
    let spec = CheckpointSpec::V2(V2CheckpointConfig::NoSidecar);
    let result = snapshot.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&spec));
    assert!(
        result.is_err(),
        "V2 spec on table without v2Checkpoint feature should fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("v2Checkpoint"),
        "error should mention v2Checkpoint feature, got: {err_msg}"
    );

    Ok(())
}

/// file_actions_per_sidecar_hint of 0 is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sidecar_hint_zero_rejected() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.v2Checkpoint", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
        file_actions_per_sidecar_hint: Some(0),
    });
    let result = snapshot.snapshot_checkpoint_placeholder(engine.as_ref(), Some(&spec));
    assert!(result.is_err(), "hint=0 should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("greater than 0"),
        "error should mention hint must be > 0, got: {err_msg}"
    );

    Ok(())
}
