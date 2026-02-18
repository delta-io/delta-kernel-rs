use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

mod common;

use itertools::Itertools;
use object_store::local::LocalFileSystem;
use test_utils::{load_test_data, read_scan};

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
    let temp_dir = tempfile::tempdir().map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
    let table_path = temp_dir
        .path()
        .to_str()
        .ok_or_else(|| delta_kernel::Error::generic("Invalid path"))?
        .to_string();
    let store = Arc::new(LocalFileSystem::new());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(executor)
            .build(),
    );
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::INTEGER,
    )])?);
    let _ = create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.v2Checkpoint", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Commit an add action via the transaction API so the checkpoint has action + metadata batches
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("INSERT".to_string())
        .with_engine_info("Test/1.0")
        .with_data_change(true);
    let write_context = Arc::new(txn.get_write_context());
    let arrow_schema: ArrowSchema = write_context.physical_schema().as_ref().try_into_arrow()?;
    let data = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let add_files_metadata = engine
        .write_parquet(
            &ArrowEngineData::new(data),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;
    txn.add_files(add_files_metadata);
    let result = txn.commit(engine.as_ref())?;
    assert!(matches!(result, CommitResult::CommittedTransaction(_)));

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;

    // This writes to parquet — will fail if the checkpointMetadata batch has a different
    // schema than the action batches.
    snapshot.checkpoint(engine.as_ref())?;

    // Verify the checkpoint was written and is readable
    let snapshot2 = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot2.version(), 1);

    Ok(())
}
