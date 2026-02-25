use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};

mod common;

use itertools::Itertools;
use test_utils::{insert_data, load_test_data, read_scan, test_table_setup_mt};

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
    snapshot.clone().checkpoint(engine.as_ref())?;

    // Verify the checkpoint was written and is used by a fresh snapshot
    let snapshot2 = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot2.version(), 1);
    let log_segment = snapshot2.log_segment();
    assert!(
        !log_segment.checkpoint_parts.is_empty(),
        "expected snapshot to use the written checkpoint, but checkpoint_parts is empty"
    );
    assert_eq!(
        log_segment.checkpoint_version,
        Some(1),
        "expected checkpoint at version 1"
    );
    assert!(
        log_segment.ascending_commit_files.is_empty(),
        "expected no commit files after checkpoint, but found: {:?}",
        log_segment.ascending_commit_files
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
