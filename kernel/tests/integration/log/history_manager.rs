//! Integration tests for the public `history_manager` earliest-commit API.
//!
//! These exercise `get_earliest_commit` end-to-end against real kernel-written logs (commit
//! JSON written via the transaction path, plus a real checkpoint parquet written by
//! `Snapshot::checkpoint`), simulating log cleanup by deleting commit files on disk. The
//! internal helpers it dispatches to are unit-tested exhaustively against synthetic listings;
//! the goal here is to prove the public surface works on real tables and that the
//! `must_be_recreatable` switch routes correctly after realistic cleanup.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::history_manager::get_earliest_commit;
use delta_kernel::Version;
use rstest::rstest;
use test_utils::{create_table_and_load_snapshot, test_table_setup_mt, write_batch_to_table};
use url::Url;

use crate::common::write_utils::{get_simple_schema, simple_id_batch};

type TestEngine = DefaultEngine<TokioMultiThreadExecutor>;

/// Builds a table with commits v0..=v8 and a checkpoint at v5. Returns the temp dir (keeps the
/// on-disk table alive), the table URL, and the engine.
async fn build_table(
) -> Result<(tempfile::TempDir, Url, Arc<TestEngine>), Box<dyn std::error::Error>> {
    let schema = get_simple_schema();
    let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;

    let mut snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;

    for version in 1..=8 {
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            simple_id_batch(&schema, vec![version]),
            HashMap::new(),
        )
        .await?;
        if version == 5 {
            (_, snapshot) = snapshot.checkpoint(engine.as_ref(), None)?;
        }
    }

    Ok((tmp_dir, table_url, engine))
}

/// Deletes the published commit JSON files for `versions`, simulating log cleanup.
fn remove_commits(table_url: &Url, versions: RangeInclusive<Version>) {
    let log_dir = table_url
        .join("_delta_log/")
        .unwrap()
        .to_file_path()
        .unwrap();
    for version in versions {
        std::fs::remove_file(log_dir.join(format!("{version:020}.json"))).unwrap();
    }
}

// Table for all cases: commits v0..=v8 with a checkpoint at v5.
//   - no cleanup: v0 present, so both modes anchor at 0
//   - cleanup v0..=v5 (commit at checkpoint version gone): recreatable falls back to the checkpoint
//     (5), published reports the lowest surviving JSON commit (6)
//   - cleanup v0..=v4 (commit at checkpoint version survives): both report 5
//   - earliest_ratified_commit_version is plumbed through but irrelevant while commits exist
#[rstest]
#[case::recreatable_v0_present(None, None, true, 0)]
#[case::published_v0_present(None, None, false, 0)]
#[case::recreatable_checkpoint_anchors(Some(0..=5), None, true, 5)]
#[case::published_lowest_surviving_json(Some(0..=5), None, false, 6)]
#[case::recreatable_commit_at_checkpoint_survives(Some(0..=4), None, true, 5)]
#[case::published_commit_at_checkpoint_survives(Some(0..=4), None, false, 5)]
#[case::recreatable_ratified_zero_passthrough(None, Some(0), true, 0)]
#[case::published_ratified_zero_passthrough(None, Some(0), false, 0)]
#[tokio::test(flavor = "multi_thread")]
async fn test_get_earliest_commit(
    #[case] cleanup: Option<RangeInclusive<Version>>,
    #[case] earliest_ratified_commit_version: Option<Version>,
    #[case] must_be_recreatable: bool,
    #[case] expected_version: Version,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_url, engine) = build_table().await?;
    if let Some(versions) = cleanup {
        remove_commits(&table_url, versions);
    }
    let log_root = table_url.join("_delta_log/")?;

    let earliest = get_earliest_commit(
        engine.as_ref(),
        &log_root,
        earliest_ratified_commit_version,
        must_be_recreatable,
    )?;
    assert_eq!(earliest, expected_version);
    Ok(())
}
