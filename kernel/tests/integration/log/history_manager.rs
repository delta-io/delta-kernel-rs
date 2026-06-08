//! Integration tests for the public `history_manager` API.

use std::ops::RangeInclusive;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::history_manager::{get_earliest_commit, HistoryCommitType};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectStore, ObjectStoreExt as _};
use delta_kernel::Version;
use rstest::rstest;
use test_utils::table_builder::{LogState, TestTableBuilder};
use url::Url;

async fn remove_commits(store: &dyn ObjectStore, versions: RangeInclusive<Version>) {
    for version in versions {
        let path = Path::from(format!("_delta_log/{version:020}.json"));
        store.delete(&path).await.unwrap();
    }
}

#[rstest]
#[case::recreatable_v0_present(None, None, HistoryCommitType::Recreatable, 0)]
#[case::published_v0_present(None, None, HistoryCommitType::Published, 0)]
#[case::recreatable_checkpoint_anchors(Some(0..=5), None, HistoryCommitType::Recreatable, 5)]
#[case::published_lowest_surviving_json(Some(0..=5), None, HistoryCommitType::Published, 6)]
#[case::recreatable_commit_at_checkpoint_survives(Some(0..=4), Some(9), HistoryCommitType::Recreatable, 5)]
#[case::published_commit_at_checkpoint_survives(Some(0..=4), Some(9), HistoryCommitType::Published, 5)]
#[tokio::test(flavor = "multi_thread")]
async fn test_get_earliest_commit(
    #[case] cleanup: Option<RangeInclusive<Version>>,
    #[case] earliest_ratified_commit_version: Option<Version>,
    #[case] commit_type: HistoryCommitType,
    #[case] expected_version: Version,
) -> Result<(), Box<dyn std::error::Error>> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(8).with_checkpoint_at([5]))
        .build()?;
    let store = table.store().clone();
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    let log_root = Url::parse(table.table_root())?.join("_delta_log/")?;

    if let Some(versions) = cleanup {
        remove_commits(store.as_ref(), versions).await;
    }

    let earliest = get_earliest_commit(
        &engine,
        &log_root,
        earliest_ratified_commit_version,
        commit_type,
    )?;
    assert_eq!(earliest, expected_version);
    Ok(())
}
