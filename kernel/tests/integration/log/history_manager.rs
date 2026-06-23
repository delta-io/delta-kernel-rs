//! Integration tests for the public `history_manager` API.

use std::fs::OpenOptions;
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::history_manager::{get_earliest_commit, HistoryCommitType};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStore;
// `ObjectStoreExt` is needed for `store.get()` etc. in arrow-58 mode where these methods moved
// off the `ObjectStore` trait. In arrow-57 mode the compat shim makes the import a no-op, so
// silence the resulting unused-import warning.
#[allow(unused_imports)]
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot, Version};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{LogState, TestTableBuilder, VersionTarget};
use test_utils::{build_snapshot, test_table_setup};
use url::Url;

use crate::common::write_utils::get_simple_int_schema;

async fn remove_commits(store: &dyn ObjectStore, versions: RangeInclusive<Version>) {
    for version in versions {
        let path = Path::from(format!("_delta_log/{version:020}.json"));
        store.delete(&path).await.unwrap();
    }
}

#[rstest]
#[case::recreatable_v0_present(None, None, HistoryCommitType::Recreatable, 0)]
#[case::published_v0_present(None, None, HistoryCommitType::Published, 0)]
#[case::recreatable_checkpoint_anchors(Some(0..=4), None, HistoryCommitType::Recreatable, 5)]
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

/// Verifies that [`VersionTarget::AtTimestamp`] resolves through `build_snapshot!` to the
/// version `latest_version_as_of` selects for a timestamp. `InMemory` collapses successive
/// `put` timestamps into a single millisecond, so this writes on the local filesystem and
/// sets each commit's modification time explicitly to get distinct, monotonic commit
/// timestamps. Resolution correctness across timestamp/commit-layout combinations is covered
/// by the `history_manager` unit tests; this is the end-to-end `TestTableBuilder` check.
#[test]
fn test_at_timestamp_resolves_to_intermediate_version() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CreateTable
    let schema = get_simple_int_schema();
    let mut snap = create_table(&table_path, schema, "AtTimestampTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    // v1..=4: noop commits (each writes a metaData-free, add-free commit JSON).
    for _ in 1..=4 {
        snap = test_utils::begin_transaction(snap.clone(), engine.as_ref())?
            .with_engine_info("AtTimestampTest")
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }

    // Set each commit's mtime to a distinct, monotonic value (in ms).
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let log_dir = table_url.to_file_path().unwrap().join("_delta_log");
    for v in 0..=4u64 {
        let file_path = log_dir.join(format!("{v:020}.json"));
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis((v + 1) * 1000);
        file.set_modified(time).unwrap();
    }

    // 2500ms lands strictly between v1 (2000) and v2 (3000), so it resolves to v1.
    let target = VersionTarget::AtTimestamp(2500);
    let snap_at_ts = build_snapshot!(target, &table_path, engine.as_ref());
    assert_eq!(snap_at_ts.version(), 1);

    // i64::MAX resolves to latest.
    let target_max = VersionTarget::AtTimestamp(i64::MAX);
    let snap_max = build_snapshot!(target_max, &table_path, engine.as_ref());
    assert_eq!(snap_max.version(), 4);

    Ok(())
}
