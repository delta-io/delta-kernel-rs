//! AMT (adaptiveMetadata) tests for [`IncrementalScanBuilder`].

use std::collections::HashSet;

use rstest::rstest;

use crate::engine::sync::SyncEngine;
use crate::log_replay::FileActionKey;
use crate::unit_test_utils::adaptive_metadata_fixtures::{TestCommit, table_with_commits};
use crate::{KernelResult, Snapshot, SnapshotRef, Version};

/// The live adds of an incremental scan from `base_version` to `target`, or `None` on fallback.
fn incremental_live_adds(
    engine: &SyncEngine,
    target: SnapshotRef,
    base_version: Version,
) -> KernelResult<Option<HashSet<FileActionKey>>> {
    let stream = target
        .incremental_scan_builder(base_version)
        .build(engine)?;
    Ok(stream.map(|stream| stream.into_summary().unwrap().live_adds))
}

fn file_keys(paths: &[&str]) -> HashSet<FileActionKey> {
    paths
        .iter()
        .map(|path| FileActionKey::new(*path, None))
        .collect()
}

#[rstest]
#[case::content_root_at_base(
    vec![TestCommit::add("a.parquet").manifest_commit(1), TestCommit::add("b.parquet")],
    2,
    1,
    Some(&["b.parquet"][..])
)]
#[case::content_root_at_target(
    vec![TestCommit::add("a.parquet"), TestCommit::add("b.parquet").manifest_commit(2)],
    2,
    1,
    None
)]
#[case::lagging_content_root_at_base(
    vec![TestCommit::add("a.parquet"), TestCommit::add("b.parquet").manifest_commit(1)],
    2,
    1,
    Some(&["b.parquet"][..])
)]
#[case::manifest_commit_after_time_travel_target(
    vec![TestCommit::add("a.parquet"), TestCommit::add("b.parquet").manifest_commit(2)],
    1,
    0,
    Some(&["a.parquet"][..])
)]
fn falls_back_iff_content_root_in_range(
    #[case] commits: Vec<TestCommit>,
    #[case] target_version: Version,
    #[case] base_version: Version,
    #[case] expected_live_adds: Option<&[&str]>,
) {
    let (engine, table_root) = table_with_commits(&commits);
    let target = Snapshot::builder_for(table_root)
        .at_version(target_version)
        .build(&engine)
        .unwrap();

    let live_adds = incremental_live_adds(&engine, target, base_version).unwrap();

    assert_eq!(live_adds, expected_live_adds.map(file_keys));
}
