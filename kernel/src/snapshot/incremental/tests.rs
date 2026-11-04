//! Contracts for selecting retained log files and resolving snapshot state independently.
//!
//! Path-only tests call `LogUpdate::from_listing` to isolate file selection and continuity checks.
//! Builder tests use real table histories to check resolved state and which files the engine reads.

use std::sync::Arc;

use rstest::rstest;
use serde_json::json;
use test_utils::table_builder::{LogState, TestTableBuilder};
use test_utils::{add_commit, add_staged_commit};
use url::Url;

use super::log_update::{LogUpdate, StateBaseline, listing_start};
use crate::engine::sync::SyncEngine;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::ParsedLogPath;
use crate::path::tests::parse_log_path;
use crate::snapshot::builder::tests::catalog_managed_tests::{
    create_log_path, setup_catalog_managed_test,
};
use crate::snapshot::{IncrementalReplay, Snapshot};
use crate::unit_test_utils::recording_reads::{Read, RecordingReads};
use crate::{KernelError, Version};

#[rstest]
#[case::no_checkpoint(None, 3, CheckpointHandling::Adopt, Some(1))]
#[case::discover_after_checkpoint(Some(1), 3, CheckpointHandling::Adopt, Some(2))]
#[case::retain_history(Some(1), 3, CheckpointHandling::Ignore, Some(4))]
#[case::retain_without_checkpoint(None, 3, CheckpointHandling::Ignore, Some(4))]
#[case::maximum_snapshot(None, Version::MAX, CheckpointHandling::Ignore, None)]
#[case::maximum_checkpoint(Some(Version::MAX), Version::MAX, CheckpointHandling::Adopt, None)]
fn listing_bounds_follow_checkpoint_policy(
    #[case] checkpoint: Option<Version>,
    #[case] version: Version,
    #[case] policy: CheckpointHandling,
    #[case] expected: Option<Version>,
) {
    let commits: Vec<_> = match checkpoint {
        Some(checkpoint) if checkpoint == version => vec![],
        Some(checkpoint) => (checkpoint + 1..=version).collect(),
        None => vec![version],
    };
    let existing = segment(listed(&commits, checkpoint, None));
    assert_eq!(listing_start(&existing, policy), expected);
}

#[rstest]
#[case::already_current(&[1, 2, 3], None, None, &[1, 2, 3])]
#[case::commits_advance(&[1, 2, 3, 4], None, Some(StateBaseline::ExistingSnapshot), &[1, 2, 3, 4])]
#[case::cached_commits_fill_listing_gap(&[1, 4], None, Some(StateBaseline::ExistingSnapshot), &[1, 2, 3, 4])]
#[case::checkpoint_below_snapshot(&[2, 3], Some(1), Some(StateBaseline::ExistingSnapshot), &[2, 3])]
#[case::checkpoint_at_snapshot(&[], Some(3), Some(StateBaseline::ExistingSnapshot), &[])]
#[case::checkpoint_and_commits(&[3, 4], Some(2), Some(StateBaseline::ExistingSnapshot), &[3, 4])]
#[case::cached_commits_fill_checkpoint_gap(&[4], Some(1), Some(StateBaseline::ExistingSnapshot), &[2, 3, 4])]
#[case::checkpoint_ahead(&[], Some(4), Some(StateBaseline::NewCheckpoint), &[])]
#[case::commits_after_new_checkpoint(&[5], Some(4), Some(StateBaseline::NewCheckpoint), &[5])]
fn checkpoint_position_selects_state_baseline_and_retained_files(
    #[case] commits: &[Version],
    #[case] checkpoint: Option<Version>,
    #[case] expected_baseline: Option<StateBaseline>,
    #[case] expected_commits: &[Version],
) {
    let mut existing = segment(listed(&[1, 2, 3], Some(0), None));
    // Distinguish cached metadata from the listing so replacing a retained path fails the
    // assertion.
    existing.listed.ascending_commit_files[2].location.size = 999;
    let update =
        LogUpdate::from_listing(&existing, listed(commits, checkpoint, None), None).unwrap();

    match update {
        None => assert_eq!(expected_baseline, None),
        Some(LogUpdate { segment, baseline }) => {
            assert_eq!(Some(baseline), expected_baseline);
            assert_eq!(commit_versions(&segment), expected_commits);
            assert_eq!(segment.checkpoint_version, checkpoint.or(Some(0)));
            if expected_commits.contains(&3) {
                let retained = segment
                    .listed
                    .ascending_commit_files
                    .iter()
                    .find(|p| p.version == 3);
                assert_eq!(retained, existing.listed.ascending_commit_files.last());
            }
        }
    }
}

#[rstest]
#[case::latest(None)]
#[case::unavailable_exact_version(Some(4))]
fn unchanged_history_distinguishes_latest_from_required_version(
    #[case] target: Option<Version>,
    #[values(false, true)] relisted_cached_commits: bool,
) {
    let existing = segment(listed(&[0, 1, 2, 3], None, None));
    let discovered = if relisted_cached_commits {
        listed(&[1, 3], None, None)
    } else {
        LogSegmentFiles::default()
    };
    let result = LogUpdate::from_listing(&existing, discovered, target);
    if target.is_some() {
        assert!(matches!(result, Err(KernelError::MissingVersion(4))));
    } else {
        assert!(matches!(result, Ok(None)));
    }
}

#[rstest]
#[case::gap_in_listing(&[4, 6], None, 5)]
#[case::gap_between_snapshot_and_listing(&[5], None, 4)]
#[case::gap_after_adopted_checkpoint(&[5], Some(2), 4)]
#[case::gap_after_rebuild_checkpoint(&[6], Some(4), 5)]
#[case::gap_in_rebuild_commits(&[5, 7], Some(4), 6)]
fn missing_commits_cannot_be_hidden_by_retained_history(
    #[case] commits: &[Version],
    #[case] checkpoint: Option<Version>,
    #[case] missing_version: Version,
) {
    let existing = segment(listed(&[1, 2, 3], Some(0), None));
    let result = LogUpdate::from_listing(&existing, listed(commits, checkpoint, None), None);
    assert!(matches!(result, Err(KernelError::MissingVersion(v)) if v == missing_version));
}

#[test]
fn listing_cannot_move_snapshot_backward() {
    let existing = segment(listed(&[1, 2, 3], Some(0), None));
    let error = LogUpdate::from_listing(&existing, listed(&[1, 2], None, None), None)
        .expect_err("listing regression must fail");
    assert!(
        error
            .to_string()
            .contains("older than the existing snapshot")
    );
}

#[rstest]
#[case::keep_checkpoint_and_crc(None, None, Some(1))]
#[case::crc_equal_to_new_checkpoint(Some(1), None, Some(1))]
#[case::drop_crc_below_new_checkpoint(Some(2), None, None)]
#[case::prefer_new_crc(Some(2), Some(4), Some(4))]
fn checkpoint_adoption_controls_crc_and_hint_retention(
    #[case] checkpoint: Option<Version>,
    #[case] new_crc: Option<Version>,
    #[case] expected_crc: Option<Version>,
) {
    let mut existing = segment(listed(&[1, 2, 3], Some(0), Some(1)));
    existing.last_checkpoint_metadata = Some(LastCheckpointHint {
        version: 0,
        size: 10,
        ..Default::default()
    });
    let first_commit = checkpoint.map_or(1, |v| v + 1);
    let commits: Vec<_> = (first_commit..=4).collect();
    let update =
        LogUpdate::from_listing(&existing, listed(&commits, checkpoint, new_crc), Some(4)).unwrap();
    let Some(LogUpdate { segment, .. }) = update else {
        panic!("new commit must produce a changed segment");
    };
    assert_eq!(
        segment.listed.latest_crc_file.as_ref().map(|p| p.version),
        expected_crc
    );
    assert_eq!(
        segment.last_checkpoint_metadata,
        checkpoint
            .is_none()
            .then(|| { existing.last_checkpoint_metadata.clone().unwrap() })
    );
    assert_eq!(
        segment
            .listed
            .latest_commit_file
            .as_ref()
            .map(|p| p.version),
        Some(4)
    );
}

#[rstest]
fn catalog_updates_preserve_cached_paths_and_publication_watermark(
    #[values(0, 1, 2, 3)] published_through: Version,
) {
    let root = Url::parse("memory:///").unwrap();
    let cached_tail: Vec<_> = (1..=2)
        .map(|v| ParsedLogPath::create_parsed_staged_commit(&root, v))
        .collect();
    let mut cached_files = listed(&[0], None, None);
    cached_files
        .ascending_commit_files
        .extend(cached_tail.clone());
    cached_files.latest_commit_file = cached_tail.last().cloned();
    let existing = segment(cached_files);
    let new_tail: Vec<_> = (3..=4)
        .map(|v| ParsedLogPath::create_parsed_staged_commit(&root, v))
        .collect();
    let filesystem = (1..=published_through)
        .map(|v| Ok(ParsedLogPath::create_parsed_published_commit(&root, v)));
    let discovered = LogSegmentFiles::build_log_segment_files(
        filesystem,
        new_tail.clone(),
        1,
        Some(4),
        CheckpointHandling::Adopt,
    )
    .unwrap();
    let Some(LogUpdate { segment, baseline }) =
        LogUpdate::from_listing(&existing, discovered, Some(4)).unwrap()
    else {
        panic!("catalog tail advances the snapshot");
    };
    assert_eq!(baseline, StateBaseline::ExistingSnapshot);
    assert_eq!(&segment.listed.ascending_commit_files[1..3], cached_tail);
    assert_eq!(&segment.listed.ascending_commit_files[3..], new_tail);
    assert_eq!(
        segment.listed.max_published_version,
        Some(published_through)
    );
}

#[rstest]
#[case::same_version(1)]
#[case::older_version(0)]
fn exact_target_at_or_before_snapshot_never_reads_storage(
    #[case] target: Version,
    #[values(false, true)] skip_checkpoints: bool,
) {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(2))
        .build()
        .unwrap();
    let inner = Arc::new(SyncEngine::new_with_store(table.store().clone()));
    let base = Snapshot::builder_for(table.table_root())
        .at_version(1)
        .build(inner.as_ref())
        .unwrap();
    let mut reads = RecordingReads::new(None);
    let recorder = Arc::get_mut(&mut reads).unwrap();
    recorder.inner = inner;
    recorder.expected_token = None;
    let builder = Snapshot::builder_from(base.clone()).at_version(target);
    let builder = if skip_checkpoints {
        builder.skip_new_checkpoints()
    } else {
        builder
    };
    let result = builder.build(&reads.engine());
    if target == 1 {
        let snapshot = result.unwrap();
        assert!(Arc::ptr_eq(&snapshot, &base));
        assert!(!snapshot.skipped_new_checkpoints());
        assert!(!snapshot.is_built_as_latest());
    } else {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("older than snapshot hint version")
        );
    }
    assert!(reads.events.lock().unwrap().is_empty());
}

#[rstest]
#[case::checkpoint_below_snapshot(1, false)]
#[case::checkpoint_at_snapshot(3, false)]
#[case::checkpoint_ahead_of_snapshot(4, true)]
fn checkpoint_reads_depend_on_state_baseline(
    #[case] checkpoint: Version,
    #[case] reads_checkpoint: bool,
) {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(5))
        .build()
        .unwrap();
    let inner = Arc::new(SyncEngine::new_with_store(table.store().clone()));
    let base = Snapshot::builder_for(table.table_root())
        .at_version(3)
        .build(inner.as_ref())
        .unwrap();
    Snapshot::builder_for(table.table_root())
        .at_version(checkpoint)
        .build(inner.as_ref())
        .unwrap()
        .checkpoint(inner.as_ref(), None)
        .unwrap();
    let mut reads = RecordingReads::new(None);
    let recorder = Arc::get_mut(&mut reads).unwrap();
    recorder.inner = inner;
    recorder.expected_token = None;
    let updated = Snapshot::builder_from(base).build(&reads.engine()).unwrap();
    assert_eq!(updated.version(), 5);
    assert_eq!(updated.log_segment.checkpoint_version, Some(checkpoint));
    assert_eq!(
        reads.events.lock().unwrap().contains(&Read::Parquet),
        reads_checkpoint
    );
    let fresh = Snapshot::builder_for(table.table_root())
        .build(reads.inner.as_ref())
        .unwrap();
    assert_eq!(updated.table_configuration(), fresh.table_configuration());
}

#[rstest]
#[case::stale_crc_exceeds_budget(
    0,
    IncrementalReplay::Disabled,
    IncrementalReplay::UpToCommits(2),
    0
)]
#[case::in_memory_crc_fits_budget(
    0,
    IncrementalReplay::Unlimited,
    IncrementalReplay::UpToCommits(2),
    4
)]
#[case::new_disk_crc_fits_budget(
    3,
    IncrementalReplay::Disabled,
    IncrementalReplay::UpToCommits(1),
    4
)]
#[case::new_disk_crc_seeds_pm(3, IncrementalReplay::Disabled, IncrementalReplay::Disabled, 3)]
fn crc_budget_starts_at_selected_crc_version(
    #[case] disk_crc: Version,
    #[case] initial_replay: IncrementalReplay,
    #[case] update_replay: IncrementalReplay,
    #[case] expected_crc: Version,
) {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4).with_crc_at([disk_crc]))
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let base = Snapshot::builder_for(table.table_root())
        .at_version(2)
        .with_incremental_crc_replay(initial_replay)
        .build(&engine)
        .unwrap();
    let updated = Snapshot::builder_from(base)
        .with_incremental_crc_replay(update_replay)
        .build(&engine)
        .unwrap();
    assert_eq!(updated.version(), 4);
    assert_eq!(updated.base_crc().unwrap().version, expected_crc);
    assert_eq!(
        updated.crc_at_version().map(|crc| crc.version),
        (expected_crc == 4).then_some(4)
    );
    let fresh = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(updated.table_configuration(), fresh.table_configuration());
}

#[rstest]
#[case::preserve_existing_metadata(0, None, "before")]
#[case::replay_metadata_after_snapshot(0, Some(3), "after")]
#[case::use_metadata_from_newer_crc(3, Some(3), "after")]
#[case::replay_metadata_after_newer_crc(3, Some(4), "after")]
#[tokio::test]
async fn metadata_uses_the_newest_state_with_or_without_crc_replay(
    #[case] disk_crc: Version,
    #[case] metadata_change: Option<Version>,
    #[case] expected_value: &str,
    #[values(IncrementalReplay::Disabled, IncrementalReplay::Unlimited)] replay: IncrementalReplay,
) {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(0).with_crc_at([0]))
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let original = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let before = original
        .table_configuration()
        .metadata()
        .clone()
        .with_configuration_entry("test.state", "before");
    let after = before
        .clone()
        .with_configuration_entry("test.state", "after");

    // The retained CRC predates the metadata already resolved by the input snapshot.
    for (version, content) in [(1, json!({"metaData": before})), (2, json!({}))] {
        add_commit(
            table.table_root(),
            table.store().as_ref(),
            version,
            content.to_string(),
        )
        .await
        .unwrap();
    }
    let base = Snapshot::builder_for(table.table_root())
        .at_version(2)
        .build(&engine)
        .unwrap();
    assert_eq!(base.metadata_configuration()["test.state"], "before");

    for version in 3..=4 {
        let content = if metadata_change == Some(version) {
            json!({"metaData": after})
        } else {
            json!({})
        };
        add_commit(
            table.table_root(),
            table.store().as_ref(),
            version,
            content.to_string(),
        )
        .await
        .unwrap();
        if version == disk_crc {
            Snapshot::builder_for(table.table_root())
                .with_incremental_crc_replay(IncrementalReplay::Unlimited)
                .build(&engine)
                .unwrap()
                .write_checksum(&engine)
                .unwrap();
        }
    }
    let updated = Snapshot::builder_from(base)
        .with_incremental_crc_replay(replay)
        .build(&engine)
        .unwrap();
    assert_eq!(
        updated.metadata_configuration()["test.state"],
        expected_value
    );
    assert_eq!(
        updated.crc_at_version().map(|crc| crc.version),
        (replay == IncrementalReplay::Unlimited).then_some(4)
    );
    let fresh = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    assert_eq!(updated.table_configuration(), fresh.table_configuration());
}

#[rstest]
#[case::time_travel(Some(3), 3, false)]
#[case::catalog_latest(None, 4, true)]
#[case::exact_catalog_latest(Some(4), 4, true)]
#[tokio::test]
async fn catalog_tail_respects_target_and_publication_watermark(
    #[case] target: Option<Version>,
    #[case] expected_version: Version,
    #[case] expected_latest: bool,
    #[values(0, 1, 2, 3, 4)] published_through: Version,
) {
    let (engine, store, root) = setup_catalog_managed_test().await;
    let mut cached_tail = Vec::new();
    for version in 1..=2 {
        let path = add_staged_commit(&root, store.as_ref(), version, "{}".into())
            .await
            .unwrap();
        cached_tail.push(create_log_path(&root, path));
    }
    let base = Snapshot::builder_for(&root)
        .with_log_tail(cached_tail)
        .with_max_catalog_version(2)
        .build(engine.as_ref())
        .unwrap();
    let mut tail = Vec::new();
    for version in 3..=4 {
        let path = add_staged_commit(&root, store.as_ref(), version, "{}".into())
            .await
            .unwrap();
        tail.push(create_log_path(&root, path));
    }
    for version in 1..=published_through {
        add_commit(&root, store.as_ref(), version, "{}".into())
            .await
            .unwrap();
    }
    let builder = Snapshot::builder_from(base)
        .with_log_tail(tail)
        .with_max_catalog_version(4);
    let builder = if let Some(target) = target {
        builder.at_version(target)
    } else {
        builder
    };
    let updated = builder.build(engine.as_ref()).unwrap();
    assert_eq!(updated.version(), expected_version);
    assert_eq!(updated.is_built_as_latest(), expected_latest);
    assert_eq!(
        updated.log_segment.listed.max_published_version,
        Some(published_through.min(expected_version))
    );
    let staged_versions: Vec<_> = updated
        .log_segment
        .listed
        .staged_commits()
        .map(|p| p.version)
        .collect();
    assert_eq!(staged_versions, (1..=expected_version).collect::<Vec<_>>());
}

fn listed(
    commits: &[Version],
    checkpoint: Option<Version>,
    crc: Option<Version>,
) -> LogSegmentFiles {
    let commits: Vec<_> = commits
        .iter()
        .map(|v| parse_log_path(&format!("{v:020}.json"), 100))
        .collect();
    LogSegmentFiles {
        latest_commit_file: commits.last().cloned(),
        max_published_version: commits.last().map(|p| p.version),
        ascending_commit_files: commits,
        checkpoint_parts: checkpoint
            .into_iter()
            .map(|v| parse_log_path(&format!("{v:020}.checkpoint.parquet"), 100))
            .collect(),
        latest_crc_file: crc.map(|v| parse_log_path(&format!("{v:020}.crc"), 100)),
        ..Default::default()
    }
}

fn segment(listed: LogSegmentFiles) -> LogSegment {
    LogSegment::try_new(
        listed,
        Url::parse("memory:///_delta_log/").unwrap(),
        None,
        None,
    )
    .unwrap()
}

fn commit_versions(segment: &LogSegment) -> Vec<Version> {
    segment
        .listed
        .ascending_commit_files
        .iter()
        .map(|p| p.version)
        .collect()
}
