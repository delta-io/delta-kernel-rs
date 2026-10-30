use std::ptr::NonNull;
use std::sync::Arc;

use delta_kernel::path::ParsedLogPath;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{LogPath, Version};
use test_utils::table_builder::{LogState, TestTableBuilder};
use test_utils::{compacted_log_path_for_versions, create_log_path};

use super::*;
use crate::{free_snapshot, TryFromStringSlice};

#[derive(Default)]
struct CapturedLogSegment {
    visits: usize,
    root: String,
    version: Version,
    checkpoint_version: Option<Version>,
    commits: Vec<LogPath>,
    compactions: Vec<LogPath>,
    checkpoints: Vec<LogPath>,
    latest_commit: Option<LogPath>,
    latest_crc: Option<LogPath>,
    published: Option<Version>,
}

fn version(value: &OptionalValue<Version>) -> Option<Version> {
    match value {
        OptionalValue::Some(value) => Some(*value),
        OptionalValue::None => None,
    }
}

extern "C" fn capture(context: NullableCvoid, segment: *const FfiLogSegment) {
    let captured = unsafe { &mut *context.unwrap().as_ptr().cast::<CapturedLogSegment>() };
    let segment = unsafe { &*segment };
    captured.visits += 1;
    captured.root = unsafe { String::try_from_slice(&segment.log_root) }.unwrap();
    captured.version = segment.end_version;
    captured.checkpoint_version = version(&segment.checkpoint_version);
    captured.commits = unsafe { segment.commit_files.log_paths() }.unwrap();
    captured.compactions = unsafe { segment.compaction_files.log_paths() }.unwrap();
    captured.checkpoints = unsafe { segment.checkpoint_files.log_paths() }.unwrap();
    captured.latest_commit = unsafe { segment.latest_commit_file.as_ref() }
        .map(|file| unsafe { file.log_path() }.unwrap());
    captured.latest_crc =
        unsafe { segment.latest_crc_file.as_ref() }.map(|file| unsafe { file.log_path() }.unwrap());
    captured.published = version(&segment.max_published_version);
}

#[rstest::rstest]
#[case::json_only(None, false, false)]
#[case::checkpoint_and_tail(Some(1), false, false)]
#[case::checkpoint_at_end(Some(3), false, false)]
#[case::checkpoint_without_commit_metadata(Some(3), false, true)]
#[case::staged_paths_with_compaction(None, true, false)]
fn snapshot_segment_export_preserves_retained_files(
    #[case] checkpoint: Option<Version>,
    #[case] staged_and_compacted: bool,
    #[case] absent_metadata: bool,
) {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(3)
                .with_checkpoint_at(checkpoint)
                .with_crc_at([2]),
        )
        .with_data(1, 1)
        .build()
        .unwrap();
    let source = Snapshot::builder_for(table.table_root())
        .build(&table.engine())
        .unwrap();
    let mut files = source.log_segment().listed.clone();
    if staged_and_compacted {
        let last = files.ascending_commit_files.last_mut().unwrap();
        last.location.location = source
            .table_root()
            .join("_delta_log/_staged_commits/00000000000000000003.550e8400-e29b-41d4-a716-446655440000.json")
            .unwrap();
        *last = ParsedLogPath::new(last.location.clone()).unwrap();
        files.latest_commit_file = Some(last.clone());
        files.ascending_compaction_files.push(
            create_log_path(
                source.table_root(),
                compacted_log_path_for_versions(0, 1, "json"),
            )
            .into(),
        );
        // Publication does not require rewriting retained staged paths.
        files.max_published_version = Some(3);
    }
    if absent_metadata {
        files.latest_commit_file = None;
        files.latest_crc_file = None;
        files.max_published_version = None;
    }
    let expected_file = |path: &ParsedLogPath| LogPath::try_new(path.location.clone()).unwrap();
    let expected_commits: Vec<_> = files
        .ascending_commit_files
        .iter()
        .map(expected_file)
        .collect();
    let expected_compactions: Vec<_> = files
        .ascending_compaction_files
        .iter()
        .map(expected_file)
        .collect();
    let expected_checkpoints: Vec<_> = files.checkpoint_parts.iter().map(expected_file).collect();
    let expected_latest = files.latest_commit_file.as_ref().map(expected_file);
    let expected_crc = files.latest_crc_file.as_ref().map(expected_file);
    let expected_published = files.max_published_version;
    let root = source.log_segment().log_root.clone();
    let snapshot = Snapshot::new(
        LogSegment::try_new(files, root.clone(), Some(3), None).unwrap(),
        source.table_configuration().clone(),
    )
    .unwrap();
    let snapshot: Handle<SharedSnapshot> = Arc::new(snapshot).into();
    let mut captured = CapturedLogSegment::default();
    unsafe {
        snapshot_log_segment(
            snapshot.shallow_copy(),
            Some(NonNull::from(&mut captured).cast()),
            capture,
        );
        // The getter borrows the handle; its owner can still use and release it.
        assert_eq!(snapshot.as_ref().version(), 3);
        free_snapshot(snapshot);
    }
    drop(source);
    drop(table);
    assert_eq!(captured.visits, 1);
    assert_eq!(captured.root, root.as_str());
    assert_eq!(captured.version, 3);
    assert_eq!(captured.checkpoint_version, checkpoint);
    assert_eq!(captured.commits, expected_commits);
    assert_eq!(captured.compactions, expected_compactions);
    assert_eq!(captured.checkpoints, expected_checkpoints);
    assert_eq!(captured.latest_commit, expected_latest);
    assert_eq!(captured.latest_crc, expected_crc);
    assert_eq!(captured.published, expected_published);
}
