//! Read-only access to the log segment retained by a snapshot.

use delta_kernel::log_segment::LogSegment;

use crate::handle::Handle;
use crate::log_path::{FfiLogPath, LogPathArray};
use crate::{
    kernel_string_slice, optional_pointer, FfiSlice, KernelStringSlice, NullableCvoid,
    OptionalValue, SharedSnapshot,
};

/// The files and publication state retained by a snapshot's log segment.
///
/// All pointers, including nested strings and arrays, are borrowed for the duration of the
/// [`LogSegmentVisitor`] callback only. Empty arrays may have a null pointer. Optional file
/// pointers are null when absent. This describes retained state; it does not refresh the snapshot
/// or establish whether its version or publication watermark is still current.
#[repr(C)]
pub struct FfiLogSegment {
    /// URL of the table's `_delta_log` directory.
    pub log_root: KernelStringSlice,
    /// Last version represented by this segment.
    pub end_version: u64,
    /// Selected checkpoint version, if any.
    pub checkpoint_version: OptionalValue<u64>,
    /// Retained commit files in ascending version order, including staged commits.
    pub commit_files: LogPathArray,
    /// Retained log compaction files, sorted by start version then end version, both ascending.
    pub compaction_files: LogPathArray,
    /// Selected checkpoint's top-level files. Does not enumerate checkpoint sidecars.
    pub checkpoint_files: LogPathArray,
    /// Retained latest commit, which may be present even when `commit_files` is empty.
    pub latest_commit_file: *const FfiLogPath,
    /// Retained latest CRC file; its version may precede `end_version`.
    pub latest_crc_file: *const FfiLogPath,
    /// Highest published commit version recorded by this segment.
    /// `None` means no publication watermark is recorded, not that checkpoint or table
    /// versions are unpublished. Retained `_staged_commits/` paths at or below the
    /// watermark represent commits already known to be published.
    pub max_published_version: OptionalValue<u64>,
}

/// Receives the complete segment once. Before returning, copy any values needed after the callback.
pub type LogSegmentVisitor = extern "C" fn(context: NullableCvoid, segment: *const FfiLogSegment);

/// Visits the snapshot's retained log segment without engine I/O or snapshot reconstruction.
///
/// The snapshot is borrowed, not consumed. `context` is passed unchanged to `visitor` and may be
/// null. The callback runs exactly once and all supplied memory expires when it returns.
///
/// # Safety
///
/// `snapshot` must be a valid handle for the entire call. The callback must not free or consume
/// the snapshot, retain borrowed pointers, or unwind across the FFI boundary.
#[no_mangle]
pub unsafe extern "C" fn snapshot_log_segment(
    snapshot: Handle<SharedSnapshot>,
    context: NullableCvoid,
    visitor: LogSegmentVisitor,
) {
    let snapshot = unsafe { snapshot.as_ref() };
    visit(snapshot.log_segment(), context, visitor);
}

fn visit(segment: &LogSegment, context: NullableCvoid, visitor: LogSegmentVisitor) {
    let files = &segment.listed;
    let commits: Vec<_> = files
        .ascending_commit_files
        .iter()
        .map(FfiLogPath::from_parsed)
        .collect();
    let compactions: Vec<_> = files
        .ascending_compaction_files
        .iter()
        .map(FfiLogPath::from_parsed)
        .collect();
    let checkpoints: Vec<_> = files
        .checkpoint_parts
        .iter()
        .map(FfiLogPath::from_parsed)
        .collect();
    let latest_commit = files
        .latest_commit_file
        .as_ref()
        .map(FfiLogPath::from_parsed);
    let latest_crc = files.latest_crc_file.as_ref().map(FfiLogPath::from_parsed);
    let log_root = segment.log_root.as_str();
    let value = FfiLogSegment {
        log_root: kernel_string_slice!(log_root),
        end_version: segment.end_version,
        checkpoint_version: segment.checkpoint_version.into(),
        // These arrays and their strings remain alive until the callback returns.
        commit_files: unsafe { FfiSlice::new_unsafe(&commits) },
        compaction_files: unsafe { FfiSlice::new_unsafe(&compactions) },
        checkpoint_files: unsafe { FfiSlice::new_unsafe(&checkpoints) },
        latest_commit_file: optional_pointer(latest_commit.as_ref()),
        latest_crc_file: optional_pointer(latest_crc.as_ref()),
        max_published_version: files.max_published_version.into(),
    };
    visitor(context, &value);
}

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
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
        captured.latest_crc = unsafe { segment.latest_crc_file.as_ref() }
            .map(|file| unsafe { file.log_path() }.unwrap());
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
        let expected_checkpoints: Vec<_> =
            files.checkpoint_parts.iter().map(expected_file).collect();
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
}
