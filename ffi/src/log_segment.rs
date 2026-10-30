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

/// Receives the complete segment once. Copy any values retained after the callback returns.
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
mod tests;
