//! Deduplication abstraction for log replay processors.
//!
//! The [`Deduplicator`] trait supports two deduplication strategies:
//!
//! - **JSON commit files** (`is_log_batch = true`): Tracks (path, dv) and updates the seen set
//!   as files are encountered. Implementation: [`FileActionDeduplicator`]
//!
//! - **Checkpoint files** (`is_log_batch = false`): Uses (path, dv) to filter actions against a
//!   read-only seen set pre-populated from the commit log phase.
//!   Implementation: [`CheckpointDeduplicator`]
//!
//! [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator

use std::hash::Hash;

use hashbrown::HashSet;

use crate::engine_data::{GetData, TypedGetData};
use crate::log_replay::FileActionKey;
use crate::DeltaResult;

/// Borrowed view of a deletion vector, used as part of [`ExtractedFileAction`] for zero-copy
/// dedup lookups. All fields are borrowed from column data; no heap allocation is needed.
#[derive(Debug, Hash)]
pub(crate) struct DvKeyRef<'a> {
    pub(crate) storage_type: &'a str,
    pub(crate) path_or_inline_dv: &'a str,
    pub(crate) offset: Option<i32>,
}

/// A file action extracted from a batch row. All fields are borrowed from column data —
/// no heap allocations are needed until the action is confirmed new and inserted into the seen
/// set.
pub(crate) struct ExtractedFileAction<'a> {
    /// Borrowed path from the action batch column data.
    pub(crate) path: &'a str,
    /// Borrowed deletion vector fields, if present.
    pub(crate) dv: Option<DvKeyRef<'a>>,
    /// `true` for add actions, `false` for remove actions.
    pub(crate) is_add: bool,
}

/// Hashes only the dedup-key fields (path and dv), ignoring `is_add`.
///
/// Must be consistent with [`FileActionKey`]'s derived `Hash` so that equivalent keys
/// produce the same hash in [`hashbrown::HashSet`] lookups.
impl Hash for ExtractedFileAction<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.dv.hash(state);
    }
}

impl hashbrown::Equivalent<FileActionKey> for ExtractedFileAction<'_> {
    fn equivalent(&self, key: &FileActionKey) -> bool {
        self.path == key.path
            && match (&self.dv, &key.dv) {
                (None, None) => true,
                (Some(r), Some(o)) => {
                    r.storage_type == o.storage_type
                        && r.path_or_inline_dv == o.path_or_inline_dv
                        && r.offset == o.offset
                }
                _ => false,
            }
    }
}

pub(crate) trait Deduplicator {
    /// Extracts a file action from row `i` without allocating an owned key.
    ///
    /// Returns `Some(action)` if a file action is present at this row, `None` otherwise.
    /// The returned `action.path` borrows from `getters` with lifetime `'a`.
    ///
    /// TODO: Remove the `skip_removes` parameter in the future. The caller is responsible for
    /// using the correct [`Deduplicator`] instance depending on whether the batch belongs to a
    /// commit or to a checkpoint.
    fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        skip_removes: bool,
    ) -> DeltaResult<Option<ExtractedFileAction<'a>>>;

    /// Checks if this file action has been seen before. When `is_log_batch() = true`, records
    /// new files so future duplicates can be recognized. Returns `true` if the file should be
    /// filtered out (already seen), `false` if it is new and should be processed.
    ///
    /// Only allocates an owned [`FileActionKey`] when the file is new and needs to be inserted
    /// into the seen set. Already-seen files are identified via a zero-copy borrowed-key lookup.
    fn check_and_record_seen(&mut self, action: ExtractedFileAction<'_>) -> bool;

    /// Returns `true` for commit log batches (updates the seen set), `false` for checkpoints
    /// (read-only).
    fn is_log_batch(&self) -> bool;

    /// Extracts the deletion vector fields as a borrowed [`DvKeyRef`] if a DV is present.
    ///
    /// Reads three consecutive getter columns starting at `dv_start_index`:
    /// - `dv_start_index` — `deletionVector.storageType`
    /// - `dv_start_index + 1` — `deletionVector.pathOrInlineDv`
    /// - `dv_start_index + 2` — `deletionVector.offset` (optional)
    ///
    /// All returned fields borrow from `getters`, so no heap allocation occurs.
    fn extract_dv_key<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        dv_start_index: usize,
    ) -> DeltaResult<Option<DvKeyRef<'a>>> {
        let Some(storage_type) =
            getters[dv_start_index].get_opt(i, "deletionVector.storageType")?
        else {
            return Ok(None);
        };
        let path_or_inline_dv =
            getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
        let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;
        Ok(Some(DvKeyRef {
            storage_type,
            path_or_inline_dv,
            offset,
        }))
    }
}

/// Read-only deduplicator for checkpoint processing.
///
/// Unlike [`FileActionDeduplicator`] which mutably tracks files, this uses an immutable
/// reference to filter checkpoint actions against files already seen from commits.
/// Only handles add actions (no removes), and never modifies the seen set.
///
/// [`FileActionDeduplicator`]: crate::log_replay::FileActionDeduplicator
#[allow(unused)]
pub(crate) struct CheckpointDeduplicator<'a> {
    seen_file_keys: &'a hashbrown::HashSet<FileActionKey>,
    add_path_index: usize,
    add_dv_start_index: usize,
}

impl<'a> CheckpointDeduplicator<'a> {
    #[allow(unused)]
    pub(crate) fn try_new(
        seen_file_keys: &'a hashbrown::HashSet<FileActionKey>,
        add_path_index: usize,
        add_dv_start_index: usize,
    ) -> DeltaResult<Self> {
        Ok(CheckpointDeduplicator {
            seen_file_keys,
            add_path_index,
            add_dv_start_index,
        })
    }
}

impl Deduplicator for CheckpointDeduplicator<'_> {
    /// Extracts add action key only (checkpoints skip removes). `skip_removes` is ignored.
    fn extract_file_action<'b>(
        &self,
        i: usize,
        getters: &[&'b dyn GetData<'b>],
        _skip_removes: bool,
    ) -> DeltaResult<Option<ExtractedFileAction<'b>>> {
        let Some(path) = getters[self.add_path_index].get_str(i, "add.path")? else {
            return Ok(None);
        };
        let dv = self.extract_dv_key(i, getters, self.add_dv_start_index)?;
        Ok(Some(ExtractedFileAction {
            path,
            dv,
            is_add: true,
        }))
    }

    /// Read-only check against seen set. Returns `true` if file should be filtered out.
    fn check_and_record_seen(&mut self, action: ExtractedFileAction<'_>) -> bool {
        self.seen_file_keys.contains(&action)
    }

    /// Always `false` - checkpoint batches never update the seen set.
    fn is_log_batch(&self) -> bool {
        false
    }
}
