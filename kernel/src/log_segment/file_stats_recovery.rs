//! Full action reconciliation for recovering absolute file statistics.
//!
//! This module implements the recovery path for snapshots whose CRC has
//! [`Indeterminate`](crate::crc::FileStatsState::Indeterminate) or
//! [`RequiresCheckpointRead`](crate::crc::FileStatsState::RequiresCheckpointRead) file stats.
//! It performs a full reverse log replay with `(path, dv_unique_id)` deduplication, summing
//! the sizes of currently-active files to produce known-correct
//! [`Valid`](crate::crc::FileStatsState::Valid) stats.
//!
//! Algorithm (newest-to-oldest reverse iteration):
//! 1. For each row in each batch, extract the file action key `(path, dv_unique_id)`.
//! 2. If we have already seen that key, skip (we processed the newest version already).
//! 3. Otherwise, record the key. If the action is an Add, count it as an active file and accumulate
//!    its size; if it is a Remove, treat it as a tombstone (record but don't count -- and don't
//!    count any older Add for the same key).
//! 4. Checkpoint batches (`is_log_batch = false`) contribute Adds only; their Removes are vacuum
//!    tombstones and are ignored.
//! 5. If any Remove encountered has a missing `size` field, the recovery returns
//!    [`Untrackable`](crate::crc::FileStatsState::Untrackable) -- byte-level totals are permanently
//!    impossible to compute.
//!
//! This is the priority-5 path from the CRC design plan. The ordinary (non-recovery)
//! snapshot-load path does cheaper incremental tracking; recovery is only needed when
//! incremental tracking has degraded.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::LazyLock;

use tracing::instrument;

use super::LogSegment;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
use crate::crc::{FileSizeHistogram, FileStatsState};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, SchemaRef};
use crate::{DeltaResult, Engine, RowVisitor};

impl LogSegment {
    /// Reconcile add/remove actions across the entire segment to compute absolute file
    /// statistics from scratch.
    ///
    /// Returns:
    /// - [`FileStatsState::Valid`] with absolute counts, byte totals, and a histogram when
    ///   reconciliation succeeds.
    /// - [`FileStatsState::Untrackable`] when any Remove has a missing `size` field. Byte totals
    ///   cannot be computed in this case.
    ///
    /// The histogram is rebuilt over the active file set using the standard
    /// [`FileSizeHistogram::create_default`] boundaries. Callers wanting different
    /// boundaries should adjust the resulting CRC's histogram via subsequent applies, OR
    /// pass a custom-bin variant -- left for future work.
    #[instrument(name = "log_seg.recover_file_stats", skip_all, err)]
    pub(crate) fn recover_file_stats(&self, engine: &dyn Engine) -> DeltaResult<FileStatsState> {
        let schema = recovery_schema()?;
        let mut accumulator = ReconciliationAccumulator::new();
        for batch_result in self.read_actions(engine, schema)? {
            let batch = batch_result?;
            let data = batch.actions.as_ref();
            let mut visitor = ReconciliationVisitor {
                is_log_batch: batch.is_log_batch,
                acc: &mut accumulator,
            };
            visitor.visit_rows_of(data)?;
        }
        Ok(accumulator.into_file_stats_state())
    }
}

/// Schema for recovery: needs `path`, `size`, and the three DV descriptor fields used to
/// compute the unique key (`storageType`, `pathOrInlineDv`, `offset`) for both Add and
/// Remove. Other Add/Remove fields (e.g. partitionValues, modificationTime, tags) are not
/// needed because we are reconciling for file-stat counts, not for emitting actions.
fn recovery_schema() -> DeltaResult<SchemaRef> {
    get_commit_schema().project(&[ADD_NAME, REMOVE_NAME])
}

// ============================================================================
// Accumulator
// ============================================================================

/// Tracks dedup state and running totals across batches.
///
/// `seen` keys are `(path, dv_unique_id)`. When the value is `true`, the first-seen action
/// for that key was an Add (file is active and counted). When `false`, the first-seen action
/// was a Remove (file is tombstoned and any older Add for the same key must be ignored).
struct ReconciliationAccumulator {
    /// Map of `(path, dv_unique_id)` → was-first-seen-an-add (true) or remove (false).
    /// Using a HashMap (not a HashSet) lets us assert the dedup invariant:
    /// the same key should never be inserted twice.
    seen: HashMap<FileKey, bool>,
    num_files: i64,
    total_size: i64,
    histogram: FileSizeHistogram,
    /// Set when a Remove had a null `size` -- byte totals become unknown forever.
    has_missing_remove_size: bool,
}

impl ReconciliationAccumulator {
    fn new() -> Self {
        Self {
            seen: HashMap::new(),
            num_files: 0,
            total_size: 0,
            histogram: FileSizeHistogram::create_default(),
            has_missing_remove_size: false,
        }
    }

    fn into_file_stats_state(self) -> FileStatsState {
        if self.has_missing_remove_size {
            return FileStatsState::Untrackable;
        }
        FileStatsState::Valid {
            num_files: self.num_files,
            table_size_bytes: self.total_size,
            histogram: Some(self.histogram),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
struct FileKey {
    path: String,
    dv_unique_id: Option<String>,
}

// ============================================================================
// Visitor
// ============================================================================

/// Per-batch visitor that drives the reconciliation accumulator.
///
/// For each row, attempts (in order):
/// 1. If the row has an Add action (`add.path` is non-null): extract its `(path, dv_id)` key. If
///    first-seen for that key, record it and count the file (sum size, update histogram). If
///    already seen, skip silently.
/// 2. If the row has a Remove action AND we are processing a commit batch: extract its `(path,
///    dv_id)` key. If first-seen, record it as a tombstone (don't count). If the Remove has a null
///    `size`, set the `has_missing_remove_size` flag.
/// 3. Checkpoint Removes (`is_log_batch = false`) are ignored entirely (vacuum tombstones).
struct ReconciliationVisitor<'a> {
    is_log_batch: bool,
    acc: &'a mut ReconciliationAccumulator,
}

impl RowVisitor for ReconciliationVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    // Add columns
                    ColumnName::new(["add", "path"]), // 0
                    ColumnName::new(["add", "size"]), // 1
                    ColumnName::new(["add", "deletionVector", "storageType"]), // 2
                    ColumnName::new(["add", "deletionVector", "pathOrInlineDv"]), // 3
                    ColumnName::new(["add", "deletionVector", "offset"]), // 4
                    // Remove columns
                    ColumnName::new(["remove", "path"]), // 5
                    ColumnName::new(["remove", "size"]), // 6
                    ColumnName::new(["remove", "deletionVector", "storageType"]), // 7
                    ColumnName::new(["remove", "deletionVector", "pathOrInlineDv"]), // 8
                    ColumnName::new(["remove", "deletionVector", "offset"]), // 9
                ],
                vec![
                    DataType::STRING,  // add.path
                    DataType::LONG,    // add.size
                    DataType::STRING,  // add.dv.storageType
                    DataType::STRING,  // add.dv.pathOrInlineDv
                    DataType::INTEGER, // add.dv.offset
                    DataType::STRING,  // remove.path
                    DataType::LONG,    // remove.size
                    DataType::STRING,  // remove.dv.storageType
                    DataType::STRING,  // remove.dv.pathOrInlineDv
                    DataType::INTEGER, // remove.dv.offset
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Try Add first.
            if let Some(add_path) = getters[0].get_str(i, "add.path")? {
                let dv_unique_id = extract_dv_unique_id(i, getters, 2)?;
                let key = FileKey {
                    path: add_path.to_string(),
                    dv_unique_id,
                };
                if let Entry::Vacant(slot) = self.acc.seen.entry(key) {
                    // First-seen-in-reverse Add: this is the newest action for this file
                    // and the file is active.
                    slot.insert(true);
                    let size: i64 = getters[1].get(i, "add.size")?;
                    self.acc.num_files += 1;
                    self.acc.total_size += size;
                    self.acc.histogram.insert(size)?;
                }
                continue;
            }

            // Then Remove. Only commit batches contribute Removes (checkpoint Removes are
            // vacuum tombstones, not active state).
            if !self.is_log_batch {
                continue;
            }
            if let Some(remove_path) = getters[5].get_str(i, "remove.path")? {
                let dv_unique_id = extract_dv_unique_id(i, getters, 7)?;
                let key = FileKey {
                    path: remove_path.to_string(),
                    dv_unique_id,
                };
                if let Entry::Vacant(slot) = self.acc.seen.entry(key) {
                    // First-seen-in-reverse Remove: file is tombstoned. Don't count, but
                    // record so older Adds for the same key are skipped.
                    slot.insert(false);
                    // Detect missing remove.size, which makes byte totals unknowable.
                    let remove_size: Option<i64> = getters[6].get_opt(i, "remove.size")?;
                    if remove_size.is_none() {
                        self.acc.has_missing_remove_size = true;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Extracts `(path, dv_id)` deletion-vector unique ID from a triple of getters starting at
/// `dv_start_index`. Mirrors
/// [`Deduplicator::extract_dv_unique_id`](crate::log_replay::deduplicator::Deduplicator::extract_dv_unique_id)
/// but kept local to avoid coupling this module to the trait machinery.
fn extract_dv_unique_id<'a>(
    i: usize,
    getters: &[&'a dyn GetData<'a>],
    dv_start_index: usize,
) -> DeltaResult<Option<String>> {
    let Some(storage_type) = getters[dv_start_index].get_opt(i, "deletionVector.storageType")?
    else {
        return Ok(None);
    };
    let path_or_inline = getters[dv_start_index + 1].get(i, "deletionVector.pathOrInlineDv")?;
    let offset = getters[dv_start_index + 2].get_opt(i, "deletionVector.offset")?;
    Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
        storage_type,
        path_or_inline,
        offset,
    )))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_acc() -> ReconciliationAccumulator {
        ReconciliationAccumulator::new()
    }

    fn key(path: &str) -> FileKey {
        FileKey {
            path: path.to_string(),
            dv_unique_id: None,
        }
    }

    #[test]
    fn empty_accumulator_yields_valid_zero_stats() {
        let state = empty_acc().into_file_stats_state();
        assert!(matches!(
            state,
            FileStatsState::Valid {
                num_files: 0,
                table_size_bytes: 0,
                ..
            }
        ));
    }

    #[test]
    fn missing_remove_size_yields_untrackable() {
        let mut acc = empty_acc();
        acc.has_missing_remove_size = true;
        assert!(matches!(
            acc.into_file_stats_state(),
            FileStatsState::Untrackable
        ));
    }

    #[test]
    fn accumulator_sums_bytes_and_counts_files() {
        let mut acc = empty_acc();
        acc.seen.insert(key("f1"), true);
        acc.seen.insert(key("f2"), true);
        acc.num_files = 2;
        acc.total_size = 300;
        acc.histogram.insert(100).unwrap();
        acc.histogram.insert(200).unwrap();
        let state = acc.into_file_stats_state();
        let FileStatsState::Valid {
            num_files,
            table_size_bytes,
            histogram,
        } = state
        else {
            panic!("expected Valid");
        };
        assert_eq!(num_files, 2);
        assert_eq!(table_size_bytes, 300);
        let h = histogram.unwrap();
        assert_eq!(h.file_counts()[0], 2);
        assert_eq!(h.total_bytes()[0], 300);
    }

    #[test]
    fn distinct_keys_with_same_path_different_dv_are_separate() {
        let mut acc = empty_acc();
        let k1 = FileKey {
            path: "f".to_string(),
            dv_unique_id: Some("a".to_string()),
        };
        let k2 = FileKey {
            path: "f".to_string(),
            dv_unique_id: Some("b".to_string()),
        };
        acc.seen.insert(k1, true);
        acc.seen.insert(k2, true);
        assert_eq!(acc.seen.len(), 2);
    }
}
