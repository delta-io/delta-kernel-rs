//! Streams the file-action diff between two table versions. See
//! [`IncrementalScanBuilder`] for the entry point.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::actions::{Add, Remove, ADD_NAME, REMOVE_NAME};
use crate::engine_data::{FilteredEngineData, GetData, RowVisitor};
use crate::expressions::{column_name, ColumnName};
use crate::log_replay::deduplicator::Deduplicator;
use crate::log_replay::{FileActionDeduplicator, FileActionKey};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::SnapshotRef;
use crate::table_features::Operation;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, FileDataReadResultIterator, FileMeta, Version,
};

/// Schema projected from each commit JSON: `add` and `remove` only. Kernel itself decodes only
/// `path` and `deletionVector.{storageType, pathOrInlineDv, offset}` from these rows (for dedup
/// keying); every other field is passed through to the caller verbatim inside
/// [`FilteredEngineData`].
///
/// Target-only protocol validation via [`Operation::Scan`] is sufficient because:
///   - The protocol's feature-immutability rule (features cannot be removed once added) makes the
///     target snapshot's `readerFeatures` an upper bound on every reader feature in use in any
///     commit in `(base_version, target_version]`.
///   - Of the fields kernel itself decodes, only `deletionVector.*` is feature-gated, and
///     pre-`deletionVectors`-enable commits cannot populate it.
///
/// Consumers that decode pass-through fields (e.g. `stats`, `partitionValues`, `baseRowId`)
/// must interpret each row against the protocol active at that row's commit version, not
/// naively against the target snapshot. The most common pitfall is column mapping enabled
/// mid-range: pre-enable rows key `stats`/`partitionValues` by logical names, post-enable
/// rows key them by physical names.
static INCREMENTAL_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ]))
});

/// Builder for an incremental scan over `(base_version, target_version]`.
///
/// The upper bound (`target_version`) is taken from the supplied target snapshot; the lower
/// bound (`base_version`, exclusive) is supplied at construction. Use this to advance a
/// cached file listing from `base_version` to the target snapshot without re-scanning the
/// whole table.
///
/// Construct via [`crate::Snapshot::incremental_scan_builder`] (preferred) and drive with
/// [`IncrementalScanBuilder::build`].
#[derive(Debug)]
pub struct IncrementalScanBuilder {
    /// The target snapshot; supplies `target_version`, the snapshot's commit list, and the
    /// effective protocol used for the reader-feature check.
    target_snapshot: SnapshotRef,
    /// Exclusive lower bound of the scan range. Must be strictly less than the target
    /// snapshot's version.
    base_version: Version,
}

impl IncrementalScanBuilder {
    /// Create a new builder for the range `(base_version, target_snapshot.version()]`.
    ///
    /// Connectors should call [`crate::Snapshot::incremental_scan_builder`] rather than
    /// constructing directly.
    ///
    /// Listing, dedup, and the protocol check happen in [`IncrementalScanBuilder::build`].
    pub(crate) fn new(target_snapshot: impl Into<SnapshotRef>, base_version: Version) -> Self {
        Self {
            target_snapshot: target_snapshot.into(),
            base_version,
        }
    }

    /// Build the incremental scan stream.
    ///
    /// Does not re-list `_delta_log/`: the target snapshot already validated its commit
    /// list during construction, so this method walks that pre-existing list and clips it
    /// to `(base_version, target_version]`. For catalog-managed tables that's important,
    /// the snapshot's `log_tail` carries staged commits that a fresh storage listing would
    /// miss. Runs version validation, the reader-feature check on the target protocol, and
    /// the snapshot-covers-the-range check upfront. If the range can be served, returns
    /// `Some(stream)` for the consumer to drive batch-by-batch. Returns `None` when the
    /// target snapshot's commit list cannot serve `(base_version, target_version]`;
    /// consumers should fall back to a full scan via [`crate::Snapshot::scan_builder`].
    ///
    /// # Errors
    /// - `Err` if `base_version >= target_snapshot.version()` (caller error).
    /// - `Err` if the target snapshot's protocol contains an unsupported reader feature.
    /// - `Err` if the engine fails to open the commit stream.
    ///
    /// I/O errors that occur while draining the stream surface from the
    /// [`IncrementalScanStream`] itself (or from its terminal methods).
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Option<IncrementalScanStream>> {
        // TODO(#2493): the version validation, the snapshot-commit-list extraction and
        // clipping below, and the "snapshot covers the range" check should all be replaced
        // by a single `CommitRangeBuilder::from_snapshot(...).build(engine)` call once
        // that primitive lands. https://github.com/delta-io/delta-kernel-rs/issues/2493
        let target_version = self.target_snapshot.version();
        require!(
            self.base_version < target_version,
            Error::generic(format!(
                "IncrementalScanBuilder: base_version ({}) must be less than target_version ({})",
                self.base_version, target_version
            ))
        );
        // `base_version < target_version` above guarantees `base_version < u64::MAX`, but use
        // `checked_add` to make the dependency explicit and panic-free.
        let start_version = self.base_version.checked_add(1).ok_or_else(|| {
            Error::generic("IncrementalScanBuilder: base_version + 1 overflowed u64")
        })?;

        // Confirm kernel supports the target's reader features. Mirrors `Scan::new`. The
        // intermediate commits in `(base_version, target_version]` are read for their `add`
        // and `remove` actions only -- their schemas/stats are passed through opaquely to
        // the caller, who is expected to interpret them against the target snapshot.
        self.target_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Scan)?;

        // TODO(#2493): replace this block with `CommitRange` once it exists.
        // Today we use the snapshot's already-validated commit list rather than re-listing
        // storage. This is correct for catalog-managed tables: the snapshot's log_segment
        // includes any staged/ratified-but-unpublished commits passed in via `with_log_tail`,
        // which a fresh storage listing would silently miss. `CommitRange` will own this
        // listing-and-validation responsibility centrally.
        let snapshot_commits = &self
            .target_snapshot
            .log_segment()
            .listed
            .ascending_commit_files;
        let snapshot_first_version = snapshot_commits.first().map(|c| c.version);

        // TODO(#2493): this "snapshot covers range" check becomes a typed error from
        // `CommitRange::build` (e.g. `Error::CommitsUnavailable { missing: Range<Version> }`),
        // pattern-matched here to return `None`.
        if snapshot_first_version.is_none_or(|v| v > start_version) {
            return Ok(None);
        }

        // Collect in-range commit locations in newest-first order. Handing all of them to
        // `read_json_files` in a single call lets the engine's prefetch (e.g.
        // `DefaultEngine`'s `buffered(buffer_size)`) overlap object-store GETs across
        // commits, instead of paying first-byte latency once per commit serially.
        // Newest-first order gives `FileActionDeduplicator` newest-wins semantics across
        // the range. `read_json_files` takes `&[FileMeta]` so materializing here is required.
        let commit_locations: Vec<FileMeta> = snapshot_commits
            .iter()
            .filter(|c| c.version >= start_version && c.version <= target_version)
            .rev()
            .map(|c| c.location.clone())
            .collect();

        let actions = engine.json_handler().read_json_files(
            &commit_locations,
            INCREMENTAL_READ_SCHEMA.clone(),
            None,
        )?;

        Ok(Some(IncrementalScanStream {
            base_version: self.base_version,
            target_version,
            actions,
            seen_file_keys: HashSet::new(),
            surviving_adds: HashSet::new(),
            removes: HashSet::new(),
            errored: false,
        }))
    }
}

/// Streaming output of an incremental scan over `(base_version, target_version]`.
///
/// # Iteration contract
///
/// As an [`Iterator`], yields surviving Add batches as [`FilteredEngineData`] in
/// newest-first order (descending commit version): commits closer to `target_version`
/// produce items before commits closer to `base_version`. Within a commit, the engine may
/// split its rows across multiple [`ActionsBatch`] yields and may interleave batches across
/// commits during prefetch; the surviving-Add filtering is per-batch and order-independent.
///
/// A commit whose Adds were all cancelled by later Removes produces no item but still
/// updates dedup state for older commits. The iterator returns `None` once every in-range
/// commit has been processed.
///
/// # Dedup state
///
/// Dedup is keyed on `(path, dv_unique_id)` and only advances when [`Iterator::next`] is
/// polled. A consumer who polls until exhaustion has fully populated `surviving_adds` /
/// `removes`. A consumer who calls a terminal method ([`finish`] / [`collect_listing`])
/// before exhausting drains the remaining batches internally before computing the final
/// state.
///
/// # Error semantics
///
/// On any underlying I/O or visitor error, the yielding `next()` call returns
/// `Some(Err(_))` and the stream transitions to an errored state. All subsequent `next()`
/// calls return `None` (no double-reporting), and calling any terminal method on an
/// errored stream returns `Err` rather than producing a partial summary. The terminal
/// methods consume `self`, so the type system also prevents accidental reuse.
///
/// On any error the consumer should fall back to a full scan via
/// [`crate::Snapshot::scan_builder`]: kernel cannot guarantee the diff is complete.
///
/// [`finish`]: Self::finish
/// [`collect_listing`]: Self::collect_listing
/// [`ActionsBatch`]: crate::log_replay::ActionsBatch
pub struct IncrementalScanStream {
    base_version: Version,
    target_version: Version,
    /// Action batches across every in-range commit, in newest-first order. The engine is
    /// free to prefetch upcoming commits (e.g. `DefaultEngine` overlaps GETs via
    /// `buffered(buffer_size)`), so within-batch and cross-commit transitions are both
    /// driven by polling this single iterator.
    actions: FileDataReadResultIterator,
    seen_file_keys: HashSet<FileActionKey>,
    surviving_adds: HashSet<FileActionKey>,
    removes: HashSet<FileActionKey>,
    /// Set when the iterator yields an `Err`. Subsequent `next()` calls return `None`;
    /// terminal methods return `Err`.
    errored: bool,
}

impl Iterator for IncrementalScanStream {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        loop {
            match self.actions.next()? {
                Err(e) => {
                    self.errored = true;
                    return Some(Err(e));
                }
                Ok(batch) => match process_batch(
                    batch,
                    &mut self.seen_file_keys,
                    &mut self.surviving_adds,
                    &mut self.removes,
                ) {
                    Ok(Some(filtered)) => return Some(Ok(filtered)),
                    Ok(None) => continue,
                    Err(e) => {
                        self.errored = true;
                        return Some(Err(e));
                    }
                },
            }
        }
    }
}

impl IncrementalScanStream {
    /// Drain any unread batches, then return the surviving file-key sets without applying
    /// cross-snapshot dedup. Consumers can intersect these against whatever base
    /// data structure they prefer.
    ///
    /// # Errors
    /// Returns `Err` if the stream previously yielded an error (dedup state is incomplete),
    /// or if draining produces an error.
    pub fn finish(mut self) -> DeltaResult<IncrementalScanSummary> {
        if self.errored {
            return Err(Error::generic(
                "IncrementalScanStream: cannot finish a stream that previously errored",
            ));
        }
        // Drain anything the consumer didn't pull. `self.next()` propagates errors via
        // `Some(Err(_))` and sets `errored`; the `?` below surfaces them.
        for item in self.by_ref() {
            item?;
        }
        Ok(IncrementalScanSummary {
            base_version: self.base_version,
            target_version: self.target_version,
            surviving_adds: self.surviving_adds,
            removes: self.removes,
        })
    }

    /// Eager helper: collect every surviving Add batch into a [`Vec`], then call
    /// [`finish`] to recover the file-key sets. Use this when the diff fits in memory and
    /// the consumer prefers the simpler eager shape over the iterator pattern.
    ///
    /// The returned `Vec` may contain multiple batches per source commit: the engine is
    /// free to split a commit's rows across [`ActionsBatch`] yields (e.g. by JSON-reader
    /// batch-size limits). One yielded batch produces at most one `Vec` entry, and a
    /// commit whose Adds were all cancelled by later Removes produces no entry at all.
    ///
    /// [`finish`]: Self::finish
    /// [`ActionsBatch`]: crate::log_replay::ActionsBatch
    pub fn collect_listing(mut self) -> DeltaResult<IncrementalListing> {
        let mut add_files: Vec<FilteredEngineData> = Vec::new();
        for item in self.by_ref() {
            add_files.push(item?);
        }
        let summary = self.finish()?;
        Ok(IncrementalListing { summary, add_files })
    }
}

impl std::fmt::Debug for IncrementalScanStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalScanStream")
            .field("base_version", &self.base_version)
            .field("target_version", &self.target_version)
            .field("surviving_adds", &self.surviving_adds.len())
            .field("removes", &self.removes.len())
            .field("errored", &self.errored)
            .finish()
    }
}

/// Surviving file-key sets without cross-snapshot classification, returned by
/// [`IncrementalScanStream::finish`].
///
/// Each set element is a [`FileActionKey`] of `(path, dv_unique_id)`. Consumers should match
/// on the full key rather than just the path: the same path with different DV ids (e.g. an
/// `add(P, dv=new) + remove(P, dv=old)` DV-replacement pair) refers to distinct logical
/// files, and collapsing to path alone loses that distinction.
#[derive(Debug)]
#[non_exhaustive]
pub struct IncrementalScanSummary {
    /// Exclusive lower bound of the scan range, as supplied to [`IncrementalScanBuilder`].
    pub base_version: Version,
    /// Inclusive upper bound; equals the source snapshot's version.
    pub target_version: Version,
    /// File keys of surviving Add actions in `(base_version, target_version]`.
    pub surviving_adds: HashSet<FileActionKey>,
    /// File keys of surviving Remove actions in `(base_version, target_version]`.
    pub removes: HashSet<FileActionKey>,
}

/// Eager output of [`IncrementalScanStream::collect_listing`]: the buffered Add
/// batches plus the summary (no cross-snapshot classification).
#[non_exhaustive]
pub struct IncrementalListing {
    /// Surviving Add and Remove file-key sets for the range.
    pub summary: IncrementalScanSummary,
    /// All surviving Add batches in descending commit-version order. One entry per source
    /// commit batch that produced any surviving Adds.
    pub add_files: Vec<FilteredEngineData>,
}

impl std::fmt::Debug for IncrementalListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalListing")
            .field("summary", &self.summary)
            .field("add_files_batch_count", &self.add_files.len())
            .finish()
    }
}

/// Visit one commit-batch, updating dedup state and accumulators. Returns the filtered Add
/// rows for this batch, or `None` if no Adds survived dedup in this batch.
fn process_batch(
    batch: Box<dyn EngineData>,
    seen_file_keys: &mut HashSet<FileActionKey>,
    surviving_adds: &mut HashSet<FileActionKey>,
    removes: &mut HashSet<FileActionKey>,
) -> DeltaResult<Option<FilteredEngineData>> {
    let row_count = batch.len();
    let mut adds_sel = vec![false; row_count];

    let deduplicator = FileActionDeduplicator::new(
        seen_file_keys,
        true, // commit batches always update the seen set
        ADD_PATH_INDEX,
        REMOVE_PATH_INDEX,
        ADD_DV_START_INDEX,
        REMOVE_DV_START_INDEX,
    );
    let mut visitor = IncrementalDedupVisitor {
        deduplicator,
        adds_sel: &mut adds_sel,
        surviving_adds,
        removes,
    };
    visitor.visit_rows_of(batch.as_ref())?;

    // No Adds in the batch survived dedup, e.g. a Removes-only commit or a commit whose
    // Adds were all cancelled by later commits in the range. Skip yielding an empty
    // batch; dedup state has still been advanced by the visit above.
    if !adds_sel.iter().any(|s| *s) {
        return Ok(None);
    }
    Ok(Some(FilteredEngineData::try_new(batch, adds_sel)?))
}

// Indices of the leaf columns visited per row; must match `selected_column_names_and_types`.
const ADD_PATH_INDEX: usize = 0;
const ADD_DV_START_INDEX: usize = 1; // .storageType, .pathOrInlineDv, .offset
const REMOVE_PATH_INDEX: usize = 4;
const REMOVE_DV_START_INDEX: usize = 5; // .storageType, .pathOrInlineDv, .offset
const NUM_GETTERS: usize = 8; // 4 add + 4 remove columns

// We share `FileActionDeduplicator` with the scan path but not `AddRemoveDedupVisitor`:
// that visitor is wired to scan-only state (`StateInfo`, `ScanMetrics`, per-row transform
// expressions, partition value parsing, base row id) and only outputs Adds. Removes are
// folded into `seen_file_keys` as a side effect and never surfaced to the caller. The
// incremental scan needs both surviving Adds and surviving Removes exposed (keyed by
// `(path, dv_unique_id)`), and it deliberately skips the scan-only per-row work, so a
// slimmer visitor is the right shape here.
struct IncrementalDedupVisitor<'a, 'seen> {
    deduplicator: FileActionDeduplicator<'seen>,
    adds_sel: &'a mut [bool],
    surviving_adds: &'a mut HashSet<FileActionKey>,
    removes: &'a mut HashSet<FileActionKey>,
}

impl RowVisitor for IncrementalDedupVisitor<'_, '_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // The DV columns are needed because [`FileActionDeduplicator`] keys on
        // `(path, dv_unique_id)`, not path alone. Same path with different DVs (e.g. an
        // `add(P, dv=new) + remove(P, dv=old)` DV-replacement pair) are distinct file
        // identities and must be tracked separately so newer-wins dedup is correct.
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let columns = vec![
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
            ];
            let (types, names) = columns.into_iter().unzip();
            (names, types).into()
        });
        let (names, types) = NAMES_AND_TYPES.as_ref();
        (names, types)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == NUM_GETTERS,
            Error::InternalError(format!(
                "IncrementalDedupVisitor expected {NUM_GETTERS} getters, got {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            let Some((key, is_add)) = self.deduplicator.extract_file_action(i, getters, false)?
            else {
                continue;
            };

            // Both Adds AND Removes update the seen set. A duplicate Add for a file
            // removed earlier in the range (later chronologically, since we read newest
            // first) must not leak through.
            if self.deduplicator.check_and_record_seen(key.clone()) {
                continue;
            }

            if is_add {
                self.adds_sel[i] = true;
                self.surviving_adds.insert(key);
            } else {
                self.removes.insert(key);
            }
        }

        Ok(())
    }
}
