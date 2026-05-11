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

/// Schema projected from each commit JSON: `add` and `remove` only. Protocol and metadata
/// updates between base and target are validated against the target snapshot's protocol via
/// [`Operation::Scan`]. By the protocol's feature-immutability rule (features cannot be removed
/// once added), any reader feature present in an intermediate commit is also present in the
/// target snapshot's protocol; checking the target alone is sufficient.
static INCREMENTAL_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ]))
});

/// Builder for an incremental scan over `(base_version, target_version]`. The target version
/// is taken from the supplied snapshot; the base version is supplied at construction.
#[derive(Debug)]
pub struct IncrementalScanBuilder {
    snapshot: SnapshotRef,
    base_version: Version,
}

impl IncrementalScanBuilder {
    /// Create a new builder for the range `(base_version, snapshot.version()]`. Listing,
    /// dedup, and the protocol check happen in [`IncrementalScanBuilder::build`].
    ///
    /// Most consumers should call [`crate::Snapshot::incremental_scan_builder`] instead
    /// of constructing directly.
    pub(crate) fn new(snapshot: impl Into<SnapshotRef>, base_version: Version) -> Self {
        Self {
            snapshot: snapshot.into(),
            base_version,
        }
    }

    /// Build the incremental scan stream.
    ///
    /// Runs version validation, the protocol-feature check, and the snapshot-covers-the-range
    /// check upfront. If the range can be served, returns `Some(stream)` for the consumer to
    /// drive batch-by-batch. Returns `None` when the target snapshot's commit list cannot serve
    /// `(base_version, target_version]`; consumers should fall back to a full scan via
    /// [`crate::Snapshot::scan_builder`].
    ///
    /// # Errors
    /// - `Err` if `base_version >= snapshot.version()` (caller error).
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
        let target_version = self.snapshot.version();
        require!(
            self.base_version < target_version,
            Error::generic(format!(
                "IncrementalScanBuilder: base_version ({}) must be less than target_version ({})",
                self.base_version, target_version
            ))
        );

        // Confirm kernel supports the target's reader features. Mirrors `Scan::new`. The
        // intermediate commits in `(base_version, target_version]` are read for their `add`
        // and `remove` actions only -- their schemas/stats are passed through opaquely to
        // the caller, who is expected to interpret them against the target snapshot.
        self.snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Scan)?;

        // TODO(#2493): replace this block with `CommitRange` once it exists.
        // Today we use the snapshot's already-validated commit list rather than re-listing
        // storage. This is correct for catalog-managed tables: the snapshot's log_segment
        // includes any staged/ratified-but-unpublished commits passed in via `with_log_tail`,
        // which a fresh storage listing would silently miss. `CommitRange` will own this
        // listing-and-validation responsibility centrally.
        let snapshot_commits = &self.snapshot.log_segment().listed.ascending_commit_files;
        let snapshot_first_version = snapshot_commits.first().map(|c| c.version);

        // TODO(#2493): this "snapshot covers range" check becomes a typed error from
        // `CommitRange::build` (e.g. `Error::CommitsUnavailable { missing: Range<Version> }`),
        // pattern-matched here to return `None`.
        let start_version = self.base_version + 1;
        if snapshot_first_version.is_none_or(|v| v > start_version) {
            return Ok(None);
        }

        // Collect in-range commit locations in newest-first order. Handing all of them to
        // `read_json_files` in a single call lets the engine's prefetch (e.g.
        // `DefaultEngine`'s `buffered(buffer_size)`) overlap object-store GETs across
        // commits, instead of paying first-byte latency once per commit serially.
        // Newest-first order gives `FileActionDeduplicator` newest-wins semantics across
        // the range.
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

/// Streaming output of an incremental scan: surviving Add batches in newest-first order,
/// followed by a terminal method that returns the surviving file-key sets.
///
/// Driving the iterator is what advances dedup state, so any of the terminal methods
/// drains anything the consumer hasn't pulled before computing the final state.
///
/// If the iterator surfaces an error, the dedup state becomes inconsistent. Calling a
/// terminal method on an errored stream returns `Err` rather than producing a stale
/// summary. The terminal methods consume `self`, so the type system prevents reuse.
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
    /// [`finish`]: Self::finish
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
            getters.len() == 8,
            Error::InternalError(format!(
                "IncrementalDedupVisitor expected 8 getters, got {}",
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
