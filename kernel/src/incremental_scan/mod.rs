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

/// Schema projected from each commit JSON: `add` and `remove` only.
static INCREMENTAL_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ]))
});

/// Builder for an incremental scan over `(base_version, target_version]`. Construct via
/// [`crate::Snapshot::incremental_scan_builder`] and drive with
/// [`IncrementalScanBuilder::build`].
#[derive(Debug)]
pub struct IncrementalScanBuilder {
    target_snapshot: SnapshotRef,
    base_version: Version,
}

impl IncrementalScanBuilder {
    /// Create a new builder for the range `(base_version, target_snapshot.version()]`.
    pub(crate) fn new(target_snapshot: impl Into<SnapshotRef>, base_version: Version) -> Self {
        Self {
            target_snapshot: target_snapshot.into(),
            base_version,
        }
    }

    /// Build the incremental scan stream, or `None` if the target snapshot's commit list
    /// cannot serve `(base_version, target_version]` (consumers should fall back to a full
    /// scan via [`crate::Snapshot::scan_builder`]).
    ///
    /// Does not re-list `_delta_log/`: walks the target snapshot's already-validated commit
    /// list and clips it to `(base_version, target_version]`. For catalog-managed tables
    /// that's important — the snapshot's `log_tail` carries staged commits that a fresh
    /// storage listing would miss.
    ///
    /// Validates only the target's reader features via [`Operation::Scan`]. By the protocol's
    /// feature-immutability rule, the target's `readerFeatures` is a superset of every
    /// reader feature used in any commit in `(base_version, target_version]`. Of the fields
    /// kernel itself decodes from each row (path + deletionVector.*), only
    /// `deletionVector.*` is feature-gated, and pre-`deletionVectors`-enable commits cannot
    /// populate it. Consumers that decode pass-through fields (stats, partitionValues,
    /// baseRowId) should interpret each row against the protocol at that row's commit
    /// version, not naively against the target snapshot.
    ///
    /// # Errors
    /// - `Err` if `base_version >= target_snapshot.version()` (caller error).
    /// - `Err` if the target snapshot's protocol contains an unsupported reader feature.
    /// - `Err` if the engine fails to open the commit stream.
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

        // TODO(#2552): surface in-range protocol/metadata changes to the consumer.
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
            live_adds: HashSet::new(),
            removes: HashSet::new(),
            errored: false,
        }))
    }
}

/// Streaming output of an incremental scan over `(base_version, target_version]`.
/// Yields live Add batches as [`FilteredEngineData`] in newest-first order via
/// [`Iterator::next`]; call [`into_summary`] or [`into_listing`] to terminate and recover
/// the live file-key sets.
///
/// An Add is "live" at the target if, walking commits newest-first and keying by
/// `(path, dv_unique_id)`, it is the first occurrence of that key (first-seen-wins dedup)
/// and no later commit in the range contains a Remove for the same key — i.e. the file is
/// still in the table at `target_version`.
///
/// On error, `next()` yields `Some(Err(_))` once and returns `None` on every subsequent
/// call; the stream's dedup state is incomplete, so terminal methods then return `Err`
/// rather than producing a partial summary.
///
/// [`into_summary`]: Self::into_summary
/// [`into_listing`]: Self::into_listing
pub struct IncrementalScanStream {
    base_version: Version,
    target_version: Version,
    actions: FileDataReadResultIterator,
    seen_file_keys: HashSet<FileActionKey>,
    live_adds: HashSet<FileActionKey>,
    removes: HashSet<FileActionKey>,
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
                    &mut self.live_adds,
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
    /// Drain any unread batches, then return the live file-key sets without applying
    /// cross-snapshot dedup. Consumers can intersect these against whatever base
    /// data structure they prefer.
    ///
    /// # Errors
    /// Returns `Err` if the stream previously yielded an error (dedup state is incomplete),
    /// or if draining produces an error.
    pub fn into_summary(mut self) -> DeltaResult<IncrementalScanSummary> {
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
            live_adds: self.live_adds,
            removes: self.removes,
        })
    }

    /// Eager helper: collect every live Add batch into a [`Vec`], then call
    /// [`into_summary`] to recover the file-key sets. Use this when the diff fits in memory
    /// and the consumer prefers the simpler eager shape over the iterator pattern.
    ///
    /// The returned `Vec` may contain multiple batches per source commit: the engine is
    /// free to split a commit's rows across [`ActionsBatch`] yields (e.g. by JSON-reader
    /// batch-size limits). One yielded batch produces at most one `Vec` entry, and a
    /// commit whose Adds were all cancelled by later Removes produces no entry at all.
    ///
    /// [`into_summary`]: Self::into_summary
    /// [`ActionsBatch`]: crate::log_replay::ActionsBatch
    pub fn into_listing(mut self) -> DeltaResult<IncrementalListing> {
        let mut add_files: Vec<FilteredEngineData> = Vec::new();
        for item in self.by_ref() {
            add_files.push(item?);
        }
        let summary = self.into_summary()?;
        Ok(IncrementalListing { summary, add_files })
    }

    /// Drain any unread batches, then intersect `base_keys` against the live-Add file-key
    /// set to compute `duplicate_adds` (file keys in the consumer's base listing that the
    /// range re-adds with new metadata, e.g. OPTIMIZE / liquid clustering re-tag).
    ///
    /// Streaming form: accepts borrowed keys from any source (`&HashSet`, `&Vec`, `&[…]`,
    /// `map.keys()`, custom iterator). Kernel streams `base_keys` once and probes the
    /// live-Add set per element. Work is `O(|base|)` with no call-site clones. Use this
    /// when your cache holds keys in a streamable shape and you don't have a faster
    /// `contains` lookup.
    ///
    /// Prefer [`into_summary_against_base_with`] when your cache supports
    /// `contains`-style lookup (HashMap, HashSet, BTreeMap, custom index). That form
    /// iterates the (typically much smaller) live-Add set and probes your base via the
    /// supplied closure, dropping work from `O(|base|)` to `O(|live_adds|)`.
    ///
    /// [`into_summary`]: Self::into_summary
    /// [`into_summary_against_base_with`]: Self::into_summary_against_base_with
    pub fn into_summary_against_base<'a>(
        self,
        base_keys: impl IntoIterator<Item = &'a FileActionKey>,
    ) -> DeltaResult<IncrementalScanSummaryAgainstBase> {
        let summary = self.into_summary()?;
        let duplicate_adds: HashSet<FileActionKey> = base_keys
            .into_iter()
            .filter(|k| summary.live_adds.contains(*k))
            .cloned()
            .collect();
        Ok(IncrementalScanSummaryAgainstBase {
            base_version: summary.base_version,
            target_version: summary.target_version,
            duplicate_adds,
            removes: summary.removes,
        })
    }

    /// Predicate form of [`into_summary_against_base`]: pass a closure that answers
    /// "is this key in your base?" Iterates the live-Add set (typically much smaller than
    /// the base) and calls `base_contains` once per live-Add key.
    ///
    /// Works with any base shape that supports membership lookup:
    /// - `HashMap<FileActionKey, _>` -> `|k| map.contains_key(k)`
    /// - `HashSet<FileActionKey>` -> `|k| set.contains(k)`
    /// - `BTreeMap<FileActionKey, _>` -> `|k| map.contains_key(k)`
    /// - Sorted `Vec<FileActionKey>` -> `|k| vec.binary_search(k).is_ok()`
    ///
    /// [`into_summary_against_base`]: Self::into_summary_against_base
    pub fn into_summary_against_base_with(
        self,
        base_contains: impl Fn(&FileActionKey) -> bool,
    ) -> DeltaResult<IncrementalScanSummaryAgainstBase> {
        let summary = self.into_summary()?;
        let duplicate_adds: HashSet<FileActionKey> = summary
            .live_adds
            .iter()
            .filter(|k| base_contains(k))
            .cloned()
            .collect();
        Ok(IncrementalScanSummaryAgainstBase {
            base_version: summary.base_version,
            target_version: summary.target_version,
            duplicate_adds,
            removes: summary.removes,
        })
    }

    /// Eager classified helper: collect every live Add batch and call
    /// [`into_summary_against_base`] against `base_keys`. Returns an
    /// [`IncrementalListingAgainstBase`] with the classified summary.
    ///
    /// Prefer [`into_listing_against_base_with`] when your base supports `contains`-style
    /// lookup — see [`into_summary_against_base_with`] for the rationale.
    ///
    /// [`into_summary_against_base`]: Self::into_summary_against_base
    /// [`into_summary_against_base_with`]: Self::into_summary_against_base_with
    /// [`into_listing_against_base_with`]: Self::into_listing_against_base_with
    pub fn into_listing_against_base<'a>(
        mut self,
        base_keys: impl IntoIterator<Item = &'a FileActionKey>,
    ) -> DeltaResult<IncrementalListingAgainstBase> {
        let mut add_files: Vec<FilteredEngineData> = Vec::new();
        for item in self.by_ref() {
            add_files.push(item?);
        }
        let summary = self.into_summary_against_base(base_keys)?;
        Ok(IncrementalListingAgainstBase { summary, add_files })
    }

    /// Predicate form of [`into_listing_against_base`]; see
    /// [`into_summary_against_base_with`] for the rationale.
    ///
    /// [`into_listing_against_base`]: Self::into_listing_against_base
    /// [`into_summary_against_base_with`]: Self::into_summary_against_base_with
    pub fn into_listing_against_base_with(
        mut self,
        base_contains: impl Fn(&FileActionKey) -> bool,
    ) -> DeltaResult<IncrementalListingAgainstBase> {
        let mut add_files: Vec<FilteredEngineData> = Vec::new();
        for item in self.by_ref() {
            add_files.push(item?);
        }
        let summary = self.into_summary_against_base_with(base_contains)?;
        Ok(IncrementalListingAgainstBase { summary, add_files })
    }
}

impl std::fmt::Debug for IncrementalScanStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalScanStream")
            .field("base_version", &self.base_version)
            .field("target_version", &self.target_version)
            .field("live_adds", &self.live_adds.len())
            .field("removes", &self.removes.len())
            .field("errored", &self.errored)
            .finish()
    }
}

/// Live file-key sets without cross-snapshot classification, returned by
/// [`IncrementalScanStream::into_summary`].
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
    /// File keys of live Add actions in `(base_version, target_version]` (i.e. Adds whose
    /// `(path, dv_unique_id)` is still present at `target_version`).
    pub live_adds: HashSet<FileActionKey>,
    /// File keys of Remove actions in `(base_version, target_version]`, deduped
    /// first-seen-wins by `(path, dv_unique_id)`.
    pub removes: HashSet<FileActionKey>,
}

/// Eager output of [`IncrementalScanStream::into_listing`]: the buffered Add
/// batches plus the summary (no cross-snapshot classification).
#[non_exhaustive]
pub struct IncrementalListing {
    /// Live Add and Remove file-key sets for the range.
    pub summary: IncrementalScanSummary,
    /// All live Add batches in descending commit-version order. One entry per source
    /// commit batch that produced any live Adds.
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

/// Cross-snapshot-classified file-key sets, returned by
/// [`IncrementalScanStream::into_summary_against_base`].
///
/// To advance a delta-on-base file listing cache, append the streamed Add batches to
/// the delta layer and use the union `removes U duplicate_adds` as the remove-mask
/// against the base. Both sets are required: `removes` masks files that left the table;
/// `duplicate_adds` masks the stale base entry of each file the range re-added with new
/// metadata. Matching against the full `(path, dv_unique_id)` key (not path alone) keeps
/// distinct DV-revision entries separate.
#[derive(Debug)]
#[non_exhaustive]
pub struct IncrementalScanSummaryAgainstBase {
    /// Exclusive lower bound of the scan range.
    pub base_version: Version,
    /// Inclusive upper bound; equals the source snapshot's version.
    pub target_version: Version,
    /// File keys from the live Add stream that also appear in the consumer's base
    /// listing (metadata-only re-adds, e.g. OPTIMIZE / liquid clustering re-tag).
    /// The corresponding rows are still in the streamed Adds.
    pub duplicate_adds: HashSet<FileActionKey>,
    /// File keys of Remove actions in the range. Consumers must union this with
    /// `duplicate_adds` when masking the base.
    pub removes: HashSet<FileActionKey>,
}

/// Eager output of [`IncrementalScanStream::into_listing_against_base`]: the buffered
/// Add batches plus the classified summary.
#[non_exhaustive]
pub struct IncrementalListingAgainstBase {
    /// Classified file-key sets for the range; see [`IncrementalScanSummaryAgainstBase`].
    pub summary: IncrementalScanSummaryAgainstBase,
    /// All live Add batches in descending commit-version order.
    pub add_files: Vec<FilteredEngineData>,
}

impl std::fmt::Debug for IncrementalListingAgainstBase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalListingAgainstBase")
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
    live_adds: &mut HashSet<FileActionKey>,
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
        live_adds,
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
// incremental scan needs both live Adds and Removes exposed (keyed by
// `(path, dv_unique_id)`), and it deliberately skips the scan-only per-row work, so a
// slimmer visitor is the right shape here.
struct IncrementalDedupVisitor<'a, 'seen> {
    deduplicator: FileActionDeduplicator<'seen>,
    adds_sel: &'a mut [bool],
    live_adds: &'a mut HashSet<FileActionKey>,
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
                self.live_adds.insert(key);
            } else {
                self.removes.insert(key);
            }
        }

        Ok(())
    }
}
