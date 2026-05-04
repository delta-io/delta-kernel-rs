//! Incremental scan API for advancing a cached file listing from a base version to a target
//! version.
//!
//! The flow:
//! 1. [`IncrementalScanBuilder::build`] returns either an [`IncrementalScanStream`] (success) or
//!    [`IncrementalScanResult::CommitsUnavailable`] when the snapshot's log_segment cannot serve
//!    `(base_version, target_version]`. The most common cause is that the snapshot's commit list
//!    starts past `base_version` (loaded from a checkpoint at version `c >= base_version + 1`, or
//!    older JSONs were vacuumed away).
//! 2. The stream is an [`Iterator`] over surviving Add batches in newest-first order. Each item is
//!    one [`FilteredEngineData`]: the underlying columnar buffer the engine read, with a selection
//!    vector that picks out the surviving Add rows. Commits whose Adds were all cancelled by later
//!    Removes do not produce an item.
//! 3. Call one of the terminal methods to consume the stream:
//!    - [`IncrementalScanStream::finish_raw`] returns the surviving path sets, no cross-snapshot
//!      classification.
//!    - [`IncrementalScanStream::finish`] takes the consumer's base file paths and classifies
//!      metadata-only re-adds (paths from the surviving Adds that also exist in the base).
//!    - [`IncrementalScanStream::collect_listing`] is eager sugar that collects every surviving Add
//!      batch and returns an [`IncrementalListing`]. This is the only path that buffers Add batches
//!      in memory; the streaming `finish` / `finish_raw` paths do not.
//!
//! V2 checkpoint sidecars and log compaction files in the range are not consulted; the stream
//! reads commit JSONs directly and ignores both. Compaction files are an optimization over the
//! same underlying actions, and V2 checkpoint sidecars are not aligned to range boundaries.
//!
//! Memory: the stream holds one [`HashSet`] of surviving Add paths and one of surviving
//! Remove paths (sized in `O(adds_in_range)` and `O(removes_in_range)`). It reads commit
//! JSONs one at a time and drops each batch after dedup. Output buffering is
//! consumer-controlled (`finish`/`finish_raw` are streaming; `collect_listing` is eager).
//!
//! [`HashSet`]: std::collections::HashSet
//!
//! # Example
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
//! # use delta_kernel::engine::default::storage::store_from_url;
//! # use delta_kernel::incremental_scan::{IncrementalScanBuilder, IncrementalScanResult};
//! # use delta_kernel::{Snapshot, DeltaResult, Error};
//! # let url = delta_kernel::try_parse_uri("./tests/data/basic_partitioned")?;
//! # let engine = Arc::new(DefaultEngineBuilder::new(store_from_url(&url)?).build());
//! let target = Snapshot::builder_for(url).at_version(1).build(engine.as_ref())?;
//! let base_paths: Vec<&str> = vec![]; // empty base for this example
//! let result = IncrementalScanBuilder::new(target, 0).build(engine.as_ref())?;
//!
//! match result {
//!     IncrementalScanResult::Stream(mut stream) => {
//!         // Consume surviving Add batches as they arrive (or buffer them).
//!         while let Some(batch) = stream.next() {
//!             let _batch = batch?; // append to delta cache
//!         }
//!         // Then finalize: kernel intersects base_paths with the surviving Adds.
//!         let footer = stream.finish(base_paths.iter().copied())?;
//!         // mask = footer.remove_files U footer.duplicate_add_paths (set union)
//!         let _ = footer;
//!     }
//!     IncrementalScanResult::CommitsUnavailable => { /* full scan fallback */ }
//!     _ => unreachable!(),
//! }
//! # Ok::<(), Error>(())
//! ```

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::actions::{Add, Remove, ADD_NAME, REMOVE_NAME};
use crate::engine_data::{FilteredEngineData, GetData, RowVisitor};
use crate::expressions::{column_name, ColumnName};
use crate::log_replay::deduplicator::Deduplicator;
use crate::log_replay::{FileActionDeduplicator, FileActionKey};
use crate::path::ParsedLogPath;
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, ToSchema};
use crate::snapshot::SnapshotRef;
use crate::table_features::Operation;
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, FileDataReadResultIterator, Version};

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
    pub fn new(snapshot: impl Into<SnapshotRef>, base_version: Version) -> Self {
        Self {
            snapshot: snapshot.into(),
            base_version,
        }
    }

    /// Build the incremental scan stream.
    ///
    /// Runs version validation, the protocol-feature check, and the snapshot-covers-the-range
    /// check upfront. If the range can be served, returns an [`IncrementalScanStream`] that
    /// the consumer drives commit-by-commit. Otherwise returns
    /// [`IncrementalScanResult::CommitsUnavailable`] and the consumer should fall back to a
    /// full scan via [`crate::Snapshot::scan_builder`].
    ///
    /// # Errors
    /// - `Err` if `base_version >= snapshot.version()` (caller error).
    /// - `Err` if the target snapshot's protocol contains an unsupported reader feature.
    ///
    /// I/O errors and other read failures surface from the [`IncrementalScanStream`] itself
    /// (or from its terminal methods), not here.
    pub fn build<'a>(self, engine: &'a dyn Engine) -> DeltaResult<IncrementalScanResult<'a>> {
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
        // pattern-matched here to produce `IncrementalScanResult::CommitsUnavailable`.
        let start_version = self.base_version + 1;
        if snapshot_first_version.is_none_or(|v| v > start_version) {
            return Ok(IncrementalScanResult::CommitsUnavailable);
        }

        // Collect the in-range commits in ascending order. The stream pops from the end so
        // the iteration order is newest-first, which gives `FileActionDeduplicator`
        // newest-wins semantics across the range.
        let commits_remaining: Vec<ParsedLogPath> = snapshot_commits
            .iter()
            .filter(|c| c.version >= start_version && c.version <= target_version)
            .cloned()
            .collect();

        Ok(IncrementalScanResult::Stream(IncrementalScanStream {
            base_version: self.base_version,
            target_version,
            engine,
            commits_remaining,
            current_batches: None,
            seen_file_keys: HashSet::new(),
            surviving_add_paths: HashSet::new(),
            remove_files: HashSet::new(),
            errored: false,
        }))
    }
}

/// Outcome of [`IncrementalScanBuilder::build`].
//
// The size disparity between `Stream` (carries the dedup state) and `CommitsUnavailable`
// (unit) is intentional. Boxing the stream would force every consumer through a heap
// allocation on the common path; consumers destructure this enum immediately after
// `build()`, so the move is one-shot and the size is fine.
#[allow(clippy::large_enum_variant)]
#[non_exhaustive]
pub enum IncrementalScanResult<'a> {
    /// Drive this stream commit-by-commit, then call one of the terminal methods
    /// ([`IncrementalScanStream::finish`], [`IncrementalScanStream::finish_raw`], or
    /// [`IncrementalScanStream::collect_listing`]) to finalize.
    Stream(IncrementalScanStream<'a>),
    /// The snapshot's log_segment cannot serve `(base_version, target_version]`.
    /// Consumers should fall back to a full scan via [`crate::Snapshot::scan_builder`].
    CommitsUnavailable,
}

impl std::fmt::Debug for IncrementalScanResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(s) => f.debug_tuple("Stream").field(s).finish(),
            Self::CommitsUnavailable => f.write_str("CommitsUnavailable"),
        }
    }
}

/// Streaming output of an incremental scan: surviving Add batches in newest-first order,
/// followed by a terminal method that returns the surviving path sets.
///
/// Driving the iterator is what advances dedup state, so any of the terminal methods
/// drains anything the consumer hasn't pulled before computing the final state.
///
/// If the iterator surfaces an error, the dedup state becomes inconsistent. Calling a
/// terminal method on an errored stream returns `Err` rather than producing a stale
/// footer. The terminal methods consume `self`, so the type system prevents reuse.
pub struct IncrementalScanStream<'a> {
    base_version: Version,
    target_version: Version,
    engine: &'a dyn Engine,
    /// Commits to read, in ascending order. We pop from the end so iteration is newest-first.
    commits_remaining: Vec<ParsedLogPath>,
    /// Batch iterator for the commit currently being read. `None` between commits.
    current_batches: Option<FileDataReadResultIterator>,
    seen_file_keys: HashSet<FileActionKey>,
    surviving_add_paths: HashSet<String>,
    remove_files: HashSet<String>,
    /// Set when the iterator yields an `Err`. Subsequent `next()` calls return `None`;
    /// terminal methods return `Err`.
    errored: bool,
}

impl Iterator for IncrementalScanStream<'_> {
    type Item = DeltaResult<FilteredEngineData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        loop {
            // Take the in-flight batch iterator, or open one for the next commit. We use
            // `Option::insert` rather than a separate `is_none()` check followed by an
            // unwrap so the borrow checker sees a single mutable borrow path.
            let batches = match &mut self.current_batches {
                Some(b) => b,
                None => {
                    let commit = self.commits_remaining.pop()?;
                    let result = self.engine.json_handler().read_json_files(
                        std::slice::from_ref(&commit.location),
                        INCREMENTAL_READ_SCHEMA.clone(),
                        None,
                    );
                    match result {
                        Ok(b) => self.current_batches.insert(b),
                        Err(e) => {
                            self.errored = true;
                            return Some(Err(e));
                        }
                    }
                }
            };

            match batches.next() {
                None => {
                    // Current commit exhausted; loop to the next commit.
                    self.current_batches = None;
                    continue;
                }
                Some(Err(e)) => {
                    self.errored = true;
                    return Some(Err(e));
                }
                Some(Ok(batch)) => {
                    match process_batch(
                        batch,
                        &mut self.seen_file_keys,
                        &mut self.surviving_add_paths,
                        &mut self.remove_files,
                    ) {
                        Ok(Some(filtered)) => return Some(Ok(filtered)),
                        Ok(None) => continue,
                        Err(e) => {
                            self.errored = true;
                            return Some(Err(e));
                        }
                    }
                }
            }
        }
    }
}

impl IncrementalScanStream<'_> {
    /// Drain any unread commits, then return the surviving path sets without applying
    /// cross-snapshot dedup. Consumers can intersect these against whatever base
    /// data structure they prefer.
    ///
    /// # Errors
    /// Returns `Err` if the stream previously yielded an error (dedup state is incomplete),
    /// or if draining produces an error.
    pub fn finish_raw(mut self) -> DeltaResult<IncrementalScanRawFooter> {
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
        Ok(IncrementalScanRawFooter {
            base_version: self.base_version,
            target_version: self.target_version,
            surviving_add_paths: self.surviving_add_paths,
            remove_files: self.remove_files,
        })
    }

    /// Drain any unread commits, then intersect `base_paths` against the surviving-Add
    /// path set to compute `duplicate_add_paths`. Sugar over [`finish_raw`] plus a single
    /// pass over `base_paths`.
    ///
    /// `base_paths` is iterated exactly once. Memory stays `O(surviving_adds)`; kernel
    /// does not materialize a HashSet of base paths.
    ///
    /// [`finish_raw`]: Self::finish_raw
    pub fn finish<P: AsRef<str>>(
        self,
        base_paths: impl IntoIterator<Item = P>,
    ) -> DeltaResult<IncrementalListingFooter> {
        let raw = self.finish_raw()?;
        let duplicate_add_paths: HashSet<String> = base_paths
            .into_iter()
            .filter(|p| raw.surviving_add_paths.contains(p.as_ref()))
            .map(|p| p.as_ref().to_string())
            .collect();
        Ok(IncrementalListingFooter {
            base_version: raw.base_version,
            target_version: raw.target_version,
            duplicate_add_paths,
            remove_files: raw.remove_files,
        })
    }

    /// Eager helper: collect every surviving Add batch into a [`Vec`], then call
    /// [`finish`] to produce a fully-classified [`IncrementalListing`]. Use this when
    /// the diff fits in memory and the consumer prefers the simpler eager shape.
    ///
    /// [`finish`]: Self::finish
    pub fn collect_listing<P: AsRef<str>>(
        mut self,
        base_paths: impl IntoIterator<Item = P>,
    ) -> DeltaResult<IncrementalListing> {
        let mut add_files: Vec<FilteredEngineData> = Vec::new();
        for item in self.by_ref() {
            add_files.push(item?);
        }
        let footer = self.finish(base_paths)?;
        Ok(IncrementalListing { footer, add_files })
    }
}

impl std::fmt::Debug for IncrementalScanStream<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalScanStream")
            .field("base_version", &self.base_version)
            .field("target_version", &self.target_version)
            .field("commits_remaining", &self.commits_remaining.len())
            .field("surviving_add_paths", &self.surviving_add_paths.len())
            .field("remove_files", &self.remove_files.len())
            .field("errored", &self.errored)
            .finish()
    }
}

/// Surviving path sets without cross-snapshot classification, returned by
/// [`IncrementalScanStream::finish_raw`].
#[derive(Debug)]
#[non_exhaustive]
pub struct IncrementalScanRawFooter {
    /// Exclusive lower bound of the scan range, as supplied to [`IncrementalScanBuilder`].
    pub base_version: Version,
    /// Inclusive upper bound; equals the source snapshot's version.
    pub target_version: Version,
    /// Paths of surviving Add actions in `(base_version, target_version]`.
    pub surviving_add_paths: HashSet<String>,
    /// Paths of surviving Remove actions in `(base_version, target_version]`.
    pub remove_files: HashSet<String>,
}

/// Cross-snapshot-classified path sets, returned by [`IncrementalScanStream::finish`].
///
/// To advance a delta-on-base file listing cache, append the streamed Add batches to
/// the delta layer and use the union `remove_files U duplicate_add_paths` as the
/// remove-mask against the base. Both sets are required: `remove_files` masks paths
/// that left the table; `duplicate_add_paths` masks the stale base entry of each
/// path that the range re-added with new metadata.
#[derive(Debug)]
#[non_exhaustive]
pub struct IncrementalListingFooter {
    /// Exclusive lower bound of the scan range.
    pub base_version: Version,
    /// Inclusive upper bound; equals the source snapshot's version.
    pub target_version: Version,
    /// Paths from the surviving Add stream that also appear in the consumer's base
    /// listing (metadata-only re-adds, e.g. OPTIMIZE / liquid clustering re-tag).
    /// Surfaced separately so consumers can mask matching base entries or mirror the
    /// set for telemetry. The corresponding rows are still in the streamed Adds.
    pub duplicate_add_paths: HashSet<String>,
    /// Paths of surviving Remove actions. Consumers must union this with
    /// `duplicate_add_paths` when masking the base; the union is what makes
    /// metadata-only re-adds correct (the new Add is the live entry, the stale
    /// base copy must be masked).
    pub remove_files: HashSet<String>,
}

/// Eager output of [`IncrementalScanStream::collect_listing`]: the buffered Add batches
/// plus the classified footer.
#[non_exhaustive]
pub struct IncrementalListing {
    /// Classified path sets for the range; see [`IncrementalListingFooter`].
    pub footer: IncrementalListingFooter,
    /// All surviving Add batches in descending commit-version order. One entry per source
    /// commit batch that produced any surviving Adds.
    pub add_files: Vec<FilteredEngineData>,
}

impl std::fmt::Debug for IncrementalListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalListing")
            .field("footer", &self.footer)
            .field("add_files_batch_count", &self.add_files.len())
            .finish()
    }
}

/// Visit one commit-batch, updating dedup state and accumulators. Returns the filtered Add
/// rows for this batch, or `None` if no Adds survived dedup in this batch.
fn process_batch(
    batch: Box<dyn EngineData>,
    seen_file_keys: &mut HashSet<FileActionKey>,
    surviving_add_paths: &mut HashSet<String>,
    remove_files: &mut HashSet<String>,
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
        surviving_add_paths,
        remove_files,
    };
    visitor.visit_rows_of(batch.as_ref())?;

    if !adds_sel.iter().any(|s| *s) {
        return Ok(None);
    }
    Ok(Some(FilteredEngineData::try_new(batch, adds_sel)?))
}

// === Visitor ===

// Indices of the leaf columns visited per row; must match `selected_column_names_and_types`.
const ADD_PATH_INDEX: usize = 0;
const ADD_DV_START_INDEX: usize = 1; // .storageType, .pathOrInlineDv, .offset
const REMOVE_PATH_INDEX: usize = 4;
const REMOVE_DV_START_INDEX: usize = 5; // .storageType, .pathOrInlineDv, .offset

struct IncrementalDedupVisitor<'a, 'seen> {
    deduplicator: FileActionDeduplicator<'seen>,
    adds_sel: &'a mut [bool],
    surviving_add_paths: &'a mut HashSet<String>,
    remove_files: &'a mut HashSet<String>,
}

impl RowVisitor for IncrementalDedupVisitor<'_, '_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
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
            let path = key.path.clone();
            if self.deduplicator.check_and_record_seen(key) {
                continue;
            }

            if is_add {
                self.adds_sel[i] = true;
                self.surviving_add_paths.insert(path);
            } else {
                self.remove_files.insert(path);
            }
        }

        Ok(())
    }
}
