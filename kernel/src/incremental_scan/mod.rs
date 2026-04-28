//! Incremental scan API for advancing a cached file listing from a base version to a target
//! version.
//!
//! [`IncrementalScanBuilder`] takes a target [`crate::Snapshot`] and a base version, reads the
//! commit range `(base_version, target_version]`, performs within-range deduplication on
//! `(path, dv_unique_id)`, and returns:
//!
//! - `add_files`: surviving Add actions (one [`FilteredEngineData`] per source commit batch);
//! - `duplicate_add_paths`: paths from `add_files` that also exist in the consumer's base listing
//!   (metadata-only re-adds, e.g. OPTIMIZE / liquid clustering re-tag);
//! - `remove_files`: paths of surviving Remove actions.
//!
//! The output is structured to map 1:1 onto a delta-on-base file listing cache: append
//! `add_files` to the delta layer, and union `remove_files` with `duplicate_add_paths` to
//! mask base entries.
//!
//! Memory: kernel holds one HashSet of surviving Add paths plus the buffered commit batches;
//! it never materializes a HashSet of base paths. The base path iterator is streamed once.
//!
//! # Example
//!
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
//! let result = IncrementalScanBuilder::new(target, 0)
//!     .build(engine.as_ref(), base_paths.iter().copied())?;
//!
//! match result {
//!     IncrementalScanResult::Listing(listing) => {
//!         for batch in listing.add_files { /* append to delta cache */ }
//!         // mask = listing.remove_files ∪ listing.duplicate_add_paths
//!     }
//!     IncrementalScanResult::CommitsUnavailable => { /* full scan fallback */ }
//! }
//! # Ok::<(), Error>(())
//! ```

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use crate::actions::{ADD_NAME, REMOVE_NAME};
use crate::commit_range::{CommitDataBatch, CommitRange};
use crate::engine_data::{FilteredEngineData, GetData, RowVisitor};
use crate::expressions::{column_name, ColumnName};
use crate::log_replay::deduplicator::Deduplicator;
use crate::log_replay::{FileActionDeduplicator, FileActionKey};
use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType};
use crate::snapshot::SnapshotRef;
use crate::table_features::Operation;
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, Error, Version};

#[cfg(test)]
mod tests;

/// Schema projected from each commit JSON: `add` and `remove` only. Protocol and metadata
/// updates between base and target are validated against the target snapshot's protocol via
/// [`Operation::Scan`], which is sufficient because protocol features are monotonic.
static INCREMENTAL_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    use crate::actions::{Add, Remove};
    use crate::schema::ToSchema as _;
    Arc::new(StructType::new_unchecked([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
    ]))
});

// === Public API ===

/// Builder for an incremental scan over `(base_version, target_version]`. The target version
/// is taken from the supplied snapshot; the base version is supplied at construction.
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

    /// Drive the incremental scan.
    ///
    /// # Parameters
    /// - `engine`: kernel engine handle for storage and JSON reading.
    /// - `base_file_paths`: every file path live in the consumer's cached listing at
    ///   `base_version`. Iterated exactly once after within-range dedup, intersected against the
    ///   surviving-Add path set to compute `duplicate_add_paths`. Kernel does not materialize a
    ///   HashSet of base paths.
    ///
    /// # Errors
    /// - `Err` if `base_version >= snapshot.version()` (caller error).
    /// - `Err` if the target snapshot's protocol contains an unsupported reader feature.
    /// - `Err` for I/O failures other than missing commits.
    ///
    /// # Returns
    /// [`IncrementalScanResult::Listing`] on success, or
    /// [`IncrementalScanResult::CommitsUnavailable`] when the commit range cannot be served
    /// (e.g. log retention removed commit JSONs in the range).
    pub fn build<'a>(
        self,
        engine: &dyn Engine,
        base_file_paths: impl IntoIterator<Item = &'a str>,
    ) -> DeltaResult<IncrementalScanResult> {
        let target_version = self.snapshot.version();
        require!(
            self.base_version < target_version,
            Error::generic(format!(
                "IncrementalScanBuilder: base_version ({}) must be less than target_version ({})",
                self.base_version, target_version
            ))
        );

        // Match `Scan::new`: confirm kernel supports every reader feature on the target.
        // Protocol features are monotonic (cannot be removed), so the target's feature set is
        // a superset of every intermediate commit's; checking only the target is sufficient.
        self.snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Scan)?;

        // List contiguous commits in (base_version, target_version] via CommitRange.
        // Listing failures (typically log retention removing JSONs that a checkpoint covers)
        // surface as CommitsUnavailable so the consumer can fall back to a full scan.
        let log_root = self.snapshot.log_segment().log_root.clone();
        let commit_range =
            match CommitRange::try_new(engine, log_root, self.base_version + 1, target_version) {
                Ok(r) => r,
                Err(_) => return Ok(IncrementalScanResult::CommitsUnavailable),
            };

        // Walk commits newest-first so `FileActionDeduplicator` (first-seen (path, dv) wins)
        // yields newest-wins semantics across the range.
        let mut seen_file_keys: HashSet<FileActionKey> = HashSet::new();
        let mut surviving_add_paths: HashSet<String> = HashSet::new();
        let mut remove_files: HashSet<String> = HashSet::new();
        let mut add_files: Vec<FilteredEngineData> = Vec::new();

        for batch in commit_range.iter_rev(engine, INCREMENTAL_READ_SCHEMA.clone()) {
            let CommitDataBatch { data, .. } = batch?;
            if let Some(filtered) = process_batch(
                data,
                &mut seen_file_keys,
                &mut surviving_add_paths,
                &mut remove_files,
            )? {
                add_files.push(filtered);
            }
        }

        // Intersect base paths with surviving-Add paths in a single linear scan over the
        // consumer's iterator. Lookup goes against the smaller, kernel-owned set.
        let mut duplicate_add_paths: HashSet<String> = HashSet::new();
        for path in base_file_paths {
            if surviving_add_paths.contains(path) {
                duplicate_add_paths.insert(path.to_string());
            }
        }

        Ok(IncrementalScanResult::Listing(IncrementalListing {
            base_version: self.base_version,
            target_version,
            add_files,
            duplicate_add_paths,
            remove_files,
        }))
    }
}

/// Outcome of [`IncrementalScanBuilder::build`].
pub enum IncrementalScanResult {
    /// The full diff over `(base_version, target_version]`.
    Listing(IncrementalListing),
    /// Commits in the range cannot be served. Most often caused by log retention removing
    /// commit JSONs that a checkpoint already covers. Consumers should fall back to a full
    /// scan via [`crate::Snapshot::scan_builder`].
    CommitsUnavailable,
}

/// Diff between a base version and a target version, suitable for advancing a delta-on-base
/// file listing cache.
pub struct IncrementalListing {
    pub base_version: Version,
    pub target_version: Version,
    /// All surviving Add actions in `(base_version, target_version]`. One entry per source
    /// commit batch, in descending commit-version order (newest first). Includes both
    /// "truly new" adds and metadata-only re-adds; consumers do not need to distinguish
    /// when applying to a delta layer.
    pub add_files: Vec<FilteredEngineData>,
    /// Paths from `add_files` whose paths also exist in the consumer's base file listing.
    /// Metadata-only re-adds (e.g. OPTIMIZE, liquid clustering re-tag). Surfaced separately
    /// from `remove_files` so consumers can mirror this set for masking stale base entries
    /// or telemetry. The corresponding rows are still present in `add_files`.
    pub duplicate_add_paths: HashSet<String>,
    /// Paths of surviving Remove actions in the range. Does not include
    /// `duplicate_add_paths`; consumers union as needed when applying the diff.
    pub remove_files: HashSet<String>,
}

// === Implementation ===

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
