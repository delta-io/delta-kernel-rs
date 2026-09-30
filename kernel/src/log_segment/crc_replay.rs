//! Reverse log replay for incremental CRC construction.
//!
//! Reverse-replays a log segment's commit files to produce a [`CrcDelta`] covering
//! commits `(X, Y]`. Per the incremental equation `Crc[X] + CrcDelta = Crc[Y]`, that
//! delta is applied to a stale base via [`Crc::apply`].
//!
//! The base `Crc[X]` itself can also be built here: [`LogSegment::build_crc_from_checkpoint`]
//! reads a checkpoint into a Complete CRC, and [`LogSegment::build_crc_from_version_zero`] builds
//! one from a full reverse replay when there is neither a CRC nor a checkpoint to root at.
//
// TODO(#2615): support log compaction files.

use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock};

use tracing::{instrument, warn};
use url::Url;

use super::LogSegment;
use crate::actions::visitors::{
    visit_metadata_at, visit_protocol_at, METADATA_LEAVES, PROTOCOL_LEAVES,
};
use crate::actions::{
    DomainMetadata, SetTransaction, ADD_NAME, COMMIT_INFO_NAME, DOMAIN_METADATA_FIELD,
    METADATA_FIELD, PROTOCOL_FIELD, REMOVE_NAME, SET_TRANSACTION_FIELD,
};
use crate::crc::{
    classify_file_stats_operation, read_crc_file_or_none, size_to_u64, Crc, CrcDelta,
    FileSizeHistogram, FileStatsDelta, FileStatsFailure, FileStatsFailureReason,
    FileStatsOperation, FileStatsSource, FileStatsValidity,
};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::metrics::ProtocolMetadataSource;
use crate::path::ParsedLogPath;
use crate::schema::{
    column_name, lazy_schema_ref, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec,
    SchemaRef, StructField,
};
use crate::snapshot::IncrementalReplay;
use crate::utils::require;
use crate::{DeltaResult, Engine, FileMeta, KernelError, RowVisitor, Version};

static REPLAY_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    // size is the only Add leaf the visitor reads, and it is required, so its presence marks
    // an Add row.
    nullable ADD_NAME: { not_null "size": LONG },
    // remove.size is optional, so we read remove.path (required) to know a row is a Remove
    // before reading its size.
    nullable REMOVE_NAME: {
        not_null "path": STRING,
        nullable "size": LONG,
    },
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (&SET_TRANSACTION_FIELD),
    (&DOMAIN_METADATA_FIELD),
    nullable COMMIT_INFO_NAME: {
        nullable "operation": STRING,
        nullable "inCommitTimestamp": LONG,
    },
    (StructField::create_metadata_column("_file", MetadataColumnSpec::FilePath)),
};

impl LogSegment {
    /// Try to build the CRC at this segment's `end_version` from the caller's resolved `base` CRC.
    /// Handles three cases:
    /// - Case 1: no base CRC available -> return None
    /// - Case 2: base CRC at `end_version` -> return it as-is
    /// - Case 3: stale base CRC older than `end_version` -> advance it to `end_version` when
    ///   `incremental_replay` permits, else fall back to normal log replay (return None)
    pub(crate) fn try_build_crc_within_budget(
        &self,
        engine: &dyn Engine,
        base: Option<&Arc<Crc>>,
        incremental_replay: IncrementalReplay,
    ) -> DeltaResult<Option<(Arc<Crc>, ProtocolMetadataSource)>> {
        let Some(base) = base else {
            return Ok(None);
        };
        if base.version == self.end_version {
            return Ok(Some((base.clone(), ProtocolMetadataSource::CrcAtTarget)));
        }
        if !incremental_replay.should_advance(base.version, self.end_version)? {
            return Ok(None);
        }
        let advanced = self.build_crc_from_base(engine, base)?;
        Ok(Some((
            Arc::new(advanced),
            ProtocolMetadataSource::CrcAdvancedByReplay,
        )))
    }

    /// Pick the latest CRC to use as an advance base: this segment's on-disk CRC or
    /// `in_memory_base`, whichever is newer, falling back to `in_memory_base` on a failed on-disk
    /// read. Drops a base below the checkpoint; returns None when no candidate remains.
    pub(crate) fn pick_latest_base_crc(
        &self,
        engine: &dyn Engine,
        in_memory_base: Option<&Arc<Crc>>,
    ) -> Option<Arc<Crc>> {
        let preferred_disk_crc = self
            .listed
            .latest_crc_file
            .as_ref()
            .filter(|f| in_memory_base.is_none_or(|m| f.version > m.version));
        preferred_disk_crc
            .and_then(|f| read_crc_file_or_none(engine, f))
            .or_else(|| in_memory_base.cloned())
            .filter(|crc| {
                self.checkpoint_version
                    .is_none_or(|ckpt| crc.version >= ckpt)
            })
    }

    /// Read this segment's latest on-disk CRC (`latest_crc_file`), at whatever version it sits.
    /// Returns None when there is no CRC file or the read fails. The returned CRC may be stale
    /// (older than `end_version`).
    pub(crate) fn read_latest_crc(&self, engine: &dyn Engine) -> Option<Arc<Crc>> {
        self.pick_latest_base_crc(engine, /* in_memory_base */ None)
    }

    /// Produce a fresh `Crc` at `self.end_version` by reverse-replaying the commits in
    /// `(base_crc.version, self.end_version]` and applying the resulting delta to
    /// `base_crc` via [`Crc::apply`].
    #[instrument(
        name = "log_seg.build_crc_from_base",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn build_crc_from_base(
        &self,
        engine: &dyn Engine,
        base_crc: &Crc,
    ) -> DeltaResult<Crc> {
        let seed_histogram = base_crc
            .file_stats()
            .and_then(|s| s.file_size_histogram())
            .map(|h| {
                FileSizeHistogram::create_empty_with_boundaries(h.sorted_bin_boundaries().to_vec())
            })
            .transpose()?;
        let delta = self.build_crc_delta_from_base(engine, base_crc.version, seed_histogram)?;
        Ok(base_crc.clone().apply(delta, self.end_version))
    }

    /// Build a Complete base [`Crc`] at `checkpoint_version` from this segment's checkpoint files.
    /// The tail commits are folded in separately by the caller as a [`CrcDelta`], so this reads
    /// only the checkpoint. Returns `None` when the segment has no checkpoint, or if protocol or
    /// metadata could not be recovered from it.
    ///
    /// File stats sum the checkpoint's AddFile actions (a reconciled checkpoint holds only live
    /// files). Domain metadata and set transactions are
    /// [`Complete`](DomainMetadataState::Complete) since a checkpoint is authoritative.
    /// `in_commit_timestamp_opt` is left `None`: a checkpoint carries no `commitInfo`, so the
    /// caller sets the ICT on the returned CRC afterward.
    pub(crate) fn build_crc_from_checkpoint(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Crc>> {
        let Some(version) = self.checkpoint_version else {
            return Ok(None);
        };
        // The checkpoint covers the full table, so `into_complete_crc` produces a Complete CRC.
        // Invalid Add sizes mark replay unsafe and degrade its file stats to `Indeterminate`.
        let mut acc = CrcReplayAccumulator::new(Some(FileSizeHistogram::create_default()));
        // Read only the checkpoint parquet plus any V2 sidecars via `create_checkpoint_stream`.
        let batches = self
            .create_checkpoint_stream(
                engine,
                CHECKPOINT_CRC_SCHEMA.clone(),
                None,
                None,
                None,
                None,
            )?
            .actions;
        for batch in batches {
            let batch = batch?;
            let mut visitor = CheckpointCrcVisitor {
                acc: &mut acc,
                version,
            };
            visitor.visit_rows_of(batch.actions())?;
        }
        Ok(acc.into_crc_delta().into_complete_crc(version))
    }

    /// Build a Complete [`Crc`] at `end_version` by reverse-replaying every commit in the segment,
    /// for a segment with no CRC and no checkpoint to root at. The commits must run contiguously
    /// from version 0.
    ///
    /// Returns `None` if protocol or metadata could not be recovered. File stats degrade to
    /// [`Indeterminate`](FileStatsState::Indeterminate) if the replay is not incremental-safe.
    pub(crate) fn build_crc_from_version_zero(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Crc>> {
        require!(
            self.checkpoint_version.is_none(),
            KernelError::internal_error(
                "build_crc_from_version_zero called with a checkpoint present"
            )
        );
        let Some(first) = self.listed.ascending_commit_files.first() else {
            return Ok(None);
        };
        // A log with no checkpoint must start at version 0; a higher first version means a table
        // truncated without a checkpoint.
        require!(first.version == 0, KernelError::MissingVersion(0));
        let delta = self.replay_commits_into_crc_delta(
            engine,
            self.listed.ascending_commit_files.iter(),
            Some(FileSizeHistogram::create_default()),
        )?;
        Ok(delta.into_complete_crc(self.end_version))
    }

    /// Build a `CrcDelta` covering commits `(base_version, self.end_version]` via reverse
    /// log replay. `seed_histogram` is an empty histogram with the same bin boundaries as
    /// the downstream base CRC, or `None` to skip histogram tracking on the delta.
    ///
    /// Errors if `base_version >= self.end_version` or if the segment is missing the
    /// commit at `base_version + 1` (i.e. has a gap above `base_version`).
    pub(crate) fn build_crc_delta_from_base(
        &self,
        engine: &dyn Engine,
        base_version: Version,
        seed_histogram: Option<FileSizeHistogram>,
    ) -> DeltaResult<CrcDelta> {
        require!(
            base_version < self.end_version,
            KernelError::internal_error(format!(
                "build_crc_delta_from_base: base_version ({}) must be strictly less \
                 than end_version ({})",
                base_version, self.end_version,
            ))
        );

        let deltas: Vec<_> = self
            .listed
            .ascending_commit_files
            .iter()
            .filter(|c| c.version > base_version)
            .collect();

        let first_above = deltas.first().map(|c| c.version);
        require!(
            first_above == Some(base_version + 1),
            KernelError::internal_error(format!(
                "build_crc_delta_from_base: segment is missing commit {} \
                 (lowest commit above base_version is {:?})",
                base_version + 1,
                first_above,
            ))
        );

        self.replay_commits_into_crc_delta(engine, deltas.into_iter(), seed_histogram)
    }

    /// Replay the given commits into a [`CrcDelta`]. The shared core of
    /// [`Self::build_crc_delta_from_base`] and [`Self::build_crc_from_version_zero`].
    /// `ascending_commits` are taken oldest-first; `seed_histogram` is an empty histogram with the
    /// downstream base's bin boundaries, or `None` to skip histogram tracking.
    fn replay_commits_into_crc_delta<'a>(
        &self,
        engine: &dyn Engine,
        ascending_commits: impl DoubleEndedIterator<Item = &'a ParsedLogPath>,
        seed_histogram: Option<FileSizeHistogram>,
    ) -> DeltaResult<CrcDelta> {
        // Replay newest-first: ICT capture reads from the newest commit only.
        let locations: Vec<FileMeta> = ascending_commits
            .rev()
            .map(|c| c.location.clone())
            .collect();
        let mut acc = CrcReplayAccumulator::new(seed_histogram);
        let batches =
            engine
                .json_handler()
                .read_json_files(&locations, REPLAY_SCHEMA.clone(), None)?;

        for batch_result in batches {
            // Transient visitor borrows the shared accumulator for the duration of the
            // batch; same pattern as `ActionReconciliationVisitor`.
            let mut visitor = CommitCrcVisitor { acc: &mut acc };
            visitor.visit_rows_of(batch_result?.as_ref())?;
        }

        // Run the per-commit invariant on the final (oldest) commit; no successor batch
        // will trigger it.
        acc.process_commit_file_end()?;

        Ok(acc.into_crc_delta())
    }
}

// ============================================================================
// Accumulator
// ============================================================================

/// In-progress [`CrcDelta`] plus the scaffolding needed to build it correctly during reverse
/// replay. The visitor calls `process_batch_start` on each batch and the `on_*` methods on
/// each row. After all batches have been folded in and `process_commit_file_end` has run for
/// the final commit, [`Self::into_crc_delta`] returns the result.
struct CrcReplayAccumulator {
    delta: CrcDelta,

    /// Only the newest commit contributes to [`CrcDelta::in_commit_timestamp`].
    is_first_commit: bool,

    current_commit: PendingCommit,
}

/// Commit-local aggregates remain provisional until all batches, including commitInfo, are read.
#[derive(Default)]
struct PendingCommit {
    source: Option<FileStatsSource>,
    operation: Option<String>,
    saw_file_action: bool,
    adds: FileStatsDelta,
    failure: Option<FileStatsFailureReason>,
}

impl PendingCommit {
    fn policy(&self) -> Option<FileStatsOperation> {
        self.operation.as_deref().map(classify_file_stats_operation)
    }
}

impl CrcReplayAccumulator {
    fn new(seed_histogram: Option<FileSizeHistogram>) -> Self {
        Self {
            current_commit: PendingCommit {
                adds: FileStatsDelta {
                    net_histogram: seed_histogram.clone(),
                    ..Default::default()
                },
                ..Default::default()
            },
            delta: CrcDelta {
                file_stats_validity: FileStatsValidity::Valid,
                file_stats: FileStatsDelta {
                    net_histogram: seed_histogram,
                    ..Default::default()
                },
                ..Default::default()
            },
            is_first_commit: true,
        }
    }

    fn process_batch_start(&mut self, batch_file_url: &str) -> DeltaResult<()> {
        if let Some(FileStatsSource::Commit { location, .. }) = &self.current_commit.source {
            if location == batch_file_url {
                return Ok(());
            }
            self.process_commit_file_end()?;
            self.is_first_commit = false;
        }
        let commit = ParsedLogPath::parse_commit(Url::parse(batch_file_url)?)?;
        self.current_commit.source = Some(FileStatsSource::Commit {
            version: commit.version,
            location: batch_file_url.to_owned(),
        });
        if self.delta.file_stats_validity != FileStatsValidity::Valid {
            return Ok(());
        }
        self.current_commit.adds.net_histogram = self
            .delta
            .file_stats
            .net_histogram
            .as_ref()
            .map(|histogram| {
                FileSizeHistogram::create_empty_with_boundaries(
                    histogram.sorted_bin_boundaries().to_vec(),
                )
            })
            .transpose()?;
        Ok(())
    }

    fn process_commit_file_end(&mut self) -> DeltaResult<()> {
        let pending = std::mem::take(&mut self.current_commit);
        if self.delta.file_stats_validity != FileStatsValidity::Valid {
            return Ok(());
        }
        let policy = pending.policy();
        let failure = pending.failure.or(match policy {
            Some(FileStatsOperation::Unsupported) => {
                Some(FileStatsFailureReason::UnsupportedOperation)
            }
            None if pending.saw_file_action => Some(FileStatsFailureReason::MissingOperation),
            _ => None,
        });
        if let Some(reason) = failure {
            let failure = FileStatsFailure {
                source: pending.source.unwrap_or(FileStatsSource::Unspecified),
                operation: pending.operation,
                reason,
            };
            warn!("CRC reverse-replay: {failure}");
            self.delta.file_stats_validity = FileStatsValidity::Invalid(failure);
        } else if policy == Some(FileStatsOperation::CountAdds) {
            self.delta.file_stats.merge(&pending.adds)?;
        }
        Ok(())
    }

    fn on_commit_info(&mut self, operation: Option<&str>, ict: Option<i64>) {
        if let Some(operation) = operation {
            // An unsupported operation remains unsupported even if another commitInfo follows it.
            let unsupported = self.current_commit.policy() == Some(FileStatsOperation::Unsupported);
            if !unsupported {
                self.current_commit.operation = Some(operation.to_owned());
            }
        }
        if self.is_first_commit {
            self.delta.in_commit_timestamp = ict;
        }
    }

    fn on_commit_add(&mut self, size: i64) -> DeltaResult<()> {
        self.current_commit.saw_file_action = true;
        if size < 0 {
            self.current_commit
                .failure
                .get_or_insert(FileStatsFailureReason::NegativeAddSize { size });
        } else if self.delta.file_stats_validity == FileStatsValidity::Valid
            && self.current_commit.failure.is_none()
            && !matches!(
                self.current_commit.policy(),
                Some(FileStatsOperation::IgnoreAdds | FileStatsOperation::Unsupported)
            )
        {
            self.current_commit.adds.add(size)?;
        }
        Ok(())
    }

    fn on_checkpoint_add(&mut self, size: i64, version: Version) -> DeltaResult<()> {
        if self.delta.file_stats_validity != FileStatsValidity::Valid {
            return Ok(());
        }
        if size < 0 {
            self.delta.file_stats_validity = FileStatsValidity::Invalid(FileStatsFailure {
                source: FileStatsSource::Checkpoint { version },
                operation: None,
                reason: FileStatsFailureReason::NegativeAddSize { size },
            });
        } else {
            self.delta.file_stats.add(size)?;
        }
        Ok(())
    }

    fn on_remove(&mut self, path: &str, size: Option<i64>) -> DeltaResult<()> {
        self.current_commit.saw_file_action = true;
        if self.delta.file_stats_validity != FileStatsValidity::Valid
            || self.current_commit.failure.is_some()
        {
            return Ok(());
        }
        match size {
            Some(size) if size < 0 => {
                self.current_commit.failure = Some(FileStatsFailureReason::NegativeRemoveSize {
                    path: path.to_owned(),
                    size,
                });
            }
            Some(size) if self.current_commit.policy() != Some(FileStatsOperation::Unsupported) => {
                let fs = &mut self.delta.file_stats;
                fs.gross_remove_files += 1;
                fs.gross_remove_bytes += size_to_u64(size)?;
                if let Some(histogram) = &mut fs.net_histogram {
                    histogram.remove(size)?;
                }
            }
            Some(_) => {}
            None => {
                self.current_commit.failure = Some(FileStatsFailureReason::MissingRemoveSize {
                    path: path.to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Tombstones (`removed=true`) are kept in the delta; [`Crc::apply`] consumes them as
    /// removals from the base map.
    fn on_domain_metadata(&mut self, domain: String, configuration: String, removed: bool) {
        if let Entry::Vacant(e) = self.delta.domain_metadata.entry(domain.clone()) {
            let dm = if removed {
                DomainMetadata::remove(domain, configuration)
            } else {
                DomainMetadata::new(domain, configuration)
            };
            e.insert(dm);
        }
    }

    fn on_set_transaction(&mut self, app_id: String, version: i64, last_updated: Option<i64>) {
        if let Entry::Vacant(e) = self.delta.set_transactions.entry(app_id.clone()) {
            e.insert(SetTransaction::new(app_id, version, last_updated));
        }
    }

    /// Apply non-file actions from row `i`. Each visitor handles Add sizes separately because
    /// checkpoint Adds count directly and commit Adds depend on the operation.
    fn apply_shared_columns<'a>(
        &mut self,
        i: usize,
        shared: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<()> {
        if let Some(domain) = shared[SHARED_COL_DM_DOMAIN].get_opt(i, "domainMetadata.domain")? {
            let configuration: String =
                shared[SHARED_COL_DM_CONFIG].get(i, "domainMetadata.configuration")?;
            let removed: bool = shared[SHARED_COL_DM_REMOVED].get(i, "domainMetadata.removed")?;
            self.on_domain_metadata(domain, configuration, removed);
        }
        if let Some(app_id) = shared[SHARED_COL_TXN_APP_ID].get_opt(i, "txn.appId")? {
            let version: i64 = shared[SHARED_COL_TXN_VERSION].get(i, "txn.version")?;
            let last_updated: Option<i64> =
                shared[SHARED_COL_TXN_LAST_UPDATED].get_opt(i, "txn.lastUpdated")?;
            self.on_set_transaction(app_id, version, last_updated);
        }
        let leaves = &shared[N_SHARED_SINGLE_LEAF_COLS..];
        let n_protocol_leaves = PROTOCOL_LEAVES.as_ref().0.len();
        if self.delta.protocol.is_none() {
            self.delta.protocol = visit_protocol_at(i, &leaves[..n_protocol_leaves])?;
        }
        if self.delta.metadata.is_none() {
            self.delta.metadata = visit_metadata_at(i, &leaves[n_protocol_leaves..])?;
        }
        Ok(())
    }

    fn into_crc_delta(self) -> CrcDelta {
        self.delta
    }
}

// ===== Shared column indices =====
// Indices into the shared slice each visitor passes to
// [`CrcReplayAccumulator::apply_shared_columns`], laid out by [`shared_columns`].
// The slice starts at the first shared column.
const SHARED_COL_ADD_SIZE: usize = 0;
const SHARED_COL_DM_DOMAIN: usize = 1;
const SHARED_COL_DM_CONFIG: usize = 2;
const SHARED_COL_DM_REMOVED: usize = 3;
const SHARED_COL_TXN_APP_ID: usize = 4;
const SHARED_COL_TXN_VERSION: usize = 5;
const SHARED_COL_TXN_LAST_UPDATED: usize = 6;
/// The single-leaf shared columns end here; the protocol and metadata leaves follow them in the
/// shared slice.
const N_SHARED_SINGLE_LEAF_COLS: usize = SHARED_COL_TXN_LAST_UPDATED + 1;

/// The columns every source carries (add file, domain metadata, set transaction), in the order
/// [`CrcReplayAccumulator::apply_shared_columns`] assumes. Each visitor appends these to its
/// source-specific columns.
fn shared_columns() -> Vec<(DataType, ColumnName)> {
    vec![
        (DataType::LONG, column_name!("add.size")),
        (DataType::STRING, column_name!("domainMetadata.domain")),
        (
            DataType::STRING,
            column_name!("domainMetadata.configuration"),
        ),
        (DataType::BOOLEAN, column_name!("domainMetadata.removed")),
        (DataType::STRING, column_name!("txn.appId")),
        (DataType::LONG, column_name!("txn.version")),
        (DataType::LONG, column_name!("txn.lastUpdated")),
    ]
}

/// Append the protocol and metadata leaf columns (in that order) to a visitor's fixed columns,
/// shared by both replay visitors' `selected_column_names_and_types`.
fn append_protocol_metadata_leaves(
    fixed: Vec<(DataType, ColumnName)>,
) -> (Vec<ColumnName>, Vec<DataType>) {
    let (mut types, mut names): (Vec<_>, Vec<_>) = fixed.into_iter().unzip();
    for leaves in [&*PROTOCOL_LEAVES, &*METADATA_LEAVES] {
        let (leaf_names, leaf_types) = leaves.as_ref();
        names.extend_from_slice(leaf_names);
        types.extend_from_slice(leaf_types);
    }
    (names, types)
}

/// Validate that `getters` carries exactly the columns a replay visitor projects: `n_fixed`
/// fixed columns plus the protocol and metadata leaves. `visitor_name` names the visitor in the
/// error.
fn check_visitor_getters(
    getters: &[&dyn GetData<'_>],
    n_fixed: usize,
    visitor_name: &str,
) -> DeltaResult<()> {
    let n_protocol_leaves = PROTOCOL_LEAVES.as_ref().0.len();
    let n_metadata_leaves = METADATA_LEAVES.as_ref().0.len();
    require!(
        getters.len() == n_fixed + n_protocol_leaves + n_metadata_leaves,
        KernelError::internal_error(format!(
            "Wrong number of {visitor_name} getters: {}",
            getters.len()
        ))
    );
    Ok(())
}

// ============================================================================
// Visitor
// ============================================================================

// ===== Visitor column indices =====
// Indices into the column list returned by [`CommitCrcVisitor::selected_column_names_and_types`].
const COL_FILE: usize = 0;
const COL_OP: usize = 1;
const COL_ICT: usize = 2;
const COL_REMOVE_PATH: usize = 3;
const COL_REMOVE_SIZE: usize = 4;
/// Source-specific columns end here; the shared columns follow.
const N_CRC_SPECIFIC_COLS: usize = COL_REMOVE_SIZE + 1;
const N_FIXED_COLS: usize = N_CRC_SPECIFIC_COLS + N_SHARED_SINGLE_LEAF_COLS;

/// Thin shim that pulls leaf values from `getters` and forwards them to the accumulator's
/// `on_*` methods. All behavior lives in [`CrcReplayAccumulator`].
struct CommitCrcVisitor<'a> {
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CommitCrcVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let mut fixed = vec![
                (DataType::STRING, column_name!("_file")),
                (DataType::STRING, column_name!("commitInfo.operation")),
                (DataType::LONG, column_name!("commitInfo.inCommitTimestamp")),
                (DataType::STRING, column_name!("remove.path")),
                (DataType::LONG, column_name!("remove.size")),
            ];
            fixed.extend(shared_columns());
            append_protocol_metadata_leaves(fixed).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        check_visitor_getters(getters, N_FIXED_COLS, "CommitCrcVisitor")?;
        if row_count == 0 {
            return Ok(());
        }
        // `_file` is constant across all rows of a batch per the JsonHandler contract. Read
        // once from row 0 and signal a potential file (commit) transition.
        let file_url: String = getters[COL_FILE].get(0, "_file")?;
        self.acc.process_batch_start(&file_url)?;

        for i in 0..row_count {
            let operation: Option<String> = getters[COL_OP].get_opt(i, "commitInfo.operation")?;
            let ict: Option<i64> = getters[COL_ICT].get_opt(i, "commitInfo.inCommitTimestamp")?;
            if operation.is_some() || ict.is_some() {
                self.acc.on_commit_info(operation.as_deref(), ict);
            }

            let remove_path: Option<String> = getters[COL_REMOVE_PATH].get_opt(i, "remove.path")?;
            if let Some(path) = remove_path {
                let remove_size: Option<i64> =
                    getters[COL_REMOVE_SIZE].get_opt(i, "remove.size")?;
                self.acc.on_remove(&path, remove_size)?;
            }

            let shared = &getters[N_CRC_SPECIFIC_COLS..];
            if let Some(size) = shared[SHARED_COL_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.on_commit_add(size)?;
            }
            self.acc.apply_shared_columns(i, shared)?;
        }
        Ok(())
    }
}

// ============================================================================
// Checkpoint base construction
// ============================================================================

/// Action schema for reading a checkpoint into a base [`Crc`], projecting only the leaves the
/// accumulator needs. `add.size` is the only Add leaf read, and it is required, so its presence
/// marks an Add row (a checkpoint Add missing `size` errors at read time). A checkpoint has no
/// `remove` or `commitInfo` to project.
static CHECKPOINT_CRC_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable ADD_NAME: { not_null "size": LONG },
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (&SET_TRANSACTION_FIELD),
    (&DOMAIN_METADATA_FIELD),
};

// A checkpoint has no source-specific columns, so its projection is the shared columns.

/// Pulls leaf values from a checkpoint batch into the shared [`CrcReplayAccumulator`].
struct CheckpointCrcVisitor<'a> {
    acc: &'a mut CrcReplayAccumulator,
    version: Version,
}

impl RowVisitor for CheckpointCrcVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| append_protocol_metadata_leaves(shared_columns()).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        check_visitor_getters(getters, N_SHARED_SINGLE_LEAF_COLS, "CheckpointCrcVisitor")?;
        for i in 0..row_count {
            if let Some(size) = getters[SHARED_COL_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.on_checkpoint_add(size, self.version)?;
            }
            self.acc.apply_shared_columns(i, getters)?;
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use test_utils::{add_commit, assert_result_error_with_message};

    use super::*;
    use crate::crc::{DomainMetadataState, FileStats, FileStatsState, SetTransactionState};
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::table_features::TableFeature;

    #[rstest::rstest]
    #[case("WRITE", true)]
    #[case("STREAMING UPDATE", true)]
    #[case("COMPUTE STATS", true)]
    #[case("UNKNOWN OPERATION", false)]
    fn commit_operation_is_classified_at_finalization(#[case] op: &str, #[case] valid: bool) {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some(op), None);
        acc.process_commit_file_end().unwrap();
        assert_eq!(
            acc.delta.file_stats_validity == FileStatsValidity::Valid,
            valid
        );
    }

    #[rstest::rstest]
    fn known_non_counting_operations_skip_discarded_arithmetic(
        #[values("COMPUTE STATS", "UNKNOWN OPERATION")] operation: &str,
        #[values(false, true)] track_histogram: bool,
    ) {
        let histogram = track_histogram.then(FileSizeHistogram::create_default);
        let mut acc = CrcReplayAccumulator::new(histogram.clone());
        acc.on_commit_info(Some(operation), None);
        for _ in 0..3 {
            acc.on_commit_add(i64::MAX).unwrap();
            if operation == "UNKNOWN OPERATION" {
                acc.on_remove("p", Some(i64::MAX)).unwrap();
            }
        }
        acc.process_commit_file_end().unwrap();
        assert_eq!(acc.delta.file_stats.gross_add_bytes, 0);
        assert_eq!(acc.delta.file_stats.gross_remove_bytes, 0);
        assert_eq!(acc.delta.file_stats.net_histogram, histogram);
        assert_eq!(
            acc.delta.file_stats_validity == FileStatsValidity::Valid,
            operation == "COMPUTE STATS"
        );
    }

    #[test]
    fn ict_only_commit_without_file_actions_is_valid() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(None, Some(1234));
        acc.process_commit_file_end().unwrap();
        assert_eq!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
        assert_eq!(acc.delta.in_commit_timestamp, Some(1234));
    }

    #[rstest::rstest]
    fn newest_commit_ict_is_preserved(#[values(None, Some(2000))] newest_ict: Option<i64>) {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000002.json")
            .unwrap();
        acc.on_commit_info(Some("COMPUTE STATS"), newest_ict);
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_info(Some("WRITE"), Some(1000));
        assert_eq!(acc.delta.in_commit_timestamp, newest_ict);
    }

    #[rstest::rstest]
    #[case::add(None, Some(-1), "add action has negative size -1")]
    #[case::remove_negative(Some("p"), Some(-2), "remove action for \"p\" has negative size -2")]
    #[case::remove_missing(Some("p"), None, "remove action for \"p\" has no size")]
    fn invalid_size_retains_late_operation_and_source(
        #[case] remove_path: Option<&str>,
        #[case] size: Option<i64>,
        #[case] reason: &str,
        #[values("WRITE", "COMPUTE STATS", "UNKNOWN OPERATION")] operation: &str,
        #[values(false, true)] operation_first: bool,
    ) {
        let mut acc = CrcReplayAccumulator::new(Some(FileSizeHistogram::create_default()));
        let location = "memory:///_delta_log/00000000000000000039.json";
        acc.process_batch_start(location).unwrap();
        if operation_first {
            acc.on_commit_info(Some(operation), None);
        }
        if let Some(path) = remove_path {
            acc.on_remove(path, size).unwrap();
        } else {
            acc.on_commit_add(size.unwrap()).unwrap();
        }
        acc.process_batch_start(location).unwrap();
        acc.on_commit_info(Some(operation), None);
        acc.process_commit_file_end().unwrap();
        let FileStatsValidity::Invalid(failure) = acc.delta.file_stats_validity else {
            panic!("expected invalid file statistics");
        };
        let message = failure.to_string();
        for expected in ["commit version 39", location, operation, reason] {
            assert!(message.contains(expected), "{message}");
        }
    }

    #[rstest::rstest]
    fn stats_readds_preserve_histogram_and_count_removes(
        #[values(false, true)] track_histogram: bool,
        #[values(false, true)] custom_boundaries: bool,
        #[values(false, true)] remove: bool,
    ) {
        let histogram = if custom_boundaries {
            FileSizeHistogram::create_empty_with_boundaries(vec![0, 200, 1000]).unwrap()
        } else {
            FileSizeHistogram::create_default()
        };
        let mut acc = CrcReplayAccumulator::new(track_histogram.then_some(histogram.clone()));
        let mut expected = histogram;
        if remove {
            acc.on_remove("a", Some(100)).unwrap();
            expected.remove(100).unwrap();
        }
        acc.on_commit_add(100).unwrap();
        acc.on_commit_add(20000).unwrap();
        acc.on_commit_info(Some("COMPUTE STATS"), None);
        acc.process_commit_file_end().unwrap();
        assert_eq!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
        assert_eq!(
            acc.delta.file_stats.net_files(),
            if remove { -1 } else { 0 }
        );
        assert_eq!(
            acc.delta.file_stats.net_bytes(),
            if remove { -100 } else { 0 }
        );
        assert_eq!(
            acc.delta.file_stats.net_histogram,
            track_histogram.then_some(expected)
        );
    }

    #[test]
    fn unknown_operation_cannot_be_reset_by_another_commit_info() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some("UNKNOWN OPERATION"), None);
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end().unwrap();
        let FileStatsValidity::Invalid(failure) = acc.delta.file_stats_validity else {
            panic!("expected invalid file statistics");
        };
        assert_eq!(failure.operation.as_deref(), Some("UNKNOWN OPERATION"));
    }

    #[test]
    fn checkpoint_failure_retains_checkpoint_version_without_operation() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_checkpoint_add(-1, 38).unwrap();
        let FileStatsValidity::Invalid(failure) = acc.delta.file_stats_validity else {
            panic!("expected invalid file statistics");
        };
        assert_eq!(failure.source, FileStatsSource::Checkpoint { version: 38 });
        assert_eq!(failure.operation, None);
        assert!(failure
            .to_string()
            .contains("add action has negative size -1"));
    }

    #[rstest::rstest]
    fn first_finalized_failure_survives_other_commits(#[values(false, true)] newest_fails: bool) {
        let mut acc = CrcReplayAccumulator::new(None);
        for version in (1..=3).rev() {
            let location = format!("memory:///_delta_log/{version:020}.json");
            acc.process_batch_start(&location).unwrap();
            let operation = if version == 2 {
                "COMPUTE STATS"
            } else if version == 1 || newest_fails {
                "UNKNOWN OPERATION"
            } else {
                "WRITE"
            };
            acc.on_commit_add(100).unwrap();
            acc.on_commit_info(Some(operation), None);
        }
        acc.process_commit_file_end().unwrap();
        let FileStatsValidity::Invalid(failure) = acc.delta.file_stats_validity else {
            panic!("expected invalid file statistics");
        };
        let version = if newest_fails { 3 } else { 1 };
        assert_eq!(
            failure.source,
            FileStatsSource::Commit {
                version,
                location: format!("memory:///_delta_log/{version:020}.json"),
            }
        );
        assert_eq!(failure.operation.as_deref(), Some("UNKNOWN OPERATION"));
        assert!(acc.current_commit.operation.is_none());
    }

    #[test]
    fn empty_batch_preserves_pending_adds_and_late_operation() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_add(100).unwrap();
        let mut visitor = CommitCrcVisitor { acc: &mut acc };
        let (names, _) = visitor.selected_column_names_and_types();
        let getters: Vec<&dyn GetData<'_>> = vec![&(); names.len()];
        visitor.visit(0, &getters).unwrap();
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end().unwrap();
        assert_eq!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
        assert_eq!(acc.delta.file_stats.net_bytes(), 100);
        assert_eq!(acc.delta.file_stats.net_files(), 1);
    }

    #[test]
    fn staged_commit_source_preserves_version_and_url() {
        let mut acc = CrcReplayAccumulator::new(None);
        let location = concat!(
            "memory:///_delta_log/_staged_commits/",
            "00000000000000000039.00000000-0000-0000-0000-000000000000.json"
        );
        acc.process_batch_start(location).unwrap();
        acc.on_remove("part-a", None).unwrap();
        acc.on_commit_info(Some("DELETE"), None);
        acc.process_commit_file_end().unwrap();
        let FileStatsValidity::Invalid(failure) = acc.delta.file_stats_validity else {
            panic!("expected invalid file statistics");
        };
        assert_eq!(
            failure.source,
            FileStatsSource::Commit {
                version: 39,
                location: location.into()
            }
        );
    }

    // ===== domainMetadata =====

    #[test]
    fn on_domain_metadata_first_seen_in_reverse_wins() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_domain_metadata("d".into(), "new".into(), false);
        acc.on_domain_metadata("d".into(), "old".into(), false);
        assert_eq!(acc.delta.domain_metadata["d"].configuration(), "new");
        assert!(!acc.delta.domain_metadata["d"].is_removed());
    }

    #[test]
    fn on_domain_metadata_tombstone_is_kept() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_domain_metadata("d".into(), "v".into(), true);
        assert!(acc.delta.domain_metadata["d"].is_removed());
    }

    // ===== txn =====

    #[test]
    fn on_set_transaction_first_seen_in_reverse_wins() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_set_transaction("a".into(), 99, Some(123));
        acc.on_set_transaction("a".into(), 1, Some(0));
        assert_eq!(acc.delta.set_transactions["a"].version, 99);
    }

    // ===== auxiliary =====

    #[test]
    fn accumulator_with_seed_histogram_inserts_into_seeded_bin() {
        let seed = FileSizeHistogram::create_empty_with_boundaries(vec![0, 200, 1000]).unwrap();
        let mut acc = CrcReplayAccumulator::new(Some(seed));
        acc.on_commit_add(150).unwrap();
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end().unwrap();
        let hist = acc.delta.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries(), &[0, 200, 1000]);
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 150);
    }

    #[test]
    fn accumulator_with_no_seed_histogram_keeps_delta_histogram_none() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_add(150).unwrap();
        assert!(acc.delta.file_stats.net_histogram.is_none());
    }

    #[test]
    fn into_crc_delta_transfers_accumulated_state() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_add(42).unwrap();
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end().unwrap();
        let delta = acc.into_crc_delta();
        assert_eq!(delta.file_stats.net_files(), 1);
        assert_eq!(delta.file_stats.net_bytes(), 42);
    }

    #[test]
    fn visitor_schema_length_matches_column_indices() {
        let mut acc = CrcReplayAccumulator::new(None);
        let visitor = CommitCrcVisitor { acc: &mut acc };
        let (names, types) = visitor.selected_column_names_and_types();
        let expected =
            N_FIXED_COLS + PROTOCOL_LEAVES.as_ref().0.len() + METADATA_LEAVES.as_ref().0.len();
        assert_eq!(names.len(), expected);
        assert_eq!(types.len(), expected);
    }

    #[test]
    fn checkpoint_visitor_schema_length_matches_column_indices() {
        let mut acc = CrcReplayAccumulator::new(None);
        let visitor = CheckpointCrcVisitor {
            acc: &mut acc,
            version: 0,
        };
        let (names, types) = visitor.selected_column_names_and_types();
        let expected = N_SHARED_SINGLE_LEAF_COLS
            + PROTOCOL_LEAVES.as_ref().0.len()
            + METADATA_LEAVES.as_ref().0.len();
        assert_eq!(names.len(), expected);
        assert_eq!(types.len(), expected);
    }

    // ===== Commit-boundary state machine: direct accumulator tests =====
    //
    // `SyncEngine` emits one batch per file, so multi-batch-per-file scenarios are only
    // reachable by driving the accumulator directly.

    #[test]
    fn per_commit_invariant_holds_when_file_action_and_commit_info_split_across_batches_of_one_file(
    ) {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_add(0).unwrap();
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end().unwrap();
        assert_eq!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_no_safe_op_across_batches() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_add(0).unwrap();
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.process_commit_file_end().unwrap();
        assert_ne!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_commit_info_but_no_operation() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        acc.on_commit_add(100).unwrap();
        acc.on_commit_info(None, Some(42));
        acc.process_commit_file_end().unwrap();
        assert_ne!(acc.delta.file_stats_validity, FileStatsValidity::Valid);
    }

    #[test]
    fn is_first_commit_stays_true_across_batches_of_same_file() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000002.json")
            .unwrap();
        acc.process_batch_start("memory:///_delta_log/00000000000000000002.json")
            .unwrap();
        acc.process_batch_start("memory:///_delta_log/00000000000000000002.json")
            .unwrap();
        assert!(acc.is_first_commit);
    }

    #[test]
    fn is_first_commit_becomes_false_after_file_transition() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("memory:///_delta_log/00000000000000000002.json")
            .unwrap();
        acc.process_batch_start("memory:///_delta_log/00000000000000000001.json")
            .unwrap();
        assert!(!acc.is_first_commit);
    }

    // ===== End-to-end smoke =====
    //
    // Tests all aspects of the full pipeline via the visitor.

    #[tokio::test]
    async fn end_to_end_smoke_full_coverage() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let root = "memory:///t/";

        // v0: bootstrap. Protocol already supports `domainMetadata` and `inCommitTimestamp`
        // so the v1 DM action and v2 commitInfo with ICT are well-formed. v0 is outside
        // the replay range; the segment covers (0, 2].
        add_commit(root, store.as_ref(), 0, r#"
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["domainMetadata","inCommitTimestamp"]}}
{"metaData":{"id":"id-0","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":0}}
"#.to_string()).await.unwrap();

        // v1: DM add, txn add, four file adds spanning histogram bins 0 (sizes 100, 200),
        // 1 (size 10000), and 2 (size 20000).
        add_commit(
            root,
            store.as_ref(),
            1,
            r#"
{"add":{"path":"a","partitionValues":{},"size":100,"modificationTime":1,"dataChange":true}}
{"add":{"path":"b","partitionValues":{},"size":200,"modificationTime":1,"dataChange":true}}
{"add":{"path":"c","partitionValues":{},"size":10000,"modificationTime":1,"dataChange":true}}
{"add":{"path":"d","partitionValues":{},"size":20000,"modificationTime":1,"dataChange":true}}
{"domainMetadata":{"domain":"keep","configuration":"cfg","removed":false}}
{"txn":{"appId":"app1","version":1,"lastUpdated":1}}
{"commitInfo":{"timestamp":1,"operation":"WRITE"}}
"#
            .to_string(),
        )
        .await
        .unwrap();

        // v2 (newest): protocol upgrade that adds `rowTracking` on top of v0's features,
        // new metadata, DM tombstone, second txn, one remove (bin 0), commitInfo with ICT.
        add_commit(root, store.as_ref(), 2, r#"
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["domainMetadata","inCommitTimestamp","rowTracking"]}}
{"metaData":{"id":"id-2","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.enableRowTracking":"true"},"createdTime":2}}
{"remove":{"path":"a","deletionTimestamp":2,"dataChange":true,"size":100}}
{"domainMetadata":{"domain":"drop","configuration":"","removed":true}}
{"txn":{"appId":"app2","version":5,"lastUpdated":2}}
{"commitInfo":{"timestamp":2,"operation":"WRITE","inCommitTimestamp":9999}}
"#.to_string()).await.unwrap();

        let log_root = url::Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let segment = LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root,
            vec![],
            None,
            Some(2),
            None,
        )
        .unwrap();

        // A seed histogram is required for the replay to track histogram bin updates.
        let base = Crc {
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 0,
                table_size_bytes: 0,
                file_size_histogram: Some(FileSizeHistogram::create_default()),
            }),
            domain_metadata_state: DomainMetadataState::Complete(HashMap::new()),
            set_transaction_state: SetTransactionState::Complete(HashMap::new()),
            ..Default::default()
        };
        let crc = segment.build_crc_from_base(&engine, &base).unwrap();

        // Newest-wins: v2's upgraded protocol (now carries `rowTracking` on top of v0's
        // features), v2's metadata, v2's ICT.
        assert_eq!(crc.version, 2);
        assert_eq!(crc.protocol.min_writer_version(), 7);
        assert!(crc.protocol.has_table_feature(&TableFeature::RowTracking));
        assert!(crc
            .protocol
            .has_table_feature(&TableFeature::InCommitTimestamp));
        assert!(crc
            .protocol
            .has_table_feature(&TableFeature::DomainMetadata));
        assert_eq!(crc.metadata.id(), "id-2");
        assert_eq!(crc.in_commit_timestamp_opt, Some(9999));

        // DM: "keep" inserted from v1; "drop" tombstone applied (no-op on empty base).
        let dm = crc.domain_metadata_state.expect_complete();
        assert_eq!(dm.get("keep").unwrap().configuration(), "cfg");
        assert!(!dm.contains_key("drop"));

        // Both txns upserted across the two commits.
        let txn = crc.set_transaction_state.expect_complete();
        assert_eq!(txn.get("app1").unwrap().version, 1);
        assert_eq!(txn.get("app2").unwrap().version, 5);

        // 4 adds spanning bins 0/1/2 minus 1 remove in bin 0:
        //   bin 0: +2 files (100 + 200 = 300 bytes) - 1 file (100 bytes) = 1 file, 200 bytes
        //   bin 1: +1 file (10000 bytes)
        //   bin 2: +1 file (20000 bytes)
        // Totals: 3 files, 30200 bytes.
        let stats = crc.file_stats().unwrap();
        assert_eq!(stats.num_files(), 3);
        assert_eq!(stats.table_size_bytes(), 30_200);
        let hist = stats.file_size_histogram().unwrap();
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 200);
        assert_eq!(hist.file_counts()[1], 1);
        assert_eq!(hist.total_bytes()[1], 10_000);
        assert_eq!(hist.file_counts()[2], 1);
        assert_eq!(hist.total_bytes()[2], 20_000);
    }

    #[tokio::test]
    async fn build_crc_delta_from_base_errors_when_base_version_geq_end_version() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let root = "memory:///t/";
        add_commit(
            root,
            store.as_ref(),
            0,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        )
        .await
        .unwrap();
        let log_root = url::Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let segment = LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root,
            vec![],
            None,
            Some(0),
            None,
        )
        .unwrap();
        for base in [0, 5] {
            assert_result_error_with_message(
                segment.build_crc_delta_from_base(&engine, base, None),
                "must be strictly less than end_version",
            );
        }
    }

    #[tokio::test]
    async fn build_crc_from_version_zero_no_checkpoint_first_commit_nonzero_errors() {
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let root = "memory:///t/";
        add_commit(
            root,
            store.as_ref(),
            1,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":1}}"#.to_string(),
        )
        .await
        .unwrap();
        let log_root = url::Url::parse(root).unwrap().join("_delta_log/").unwrap();
        let segment = LogSegment::for_snapshot_impl(
            engine.storage_handler().as_ref(),
            log_root,
            vec![],
            None,
            Some(1),
            None,
        )
        .unwrap();
        assert!(matches!(
            segment.build_crc_from_version_zero(&engine),
            Err(KernelError::MissingVersion(0))
        ));
    }
}
