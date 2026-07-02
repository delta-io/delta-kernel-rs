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

use super::LogSegment;
use crate::actions::visitors::{
    visit_metadata_at, visit_protocol_at, METADATA_LEAVES, PROTOCOL_LEAVES,
};
use crate::actions::{
    DomainMetadata, Metadata, Protocol, SetTransaction, ADD_NAME, COMMIT_INFO_NAME,
    DOMAIN_METADATA_NAME, METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME,
};
use crate::crc::{
    is_incremental_safe_operation, read_crc_file_or_none, size_to_u64, Crc, CrcDelta,
    FileSizeHistogram, FileStatsDelta,
};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::path::ParsedLogPath;
use crate::schema::{
    column_name, schema, ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef,
    ToSchema as _,
};
use crate::snapshot::IncrementalReplay;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, FileMeta, RowVisitor, Version};

#[allow(clippy::expect_used)]
static REPLAY_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let base = schema! {
        // size is the only Add leaf the visitor reads, and it is required, so its presence marks
        // an Add row.
        nullable ADD_NAME: { not_null "size": LONG },
        // remove.size is optional, so we read remove.path (required) to know a row is a Remove
        // before reading its size.
        nullable REMOVE_NAME: {
            not_null "path": STRING,
            nullable "size": LONG,
        },
        nullable PROTOCOL_NAME: (Protocol::to_schema()),
        nullable METADATA_NAME: (Metadata::to_schema()),
        nullable SET_TRANSACTION_NAME: (SetTransaction::to_schema()),
        nullable DOMAIN_METADATA_NAME: (DomainMetadata::to_schema()),
        nullable COMMIT_INFO_NAME: {
            nullable "operation": STRING,
            nullable "inCommitTimestamp": LONG,
        },
    };
    let with_file = base
        .add_metadata_column("_file", MetadataColumnSpec::FilePath)
        .expect("add _file metadata column");
    Arc::new(with_file)
});

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
    ) -> DeltaResult<Option<Arc<Crc>>> {
        let Some(base) = base else {
            return Ok(None);
        };
        if base.version == self.end_version {
            return Ok(Some(base.clone()));
        }
        if !incremental_replay.should_advance(base.version, self.end_version)? {
            return Ok(None);
        }
        let advanced = self.build_crc_from_base(engine, base)?;
        Ok(Some(Arc::new(advanced)))
    }

    /// Pick the latest CRC to use as an advance base: this segment's on-disk CRC or
    /// `in_memory_base` (e.g. the CRC an updating snapshot holds), whichever is newer. A failed
    /// on-disk read falls back to `in_memory_base`. Returns None when neither is available.
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
    #[instrument(name = "log_seg.build_crc_from_base", skip_all, err)]
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
        // No commit boundaries here, so the delta stays incremental-safe. It covers the full
        // table, which `into_complete_crc` turns into a Complete CRC.
        let mut acc = CrcReplayAccumulator::new(Some(FileSizeHistogram::create_default()));
        // Read only the checkpoint parquet plus any V2 sidecars via `create_checkpoint_stream`.
        let batches = self
            .create_checkpoint_stream(engine, CHECKPOINT_CRC_SCHEMA.clone(), None, None, None)?
            .actions;
        for batch in batches {
            let batch = batch?;
            let mut visitor = CheckpointCrcVisitor { acc: &mut acc };
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
            Error::internal_error("build_crc_from_version_zero called with a checkpoint present")
        );
        let Some(first) = self.listed.ascending_commit_files.first() else {
            return Ok(None);
        };
        // A log with no checkpoint must start at version 0; a higher first version means a table
        // truncated without a checkpoint.
        require!(
            first.version == 0,
            Error::generic(format!(
                "Cannot build CRC: log has no checkpoint but its first commit is at version {} \
                 (expected 0); the log appears truncated without a checkpoint",
                first.version
            ))
        );
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
            Error::internal_error(format!(
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
            Error::internal_error(format!(
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
            let mut visitor = CrcReplayVisitor { acc: &mut acc };
            visitor.visit_rows_of(batch_result?.as_ref())?;
        }

        // Run the per-commit invariant on the final (oldest) commit; no successor batch
        // will trigger it.
        acc.process_commit_file_end();

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

    /// True while the visitor is still on the newest commit. Used to gate ICT capture
    /// (only the newest commit contributes to [`CrcDelta::in_commit_timestamp`]). Cannot
    /// be derived from `current_file_url` alone, since after the first batch the URL is
    /// `Some` but we're still on the newest commit until a transition.
    is_first_commit: bool,

    /// URL of the commit file currently being processed. Drives commit-boundary detection:
    /// a different URL on a later batch means we've moved to an older commit.
    current_file_url: Option<String>,

    /// True if the current commit had at least one add/remove row. Combined with
    /// `current_commit_saw_safe_op` for the per-commit invariant check.
    current_commit_saw_file_action: bool,

    /// True if the current commit had a commitInfo row whose `operation` was in the
    /// [`is_incremental_safe_operation`] safelist. If false at commit end and the commit
    /// had file actions, we can't trust the file-stats delta and mark it unsafe.
    current_commit_saw_safe_op: bool,
}

impl CrcReplayAccumulator {
    fn new(seed_histogram: Option<FileSizeHistogram>) -> Self {
        Self {
            delta: CrcDelta {
                is_incremental_safe: true,
                file_stats: FileStatsDelta {
                    net_histogram: seed_histogram,
                    ..Default::default()
                },
                ..Default::default()
            },
            is_first_commit: true,
            current_file_url: None,
            current_commit_saw_file_action: false,
            current_commit_saw_safe_op: false,
        }
    }

    fn process_batch_start(&mut self, batch_file_url: &str) {
        if self.current_file_url.as_deref() == Some(batch_file_url) {
            return; // same file, still inside the current commit
        }
        if self.current_file_url.is_some() {
            // commit boundary: finalize the previous one, drop "newest" status
            self.process_commit_file_end();
            self.is_first_commit = false;
        }
        self.current_file_url = Some(batch_file_url.to_owned());
    }

    fn process_commit_file_end(&mut self) {
        // `is_incremental_safe` is one-way; once false, this check has nothing to set.
        if !self.delta.is_incremental_safe {
            return;
        }
        // File actions without a safe-classified operation: we can't trust file stats.
        // Covers both "no commitInfo at all" and "commitInfo with no `operation` field".
        if self.current_commit_saw_file_action && !self.current_commit_saw_safe_op {
            warn!(
                "CRC reverse-replay: commit at {} carried file actions but no safe-classified \
                 operation; defaulting to non-incremental-safe",
                self.current_file_url.as_deref().unwrap_or("?")
            );
            self.delta.is_incremental_safe = false;
        }
        self.current_commit_saw_file_action = false;
        self.current_commit_saw_safe_op = false;
    }

    // ===== Row-level updates (also the seams used by `on_*` unit tests) =====

    /// Called by the visitor once per commitInfo row (gated on operation or ict being
    /// present). Handles both pieces: `operation` drives per-commit safety classification,
    /// `ict` is captured into the delta from the newest commit only.
    fn on_commit_info(&mut self, operation: Option<&str>, ict: Option<i64>) {
        if let Some(op) = operation {
            if is_incremental_safe_operation(op) {
                self.current_commit_saw_safe_op = true;
            } else {
                warn!("CRC reverse-replay: non-incremental op {op}");
                self.delta.is_incremental_safe = false;
            }
        }
        if self.is_first_commit {
            self.delta.in_commit_timestamp = ict;
        }
    }

    fn on_add(&mut self, size: i64) -> DeltaResult<()> {
        self.current_commit_saw_file_action = true;
        // Once the delta is no longer incremental-safe, [`Crc::apply`] will transition the
        // file-stats state to `Indeterminate` and discard the accumulated file stats and
        // histogram. Stop accumulating; further math is wasted work.
        if !self.delta.is_incremental_safe {
            return Ok(());
        }
        let fs = &mut self.delta.file_stats;
        fs.gross_add_files += 1;
        // TODO(#2676): a negative size errors here and fails the snapshot load; degrade to
        //              Indeterminate instead, like a missing remove size.
        fs.gross_add_bytes += size_to_u64(size)?;
        if let Some(hist) = fs.net_histogram.as_mut() {
            hist.insert(size)?;
        }
        Ok(())
    }

    /// `size = None` means the remove row had a path but no size, which makes incremental
    /// tracking impossible.
    fn on_remove(&mut self, path: &str, size: Option<i64>) -> DeltaResult<()> {
        self.current_commit_saw_file_action = true;
        // Once the delta is no longer incremental-safe, [`Crc::apply`] will transition the
        // file-stats state to `Indeterminate` and discard the accumulated file stats and
        // histogram. Stop accumulating; further math is wasted work.
        if !self.delta.is_incremental_safe {
            return Ok(());
        }
        match size {
            Some(s) => {
                let fs = &mut self.delta.file_stats;
                fs.gross_remove_files += 1;
                fs.gross_remove_bytes += size_to_u64(s)?;
                if let Some(hist) = fs.net_histogram.as_mut() {
                    hist.remove(s)?;
                }
            }
            None => {
                warn!("CRC reverse-replay: remove action at {path} has missing size");
                self.delta.is_incremental_safe = false;
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

    /// Apply the columns common to commit and checkpoint replay (domain metadata, set
    /// transactions, protocol, metadata) from row `i` to the delta. Each visitor handles its
    /// source-specific columns (commit: `_file`/commitInfo/add/remove; checkpoint: add) and
    /// delegates the rest here so the shared leaves live in one place.
    fn visit_common_columns<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        cols: &CommonColumns,
    ) -> DeltaResult<()> {
        if let Some(domain) = getters[cols.dm_domain].get_opt(i, "domainMetadata.domain")? {
            let configuration: String =
                getters[cols.dm_config].get(i, "domainMetadata.configuration")?;
            let removed: bool = getters[cols.dm_removed].get(i, "domainMetadata.removed")?;
            self.on_domain_metadata(domain, configuration, removed);
        }
        if let Some(app_id) = getters[cols.txn_app_id].get_opt(i, "txn.appId")? {
            let version: i64 = getters[cols.txn_version].get(i, "txn.version")?;
            let last_updated: Option<i64> =
                getters[cols.txn_last_updated].get_opt(i, "txn.lastUpdated")?;
            self.on_set_transaction(app_id, version, last_updated);
        }
        let pm = &getters[cols.proto_meta_base..];
        if self.delta.protocol.is_none() {
            self.delta.protocol = visit_protocol_at(i, &pm[..cols.n_protocol_leaves])?;
        }
        if self.delta.metadata.is_none() {
            self.delta.metadata = visit_metadata_at(i, &pm[cols.n_protocol_leaves..])?;
        }
        Ok(())
    }

    fn into_crc_delta(self) -> CrcDelta {
        self.delta
    }
}

/// Number of columns shared by [`CrcReplayVisitor`] and [`CheckpointCrcVisitor`]: domain metadata
/// (domain, configuration, removed) then set transaction (appId, version, lastUpdated). Each
/// visitor's column list is {source-specific} then {shared} then {protocol leaves, metadata
/// leaves}. Protocol and metadata are appended last because each expands to many leaf columns,
/// while every column before them is a single leaf.
const N_SHARED_COLS: usize = 6;

/// Getter indices for the columns shared by commit and checkpoint replay, plus the count of
/// protocol leaves so the trailing protocol/metadata leaf slices can be split. Built via
/// [`CommonColumns::new`] and handed to [`CrcReplayAccumulator::visit_common_columns`].
struct CommonColumns {
    dm_domain: usize,
    dm_config: usize,
    dm_removed: usize,
    txn_app_id: usize,
    txn_version: usize,
    txn_last_updated: usize,
    /// Index of the first protocol leaf; metadata leaves follow the protocol leaves.
    proto_meta_base: usize,
    n_protocol_leaves: usize,
}

impl CommonColumns {
    /// The shared columns are contiguous starting at `base`, in the order laid out by
    /// [`shared_dm_txn_columns`], followed by the protocol leaves and then the metadata leaves.
    fn new(base: usize, n_protocol_leaves: usize) -> Self {
        Self {
            dm_domain: base,
            dm_config: base + 1,
            dm_removed: base + 2,
            txn_app_id: base + 3,
            txn_version: base + 4,
            txn_last_updated: base + 5,
            proto_meta_base: base + N_SHARED_COLS,
            n_protocol_leaves,
        }
    }
}

/// The domain-metadata and set-transaction columns shared by both replay visitors, in the order
/// [`CommonColumns::new`] assumes. Each visitor appends these to its source-specific columns.
fn shared_dm_txn_columns() -> Vec<(DataType, ColumnName)> {
    vec![
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
/// fixed columns plus the protocol and metadata leaves. Returns the protocol-leaf count, which
/// the caller needs to split the trailing protocol/metadata leaf slices. `visitor_name` names
/// the visitor in the error.
fn check_visitor_getters(
    getters: &[&dyn GetData<'_>],
    n_fixed: usize,
    visitor_name: &str,
) -> DeltaResult<usize> {
    let n_protocol_leaves = PROTOCOL_LEAVES.as_ref().0.len();
    let n_metadata_leaves = METADATA_LEAVES.as_ref().0.len();
    require!(
        getters.len() == n_fixed + n_protocol_leaves + n_metadata_leaves,
        Error::internal_error(format!(
            "Wrong number of {visitor_name} getters: {}",
            getters.len()
        ))
    );
    Ok(n_protocol_leaves)
}

// ============================================================================
// Visitor
// ============================================================================

// ===== Visitor column indices =====
// Indices into the column list returned by [`CrcReplayVisitor::selected_column_names_and_types`].
const COL_FILE: usize = 0;
const COL_OP: usize = 1;
const COL_ICT: usize = 2;
const COL_ADD_SIZE: usize = 3;
const COL_REMOVE_PATH: usize = 4;
const COL_REMOVE_SIZE: usize = 5;
/// Source-specific columns end here; the shared columns follow.
const N_CRC_SPECIFIC_COLS: usize = COL_REMOVE_SIZE + 1;
const N_FIXED_COLS: usize = N_CRC_SPECIFIC_COLS + N_SHARED_COLS;

/// Thin shim that pulls leaf values from `getters` and forwards them to the accumulator's
/// `on_*` methods. All behavior lives in [`CrcReplayAccumulator`].
struct CrcReplayVisitor<'a> {
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CrcReplayVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let mut fixed = vec![
                (DataType::STRING, column_name!("_file")),
                (DataType::STRING, column_name!("commitInfo.operation")),
                (DataType::LONG, column_name!("commitInfo.inCommitTimestamp")),
                (DataType::LONG, column_name!("add.size")),
                (DataType::STRING, column_name!("remove.path")),
                (DataType::LONG, column_name!("remove.size")),
            ];
            fixed.extend(shared_dm_txn_columns());
            append_protocol_metadata_leaves(fixed).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let n_protocol_leaves = check_visitor_getters(getters, N_FIXED_COLS, "CrcReplayVisitor")?;
        if row_count == 0 {
            return Ok(());
        }
        // `_file` is constant across all rows of a batch per the JsonHandler contract. Read
        // once from row 0 and signal a potential file (commit) transition.
        let file_url: String = getters[COL_FILE].get(0, "_file")?;
        self.acc.process_batch_start(&file_url);

        let common = CommonColumns::new(N_CRC_SPECIFIC_COLS, n_protocol_leaves);

        for i in 0..row_count {
            let operation: Option<String> = getters[COL_OP].get_opt(i, "commitInfo.operation")?;
            let ict: Option<i64> = getters[COL_ICT].get_opt(i, "commitInfo.inCommitTimestamp")?;
            if operation.is_some() || ict.is_some() {
                self.acc.on_commit_info(operation.as_deref(), ict);
            }

            if let Some(size) = getters[COL_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.on_add(size)?;
            }

            let remove_path: Option<String> = getters[COL_REMOVE_PATH].get_opt(i, "remove.path")?;
            if let Some(path) = remove_path {
                let remove_size: Option<i64> =
                    getters[COL_REMOVE_SIZE].get_opt(i, "remove.size")?;
                self.acc.on_remove(&path, remove_size)?;
            }

            self.acc.visit_common_columns(i, getters, &common)?;
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
#[allow(clippy::expect_used)]
static CHECKPOINT_CRC_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(schema! {
        nullable ADD_NAME: { not_null "size": LONG },
        nullable PROTOCOL_NAME: (Protocol::to_schema()),
        nullable METADATA_NAME: (Metadata::to_schema()),
        nullable SET_TRANSACTION_NAME: (SetTransaction::to_schema()),
        nullable DOMAIN_METADATA_NAME: (DomainMetadata::to_schema()),
    })
});

// ===== Checkpoint visitor column indices =====
const COL_CP_ADD_SIZE: usize = 0;
/// Source-specific columns end here; the shared columns follow.
const N_CP_SPECIFIC_COLS: usize = COL_CP_ADD_SIZE + 1;
const N_CP_FIXED_COLS: usize = N_CP_SPECIFIC_COLS + N_SHARED_COLS;

/// Pulls leaf values from a checkpoint batch into the shared [`CrcReplayAccumulator`].
struct CheckpointCrcVisitor<'a> {
    acc: &'a mut CrcReplayAccumulator,
}

impl RowVisitor for CheckpointCrcVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let mut fixed = vec![(DataType::LONG, column_name!("add.size"))];
            fixed.extend(shared_dm_txn_columns());
            append_protocol_metadata_leaves(fixed).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let n_protocol_leaves =
            check_visitor_getters(getters, N_CP_FIXED_COLS, "CheckpointCrcVisitor")?;
        let common = CommonColumns::new(N_CP_SPECIFIC_COLS, n_protocol_leaves);
        for i in 0..row_count {
            // `add.size` (required) marks an Add row.
            if let Some(size) = getters[COL_CP_ADD_SIZE].get_opt(i, "add.size")? {
                self.acc.on_add(size)?;
            }

            self.acc.visit_common_columns(i, getters, &common)?;
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

    // ===== Unit tests on `on_*` methods =====

    // ===== commitInfo =====

    #[rstest::rstest]
    #[case::safe("WRITE", true)]
    #[case::unsafe_op("ANALYZE STATS", false)]
    fn on_commit_info_classifies_operation(#[case] op: &str, #[case] is_safe: bool) {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(Some(op), None);
        assert_eq!(acc.current_commit_saw_safe_op, is_safe);
        assert_eq!(acc.delta.is_incremental_safe, is_safe);
    }

    #[test]
    fn on_commit_info_ict_only_no_operation_does_not_mark_saw_safe_op() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_commit_info(None, Some(1234));
        assert!(!acc.current_commit_saw_safe_op);
        assert!(acc.delta.is_incremental_safe);
        assert_eq!(acc.delta.in_commit_timestamp, Some(1234));
    }

    #[test]
    fn on_commit_info_captures_ict_only_on_first_commit() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.on_commit_info(Some("WRITE"), Some(2000));
        assert_eq!(acc.delta.in_commit_timestamp, Some(2000));
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), Some(1000));
        assert_eq!(acc.delta.in_commit_timestamp, Some(2000));
    }

    #[test]
    fn on_commit_info_first_commit_none_ict_does_not_get_overwritten_by_older() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.on_commit_info(Some("WRITE"), None);
        assert_eq!(acc.delta.in_commit_timestamp, None);
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), Some(1000));
        assert_eq!(acc.delta.in_commit_timestamp, None);
    }

    // ===== add =====

    #[test]
    fn on_add_increments_files_and_bytes() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(100).unwrap();
        acc.on_add(200).unwrap();
        assert_eq!(acc.delta.file_stats.net_files(), 2);
        assert_eq!(acc.delta.file_stats.net_bytes(), 300);
        assert!(acc.delta.is_incremental_safe);
    }

    // ===== remove =====

    #[test]
    fn on_remove_with_size_decrements_files_and_bytes() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_remove("p", Some(50)).unwrap();
        assert_eq!(acc.delta.file_stats.net_files(), -1);
        assert_eq!(acc.delta.file_stats.net_bytes(), -50);
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn on_remove_missing_size_trips_is_incremental_safe() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_remove("p", None).unwrap();
        assert!(!acc.delta.is_incremental_safe);
        assert!(acc.current_commit_saw_file_action);
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
        acc.on_add(150).unwrap();
        let hist = acc.delta.file_stats.net_histogram.as_ref().unwrap();
        assert_eq!(hist.sorted_bin_boundaries(), &[0, 200, 1000]);
        assert_eq!(hist.file_counts()[0], 1);
        assert_eq!(hist.total_bytes()[0], 150);
    }

    #[test]
    fn accumulator_with_no_seed_histogram_keeps_delta_histogram_none() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(150).unwrap();
        assert!(acc.delta.file_stats.net_histogram.is_none());
    }

    #[test]
    fn into_crc_delta_transfers_accumulated_state() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.on_add(42).unwrap();
        let delta = acc.into_crc_delta();
        assert_eq!(delta.file_stats.net_files(), 1);
        assert_eq!(delta.file_stats.net_bytes(), 42);
    }

    #[test]
    fn visitor_schema_length_matches_column_indices() {
        let mut acc = CrcReplayAccumulator::new(None);
        let visitor = CrcReplayVisitor { acc: &mut acc };
        let (names, types) = visitor.selected_column_names_and_types();
        let expected =
            N_FIXED_COLS + PROTOCOL_LEAVES.as_ref().0.len() + METADATA_LEAVES.as_ref().0.len();
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
        acc.process_batch_start("v1.json");
        acc.on_add(0).unwrap();
        acc.process_batch_start("v1.json");
        acc.on_commit_info(Some("WRITE"), None);
        acc.process_commit_file_end();
        assert!(acc.delta.is_incremental_safe);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_no_safe_op_across_batches() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(0).unwrap();
        acc.process_batch_start("v1.json");
        acc.process_commit_file_end();
        assert!(!acc.delta.is_incremental_safe);
    }

    #[test]
    fn per_commit_invariant_trips_when_file_action_has_commit_info_but_no_operation() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v1.json");
        acc.on_add(100).unwrap();
        acc.on_commit_info(None, Some(42));
        acc.process_commit_file_end();
        assert!(!acc.delta.is_incremental_safe);
    }

    #[test]
    fn is_first_commit_stays_true_across_batches_of_same_file() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v2.json");
        assert!(acc.is_first_commit);
    }

    #[test]
    fn is_first_commit_becomes_false_after_file_transition() {
        let mut acc = CrcReplayAccumulator::new(None);
        acc.process_batch_start("v2.json");
        acc.process_batch_start("v1.json");
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
        )
        .unwrap();
        assert_result_error_with_message(
            segment.build_crc_from_version_zero(&engine),
            "log appears truncated without a checkpoint",
        );
    }
}
