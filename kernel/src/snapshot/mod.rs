//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::{debug, info, instrument, warn};
use url::Url;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::set_transaction::{is_set_txn_expired, SetTransactionScanner};
use crate::actions::{DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::checkpoint::{CheckpointWriter, LastCheckpointHintStats};
use crate::clustering::{parse_clustering_columns, CLUSTERING_DOMAIN_NAME};
use crate::committer::{Committer, PublishMetadata};
use crate::crc::{
    try_read_crc_file, try_write_crc_file, Crc, CrcUpdate, DomainMetadataState, FileStats, LazyCrc,
    SetTransactionState,
};
use crate::expressions::ColumnName;
use crate::log_segment::{DomainMetadataMap, LogSegment};
use crate::log_segment_files::LogSegmentFiles;
use crate::metrics::MetricId;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::SchemaRef;
use crate::table_configuration::{InCommitTimestampEnablement, TableConfiguration};
use crate::table_features::{physical_to_logical_column_name, ColumnMappingMode, TableFeature};
use crate::table_properties::TableProperties;
use crate::transaction::builder::alter_table::AlterTableTransactionBuilder;
use crate::transaction::Transaction;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, LogCompactionWriter, Version};

mod builder;
pub use builder::SnapshotBuilder;

/// A shared, thread-safe reference to a [`Snapshot`].
pub type SnapshotRef = Arc<Snapshot>;

/// Result of attempting to write a version checksum (CRC) file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumWriteResult {
    /// A CRC file already exists at this version. Per the Delta protocol, writers MUST NOT
    /// overwrite existing version checksum files.
    AlreadyExists,
    /// The CRC file was successfully written to storage.
    Written,
}

/// Result of attempting to write a checkpoint file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointWriteResult {
    /// A checkpoint already exists at this version.
    AlreadyExists,
    /// The checkpoint was successfully written to storage.
    Written,
}

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
pub struct Snapshot {
    span: tracing::Span,
    log_segment: LogSegment,
    table_configuration: TableConfiguration,
    /// Eager in-memory CRC state for this snapshot, always at the snapshot's version.
    ///
    /// `Crc` is the universal type that captures everything CRC tracks: P/M, file
    /// statistics with typed validity, domain metadata / set transactions with
    /// completeness tracking, and ICT. Constructed during snapshot creation by:
    ///
    /// - Loading the CRC file at the target version, OR
    /// - Loading a stale CRC file + reverse-replaying commits after it (yields the same `Crc[N]`
    ///   as if it had been written at version N), OR
    /// - Building from scratch via full reverse log replay (no CRC file on disk), OR
    /// - For the incremental fallback path: a minimal CRC with `RequiresCheckpointRead` file
    ///   stats.
    ///
    /// Mutated only via [`Crc::apply`] (see the post-commit
    /// [`new_post_commit`](Self::new_post_commit) flow).
    crc: Arc<Crc>,
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.log_segment == other.log_segment
            && self.table_configuration == other.table_configuration
    }
}

impl Eq for Snapshot {}

impl Drop for Snapshot {
    fn drop(&mut self) {
        debug!("Dropping snapshot");
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version())
            .field("metadata", &self.table_configuration().metadata())
            .field("log_segment", &self.log_segment)
            .finish()
    }
}

impl Snapshot {
    /// Create a new [`SnapshotBuilder`] to build a new [`Snapshot`] for a given table root. If you
    /// instead have an existing [`Snapshot`] you would like to do minimal work to update, consider
    /// using
    pub fn builder_for(table_root: impl AsRef<str>) -> SnapshotBuilder {
        SnapshotBuilder::new_for(table_root)
    }

    /// Create a new [`SnapshotBuilder`] to incrementally update a [`Snapshot`] to a more recent
    /// version.
    ///
    /// We implement a simple heuristic:
    /// 1. if the caller explicitly requests the existing version, just return the existing snapshot
    /// 2. if the new version < existing version, error: there is no optimization to do here
    /// 3. list from (existing checkpoint version + 1) onward (or from version 1 if there is no
    ///    checkpoint yet)
    /// 4. if a newer or newly discovered checkpoint is found while refreshing to the latest
    ///    version, create a new snapshot from that checkpoint (and commits after it), even if the
    ///    table version itself did not advance
    /// 5. if no new checkpoint is found and the table version did not advance, return the existing
    ///    snapshot
    /// 6. if no new checkpoint is found, do lightweight P+M replay on the latest commits after
    ///    ensuring we only retain commits > any checkpoints
    ///
    /// # Parameters
    ///
    /// - `existing_snapshot`: reference to an existing [`Snapshot`]
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `target_version`: target version of the [`Snapshot`]. None will create a snapshot at the
    ///   latest version of the table.
    pub fn builder_from(existing_snapshot: SnapshotRef) -> SnapshotBuilder {
        SnapshotBuilder::new_from(existing_snapshot)
    }

    /// Create a new [`Snapshot`] from a [`LogSegment`] and [`TableConfiguration`].
    ///
    /// Builds a minimal CRC from the table configuration's P&M (via the kernel-internal
    /// `Crc::minimal_from_pm`).
    /// File stats are
    /// [`RequiresCheckpointRead`](crate::crc::FileStatsState::RequiresCheckpointRead),
    /// DM/txns are [`Untracked`](crate::crc::DomainMetadataState::Untracked).
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn new(log_segment: LogSegment, table_configuration: TableConfiguration) -> Self {
        let crc = Arc::new(Crc::minimal_from_pm(
            table_configuration.protocol().clone(),
            table_configuration.metadata().clone(),
        ));
        Self::new_with_crc(log_segment, table_configuration, crc)
    }

    /// Internal constructor that accepts an explicit [`Crc`].
    ///
    /// Callers are responsible for ensuring the CRC matches the snapshot's version. Used by
    /// snapshot creation paths that have already constructed (or loaded) the CRC.
    pub(crate) fn new_with_crc(
        log_segment: LogSegment,
        table_configuration: TableConfiguration,
        crc: Arc<Crc>,
    ) -> Self {
        let span = tracing::info_span!(
            parent: tracing::Span::none(),
            "snap",
            path = %table_configuration.table_root(),
            version = table_configuration.version(),
        );
        info!(parent: &span, "Created snapshot");
        Self {
            span,
            log_segment,
            table_configuration,
            crc,
        }
    }

    /// Create a new [`Snapshot`] instance from an existing [`Snapshot`]. This is useful when you
    /// already have a [`Snapshot`] lying around and want to do the minimal work to 'update' the
    /// snapshot to a later version.
    #[instrument(err, fields(version, operation_id = %operation_id), skip(engine, target_version))]
    fn try_new_from(
        existing_snapshot: Arc<Snapshot>,
        log_tail: Vec<ParsedLogPath>,
        engine: &dyn Engine,
        target_version: impl Into<Option<Version>>,
        operation_id: MetricId,
    ) -> DeltaResult<Arc<Self>> {
        let old_log_segment = &existing_snapshot.log_segment;
        let old_version = existing_snapshot.version();
        let requested_version = target_version.into();
        if let Some(requested_version) = requested_version {
            tracing::Span::current().record("version", requested_version);
            if requested_version == old_version {
                // Re-requesting the same version
                return Ok(existing_snapshot.clone());
            }
            if requested_version < old_version {
                // Hint is too new: error since this is effectively an incorrect optimization
                return Err(Error::Generic(format!(
                    "Requested snapshot version {requested_version} is older than snapshot hint version {old_version}"
                )));
            }
        } else {
            tracing::Span::current().record("version", old_version);
        }

        let log_root = old_log_segment.log_root.clone();
        let storage = engine.storage_handler();

        // Start listing just after the previous segment's checkpoint, if any.
        let listing_start = old_log_segment.checkpoint_version.unwrap_or(0) + 1;

        // Check for new commits (and CRC)
        let new_listed_files = LogSegmentFiles::list(
            storage.as_ref(),
            &log_root,
            log_tail,
            Some(listing_start),
            requested_version,
        )?;

        // NB: we need to check both checkpoints and commits since we filter commits at and below
        // the checkpoint version. Example: if we have a checkpoint + commit at version 1, the log
        // listing above will only return the checkpoint and not the commit.
        if new_listed_files.ascending_commit_files().is_empty()
            && new_listed_files.checkpoint_parts().is_empty()
        {
            match requested_version {
                Some(requested_version) if requested_version != old_version => {
                    // No new commits, but we are looking for a new version
                    return Err(Error::Generic(format!(
                        "Requested snapshot version {requested_version} is newer than the latest version {old_version}"
                    )));
                }
                _ => {
                    // No new commits, just return the same snapshot
                    return Ok(existing_snapshot.clone());
                }
            }
        }

        // create a log segment just from existing_checkpoint.version -> new_version
        // OR could be from 1 -> new_version
        // Save the latest_commit before moving new_listed_files
        let new_latest_commit_file = new_listed_files.latest_commit_file().clone();
        // Note: new_log_segment won't have last_checkpoint_metadata since we're listing without a
        // hint. If the new segment has a checkpoint, we will return it as is. Otherwise, we
        // will preserve last_checkpoint_metadata when merging the new log segment with the
        // old one.
        let mut new_log_segment =
            LogSegment::try_new(new_listed_files, log_root.clone(), requested_version, None)?;

        let new_end_version = new_log_segment.end_version;
        if new_end_version < old_version {
            // we should never see a new log segment with a version < the existing snapshot
            // version, that would mean a commit was incorrectly deleted from the log
            return Err(Error::Generic(format!(
                "Unexpected state: The newest version in the log {new_end_version} is older than the old version {old_version}")));
        }
        if new_log_segment.checkpoint_version.is_some() {
            // We found a checkpoint in the new log segment, so build a fresh snapshot from it.
            // TODO(#2217): reuse old LazyCrc when CRC file matches.
            // TODO(#2218): consider incremental P&M replay instead of full rebuild.
            let snapshot = Self::try_new_from_log_segment(
                existing_snapshot.table_root().clone(),
                new_log_segment,
                engine,
                operation_id,
            );
            return Ok(Arc::new(snapshot?));
        }

        if new_end_version == old_version {
            // No new commits and no newly discovered checkpoint, just return the same snapshot.
            return Ok(existing_snapshot.clone());
        }

        // after this point, we incrementally update the snapshot with the new log segment.
        // first we remove the 'overlap' in commits, example:
        //
        //    old logsegment checkpoint1-commit1-commit2-commit3
        // 1. new logsegment             commit1-commit2-commit3
        // 2. new logsegment             commit1-commit2-commit3-commit4
        // 3. new logsegment                     checkpoint2+commit2-commit3-commit4
        //
        // retain does
        // 1. new logsegment             [empty] -> caught above
        // 2. new logsegment             [commit4]
        // 3. new logsegment             [checkpoint2-commit3] -> caught above
        new_log_segment
            .listed
            .ascending_commit_files
            .retain(|log_path| old_version < log_path.version);
        // Deduplicate compaction files the same way: the new listing re-lists from
        // checkpoint_version, so it includes compaction files already in the old segment.
        // Note: This removes all _new_ compaction files that start at or before `old_version`,
        // which may drop useful compaction files that span across the old/new boundary
        // (e.g. a new compaction(1, 3) when old_version=2). This is conservative but safe.
        new_log_segment
            .listed
            .ascending_compaction_files
            .retain(|log_path| old_version < log_path.version);

        // We have new commits and no new checkpoint: replay new commits for P+M and then
        // create a new snapshot by combining LogSegments and building a new TableConfiguration.
        //
        // For the P&M optimization here we still use a transient `LazyCrc` (it lets
        // `read_protocol_metadata_opt` use the CRC file to skip log replay if P&M are in
        // the CRC). The Snapshot's eager CRC is built separately below via
        // `try_build_crc_from_file`.
        let crc_file = Self::resolve_crc_file(&new_log_segment, old_log_segment);
        let local_lazy_crc = LazyCrc::new(crc_file.clone());

        let (new_metadata, new_protocol) =
            new_log_segment.read_protocol_metadata_opt(engine, &local_lazy_crc)?;
        let table_configuration = TableConfiguration::try_new_from(
            existing_snapshot.table_configuration(),
            new_metadata,
            new_protocol,
            new_log_segment.end_version,
        )?;

        // NB: we must add the new log segment to the existing snapshot's log segment
        let mut ascending_commit_files = old_log_segment.listed.ascending_commit_files.clone();
        ascending_commit_files.extend(new_log_segment.listed.ascending_commit_files);
        let mut ascending_compaction_files =
            old_log_segment.listed.ascending_compaction_files.clone();
        ascending_compaction_files.extend(new_log_segment.listed.ascending_compaction_files);

        // Use the new latest_commit if available, otherwise use the old one
        // This handles the case where the new listing returned no commits
        let latest_commit_file =
            new_latest_commit_file.or_else(|| old_log_segment.listed.latest_commit_file.clone());
        // we can pass in just the old checkpoint parts since by the time we reach this line, we
        // know there are no checkpoints in the new log segment.
        let combined_log_segment = LogSegment::try_new(
            LogSegmentFiles {
                ascending_commit_files,
                ascending_compaction_files,
                checkpoint_parts: old_log_segment.listed.checkpoint_parts.clone(),
                latest_crc_file: crc_file,
                latest_commit_file,
                max_published_version: new_log_segment
                    .listed
                    .max_published_version
                    .max(old_log_segment.listed.max_published_version),
            },
            log_root,
            requested_version,
            // Preserve `_last_checkpoint` hint from old segment
            old_log_segment.last_checkpoint_hint_summary(),
        )?;

        // Build the snapshot's eager CRC. Try CRC file first; fall back to a minimal CRC
        // built from the just-loaded P&M (no file stats / DM / txn — RequiresCheckpointRead
        // and Untracked respectively).
        let crc =
            Self::try_build_crc_from_file(&combined_log_segment, engine).unwrap_or_else(|| {
                Arc::new(Crc::minimal_from_pm(
                    table_configuration.protocol().clone(),
                    table_configuration.metadata().clone(),
                ))
            });

        tracing::Span::current().record("version", table_configuration.version());
        Ok(Arc::new(Snapshot::new_with_crc(
            combined_log_segment,
            table_configuration,
            crc,
        )))
    }

    /// Determine which CRC file to use for an incremental snapshot update. Prefers the new
    /// segment's CRC file, falls back to the old segment's.
    fn resolve_crc_file(
        new_log_segment: &LogSegment,
        old_log_segment: &LogSegment,
    ) -> Option<ParsedLogPath> {
        new_log_segment
            .listed
            .latest_crc_file
            .clone()
            .or_else(|| old_log_segment.listed.latest_crc_file.clone())
    }

    /// Create a new [`Snapshot`] instance.
    ///
    /// Eagerly builds the [`Crc`] from disk:
    /// - CRC at target version -> load directly, P&M from the CRC.
    /// - CRC at an older version (stale) -> load + reverse-replay newer commits + apply.
    /// - No CRC, or CRC failed -> reverse-replay the entire segment (checkpoint + commits) to build
    ///   a fresh `Crc` from scratch.
    #[instrument(err, fields(version, operation_id = %operation_id), skip(engine))]
    fn try_new_from_log_segment(
        location: Url,
        log_segment: LogSegment,
        engine: &dyn Engine,
        operation_id: MetricId,
    ) -> DeltaResult<Self> {
        let crc = Self::build_crc(&log_segment, engine, operation_id)?;

        let table_configuration = TableConfiguration::try_new(
            crc.metadata.clone(),
            crc.protocol.clone(),
            location,
            log_segment.end_version,
        )?;

        tracing::Span::current().record("version", table_configuration.version());

        Ok(Self::new_with_crc(log_segment, table_configuration, crc))
    }

    /// Build the [`Crc`] for a snapshot at `log_segment.end_version` from storage.
    ///
    /// Strategy:
    /// 1. **CRC file present at target version**: load directly, no log replay.
    /// 2. **CRC file present at older version** (stale): load it, reverse-replay commits in
    ///    `(crc_version, end_version]`, apply update.
    /// 3. **No CRC file** (or load failed): reverse-replay the entire segment to build fresh.
    ///
    /// All paths produce a `Crc[end_version]` with P&M, file stats (typed validity), DM,
    /// txns, and ICT populated to the extent possible from the available log.
    ///
    /// The span name `segment.read_metadata` is significant -- the metrics reporter
    /// converts it into a
    /// [`ProtocolMetadataLoaded`](crate::metrics::events::MetricEvent::ProtocolMetadataLoaded)
    /// event.
    #[instrument(name = "segment.read_metadata", fields(report, operation_id = %operation_id), skip_all, err)]
    fn build_crc(
        log_segment: &LogSegment,
        engine: &dyn Engine,
        operation_id: MetricId,
    ) -> DeltaResult<Arc<Crc>> {
        let _ = operation_id; // span field
        match Self::try_build_crc_from_file(log_segment, engine) {
            Some(crc) => Ok(crc),
            None => {
                info!("No CRC file usable; building Crc from scratch via full log replay");
                Ok(Arc::new(log_segment.build_crc_from_scratch(engine)?))
            }
        }
    }

    /// Try to build a [`Crc`] from a CRC file on disk. Handles both CRC at target version
    /// (loaded directly, no replay) and stale CRC (loaded + incremental replay of newer
    /// commits). Returns `None` if no CRC file exists or any step (read or replay) failed.
    fn try_build_crc_from_file(log_segment: &LogSegment, engine: &dyn Engine) -> Option<Arc<Crc>> {
        let crc_file = log_segment.listed.latest_crc_file.as_ref()?;
        let crc_version = crc_file.version;
        let target_version = log_segment.end_version;

        let loaded = match try_read_crc_file(engine, crc_file) {
            Ok(crc) => crc,
            Err(e) => {
                warn!(
                    "Failed to read CRC file at version {crc_version}: {e}; \
                     falling back to log replay"
                );
                return None;
            }
        };

        if crc_version == target_version {
            info!("CRC loaded from disk at target version {target_version}");
            return Some(Arc::new(loaded));
        }

        if crc_version > target_version {
            // Time-travel target older than CRC: cannot use this CRC. Fall back.
            warn!(
                "CRC at version {crc_version} is newer than target {target_version}; \
                 ignoring (time-travel)"
            );
            return None;
        }

        // Stale CRC: replay commits in (crc_version, target_version] and apply.
        info!(
            "Building Crc[{}] from stale CRC[{}] + incremental replay",
            target_version, crc_version
        );
        let pruned = log_segment.segment_after_crc(crc_version);
        let mut crc = loaded;
        match pruned.build_crc_from_stale(engine, &mut crc) {
            Ok(()) => {
                info!(
                    "Built Crc[{}] from stale CRC[{}]",
                    target_version, crc_version
                );
                Some(Arc::new(crc))
            }
            Err(e) => {
                warn!(
                    "Failed to incrementally replay onto stale CRC[{crc_version}]: {e}; \
                     falling back to full log replay"
                );
                None
            }
        }
    }

    /// Creates a new [`Snapshot`] representing the table state immediately after a commit.
    ///
    /// Implements the universal invariant `Crc[N] + CrcUpdate(N->N+1) = Crc[N+1]`:
    /// 1. Append the new commit to the log segment, bumping version to N+1.
    /// 2. Clone this snapshot's CRC (which is `Crc[N]`).
    /// 3. Apply the `crc_update` produced by the transaction.
    /// 4. Wrap the result as the post-commit snapshot's CRC.
    ///
    /// The result is `Crc[N+1]` available in memory without any I/O. Connectors may then
    /// optionally call [`write_checksum`](Self::write_checksum) to persist it as N+1.crc.
    ///
    /// Note: CREATE TABLE doesn't go through this path; it builds the CRC via
    /// [`CrcUpdate::into_fresh_crc`] in [`Transaction::into_committed`].
    pub(crate) fn new_post_commit(
        &self,
        commit: ParsedLogPath,
        crc_update: CrcUpdate,
    ) -> DeltaResult<Self> {
        require!(
            commit.is_commit(),
            Error::internal_error(format!(
                "Cannot create post-commit Snapshot. Log file is not a commit file. \
                Path: {}, Type: {:?}.",
                commit.location.location, commit.file_type
            ))
        );
        let read_version = self.version();
        let new_version = commit.version;
        require!(
            new_version == read_version.wrapping_add(1),
            Error::internal_error(format!(
                "Cannot create post-commit Snapshot. Log file version ({new_version}) does not \
                equal Snapshot version ({read_version}) + 1."
            ))
        );

        let new_table_configuration = TableConfiguration::new_post_commit(
            self.table_configuration(),
            new_version,
            crc_update.metadata.clone(),
            crc_update.protocol.clone(),
        )?;

        let new_log_segment = self.log_segment.new_with_commit_appended(commit)?;

        // Crc[N] + CrcUpdate(N->N+1) = Crc[N+1].
        let mut new_crc = (*self.crc).clone();
        new_crc.apply(crc_update);

        Ok(Snapshot::new_with_crc(
            new_log_segment,
            new_table_configuration,
            Arc::new(new_crc),
        ))
    }

    /// Creates a [`CheckpointWriter`] for generating a checkpoint from this snapshot.
    ///
    /// See the [`crate::checkpoint`] module documentation for more details on checkpoint types
    /// and the overall checkpoint process.
    pub fn create_checkpoint_writer(self: Arc<Self>) -> DeltaResult<CheckpointWriter> {
        CheckpointWriter::try_new(self)
    }

    /// Performs a complete checkpoint of this snapshot using the provided engine.
    ///
    /// If a checkpoint already exists at this version, returns
    /// [`CheckpointWriteResult::AlreadyExists`] with the original snapshot unchanged.
    /// Otherwise, writes a checkpoint parquet file and the `_last_checkpoint` file and returns
    /// [`CheckpointWriteResult::Written`] with an updated [`SnapshotRef`] whose log segment
    /// reflects the new checkpoint. Commits and compaction files subsumed by the checkpoint are
    /// dropped from the returned snapshot.
    ///
    /// Note:
    ///     - It is still possible that an existing checkpoint gets overwritten if that checkpoint
    ///       was written by a concurrent writer.
    ///     - This function uses [`crate::ParquetHandler::write_parquet_file`] and
    ///       [`crate::StorageHandler::head`], which may not be implemented by all engines. If you
    ///       are using the default engine, make sure to build it with the multi-threaded executor
    ///       if you want to use this method.
    #[instrument(parent = &self.span, name = "snap.checkpoint", skip_all, err)]
    pub fn checkpoint(
        self: &SnapshotRef,
        engine: &dyn Engine,
    ) -> DeltaResult<(CheckpointWriteResult, SnapshotRef)> {
        if self.log_segment.checkpoint_version == Some(self.log_segment.end_version) {
            info!(
                "Checkpoint already exists for snapshot version {}",
                self.version()
            );
            return Ok((CheckpointWriteResult::AlreadyExists, Arc::clone(self)));
        }

        let writer = Arc::clone(self).create_checkpoint_writer()?;
        let checkpoint_path = writer.checkpoint_path()?;
        let data_iter = writer.checkpoint_data(engine)?;
        let state = data_iter.state();
        let lazy_data = data_iter.map(|r| r.and_then(|f| f.apply_selection_vector()));
        match engine
            .parquet_handler()
            .write_parquet_file(checkpoint_path.clone(), Box::new(lazy_data))
        {
            Ok(()) => (),
            Err(Error::FileAlreadyExists(_)) => {
                // NOTE: Per write_parquet_file's documentation, it should silently overwrite
                // existing files, so we log a warning but still return the correct
                // result.
                warn!(
                    "ParquetHandler::write_parquet_file unexpectedly failed on \
                    FileAlreadyExists for version {}",
                    self.version()
                );
                return Ok((CheckpointWriteResult::AlreadyExists, Arc::clone(self)));
            }
            Err(e) => return Err(e),
        }

        let file_meta = engine.storage_handler().head(&checkpoint_path)?;

        // Build last-checkpoint stats from the iterator state, then finalize
        // (writes `_last_checkpoint`).
        let state = Arc::into_inner(state).ok_or_else(|| {
            Error::internal_error("ActionReconciliationIteratorState Arc has other references")
        })?;
        // V1 checkpoint: no sidecars are written.
        let last_checkpoint_stats = LastCheckpointHintStats::from_reconciliation_state(
            state,
            file_meta.size,
            0, /* num_sidecars */
        )?;
        writer.finalize(engine, &last_checkpoint_stats)?;

        let checkpoint_log_path = ParsedLogPath::try_from(file_meta)?.ok_or_else(|| {
            Error::internal_error("Checkpoint path could not be parsed as a log path")
        })?;
        let _checkpoint_version = checkpoint_log_path.version;
        let new_log_segment = self
            .log_segment
            .try_new_with_checkpoint(checkpoint_log_path)?;
        // The CRC carries forward unchanged: it represents the same logical state as before
        // the checkpoint write (just at a more compact log representation). Stale-CRC
        // detection is per-load-time, not per-checkpoint-time.
        Ok((
            CheckpointWriteResult::Written,
            Arc::new(Snapshot::new_with_crc(
                new_log_segment,
                self.table_configuration().clone(),
                self.crc.clone(),
            )),
        ))
    }

    /// Creates a [`LogCompactionWriter`] for generating a log compaction file.
    ///
    /// Log compaction aggregates commit files in a version range into a single compacted file,
    /// improving performance by reducing the number of files to process during log replay.
    ///
    /// # Parameters
    /// - `start_version`: The first version to include in the compaction (inclusive)
    /// - `end_version`: The last version to include in the compaction (inclusive)
    ///
    /// # Returns
    /// A [`LogCompactionWriter`] that can be used to generate the compaction file.
    ///
    /// NOTE: This method is currently a no-op because log compaction is disabled (#2337)
    pub fn log_compaction_writer(
        self: Arc<Self>,
        start_version: Version,
        end_version: Version,
    ) -> DeltaResult<LogCompactionWriter> {
        LogCompactionWriter::try_new(self, start_version, end_version)
    }

    /// Log segment this snapshot uses
    #[internal_api]
    pub(crate) fn log_segment(&self) -> &LogSegment {
        &self.log_segment
    }

    pub fn table_root(&self) -> &Url {
        self.table_configuration.table_root()
    }

    /// Version of this `Snapshot` in the table.
    pub fn version(&self) -> Version {
        self.table_configuration().version()
    }

    /// Table [`Schema`] at this `Snapshot`s version.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> SchemaRef {
        self.table_configuration.logical_schema()
    }

    /// Get the [`TableProperties`] for this [`Snapshot`].
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration().table_properties()
    }

    /// Returns the protocol-derived table properties as a map of key-value pairs.
    ///
    /// This includes:
    /// - `delta.minReaderVersion` and `delta.minWriterVersion`
    /// - `delta.feature.<name> = "supported"` for each reader and writer feature (when using table
    ///   features protocol, i.e. reader version 3 / writer version 7)
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_protocol_derived_properties(&self) -> HashMap<String, String> {
        let protocol = self.table_configuration().protocol();

        let mut properties = HashMap::from([
            (
                "delta.minReaderVersion".into(),
                protocol.min_reader_version().to_string(),
            ),
            (
                "delta.minWriterVersion".into(),
                protocol.min_writer_version().to_string(),
            ),
        ]);

        let features = protocol
            .reader_features()
            .into_iter()
            .flatten()
            .chain(protocol.writer_features().into_iter().flatten());

        for feature in features {
            properties
                .entry(format!("delta.feature.{}", feature.as_ref()))
                .or_insert_with(|| "supported".to_string());
        }

        properties
    }

    /// Get the raw metadata configuration for this table.
    ///
    /// This returns the `Metadata.configuration` map as stored in the Delta log, containing
    /// user-defined properties, delta table properties (e.g., `delta.enableInCommitTimestamps`),
    /// and application-specific properties (e.g., `io.unitycatalog.tableId`).
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn metadata_configuration(&self) -> &HashMap<String, String> {
        self.table_configuration().metadata().configuration()
    }

    /// Get the [`TableConfiguration`] for this [`Snapshot`].
    #[internal_api]
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        &self.table_configuration
    }

    /// Create a [`ScanBuilder`] for an `SnapshotRef`.
    pub fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Create a [`Transaction`] for this `SnapshotRef`. With the specified [`Committer`].
    ///
    /// Note: For tables with clustering enabled, this performs log replay to read clustering
    /// columns from domain metadata, which may have a performance cost.
    pub fn transaction(
        self: Arc<Self>,
        committer: Box<dyn Committer>,
        engine: &dyn Engine,
    ) -> DeltaResult<Transaction> {
        Transaction::try_new_existing_table(self, committer, engine)
    }

    /// Creates a builder for altering this table's metadata. Currently supports schema change
    /// operations.
    ///
    /// The returned builder allows chaining operations before building an
    /// [`AlterTableTransaction`] that can be committed.
    ///
    /// [`AlterTableTransaction`]: crate::transaction::AlterTableTransaction
    pub fn alter_table(self: Arc<Self>) -> AlterTableTransactionBuilder {
        AlterTableTransactionBuilder::new(self)
    }

    /// Fetch the latest version of the provided `application_id` for this snapshot. Filters
    /// the txn based on `delta.setTransactionRetentionDuration` and `lastUpdated`.
    ///
    /// Branches on the CRC's [`SetTransactionState`]:
    /// - [`Complete`](SetTransactionState::Complete): the cache is authoritative; a miss means no
    ///   such txn exists at this version. Return directly.
    /// - [`Partial`](SetTransactionState::Partial): the cache has known-correct entries from newer
    ///   commits. On hit, return; on miss, the txn might exist in older commits not covered by the
    ///   cache -> fall through to log replay.
    /// - [`Untracked`](SetTransactionState::Untracked): no cache -> fall through to log replay.
    // TODO: add a get_app_id_versions to fetch all at once using SetTransactionScanner::get_all
    #[instrument(parent = &self.span, name = "snap.get_app_id_version", skip_all, err)]
    pub fn get_app_id_version(
        &self,
        application_id: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<i64>> {
        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(self.table_properties())?;

        match &self.crc.set_transactions {
            SetTransactionState::Complete(map) => {
                // Authoritative for both hits and misses.
                return Ok(map
                    .get(application_id)
                    .filter(|txn| !is_set_txn_expired(expiration_timestamp, txn.last_updated))
                    .map(|txn| txn.version));
            }
            SetTransactionState::Partial(map) => {
                // Hit: return. Miss: must fall through to log replay (older commits may
                // have set this txn, and the partial cache doesn't know).
                if let Some(txn) = map.get(application_id) {
                    return Ok(
                        if is_set_txn_expired(expiration_timestamp, txn.last_updated) {
                            None
                        } else {
                            Some(txn.version)
                        },
                    );
                }
            }
            SetTransactionState::Untracked => {}
        }

        // Fallback: full log replay.
        let txn = SetTransactionScanner::get_one(
            self.log_segment(),
            application_id,
            engine,
            expiration_timestamp,
        )?;
        Ok(txn.map(|t| t.version))
    }

    /// Fetch the domainMetadata for a specific domain in this snapshot. This returns the latest
    /// configuration for the domain, or None if the domain does not exist.
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    pub fn get_domain_metadata(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<String>> {
        if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
            return Err(Error::generic(
                "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain",
            ));
        }

        self.get_domain_metadata_internal(domain, engine)
    }

    /// Get the logical clustering columns for this snapshot, if clustering is enabled.
    ///
    /// Returns `Ok(Some(columns))` if the ClusteredTable feature is enabled and clustering
    /// columns are defined, `Ok(None)` if clustering is not enabled, or an error if the
    /// clustering metadata is malformed.
    ///
    /// The columns are returned as logical [`ColumnName`]s. When column mapping is enabled,
    /// this converts the physical names stored in domain metadata back to logical names using
    /// the table schema.
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    ///
    /// # Errors
    ///
    /// Returns an error if the clustering domain metadata is malformed, or if a physical
    /// column name cannot be resolved to a logical name in the schema.
    ///
    /// [`ColumnName`]: crate::expressions::ColumnName
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_logical_clustering_columns(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        let physical_columns = match self.get_physical_clustering_columns(engine)? {
            Some(cols) => cols,
            None => return Ok(None),
        };
        let column_mapping_mode = self.table_configuration.column_mapping_mode();
        if column_mapping_mode == ColumnMappingMode::None {
            // No column mapping: physical = logical
            return Ok(Some(physical_columns));
        }
        // Convert physical column names to logical names by walking the schema
        let logical_schema = self.table_configuration.logical_schema();
        let logical_columns = physical_columns
            .iter()
            .map(|physical_col| {
                physical_to_logical_column_name(&logical_schema, physical_col, column_mapping_mode)
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Some(logical_columns))
    }

    /// Get the clustering columns for this snapshot, if the table has clustering enabled.
    ///
    /// Returns `Ok(Some(columns))` if the ClusteredTable feature is enabled and clustering
    /// columns are defined, `Ok(None)` if clustering is not enabled, or an error if the
    /// clustering metadata is malformed.
    ///
    /// The columns are returned as physical column names, respecting the column mapping mode.
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    #[internal_api]
    pub(crate) fn get_physical_clustering_columns(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        if !self
            .table_configuration
            .protocol()
            .has_table_feature(&TableFeature::ClusteredTable)
        {
            return Ok(None);
        }
        match self.get_domain_metadata_internal(CLUSTERING_DOMAIN_NAME, engine)? {
            Some(config) => Ok(Some(parse_clustering_columns(&config)?)),
            None => Ok(None),
        }
    }

    /// Load domain metadata from this snapshot. If `domains` is `Some`, only load the
    /// specified domains. If `None`, load all domains.
    ///
    /// Branches on the CRC's [`DomainMetadataState`]:
    /// - [`Complete`](DomainMetadataState::Complete): cache is authoritative for hits AND misses.
    ///   Return directly (filtered by `domains` if specified).
    /// - [`Partial`](DomainMetadataState::Partial): cache has known-correct entries from newer
    ///   commits. For specific-domain queries, check cache; on miss, replay only the missing
    ///   domains. For "all domains" queries, must fully replay (cache might be missing older
    ///   domains).
    /// - [`Untracked`](DomainMetadataState::Untracked): no cache -> log replay.
    ///
    /// This is the single entry point for all domain metadata reads on a snapshot.
    #[internal_api]
    pub(crate) fn get_domain_metadatas_internal(
        &self,
        engine: &dyn Engine,
        domains: Option<&HashSet<&str>>,
    ) -> DeltaResult<DomainMetadataMap> {
        match &self.crc.domain_metadata {
            DomainMetadataState::Complete(map) => {
                let result = match domains {
                    None => map.clone(),
                    Some(filter) => map
                        .iter()
                        .filter(|(k, _)| filter.contains(k.as_str()))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                };
                return Ok(result);
            }
            DomainMetadataState::Partial(map) => {
                if let Some(filter) = domains {
                    // Specific-domain query: serve hits from cache, replay only misses.
                    let mut result: DomainMetadataMap = HashMap::new();
                    let mut missing: HashSet<&str> = HashSet::new();
                    for &domain in filter {
                        if let Some(dm) = map.get(domain) {
                            result.insert(domain.to_string(), dm.clone());
                        } else {
                            missing.insert(domain);
                        }
                    }
                    if missing.is_empty() {
                        return Ok(result);
                    }
                    let replayed = self
                        .log_segment()
                        .scan_domain_metadatas(Some(&missing), engine)?;
                    result.extend(replayed);
                    return Ok(result);
                }
                // "All domains" query against Partial: must replay (cache may miss older
                // domains).
            }
            DomainMetadataState::Untracked => {}
        }
        // Fallback: full log replay.
        self.log_segment().scan_domain_metadatas(domains, engine)
    }

    /// Returns file-level statistics if the CRC has [`Valid`] file stats. Performs no I/O.
    ///
    /// Returns `None` for non-`Valid` states ([`Indeterminate`], [`Untrackable`],
    /// [`RequiresCheckpointRead`]). Callers that need to recover from a non-`Valid` state
    /// should call [`load_file_stats`](Self::load_file_stats), which returns a new
    /// [`SnapshotRef`] with rebuilt stats.
    ///
    /// [`Valid`]: crate::crc::FileStatsState::Valid
    /// [`Indeterminate`]: crate::crc::FileStatsState::Indeterminate
    /// [`Untrackable`]: crate::crc::FileStatsState::Untrackable
    /// [`RequiresCheckpointRead`]: crate::crc::FileStatsState::RequiresCheckpointRead
    pub fn get_file_stats(&self) -> Option<FileStats> {
        self.crc.file_stats()
    }

    /// Returns the snapshot's eager CRC. Always available since `crc` is always present.
    ///
    /// Test-only helper for integration tests to inspect CRC state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn get_current_crc_if_loaded_for_testing(&self) -> Option<&Crc> {
        Some(self.crc.as_ref())
    }

    /// Returns the snapshot's CRC version. Always equals [`version`](Self::version) under
    /// the eager-`Arc<Crc>` design.
    ///
    /// Test-only helper.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn crc_version_for_testing(&self) -> Option<Version> {
        Some(self.version())
    }

    /// Recover file stats by performing full action reconciliation across the log.
    ///
    /// Returns a new [`SnapshotRef`] whose CRC has either:
    /// - [`Valid`](crate::crc::FileStatsState::Valid) absolute counts, byte totals, and a
    ///   histogram, OR
    /// - [`Untrackable`](crate::crc::FileStatsState::Untrackable) if any Remove action in the
    ///   reachable history had a missing `size` field (byte totals are then permanently impossible
    ///   to compute).
    ///
    /// Behavior by current CRC state:
    /// - [`Valid`](crate::crc::FileStatsState::Valid): returns the same snapshot. No work.
    /// - [`Indeterminate`](crate::crc::FileStatsState::Indeterminate) /
    ///   [`RequiresCheckpointRead`](crate::crc::FileStatsState::RequiresCheckpointRead):
    ///   reverse-replays the entire log segment with `(path, dv_unique_id)` deduplication and
    ///   rebuilds file stats from scratch.
    /// - [`Untrackable`](crate::crc::FileStatsState::Untrackable): returns
    ///   [`Error::ChecksumWriteUnsupported`]. There is no recovery for this state -- a Remove
    ///   action committed without a `size` permanently destroys byte-level tracking.
    ///
    /// All other CRC fields (P&M, DM, txns, ICT) are preserved.
    ///
    /// # Cost
    ///
    /// Reads every commit in the log segment plus the checkpoint (if any), with a wider
    /// schema than ordinary snapshot loads (includes Add/Remove `path`, `size`, and DV
    /// descriptor columns for deduplication). Connectors should call this only when
    /// [`Crc::file_stats_validity`](crate::crc::Crc::file_stats_validity) indicates a
    /// non-`Valid` state.
    ///
    /// # Errors
    ///
    /// - I/O errors from the engine while reading commits and checkpoint.
    /// - [`Error::ChecksumWriteUnsupported`] when the current state is `Untrackable`.
    #[instrument(parent = &self.span, name = "snap.load_file_stats", skip_all, err)]
    pub fn load_file_stats(self: &SnapshotRef, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
        use crate::crc::FileStatsValidity;

        match self.crc.file_stats_validity() {
            FileStatsValidity::Valid => return Ok(Arc::clone(self)),
            FileStatsValidity::Untrackable => {
                return Err(Error::ChecksumWriteUnsupported(
                    "File stats are Untrackable -- a Remove action with a missing size \
                     was encountered. Byte-level statistics are permanently unrecoverable."
                        .to_string(),
                ));
            }
            FileStatsValidity::Indeterminate | FileStatsValidity::RequiresCheckpointRead => {}
        }

        let recovered = self.log_segment().recover_file_stats(engine)?;

        // Preserve all other CRC fields; only file_stats is rebuilt.
        let mut new_crc = (*self.crc).clone();
        new_crc.file_stats = recovered;

        Ok(Arc::new(Snapshot::new_with_crc(
            self.log_segment().clone(),
            self.table_configuration().clone(),
            Arc::new(new_crc),
        )))
    }

    /// Writes a version checksum (CRC) file for this snapshot. Writers should call this
    /// after every commit because checksums enable faster snapshot loading and table state
    /// validation.
    ///
    /// Returns a tuple of [`ChecksumWriteResult`] and a [`SnapshotRef`]. On
    /// [`ChecksumWriteResult::Written`], the returned snapshot has the CRC file recorded in
    /// its log segment. On [`ChecksumWriteResult::AlreadyExists`], the original snapshot is
    /// returned unchanged.
    ///
    /// # Errors
    ///
    /// - [`Error::ChecksumWriteUnsupported`] if the CRC's file stats are not in a writable state
    ///   ([`Valid`](crate::crc::FileStatsState::Valid)). Non-Valid states arise from: (a) a
    ///   non-incremental operation like `ANALYZE STATS`
    ///   ([`Indeterminate`](crate::crc::FileStatsState::Indeterminate)) -- recoverable via
    ///   [`load_file_stats`](Self::load_file_stats); (b) a file action had a missing size
    ///   ([`Untrackable`](crate::crc::FileStatsState::Untrackable)) -- permanently
    ///   unrecoverable; (c) the snapshot was built without a checkpoint read
    ///   ([`RequiresCheckpointRead`](crate::crc::FileStatsState::RequiresCheckpointRead)) --
    ///   recoverable via [`load_file_stats`](Self::load_file_stats).
    /// - I/O errors from the engine's storage handler if the write fails.
    ///
    /// [`CommittedTransaction::post_commit_snapshot`]: crate::transaction::CommittedTransaction::post_commit_snapshot
    #[instrument(parent = &self.span, name = "snap.write_checksum", skip_all, err)]
    pub fn write_checksum(
        self: &SnapshotRef,
        engine: &dyn Engine,
    ) -> DeltaResult<(ChecksumWriteResult, SnapshotRef)> {
        let has_crc_on_disk = self
            .log_segment
            .listed
            .latest_crc_file
            .as_ref()
            .is_some_and(|f| f.version == self.version());

        if has_crc_on_disk {
            info!(
                "CRC file already exists on disk at version {}",
                self.version()
            );
            return Ok((ChecksumWriteResult::AlreadyExists, Arc::clone(self)));
        }

        let crc_path = ParsedLogPath::new_crc(self.table_root(), self.version())?;

        // try_write_crc_file validates is_writable() before writing.
        match try_write_crc_file(engine, &crc_path.location, &self.crc) {
            Ok(()) => {
                info!("Wrote CRC file at {}", crc_path.location);
                let new_log_segment = self.log_segment.try_new_with_crc_file(crc_path)?;
                let new_snapshot = Arc::new(Snapshot::new_with_crc(
                    new_log_segment,
                    self.table_configuration().clone(),
                    self.crc.clone(),
                ));
                Ok((ChecksumWriteResult::Written, new_snapshot))
            }
            Err(Error::FileAlreadyExists(_)) => {
                info!(
                    "Another writer beat us to writing CRC file at {}",
                    crc_path.location
                );
                Ok((ChecksumWriteResult::AlreadyExists, Arc::clone(self)))
            }
            Err(e) => Err(e),
        }
    }

    /// Publishes all catalog commits at this table version. Applicable only to catalog-managed
    /// tables. This method is a no-op for filesystem-managed tables or if there are no catalog
    /// commits to publish.
    ///
    /// Publishing copies ratified catalog commits to the Delta log as published Delta files,
    /// reducing catalog storage requirements and enabling some table maintenance operations,
    /// like checkpointing.
    ///
    /// # Parameters
    ///
    /// - `engine`: The engine to use for publishing commits
    ///
    /// # Errors
    ///
    /// Returns an error if the publish operation fails, or if there are catalog commits that need
    /// publishing but the table or committer don't support publishing.
    ///
    /// # See Also
    ///
    /// - [`Committer::publish`]
    #[instrument(parent = &self.span, name = "snap.publish", skip_all, err)]
    pub fn publish(
        self: &SnapshotRef,
        engine: &dyn Engine,
        committer: &dyn Committer,
    ) -> DeltaResult<SnapshotRef> {
        let unpublished_catalog_commits = self.log_segment().get_unpublished_catalog_commits()?;

        if unpublished_catalog_commits.is_empty() {
            return Ok(Arc::clone(self));
        }

        require!(
            unpublished_catalog_commits
                .windows(2)
                .all(|commits| commits[0].version() + 1 == commits[1].version()),
            Error::generic(format!(
                "Expected ordered and contiguous unpublished catalog commits. \
                 Got: {unpublished_catalog_commits:?}"
            ))
        );

        require!(
            self.table_configuration().is_catalog_managed(),
            Error::generic(
                "There are catalog commits that need publishing, but the table is not catalog-managed.",
            )
        );

        require!(
            committer.is_catalog_committer(),
            Error::generic(
                "There are catalog commits that need publishing, but the committer is not a catalog committer.",
            )
        );

        let publish_metadata =
            PublishMetadata::try_new(self.version(), unpublished_catalog_commits)?;

        committer.publish(engine, publish_metadata)?;

        Ok(Arc::new(Snapshot::new_with_crc(
            self.log_segment().new_as_published()?,
            self.table_configuration().clone(),
            self.crc.clone(),
        )))
    }

    /// Fetch both user-controlled and system-controlled domain metadata for a specific domain
    /// in this snapshot.
    ///
    /// Returns the latest configuration for the domain, or `None` if the domain does not exist
    /// (or was removed). Unlike [`Snapshot::get_domain_metadata`], this does not reject `delta.*`
    /// domains.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_domain_metadata_internal(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<String>> {
        let mut map = self.get_domain_metadatas_internal(engine, Some(&HashSet::from([domain])))?;
        Ok(map.remove(domain).map(|dm| dm.configuration().to_owned()))
    }

    /// Fetch all non-internal domain metadata for this snapshot as a `Vec`.
    ///
    /// Internal (`delta.*`) domains are filtered out.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_all_domain_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        let all_metadata = self.get_domain_metadatas_internal(engine, None)?;
        Ok(all_metadata
            .into_values()
            .filter(|domain| !domain.is_internal())
            .collect())
    }

    /// Get the In-Commit Timestamp (ICT) for this snapshot.
    ///
    /// Returns the `inCommitTimestamp` from the CommitInfo action of the commit that created this
    /// snapshot.
    ///
    /// # Returns
    /// - `Ok(Some(timestamp))` - ICT is enabled and available for this version
    /// - `Ok(None)` - ICT is not enabled
    /// - `Err(...)` - ICT is enabled but cannot be read, or enablement version is invalid
    #[instrument(parent = &self.span, name = "snap.get_ict", skip_all, err)]
    #[internal_api]
    pub(crate) fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        // Get ICT enablement info and check if we should read ICT for this version
        let enablement = self
            .table_configuration()
            .in_commit_timestamp_enablement()?;

        // Return None if ICT is not enabled at all
        if matches!(enablement, InCommitTimestampEnablement::NotEnabled) {
            return Ok(None);
        }

        // If ICT is enabled with an enablement version, verify the enablement version is not in the
        // future
        if let InCommitTimestampEnablement::Enabled {
            enablement: Some((enablement_version, _)),
        } = enablement
        {
            if self.version() < enablement_version {
                return Err(Error::generic(format!(
                    "Invalid state: snapshot at version {} has ICT enablement version {} in the future",
                    self.version(),
                    enablement_version
                )));
            }
        }

        // Fast path: serve from CRC if it has an ICT.
        if let Some(ict) = self.crc.in_commit_timestamp_opt {
            return Ok(Some(ict));
        }

        // Fallback: read the ICT from the latest commit file. (CRC may lack ICT if it was
        // written before ICT was enabled, or if the CRC was built via an incremental
        // path that did not capture commitInfo.)
        match &self.log_segment.listed.latest_commit_file {
            Some(commit_file_meta) => {
                let ict = commit_file_meta.read_in_commit_timestamp(engine)?;
                Ok(Some(ict))
            }
            None => Err(Error::generic("Last commit file not found in log segment")),
        }
    }

    /// Get the timestamp for this snapshot's version, in milliseconds since the Unix epoch.
    ///
    /// When In-Commit Timestamp (ICT) are enabled, returns the In-Commit Timestamp value.
    /// Otherwise, falls back to the filesystem last-modified time of the latest commit file.
    ///
    /// Returns an error if the commit file is missing, the ICT configuration is invalid, or the
    /// ICT value cannot be read.
    ///
    /// See also [`get_in_commit_timestamp`] for ICT-only semantics.
    ///
    /// [`get_in_commit_timestamp`]: Self::get_in_commit_timestamp
    #[allow(unused)]
    #[instrument(parent = &self.span, name = "snap.get_ts", skip_all, err)]
    pub fn get_timestamp(&self, engine: &dyn Engine) -> DeltaResult<i64> {
        match self
            .table_configuration()
            .in_commit_timestamp_enablement()?
        {
            InCommitTimestampEnablement::NotEnabled => {
                match &self.log_segment.listed.latest_commit_file {
                    Some(commit_file_meta) => {
                        let ts = commit_file_meta.location.last_modified;
                        Ok(ts)
                    }
                    None => Err(Error::generic(format!(
                        "Last commit file not found in log segment for version {} \
                         (ICT disabled): cannot read filesystem modification timestamp",
                        self.version()
                    ))),
                }
            }
            InCommitTimestampEnablement::Enabled { .. } => self
                .get_in_commit_timestamp(engine)
                .map_err(|e| {
                    Error::generic(format!(
                        "Unable to read in-commit timestamp for version {}: {e}",
                        self.version()
                    ))
                })?
                .ok_or_else(|| {
                    Error::internal_error(format!(
                        "Invalid state: version {}, ICT is enabled \
                        but get_in_commit_timestamp returned None",
                        self.version()
                    ))
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::json;
    use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder, VersionTarget};
    use test_utils::{add_commit, delta_path_for_version};

    use super::*;
    use crate::actions::{DomainMetadata, Protocol};
    use crate::arrow::array::StringArray;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::{
        TokioBackgroundExecutor, TokioMultiThreadExecutor,
    };
    use crate::engine::default::filesystem::ObjectStoreStorageHandler;
    use crate::engine::default::{DefaultEngine, DefaultEngineBuilder};
    use crate::engine::sync::SyncEngine;
    use crate::last_checkpoint_hint::LastCheckpointHint;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::local::LocalFileSystem;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::ArrowWriter;
    use crate::path::{LogPathFileType, ParsedLogPath};
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::{
        TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
    };
    use crate::table_properties::ENABLE_IN_COMMIT_TIMESTAMPS;
    use crate::transaction::create_table::create_table;
    use crate::utils::test_utils::{assert_result_error_with_message, string_array_to_engine_data};

    /// Helper function to create a commitInfo action with optional ICT
    fn create_commit_info(timestamp: i64, ict: Option<i64>) -> serde_json::Value {
        let mut commit_info = json!({
            "timestamp": timestamp,
            "operation": "WRITE",
        });

        if let Some(ict_value) = ict {
            commit_info["inCommitTimestamp"] = json!(ict_value);
        }

        json!({
            "commitInfo": commit_info
        })
    }

    fn create_protocol(ict_enabled: bool, min_reader_version: Option<u32>) -> serde_json::Value {
        let reader_version = min_reader_version.unwrap_or(1);

        if ict_enabled {
            let mut protocol = json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": TABLE_FEATURES_MIN_WRITER_VERSION,
                    "writerFeatures": ["inCommitTimestamp"]
                }
            });

            // Only include readerFeatures if minReaderVersion >= table-features minimum.
            if reader_version >= TABLE_FEATURES_MIN_READER_VERSION as u32 {
                protocol["protocol"]["readerFeatures"] = json!([]);
            }

            protocol
        } else {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": 2
                }
            })
        }
    }

    fn create_metadata(
        id: Option<&str>,
        schema_string: Option<&str>,
        created_time: Option<u64>,
        ict_config: Option<(String, String)>,
        ict_enabled_but_missing_version: bool,
    ) -> serde_json::Value {
        let config = if ict_enabled_but_missing_version {
            // Special case for testing ICT enabled but missing enablement info
            json!({
                "delta.enableInCommitTimestamps": "true"
            })
        } else if let Some((enablement_version, enablement_timestamp)) = ict_config {
            json!({
                "delta.enableInCommitTimestamps": "true",
                "delta.inCommitTimestampEnablementVersion": enablement_version,
                "delta.inCommitTimestampEnablementTimestamp": enablement_timestamp
            })
        } else {
            json!({})
        };

        json!({
            "metaData": {
                "id": id.unwrap_or("testId"),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string.unwrap_or("{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"),
                "partitionColumns": [],
                "configuration": config,
                "createdTime": created_time.unwrap_or(1587968586154u64)
            }
        })
    }

    fn create_basic_commit(ict_enabled: bool, ict_config: Option<(String, String)>) -> String {
        let protocol = create_protocol(ict_enabled, None);
        let metadata = create_metadata(None, None, None, ict_config, false);
        format!("{protocol}\n{metadata}")
    }

    fn create_snapshot_with_commit_file_absent_from_log_segment(
        url: &Url,
        table_cfg: TableConfiguration,
    ) -> DeltaResult<Snapshot> {
        // Create a log segment with only checkpoint and no commit file (simulating scenario
        // where a checkpoint exists but the commit file has been cleaned up)
        let checkpoint_parts = vec![ParsedLogPath::try_from(crate::FileMeta {
            location: url.join("_delta_log/00000000000000000000.checkpoint.parquet")?,
            last_modified: 0,
            size: 100,
        })?
        .unwrap()];

        let listed_files = LogSegmentFiles {
            checkpoint_parts,
            ..Default::default()
        };

        let log_segment =
            LogSegment::try_new(listed_files, url.join("_delta_log/")?, Some(0), None)?;

        let crc = Arc::new(Crc::minimal_from_pm(
            table_cfg.protocol().clone(),
            table_cfg.metadata().clone(),
        ));
        Ok(Snapshot::new_with_crc(log_segment, table_cfg, crc))
    }

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();

        let expected = Protocol::try_new_modern(["deletionVectors"], ["deletionVectors"]).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    #[test]
    fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let expected = Protocol::try_new_modern(["deletionVectors"], ["deletionVectors"]).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    // TODO: unify this and lots of stuff in LogSegment tests and test_utils.
    async fn commit(
        table_root: impl AsRef<str>,
        store: &InMemory,
        version: Version,
        commit: Vec<serde_json::Value>,
    ) {
        let commit_data = commit
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join("\n");
        add_commit(table_root, store, version, commit_data)
            .await
            .unwrap();
    }

    // interesting cases for testing Snapshot::new_from:
    // 1. new version < existing version
    // 2. new version == existing version
    // 3. new version > existing version AND
    //   a. log segment hasn't changed
    //   b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
    //   b. log segment for old..=new version has no checkpoint
    //     i. commits have (new protocol, new metadata)
    //     ii. commits have (new protocol, no metadata)
    //     iii. commits have (no protocol, new metadata)
    //     iv. commits have (no protocol, no metadata)
    #[tokio::test]
    async fn test_snapshot_new_from() -> DeltaResult<()> {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let old_snapshot = Snapshot::builder_for(url.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        // 1. new version < existing version: error
        let snapshot_res = Snapshot::builder_from(old_snapshot.clone())
            .at_version(0)
            .build(&engine);
        assert!(matches!(
            snapshot_res,
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 0 is older than snapshot hint version 1"
        ));

        // 2. new version == existing version
        let snapshot = Snapshot::builder_from(old_snapshot.clone())
            .at_version(1)
            .build(&engine)
            .unwrap();
        let expected = old_snapshot.clone();
        assert_eq!(snapshot, expected);

        // tests Snapshot::new_from by:
        // 1. creating a snapshot with new API for commits 0..=2 (based on old snapshot at 0)
        // 2. comparing with a snapshot created directly at version 2
        //
        // the commits tested are:
        // - commit 0 -> base snapshot at this version
        // - commit 1 -> final snapshots at this version
        //
        // in each test we will modify versions 1 and 2 to test different scenarios
        fn test_new_from(store: Arc<InMemory>) -> DeltaResult<()> {
            let table_root = "memory:///";
            let engine = DefaultEngineBuilder::new(store).build();
            let base_snapshot = Snapshot::builder_for(table_root)
                .at_version(0)
                .build(&engine)?;
            let snapshot = Snapshot::builder_from(base_snapshot.clone())
                .at_version(1)
                .build(&engine)?;
            let expected = Snapshot::builder_for(table_root)
                .at_version(1)
                .build(&engine)?;
            assert_eq!(snapshot, expected);
            Ok(())
        }

        // for (3) we will just engineer custom log files
        let store = Arc::new(InMemory::new());
        // everything will have a starting 0 commit with commitInfo, protocol, metadata
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 2
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];
        let table_root = "memory:///";
        commit(table_root, store.as_ref(), 0, commit0.clone()).await;
        // 3. new version > existing version
        // a. no new log segment
        let engine = DefaultEngineBuilder::new(Arc::new(store.fork())).build();
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        let snapshot = Snapshot::builder_from(base_snapshot.clone()).build(&engine)?;
        let expected = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
        // version exceeds latest version of the table = err
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(1).build(&engine),
            Err(Error::Generic(msg)) if msg == "Requested snapshot version 1 is newer than the latest version 0"
        ));

        // b. log segment for old..=new version has a checkpoint (with new protocol/metadata)
        let store_3a = store.fork();
        let mut checkpoint1 = commit0.clone();
        commit(table_root, &store_3a, 1, commit0.clone()).await;
        checkpoint1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        checkpoint1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint1
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler
            .parse_json(
                string_array_to_engine_data(json_strings),
                crate::actions::get_commit_schema().clone(),
            )
            .unwrap();
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let checkpoint: RecordBatch = checkpoint.into();

        // Write the record batch to a Parquet file
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        store_3a
            .put(
                &delta_path_for_version(1, "checkpoint.parquet"),
                buffer.into(),
            )
            .await
            .unwrap();
        test_new_from(store_3a.into())?;

        // c. log segment for old..=new version has no checkpoint
        // i. commits have (new protocol, new metadata)
        let store_3c_i = Arc::new(store.fork());
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit(table_root, store_3c_i.as_ref(), 1, commit1).await;
        test_new_from(store_3c_i.clone())?;

        // new commits AND request version > end of log
        let engine = DefaultEngineBuilder::new(store_3c_i).build();
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        assert!(matches!(
            Snapshot::builder_from(base_snapshot.clone()).at_version(2).build(&engine),
            Err(Error::Generic(msg)) if msg == "LogSegment end version 1 not the same as the specified end version 2"
        ));

        // ii. commits have (new protocol, no metadata)
        let store_3c_ii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[1] = json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 5
            }
        });
        commit1.remove(2); // remove metadata
        commit(table_root, &store_3c_ii, 1, commit1).await;
        test_new_from(store_3c_ii.into())?;

        // iii. commits have (no protocol, new metadata)
        let store_3c_iii = store.fork();
        let mut commit1 = commit0.clone();
        commit1[2]["partitionColumns"] = serde_json::to_value(["some_partition_column"])?;
        commit1.remove(1); // remove protocol
        commit(table_root, &store_3c_iii, 1, commit1).await;
        test_new_from(store_3c_iii.into())?;

        // iv. commits have (no protocol, no metadata)
        let store_3c_iv = store.fork();
        let commit1 = vec![commit0[0].clone()];
        commit(table_root, &store_3c_iv, 1, commit1).await;
        test_new_from(store_3c_iv.into())?;

        Ok(())
    }

    // test new CRC in new log segment (old log segment has old CRC)
    #[tokio::test]
    async fn test_snapshot_new_from_crc() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(store.clone()).build();
        let protocol = |reader_version, writer_version| {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": writer_version
                }
            })
        };
        let metadata = json!({
            "metaData": {
                "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }
        });
        let commit0 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 1),
            metadata.clone(),
        ];
        let commit1 = vec![
            json!({
                "commitInfo": {
                    "timestamp": 1587968586154i64,
                    "operation": "WRITE",
                    "operationParameters": {"mode":"ErrorIfExists","partitionBy":"[]"},
                    "isBlindAppend":true
                }
            }),
            protocol(1, 2),
        ];

        // commit 0 and 1 jsons
        commit(table_root, &store, 0, commit0.clone()).await;
        commit(table_root, &store, 1, commit1).await;

        // Test CRC handling during incremental snapshot update (v0 -> v1).
        // The new log listing starts at v1, so the new log segment doesn't find 0.crc.
        // a) Only 0.crc exists: resolve_crc falls back to old segment's 0.crc.
        // b) Both 0.crc and 1.crc exist: resolve_crc picks up 1.crc.
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 1),
        });

        // put the old crc
        let path = delta_path_for_version(0, "crc");
        store.put(&path, crc.to_string().into()).await?;

        // base snapshot is at version 0
        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;

        // a) only 0.crc exists -- falls back to old segment's 0.crc
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        assert_eq!(
            snapshot
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            0
        );

        // b) both 0.crc and 1.crc exist -- resolve_crc picks up 1.crc
        let path = delta_path_for_version(1, "crc");
        let crc = json!({
            "table_size_bytes": 100,
            "num_files": 1,
            "num_metadata": 1,
            "num_protocol": 1,
            "metadata": metadata,
            "protocol": protocol(1, 2),
        });
        store.put(&path, crc.to_string().into()).await?;
        let snapshot = Snapshot::builder_from(base_snapshot.clone())
            .at_version(1)
            .build(&engine)?;
        let expected = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        assert_eq!(snapshot, expected);
        assert_eq!(
            snapshot
                .log_segment
                .listed
                .latest_crc_file
                .as_ref()
                .unwrap()
                .version,
            1
        );

        Ok(())
    }

    #[test]
    fn test_read_table_with_missing_last_checkpoint() {
        // this table doesn't have a _last_checkpoint file
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);
        let cp = LastCheckpointHint::try_read(&storage, &url).unwrap();
        assert!(cp.is_none());
    }

    fn valid_last_checkpoint() -> (Vec<u8>, LastCheckpointHint) {
        let checkpoint = LastCheckpointHint {
            version: 1,
            size: 8,
            parts: None,
            size_in_bytes: Some(21857),
            num_of_add_files: None,
            checkpoint_schema: None,
            checksum: None,
            tags: None,
        };
        let data = checkpoint.to_json_bytes();
        (data, checkpoint)
    }

    fn valid_last_checkpoint_with_tags() -> (Vec<u8>, LastCheckpointHint) {
        use std::collections::HashMap;

        let (_, base_checkpoint) = valid_last_checkpoint();

        let mut tags = HashMap::new();
        tags.insert(
            "author".to_string(),
            "test_read_table_with_last_checkpoint".to_string(),
        );
        tags.insert("environment".to_string(), "snapshot_tests".to_string());
        tags.insert("created_by".to_string(), "delta-kernel-rs".to_string());

        let checkpoint = LastCheckpointHint {
            tags: Some(tags),
            ..base_checkpoint
        };

        let data = checkpoint.to_json_bytes();
        (data, checkpoint)
    }

    #[tokio::test]
    async fn test_read_table_with_empty_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // do a _last_checkpoint file with "{}" as content
        let empty = "{}".as_bytes().to_vec();
        let invalid_path = Path::from("invalid/_last_checkpoint");

        store
            .put(&invalid_path, empty.into())
            .await
            .expect("put _last_checkpoint");

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid = LastCheckpointHint::try_read(&storage, &url).expect("read last checkpoint");
        assert!(invalid.is_none())
    }

    #[tokio::test]
    async fn test_read_table_with_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // Define test cases: (path, data, expected_result)
        let (data, expected) = valid_last_checkpoint();
        let (data_with_tags, expected_with_tags) = valid_last_checkpoint_with_tags();
        let test_cases = vec![
            ("valid", data, Some(expected)),
            ("invalid", "invalid".as_bytes().to_vec(), None),
            ("valid_with_tags", data_with_tags, Some(expected_with_tags)),
        ];

        // Write all test files to the in memory file system
        for (path_prefix, data, _) in &test_cases {
            let path = Path::from(format!("{path_prefix}/_last_checkpoint"));
            store
                .put(&path, data.clone().into())
                .await
                .expect("put _last_checkpoint");
        }

        let executor = Arc::new(TokioBackgroundExecutor::new());
        let storage = ObjectStoreStorageHandler::new(store, executor);

        // Test reading all checkpoints from the in memory file system for cases where the data is
        // valid, invalid and valid with tags.
        for (path_prefix, _, expected_result) in test_cases {
            let url = Url::parse(&format!("memory:///{path_prefix}/")).expect("valid url");
            let result =
                LastCheckpointHint::try_read(&storage, &url).expect("read last checkpoint");
            assert_eq!(result, expected_result);
        }
    }

    #[test_log::test]
    fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(location).build(&engine).unwrap();

        assert_eq!(snapshot.log_segment.listed.checkpoint_parts.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(
                snapshot.log_segment.listed.checkpoint_parts[0]
                    .location
                    .clone()
            )
            .unwrap()
            .unwrap()
            .version,
            2,
        );
        assert_eq!(snapshot.log_segment.listed.ascending_commit_files.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(
                snapshot.log_segment.listed.ascending_commit_files[0]
                    .location
                    .clone()
            )
            .unwrap()
            .unwrap()
            .version,
            3,
        );
    }

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // commit0
        // - domain1: not removed
        // - domain2: not removed
        let commit = [
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain3",
                    "configuration": "domain3_commit0",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(table_root, store.clone().as_ref(), 0, commit)
            .await
            .unwrap();

        // commit1
        // - domain1: removed
        // - domain2: not-removed
        // - internal domain
        let commit = [
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit1",
                    "removed": true
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit1",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "delta.domain3",
                    "configuration": "domain3_commit1",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(table_root, store.as_ref(), 1, commit)
            .await
            .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

        // Test get_domain_metadata

        assert_eq!(snapshot.get_domain_metadata("domain1", &engine)?, None);
        assert_eq!(
            snapshot.get_domain_metadata("domain2", &engine)?,
            Some("domain2_commit1".to_string())
        );
        assert_eq!(
            snapshot.get_domain_metadata("domain3", &engine)?,
            Some("domain3_commit0".to_string())
        );
        let err = snapshot
            .get_domain_metadata("delta.domain3", &engine)
            .unwrap_err();
        assert!(matches!(err, Error::Generic(msg) if
                msg == "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"));

        // Test get_domain_metadata_internal
        assert_eq!(
            snapshot.get_domain_metadata_internal("delta.domain3", &engine)?,
            Some("domain3_commit1".to_string())
        );

        // Test get_all_domain_metadata
        let mut metadata = snapshot.get_all_domain_metadata(&engine)?;
        metadata.sort_by(|a, b| a.domain().cmp(b.domain()));

        let mut expected = vec![
            DomainMetadata::new("domain2".to_string(), "domain2_commit1".to_string()),
            DomainMetadata::new("domain3".to_string(), "domain3_commit0".to_string()),
        ];
        expected.sort_by(|a, b| a.domain().cmp(b.domain()));

        assert_eq!(metadata, expected);

        Ok(())
    }

    #[test]
    #[ignore = "log compaction disabled (#2337)"]
    fn test_log_compaction_writer() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        // Test creating a log compaction writer
        let writer = snapshot.clone().log_compaction_writer(0, 1).unwrap();
        let path = writer.compaction_path();

        // Verify the path format is correct
        let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
        assert!(path.to_string().ends_with(expected_filename));

        // Test invalid version range (start >= end)
        let result = snapshot.clone().log_compaction_writer(2, 1);
        assert_result_error_with_message(result, "Invalid version range");

        // Test equal version range (also invalid)
        let result = snapshot.log_compaction_writer(1, 1);
        assert_result_error_with_message(result, "Invalid version range");
    }

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[test]
    fn test_log_compaction_writer_unsupported() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let result = snapshot.log_compaction_writer(0, 1);
        assert_result_error_with_message(result, "not currently supported");
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_disabled() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory://test/";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create a basic commit without ICT enabled
        let commit0 = create_basic_commit(false, None);
        add_commit(table_root, store.as_ref(), 0, commit0).await?;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

        // When ICT is disabled, get_timestamp should return None
        let result = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_enablement_timeline() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = Arc::new(InMemory::new());
        let table_root = "memory://test/";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create initial commit without ICT
        let commit0 = create_basic_commit(false, None);
        add_commit(table_root, store.as_ref(), 0, commit0).await?;

        // Create commit that enables ICT (version 1 = enablement version)
        let commit1 =
            create_basic_commit(true, Some(("1".to_string(), "1587968586154".to_string())));
        add_commit(table_root, store.as_ref(), 1, commit1).await?;

        // Create commit with ICT enabled
        let expected_timestamp = 1587968586200i64;
        let commit2 = format!(
            r#"{{"commitInfo":{{"timestamp":1587968586154,"inCommitTimestamp":{expected_timestamp},"operation":"WRITE"}}}}"#,
        );
        add_commit(table_root, store.as_ref(), 2, commit2.to_string()).await?;

        // Read snapshot at version 0 (before ICT enablement)
        let snapshot_v0 = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        // This snapshot version predates ICT enablement, so ICT is not available
        let result_v0 = snapshot_v0.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v0, None);

        // Read snapshot at version 2 (after ICT enabled)
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        // When ICT is enabled and available, timestamp() should return inCommitTimestamp
        let result_v2 = snapshot_v2.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v2, Some(expected_timestamp));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_enablement_version_in_future() -> DeltaResult<()> {
        // Test invalid state where snapshot has enablement version in the future - should error
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        let commit_data = [
            json!({
                "protocol": {
                    "minReaderVersion": TABLE_FEATURES_MIN_READER_VERSION,
                    "minWriterVersion": TABLE_FEATURES_MIN_WRITER_VERSION,
                    "readerFeatures": [],
                    "writerFeatures": ["inCommitTimestamp"]
                }
            }),
            json!({
                "metaData": {
                    "id": "test_id2",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {
                        "delta.enableInCommitTimestamps": "true",
                        "delta.inCommitTimestampEnablementVersion": "5", // Enablement after version 1
                        "delta.inCommitTimestampEnablementTimestamp": "1612345678"
                    },
                    "createdTime": 1677811175819u64
                }
            }),
        ];
        commit(table_root, store.as_ref(), 0, commit_data.to_vec()).await;

        // Create commit that predates ICT enablement (no inCommitTimestamp)
        let commit_predates = [create_commit_info(1234567890, None)];
        commit(table_root, store.as_ref(), 1, commit_predates.to_vec()).await;

        let snapshot_predates = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        let result_predates = snapshot_predates.get_in_commit_timestamp(&engine);

        // Version 1 with enablement at version 5 is invalid - should error
        assert_result_error_with_message(
            result_predates,
            "Invalid state: snapshot at version 1 has ICT enablement version 5 in the future",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_missing_ict_when_enabled() -> DeltaResult<()> {
        // Test missing ICT when it should be present - should error
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        let commit_data = [
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())),
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit_data.to_vec()).await; // ICT enabled from version 0

        // Create commit without ICT despite being enabled (corrupt case)
        let commit_missing_ict = [create_commit_info(1234567890, None)];
        commit(table_root, store.as_ref(), 1, commit_missing_ict.to_vec()).await;

        let snapshot_missing = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        let result = snapshot_missing.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "In-Commit Timestamp not found");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_fails_when_commit_missing() -> DeltaResult<()> {
        // When ICT is enabled but commit file is not found in log segment,
        // get_in_commit_timestamp should return an error

        let url = Url::parse("memory:///")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create initial commit with ICT enabled
        let commit_data = [
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())), // ICT enabled from version 0
                false,
            ),
        ];
        commit(url.as_str(), store.as_ref(), 0, commit_data.to_vec()).await;

        // Build snapshot to get table configuration
        let snapshot = Snapshot::builder_for(url.as_str())
            .at_version(0)
            .build(&engine)?;

        let snapshot_no_commit = create_snapshot_with_commit_file_absent_from_log_segment(
            &url,
            snapshot.table_configuration().clone(),
        )?;

        // Should return an error when commit file is missing
        let result = snapshot_no_commit.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "Last commit file not found in log segment");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_with_checkpoint_and_commit_same_version() -> DeltaResult<()> {
        // Test the scenario where both checkpoint and commit exist at the same version with ICT
        // enabled.
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create 00000000000000000000.json with ICT enabled
        let commit0_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit0_data.to_vec()).await;

        // Create 00000000000000000001.checkpoint.parquet
        let checkpoint_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint_data
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler.parse_json(
            string_array_to_engine_data(json_strings),
            crate::actions::get_commit_schema().clone(),
        )?;
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed)?;
        let checkpoint: RecordBatch = checkpoint.into();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        let checkpoint_path = delta_path_for_version(1, "checkpoint.parquet");
        store.put(&checkpoint_path, buffer.into()).await?;

        // Create 00000000000000000001.json with ICT
        let expected_ict = 1587968586200i64;
        let commit1_data = [create_commit_info(1587968586200, Some(expected_ict))];
        commit(table_root, store.as_ref(), 1, commit1_data.to_vec()).await;

        // Build snapshot - LogSegment will filter out the commit file because checkpoint exists at
        // same version
        let snapshot = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;

        // We should successfully read ICT by falling back to storage
        let timestamp = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(timestamp, Some(expected_ict));

        Ok(())
    }

    #[rstest]
    #[case::ict_disabled(false)]
    #[case::ict_enabled(true)]
    fn test_get_timestamp_returns_valid_timestamp(#[case] ict_enabled: bool) -> DeltaResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = Url::from_directory_path(temp_dir.path())
            .unwrap()
            .to_string();
        let store = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngineBuilder::new(store).build();

        let schema = Arc::new(StructType::try_new(vec![StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )])?);

        let mut create_table_builder = create_table(&table_path, schema, "Test/1.0");
        if ict_enabled {
            create_table_builder = create_table_builder
                .with_table_properties(vec![(ENABLE_IN_COMMIT_TIMESTAMPS, "true")]);
        }

        let _ = create_table_builder
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;

        let snapshot = Snapshot::builder_for(&table_path).build(&engine)?;
        let ts = snapshot.get_timestamp(&engine)?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let two_days_ms = 2 * 24 * 60 * 60 * 1000_i64;
        assert!(
            (now_ms - two_days_ms..=now_ms).contains(&ts),
            "timestamp {ts} not within 2 days of now ({now_ms})"
        );

        if ict_enabled {
            let ict_ts = snapshot.get_in_commit_timestamp(&engine)?.unwrap();
            assert_eq!(ts, ict_ts);
        }
        Ok(())
    }

    #[rstest]
    #[case::ict_enabled(true)]
    #[case::ict_disabled(false)]
    #[tokio::test]
    async fn test_get_timestamp_errors_when_commit_file_missing(
        #[case] ict_enabled: bool,
    ) -> DeltaResult<()> {
        let url = Url::parse("memory:///")?;
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // TODO: refactor `ict_config` from a raw tuple to a dedicated ICTConfig struct so the
        // enablement version and enablement timestamp fields are named and self-documenting.
        // The ict_config tuple is (inCommitTimestampEnablementVersion,
        // inCommitTimestampEnablementTimestamp): if ICT is enabled, the enablement version
        // is 0 with an arbitrary enablement timestamp.
        let ict_config = ict_enabled.then(|| ("0".to_string(), "1612345678".to_string()));
        let reader_version = ict_enabled.then_some(TABLE_FEATURES_MIN_READER_VERSION as u32);

        let mut commit_data = vec![];
        // When ICT is enabled, commitInfo must be the first action (protocol requirement)
        if ict_enabled {
            commit_data.push(create_commit_info(1677811175819, Some(1677811175999)));
        }
        commit_data.extend([
            create_protocol(ict_enabled, reader_version),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                ict_config,
                false,
            ),
        ]);
        commit(url.as_str(), store.as_ref(), 0, commit_data).await;

        let snapshot = Snapshot::builder_for(url.as_str())
            .at_version(0)
            .build(&engine)?;

        let snapshot_no_commit = create_snapshot_with_commit_file_absent_from_log_segment(
            &url,
            snapshot.table_configuration().clone(),
        )?;

        let result = snapshot_no_commit.get_timestamp(&engine);
        assert_result_error_with_message(result, "Last commit file not found in log segment");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_errors_when_ict_missing_from_commit_info() -> DeltaResult<()> {
        // ICT is enabled and commit file IS present in the log segment, but the commitInfo
        // action does not carry an inCommitTimestamp value (corrupt/incomplete commit).
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        let commit0_data = vec![
            create_commit_info(1677811175819, None), // commitInfo without inCommitTimestamp
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())), /* ict enabled at version 0, and an arbitrary timestamp */
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit0_data).await;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        let result = snapshot.get_timestamp(&engine);
        assert_result_error_with_message(result, "In-Commit Timestamp not found in commit file");

        Ok(())
    }

    // Verifies the test_context! macro works from kernel/src/ unit tests
    // (crosses the crate type boundary via macro expansion).
    #[test]
    fn test_context_macro_works_in_unit_test() {
        let (_engine, snap, _table) = test_utils::test_context!(
            LogState::with_commits(3),
            FeatureSet::empty(),
            VersionTarget::Latest
        );
        assert_eq!(snap.version(), 2);
    }

    #[test]
    fn test_try_new_from_empty_log_tail() -> DeltaResult<()> {
        let table = TestTableBuilder::new().build().unwrap();
        let engine = DefaultEngineBuilder::new(table.store().clone()).build();

        let base_snapshot = Snapshot::builder_for(table.table_root())
            .at_version(0)
            .build(&engine)?;

        let result = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            None,
            MetricId::default(),
        )?;
        assert_eq!(result, base_snapshot);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_latest_commit_preservation() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits 0-2
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(url.as_str(), store.as_ref(), 0, base_commit.clone()).await;
        commit(
            url.as_str(),
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;
        commit(
            url.as_str(),
            store.as_ref(),
            2,
            vec![json!({"commitInfo": {"timestamp": 5678}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(url.as_str())
            .at_version(1)
            .build(&engine)?;

        // Verify base snapshot has latest_commit_file at version 1
        assert_eq!(
            base_snapshot
                .log_segment
                .listed
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(1)
        );

        // Create log_tail with FileMeta for version 2
        let commit_2_url = url.join("_delta_log/")?.join("00000000000000000002.json")?;
        let file_meta = crate::FileMeta {
            location: commit_2_url,
            last_modified: 1234567890,
            size: 100,
        };
        let parsed_path = ParsedLogPath::try_from(file_meta)?
            .ok_or_else(|| Error::Generic("Failed to parse log path".to_string()))?;
        let log_tail = vec![parsed_path];

        // Create new snapshot from base to version 2 using try_new_from directly
        let new_snapshot = Snapshot::try_new_from(
            base_snapshot.clone(),
            log_tail,
            &engine,
            Some(2),
            MetricId::default(),
        )?;

        // Latest commit should now be version 2
        assert_eq!(
            new_snapshot
                .log_segment
                .listed
                .latest_commit_file
                .as_ref()
                .map(|f| f.version),
            Some(2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_try_new_from_version_boundary_cases() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits
        let base_commit = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
        ];

        commit(table_root, store.as_ref(), 0, base_commit).await;
        commit(
            table_root,
            store.as_ref(),
            1,
            vec![json!({"commitInfo": {"timestamp": 1234}})],
        )
        .await;

        let base_snapshot = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;

        // Test requesting same version - should return same snapshot
        let same_version = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            Some(1),
            MetricId::default(),
        )?;
        assert!(Arc::ptr_eq(&same_version, &base_snapshot));

        // Test requesting older version - should error
        let older_version = Snapshot::try_new_from(
            base_snapshot.clone(),
            vec![],
            &engine,
            Some(0),
            MetricId::default(),
        );
        assert!(matches!(
            older_version,
            Err(Error::Generic(msg)) if msg.contains("older than snapshot hint version")
        ));

        Ok(())
    }

    #[test]
    fn test_new_post_commit_simple() {
        // GIVEN
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let base_snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
        let next_version = base_snapshot.version() + 1;

        // WHEN
        let fake_new_commit = ParsedLogPath::create_parsed_published_commit(&url, next_version);
        let post_commit_snapshot = base_snapshot
            .new_post_commit(fake_new_commit, CrcUpdate::default())
            .unwrap();

        // THEN
        assert_eq!(post_commit_snapshot.version(), next_version);
        assert_eq!(post_commit_snapshot.log_segment().end_version, next_version);
    }

    // Helper: create a minimal test table with commits 0-N
    async fn setup_test_table_with_commits(
        table_root: impl AsRef<str>,
        store: &InMemory,
        num_commits: u64,
    ) -> DeltaResult<()> {
        // Commit 0: protocol + metadata + first file
        let commit0 = vec![
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({
                "metaData": {
                    "id": "test-id",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({"add": {"path": "file1.parquet", "partitionValues": {}, "size": 100, "modificationTime": 1000, "dataChange": true}}),
        ];
        commit(table_root.as_ref(), store, 0, commit0).await;

        // Additional commits with just add actions
        for i in 1..num_commits {
            let commit_i = vec![json!({
                "add": {
                    "path": format!("file{}.parquet", i + 1),
                    "partitionValues": {},
                    "size": (i + 1) * 100,
                    "modificationTime": (i + 1) * 1000,
                    "dataChange": true
                }
            })];
            commit(table_root.as_ref(), store, i, commit_i).await;
        }
        Ok(())
    }

    // Helper: write a compaction file
    async fn write_compaction_file(store: &InMemory, start: u64, end: u64) -> DeltaResult<()> {
        let content = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
        store
            .put(
                &test_utils::compacted_log_path_for_versions(start, end, "json"),
                content.into(),
            )
            .await?;
        Ok(())
    }

    struct IncrementalSnapshotTestContext {
        store: Arc<InMemory>,
        url: Url,
        engine: Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    }

    fn setup_incremental_snapshot_test() -> DeltaResult<IncrementalSnapshotTestContext> {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///")?;
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let engine = Arc::new(
            DefaultEngineBuilder::new(store.clone())
                .with_task_executor(executor)
                .build(),
        );

        Ok(IncrementalSnapshotTestContext { store, url, engine })
    }

    /// Compares two Snapshots field-by-field. LogSegment fields are compared individually,
    /// intentionally skipping `last_checkpoint_metadata` which is only populated on the
    /// from-scratch path (via `_last_checkpoint` hint) and not on the incremental update path.
    fn compare_snapshots(left: &Snapshot, right: &Snapshot) {
        assert_eq!(left.table_configuration, right.table_configuration);
        assert_eq!(left.log_segment.end_version, right.log_segment.end_version);
        assert_eq!(
            left.log_segment.checkpoint_version,
            right.log_segment.checkpoint_version
        );
        assert_eq!(left.log_segment.log_root, right.log_segment.log_root);
        assert_eq!(left.log_segment.listed, right.log_segment.listed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_picks_up_checkpoint_written_at_current_version(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 2).await?;

        let snapshot_v1 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v1.log_segment.checkpoint_version, None);

        snapshot_v1.clone().checkpoint(ctx.engine.as_ref())?;

        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(fresh.version(), 1);
        assert_eq!(fresh.log_segment.checkpoint_version, Some(1));

        let updated = Snapshot::builder_from(snapshot_v1).build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 1);
        assert_eq!(updated.log_segment.checkpoint_version, Some(1));
        compare_snapshots(&updated, &fresh);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incremental_snapshot_picks_up_newer_checkpoint_below_current_version(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 4).await?;

        Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref())?;

        let snapshot_v3 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(3)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v3.log_segment.checkpoint_version, Some(1));

        Snapshot::builder_for(ctx.url.as_str())
            .at_version(2)
            .build(ctx.engine.as_ref())?
            .checkpoint(ctx.engine.as_ref())?;

        let fresh = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(fresh.version(), 3);
        assert_eq!(fresh.log_segment.checkpoint_version, Some(2));

        let updated = Snapshot::builder_from(snapshot_v3).build(ctx.engine.as_ref())?;
        assert_eq!(updated.version(), 3);
        assert_eq!(updated.log_segment.checkpoint_version, Some(2));
        compare_snapshots(&updated, &fresh);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_explicit_same_version_request_keeps_existing_snapshot_after_checkpoint_write(
    ) -> DeltaResult<()> {
        let ctx = setup_incremental_snapshot_test()?;

        setup_test_table_with_commits(ctx.url.as_str(), &ctx.store, 2).await?;

        let snapshot_v1 = Snapshot::builder_for(ctx.url.as_str())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert_eq!(snapshot_v1.log_segment.checkpoint_version, None);

        snapshot_v1.clone().checkpoint(ctx.engine.as_ref())?;

        let refreshed = Snapshot::builder_for(ctx.url.as_str()).build(ctx.engine.as_ref())?;
        assert_eq!(refreshed.log_segment.checkpoint_version, Some(1));

        let pinned = Snapshot::builder_from(snapshot_v1.clone())
            .at_version(1)
            .build(ctx.engine.as_ref())?;
        assert!(Arc::ptr_eq(&pinned, &snapshot_v1));
        assert_eq!(pinned.log_segment.checkpoint_version, None);

        Ok(())
    }

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[tokio::test]
    async fn test_compaction_files_ignored_on_read() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits 0-2 and write compaction files to storage
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 0, 1).await?;
        write_compaction_file(&store, 0, 2).await?;

        // Compaction files exist on disk but should be skipped during listing
        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        assert!(
            snapshot
                .log_segment
                .listed
                .ascending_compaction_files
                .is_empty(),
            "Compaction files should be ignored when log compaction is disabled"
        );

        Ok(())
    }

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[tokio::test]
    async fn test_incremental_snapshot_ignores_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits 0-2 with compaction files in storage
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 0, 1).await?;
        write_compaction_file(&store, 0, 2).await?;

        // Build base snapshot at v2
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(snapshot_v2.version(), 2);
        assert!(snapshot_v2
            .log_segment
            .listed
            .ascending_compaction_files
            .is_empty());

        // Add commit 3
        commit(
            table_root,
            &store,
            3,
            vec![json!({"add": {"path": "file4.parquet", "partitionValues": {}, "size": 400, "modificationTime": 4000, "dataChange": true}})],
        )
        .await;

        // Build v3 incrementally -- compaction files should still be skipped
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;
        assert_eq!(snapshot_v3.version(), 3);
        assert!(snapshot_v3
            .log_segment
            .listed
            .ascending_compaction_files
            .is_empty());

        Ok(())
    }

    /// The incremental snapshot path (try_new_from_impl) re-lists files from the checkpoint
    /// version onwards. We must ensure that it deduplicates compaction files, since producing
    /// duplicates violated the sort invariant in LogSegmentFilesBuilder::build().
    #[tokio::test]
    #[ignore = "log compaction disabled (#2337)"]
    async fn test_incremental_snapshot_with_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits 0-3 and compaction files (1,1) and (1,2)
        setup_test_table_with_commits(table_root, &store, 3).await?;
        write_compaction_file(&store, 1, 1).await?;
        write_compaction_file(&store, 1, 2).await?;

        // Build snapshot at v2 (includes both compaction files)
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(
            snapshot_v2
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Add commit 3
        commit(
            table_root,
            &store,
            3,
            vec![json!({"add": {"path": "file4.parquet", "partitionValues": {}, "size": 400, "modificationTime": 4000, "dataChange": true}})],
        )
        .await;

        // Build v3 incrementally - before the fix, this panicked due to duplicate compaction files
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;

        assert_eq!(snapshot_v3.version(), 3);
        assert_eq!(
            snapshot_v3
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        Ok(())
    }

    /// This test documents a limitation: When deduplicating compactions, the deduplication logic
    /// only checks the start version (lo), not the hi version. So a new compaction file (1,3)
    /// added after building the base snapshot at v2 gets filtered out because its start version
    /// (1) <= old_version (2).
    #[tokio::test]
    #[ignore = "log compaction disabled (#2337)"]
    async fn test_incremental_snapshot_with_new_compaction_files() -> DeltaResult<()> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        // Create commits 0-3 and compaction files (1,2) and (2,2)
        setup_test_table_with_commits(table_root, &store, 4).await?;
        write_compaction_file(&store, 1, 2).await?;
        write_compaction_file(&store, 2, 2).await?;

        // Build snapshot at v2
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        assert_eq!(
            snapshot_v2
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Add new compaction file (1,3) after building the base snapshot
        write_compaction_file(&store, 1, 3).await?;

        // Build v3 incrementally - the new (1,3) file gets filtered out because
        // the deduplication only looks at start version: 1 <= old_version (2)
        let snapshot_v3 = Snapshot::builder_from(snapshot_v2)
            .at_version(3)
            .build(&engine)?;

        assert_eq!(snapshot_v3.version(), 3);
        assert_eq!(
            snapshot_v3
                .log_segment
                .listed
                .ascending_compaction_files
                .len(),
            2
        );

        // Verify we still have the original (1,2) and (2,2) files
        let versions_and_his: Vec<_> = snapshot_v3
            .log_segment
            .listed
            .ascending_compaction_files
            .iter()
            .map(|p| match p.file_type {
                LogPathFileType::CompactedCommit { hi } => (p.version, hi),
                _ => panic!("Expected CompactedCommit"),
            })
            .collect();
        assert_eq!(versions_and_his, vec![(1, 2), (2, 2)]);

        Ok(())
    }

    #[test]
    fn test_get_protocol_derived_properties() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let props = snapshot.get_protocol_derived_properties();
        assert_eq!(
            props.get("delta.minReaderVersion").unwrap(),
            &TABLE_FEATURES_MIN_READER_VERSION.to_string()
        );
        assert_eq!(
            props.get("delta.minWriterVersion").unwrap(),
            &TABLE_FEATURES_MIN_WRITER_VERSION.to_string()
        );
        assert_eq!(
            props.get("delta.feature.deletionVectors").unwrap(),
            "supported"
        );
    }

    #[tokio::test]
    async fn test_metadata_configuration() {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = DefaultEngineBuilder::new(storage.clone()).build();

        // Create a commit with custom configuration
        let actions = vec![
            json!({"commitInfo": {"timestamp": 123, "operation": "CREATE TABLE"}}),
            json!({"protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": []
            }}),
            json!({"metaData": {
                "id": "test-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {
                    "io.unitycatalog.tableId": "abc-123",
                    "myapp.setting": "value"
                },
                "createdTime": 1234567890
            }}),
        ];
        commit(table_root, &storage, 0, actions).await;

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        let config = snapshot.metadata_configuration();
        assert_eq!(
            config.get("io.unitycatalog.tableId"),
            Some(&"abc-123".to_string())
        );
        assert_eq!(config.get("myapp.setting"), Some(&"value".to_string()));
    }

    #[rstest::rstest]
    #[case::no_clustering(None, None, None)]
    #[case::clustered_no_column_mapping(
        Some(vec!["region"]),
        None,
        Some(vec![ColumnName::new(["region"])])
    )]
    #[case::clustered_with_column_mapping(
        Some(vec!["region"]),
        Some("name"),
        Some(vec![ColumnName::new(["region"])])
    )]
    fn test_get_logical_clustering_columns(
        #[case] clustering_cols: Option<Vec<&str>>,
        #[case] column_mapping_mode: Option<&str>,
        #[case] expected: Option<Vec<ColumnName>>,
    ) {
        use crate::transaction::create_table::create_table;
        use crate::transaction::data_layout::DataLayout;

        let storage = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(storage).build();
        let schema = Arc::new(
            crate::schema::StructType::try_new(vec![
                crate::schema::StructField::new("id", crate::schema::DataType::INTEGER, true),
                crate::schema::StructField::new("region", crate::schema::DataType::STRING, true),
            ])
            .unwrap(),
        );
        let mut builder = create_table("memory:///", schema, "test");
        if let Some(cols) = &clustering_cols {
            builder = builder.with_data_layout(DataLayout::clustered(cols.clone()));
        }
        if let Some(mode) = column_mapping_mode {
            builder = builder.with_table_properties([("delta.columnMapping.mode", mode)]);
        }
        let _ = builder
            .build(
                &engine,
                Box::new(crate::committer::FileSystemCommitter::new()),
            )
            .unwrap()
            .commit(&engine)
            .unwrap();
        let snapshot = Snapshot::builder_for("memory:///").build(&engine).unwrap();
        let result = snapshot.get_logical_clustering_columns(&engine).unwrap();
        assert_eq!(result, expected);
    }
}
