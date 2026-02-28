//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`]. It also piggybacks on the P&M scan to
//! cache file stats, domain metadata, and operation info from commit files ([`CachedLogStats`]).

use std::collections::hash_map::Entry;
use std::sync::{Arc, LazyLock};

use crate::actions::{
    DomainMetadata, Metadata, Protocol, ADD_NAME, COMMIT_INFO_NAME, DOMAIN_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::crc::{CrcLoadResult, LazyCrc};
use crate::engine_data::{GetData, TypedGetData};
use crate::log_replay::ActionsBatch;
use crate::schema::{
    ColumnName, ColumnNamesAndTypes, DataType, SchemaRef, StructField, StructType, ToSchema as _,
};
use crate::{DeltaResult, Engine, Error, Expression, Predicate, PredicateRef, RowVisitor};

use tracing::{info, instrument, warn};

use super::DomainMetadataMap;
use super::LogSegment;

/// Operations for which incremental counting (+adds -removes) is known to be safe.
/// For these operations, every add is genuinely new and every remove cancels exactly one
/// prior add (no file re-additions without an intervening remove).
const WHITELISTED_OPERATIONS: &[&str] = &[
    "WRITE",
    "MERGE",
    "UPDATE",
    "DELETE",
    "OPTIMIZE",
    "CREATE TABLE",
    "REPLACE TABLE",
    "CREATE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE AS SELECT",
];

fn is_whitelisted_operation(op: &str) -> bool {
    WHITELISTED_OPERATIONS
        .iter()
        .any(|&w| w.eq_ignore_ascii_case(op))
}

/// Stats accumulated from commit files during P&M log replay.
/// Piggybacks on the existing P&M scan -- zero additional I/O.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)] // Fields used by CRC writer (future PR)
pub(crate) struct CachedLogStats {
    /// Number of add file actions across scanned commit batches.
    pub add_count: i64,
    /// Total size in bytes of all add files.
    pub add_size_bytes: i64,
    /// Number of remove file actions across scanned commit batches.
    pub remove_count: i64,
    /// Total size in bytes of all remove files.
    pub remove_size_bytes: i64,
    /// Domain metadata map: domain name -> latest DomainMetadata.
    /// Built using first-seen-wins per domain (reverse replay order).
    pub domain_metadata: DomainMetadataMap,
    /// Whether ALL commit batches in the scan range were consumed
    /// (the iterator was exhausted). If true, stats are complete for
    /// the scanned commit range. If false, the P&M scan broke early.
    pub complete: bool,
    /// Whether all encountered commitInfo.operation values were in the
    /// whitelist. If false, numFiles/tableSizeBytes are unreliable --
    /// incremental counting (+adds -removes) may be incorrect.
    pub stats_reliable: bool,
}

impl CachedLogStats {
    /// Create an empty cache that is already complete (nothing to scan).
    fn empty_complete() -> Self {
        Self {
            complete: true,
            stats_reliable: true,
            ..Default::default()
        }
    }
}

/// Visitor that accumulates file stats, domain metadata, and operation info
/// from commit file batches during P&M log replay.
#[derive(Debug, Default)]
struct StatsAccumulatorVisitor {
    add_count: i64,
    add_size_bytes: i64,
    remove_count: i64,
    remove_size_bytes: i64,
    domain_metadata: DomainMetadataMap,
    stats_reliable: bool,
}

impl StatsAccumulatorVisitor {
    fn new() -> Self {
        Self {
            stats_reliable: true,
            ..Default::default()
        }
    }

    /// Convert visitor state into a [`CachedLogStats`] with the given completeness flag.
    fn into_cached_log_stats(self, complete: bool) -> CachedLogStats {
        CachedLogStats {
            add_count: self.add_count,
            add_size_bytes: self.add_size_bytes,
            remove_count: self.remove_count,
            remove_size_bytes: self.remove_size_bytes,
            domain_metadata: self.domain_metadata,
            complete,
            stats_reliable: self.stats_reliable,
        }
    }

    /// Merge another visitor's state into this one. For domain metadata,
    /// existing entries take priority (first-seen-wins in reverse replay order).
    fn merge(&mut self, other: StatsAccumulatorVisitor) {
        self.add_count += other.add_count;
        self.add_size_bytes += other.add_size_bytes;
        self.remove_count += other.remove_count;
        self.remove_size_bytes += other.remove_size_bytes;
        if !other.stats_reliable {
            self.stats_reliable = false;
        }
        // Existing entries take priority (first-seen-wins from newer commits)
        for (domain, dm) in other.domain_metadata {
            self.domain_metadata.entry(domain).or_insert(dm);
        }
    }
}

impl RowVisitor for StatsAccumulatorVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    ColumnName::new(["add", "size"]),
                    ColumnName::new(["remove", "size"]),
                    ColumnName::new(["domainMetadata", "domain"]),
                    ColumnName::new(["domainMetadata", "configuration"]),
                    ColumnName::new(["domainMetadata", "removed"]),
                    ColumnName::new(["commitInfo", "operation"]),
                ],
                vec![
                    DataType::LONG,    // add.size
                    DataType::LONG,    // remove.size
                    DataType::STRING,  // domainMetadata.domain
                    DataType::STRING,  // domainMetadata.configuration
                    DataType::BOOLEAN, // domainMetadata.removed
                    DataType::STRING,  // commitInfo.operation
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Add file: if add.size is not null, count it
            let add_size: Option<i64> = getters[0].get_opt(i, "add.size")?;
            if let Some(size) = add_size {
                self.add_count += 1;
                self.add_size_bytes += size;
            }
            // Remove file: if remove.size is not null, count it
            let remove_size: Option<i64> = getters[1].get_opt(i, "remove.size")?;
            if let Some(size) = remove_size {
                self.remove_count += 1;
                self.remove_size_bytes += size;
            }
            // Domain metadata: first-seen-wins (reverse replay = newest first)
            let domain: Option<String> = getters[2].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = domain {
                if let Entry::Vacant(entry) = self.domain_metadata.entry(domain.clone()) {
                    let configuration: String = getters[3]
                        .get_opt(i, "domainMetadata.configuration")?
                        .unwrap_or_default();
                    let removed: bool = getters[4]
                        .get_opt(i, "domainMetadata.removed")?
                        .unwrap_or(false);
                    let dm = if removed {
                        DomainMetadata::remove(domain, configuration)
                    } else {
                        DomainMetadata::new(domain, configuration)
                    };
                    entry.insert(dm);
                }
            }
            // CommitInfo operation: check whitelist
            let operation: Option<String> = getters[5].get_opt(i, "commitInfo.operation")?;
            if let Some(op) = operation {
                if !is_whitelisted_operation(&op) {
                    self.stats_reliable = false;
                }
            }
        }
        Ok(())
    }
}

/// Narrow P&M-only schema used for checkpoint files (column pruning matters for parquet).
static CHECKPOINT_PM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
    ]))
});

/// Widened commit schema: P&M plus file stats, domain metadata, and commitInfo.operation.
/// Used for commit files (JSON) where column pruning doesn't matter.
static COMMIT_PM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        // Add: only need size for counting
        StructField::nullable(
            ADD_NAME,
            StructType::new_unchecked([StructField::nullable("size", DataType::LONG)]),
        ),
        // Remove: only need size for counting
        StructField::nullable(
            REMOVE_NAME,
            StructType::new_unchecked([StructField::nullable("size", DataType::LONG)]),
        ),
        // DomainMetadata: all fields
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
        // CommitInfo: only need operation
        StructField::nullable(
            COMMIT_INFO_NAME,
            StructType::new_unchecked([StructField::nullable("operation", DataType::STRING)]),
        ),
    ]))
});

/// Predicate to filter checkpoint row groups that don't contain P&M information.
static META_PREDICATE: LazyLock<Option<PredicateRef>> = LazyLock::new(|| {
    Some(Arc::new(Predicate::or(
        Expression::column([METADATA_NAME, "id"]).is_not_null(),
        Expression::column([PROTOCOL_NAME, "minReaderVersion"]).is_not_null(),
    )))
});

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing. Also returns [`CachedLogStats`] accumulated
    /// from commit files during the P&M scan.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_opt`], used for
    /// fresh snapshot creation where both Protocol and Metadata must exist.
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Metadata, Protocol, CachedLogStats)> {
        match self.read_protocol_metadata_opt(engine, lazy_crc)? {
            (Some(m), Some(p), stats) => Ok((m, p, stats)),
            (None, Some(_), _) => Err(Error::MissingMetadata),
            (Some(_), None, _) => Err(Error::MissingProtocol),
            (None, None, _) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns `None` for either if not found. Also returns [`CachedLogStats`] accumulated
    /// from commit files during the P&M scan.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata`], used for
    /// incremental snapshot updates where the caller can fall back to an existing snapshot's
    /// Protocol and Metadata.
    ///
    /// The `lazy_crc` parameter allows the CRC to be loaded at most once and shared for
    /// future use (domain metadata, in-commit timestamp, etc.).
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        lazy_crc: &LazyCrc,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>, CachedLogStats)> {
        let crc_version = lazy_crc.crc_version();

        // Case 1: If CRC at target version, use it directly and exit early.
        // No commits to scan, so the cache is empty and complete.
        if crc_version == Some(self.end_version) {
            if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                info!("P&M from CRC at target version {}", self.end_version);
                let stats = CachedLogStats::empty_complete();
                return Ok((
                    Some(crc.metadata.clone()),
                    Some(crc.protocol.clone()),
                    stats,
                ));
            }
            warn!(
                "CRC at target version {} failed to load, falling back to log replay",
                self.end_version
            );
        }

        // We didn't return above, so we need to do log replay to find P&M.
        //
        // Case 2: CRC exists at an earlier version => Prune the log segment to only replay
        //         commits *after* the CRC version.
        //   (a) If we find new P&M in the pruned replay, return it.
        //   (b) If we don't find new P&M, fall back to the CRC.
        //   (c) If the CRC also fails, fall back to replaying the remaining segment
        //       (checkpoint + commits up through the CRC version).
        //
        // Case 3: CRC at target version failed to load => Full P&M log replay.
        //
        // Case 4: No CRC exists at all => Full P&M log replay.

        if let Some(crc_v) = crc_version.filter(|&v| v < self.end_version) {
            // Case 2(a): Replay only commits after CRC version
            info!("Pruning log segment to commits after CRC version {}", crc_v);
            let pruned = self.segment_after_crc(crc_v);
            let mut visitor = StatsAccumulatorVisitor::new();
            let (metadata_opt, protocol_opt, exhausted) =
                pruned.replay_for_pm(engine, None, None, &mut visitor)?;

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                let stats = visitor.into_cached_log_stats(exhausted);
                return Ok((metadata_opt, protocol_opt, stats));
            }

            // Case 2(b): P&M incomplete from pruned replay, try CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values. Stats from the pruned replay cover commits after
            // the CRC version -- this is the most common case (P&M rarely changes).
            if let CrcLoadResult::Loaded(crc) = lazy_crc.get_or_load(engine) {
                info!("P&M fallback to CRC (no P&M changes after CRC version)");
                let stats = visitor.into_cached_log_stats(exhausted);
                return Ok((
                    metadata_opt.or_else(|| Some(crc.metadata.clone())),
                    protocol_opt.or_else(|| Some(crc.protocol.clone())),
                    stats,
                ));
            }

            // Case 2(c): CRC failed to load. Replay the remaining segment (checkpoint +
            // commits up through CRC version), carrying forward any partial results from the
            // pruned replay above. Merge stats from both segments and mark incomplete since
            // we're mixing two segments with a checkpoint boundary.
            warn!(
                "CRC at version {} failed to load, replaying remaining segment",
                crc_v
            );
            let remaining = self.segment_through_crc(crc_v);
            let mut remaining_visitor = StatsAccumulatorVisitor::new();
            let (m, p, _) = remaining.replay_for_pm(
                engine,
                metadata_opt,
                protocol_opt,
                &mut remaining_visitor,
            )?;
            visitor.merge(remaining_visitor);
            let stats = visitor.into_cached_log_stats(false); // conservative: mixed segments
            return Ok((m, p, stats));
        }

        // Case 3 / Case 4: Full P&M log replay.
        let mut visitor = StatsAccumulatorVisitor::new();
        let (m, p, exhausted) = self.replay_for_pm(engine, None, None, &mut visitor)?;
        Ok((m, p, visitor.into_cached_log_stats(exhausted)))
    }

    /// Replays the log segment for Protocol and Metadata, merging with any already-found values.
    /// Also accumulates file stats from commit files into the provided visitor.
    /// Stops early once both P&M are found. Returns `(metadata, protocol, exhausted)` where
    /// `exhausted` is true if the iterator was fully consumed (no early break).
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
        mut metadata_opt: Option<Metadata>,
        mut protocol_opt: Option<Protocol>,
        visitor: &mut StatsAccumulatorVisitor,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>, bool)> {
        let mut exhausted = true;
        for actions_batch in self.read_pm_batches(engine)? {
            let batch = actions_batch?;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(batch.actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(batch.actions.as_ref())?;
            }
            // Accumulate stats from commit files only (not checkpoints).
            // Checkpoint stats are handled separately by the CRC writer.
            if batch.is_log_batch {
                visitor.visit_rows_of(batch.actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                exhausted = false;
                break;
            }
        }
        Ok((metadata_opt, protocol_opt, exhausted))
    }

    /// Replay the commit log for Protocol, Metadata, and stats accumulation columns.
    /// Commits use a widened schema (add.size, remove.size, domainMetadata, commitInfo.operation)
    /// while checkpoints use the narrow P&M-only schema (column pruning matters for parquet).
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let result = self.read_actions_with_projected_checkpoint_actions(
            engine,
            COMMIT_PM_SCHEMA.clone(),
            CHECKPOINT_PM_SCHEMA.clone(),
            META_PREDICATE.clone(),
            None,
        )?;
        Ok(result.actions)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use itertools::Itertools;
    use test_log::test;

    use crate::engine::sync::SyncEngine;
    use crate::Snapshot;

    // NOTE: In addition to testing the meta-predicate for metadata replay, this test also verifies
    // that the parquet reader properly infers nullcount = rowcount for missing columns. The two
    // checkpoint part files that contain transaction app ids have truncated schemas that would
    // otherwise fail skipping due to their missing nullcount stat:
    //
    // Row group 0:  count: 1  total(compressed): 111 B total(uncompressed):107 B
    // --------------------------------------------------------------------------------
    //              type    nulls  min / max
    // txn.appId    BINARY  0      "3ae45b72-24e1-865a-a211-3..." / "3ae45b72-24e1-865a-a211-3..."
    // txn.version  INT64   0      "4390" / "4390"
    #[test]
    fn test_replay_for_metadata() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parquet_row_group_skipping/"));
        let url = url::Url::from_directory_path(path.unwrap()).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();
        let data: Vec<_> = snapshot
            .log_segment()
            .read_pm_batches(&engine)
            .unwrap()
            .try_collect()
            .unwrap();

        // The checkpoint has five parts, each containing one action:
        // 1. txn (physically missing P&M columns)
        // 2. metaData
        // 3. protocol
        // 4. add
        // 5. txn (physically missing P&M columns)
        //
        // The parquet reader should skip parts 1, 3, and 5. Note that the actual `read_metadata`
        // always skips parts 4 and 5 because it terminates the iteration after finding both P&M.
        //
        // NOTE: Each checkpoint part is a single-row file -- guaranteed to produce one row group.
        //
        // WARNING: https://github.com/delta-io/delta-kernel-rs/issues/434 -- We currently
        // read parts 1 and 5 (4 in all instead of 2) because row group skipping is disabled for
        // missing columns, but can still skip part 3 because has valid nullcount stats for P&M.
        assert_eq!(data.len(), 4);
    }
}
