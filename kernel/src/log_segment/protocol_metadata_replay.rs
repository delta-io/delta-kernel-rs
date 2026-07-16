//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::Arc;

use tracing::{info, instrument};

use super::LogSegment;
use crate::actions::{Metadata, Protocol, METADATA_FIELD, PROTOCOL_FIELD};
#[cfg(feature = "declarative-plans")]
use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
#[cfg(feature = "declarative-plans")]
use crate::expressions::ColumnName;
use crate::log_replay::ActionsBatch;
use crate::metrics::events::PROTOCOL_METADATA_LOADED_SPAN;
use crate::metrics::SnapshotLoadMetricContext;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder};
#[cfg(feature = "declarative-plans")]
use crate::schema::column_name;
use crate::schema::schema_ref;
use crate::{DeltaResult, Engine, Error};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_opt`], used for fresh
    /// snapshot creation where both Protocol and Metadata must exist.
    ///
    /// Reports metrics: `ProtocolMetadataLoadSuccess` or `ProtocolMetadataLoadFailure`.
    #[instrument(name = PROTOCOL_METADATA_LOADED_SPAN, err, fields(report, operation_id = %metric_context.operation_id, is_catalog_managed = metric_context.is_catalog_managed, correlation_id = metric_context.correlation_id.as_deref().unwrap_or("")), skip(engine, crc))]
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
        metric_context: SnapshotLoadMetricContext,
    ) -> DeltaResult<(Metadata, Protocol)> {
        match self.read_protocol_metadata_opt(engine, crc)? {
            (Some(m), Some(p)) => Ok((m, p)),
            (None, Some(_)) => Err(Error::MissingMetadata),
            (Some(_), None) => Err(Error::MissingProtocol),
            (None, None) => Err(Error::MissingMetadataAndProtocol),
        }
    }

    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns `None` for either if not found.
    ///
    /// This is the unchecked variant of [`Self::read_protocol_metadata`], used for incremental
    /// snapshot updates where the caller can fall back to an existing snapshot's Protocol and
    /// Metadata.
    ///
    /// The `crc` parameter is the CRC eagerly resolved by the caller; it is used to
    /// short-circuit or seed the replay.
    #[instrument(name = "log_seg.load_p_m", skip_all, err)]
    pub(crate) fn read_protocol_metadata_opt(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        // Case 1: If CRC at target version, use it directly and exit early.
        if let Some(crc) = crc.filter(|c| c.version == self.end_version) {
            info!("P&M from CRC at target version {}", self.end_version);
            return Ok((Some(crc.metadata.clone()), Some(crc.protocol.clone())));
        }

        // We didn't return above, so we need to do log replay to find P&M.
        //
        // Case 2: CRC exists at an earlier version => Prune the log segment to only replay
        //         commits *after* the CRC version.
        //   (a) If we find new P&M in the pruned replay, return it.
        //   (b) If we don't find new P&M, fall back to the CRC.
        //
        // Case 3: No CRC exists => Full P&M log replay.

        if let Some(crc) = crc.filter(|c| c.version < self.end_version) {
            // Case 2(a): Replay only commits after CRC version
            info!(
                "Pruning log segment to commits after CRC version {}",
                crc.version
            );
            let pruned = self.segment_after_version(crc.version);
            let (metadata_opt, protocol_opt) = pruned.replay_for_pm(engine)?;

            if metadata_opt.is_some() && protocol_opt.is_some() {
                info!("Found P&M from pruned log replay");
                return Ok((metadata_opt, protocol_opt));
            }

            // Case 2(b): P&M incomplete from pruned replay, use the CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            return Ok((
                metadata_opt.or_else(|| Some(crc.metadata.clone())),
                protocol_opt.or_else(|| Some(crc.protocol.clone())),
            ));
        }

        // Case 3: Full P&M log replay.
        self.replay_for_pm(engine)
    }

    /// Replays the log segment for Protocol and Metadata, stopping early once both are found.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        // The declarative P&M plan is not yet the default. It stays compiled and type-checked (see
        // `read_pm_batches_via_plan`) but disabled behind an always-false cfg (`any()`) until a
        // followup makes it the default; legacy replay runs for now. TODO(#2874-followup).
        #[cfg(all(feature = "declarative-plans", any()))]
        let actions_batches = match self.read_pm_batches_via_plan(engine) {
            Ok(batches) => batches,
            // Fall back to legacy replay only when the engine's plan executor does not support the
            // P&M plan. A genuine execution failure must propagate rather than silently re-run.
            Err(Error::Unsupported(msg)) => {
                info!("declarative P&M plan unsupported, using legacy replay: {msg}");
                Box::new(self.read_pm_batches(engine)?)
            }
            Err(err) => return Err(err),
        };

        #[cfg(not(all(feature = "declarative-plans", any())))]
        let actions_batches = self.read_pm_batches(engine)?;

        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in actions_batches {
            let actions = actions_batch?.actions;
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                break;
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    // Disabled on the snapshot-load path (see `replay_for_pm`); kept compiled and type-checked
    // until a followup makes the declarative P&M plan the default. TODO(#2874-followup).
    #[cfg(feature = "declarative-plans")]
    #[allow(dead_code)]
    fn read_pm_batches_via_plan(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<ActionsBatch>> + Send>> {
        let versioned_schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
            not_null "version": LONG,
        };

        let commit_files = self.commit_cover_version_tagged_scan_files()?;
        let commits = PlanBuilder::scan_json(commit_files, &["version"], versioned_schema.clone())?;

        // A checkpoint's parts share one format; scan them with the matching operator.
        let checkpoint = self
            .checkpoint_version_tagged_scan_files()?
            .map(|(file_type, checkpoint_files)| {
                let scan = match file_type {
                    FileType::Json => PlanBuilder::scan_json,
                    FileType::Parquet => PlanBuilder::scan_parquet,
                };
                scan(checkpoint_files, &["version"], versioned_schema.clone())
            })
            .transpose()?;

        let plan = PlanBuilder::union_all(std::iter::once(commits).chain(checkpoint))?
            .aggregate_ungrouped(|a| {
                a.max_non_null_by(ColumnName::new([PROTOCOL_NAME]), column_name!("version"))
                    .max_non_null_by(ColumnName::new([METADATA_NAME]), column_name!("version"))
            })?
            .build()?;

        // NOTE: The plan dedupes all actions, so mark all results as coming from checkpoint
        let batches = engine
            .plan_executor()
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| Ok(ActionsBatch::new(batch?, true)));
        Ok(Box::new(batches))
    }

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ActionsBatch>> + Send> {
        let schema = schema_ref! {
            (&PROTOCOL_FIELD),
            (&METADATA_FIELD),
        };
        self.read_actions(engine, schema)
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
