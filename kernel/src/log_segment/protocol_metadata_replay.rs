//! Protocol and Metadata replay logic for [`LogSegment`].
//!
//! This module contains the methods that perform a lightweight log replay to extract the latest
//! Protocol and Metadata actions from a [`LogSegment`].

use std::sync::Arc;

use tracing::{info, instrument};

use super::LogSegment;
use crate::actions::{get_commit_schema, Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::crc::Crc;
use crate::log_replay::ActionsBatch;
use crate::metrics::events::PROTOCOL_METADATA_LOADED_SPAN;
use crate::metrics::SnapshotLoadMetricContext;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, Plan};
#[cfg(feature = "declarative-plans")]
use crate::schema::SchemaRef;
use crate::{DeltaResult, DeltaResultIteratorStatic, Engine, Error};

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
        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in self.read_pm_batches(engine)? {
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

    // Replay the commit log, projecting rows to only contain Protocol and Metadata action columns.
    fn read_pm_batches(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<DeltaResultIteratorStatic<ActionsBatch>> {
        let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;

        // Best-effort declarative path: when the engine provides a plan executor that can run the
        // P&M query plan, use it; otherwise fall back to direct log replay. The default unit-type
        // executor (and any executor that cannot yet run multi-node plans) errors, which triggers
        // the same fallback transparently.
        #[cfg(feature = "declarative-plans")]
        if let Some(batches) = self.read_pm_batches_via_plan(engine, &schema) {
            return Ok(batches);
        }

        Ok(Box::new(self.read_actions(engine, schema)?))
    }
}

#[cfg(feature = "declarative-plans")]
impl LogSegment {
    /// Best-effort execution of the declarative P&M query plan. Returns `None` -- signaling the
    /// caller to fall back to [`Self::read_actions`] -- when the plan cannot be built or the
    /// engine's [`PlanExecutor`](crate::plans::PlanExecutor) cannot run it (including the default
    /// unit-type executor, which errors on every operation).
    fn read_pm_batches_via_plan(
        &self,
        engine: &dyn Engine,
        pm_schema: &SchemaRef,
    ) -> Option<DeltaResultIteratorStatic<ActionsBatch>> {
        match self.try_execute_pm_plan(engine, pm_schema) {
            Ok(batches) => Some(batches),
            Err(err) => {
                info!("declarative P&M plan unavailable, using direct replay: {err}");
                None
            }
        }
    }

    /// Builds and executes the declarative P&M plan, mapping its output to commit
    /// [`ActionsBatch`]es.
    fn try_execute_pm_plan(
        &self,
        engine: &dyn Engine,
        pm_schema: &SchemaRef,
    ) -> DeltaResult<DeltaResultIteratorStatic<ActionsBatch>> {
        let Some(plan) = self.build_pm_query_plan(pm_schema.clone())? else {
            return Ok(Box::new(std::iter::empty()));
        };
        let batches = engine
            .plan_executor()
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| Ok(ActionsBatch::new(batch?, true)));
        Ok(Box::new(batches))
    }

    /// Gathers this segment's checkpoint (parquet) and commit (json) files -- each tagged with its
    /// version as a file constant -- into the declarative P&M query plan (see
    /// [`query_plan::build_pm_plan`]).
    ///
    /// Compaction files are intentionally skipped: a single file constant cannot stand in for the
    /// multiple commit versions a compaction spans, and the individual commit files already cover
    /// the full version range.
    fn build_pm_query_plan(&self, pm_schema: SchemaRef) -> DeltaResult<Option<Plan>> {
        query_plan::build_pm_plan(
            self.listed
                .checkpoint_parts
                .iter()
                .map(query_plan::version_tagged_scan_file)
                .collect(),
            self.listed
                .ascending_commit_files
                .iter()
                .map(query_plan::version_tagged_scan_file)
                .collect(),
            pm_schema,
        )
    }
}

// === Declarative Protocol & Metadata query plan ===
//
// WIP: `read_pm_batches` builds this plan and hands it to the engine's plan executor on a
// best-effort basis, falling back to direct log replay when no executor can run it. The default
// unit-type executor, and any executor that does not yet support multi-node union/aggregate plans,
// error and thereby trigger that fallback.
#[cfg(feature = "declarative-plans")]
mod query_plan {
    use std::sync::Arc;

    use crate::actions::{METADATA_NAME, PROTOCOL_NAME};
    use crate::expressions::{ColumnName, Expression, Scalar};
    use crate::path::ParsedLogPath;
    use crate::plans::ir::nodes::{Agg, Aggregate, ScanFile};
    use crate::plans::{Plan, PlanBuilder};
    use crate::schema::{DataType, SchemaRef, StructField, StructType};
    use crate::{DeltaResult, Predicate};

    /// Synthetic file-constant column carrying each file's commit/checkpoint version. It exists
    /// only to order the `max_by` aggregates and is not emitted by the plan (the aggregate
    /// projects away everything but the winning Protocol and Metadata).
    const VERSION_COLUMN: &str = "version";

    /// Builds the declarative Protocol & Metadata query plan over already-resolved scan files:
    ///
    /// ```text
    /// ScanParquet(checkpoint)   ScanJson(commits)
    ///            \                   /
    ///             \                 /
    ///              v               v
    ///                 UnionAll
    ///                    |
    ///                    v
    ///   Aggregate: max_by(protocol, version) FILTER (protocol IS NOT NULL),
    ///              max_by(metaData, version) FILTER (metaData IS NOT NULL)
    /// ```
    ///
    /// Returns `Ok(None)` when there are no files to scan (an empty plan). `pm_schema` is the
    /// Protocol/Metadata projection; the scans additionally expose [`VERSION_COLUMN`].
    pub(crate) fn build_pm_plan(
        checkpoint_files: Vec<ScanFile>,
        commit_files: Vec<ScanFile>,
        pm_schema: SchemaRef,
    ) -> DeltaResult<Option<Plan>> {
        let scan_schema = scan_schema_with_version(&pm_schema)?;
        let version_columns = vec![ColumnName::new([VERSION_COLUMN])];

        let mut builder = PlanBuilder::new();
        let checkpoint = builder.scan_parquet(
            checkpoint_files,
            version_columns.clone(),
            scan_schema.clone(),
        )?;
        let commits = builder.scan_json(commit_files, version_columns, scan_schema.clone())?;
        let source = builder.union_all([checkpoint, commits])?;

        let version = ColumnName::new([VERSION_COLUMN]);
        builder.aggregate(
            source,
            Aggregate::group_by(scan_schema, [])
                .aggregate(newest_by_version(PROTOCOL_NAME, &version))
                .aggregate(newest_by_version(METADATA_NAME, &version)),
        )?;
        builder.build()
    }

    /// `max_by(<action>, version) FILTER (<action> IS NOT NULL)`: the `<action>` value from the
    /// newest commit that actually carries one. The filter excludes rows missing the action so a
    /// later commit that omits it does not shadow an earlier one that has it.
    fn newest_by_version(action: &str, version: &ColumnName) -> Agg {
        let action = ColumnName::new([action]);
        let present = Predicate::is_not_null(Expression::column(action.clone()));
        Agg::max_by(action, version.clone()).filter(present)
    }

    /// Returns `pm_schema` extended with the synthetic, non-null [`VERSION_COLUMN`] (`LONG`).
    fn scan_schema_with_version(pm_schema: &StructType) -> DeltaResult<SchemaRef> {
        let mut fields = Vec::from_iter(pm_schema.fields().cloned());
        fields.push(StructField::not_null(VERSION_COLUMN, DataType::LONG));
        Ok(Arc::new(StructType::try_new(fields)?))
    }

    /// Builds a [`ScanFile`] for `path`, tagging it with its log version as the [`VERSION_COLUMN`]
    /// file constant so the plan's `max_by(version)` aggregates can order rows across files.
    pub(crate) fn version_tagged_scan_file(path: &ParsedLogPath) -> ScanFile {
        ScanFile {
            meta: path.location.clone(),
            file_constants: vec![Scalar::Long(path.version as i64)],
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::actions::get_commit_schema;
        use crate::expressions::Scalar;
        use crate::plans::ir::nodes::{AggOp, Operator};
        use crate::{FileMeta, Version};

        fn pm_schema() -> SchemaRef {
            get_commit_schema()
                .project(&[PROTOCOL_NAME, METADATA_NAME])
                .unwrap()
        }

        fn scan_file(name: &str, version: Version) -> ScanFile {
            ScanFile {
                meta: FileMeta {
                    location: url::Url::parse(&format!("memory:///{name}")).unwrap(),
                    last_modified: 0,
                    size: 0,
                },
                file_constants: vec![Scalar::Long(version as i64)],
            }
        }

        #[test]
        fn build_pm_plan_wires_scans_union_and_aggregate() {
            let plan = build_pm_plan(
                vec![scan_file("checkpoint.parquet", 1)],
                vec![scan_file("2.json", 2), scan_file("3.json", 3)],
                pm_schema(),
            )
            .unwrap()
            .expect("non-empty plan");

            let nodes = plan.into_nodes();
            // ScanParquet[0], ScanJson[1], UnionAll[2], Aggregate[3] (terminal).
            assert_eq!(nodes.len(), 4);
            assert!(matches!(nodes[0].op, Operator::ScanParquet(_)));
            assert!(matches!(nodes[1].op, Operator::ScanJson(_)));
            assert!(matches!(nodes[2].op, Operator::UnionAll));
            assert_eq!(nodes[2].inputs, vec![0, 1]);

            let Operator::Aggregate(agg) = &nodes[3].op else {
                panic!("expected Aggregate terminal, got {:?}", nodes[3].op);
            };
            assert_eq!(nodes[3].inputs, vec![2]);
            // Global aggregate (no group keys) producing newest protocol + metaData by version.
            assert!(agg.group_by.is_empty());
            assert!(agg
                .aggs
                .iter()
                .all(|a| matches!(a.op, AggOp::MaxBy(_)) && a.filter.is_some()));
            let out: Vec<&str> = agg.schema.fields().map(|f| f.name().as_str()).collect();
            assert_eq!(out, [PROTOCOL_NAME, METADATA_NAME]);
        }

        #[test]
        fn build_pm_plan_with_no_files_is_empty() {
            assert!(build_pm_plan(vec![], vec![], pm_schema())
                .unwrap()
                .is_none());
        }

        #[test]
        fn build_pm_plan_with_only_commits_skips_union() {
            let plan = build_pm_plan(vec![], vec![scan_file("0.json", 0)], pm_schema())
                .unwrap()
                .expect("non-empty plan");
            let nodes = plan.into_nodes();
            // Union over a single arm collapses, so only ScanJson[0] and Aggregate[1] remain.
            assert_eq!(nodes.len(), 2);
            assert!(matches!(nodes[0].op, Operator::ScanJson(_)));
            assert!(matches!(nodes[1].op, Operator::Aggregate(_)));
            assert_eq!(nodes[1].inputs, vec![0]);
        }
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
