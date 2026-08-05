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
use crate::error::{delta_errors, DeltaErrorCode};
#[cfg(feature = "declarative-plans")]
use crate::expressions::ColumnName;
use crate::log_replay::ActionsBatch;
use crate::metrics::ProtocolMetadataSource;
#[cfg(feature = "declarative-plans")]
use crate::plans::ir::nodes::FileType;
#[cfg(feature = "declarative-plans")]
use crate::plans::{Operation, PlanBuilder, PlanExecutor};
#[cfg(feature = "declarative-plans")]
use crate::schema::column_name;
use crate::schema::schema_ref;
use crate::{DeltaResult, Engine, EngineErrorKind, Error};

impl LogSegment {
    /// Read the latest Protocol and Metadata from this log segment, using CRC when available.
    /// Returns an error if either is missing, and the [`ProtocolMetadataSource`] describing how
    /// P&M was resolved.
    ///
    /// This is the checked variant of [`Self::read_protocol_metadata_opt`], used for fresh
    /// snapshot creation where both Protocol and Metadata must exist.
    pub(crate) fn read_protocol_metadata(
        &self,
        engine: &dyn Engine,
        crc: Option<&Arc<Crc>>,
    ) -> DeltaResult<(Metadata, Protocol, ProtocolMetadataSource)> {
        match self.read_protocol_metadata_opt(engine, crc)? {
            (Some(m), Some(p), source) => Ok((m, p, source)),
            (None, Some(_), _) => Err(delta_errors::state_recover_error(
                "metadata",
                self.end_version,
                Error::missing_metadata(),
            )),
            (Some(_), None, _) => Err(delta_errors::state_recover_error(
                "protocol",
                self.end_version,
                Error::missing_protocol(),
            )),
            (None, None, _) => Err(delta_errors::state_recover_error(
                "protocol",
                self.end_version,
                Error::missing_metadata_and_protocol(),
            )),
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
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>, ProtocolMetadataSource)> {
        // Case 1: If CRC at target version, use it directly and exit early.
        if let Some(crc) = crc.filter(|c| c.version == self.end_version) {
            info!("P&M from CRC at target version {}", self.end_version);
            return Ok((
                Some(crc.metadata.clone()),
                Some(crc.protocol.clone()),
                ProtocolMetadataSource::CrcAtTarget,
            ));
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
                return Ok((
                    metadata_opt,
                    protocol_opt,
                    ProtocolMetadataSource::CrcSeededPmOnlyReplay,
                ));
            }

            // Case 2(b): P&M incomplete from pruned replay, use the CRC.
            // Use `or_else` so any newer P or M found in the pruned replay takes priority
            // over the (older) CRC values.
            info!("P&M fallback to CRC (no P&M changes after CRC version)");
            return Ok((
                metadata_opt.or_else(|| Some(crc.metadata.clone())),
                protocol_opt.or_else(|| Some(crc.protocol.clone())),
                ProtocolMetadataSource::CrcSeededPmOnlyReplay,
            ));
        }

        // Case 3: Full P&M log replay.
        let (metadata_opt, protocol_opt) = self.replay_for_pm(engine)?;
        Ok((
            metadata_opt,
            protocol_opt,
            ProtocolMetadataSource::FullReplay,
        ))
    }

    /// Replays the log segment for Protocol and Metadata, stopping early once both are found.
    fn replay_for_pm(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<(Option<Metadata>, Option<Protocol>)> {
        // Providing a plan executor opts the engine into declarative P&M replay.
        #[cfg(feature = "declarative-plans")]
        let actions_batches = match engine.plan_executor() {
            Some(executor) => self
                .read_pm_batches_via_plan(executor.as_ref())
                .map_err(|error| self.prefer_protocol_error(engine, error))?,
            None => Box::new(self.read_pm_batches(engine)?) as _,
        };

        #[cfg(not(feature = "declarative-plans"))]
        let actions_batches = self.read_pm_batches(engine)?;

        let mut metadata_opt = None;
        let mut protocol_opt = None;
        for actions_batch in actions_batches {
            let actions = match actions_batch {
                Ok(actions_batch) => actions_batch.actions,
                Err(error) => return Err(self.prefer_protocol_error(engine, error)),
            };
            if protocol_opt.is_none() {
                protocol_opt = Protocol::try_new_from_data_for_table(
                    actions.as_ref(),
                    self.log_root.join("../")?.to_string(),
                )?;
            }
            if metadata_opt.is_none() {
                metadata_opt = Metadata::try_new_from_data(actions.as_ref())?;
            }
            if metadata_opt.is_some() && protocol_opt.is_some() {
                break;
            }
        }
        Ok((metadata_opt, protocol_opt))
    }

    fn prefer_protocol_error(&self, engine: &dyn Engine, error: Error) -> Error {
        if error
            .as_engine_error()
            .is_some_and(|error| error.kind() == EngineErrorKind::CorruptData)
        {
            match self.protocol_error_precedence(engine) {
                Some(protocol_error) => protocol_error.with_delta_source(error),
                None => error,
            }
        } else {
            error
        }
    }

    /// Replays only protocol actions after combined P&M decoding failed. This preserves protocol
    /// validation precedence without replacing an unrelated engine failure.
    fn protocol_error_precedence(&self, engine: &dyn Engine) -> Option<Error> {
        let schema = schema_ref! { (&PROTOCOL_FIELD) };
        let actions_batches = self.read_actions(engine, schema).ok()?;
        let table_name_or_path = self.log_root.join("../").ok()?.to_string();
        for actions_batch in actions_batches {
            let actions = actions_batch.ok()?.actions;
            match Protocol::try_new_from_data_for_table(actions.as_ref(), &table_name_or_path) {
                Ok(Some(_)) => return None,
                Ok(None) => {}
                Err(error)
                    if error.as_delta_error().is_some_and(|error| {
                        matches!(
                            error.code(),
                            DeltaErrorCode::DeltaInvalidProtocolVersion
                                | DeltaErrorCode::DeltaUnsupportedFeaturesForRead
                        )
                    }) =>
                {
                    return Some(error);
                }
                Err(_) => return None,
            }
        }
        None
    }

    #[cfg(feature = "declarative-plans")]
    fn read_pm_batches_via_plan(
        &self,
        executor: &dyn PlanExecutor,
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
        let batches = executor
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
    use std::sync::Arc;

    use itertools::Itertools;
    use rstest::rstest;
    use test_utils::add_commit;

    use crate::engine::sync::SyncEngine;
    #[cfg(feature = "declarative-plans")]
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::object_store::memory::InMemory;
    #[cfg(feature = "declarative-plans")]
    use crate::plans::{Operation, PlanExecutor, PlanResult};
    use crate::{DeltaErrorCode, EngineErrorKind, Error, Snapshot};
    #[cfg(feature = "declarative-plans")]
    use crate::{EngineError, EngineResult};

    // A [`PlanExecutor`] whose every operation fails, used to prove that a plan-path failure
    // surfaces from P&M replay rather than falling back to legacy replay.
    #[cfg(feature = "declarative-plans")]
    struct FailingPlanExecutor;

    #[cfg(feature = "declarative-plans")]
    impl PlanExecutor for FailingPlanExecutor {
        fn execute_op(&self, _op: Operation) -> EngineResult<PlanResult> {
            Err(EngineError::other("plan executor deliberately failed"))
        }
    }

    #[rstest]
    #[case(
        r#"{"minReaderVersion":2147483647,"minWriterVersion":2147483647,"readerFeatures":[],"writerFeatures":[]}"#,
        DeltaErrorCode::DeltaInvalidProtocolVersion
    )]
    #[case(
        r#"{"minReaderVersion":3,"minWriterVersion":6}"#,
        DeltaErrorCode::DeltaInvalidProtocolVersion
    )]
    #[case(
        r#"{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["unknownReaderFeature"],"writerFeatures":[]}"#,
        DeltaErrorCode::DeltaUnsupportedFeaturesForRead
    )]
    fn protocol_validation_precedes_malformed_metadata(
        #[case] protocol: &str,
        #[case] expected: DeltaErrorCode,
    ) {
        let table_root = "memory:///";
        let store = Arc::new(InMemory::new());
        let commit = format!(
            r#"{{"metaData":{{"id":"id","format":{{"provider":"parquet","options":{{}}}},"partitionColumns":[],"configuration":{{}}}}}}
{{"protocol":{protocol}}}"#
        );
        futures::executor::block_on(add_commit(table_root, store.as_ref(), 0, commit)).unwrap();
        let engine = SyncEngine::new_with_store(store);

        let error = Snapshot::builder_for(table_root)
            .build(&engine)
            .unwrap_err();

        let delta_error = error
            .as_delta_error()
            .unwrap_or_else(|| panic!("expected Delta error, got {error:?}"));
        assert_eq!(delta_error.code(), expected);
        let source = std::error::Error::source(delta_error)
            .and_then(|source| source.downcast_ref::<Error>())
            .and_then(Error::as_engine_error)
            .expect("structured protocol error should retain the engine failure");
        assert_eq!(source.kind(), EngineErrorKind::CorruptData);
    }

    #[rstest]
    #[case(r#"{"minReaderVersion":3,"minWriterVersion":7,"writerFeatures":[]}"#)]
    #[case(r#"{"minReaderVersion":1,"minWriterVersion":2,"readerFeatures":[]}"#)]
    #[case(
        r#"{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":[]}"#
    )]
    fn malformed_protocol_shapes_remain_unclassified(#[case] protocol: &str) {
        let table_root = "memory:///";
        let store = Arc::new(InMemory::new());
        let commit = format!(
            r#"{{"metaData":{{"id":"id","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[]}}","partitionColumns":[],"configuration":{{}}}}}}
{{"protocol":{protocol}}}"#
        );
        futures::executor::block_on(add_commit(table_root, store.as_ref(), 0, commit)).unwrap();
        let engine = SyncEngine::new_with_store(store);

        let error = Snapshot::builder_for(table_root)
            .build(&engine)
            .unwrap_err();

        let delta_error = error
            .as_delta_error()
            .unwrap_or_else(|| panic!("expected Delta error, got {error:?}"));
        assert_eq!(delta_error.code(), DeltaErrorCode::DeltaKernelUnclassified);
        assert_eq!(error.legacy_error_kind(), Some("InvalidProtocol"));
    }

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
    #[test_log::test]
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

    // With the `declarative-plans` feature flag on, `SyncEngine` resolves P&M through the
    // declarative plan.
    //
    // This fixture's checkpoint names its map entry fields `entries` where kernel expects
    // `key_value`. Parquet takes that name from the writer's Arrow schema unless the writer sets
    // `WriterProperties::coerce_types`, which is off by default, and Arrow's own
    // `MapFieldNames::default()` is `entries`. So a writer that builds its maps from Arrow defaults
    // produces a file kernel must translate on read. Spark and kernel both write `key_value`,
    // covered by
    // `scan_plan::execution_tests::declarative_metadata_reconciles_checkpoint_with_later_commits`.
    #[test_log::test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_entries_named_maps() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 1);
        assert_eq!(snapshot.schema().fields().count(), 3);
    }

    // The array counterpart of the test above. This fixture's checkpoint names its array element
    // fields `item` where kernel expects `element`, so it covers the other half of the naming
    // disagreement. `metaData.partitionColumns` is the array in question, and it is present in
    // every `metaData` action, so its element name is checked on every P&M replay.
    #[test_log::test]
    fn test_snapshot_build_via_plan_over_parquet_checkpoint_with_item_named_arrays() {
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/parsed-stats/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();

        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        assert_eq!(snapshot.version(), 5);
        assert_eq!(snapshot.schema().fields().count(), 5);
    }

    #[cfg(feature = "declarative-plans")]
    #[test_log::test]
    fn test_snapshot_build_via_failing_plan_executor_surfaces_error_without_fallback() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/app-txn-checkpoint/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = DelegatingEngine::new(Arc::new(SyncEngine::new()))
            .with_plan_executor(Arc::new(FailingPlanExecutor));

        let result = Snapshot::builder_for(url).build(&engine);

        assert!(
            result.is_err(),
            "plan failure must surface, not fall back to legacy replay"
        );
    }
}
