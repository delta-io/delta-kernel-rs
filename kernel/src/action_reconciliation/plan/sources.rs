//! Physical action reads using the caller's requested schema.

use super::VERSION;
use super::action_set::{ActionSet, ActionSource};
use crate::action_reconciliation::file_metadata::STATS_FIELD as STATS;
use crate::actions::{ADD_NAME, Add, LOG_ADD_SCHEMA};
use crate::checkpoint::{CheckpointKind, CheckpointShape};
use crate::expressions::{column_name, lit};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{FileType, ScanFile};
use crate::schema::{DataType, SchemaRef, StructField};
use crate::{KernelError, KernelResult, PlanBuilder, Version};

/// Reads selected checkpoint actions. Non-content metadata reads use the root without sidecar
/// discovery.
/// Supplies `version`; absent checkpoints and empty selections return an empty relation.
pub(crate) fn checkpoint_actions(
    segment: &LogSegment,
    checkpoint_shape: Option<&CheckpointShape>,
    actions: ActionSet,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let Some((file_type, parts)) = segment.checkpoint_version_tagged_scan_files()? else {
        // The plan builder eliminates this empty relation from replay unions.
        return PlanBuilder::values(schema, vec![]);
    };
    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    if !actions.contains_content_actions() {
        return if actions.contains_non_content_actions() {
            scan(parts, &[VERSION], schema)
        } else {
            PlanBuilder::values(schema, vec![])
        };
    }
    let shape = checkpoint_shape
        .ok_or_else(|| KernelError::schema("File reads require a checkpoint shape"))?;
    if shape.kind() == CheckpointKind::Inline {
        return scan(parts, &[VERSION], schema);
    }
    let constants = parts
        .first()
        .map(|part| part.file_constants.clone())
        .unwrap_or_default();
    let sidecars = shape.sidecars.iter().map(|file| ScanFile {
        meta: file.clone(),
        file_constants: constants.clone(),
    });
    let sidecar_files = PlanBuilder::scan_parquet(sidecars, &[VERSION], schema.clone())?;
    if !actions.contains_non_content_actions() {
        return Ok(sidecar_files);
    }
    PlanBuilder::union_all([
        sidecar_files.filter(actions.content_predicate(ActionSource::Checkpoint))?,
        scan(parts, &[VERSION], schema)?.filter(actions.non_content_predicate())?,
    ])
}

/// Builds live-add rows from CRC files with the supplied `version`.
/// Requires [`ActionSet::live_adds`].
pub(crate) fn crc_arm(
    files: Vec<Add>,
    version: Version,
    actions: ActionSet,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    if actions != ActionSet::live_adds() {
        return Err(KernelError::schema("CRC replay requires adds-only actions"));
    }
    let version = crate::version_as_i64(version)?;
    PlanBuilder::values(
        LOG_ADD_SCHEMA.clone(),
        files.into_iter().map(|add| vec![add.into()]).collect(),
    )?
    .project_patch(|patch| {
        let patch = patch.append(StructField::nullable(VERSION, DataType::LONG), lit(version));
        if schema.field_at(&column_name!(ADD_NAME, STATS)).is_ok() {
            patch
        } else {
            patch.drop_at([ADD_NAME], STATS)
        }
    })
}

/// Build the commit JSON source with file versions in `version`.
/// When `base_version` is set, only commits newer than that base are read.
pub(crate) fn commit_arm(
    log_segment: &LogSegment,
    base_version: Option<Version>,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let commit_files = match base_version {
        Some(version) => log_segment
            .segment_after_version(version)
            .commit_cover_version_tagged_scan_files()?,
        None => log_segment.commit_cover_version_tagged_scan_files()?,
    };
    PlanBuilder::scan_json(commit_files, &[VERSION], schema)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::test_utils::assert_batches_sorted_eq;
    use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
    use rstest::rstest;

    use super::*;
    use crate::action_reconciliation::plan::MetadataPlanner;
    use crate::action_reconciliation::plan::action_set::ACTION_KEY;
    use crate::actions::visitors::SidecarVisitor;
    use crate::arrow::array::{BooleanArray, Int64Array, RecordBatch};
    use crate::arrow::compute::filter_record_batch;
    use crate::arrow::util::pretty::pretty_format_batches;
    use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::last_checkpoint_hint::{LastCheckpointHint, LastCheckpointV2};
    use crate::path::ParsedLogPath;
    use crate::plans::ir::nodes::Operator;
    use crate::scan::StatsOptions;
    use crate::transaction::UpdateTableOperation;
    use crate::unit_test_utils::plans::execute_plan;
    use crate::{Engine, FilteredEngineData, RowVisitor as _, Snapshot};

    #[rstest]
    #[case::live_adds(ActionSet::live_adds(), &["add"])]
    #[case::tombstones(ActionSet::live_adds_with_tombstones(), &["add", "remove"])]
    #[case::non_content(
        ActionSet { live_adds: false, include_tombstones: false, ..ActionSet::ALL },
        &["metaData", "protocol", "txn", "domainMetadata"],
    )]
    #[case::mixed_without_tombstones(
        ActionSet { include_tombstones: false, ..ActionSet::ALL },
        &["add", "metaData", "protocol", "txn", "domainMetadata"],
    )]
    #[case::all(
        ActionSet::ALL,
        &["add", "remove", "metaData", "protocol", "txn", "domainMetadata"],
    )]
    fn checkpoint_actions_match_request(
        #[case] actions: ActionSet,
        #[case] names: &[&str],
        #[values(
            (CheckpointSpec::V1, false),
            (CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(1),
            }), true),
            (CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(1),
            }), false),
        )]
        checkpoint: (CheckpointSpec, bool),
    ) -> crate::Result<()> {
        // Given: a checkpoint containing file actions, tombstones, and non-file actions.
        let (spec, use_sidecar_hint) = checkpoint;
        let mut features = FeatureSet::new().domain_metadata();
        if matches!(spec, CheckpointSpec::V2(_)) {
            features = features.v2_checkpoint();
        }
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data(2, 10)
            .with_features(features)
            .build()
            .unwrap();
        let engine = SyncEngine::new_with_store(table.store().clone());
        let snapshot = Snapshot::builder_for(table.table_root())
            .build(&engine)
            .unwrap();
        let files = snapshot
            .clone()
            .scan_builder()
            .build()?
            .scan_metadata(&engine)?;
        let mut transaction = snapshot
            .update_table(UpdateTableOperation::Write, "test", true)
            .with_transaction_id("app", 7)
            .with_domain_metadata("test", "config")
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        for metadata in files {
            let data = metadata?.scan_files.apply_selection_vector()?;
            if !data.is_empty() {
                let mut selection = vec![false; data.len()];
                selection[0] = true;
                transaction.remove_files(FilteredEngineData::try_new(data, selection)?);
                break;
            }
        }
        let snapshot = transaction.commit(&engine)?.unwrap_post_commit_snapshot();
        let version = crate::version_as_i64(snapshot.version())?;

        // Compare full selected payloads against imperative checkpoint reconstruction.
        let writer = snapshot.clone().create_checkpoint_writer(&engine)?;
        let expected = writer
            .checkpoint_data(&engine)?
            .map(|data| {
                let batch = data?.apply_selection_vector()?.try_into_record_batch()?;
                let selected = BooleanArray::from_iter(
                    (0..batch.num_rows())
                        .map(|row| names.iter().any(|name| batch[*name].is_valid(row))),
                );
                let batch = filter_record_batch(&batch, &selected)?;
                RecordBatch::try_from_iter(
                    names
                        .iter()
                        .map(|name| (*name, batch[*name].clone()))
                        .chain([(
                            VERSION,
                            Arc::new(Int64Array::from(vec![version; batch.num_rows()])) as _,
                        )]),
                )
                .map_err(Into::into)
            })
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(
            expected.iter().map(RecordBatch::num_rows).sum::<usize>(),
            names.len()
        );

        let snapshot = snapshot.checkpoint(&engine, Some(&spec))?.1;
        let mut segment = snapshot.log_segment().clone();
        // V2 hints identify UUID-named manifests; Kernel's writer uses classic naming.
        if matches!(spec, CheckpointSpec::V2(_)) {
            let checkpoint = &mut segment.listed.checkpoint_parts[0];
            let path = ParsedLogPath::new_uuid_parquet_checkpoint(
                snapshot.table_root(),
                snapshot.version(),
            )?
            .location;
            let storage = engine.storage_handler();
            let data = storage
                .read_files(vec![(checkpoint.location.location.clone(), None)])?
                .next()
                .unwrap()?;
            storage.put(&path, data, false)?;
            *checkpoint = ParsedLogPath::new(storage.head(&path)?).unwrap();
        }
        if use_sidecar_hint {
            let checkpoint = &segment.listed.checkpoint_parts[0];
            let mut visitor = SidecarVisitor::default();
            for batch in engine.parquet_handler().read_parquet_files(
                std::slice::from_ref(&checkpoint.location),
                LogSegment::sidecar_read_schema(),
                None,
            )? {
                visitor.visit_rows_of(batch?.as_ref())?;
            }
            assert!(!visitor.sidecars.is_empty());
            segment.last_checkpoint_metadata = Some(LastCheckpointHint {
                version: snapshot.version(),
                v2_checkpoint: Some(LastCheckpointV2 {
                    path: checkpoint.filename.clone(),
                    sidecar_files: Some(visitor.sidecars),
                    ..Default::default()
                }),
                ..Default::default()
            });
        } else {
            segment.last_checkpoint_metadata = None;
        }
        assert_eq!(
            segment.checkpoint_hint_sidecars().is_some(),
            use_sidecar_hint
        );
        let snapshot = Snapshot::new(segment, snapshot.table_configuration().clone())?;
        let checkpoint_shape = actions
            .contains_content_actions()
            .then(|| snapshot.checkpoint_info(&engine))
            .transpose()?
            .flatten();

        // When: selected checkpoint actions are planned from the same resolved shape.
        let plan = MetadataPlanner::new(snapshot.log_segment(), actions)
            .with_stats(StatsOptions::json_only())
            .with_checkpoint_shape(checkpoint_shape)
            .checkpoint_actions()?
            .project_patch(|patch| patch.drop(ACTION_KEY))?
            .build()?;
        // Then: file-action reads always use a complete static sidecar list.
        assert!(
            plan.nodes
                .iter()
                .all(|node| !matches!(node.op, Operator::DynamicScan(_)))
        );
        assert_eq!(
            plan.schema
                .fields()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            names.iter().copied().chain([VERSION]).collect::<Vec<_>>(),
        );
        let actual = execute_plan(&engine, plan)?;
        let expected = pretty_format_batches(&expected)
            .map_err(KernelError::from)?
            .to_string();
        let mut expected = expected.trim().lines().collect::<Vec<_>>();
        ::test_utils::sort_lines!(expected);
        assert_batches_sorted_eq!(expected, &actual);
        Ok(())
    }
}
