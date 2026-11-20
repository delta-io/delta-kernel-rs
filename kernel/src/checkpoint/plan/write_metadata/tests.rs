use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
use rstest::rstest;

use super::*;
use crate::actions::{CheckpointMetadata, SET_TRANSACTION_FIELD, SetTransaction};
use crate::arrow::record_batch::RecordBatch;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::expressions::Scalar;
use crate::last_checkpoint_hint::HintAction;
use crate::plans::{Operation, PlanExecutor, ScopedPlanExecutor};
use crate::schema::schema_ref;
use crate::{Engine as _, Snapshot};

fn execute(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<Vec<RecordBatch>> {
    executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect()
}

fn assert_rows(
    executor: &dyn PlanExecutor,
    plan: Plan,
    rows: Vec<Vec<Scalar>>,
) -> KernelResult<()> {
    let expected = PlanBuilder::values(plan.schema.clone(), rows)?.build()?;
    assert_eq!(execute(executor, plan)?, execute(executor, expected)?);
    Ok(())
}

fn file_results(
    executor: &dyn ScopedPlanExecutor,
    name: &str,
    rows: Vec<Vec<Scalar>>,
) -> KernelResult<RelationRef> {
    executor.execute_and_retain(
        name,
        PlanBuilder::values(
            schema_ref! {
                not_null "path": STRING,
                not_null "size": LONG,
                not_null "file_modification": LONG,
            },
            rows,
        )?
        .build()?,
    )
}

#[rstest]
#[case::v1(None)]
#[case::v2_classic(Some("classic"))]
#[case::v2_json(Some("json"))]
#[case::v2_parquet(Some("parquet"))]
#[tokio::test]
async fn checkpoint_statistics_match_reconciliation_and_write_results(
    #[case] manifest_format: Option<&str>,
    #[values(0, 3)] files: usize,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let v2 = manifest_format.is_some();
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(1))
        .with_features(if v2 {
            FeatureSet::new().v2_checkpoint()
        } else {
            FeatureSet::new()
        })
        .with_data(files, 1)
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let executor = SyncPlanExecutor::new(Some(table.store().clone())).get_scoped();
    let snapshot = Snapshot::builder_for(table.table_root())
        .build(&engine)
        .unwrap();
    let writer = snapshot.create_checkpoint_writer(&engine)?;
    let legacy = writer.checkpoint_data(&engine)?;
    let legacy_state = legacy.state();
    for batch in legacy {
        batch?;
    }
    assert!(legacy_state.is_exhausted());
    assert_eq!(legacy_state.add_actions_count(), files as i64);

    let (actions, content_counts, sidecars, sidecar_count, sidecar_bytes) = if v2 {
        let (content, non_content) = writer.declarative_v2_checkpoint_data_plans(&engine)?;
        let content = executor.execute_and_retain("content", content)?;
        let actions = executor.execute_and_retain("non-content", non_content)?;
        let counts = executor.execute_and_retain(
            "content-counts",
            CheckpointWriter::declarative_checkpoint_counts_plan(content.clone())?,
        )?;
        assert_rows(
            &executor,
            PlanBuilder::relation_source(counts.clone()).build()?,
            vec![vec![(files as i64).into(), (files as i64).into()]],
        )?;
        // File boundaries do not determine action counts: three adds span two sidecars.
        let descriptors = if files == 0 {
            vec![]
        } else {
            vec![
                vec!["a.parquet".into(), 128i64.into(), 5i64.into()],
                vec!["b.parquet".into(), 256i64.into(), 6i64.into()],
            ]
        };
        let sidecar_count = descriptors.len() as i64;
        let sidecar_bytes = if files == 0 { 0i64 } else { 384i64 };
        let sidecars = file_results(&executor, "sidecars", descriptors)?;
        let expected = if files == 0 {
            vec![]
        } else {
            vec![
                vec![Scalar::Struct(
                    Sidecar::new("a.parquet".into(), 128, 5, None).into(),
                )],
                vec![Scalar::Struct(
                    Sidecar::new("b.parquet".into(), 256, 6, None).into(),
                )],
            ]
        };
        assert_rows(
            &executor,
            CheckpointWriter::declarative_sidecar_actions_plan(sidecars.clone())?,
            expected,
        )?;
        let actions = executor.execute_and_retain(
            "manifest",
            writer.declarative_v2_checkpoint_manifest_plan(
                actions,
                sidecars.clone(),
                counts.clone(),
                content.schema(),
            )?,
        )?;
        (
            actions,
            Some(counts),
            Some(sidecars),
            sidecar_count,
            sidecar_bytes,
        )
    } else {
        let actions = executor.execute_and_retain(
            "checkpoint",
            writer.declarative_checkpoint_data_plan(&engine)?,
        )?;
        assert_rows(
            &executor,
            CheckpointWriter::declarative_checkpoint_counts_plan(actions.clone())?,
            vec![vec![
                legacy_state.actions_count().into(),
                (files as i64).into(),
            ]],
        )?;
        (actions, None, None, 0, 0)
    };
    let uuid_root = v2 && manifest_format != Some("classic");
    let root_path = if let Some(format) = manifest_format.filter(|_| uuid_root) {
        format!(
            "{:020}.checkpoint.00000000-0000-0000-0000-000000000001.{format}",
            writer.version
        )
    } else {
        format!("{:020}.checkpoint.parquet", writer.version)
    };
    let root_meta = FileMeta::new(
        writer.snapshot.log_segment().log_root.join(&root_path)?,
        17,
        1024,
    );
    let root = file_results(
        &executor,
        "root",
        vec![vec![root_path.clone().into(), 1024i64.into(), 17i64.into()]],
    )?;
    let checkpoint_schema = actions.schema().clone();
    let prefetch = if uuid_root {
        execute(
            &executor,
            CheckpointWriter::declarative_last_checkpoint_prefetch_plan(actions.clone())?,
        )?
    } else {
        vec![]
    };
    let plan = writer.declarative_last_checkpoint_statistics_plan(
        actions,
        root,
        content_counts,
        sidecars,
    )?;
    let batches = execute(&executor, plan)?;
    assert_eq!(batches.len(), 1);
    let aggregate = ArrowEngineData::new(batches[0].clone());
    let bytes = writer.last_checkpoint_bytes(
        &aggregate,
        &root_meta,
        &checkpoint_schema,
        prefetch
            .into_iter()
            .map(|batch| Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)),
    )?;
    assert!(bytes.starts_with(br#"{"version":1,"size":"#));
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(value["checksum"], json_checksum::json_checksum(&value));
    assert_eq!(value.get("checkpointFiles").is_some(), !uuid_root);
    if !uuid_root {
        assert_eq!(
            value["checkpointFiles"],
            serde_json::json!([{
                "path": root_path, "length": 1024, "isDir": false, "modificationTime": 17,
            }])
        );
    }
    let hint: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    assert_eq!(hint.version, writer.snapshot.version());
    assert_eq!(hint.size, legacy_state.actions_count() + sidecar_count);
    assert_eq!(hint.parts, None);
    assert_eq!(hint.size_in_bytes, Some(1024 + sidecar_bytes));
    assert_eq!(hint.num_of_add_files, Some(files as i64));
    assert_eq!(
        hint.checkpoint_schema,
        (manifest_format != Some("json")).then_some(checkpoint_schema.clone())
    );
    assert!(hint.applies_to(&[ParsedLogPath::new(root_meta.clone()).unwrap()]));
    if uuid_root {
        let locator = hint.v2_checkpoint.unwrap();
        assert_eq!(locator.path, root_path);
        assert_eq!(locator.size_in_bytes, Some(1024));
        assert_eq!(locator.modification_time, Some(17));
        let sidecars = locator.sidecar_files.unwrap();
        assert_eq!(sidecars.len() as i64, sidecar_count);
        if files != 0 {
            assert_eq!(
                sidecars,
                vec![
                    Sidecar::new("a.parquet".into(), 128, 5, None),
                    Sidecar::new("b.parquet".into(), 256, 6, None),
                ]
            );
        }
        let non_content = locator.non_file_actions.unwrap();
        assert_eq!(non_content.len(), 3);
        let metadata = non_content
            .iter()
            .find_map(|action| match action {
                HintAction::CheckpointMetadata(metadata) => Some(metadata),
                _ => None,
            })
            .unwrap();
        assert_eq!(metadata.version, writer.version);
        let tags = metadata.tags.as_ref().unwrap();
        assert_eq!(tags["sidecarNumActions"], files.to_string());
        assert_eq!(tags["numOfAddFiles"], files.to_string());
        assert_eq!(tags["sidecarSizeInBytes"], sidecar_bytes.to_string());
        let schema: StructType = serde_json::from_str(&tags[SIDECAR_FILE_SCHEMA_TAG])?;
        assert_eq!(
            schema
                .fields()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["add", "remove"]
        );
    } else {
        assert_eq!(hint.v2_checkpoint, None);
    }
    let oversized_schema = Arc::new(StructType::try_new([StructField::nullable(
        "x".repeat(CHECKPOINT_SCHEMA_THRESHOLD),
        DataType::LONG,
    )])?);
    let bytes = writer.last_checkpoint_bytes(&aggregate, &root_meta, &oversized_schema, [])?;
    let oversized: LastCheckpointHint = serde_json::from_slice(&bytes)?;
    assert_eq!(oversized.checkpoint_schema, None);
    for rows in [
        vec![],
        vec![vec![1, 0, 1024], vec![1, 0, 1024]],
        vec![vec![-1, 0, 1024]],
        vec![vec![1, -1, 1024]],
        vec![vec![1, 2, 1024]],
        vec![vec![1, 0, -1]],
        vec![vec![1, 0, 1023]],
    ] {
        let invalid = engine.evaluation_handler().create_many(
            STATISTICS_SCHEMA.clone(),
            rows.into_iter()
                .map(|row| row.into_iter().map(Scalar::Long).collect())
                .collect(),
        )?;
        assert!(
            writer
                .last_checkpoint_bytes(invalid.as_ref(), &root_meta, &checkpoint_schema, [])
                .is_err()
        );
    }
    let mut wrong_root = root_meta.clone();
    wrong_root.location = writer
        .snapshot
        .log_segment()
        .log_root
        .join("00000000000000000002.checkpoint.parquet")?;
    assert!(
        writer
            .last_checkpoint_bytes(&aggregate, &wrong_root, &checkpoint_schema, [])
            .is_err()
    );
    Ok(())
}

#[rstest]
fn prefetch_lists_are_complete_or_omitted(
    #[values(0, 30, 31)] non_content_count: usize,
    #[values(0, 30, 31)] sidecar_count: usize,
) -> KernelResult<()> {
    let executor = SyncPlanExecutor::new(None).get_scoped();
    let schema = Arc::new(schema! {
        (&SET_TRANSACTION_FIELD),
        (&SIDECAR_FIELD),
        (&CHECKPOINT_METADATA_FIELD),
    });
    let null_txn = Scalar::null(SetTransaction::to_schema());
    let null_sidecar = Scalar::null(Sidecar::to_schema());
    let null_metadata = Scalar::null(CheckpointMetadata::to_schema());
    let mut rows = Vec::new();
    for index in 0..non_content_count {
        let (txn, metadata) = if index == 0 {
            (
                null_txn.clone(),
                CheckpointMetadata {
                    version: 7,
                    tags: None,
                }
                .into(),
            )
        } else {
            (
                SetTransaction {
                    app_id: format!("app-{index}"),
                    version: index as i64,
                    last_updated: Some(17),
                }
                .into(),
                null_metadata.clone(),
            )
        };
        rows.push(vec![txn, null_sidecar.clone(), metadata]);
    }
    for index in 0..sidecar_count {
        rows.push(vec![
            null_txn.clone(),
            Sidecar::new(format!("{index}.parquet"), 128, 17, None).into(),
            null_metadata.clone(),
        ]);
    }
    let manifest =
        executor.execute_and_retain("manifest", PlanBuilder::values(schema, rows)?.build()?)?;
    let batches = execute(
        &executor,
        CheckpointWriter::declarative_last_checkpoint_prefetch_plan(manifest)?,
    )?;
    let mut hint = LastCheckpointV2::default();
    for batch in &batches {
        hint.visit_rows_of(&ArrowEngineData::new(batch.clone()))?;
    }
    assert_eq!(
        hint.non_file_actions.as_ref().map(Vec::len),
        (non_content_count <= 30).then_some(non_content_count)
    );
    assert_eq!(
        hint.sidecar_files.as_ref().map(Vec::len),
        (sidecar_count <= 30).then_some(sidecar_count)
    );
    if let Some(actions) = hint.non_file_actions {
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, HintAction::CheckpointMetadata(_)))
                .count(),
            usize::from(non_content_count > 0)
        );
    }
    if let Some(sidecars) = hint.sidecar_files {
        let mut actual = sidecars
            .into_iter()
            .map(|sidecar| sidecar.path)
            .collect::<Vec<_>>();
        let mut expected = (0..sidecar_count)
            .map(|index| format!("{index}.parquet"))
            .collect::<Vec<_>>();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[rstest]
#[case::below(CHECKPOINT_SCHEMA_THRESHOLD - 1, true)]
#[case::at(CHECKPOINT_SCHEMA_THRESHOLD, true)]
#[case::above(CHECKPOINT_SCHEMA_THRESHOLD + 1, false)]
fn checkpoint_schema_respects_dbr_length_limit(
    #[case] length: usize,
    #[case] included: bool,
    #[values("", "雪😀")] prefix: &str,
) -> KernelResult<()> {
    let empty = schema_ref! { nullable "": STRING };
    let overhead = serde_json::to_string(&empty)?.encode_utf16().count();
    let name = format!(
        "{prefix}{}",
        "x".repeat(length - overhead - prefix.encode_utf16().count())
    );
    let schema = Arc::new(StructType::try_new([StructField::nullable(
        name,
        DataType::STRING,
    )])?);
    assert_eq!(checkpoint_schema_json(&schema)?.is_some(), included);
    Ok(())
}
