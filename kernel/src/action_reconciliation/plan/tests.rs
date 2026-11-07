use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
use rstest::rstest;

use super::*;
use crate::actions::ActionSet;
use crate::arrow::array::AsArray as _;
use crate::arrow::compute::concat_batches;
use crate::arrow::datatypes::Int64Type;
use crate::checkpoint::CheckpointShape;
use crate::committer::FileSystemCommitter;
use crate::engine::sync::SyncEngine;
use crate::schema::{DataType, StructField};
use crate::transaction::UpdateTableOperation;
use crate::unit_test_utils::plans::execute_plan;
use crate::{Engine, KernelResult, Snapshot};

#[rstest]
fn replay_selects_latest_action_without_retention(
    #[values(
        ActionSet::files(),
        ActionSet { protocol: true, ..Default::default() },
        ActionSet { metadata: true, ..Default::default() },
        ActionSet { transactions: true, ..Default::default() },
        ActionSet { domain_metadata: true, ..Default::default() },
        ActionSet::ALL
    )]
    actions: ActionSet,
    #[values(false, true)] with_checkpoint: bool,
) -> KernelResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(1).maybe_with_checkpoint_at(with_checkpoint.then_some(1)),
        )
        .with_features(FeatureSet::new().domain_metadata())
        .build()
        .unwrap();
    let engine = SyncEngine::new_with_store(table.store().clone());
    let mut snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    for version in 1..=2 {
        snapshot = snapshot
            .update_table(UpdateTableOperation::AlterTable, "test", false)
            .add_column(StructField::nullable(
                format!("column_{version}"),
                DataType::LONG,
            ))
            .with_transaction_id("app", version)
            .with_domain_metadata("test", version.to_string())
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_post_commit_snapshot();
    }
    let snapshot = snapshot
        .update_table(UpdateTableOperation::Write, "test", false)
        .with_domain_metadata_removed("test")
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    let shape = actions
        .file_actions
        .then(|| CheckpointShape::try_new(engine.require_plan_executor()?.as_ref(), &snapshot))
        .transpose()?;
    let plan = MetadataPlanner::new(snapshot.log_segment(), actions)
        .with_checkpoint_shape(shape)
        .replay()?
        .build()?;
    let batches = execute_plan(&engine, plan)?;
    let actual = concat_batches(&batches[0].schema(), &batches)?;
    let versions = actual[VERSION].as_primitive::<Int64Type>();
    let mut rows = Vec::new();
    for (row, version) in versions.iter().enumerate() {
        let names: Vec<_> = actions
            .names()
            .filter(|name| actual[*name].is_valid(row))
            .collect();
        assert_eq!(names.len(), 1);
        let name = names[0];
        let action = actual[name].as_struct();
        if name == "txn" {
            let txn_version = action["version"].as_primitive::<Int64Type>();
            assert_eq!(txn_version.iter().nth(row), Some(Some(2)));
        }
        // Replay retains domain deletions until checkpoint retention is applied.
        if name == "domainMetadata" {
            let removed = action["removed"].as_boolean();
            assert_eq!(removed.iter().nth(row), Some(Some(true)));
        }
        rows.push((name, version));
    }
    rows.sort_unstable();
    let expected: Vec<_> = [
        ("add", Some(1)),
        ("domainMetadata", Some(4)),
        ("metaData", Some(3)),
        ("protocol", Some(i64::from(with_checkpoint))),
        ("txn", Some(3)),
    ]
    .into_iter()
    .filter(|(name, _)| actions.names().any(|selected| selected == *name))
    .collect();
    assert_eq!(rows, expected);
    Ok(())
}
