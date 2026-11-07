use ::test_utils::assert_batches_sorted_eq;
use ::test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
use rstest::rstest;

use super::*;
use crate::actions::ActionSet;
use crate::checkpoint::CheckpointShape;
use crate::committer::FileSystemCommitter;
use crate::engine::sync::SyncEngine;
use crate::expressions::{Expression as Expr, col, lit, null_lit};
use crate::schema::{DataType, StructField, schema_ref};
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
        .project(
            Expr::struct_from([
                Expr::case_when(
                    actions
                        .names()
                        .map(|name| (Expr::column([name]).is_not_null(), lit(name))),
                    lit("unknown"),
                ),
                col!("version"),
                if actions.transactions {
                    col!("txn.version")
                } else {
                    null_lit(DataType::LONG)
                },
                if actions.domain_metadata {
                    col!("domainMetadata.removed")
                } else {
                    null_lit(DataType::BOOLEAN)
                },
            ]),
            schema_ref! {
                nullable "replayed_action": STRING,
                nullable "version": LONG,
                nullable "txn_version": LONG,
                nullable "removed": BOOLEAN,
            },
        )?
        .build()?;
    let actual = execute_plan(&engine, plan)?;
    const BORDER: &str = "+-----------------+---------+-------------+---------+";
    let mut expected = vec![
        BORDER,
        "| replayed_action | version | txn_version | removed |",
        BORDER,
    ];
    // Replay retains domain deletions until checkpoint retention is applied.
    #[rustfmt::skip]
    let rows = [
        ("add",            "| add             | 1       |             |         |"),
        ("domainMetadata", "| domainMetadata  | 4       |             | true    |"),
        ("metaData",       "| metaData        | 3       |             |         |"),
        ("protocol",      ["| protocol        | 0       |             |         |",
                           "| protocol        | 1       |             |         |"][usize::from(with_checkpoint)]),
        ("txn",            "| txn             | 3       | 2           |         |"),
    ];
    expected.extend(
        rows.into_iter()
            .filter(|(name, _)| actions.names().any(|selected| selected == *name))
            .map(|(_, row)| row),
    );
    expected.push(BORDER);
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}
