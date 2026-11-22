//! Integration tests for setting the table description through update-table transactions.

use delta_kernel::Engine;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::transaction::create_table::create_table;
use rstest::rstest;
use test_utils::{read_actions_from_commit, test_table_setup};

use crate::common::write_utils::get_simple_int_schema;

const DESCRIPTION: &str = "table description";

fn create_table_with_description(
    table_path: &str,
    engine: &dyn Engine,
    description: Option<&str>,
) -> delta_kernel::Result<SnapshotRef> {
    let mut create = create_table(table_path, get_simple_int_schema(), "Test/1.0", false);
    if let Some(description) = description {
        create = create.with_description(description);
    }
    Ok(create
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?
        .unwrap_post_commit_snapshot())
}

#[rstest]
#[case::alter_sets_description(UpdateTableOperation::AlterTable, None, true)]
#[case::alter_writes_unchanged_description(
    UpdateTableOperation::AlterTable,
    Some(DESCRIPTION),
    true
)]
#[case::write_replaces_description(UpdateTableOperation::Write, Some("old description"), true)]
#[case::write_skips_unchanged_description(UpdateTableOperation::Write, Some(DESCRIPTION), false)]
#[tokio::test]
async fn update_table_sets_description(
    #[case] operation: UpdateTableOperation,
    #[case] initial_description: Option<&str>,
    #[case] expect_metadata_action: bool,
) -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let before = create_table_with_description(&table_path, engine.as_ref(), initial_description)?;
    let after = before
        .clone()
        .update_table(operation, "Test/1.0", false)
        .with_description(DESCRIPTION)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    assert_eq!(
        after.table_configuration().metadata().description(),
        Some(DESCRIPTION)
    );
    assert_eq!(
        after.table_configuration().metadata().id(),
        before.table_configuration().metadata().id()
    );
    let metadata_actions =
        read_actions_from_commit(after.table_root(), after.version(), "metaData")?;
    assert_eq!(metadata_actions.len(), usize::from(expect_metadata_action));
    Ok(())
}

#[tokio::test]
async fn alter_table_sets_description_with_schema_change() -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let after = create_table_with_description(&table_path, engine.as_ref(), None)?
        .update_table(UpdateTableOperation::AlterTable, "Test/1.0", false)
        .add_column(StructField::nullable("value", DataType::STRING))
        .with_description(DESCRIPTION)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    assert!(after.schema().contains("value"));
    assert_eq!(
        after.table_configuration().metadata().description(),
        Some(DESCRIPTION)
    );
    let metadata_actions =
        read_actions_from_commit(after.table_root(), after.version(), "metaData")?;
    assert_eq!(metadata_actions.len(), 1);
    Ok(())
}
