//! Integration tests for setting the table description through update-table transactions.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::transaction::create_table::create_table;
use rstest::rstest;
use test_utils::{read_actions_from_commit, test_table_setup};

use crate::common::write_utils::get_simple_int_schema;

#[rstest]
#[case::sets_description_on_table_without_one(None, "table description", true)]
#[case::replaces_existing_description(Some("old description"), "table description", true)]
#[case::same_description_writes_no_metadata(Some("table description"), "table description", false)]
#[tokio::test]
async fn update_table_description_writes_metadata_only_when_it_changes(
    #[case] initial_description: Option<&str>,
    #[case] new_description: &str,
    #[case] expect_metadata_action: bool,
) -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let mut create = create_table(&table_path, get_simple_int_schema(), "Test/1.0", false);
    if let Some(description) = initial_description {
        create = create.with_description(description);
    }
    let _ = create
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let before = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let after = before
        .clone()
        .update_table(
            UpdateTableOperation::Custom("Manual Update".to_string()),
            "Test/1.0",
            false,
        )
        .with_description(new_description)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    assert_eq!(
        after.table_configuration().metadata().description(),
        Some(new_description)
    );
    assert_eq!(
        after.table_configuration().metadata().id(),
        before.table_configuration().metadata().id()
    );
    let metadata_actions = read_actions_from_commit(&table_url, after.version(), "metaData")?;
    assert_eq!(metadata_actions.len(), usize::from(expect_metadata_action));
    Ok(())
}

#[tokio::test]
async fn alter_table_sets_description_with_schema_change() -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let _ = create_table(&table_path, get_simple_int_schema(), "Test/1.0", false)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let after = Snapshot::builder_for(table_url.clone())
        .build(engine.as_ref())?
        .update_table(UpdateTableOperation::AlterTable, "Test/1.0", false)
        .add_column(StructField::nullable("value", DataType::STRING))
        .with_description("table description")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    assert!(after.schema().contains("value"));
    assert_eq!(
        after.table_configuration().metadata().description(),
        Some("table description")
    );
    let metadata_actions = read_actions_from_commit(&table_url, after.version(), "metaData")?;
    assert_eq!(metadata_actions.len(), 1);
    Ok(())
}
