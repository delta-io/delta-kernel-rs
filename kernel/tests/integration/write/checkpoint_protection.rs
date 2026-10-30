use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField};
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelResult, Snapshot};
use rstest::rstest;
use test_utils::{assert_result_error_with_message, create_table, test_read, test_table_setup_mt};
use url::Url;

use crate::common::write_utils::{get_simple_schema, simple_id_batch};

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_protection_gates_transactions_and_checkpoints(
    #[values(false, true)] has_feature: bool,
) -> KernelResult<()> {
    let schema = get_simple_schema();
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let writer_features = if has_feature {
        vec!["checkpointProtection"]
    } else {
        vec![]
    };
    // The create-table builder cannot enable checkpointProtection.
    create_table(
        Arc::new(LocalFileSystem::new()),
        table_url.clone(),
        schema.clone(),
        &[],    /* partition_columns */
        true,   /* use_37_protocol */
        vec![], /* reader_features */
        writer_features,
    )
    .await?;
    let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let result = snapshot
        .clone()
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if has_feature && !cfg!(feature = "checkpoint-protection") {
        let expected = "Feature 'checkpointProtection' is not supported";
        assert_result_error_with_message(result, expected);
        assert_result_error_with_message(
            snapshot.clone().create_checkpoint_writer(engine.as_ref()),
            expected,
        );
        assert_result_error_with_message(snapshot.checkpoint(engine.as_ref(), None), expected);
        return Ok(());
    }

    let mut txn = result?;
    let context = txn.write_state()?.write_context_builder().build()?;
    let data = ArrowEngineData::new(simple_id_batch(&schema, vec![1, 2, 3]));
    txn.add_files(engine.write_parquet(&data, &context).await?);
    let committed = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let (_, checkpointed) = committed.checkpoint(engine.as_ref(), None)?;
    checkpointed.write_checksum(engine.as_ref())?;
    test_read(&data, &table_url, engine.clone())?;

    let reloaded = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let altered = reloaded
        .update_table(
            UpdateTableOperation::AlterTable,
            "test-engine",
            false, /* data_change */
        )
        .add_column(StructField::nullable("note", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert!(altered.schema().field("note").is_some());
    Ok(())
}
