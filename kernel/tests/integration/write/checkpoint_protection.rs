use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelResult, Snapshot};
use test_utils::{create_table, test_read, test_table_setup_mt};
use url::Url;

use crate::common::write_utils::{get_simple_schema, simple_id_batch};

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_protection_allows_writes() -> delta_kernel::Result<()> {
    let schema = get_simple_schema();
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    create_checkpoint_protection_table(&table_url).await?;
    let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = txn.write_state()?.write_context_builder().build()?;
    let data = ArrowEngineData::new(simple_id_batch(&schema, vec![1, 2, 3]));
    txn.add_files(engine.write_parquet(&data, &context).await?);
    let committed = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let (_, checkpointed) = committed.checkpoint(engine.as_ref(), None /* spec */)?;
    checkpointed.write_checksum(engine.as_ref())?;
    test_read(&data, &table_url, engine.clone())?;

    Ok(())
}

async fn create_checkpoint_protection_table(table_url: &Url) -> KernelResult<()> {
    create_table(
        Arc::new(LocalFileSystem::new()),
        table_url.clone(),
        get_simple_schema(),
        &[],    /* partition_columns */
        true,   /* use_37_protocol */
        vec![], /* reader_features */
        vec!["checkpointProtection"],
    )
    .await?;
    Ok(())
}
