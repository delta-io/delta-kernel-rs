use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
#[cfg(feature = "checkpoint-protection")]
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelResult, Snapshot};
#[cfg(not(feature = "checkpoint-protection"))]
use test_utils::assert_result_error_with_message;
#[cfg(feature = "checkpoint-protection")]
use test_utils::test_read;
use test_utils::{create_table, test_table_setup_mt};
use url::Url;

use crate::common::write_utils::get_simple_schema;
#[cfg(feature = "checkpoint-protection")]
use crate::common::write_utils::simple_id_batch;

#[cfg(feature = "checkpoint-protection")]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_protection_with_feature_allows_writes() -> KernelResult<()> {
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

#[cfg(not(feature = "checkpoint-protection"))]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_protection_without_feature_blocks_writes() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    create_checkpoint_protection_table(&table_url).await?;
    let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let scan = snapshot.clone().scan_builder().build()?;
    assert!(scan.execute(engine.clone())?.next().is_none());

    let result = snapshot
        .clone()
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    let expected = "Feature 'checkpointProtection' is not supported";
    assert_result_error_with_message(result, expected);
    assert_result_error_with_message(
        snapshot.clone().create_checkpoint_writer(engine.as_ref()),
        expected,
    );
    assert_result_error_with_message(
        snapshot.checkpoint(engine.as_ref(), None /* spec */),
        expected,
    );
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
