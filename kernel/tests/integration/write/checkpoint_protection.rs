use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField};
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelError, KernelResult, Snapshot};
use rstest::rstest;
use test_utils::{
    assert_result_error_with_message, create_add_files_metadata, create_table, engine_store_setup,
    test_read, test_table_setup_mt,
};
use url::Url;

use crate::common::write_utils::{get_scan_files, get_simple_schema, simple_id_batch};

#[rstest]
#[tokio::test]
async fn checkpoint_protection_requires_ack_for_write_state_and_commits(
    #[values(false, true)] has_feature: bool,
    #[values(false, true)] acknowledge: bool,
    #[values(false, true)] add_file: bool,
) -> KernelResult<()> {
    let (store, engine, table_url) = engine_store_setup("checkpoint_protection_ack", None);
    let writer_features = if has_feature {
        vec!["checkpointProtection"]
    } else {
        vec![]
    };
    // The create-table builder cannot enable checkpointProtection.
    create_table(
        store,
        table_url.clone(),
        get_simple_schema(),
        &[],    /* partition_columns */
        true,   /* use_37_protocol */
        vec![], /* reader_features */
        writer_features,
    )
    .await?;
    let snapshot = Snapshot::builder_for(&table_url).build(&engine)?;
    let mut txn = snapshot
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    if acknowledge {
        txn.ack_checkpoint_protection_requirements();
    }
    if has_feature && !acknowledge {
        assert_result_error_with_message(
            txn.write_state(),
            "ack_checkpoint_protection_requirements",
        );
    } else {
        txn.write_state()?;
    }

    // Staging files directly must not bypass the acknowledgment required at commit.
    if add_file {
        txn.add_files(create_add_files_metadata(
            txn.add_files_schema(),
            vec![("part-00000.parquet", 1024, 1_000_000, Some(1))],
        )?);
    }
    let result = txn.commit(&engine);
    if has_feature && !acknowledge {
        assert_result_error_with_message(result, "ack_checkpoint_protection_requirements");
        assert_eq!(
            Snapshot::builder_for(&table_url).build(&engine)?.version(),
            0
        );
        return Ok(());
    }

    let committed = result?.unwrap_post_commit_snapshot();
    assert_eq!(committed.version(), 1);
    let files = get_scan_files(committed.clone(), &engine)?;
    let file_count = files.into_iter().try_fold(0, |count, batch| {
        Ok::<_, KernelError>(count + batch.apply_selection_vector()?.len())
    })?;
    assert_eq!(file_count, usize::from(add_file));

    let next_txn = committed
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    if has_feature {
        assert_result_error_with_message(
            next_txn.write_state(),
            "ack_checkpoint_protection_requirements",
        );
        assert_result_error_with_message(
            next_txn.commit(&engine),
            "ack_checkpoint_protection_requirements",
        );
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_protection_write_checkpoint_checksum_and_read_round_trip(
    #[values(false, true)] acknowledge_alter: bool,
) -> KernelResult<()> {
    let schema = get_simple_schema();
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    create_table(
        Arc::new(LocalFileSystem::new()),
        table_url.clone(),
        schema.clone(),
        &[],    /* partition_columns */
        true,   /* use_37_protocol */
        vec![], /* reader_features */
        vec!["checkpointProtection"],
    )
    .await?;
    let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .update_table(
            UpdateTableOperation::Write,
            "test-engine",
            true, /* data_change */
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    txn.ack_checkpoint_protection_requirements();
    let context = txn.write_state()?.write_context_builder().build()?;
    let data = ArrowEngineData::new(simple_id_batch(&schema, vec![1, 2, 3]));
    txn.add_files(engine.write_parquet(&data, &context).await?);
    let committed = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let (_, checkpointed) = committed.checkpoint(engine.as_ref(), None)?;
    checkpointed.write_checksum(engine.as_ref())?;
    test_read(&data, &table_url, engine.clone())?;

    let reloaded = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let mut alter = reloaded
        .update_table(
            UpdateTableOperation::AlterTable,
            "test-engine",
            false, /* data_change */
        )
        .add_column(StructField::nullable("note", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if acknowledge_alter {
        alter.ack_checkpoint_protection_requirements();
        let altered = alter.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
        assert!(altered.schema().field("note").is_some());
    } else {
        assert_result_error_with_message(
            alter.commit(engine.as_ref()),
            "ack_checkpoint_protection_requirements",
        );
    }
    Ok(())
}
