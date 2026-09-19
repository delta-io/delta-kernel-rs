//! Integration tests for change-data-feed aware write paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::{Int32Array, Int64Array, StringArray, StructArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::SchemaRef;
use delta_kernel::transaction::CommitResult;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{Engine as _, Snapshot, Version};
use rstest::rstest;
use tempfile::{tempdir, TempDir};
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    assert_result_error_with_message, begin_transaction, create_add_files_metadata, create_table,
    engine_store_setup, into_record_batch, load_and_begin_transaction,
};
use url::Url;

use crate::common::write_utils::{get_scan_files, get_simple_int_schema, resolve_struct_field};

// Helper function to create a table with CDF enabled
async fn create_cdf_table(
    table_name: &str,
    schema: SchemaRef,
) -> Result<(Url, Arc<DefaultEngine<TokioBackgroundExecutor>>, TempDir), Box<dyn std::error::Error>>
{
    let tmp_dir = tempdir()?;
    let tmp_test_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    let (store, engine, table_location) = engine_store_setup(table_name, Some(&tmp_test_dir_url));

    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true, // use protocol 3.7
        vec![],
        vec!["changeDataFeed"],
    )
    .await?;

    Ok((table_url, Arc::new(engine), tmp_dir))
}

// Helper function to write data to a table
async fn write_data_to_table(
    table_url: &Url,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<Version, Box<dyn std::error::Error>> {
    let mut txn =
        load_and_begin_transaction(table_url.clone(), engine.as_ref())?.with_engine_info("test");

    add_files_to_transaction(&mut txn, engine, schema, values).await?;

    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => Ok(committed.commit_version()),
        _ => panic!("Transaction should be committed"),
    }
}

// Helper function to add files to an existing transaction
async fn add_files_to_transaction(
    txn: &mut delta_kernel::transaction::Transaction,
    engine: &Arc<DefaultEngine<TokioBackgroundExecutor>>,
    schema: SchemaRef,
    values: Vec<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(values))],
    )?;

    let write_context = txn.write_state()?.write_context_builder().build()?;
    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(data), &write_context)
        .await?;
    txn.add_files(add_files_metadata);
    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_adds_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that add-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_adds", schema.clone()).await?;

    // Add files - this should succeed
    let version = write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;
    assert_eq!(version, 1);

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_all_removes_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that remove-only transactions work with CDF enabled
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_all_removes", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;

    // Now remove the files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("cdf remove test")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - remove-only transactions are allowed with CDF
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_no_data_change_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions work when dataChange=false.
    // It's allowed because the transaction does not contain any logical data changes.
    // This can happen when a table is being optimized/compacted.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_no_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files, but dataChange=false
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("cdf mixed test")
        .with_data_change(false); // dataChange=false is key here

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - mixed operations are allowed when dataChange=false
    let result = txn.commit(engine.as_ref())?;
    match result {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), 2);
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_cdf_write_mixed_with_data_change_fails() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that mixed add+remove transactions fail with helpful error when
    // dataChange=true
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    let (table_url, engine, _tmp_dir) =
        create_cdf_table("test_cdf_mixed_with_data_change", schema.clone()).await?;

    // First, add some data
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    // Now create a transaction with both add AND remove files with dataChange=true
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("cdf mixed fail test")
        .with_data_change(true); // dataChange=true - this should fail

    // Add new files
    add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    assert_result_error_with_message(
        txn.commit(engine.as_ref()),
        "Cannot add and remove data in the same transaction when Change Data Feed is enabled (delta.enableChangeDataFeed = true). \
         This would require writing CDC files for DML operations, which is not yet supported. \
         Consider using separate transactions: one to add files, another to remove files or update deletion vectors.",
    );

    Ok(())
}

#[rstest]
#[case::cdf_disabled_no_data_change(
    false, /* cdf_enabled */
    false, /* data_change */
    None
)]
#[case::cdf_disabled_data_change(
    false, /* cdf_enabled */
    true,  /* data_change */
    None
)]
#[case::cdf_enabled_no_data_change(
    true,  /* cdf_enabled */
    false, /* data_change */
    None
)]
#[case::cdf_enabled_data_change(
    true, /* cdf_enabled */
    true, /* data_change */
    Some("Cannot add and remove data in the same transaction")
)]
#[tokio::test]
async fn test_add_and_dv_update_fails_for_data_changing_cdf_transaction(
    #[case] cdf_enabled: bool,
    #[case] data_change: bool,
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = get_simple_int_schema();
    let (store, engine, table_location) = engine_store_setup(
        &format!("test_add_and_dv_update_{cdf_enabled}_{data_change}"),
        None, /* local_directory */
    );
    let mut writer_features = vec!["deletionVectors"];
    if cdf_enabled {
        writer_features.push("changeDataFeed");
    }
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],  /* partition_columns */
        true, /* use_37_protocol */
        vec!["deletionVectors"],
        writer_features,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut setup_txn = begin_transaction(snapshot, &engine)?;
    let existing_file = create_add_files_metadata(
        setup_txn.add_files_schema(),
        vec![(
            "existing.parquet",
            100,   /* size */
            1_000, /* modification_time */
            Some(3),
        )],
    )?;
    setup_txn.add_files(existing_file);
    let snapshot = setup_txn.commit(&engine)?.unwrap_post_commit_snapshot();

    let mut txn = begin_transaction(snapshot.clone(), &engine)?.with_data_change(data_change);
    let new_file = create_add_files_metadata(
        txn.add_files_schema(),
        vec![(
            "new.parquet",
            100,   /* size */
            2_000, /* modification_time */
            Some(1),
        )],
    )?;
    txn.add_files(new_file);
    let descriptor = DeletionVectorDescriptor::try_new(
        DeletionVectorStorageType::PersistedAbsolute,
        "memory:///dv.bin",
        Some(0), /* offset */
        1,       /* size_in_bytes */
        1,       /* cardinality */
    )?;
    txn.update_deletion_vectors(
        HashMap::from([("existing.parquet".to_string(), descriptor)]),
        get_scan_files(snapshot, &engine)?.into_iter().map(Ok),
    )?;

    let commit_result = txn.commit(&engine);
    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(commit_result, expected_error);
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        assert_eq!(snapshot.version(), 1);
    } else {
        let snapshot = commit_result?.unwrap_post_commit_snapshot();
        let mut active_files = 0;
        let mut existing_dv = None;
        for files in get_scan_files(snapshot.clone(), &engine)? {
            let batch = into_record_batch(files.apply_selection_vector()?);
            active_files += batch.num_rows();

            let batch = StructArray::from(batch);
            let paths: &StringArray = resolve_struct_field(&batch, &["path".into()]);
            if let Some(row) = paths
                .iter()
                .position(|path| path == Some("existing.parquet"))
            {
                let dv_paths: &StringArray = resolve_struct_field(
                    &batch,
                    &["deletionVector".into(), "pathOrInlineDv".into()],
                );
                let cardinalities: &Int64Array =
                    resolve_struct_field(&batch, &["deletionVector".into(), "cardinality".into()]);
                existing_dv = Some((dv_paths.value(row).to_string(), cardinalities.value(row)));
            }
        }

        assert_eq!(snapshot.version(), 2);
        assert_eq!(active_files, 2);
        assert_eq!(existing_dv, Some(("memory:///dv.bin".to_string(), 1)));
    }

    Ok(())
}

// ---- Concurrent-commit race regression (#3173) -----------------------------

/// Storage handler that forwards to the real handler, but writes one extra
/// commit into `_delta_log/` right after the FIRST listing of that directory —
/// exactly the interleaving of a concurrent committer landing between
/// `LogSegment::for_table_changes` and the end-snapshot build.
struct RacingCommitterStorage {
    inner: Arc<dyn delta_kernel::StorageHandler>,
    injected: std::sync::atomic::AtomicBool,
    commit_path: Url,
    commit_bytes: bytes::Bytes,
}

impl delta_kernel::StorageHandler for RacingCommitterStorage {
    fn list_from(
        &self,
        path: &Url,
    ) -> delta_kernel::DeltaResult<Box<dyn Iterator<Item = delta_kernel::DeltaResult<delta_kernel::FileMeta>>>>
    {
        // Capture the listing BEFORE injecting the commit so this listing
        // (the CDF log segment's) does not see the new version.
        let listing = self.inner.list_from(path)?;
        if !self.injected.swap(true, std::sync::atomic::Ordering::SeqCst)
            && path.as_str().contains("_delta_log")
        {
            self.inner
                .put(&self.commit_path, self.commit_bytes.clone(), false)
                .expect("inject racing commit");
        }
        Ok(listing)
    }

    fn read_files(
        &self,
        files: Vec<delta_kernel::FileSlice>,
    ) -> delta_kernel::DeltaResult<Box<dyn Iterator<Item = delta_kernel::DeltaResult<bytes::Bytes>>>>
    {
        self.inner.read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> delta_kernel::DeltaResult<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: bytes::Bytes, overwrite: bool) -> delta_kernel::DeltaResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> delta_kernel::DeltaResult<delta_kernel::FileMeta> {
        self.inner.head(path)
    }

    fn delete(&self, path: &Url) -> delta_kernel::DeltaResult<()> {
        self.inner.delete(path)
    }
}

/// Engine that answers every handler with the real engine's except storage,
/// which is wrapped in [`RacingCommitterStorage`].
struct RacingEngine {
    inner: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    storage: Arc<RacingCommitterStorage>,
}

impl delta_kernel::Engine for RacingEngine {
    fn evaluation_handler(&self) -> Arc<dyn delta_kernel::EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn delta_kernel::StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn delta_kernel::JsonHandler> {
        self.inner.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn delta_kernel::ParquetHandler> {
        self.inner.parquet_handler()
    }
}

/// A commit landing between the CDF log-segment listing and the end-snapshot
/// build must not change which version the change feed reads: the end snapshot
/// is pinned to the listed end version instead of re-resolving "latest".
#[tokio::test]
async fn cdf_end_snapshot_pinned_to_listed_end_version() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = get_simple_int_schema();
    let (table_url, engine, _tmp) = create_cdf_table("cdf-race", schema.clone()).await?;
    let v1 = write_data_to_table(&table_url, &engine, schema, vec![1, 2, 3]).await?;
    assert_eq!(v1, 1, "table should be at version 1 before the race");

    // Build the racing commit (v2): the table's own create metadata with the
    // partition columns flipped — a valid metaData action that changes how the
    // range would be interpreted if the end snapshot were to load it.
    let v0 = table_url.join("_delta_log/00000000000000000000.json")?;
    let v0_meta = engine.storage_handler().head(&v0)?;
    let v0_bytes = engine
        .storage_handler()
        .read_files(vec![(v0_meta.location, None)])?
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    // The create commit holds one action per line; keep the metaData line
    // and flip its partition columns.
    let v0_text = String::from_utf8(v0_bytes)?;
    let metadata_line = v0_text
        .lines()
        .find(|l| l.trim_start().starts_with("{\"metaData\""))
        .expect("create commit carries a metaData action");
    let mut metadata_json: serde_json::Value = serde_json::from_str(metadata_line)?;
    metadata_json["metaData"]["partitionColumns"] = serde_json::json!(["number"]);
    let commit_bytes = bytes::Bytes::from(serde_json::to_string(&metadata_json)?);

    let racing_storage = Arc::new(RacingCommitterStorage {
        inner: engine.storage_handler(),
        injected: std::sync::atomic::AtomicBool::new(false),
        commit_path: table_url.join("_delta_log/00000000000000000002.json")?,
        commit_bytes,
    });
    let racing_engine = RacingEngine {
        inner: engine.clone(),
        storage: racing_storage.clone(),
    };

    let table_changes = TableChanges::try_new(table_url.clone(), &racing_engine, 0, None)?;
    assert_eq!(
        table_changes.end_version(),
        1,
        "the CDF range must end at the version the log segment listed"
    );

    // Reading the feed to completion must work with the pinned end snapshot.
    let racing_engine = Arc::new(racing_engine);
    let scan = table_changes.into_scan_builder().build()?;
    let _ = scan
        .execute(racing_engine.clone())?
        .collect::<Result<Vec<_>, _>>()?;

    // The racing commit really did land before the snapshot build ran.
    assert!(
        racing_storage.injected.load(std::sync::atomic::Ordering::SeqCst),
        "the racing commit should have been injected during construction"
    );
    Ok(())
}
