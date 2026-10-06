//! Integration tests for change-data-feed aware write paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
use delta_kernel::arrow::array::{Int32Array, Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::schema::SchemaRef;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{Snapshot, Version};
use rstest::rstest;
use tempfile::{tempdir, TempDir};
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    assert_result_error_with_message, begin_transaction, create_add_files_metadata, create_table,
    engine_store_setup, into_record_batch, load_and_begin_transaction, read_add_infos, read_scan,
    test_table_setup,
};
use url::Url;

use crate::common::write_utils::{
    get_scan_files, get_simple_int_schema, write_deletion_vector_to_store,
};

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
        CommitResult::Committed(committed) => Ok(committed.commit_version()),
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
        CommitResult::Committed(committed) => {
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

    // Rewrite the same logical rows; dataChange=false must preserve the data.
    add_files_to_transaction(&mut txn, &engine, schema, vec![1, 2, 3]).await?;

    // Also remove existing files
    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

    // This should succeed - mixed operations are allowed when dataChange=false
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    assert_eq!(snapshot.version(), 2);
    assert_table_and_cdf_rows(snapshot, engine, &[1, 2, 3], &[])?;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NoOpFileAction {
    None,
    Add,
    EmptyRemove,
    Remove,
    DeletionVector,
}

impl NoOpFileAction {
    fn stage(
        self,
        txn: &mut delta_kernel::transaction::Transaction,
        snapshot: Arc<Snapshot>,
        engine: &DefaultEngine<TokioBackgroundExecutor>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::None => {}
            Self::Add => txn.add_files(create_add_files_metadata(txn.add_files_schema(), vec![])?),
            Self::EmptyRemove | Self::Remove => {
                for files in get_scan_files(snapshot, engine)? {
                    let (data, _) = files.into_parts();
                    let files = if self == Self::EmptyRemove {
                        let batch = into_record_batch(data).slice(0, 0);
                        FilteredEngineData::with_all_rows_selected(Box::new(ArrowEngineData::new(
                            batch,
                        )))
                    } else {
                        let selection = vec![false; data.len()];
                        FilteredEngineData::try_new(data, selection)?
                    };
                    txn.remove_files(files);
                }
            }
            Self::DeletionVector => txn.update_deletion_vectors(
                HashMap::new(),
                get_scan_files(snapshot, engine)?.into_iter().map(Ok),
            )?,
        }
        Ok(())
    }
}

#[rstest]
#[case::absent(NoOpFileAction::None, false)]
#[case::empty_add(NoOpFileAction::Add, false)]
#[case::empty_remove(NoOpFileAction::EmptyRemove, false)]
#[case::unselected_remove(NoOpFileAction::Remove, false)]
#[case::unmatched_dv(NoOpFileAction::DeletionVector, false)]
#[case::blind_append_absent(NoOpFileAction::None, true)]
#[case::blind_append_empty_add(NoOpFileAction::Add, true)]
#[case::blind_append_empty_remove(NoOpFileAction::EmptyRemove, true)]
#[case::blind_append_unselected_remove(NoOpFileAction::Remove, true)]
#[case::blind_append_unmatched_dv(NoOpFileAction::DeletionVector, true)]
#[tokio::test]
async fn test_cdf_data_change_ignores_noop_file_actions(
    #[case] no_op_action: NoOpFileAction,
    #[case] blind_append: bool,
    #[values(false, true)] with_real_action: bool,
    #[values(false, true)] no_op_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let schema = get_simple_int_schema();
    kernel_create_table(&table_path, schema.clone(), "test")
        .with_table_properties([
            ("delta.enableChangeDataFeed", "true"),
            ("delta.enableDeletionVectors", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?.with_data_change(true);
    if blind_append {
        txn = txn.with_blind_append();
    }

    if no_op_first {
        no_op_action.stage(&mut txn, snapshot.clone(), engine.as_ref())?;
    }
    if with_real_action {
        if no_op_action == NoOpFileAction::Add && !blind_append {
            for files in get_scan_files(snapshot.clone(), engine.as_ref())? {
                txn.remove_files(files);
            }
        } else {
            add_files_to_transaction(&mut txn, &engine, schema, vec![4, 5, 6]).await?;
        }
    }
    if !no_op_first {
        no_op_action.stage(&mut txn, snapshot, engine.as_ref())?;
    }

    let result = txn.commit(engine.as_ref());
    if blind_append && !with_real_action {
        assert_result_error_with_message(
            result,
            "Blind append requires at least one added data file",
        );
        return Ok(());
    }
    let snapshot = result?.unwrap_post_commit_snapshot();
    assert_eq!(snapshot.version(), 2);
    let (expected_rows, expected_changes): (&[i32], &[(i32, &str)]) = if !with_real_action {
        (&[1, 2, 3], &[])
    } else if no_op_action == NoOpFileAction::Add && !blind_append {
        (&[], &[(1, "delete"), (2, "delete"), (3, "delete")])
    } else {
        (
            &[1, 2, 3, 4, 5, 6],
            &[(4, "insert"), (5, "insert"), (6, "insert")],
        )
    };
    assert_table_and_cdf_rows(snapshot, engine, expected_rows, expected_changes)?;
    Ok(())
}

fn assert_table_and_cdf_rows(
    snapshot: Arc<Snapshot>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    expected_rows: &[i32],
    expected_changes: &[(i32, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let scan = snapshot.clone().scan_builder().build()?;
    let mut rows = Vec::new();
    for batch in read_scan(&scan, engine.clone())? {
        let numbers = batch
            .column_by_name("number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend(numbers.values().iter().copied());
    }
    rows.sort_unstable();
    assert_eq!(rows, expected_rows);

    let version = snapshot.version();
    let changes = TableChanges::try_new(
        snapshot.table_root().clone(),
        engine.as_ref(),
        version,
        Some(version),
    )?;
    let schema = changes
        .schema()
        .project(&["number", "_change_type", "_commit_version"])?;
    let scan = changes.into_scan_builder().with_schema(schema).build()?;
    let mut actual_changes = Vec::new();
    for data in scan.execute(engine)? {
        let batch = into_record_batch(data?);
        let numbers = batch
            .column_by_name("number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let types = batch
            .column_by_name("_change_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column_by_name("_commit_version")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(versions.value(row), version as i64);
            actual_changes.push((numbers.value(row), types.value(row).to_owned()));
        }
    }
    actual_changes.sort_unstable();
    let expected_changes: Vec<_> = expected_changes
        .iter()
        .map(|(value, kind)| (*value, (*kind).to_owned()))
        .collect();
    assert_eq!(actual_changes, expected_changes);
    Ok(())
}

#[rstest]
#[case::cdf_disabled_no_data_change(
    false, /* cdf_enabled */
    false, /* data_change */
    None,
    true /* real_add */
)]
#[case::cdf_disabled_data_change(
    false, /* cdf_enabled */
    true,  /* data_change */
    None,
    true /* real_add */
)]
#[case::cdf_enabled_no_data_change(
    true,  /* cdf_enabled */
    false, /* data_change */
    None,
    true /* real_add */
)]
#[case::cdf_enabled_data_change(
    true, /* cdf_enabled */
    true, /* data_change */
    Some("Cannot add and remove data in the same transaction"),
    true /* real_add */
)]
#[case::cdf_enabled_empty_add(
    true, /* cdf_enabled */
    true, /* data_change */
    None,
    false /* real_add */
)]
#[tokio::test]
async fn test_add_and_dv_update_fails_for_data_changing_cdf_transaction(
    #[case] cdf_enabled: bool,
    #[case] data_change: bool,
    #[case] expected_error: Option<&str>,
    #[case] real_add: bool,
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
        store.clone(),
        table_location,
        schema.clone(),
        &[],  /* partition_columns */
        true, /* use_37_protocol */
        vec!["deletionVectors"],
        writer_features,
    )
    .await?;

    let engine = Arc::new(engine);
    write_data_to_table(&table_url, &engine, schema.clone(), vec![1, 2, 3]).await?;
    let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
    let mut txn =
        begin_transaction(snapshot.clone(), engine.as_ref())?.with_data_change(data_change);
    if real_add {
        let values = if data_change { vec![4, 5, 6] } else { vec![1] };
        add_files_to_transaction(&mut txn, &engine, schema, values).await?;
    } else {
        txn.add_files(create_add_files_metadata(txn.add_files_schema(), vec![])?);
    }
    let context = txn.write_state()?.write_context_builder().build()?;
    let mut dv = KernelDeletionVector::new();
    dv.add_deleted_row_indexes([0]);
    let descriptor = write_deletion_vector_to_store(&store, &context, dv, "").await?;
    let file_path = read_add_infos(snapshot.as_ref(), engine.as_ref())?[0]
        .path
        .clone();
    txn.update_deletion_vectors(
        HashMap::from([(file_path, descriptor)]),
        get_scan_files(snapshot, engine.as_ref())?
            .into_iter()
            .map(Ok),
    )?;

    let commit_result = txn.commit(engine.as_ref());
    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(commit_result, expected_error);
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
        assert_eq!(snapshot.version(), 1);
    } else {
        let snapshot = commit_result?.unwrap_post_commit_snapshot();
        assert_eq!(snapshot.version(), 2);
        let expected_rows: &[i32] = if !data_change {
            &[1, 2, 3]
        } else if real_add {
            &[2, 3, 4, 5, 6]
        } else {
            &[2, 3]
        };
        if cdf_enabled {
            let expected_changes: &[(i32, &str)] = if data_change { &[(1, "delete")] } else { &[] };
            assert_table_and_cdf_rows(snapshot, engine, expected_rows, expected_changes)?;
        } else {
            let scan = snapshot.scan_builder().build()?;
            let mut rows = Vec::new();
            for batch in read_scan(&scan, engine)? {
                let numbers = batch
                    .column_by_name("number")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                rows.extend(numbers.values().iter().copied());
            }
            rows.sort_unstable();
            assert_eq!(rows, expected_rows);
        }
    }

    Ok(())
}
