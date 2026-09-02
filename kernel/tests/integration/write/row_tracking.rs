use std::sync::Arc;

use delta_kernel::arrow::array::{AsArray, Int32Array};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::{Int32Type, Int64Type};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{schema_ref, MetadataColumnSpec};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::RowTrackingMetadataColumns;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use test_utils::{
    assert_result_error_with_message, insert_data, into_record_batch, read_scan, test_table_setup,
    test_table_setup_mt,
};
use url::Url;

use crate::common::write_utils::set_table_properties;

/// `Transaction::commit` is rejected when it contains staged removeFiles and row
/// tracking is _supported_ and not _suspended_, which is broader than _enabled_.
#[rstest::rstest]
#[case::enabled(
    &[("delta.enableRowTracking", "true")],
    false, /* suspend_after_create */
    true,  /* expect_err */
)]
#[case::supported_only(
    &[("delta.feature.rowTracking", "supported")],
    false, /* suspend_after_create */
    true,  /* expect_err */
)]
#[case::supported_and_suspended(
    &[("delta.feature.rowTracking", "supported")],
    true,  /* suspend_after_create */
    false, /* expect_err */
)]
#[case::iceberg_compat_v3(
    // V3 auto-enables row tracking, so the gate fires.
    &[("delta.enableIcebergCompatV3", "true")],
    false, /* suspend_after_create */
    true,  /* expect_err */
)]
#[tokio::test(flavor = "multi_thread")]
async fn test_row_tracking_remove_gate(
    #[case] create_properties: &[(&str, &str)],
    #[case] suspend_after_create: bool,
    #[case] expect_err: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "number": INTEGER };
    let table_url = Url::from_directory_path(&table_path).unwrap();

    // v0: create table with the requested initial properties.
    kernel_create_table(table_path.as_str(), schema.clone(), "Test/1.0")
        .with_table_properties(create_properties.iter().copied())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Optional v1: inject a metadata-only commit that sets `delta.rowTrackingSuspended=true`.
    // kernel's create_table rejects this property at create time, so we set it via
    // the integration test hack here.
    let initial_snapshot = if suspend_after_create {
        set_table_properties(
            &table_path,
            &table_url,
            engine.as_ref(),
            0, /* current_version */
            &[("delta.rowTrackingSuspended", "true")],
        )?
    } else {
        Snapshot::builder_for(&table_path).build(engine.as_ref())?
    };

    // Insert a file.
    test_utils::insert_data(
        initial_snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_committed();

    // Remove the inserted file.
    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let scan = snapshot.clone().scan_builder().build()?;
    let scan_files = scan
        .scan_metadata(engine.as_ref())?
        .next()
        .unwrap()?
        .scan_files;
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_data_change(true);
    txn.remove_files(scan_files);

    if expect_err {
        let err = txn
            .commit(engine.as_ref())
            .expect_err("commit must fail when rowTracking is supported and not suspended");
        let msg = err.to_string();
        assert!(
            msg.contains("Remove actions are not yet supported") && msg.contains("rowTracking"),
            "expected remove-block error mentioning rowTracking, got: {msg}",
        );
    } else {
        txn.commit(engine.as_ref())?.unwrap_committed();
    }
    Ok(())
}

#[rstest::rstest]
#[case::supported(
    &[
        ("delta.enableRowTracking", "true"),
        ("delta.columnMapping.mode", "name"),
        ("delta.feature.icebergCompatV3", "supported"),
    ],
    None,
)]
#[case::enabled(
    &[("delta.enableIcebergCompatV3", "true")],
    Some(
        "Kernel does not support writing materialized Row IDs or Row Commit Versions to \
         IcebergCompatV3 tables",
    ),
)]
fn write_context_row_tracking_columns_respect_iceberg_compat_v3(
    #[case] table_properties: &[(&str, &str)],
    #[case] expected_error: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = kernel_create_table(
        table_path.as_str(),
        schema_ref! { nullable "number": INTEGER },
        "Test/1.0",
    )
    .with_table_properties(table_properties.iter().copied())
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let result = txn
        .write_state()?
        .write_context_builder()
        .with_row_tracking_columns(RowTrackingMetadataColumns {
            row_id_col_name: Some("row_id"),
            row_commit_version_col_name: Some("row_commit_version"),
        })
        .build();

    if let Some(expected_error) = expected_error {
        assert_result_error_with_message(result, expected_error);
    } else {
        result?;
    }
    Ok(())
}

#[rstest::rstest]
#[tokio::test(flavor = "multi_thread")]
async fn write_context_maps_row_tracking_metadata_to_physical(
    #[values("none", "name", "id")] column_mapping_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // === Create table and insert data ===
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "number": INTEGER };
    let snapshot = kernel_create_table(table_path.as_str(), schema, "Test/1.0")
        .with_table_properties([
            ("delta.columnMapping.mode", column_mapping_mode),
            ("delta.enableRowTracking", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let snapshot = insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![10, 20]))],
    )
    .await?
    .unwrap_post_commit_snapshot();
    let source_snapshot = insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![30, 40, 50]))],
    )
    .await?
    .unwrap_post_commit_snapshot();

    // === Read data with row tracking ===
    let read_with_row_tracking = |snapshot: Arc<Snapshot>| -> DeltaResult<Vec<RecordBatch>> {
        let scan_schema = Arc::new(
            snapshot
                .schema()
                .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
                .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?,
        );
        let scan = snapshot.scan_builder().with_schema(scan_schema).build()?;
        read_scan(&scan, engine.clone())
    };
    let source_batches = read_with_row_tracking(source_snapshot.clone())?;

    // === Merge the batches ===
    let merged_batch = concat_batches(&source_batches[0].schema(), &source_batches)?;

    // === Assert merged logical data ===
    let expected_rows = vec![(10, 0, 1), (20, 1, 1), (30, 2, 2), (40, 3, 2), (50, 4, 2)];
    assert_eq!(
        collect_rows(
            std::slice::from_ref(&merged_batch),
            "number",
            "row_id",
            "row_commit_version",
        ),
        expected_rows
    );

    // === Build the write context ===
    let txn = source_snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let write_context = txn
        .write_state()?
        .write_context_builder()
        .with_row_tracking_columns(RowTrackingMetadataColumns {
            row_id_col_name: Some("row_id"),
            row_commit_version_col_name: Some("row_commit_version"),
        })
        .build()?;

    // === Evaluate logical-to-physical ===
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        write_context.logical_data_schema().clone(),
        write_context.logical_to_physical(),
        write_context.physical_data_schema().clone().into(),
    )?;
    let physical_batch =
        into_record_batch(evaluator.evaluate(&ArrowEngineData::new(merged_batch))?);

    // === Assert physical schema and data ===
    let physical_row_id_name = source_snapshot
        .table_properties()
        .materialized_row_id_column_name
        .as_deref()
        .expect("row-tracking table must have a materialized Row ID column name");
    let physical_row_commit_version_name = source_snapshot
        .table_properties()
        .materialized_row_commit_version_column_name
        .as_deref()
        .expect("row-tracking table must have a materialized Row Commit Version column name");
    let expected_physical_names: Vec<_> = write_context
        .physical_data_schema()
        .fields()
        .map(|field| field.name.as_str())
        .collect();
    let physical_arrow_schema = physical_batch.schema();
    let actual_physical_names: Vec<_> = physical_arrow_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(actual_physical_names, expected_physical_names);
    assert_eq!(
        expected_physical_names[1..],
        [physical_row_id_name, physical_row_commit_version_name]
    );
    let physical_rows = collect_rows(
        std::slice::from_ref(&physical_batch),
        expected_physical_names[0],
        physical_row_id_name,
        physical_row_commit_version_name,
    );
    assert_eq!(physical_rows, expected_rows);

    Ok(())
}

fn collect_rows(
    batches: &[RecordBatch],
    number_name: &str,
    row_id_name: &str,
    version_name: &str,
) -> Vec<(i32, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let numbers = batch
            .column_by_name(number_name)
            .unwrap()
            .as_primitive::<Int32Type>();
        let row_ids = batch
            .column_by_name(row_id_name)
            .unwrap()
            .as_primitive::<Int64Type>();
        let row_commit_versions = batch
            .column_by_name(version_name)
            .unwrap()
            .as_primitive::<Int64Type>();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                numbers.value(row),
                row_ids.value(row),
                row_commit_versions.value(row),
            )
        }));
    }
    rows.sort_unstable();
    rows
}
