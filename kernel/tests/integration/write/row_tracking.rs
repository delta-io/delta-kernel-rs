use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::Snapshot;
use test_utils::test_table_setup_mt;
use url::Url;

use crate::common::write_utils::set_table_properties;

/// The remove-block gate fires whenever row tracking is _supported_ and not _suspended_,
/// which is broader than _enabled_.
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
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);
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
