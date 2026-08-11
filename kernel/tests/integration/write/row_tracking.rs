use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Int32Type, Int64Type, Schema as ArrowSchema,
};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{schema_ref, MetadataColumnSpec};
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use test_utils::{
    insert_data_with, read_actions_from_commit, read_scan, test_table_setup_mt,
    TestCatalogCommitter,
};
use url::Url;

use crate::common::write_utils::set_table_properties;

fn read_stable_values(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn Engine>,
) -> DeltaResult<HashMap<i32, (i64, i64)>> {
    let scan_schema = Arc::new(
        snapshot
            .schema()
            .add_metadata_column("row_id", MetadataColumnSpec::RowId)?
            .add_metadata_column("row_commit_version", MetadataColumnSpec::RowCommitVersion)?,
    );
    let scan = snapshot.scan_builder().with_schema(scan_schema).build()?;
    let mut values = HashMap::new();
    for batch in read_scan(&scan, engine)? {
        let numbers = batch
            .column_by_name("number")
            .expect("number column")
            .as_primitive::<Int32Type>();
        let row_ids = batch
            .column_by_name("row_id")
            .expect("row ID metadata column")
            .as_primitive::<Int64Type>();
        let row_commit_versions = batch
            .column_by_name("row_commit_version")
            .expect("row commit version metadata column")
            .as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            values.insert(
                numbers.value(row),
                (row_ids.value(row), row_commit_versions.value(row)),
            );
        }
    }
    Ok(values)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InvalidPreservingBatch {
    MissingRowId,
    MissingRowCommitVersion,
    NullRowId,
    NullRowCommitVersion,
    WrongRowIdType,
}

#[rstest::rstest]
#[case::missing_row_id(InvalidPreservingBatch::MissingRowId, "missing configured field")]
#[case::missing_row_commit_version(
    InvalidPreservingBatch::MissingRowCommitVersion,
    "missing configured field"
)]
#[case::null_row_id(InvalidPreservingBatch::NullRowId, "contains null values")]
#[case::null_row_commit_version(
    InvalidPreservingBatch::NullRowCommitVersion,
    "contains null values"
)]
#[case::wrong_row_id_type(InvalidPreservingBatch::WrongRowIdType, "must have type Int64")]
#[tokio::test(flavor = "multi_thread")]
async fn test_preserving_writer_rejects_missing_or_null_stable_values(
    #[case] invalid_batch: InvalidPreservingBatch,
    #[case] expected_error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "number": INTEGER };
    let snapshot = kernel_create_table(table_path.as_str(), schema, "Test/1.0")
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let write_context = txn.unpartitioned_write_context()?;
    let row_id_field = write_context
        .materialized_row_id_field()
        .expect("row tracking write context must expose row ID field");
    let row_commit_version_field = write_context
        .materialized_row_commit_version_field()
        .expect("row tracking write context must expose row commit version field");

    let mut fields = vec![ArrowField::new("number", ArrowDataType::Int32, true)];
    let mut columns = vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef];
    if invalid_batch != InvalidPreservingBatch::MissingRowId {
        if invalid_batch == InvalidPreservingBatch::WrongRowIdType {
            fields.push(ArrowField::new(
                row_id_field.name(),
                ArrowDataType::Int32,
                true,
            ));
            columns.push(Arc::new(Int32Array::from(vec![0, 1])));
        } else {
            fields.push(row_id_field.try_into_arrow()?);
            let values = match invalid_batch {
                InvalidPreservingBatch::NullRowId => vec![Some(0), None],
                _ => vec![Some(0), Some(1)],
            };
            columns.push(Arc::new(Int64Array::from(values)));
        }
    }
    if invalid_batch != InvalidPreservingBatch::MissingRowCommitVersion {
        fields.push(row_commit_version_field.try_into_arrow()?);
        let values = match invalid_batch {
            InvalidPreservingBatch::NullRowCommitVersion => vec![Some(0), None],
            _ => vec![Some(0), Some(0)],
        };
        columns.push(Arc::new(Int64Array::from(values)));
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
    let result = engine
        .write_parquet_preserving_row_tracking(&ArrowEngineData::new(batch), &write_context)
        .await;
    let error = result.err().expect("invalid stable values must fail");
    assert!(
        error.to_string().contains(expected_error),
        "unexpected error: {error}"
    );
    Ok(())
}

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum RewriteConfig {
    Valid,
    DataChange,
    Disabled,
    Suspended,
    CatalogManaged,
    IcebergV3,
    DvUpdate,
}

impl RewriteConfig {
    fn table_properties(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::CatalogManaged => &[
                ("delta.enableRowTracking", "true"),
                ("delta.feature.catalogManaged", "supported"),
                ("delta.feature.vacuumProtocolCheck", "supported"),
                ("io.unitycatalog.tableId", "row-tracking-rewrite-test"),
            ],
            Self::IcebergV3 => &[("delta.enableIcebergCompatV3", "true")],
            Self::DvUpdate => &[
                ("delta.enableRowTracking", "true"),
                ("delta.enableDeletionVectors", "true"),
            ],
            _ => &[("delta.enableRowTracking", "true")],
        }
    }

    fn committer(self) -> Box<dyn Committer> {
        if self == Self::CatalogManaged {
            Box::new(TestCatalogCommitter)
        } else {
            Box::new(FileSystemCommitter::new())
        }
    }
}

fn collect_scan_path(paths: &mut Vec<String>, scan_file: delta_kernel::scan::state::ScanFile) {
    paths.push(scan_file.path);
}

#[rstest::rstest]
#[case::valid(RewriteConfig::Valid, None)]
#[case::data_change_true(RewriteConfig::DataChange, Some("dataChange=false"))]
#[case::disabled(RewriteConfig::Disabled, Some("enabled and not suspended"))]
#[case::suspended(RewriteConfig::Suspended, Some("enabled and not suspended"))]
#[case::catalog_managed(RewriteConfig::CatalogManaged, Some("catalog-managed"))]
#[case::iceberg_v3(RewriteConfig::IcebergV3, Some("icebergCompatV3"))]
#[case::dv_update(RewriteConfig::DvUpdate, Some("deletion-vector updates"))]
#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledged_row_tracking_rewrite(
    #[case] config: RewriteConfig,
    #[case] expected_error: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "number": INTEGER };
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let created = kernel_create_table(table_path.as_str(), schema.clone(), "Test/1.0")
        .with_table_properties(config.table_properties().iter().copied())
        .build(engine.as_ref(), config.committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let source_snapshot = insert_data_with(
        created,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        config.committer(),
        "WRITE",
        true,
        false,
    )
    .await?
    .unwrap_post_commit_snapshot();

    let stable_values_before = read_stable_values(source_snapshot.clone(), engine.clone())?;
    let snapshot = match config {
        RewriteConfig::Disabled => set_table_properties(
            &table_path,
            &table_url,
            engine.as_ref(),
            source_snapshot.version(),
            &[("delta.enableRowTracking", "false")],
        )?,
        RewriteConfig::Suspended => set_table_properties(
            &table_path,
            &table_url,
            engine.as_ref(),
            source_snapshot.version(),
            &[("delta.rowTrackingSuspended", "true")],
        )?,
        _ => source_snapshot,
    };
    let scan_metadata = snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine.as_ref())?
        .next()
        .expect("source file scan metadata")?;
    let source_paths = scan_metadata.visit_scan_files(Vec::new(), collect_scan_path)?;
    let scan_files = scan_metadata.scan_files;
    let mut txn = snapshot
        .transaction(config.committer(), engine.as_ref())?
        .with_data_change(config == RewriteConfig::DataChange);
    let write_context = txn.unpartitioned_write_context()?;
    let add_metadata = if matches!(config, RewriteConfig::Valid | RewriteConfig::DataChange) {
        let row_id_field = write_context
            .materialized_row_id_field()
            .expect("row tracking write context must expose row ID field");
        let row_commit_version_field = write_context
            .materialized_row_commit_version_field()
            .expect("row tracking write context must expose row commit version field");
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("number", ArrowDataType::Int32, true),
            row_id_field.try_into_arrow()?,
            row_commit_version_field.try_into_arrow()?,
        ]));
        let replacement_batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![
                    stable_values_before[&1].0,
                    stable_values_before[&2].0,
                    stable_values_before[&3].0,
                ])),
                Arc::new(Int64Array::from(vec![
                    stable_values_before[&1].1,
                    stable_values_before[&2].1,
                    stable_values_before[&3].1,
                ])),
            ],
        )?;
        engine
            .write_parquet_preserving_row_tracking(
                &ArrowEngineData::new(replacement_batch),
                &write_context,
            )
            .await?
    } else {
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
        let replacement_batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        engine
            .write_parquet(&ArrowEngineData::new(replacement_batch), &write_context)
            .await?
    };
    txn.add_files(add_metadata);
    if config == RewriteConfig::DvUpdate {
        let descriptor = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "abc",
            None,
            3,
            1,
        )?;
        txn.update_deletion_vectors(
            HashMap::from([(source_paths[0].clone(), descriptor)]),
            std::iter::once(Ok(scan_files)),
        )?;
    } else {
        txn.remove_files(scan_files);
    }
    txn.ack_row_tracking_preservation();

    if let Some(expected_error) = expected_error {
        let error = txn
            .commit(engine.as_ref())
            .expect_err("malformed acknowledged rewrite must fail");
        assert!(
            error.to_string().contains(expected_error),
            "unexpected error: {error}"
        );
        return Ok(());
    }

    let committed = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    assert_eq!(committed.version(), 2);
    assert_eq!(
        read_stable_values(committed, engine.clone())?,
        stable_values_before
    );

    let adds = read_actions_from_commit(&table_url, 2, "add")?;
    assert_eq!(adds.len(), 1);
    assert_eq!(adds[0]["dataChange"], false);
    assert_eq!(adds[0]["baseRowId"], 3);
    assert_eq!(adds[0]["defaultRowCommitVersion"], 2);
    assert!(adds[0]["deletionVector"].is_null());

    let removes = read_actions_from_commit(&table_url, 2, "remove")?;
    assert_eq!(removes.len(), 1);
    assert_eq!(removes[0]["dataChange"], false);
    assert_eq!(removes[0]["baseRowId"], 0);
    assert_eq!(removes[0]["defaultRowCommitVersion"], 1);

    let domains = read_actions_from_commit(&table_url, 2, "domainMetadata")?;
    let row_tracking_domain = domains
        .iter()
        .find(|domain| domain["domain"] == "delta.rowTracking")
        .expect("row tracking domain metadata");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            row_tracking_domain["configuration"]
                .as_str()
                .expect("domain configuration string")
        )?["rowIdHighWaterMark"],
        5
    );

    let commit_info = read_actions_from_commit(&table_url, 2, "commitInfo")?;
    assert_eq!(
        commit_info[0]["tags"]["delta.rowTracking.preserved"],
        "true"
    );
    Ok(())
}
