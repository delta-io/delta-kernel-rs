use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::{AsArray, Int32Array, Int64Array, StringArray};
use delta_kernel::arrow::datatypes::{Int32Type, Int64Type};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{schema_ref, MetadataColumnSpec, StructField};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::WriteMode;
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

#[rstest::rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_preserving_write_context_transforms_complete_input(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    column_mapping_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! {
        nullable "number": INTEGER,
        nullable "label": STRING,
    };
    let column_mapping_mode = match column_mapping_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Name => "name",
        ColumnMappingMode::Id => "id",
    };
    let snapshot = kernel_create_table(table_path.as_str(), schema, "Test/1.0")
        .with_table_properties([
            ("delta.enableRowTracking", "true"),
            ("delta.columnMapping.mode", column_mapping_mode),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let write_context =
        txn.unpartitioned_write_context_with_input(WriteMode::PreserveRowTracking {
            row_id_column: "stable_row_id",
            row_commit_version_column: "stable_row_commit_version",
        })?;
    let logical_data_schema = write_context.logical_data_schema().clone();
    assert_eq!(
        logical_data_schema
            .metadata_column(&MetadataColumnSpec::RowId)
            .map(|field| field.name().as_str()),
        Some("stable_row_id")
    );
    assert_eq!(
        logical_data_schema
            .metadata_column(&MetadataColumnSpec::RowCommitVersion)
            .map(|field| field.name().as_str()),
        Some("stable_row_commit_version")
    );

    let row_id_index = write_context.physical_data_schema().num_fields() - 2;
    let row_id_field = write_context
        .physical_data_schema()
        .field_at_index(row_id_index)
        .expect("output row ID field");
    let row_commit_version_field = write_context
        .physical_data_schema()
        .field_at_index(row_id_index + 1)
        .expect("output row commit version field");
    assert_eq!(
        Some(row_id_field.name()),
        write_context
            .materialized_row_id_field()
            .map(StructField::name)
    );
    assert_eq!(
        Some(row_commit_version_field.name()),
        write_context
            .materialized_row_commit_version_field()
            .map(StructField::name)
    );
    assert!(!row_id_field.is_nullable());
    assert!(!row_commit_version_field.is_nullable());

    let input = RecordBatch::try_new(
        Arc::new(logical_data_schema.as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["value"])),
            Arc::new(Int64Array::from(vec![101])),
            Arc::new(Int64Array::from(vec![11])),
        ],
    )?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        logical_data_schema,
        write_context.logical_to_physical(),
        write_context.physical_data_schema().clone().into(),
    )?;
    let output =
        ArrowEngineData::try_from_engine_data(evaluator.evaluate(&ArrowEngineData::new(input))?)?;
    let output = output.record_batch();
    let expected_names = write_context
        .physical_data_schema()
        .fields()
        .map(StructField::name)
        .collect::<Vec<_>>();
    let output_arrow_schema = output.schema();
    let actual_names = output_arrow_schema
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();
    assert_eq!(actual_names, expected_names);
    assert_eq!(output.column(0).as_primitive::<Int32Type>().value(0), 7);
    assert_eq!(output.column(1).as_string::<i32>().value(0), "value");
    assert_eq!(output.column(2).as_primitive::<Int64Type>().value(0), 101);
    assert_eq!(output.column(3).as_primitive::<Int64Type>().value(0), 11);
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
    let add_metadata = if matches!(config, RewriteConfig::Valid | RewriteConfig::DataChange) {
        let write_context =
            txn.unpartitioned_write_context_with_input(WriteMode::PreserveRowTracking {
                row_id_column: "row_id",
                row_commit_version_column: "row_commit_version",
            })?;
        let logical_data_schema = write_context.logical_data_schema().clone();
        let replacement_batch = RecordBatch::try_new(
            Arc::new(logical_data_schema.as_ref().try_into_arrow()?),
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
            .write_parquet(&ArrowEngineData::new(replacement_batch), &write_context)
            .await?
    } else {
        let write_context = txn.write_state()?.unpartitioned_write_context()?;
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
    if !matches!(config, RewriteConfig::Valid | RewriteConfig::DataChange) {
        txn.ack_row_tracking_preservation();
    }

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
