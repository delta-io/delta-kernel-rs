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

use crate::common::read_utils::read_row_tracking_scan;
mod row_tracking_preservation {
    use std::collections::HashMap;

    use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
    use delta_kernel::arrow::array::{Array, ArrayRef, MapBuilder, StringArray, StringBuilder};
    use delta_kernel::schema::{DataType, SchemaRef, StructField};
    use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
    use test_utils::delta_kernel_default_engine::DefaultEngine;
    use test_utils::{read_actions_from_commit, read_add_infos};
    use url::Url;

    use super::*;
    use crate::common::read_utils::read_parquet_file;
    use crate::common::write_utils::{
        create_dv_update_transaction, get_scan_files, set_table_properties,
        write_deletion_vector_to_store,
    };
    use crate::features::row_tracking::setup_number_table_with_features;

    const ROW_TRACKING_PRESERVED_TAG: &str = "delta.rowTracking.preserved";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CommitInfoTagTestCase {
        Enabled,
        Supported,
        Suspended,
    }

    impl CommitInfoTagTestCase {
        /// Returns the properties that create an enabled or supported-only Row Tracking table.
        /// The suspended case starts supported-only and is suspended in a later commit.
        fn create_table_properties(self) -> &'static [(&'static str, &'static str)] {
            match self {
                Self::Enabled => &[("delta.enableRowTracking", "true")],
                Self::Supported | Self::Suspended => &[("delta.feature.rowTracking", "supported")],
            }
        }

        fn expected_tag(self) -> Option<&'static str> {
            (self == Self::Enabled).then_some("true")
        }
    }

    fn assert_row_tracking_preserved_tag(
        table_url: &Url,
        commit_version: u64,
        expected: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let commit_infos = read_actions_from_commit(table_url, commit_version, "commitInfo")?;
        assert_eq!(commit_infos.len(), 1);
        assert_eq!(
            commit_infos[0]["tags"][ROW_TRACKING_PRESERVED_TAG].as_str(),
            expected
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::enabled(CommitInfoTagTestCase::Enabled)]
    #[case::supported(CommitInfoTagTestCase::Supported)]
    #[case::suspended(CommitInfoTagTestCase::Suspended)]
    #[tokio::test]
    async fn insert_commit_info_preservation_tag_requires_row_tracking_enabled(
        #[case] test_case: CommitInfoTagTestCase,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Create a table in the requested Row Tracking state ===
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let table_url = Url::from_directory_path(&table_path).unwrap();
        let snapshot = kernel_create_table(
            table_path.as_str(),
            schema_ref! { nullable "number": INTEGER },
            "Test/1.0",
        )
        .with_table_properties(test_case.create_table_properties().iter().copied())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
        let snapshot = if test_case == CommitInfoTagTestCase::Suspended {
            set_table_properties(
                &table_path,
                &table_url,
                engine.as_ref(),
                0, /* current_version */
                &[("delta.rowTrackingSuspended", "true")],
            )?
        } else {
            snapshot
        };

        // === Insert data and validate the preservation tag ===
        let commit_version = insert_data(
            snapshot,
            &engine,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .await?
        .unwrap_committed()
        .commit_version();
        assert_row_tracking_preserved_tag(&table_url, commit_version, test_case.expected_tag())?;
        Ok(())
    }

    #[test]
    fn create_table_does_not_emit_row_tracking_preservation_tag(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let table_url = Url::from_directory_path(&table_path).unwrap();
        let commit_version = kernel_create_table(
            table_path.as_str(),
            schema_ref! { nullable "number": INTEGER },
            "Test/1.0",
        )
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed()
        .commit_version();

        assert_row_tracking_preserved_tag(&table_url, commit_version, None)?;
        Ok(())
    }

    #[test]
    fn alter_table_emits_row_tracking_preservation_tag() -> Result<(), Box<dyn std::error::Error>> {
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let table_url = Url::from_directory_path(&table_path).unwrap();
        let snapshot = kernel_create_table(
            table_path.as_str(),
            schema_ref! { nullable "number": INTEGER },
            "Test/1.0",
        )
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
        let commit_version = snapshot
            .alter_table()
            .add_column(StructField::nullable("added", DataType::INTEGER))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_committed()
            .commit_version();

        assert_row_tracking_preserved_tag(&table_url, commit_version, Some("true"))?;
        Ok(())
    }

    #[rstest::rstest]
    #[case::connector_commit_info_with_tags(ConnectorCommitInfoTestCase::WithTags)]
    #[case::connector_commit_info_without_tags(ConnectorCommitInfoTestCase::WithoutTags)]
    #[case::connector_commit_info_with_null_tags(ConnectorCommitInfoTestCase::NullTags)]
    fn commit_info_preservation_tag_merges_connector_commit_info(
        #[case] test_case: ConnectorCommitInfoTestCase,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Create a Row Tracking table ===
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let table_url = Url::from_directory_path(&table_path).unwrap();
        let snapshot = kernel_create_table(
            table_path.as_str(),
            schema_ref! { nullable "number": INTEGER },
            "Test/1.0",
        )
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

        // === Commit with connector-provided CommitInfo ===
        let (connector_commit_info, connector_commit_info_schema) =
            test_case.connector_commit_info()?;
        let commit_version = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_commit_info(
                Box::new(ArrowEngineData::new(connector_commit_info)),
                connector_commit_info_schema,
            )
            .commit(engine.as_ref())?
            .unwrap_committed()
            .commit_version();

        // === Validate the merged CommitInfo ===
        let commit_infos = read_actions_from_commit(&table_url, commit_version, "commitInfo")?;
        assert_eq!(commit_infos.len(), 1);
        let committed_commit_info = &commit_infos[0];
        assert_eq!(
            committed_commit_info["tags"][ROW_TRACKING_PRESERVED_TAG],
            "true"
        );
        match test_case {
            ConnectorCommitInfoTestCase::WithTags => {
                assert_eq!(
                    committed_commit_info["tags"]["connectorTag"],
                    "connectorValue"
                );
                assert!(committed_commit_info["tags"]["nullableConnectorTag"].is_null());
            }
            ConnectorCommitInfoTestCase::WithoutTags => {
                assert_eq!(committed_commit_info["customApp"], "connector");
            }
            ConnectorCommitInfoTestCase::NullTags => {}
        }
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum ConnectorCommitInfoTestCase {
        WithTags,
        WithoutTags,
        NullTags,
    }

    impl ConnectorCommitInfoTestCase {
        /// Returns `(connector_commit_info, connector_commit_info_schema)` for this case.
        fn connector_commit_info(
            self,
        ) -> Result<(RecordBatch, SchemaRef), Box<dyn std::error::Error>> {
            match self {
                Self::WithTags => {
                    let mut tags =
                        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                    tags.keys().append_value("connectorTag");
                    tags.values().append_value("connectorValue");
                    tags.keys().append_value("nullableConnectorTag");
                    tags.values().append_null();
                    tags.keys().append_value(ROW_TRACKING_PRESERVED_TAG);
                    tags.values().append_value("false");
                    tags.append(true)?;
                    Ok((
                        RecordBatch::try_from_iter([(
                            "tags",
                            Arc::new(tags.finish()) as ArrayRef,
                        )])?,
                        schema_ref! { nullable "tags": { STRING => nullable STRING } },
                    ))
                }
                Self::WithoutTags => Ok((
                    RecordBatch::try_from_iter([(
                        "customApp",
                        Arc::new(StringArray::from(vec!["connector"])) as ArrayRef,
                    )])?,
                    schema_ref! { nullable "customApp": STRING },
                )),
                Self::NullTags => {
                    let mut tags =
                        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                    tags.append(false)?;
                    Ok((
                        RecordBatch::try_from_iter([(
                            "tags",
                            Arc::new(tags.finish()) as ArrayRef,
                        )])?,
                        schema_ref! { nullable "tags": { STRING => nullable STRING } },
                    ))
                }
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RemoveTestCase {
        EnabledUnacknowledged,
        EnabledAcknowledged,
        SupportedUnacknowledged,
        SuspendedUnacknowledged,
        IcebergCompatV3SupportedAcknowledged,
        IcebergCompatV3EnabledUnacknowledged,
        IcebergCompatV3EnabledAcknowledged,
    }

    impl RemoveTestCase {
        // Each pair is a table property name/value used to create the test table. Acknowledgment is
        // transaction state, so acknowledged and unacknowledged cases share table properties.
        fn create_table_properties(self) -> &'static [(&'static str, &'static str)] {
            match self {
                Self::EnabledUnacknowledged | Self::EnabledAcknowledged => {
                    &[("delta.enableRowTracking", "true")]
                }
                Self::SupportedUnacknowledged | Self::SuspendedUnacknowledged => {
                    &[("delta.feature.rowTracking", "supported")]
                }
                Self::IcebergCompatV3SupportedAcknowledged => &[
                    ("delta.enableRowTracking", "true"),
                    ("delta.columnMapping.mode", "name"),
                    ("delta.feature.icebergCompatV3", "supported"),
                ],
                Self::IcebergCompatV3EnabledUnacknowledged
                | Self::IcebergCompatV3EnabledAcknowledged => {
                    &[("delta.enableIcebergCompatV3", "true")]
                }
            }
        }

        fn acknowledges_preservation(self) -> bool {
            self == Self::EnabledAcknowledged
                || self == Self::IcebergCompatV3SupportedAcknowledged
                || self == Self::IcebergCompatV3EnabledAcknowledged
        }

        fn expects_error(self) -> Option<&'static str> {
            match self {
                Self::EnabledUnacknowledged => Some("Transaction::ack_row_tracking_preservation()"),
                Self::IcebergCompatV3EnabledUnacknowledged
                | Self::IcebergCompatV3EnabledAcknowledged => Some("icebergCompatV3"),
                _ => None,
            }
        }
    }

    #[rstest::rstest]
    #[case::enabled_without_acknowledgment(RemoveTestCase::EnabledUnacknowledged)]
    #[case::enabled_with_acknowledgment(RemoveTestCase::EnabledAcknowledged)]
    #[case::supported_without_acknowledgment(RemoveTestCase::SupportedUnacknowledged)]
    #[case::suspended_without_acknowledgment(RemoveTestCase::SuspendedUnacknowledged)]
    #[case::iceberg_compat_v3_supported_with_acknowledgment(
        RemoveTestCase::IcebergCompatV3SupportedAcknowledged
    )]
    #[case::iceberg_compat_v3_enabled_without_acknowledgment(
        RemoveTestCase::IcebergCompatV3EnabledUnacknowledged
    )]
    #[case::iceberg_compat_v3_enabled_with_acknowledgment(
        RemoveTestCase::IcebergCompatV3EnabledAcknowledged
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn remove_files_requires_row_tracking_preservation_acknowledgment(
        #[case] test_case: RemoveTestCase,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Create the table and insert data ===
        let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
        let schema = schema_ref! { nullable "number": INTEGER };
        let table_url = Url::from_directory_path(&table_path).unwrap();
        kernel_create_table(table_path.as_str(), schema.clone(), "Test/1.0")
            .with_table_properties(test_case.create_table_properties().iter().copied())
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_committed();

        let initial_snapshot = if test_case == RemoveTestCase::SuspendedUnacknowledged {
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
        insert_data(
            initial_snapshot,
            &engine,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .await?
        .unwrap_committed();

        // === Stage file removal and optionally acknowledge preservation ===
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
        if test_case.acknowledges_preservation() {
            txn.ack_row_tracking_preservation();
        }

        // === Commit and verify the result ===
        if let Some(expected_error) = test_case.expects_error() {
            assert_result_error_with_message(txn.commit(engine.as_ref()), expected_error);
        } else {
            let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
            let scan = snapshot.scan_builder().build()?;
            let row_count: usize = read_scan(&scan, engine)?
                .iter()
                .map(RecordBatch::num_rows)
                .sum();
            assert_eq!(row_count, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn deletion_vector_update_requires_preservation_acknowledgment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Create a Row Tracking table with deletion vectors and insert data ===
        let tmp_dir = tempfile::tempdir()?;
        let (_schema, table_url, engine, store) = setup_number_table_with_features(
            &tmp_dir,
            "dv_update_without_preservation_acknowledgment",
            &["deletionVectors"],
            &[],
        )
        .await?;
        let snapshot = Snapshot::builder_for(&table_url).build(engine.as_ref())?;
        let snapshot = insert_data(
            snapshot,
            &engine,
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .await?
        .unwrap_post_commit_snapshot();

        // === Stage a deletion-vector update without preservation acknowledgment ===
        let mut txn = create_dv_update_transaction(&table_url, engine.as_ref())?;
        let write_context = txn.write_state()?.write_context_builder().build()?;
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes([0]);
        let descriptor =
            write_deletion_vector_to_store(&store, &write_context, deletion_vector, "").await?;
        let file_path = read_add_infos(snapshot.as_ref(), engine.as_ref())?[0]
            .path
            .clone();
        txn.update_deletion_vectors(
            HashMap::from([(file_path, descriptor)]),
            get_scan_files(snapshot, engine.as_ref())?
                .into_iter()
                .map(Ok),
        )?;

        // === Verify the commit is rejected ===
        assert_result_error_with_message(
            txn.commit(engine.as_ref()),
            "Transaction::ack_row_tracking_preservation()",
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::minimum_feature_set(&[("delta.enableRowTracking", "true")])]
    #[case::maximum_feature_set(&[
        ("delta.columnMapping.mode", "name"),
        ("delta.enableDeletionVectors", "true"),
        ("delta.enableRowTracking", "true"),
        ("delta.feature.domainMetadata", "supported"),
        ("delta.enableInCommitTimestamps", "true"),
        ("delta.feature.v2Checkpoint", "supported"),
        ("delta.feature.vacuumProtocolCheck", "supported"),
        ("delta.enableChangeDataFeed", "true"),
        ("delta.feature.icebergCompatV3", "supported"),
    ])]
    #[tokio::test(flavor = "multi_thread")]
    async fn optimize_preserves_row_tracking_metadata_in_data_and_checkpoint(
        #[case] table_properties: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Write two source files ===
        let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
        let schema = schema_ref! { nullable "number": INTEGER };
        let snapshot = kernel_create_table(table_path.as_str(), schema, "Test/1.0")
            .with_table_properties(table_properties.iter().copied())
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

        // === Read and merge the source files ===
        let source_batches = read_row_tracking_scan(
            source_snapshot.clone(),
            engine.clone(),
            [
                MetadataColumnSpec::RowId,
                MetadataColumnSpec::RowCommitVersion,
            ],
        )?;
        let source_schema = source_batches
            .first()
            .expect("source scan must return data")
            .schema();
        let merged_batch = concat_batches(&source_schema, &source_batches)?;
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

        // === Rewrite the rows into one file ===
        let optimized_snapshot =
            rewrite_preserving_row_tracking(source_snapshot, &engine, merged_batch).await?;
        let optimize_version = optimized_snapshot.version();

        let optimized_batches = read_row_tracking_scan(
            optimized_snapshot.clone(),
            engine.clone(),
            [
                MetadataColumnSpec::RowId,
                MetadataColumnSpec::RowCommitVersion,
            ],
        )?;
        assert_eq!(
            collect_rows(&optimized_batches, "number", "row_id", "row_commit_version",),
            expected_rows
        );

        // === Checkpoint the OPTIMIZE commit ===
        optimized_snapshot.checkpoint(engine.as_ref(), None)?;
        let checkpoint_path = std::path::Path::new(&table_path)
            .join("_delta_log")
            .join(format!("{optimize_version:020}.checkpoint.parquet"));
        let checkpoint = read_parquet_file(&checkpoint_path);
        assert_eq!(
            collect_checkpoint_row_tracking_metadata(&checkpoint, "add"),
            vec![(5, 3)]
        );
        assert_eq!(
            collect_checkpoint_row_tracking_metadata(&checkpoint, "remove"),
            vec![(0, 1), (2, 2)]
        );

        let checkpoint_snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        assert_eq!(
            collect_rows(
                &read_row_tracking_scan(
                    checkpoint_snapshot,
                    engine.clone(),
                    [
                        MetadataColumnSpec::RowId,
                        MetadataColumnSpec::RowCommitVersion,
                    ],
                )?,
                "number",
                "row_id",
                "row_commit_version",
            ),
            expected_rows
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn optimize_preserves_row_tracking_metadata_after_deletion_vector_update(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // === Write the source file ===
        let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
        let table_url = Url::from_directory_path(&table_path).unwrap();
        let snapshot = kernel_create_table(
            table_path.as_str(),
            schema_ref! { nullable "number": INTEGER },
            "Test/1.0",
        )
        .with_table_properties([
            ("delta.enableDeletionVectors", "true"),
            ("delta.enableRowTracking", "true"),
        ])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
        let source_snapshot = insert_data(
            snapshot,
            &engine,
            vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        )
        .await?
        .unwrap_post_commit_snapshot();

        // === Delete rows 20 and 40 with a deletion vector ===
        let mut txn = source_snapshot
            .clone()
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("DELETE".to_string());
        let write_context = txn.write_state()?.write_context_builder().build()?;
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes([1, 3]);
        let store = engine
            .get_object_store_for_url(&table_url)
            .expect("default engine must expose its object store");
        let descriptor =
            write_deletion_vector_to_store(&store, &write_context, deletion_vector, "").await?;
        let file_path = read_add_infos(source_snapshot.as_ref(), engine.as_ref())?[0]
            .path
            .clone();
        txn.update_deletion_vectors(
            HashMap::from([(file_path, descriptor)]),
            get_scan_files(source_snapshot, engine.as_ref())?
                .into_iter()
                .map(Ok),
        )?;
        txn.ack_row_tracking_preservation();
        let deletion_vector_snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

        // === Read and merge the surviving rows ===
        let survivor_batches = read_row_tracking_scan(
            deletion_vector_snapshot.clone(),
            engine.clone(),
            [
                MetadataColumnSpec::RowId,
                MetadataColumnSpec::RowCommitVersion,
            ],
        )?;
        let survivor_schema = survivor_batches
            .first()
            .expect("deletion vector scan must return data")
            .schema();
        let merged_batch = concat_batches(&survivor_schema, &survivor_batches)?;
        let expected_rows = vec![(10, 0, 1), (30, 2, 1), (50, 4, 1)];
        assert_eq!(
            collect_rows(
                std::slice::from_ref(&merged_batch),
                "number",
                "row_id",
                "row_commit_version",
            ),
            expected_rows
        );

        // === Rewrite the surviving rows into a file without a deletion vector ===
        let optimized_snapshot =
            rewrite_preserving_row_tracking(deletion_vector_snapshot, &engine, merged_batch)
                .await?;

        // === Verify the OPTIMIZE result ===
        let optimized_batches = read_row_tracking_scan(
            optimized_snapshot,
            engine,
            [
                MetadataColumnSpec::RowId,
                MetadataColumnSpec::RowCommitVersion,
            ],
        )?;
        assert_eq!(
            collect_rows(&optimized_batches, "number", "row_id", "row_commit_version",),
            expected_rows
        );
        Ok(())
    }

    async fn rewrite_preserving_row_tracking(
        snapshot: Arc<Snapshot>,
        engine: &Arc<DefaultEngine<TokioMultiThreadExecutor>>,
        batch: RecordBatch,
    ) -> DeltaResult<Arc<Snapshot>> {
        let source_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
        let mut txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("OPTIMIZE".to_string())
            .with_data_change(false);
        let write_context = txn
            .write_state()?
            .write_context_builder()
            .with_row_tracking_columns(RowTrackingMetadataColumns {
                row_id_col_name: Some("row_id"),
                row_commit_version_col_name: Some("row_commit_version"),
            })
            .build()?;
        txn.add_files(
            engine
                .write_parquet(&ArrowEngineData::new(batch), &write_context)
                .await?,
        );
        for files in source_files {
            txn.remove_files(files);
        }
        txn.ack_row_tracking_preservation();
        Ok(txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot())
    }

    fn collect_checkpoint_row_tracking_metadata(
        checkpoint: &RecordBatch,
        action_name: &str,
    ) -> Vec<(i64, i64)> {
        let actions = checkpoint
            .column_by_name(action_name)
            .unwrap_or_else(|| panic!("{action_name} column not found"))
            .as_struct();
        let base_row_ids = actions
            .column_by_name("baseRowId")
            .unwrap_or_else(|| panic!("{action_name}.baseRowId column not found"))
            .as_primitive::<Int64Type>();
        let default_row_commit_versions = actions
            .column_by_name("defaultRowCommitVersion")
            .unwrap_or_else(|| panic!("{action_name}.defaultRowCommitVersion column not found"))
            .as_primitive::<Int64Type>();
        let mut metadata = (0..checkpoint.num_rows())
            .filter(|row| actions.is_valid(*row))
            .map(|row| {
                assert!(base_row_ids.is_valid(row));
                assert!(default_row_commit_versions.is_valid(row));
                (
                    base_row_ids.value(row),
                    default_row_commit_versions.value(row),
                )
            })
            .collect::<Vec<_>>();
        metadata.sort_unstable();
        metadata
    }
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
    let source_batches = read_row_tracking_scan(
        source_snapshot.clone(),
        engine.clone(),
        [
            MetadataColumnSpec::RowId,
            MetadataColumnSpec::RowCommitVersion,
        ],
    )?;

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
