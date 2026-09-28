use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::actions::Metadata;
use delta_kernel::arrow::array::{Int32Array, StringArray};
use delta_kernel::arrow::json::ReaderBuilder;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::committer::{
    CommitMetadata, CommitResponse, CommitType, Committer, FileSystemCommitter, PublishMetadata,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow as _, TryIntoKernel as _};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{column_name, Scalar};
use delta_kernel::schema::{
    schema, schema_ref, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, SchemaRef,
    StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, DeltaResultIterator, Engine, FilteredEngineData};
use rstest::rstest;
use test_utils::{
    assert_result_error_with_message, copy_directory, create_table_and_load_snapshot,
    engine_store_setup, read_actions_from_commit, read_scan, test_table_setup_mt,
    write_batch_to_table, TestCatalogCommitter,
};
use url::Url;

use crate::common::read_utils::read_parquet_file;

struct InspectOverwriteCommitter {
    previous: Metadata,
}

impl Committer for InspectOverwriteCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        assert_eq!(metadata.commit_type(), CommitType::CatalogManagedWrite);
        let pm = metadata.protocol_metadata();
        assert!(pm.new_protocol().is_none());
        assert_eq!(pm.read_metadata(), Some(&self.previous));
        let updated = pm
            .new_metadata()
            .expect("replacement metadata must reach the catalog");
        assert_eq!(updated.id(), self.previous.id());
        assert_eq!(updated.configuration(), self.previous.configuration());
        assert_eq!(
            updated.parse_schema()?.field("amount").unwrap().data_type(),
            &DataType::STRING
        );
        assert!(metadata.has_domain_metadata_change("app.version"));
        TestCatalogCommitter.commit(engine, actions, metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        true
    }

    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> DeltaResult<()> {
        TestCatalogCommitter.publish(engine, metadata)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_catalog_managed_forwards_metadata_and_preserves_required_features(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&path).unwrap();
    let mut create = create_table(&path, schema_ref! { not_null "amount": INTEGER }, "test")
        .with_table_properties([
            ("delta.feature.catalogManaged", "supported"),
            ("delta.feature.domainMetadata", "supported"),
            ("delta.feature.vacuumProtocolCheck", "supported"),
            ("delta.feature.invariants", "supported"),
            ("delta.columnMapping.mode", "name"),
        ])
        .build(engine.as_ref(), Box::new(TestCatalogCommitter))?;
    let context = create.write_state()?.write_context_builder().build()?;
    let old_batch = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;
    create.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(old_batch), &context)
            .await?,
    );
    let snapshot = create
        .with_domain_metadata("app.version".into(), "1".into())
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let old_config = snapshot.table_configuration().clone();
    let committer = InspectOverwriteCommitter {
        previous: old_config.metadata().clone(),
    };
    let mut txn = snapshot
        .overwrite(schema_ref! { not_null "amount": STRING }, vec![])
        .build(engine.as_ref(), Box::new(committer))?;
    let context = txn.write_state()?.write_context_builder().build()?;
    let replacement = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        vec![Arc::new(StringArray::from(vec!["new"]))],
    )?;
    txn.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(replacement), &context)
            .await?,
    );
    let post = txn
        .with_domain_metadata("app.version".into(), "2".into())
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(post.table_configuration().protocol(), old_config.protocol());
    assert_eq!(
        post.get_domain_metadata("app.version", engine.as_ref())?,
        Some("2".into())
    );
    let before = read_actions_from_commit(&table_url, 0, "commitInfo")?;
    let after = read_actions_from_commit(&table_url, 1, "commitInfo")?;
    assert!(
        after[0]["inCommitTimestamp"].as_i64().unwrap()
            > before[0]["inCommitTimestamp"].as_i64().unwrap()
    );
    let reloaded = Snapshot::builder_for(&path)
        .with_max_catalog_version(post.version())
        .build(engine.as_ref())?;
    let batches = read_scan(&reloaded.scan_builder().build()?, engine.clone())?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "new"
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_assigns_fresh_row_tracking_metadata(
    #[values("true", "false")] enabled: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&path).unwrap();
    let schema = schema_ref! { nullable "amount": INTEGER };
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema.clone(),
        engine.as_ref(),
        &[
            ("delta.feature.rowTracking", "supported"),
            ("delta.enableRowTracking", enabled),
        ],
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    let old_adds = read_actions_from_commit(&table_url, 1, "add")?;
    let mut txn = snapshot
        .overwrite(schema_ref! { nullable "amount": STRING }, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = txn.write_state()?.write_context_builder().build()?;
    let batch = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        vec![Arc::new(StringArray::from(vec!["new1", "new2"]))],
    )?;
    txn.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(batch), &context)
            .await?,
    );
    let post = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let adds = read_actions_from_commit(&table_url, post.version(), "add")?;
    let removes = read_actions_from_commit(&table_url, post.version(), "remove")?;
    assert_eq!(adds.len(), 1);
    assert_eq!(removes.len(), 1);
    assert_eq!(adds[0]["baseRowId"], 3);
    assert_eq!(adds[0]["defaultRowCommitVersion"], 2);
    for field in ["baseRowId", "defaultRowCommitVersion"] {
        assert_eq!(removes[0][field], old_adds[0][field]);
    }
    let domains = read_actions_from_commit(&table_url, post.version(), "domainMetadata")?;
    let row_tracking = domains
        .iter()
        .find(|d| d["domain"] == "delta.rowTracking")
        .unwrap();
    let state: serde_json::Value =
        serde_json::from_str(row_tracking["configuration"].as_str().unwrap())?;
    assert_eq!(state["rowIdHighWaterMark"], 4);
    Ok(())
}

#[rstest]
#[case("changeDataFeed", "delta.enableChangeDataFeed")]
#[case("appendOnly", "delta.appendOnly")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_requires_cdf_and_append_only_disabled(
    #[case] feature: &str,
    #[case] property: &str,
    #[values(false, true)] enabled: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "amount": INTEGER };
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema.clone(),
        engine.as_ref(),
        &[
            (&format!("delta.feature.{feature}"), "supported"),
            (property, if enabled { "true" } else { "false" }),
        ],
    )?;
    let protocol = snapshot.table_configuration().protocol().clone();
    let result = snapshot
        .overwrite(schema, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if enabled {
        assert_result_error_with_message(result, feature);
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .version(),
            0
        );
    } else {
        let post = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(post.table_configuration().protocol(), &protocol);
    }
    Ok(())
}

#[rstest]
#[case("delta.rowTracking.materializedRowIdColumnName", "none")]
#[case("delta.rowTracking.materializedRowCommitVersionColumnName", "none")]
#[case("delta.rowTracking.materializedRowIdColumnName", "name")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_rejects_reserved_row_tracking_names(
    #[case] property: &str,
    #[case] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "amount": INTEGER },
        engine.as_ref(),
        &[
            ("delta.enableRowTracking", "true"),
            ("delta.columnMapping.mode", mapping),
        ],
    )?;
    let reserved = snapshot.table_configuration().metadata().configuration()[property].clone();
    let replacement = Arc::new(StructType::try_new([StructField::nullable(
        reserved,
        DataType::LONG,
    )])?);
    let result = snapshot
        .overwrite(replacement, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if mapping == "none" {
        assert_result_error_with_message(result, "row-tracking");
    } else {
        let post = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_ne!(
            post.schema()
                .field_at_index(0)
                .unwrap()
                .physical_name(post.table_configuration().column_mapping_mode()),
            post.table_configuration().metadata().configuration()[property]
        );
    }
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_not_null_requires_existing_invariants_support(
    #[values(false, true)] supported: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let replacement = schema_ref! { nullable "nested": { not_null "amount": INTEGER } };
    let properties = if supported {
        vec![("delta.feature.invariants", "supported")]
    } else {
        vec![]
    };
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "amount": INTEGER },
        engine.as_ref(),
        &properties,
    )?;
    let protocol = snapshot.table_configuration().protocol().clone();
    let result = snapshot
        .overwrite(replacement.clone(), vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    if supported {
        let post = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(post.schema(), replacement);
        assert_eq!(post.table_configuration().protocol(), &protocol);
    } else {
        assert_result_error_with_message(result, "invariants");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_initializes_empty_schema_table() -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let empty_schema = Arc::new(StructType::try_new([])?);
    let snapshot = create_table_and_load_snapshot(&path, empty_schema, engine.as_ref(), &[])?;
    let table_id = snapshot.table_configuration().metadata().id().to_string();
    assert_result_error_with_message(snapshot.clone().scan_builder().build(), "empty schema");
    let mut txn = snapshot
        .overwrite(schema_ref! { nullable "amount": INTEGER }, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = txn.write_state()?.write_context_builder().build()?;
    let batch = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![42]))],
    )?;
    txn.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(batch), &context)
            .await?,
    );
    let post = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    assert_eq!(post.table_configuration().metadata().id(), table_id);
    let batches = read_scan(&post.scan_builder().build()?, engine.clone())?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        42
    );
    Ok(())
}

#[rstest]
#[case("none", false)]
#[case("name", false)]
#[case("id", false)]
#[case("name", true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_replaces_all_rows_and_preserves_retained_column_identity(
    #[case] mapping: &str,
    #[case] empty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let old_schema = schema_ref! { nullable "amount": INTEGER, nullable "removed": STRING };
    let snapshot = create_table_and_load_snapshot(
        &path,
        old_schema.clone(),
        engine.as_ref(),
        &[
            ("delta.columnMapping.mode", mapping),
            ("delta.feature.domainMetadata", "supported"),
            ("app.property", "preserved"),
        ],
    )?;
    let old_batch = RecordBatch::try_new(
        Arc::new(old_schema.as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["old", "old"])),
        ],
    )?;
    let snapshot = write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        old_batch.clone(),
        HashMap::new(),
    )
    .await?;
    let snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), old_batch, HashMap::new()).await?;
    let snapshot = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_domain_metadata("app.version".into(), "1".into())
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let old_config = snapshot.table_configuration();
    let old_amount = snapshot.schema().field("amount").unwrap().clone();
    let mut amount = StructField::nullable("amount", DataType::STRING);
    if mapping == "none" {
        amount = amount.add_metadata([
            ("delta.columnMapping.id", MetadataValue::Number(100)),
            (
                "delta.columnMapping.physicalName",
                MetadataValue::String("old-physical-name".into()),
            ),
        ]);
    }
    let replacement = schema_ref! { nullable "added": INTEGER, (amount) };
    let mut txn = snapshot
        .clone()
        .overwrite(replacement, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let context = txn.write_state()?.write_context_builder().build()?;
    assert_eq!(
        context
            .physical_data_schema()
            .field_at_index(1)
            .unwrap()
            .name(),
        old_amount.physical_name(old_config.column_mapping_mode())
    );
    if !empty {
        let batch = RecordBatch::try_new(
            Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int32Array::from(vec![30, 40])),
                Arc::new(StringArray::from(vec!["three", "four"])),
            ],
        )?;
        txn.add_files(
            engine
                .write_parquet(&ArrowEngineData::new(batch), &context)
                .await?,
        );
    }
    let post = txn
        .with_domain_metadata("app.version".into(), "2".into())
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let reloaded = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let table_url = Url::from_directory_path(&path).unwrap();
    let adds = read_actions_from_commit(&table_url, reloaded.version(), "add")?;
    for add in adds {
        let parquet = read_parquet_file(
            &table_url
                .join(add["path"].as_str().unwrap())?
                .to_file_path()
                .unwrap(),
        );
        assert_eq!(
            parquet.schema().field(1).name(),
            old_amount.physical_name(old_config.column_mapping_mode())
        );
    }
    assert_eq!(
        read_actions_from_commit(&table_url, reloaded.version(), "remove")?.len(),
        2
    );
    for current in [post, reloaded] {
        let config = current.table_configuration();
        assert_eq!(config.protocol(), old_config.protocol());
        assert_eq!(config.metadata().id(), old_config.metadata().id());
        assert_eq!(
            config.metadata().configuration()["app.property"],
            "preserved"
        );
        assert_eq!(
            config.column_mapping_mode(),
            old_config.column_mapping_mode()
        );
        assert_eq!(
            current.get_domain_metadata("app.version", engine.as_ref())?,
            Some("2".into())
        );
        let schema = current.schema();
        assert!(schema.field("removed").is_none());
        let amount = schema.field("amount").unwrap();
        assert_eq!(amount.data_type(), &DataType::STRING);
        for key in [
            ColumnMetadataKey::ColumnMappingId,
            ColumnMetadataKey::ColumnMappingPhysicalName,
        ] {
            assert_eq!(
                amount.get_config_value(&key),
                old_amount.get_config_value(&key)
            );
        }
        if mapping != "none" {
            let old_max = old_config
                .table_properties()
                .column_mapping_max_column_id
                .unwrap();
            assert!(schema.field("added").unwrap().column_mapping_id().unwrap() > old_max);
        }
        let batches = read_scan(&current.scan_builder().build()?, engine.clone())?;
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            if empty { 0 } else { 2 }
        );
        for batch in batches {
            let amounts = batch
                .column_by_name("amount")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                amounts.iter().collect::<Vec<_>>(),
                vec![Some("three"), Some("four")]
            );
        }
    }
    let old_rows = read_scan(&snapshot.clone().scan_builder().build()?, engine.clone())?;
    assert_eq!(old_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    assert_eq!(
        snapshot.get_domain_metadata("app.version", engine.as_ref())?,
        Some("1".into())
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_changes_partitioning_from_checkpoint(
    #[values("none", "name", "id")] mapping: &str,
    #[values(false, true)] partitioned: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&path).unwrap();
    let mut snapshot = create_table(
        &path,
        schema_ref! { nullable "amount": INTEGER, nullable "old_part": STRING },
        "test",
    )
    .with_table_properties([("delta.columnMapping.mode", mapping)])
    .with_data_layout(DataLayout::Partitioned {
        columns: vec![column_name!("old_part")],
    })
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    for part in ["a", "b"] {
        let batch = RecordBatch::try_new(
            Arc::new(
                schema_ref! { nullable "amount": INTEGER }
                    .as_ref()
                    .try_into_arrow()?,
            ),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            batch,
            HashMap::from([("old_part".into(), Scalar::from(part))]),
        )
        .await?;
    }
    let old_partition_key = snapshot
        .schema()
        .field("old_part")
        .unwrap()
        .physical_name(snapshot.table_configuration().column_mapping_mode())
        .to_string();
    let (_, snapshot) = snapshot.checkpoint(engine.as_ref(), None)?;
    let partition_columns = if partitioned {
        vec!["new_part".to_string()]
    } else {
        vec![]
    };
    let mut txn = snapshot
        .overwrite(
            schema_ref! { nullable "amount": STRING, nullable "new_part": INTEGER },
            partition_columns.clone(),
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let state = txn.write_state()?;
    let builder = state.write_context_builder();
    let context = if partitioned {
        builder
            .with_partition_values(HashMap::from([("new_part".into(), Scalar::from(7))]))
            .build()?
    } else {
        builder.build()?
    };
    let mut columns: Vec<delta_kernel::arrow::array::ArrayRef> =
        vec![Arc::new(StringArray::from(vec!["new"]))];
    if !partitioned {
        columns.push(Arc::new(Int32Array::from(vec![7])));
    }
    let batch = RecordBatch::try_new(
        Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?),
        columns,
    )?;
    txn.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(batch), &context)
            .await?,
    );
    let post = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    assert_eq!(
        post.table_configuration().logical_partition_columns(),
        &partition_columns
    );
    let removes = read_actions_from_commit(&table_url, post.version(), "remove")?;
    assert_eq!(removes.len(), 2);
    let mut old_parts = removes
        .iter()
        .map(|remove| {
            let values = remove["partitionValues"].as_object().unwrap();
            assert_eq!(values.len(), 1);
            values[&old_partition_key].as_str().unwrap()
        })
        .collect::<Vec<_>>();
    old_parts.sort();
    assert_eq!(old_parts, ["a", "b"]);
    post.checkpoint(engine.as_ref(), None)?;
    let reloaded = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let batches = read_scan(&reloaded.scan_builder().build()?, engine.clone())?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let part = batches[0]
        .column_by_name("new_part")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(part.value(0), 7);
    Ok(())
}

#[rstest]
#[case("struct", "struct", "name")]
#[case("array", "array", "name")]
#[case("map_key", "map_key", "name")]
#[case("map_value", "map_value", "name")]
#[case("array", "array", "id")]
#[case("array", "array", "none")]
#[case::struct_to_array("struct", "array", "name")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_preserves_nested_mappings(
    #[case] original_container: &str,
    #[case] replacement_container: &str,
    #[case] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let wrap = |container: &str, inner: StructType| -> DataType {
        match container {
            "struct" => inner.into(),
            "array" => ArrayType::new(inner, true).into(),
            "map_key" => MapType::new(inner, DataType::STRING, true).into(),
            "map_value" => MapType::new(DataType::STRING, inner, true).into(),
            _ => unreachable!(),
        }
    };
    let (_temp, path, engine) = test_table_setup_mt()?;
    let original = schema_ref! { (StructField::nullable("container", wrap(original_container, schema! { nullable "kept": INTEGER, nullable "removed": STRING }))) };
    let snapshot = create_table_and_load_snapshot(
        &path,
        original,
        engine.as_ref(),
        &[("delta.columnMapping.mode", mapping)],
    )?;
    let original_schema = snapshot.schema();
    let original_max = snapshot
        .table_configuration()
        .table_properties()
        .column_mapping_max_column_id;
    let unchanged = snapshot
        .overwrite(original_schema.clone(), vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(unchanged.schema(), original_schema);
    assert_eq!(
        unchanged
            .table_configuration()
            .table_properties()
            .column_mapping_max_column_id,
        original_max
    );
    let replacement = schema_ref! { (StructField::nullable("container", wrap(replacement_container, schema! { nullable "added": STRING, nullable "kept": STRING }))) };
    let mut txn = unchanged
        .overwrite(replacement, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let batch = if replacement_container == "array" {
        let context = txn.write_state()?.write_context_builder().build()?;
        let arrow_schema = Arc::new(context.logical_data_schema().as_ref().try_into_arrow()?);
        let batch = ReaderBuilder::new(arrow_schema)
            .build(Cursor::new(
                r#"{"container":[{"added":"added-value","kept":"kept-value"}]}"#,
            ))?
            .next()
            .unwrap()?;
        txn.add_files(
            engine
                .write_parquet(&ArrowEngineData::new(batch.clone()), &context)
                .await?,
        );
        Some(batch)
    } else {
        None
    };
    let post = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let replacement_schema = post.schema();
    let old_parent = original_schema.field("container").unwrap();
    let new_parent = replacement_schema.field("container").unwrap();
    assert_eq!(old_parent.metadata, new_parent.metadata);
    fn inner(data_type: &DataType) -> &StructType {
        match data_type {
            DataType::Struct(s) => s,
            DataType::Array(a) => inner(a.element_type()),
            DataType::Map(m) => match m.key_type() {
                DataType::Struct(_) => inner(m.key_type()),
                _ => inner(m.value_type()),
            },
            _ => unreachable!(),
        }
    }
    let old_inner = inner(old_parent.data_type());
    let new_inner = inner(new_parent.data_type());
    let old_kept = old_inner.field("kept").unwrap();
    let new_kept = new_inner.field("kept").unwrap();
    if original_container == replacement_container {
        assert_eq!(old_kept.metadata, new_kept.metadata);
    } else {
        assert!(new_kept.column_mapping_id().unwrap() > original_max.unwrap());
        assert_ne!(
            new_kept.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
            old_kept.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
        );
    }
    if mapping != "none" {
        assert!(
            new_inner
                .field("added")
                .unwrap()
                .column_mapping_id()
                .unwrap()
                > original_max.unwrap()
        );
    }
    assert!(new_inner.field("removed").is_none());
    if let Some(batch) = batch {
        let url = Url::from_directory_path(&path).unwrap();
        let adds = read_actions_from_commit(&url, post.version(), "add")?;
        assert_eq!(adds.len(), 1);
        let parquet = read_parquet_file(
            &url.join(adds[0]["path"].as_str().unwrap())?
                .to_file_path()
                .unwrap(),
        );
        let physical: StructType = parquet.schema().try_into_kernel()?;
        let mode = post.table_configuration().column_mapping_mode();
        let physical_parent = physical.field(old_parent.physical_name(mode)).unwrap();
        let physical_kept = inner(physical_parent.data_type())
            .field(new_kept.physical_name(mode))
            .unwrap();
        assert_eq!(physical_kept.data_type(), &DataType::STRING);
        let batches = read_scan(&post.scan_builder().build()?, engine.clone())?;
        assert_eq!(
            pretty_format_batches(&batches)?.to_string(),
            pretty_format_batches(&[batch])?.to_string()
        );
    }
    Ok(())
}

#[rstest]
#[case::missing_partition(schema_ref! { nullable "amount": INTEGER, nullable "other": STRING }, vec!["absent".into()], "not found")]
#[case::ntz_requires_feature(schema_ref! { nullable "amount": TIMESTAMP_NTZ }, vec![], "timestampNtz")]
#[case::empty_schema(Arc::new(StructType::try_new([]).unwrap()), vec![], "non-empty")]
#[case::orphan_default(schema_ref! { (StructField::nullable("amount", DataType::INTEGER).add_metadata([("CURRENT_DEFAULT", MetadataValue::String("42".into()))])) }, vec![], "CURRENT_DEFAULT")]
#[case::invariant(Arc::new(StructType::try_new([StructField::nullable("amount", DataType::INTEGER).add_metadata([("delta.invariants", MetadataValue::String(r#"{"expression":{"expression":"amount > 0"}}"#.into()))])]).unwrap()), vec![], "invariants")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_rejects_invalid_replacement_before_commit(
    #[case] replacement: SchemaRef,
    #[case] partitions: Vec<String>,
    #[case] error: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "amount": INTEGER },
        engine.as_ref(),
        &[],
    )?;
    assert_result_error_with_message(
        snapshot
            .overwrite(replacement, partitions)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        error,
    );
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .version(),
        0
    );
    Ok(())
}

#[rstest]
#[case::id(ColumnMetadataKey::ColumnMappingId, MetadataValue::Number(100))]
#[case::physical_name(ColumnMetadataKey::ColumnMappingPhysicalName, MetadataValue::String("different".into()))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_rejects_conflicting_mapping_annotations(
    #[case] key: ColumnMetadataKey,
    #[case] value: MetadataValue,
    #[values(false, true)] added: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "amount": INTEGER },
        engine.as_ref(),
        &[("delta.columnMapping.mode", "name")],
    )?;
    let field = StructField::nullable(if added { "added" } else { "amount" }, DataType::INTEGER)
        .add_metadata([(key.as_ref(), value)]);
    assert_result_error_with_message(
        snapshot
            .overwrite(Arc::new(StructType::try_new([field])?), vec![])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new())),
        "Conflicting",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_rejects_data_change_false_and_conflicts(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let schema = schema_ref! { nullable "amount": INTEGER };
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema.clone(),
        engine.as_ref(),
        &[("delta.feature.domainMetadata", "supported")],
    )?;
    let old = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), old, HashMap::new()).await?;
    let txn = snapshot
        .clone()
        .overwrite(schema.clone(), vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    assert_result_error_with_message(
        txn.with_data_change(false).commit(engine.as_ref()),
        "dataChange",
    );
    let txn = snapshot
        .clone()
        .overwrite(schema_ref! { nullable "replacement": STRING }, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata("app.version".into(), "loser".into());
    let mut winner = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let context = winner.write_state()?.write_context_builder().build()?;
    let batch = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![Arc::new(Int32Array::from(vec![42]))],
    )?;
    winner.add_files(
        engine
            .write_parquet(&ArrowEngineData::new(batch), &context)
            .await?,
    );
    winner
        .with_domain_metadata("app.version".into(), "winner".into())
        .commit(engine.as_ref())?
        .unwrap_committed();
    assert!(matches!(
        txn.commit(engine.as_ref())?,
        CommitResult::Conflicted(_)
    ));
    let current = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(current.schema(), schema);
    assert_eq!(current.version(), 2);
    assert_eq!(
        current.get_domain_metadata("app.version", engine.as_ref())?,
        Some("winner".into())
    );
    let batches = read_scan(&current.scan_builder().build()?, engine.clone())?;
    let mut amounts = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    amounts.sort_unstable();
    assert_eq!(amounts, [Some(1), Some(42)]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_removes_dv_files_with_original_metadata(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    copy_directory(
        Path::new("tests/data/table-with-dv-small"),
        Path::new(&path),
    )?;
    let url = Url::from_directory_path(&path).unwrap();
    let snapshot = Snapshot::builder_for(&path)
        .at_version(1)
        .build(engine.as_ref())?;
    let original = read_actions_from_commit(&url, 1, "add")?;
    assert_eq!(original.len(), 1);
    let post = snapshot
        .clone()
        .overwrite(schema_ref! { nullable "replacement": STRING }, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let removes = read_actions_from_commit(&url, post.version(), "remove")?;
    assert_eq!(removes.len(), 1);
    for field in ["path", "size", "partitionValues", "tags", "deletionVector"] {
        assert_eq!(removes[0][field], original[0][field], "{field}");
    }
    assert_eq!(removes[0]["dataChange"], true);
    assert_eq!(removes[0]["extendedFileMetadata"], true);
    assert!(read_scan(&post.scan_builder().build()?, engine.clone())?.is_empty());
    let old_rows = read_scan(&snapshot.scan_builder().build()?, engine.clone())?;
    assert_eq!(old_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 8);
    Ok(())
}

#[tokio::test]
async fn overwrite_cannot_bypass_source_invariants() -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, location) = engine_store_setup("overwrite-invariants", None);
    let constrained = StructField::nullable("amount", DataType::INTEGER).add_metadata([(
        "delta.invariants",
        MetadataValue::String(r#"{"expression":{"expression":"amount > 0"}}"#.into()),
    )]);
    let location = test_utils::create_table(
        store,
        location,
        Arc::new(StructType::try_new([constrained])?),
        &[],
        true,
        vec![],
        vec!["invariants"],
    )
    .await?;
    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    assert_result_error_with_message(
        snapshot
            .overwrite(schema_ref! { nullable "replacement": STRING }, vec![])
            .build(&engine, Box::new(FileSystemCommitter::new())),
        "invariants",
    );
    Ok(())
}

#[rstest]
#[case("clustering")]
#[case("allowColumnDefaults")]
#[tokio::test]
async fn overwrite_rejects_schema_dependent_features(
    #[case] feature: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, location) = engine_store_setup("overwrite-features", None);
    let location = test_utils::create_table(
        store,
        location,
        schema_ref! { nullable "amount": INTEGER },
        &[],
        true,
        vec![],
        vec![feature, "domainMetadata"],
    )
    .await?;
    let snapshot = Snapshot::builder_for(location).build(&engine)?;
    assert_result_error_with_message(
        snapshot
            .overwrite(schema_ref! { nullable "replacement": STRING }, vec![])
            .build(&engine, Box::new(FileSystemCommitter::new())),
        feature,
    );
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_does_not_recycle_dropped_mapping_ids(
    #[values("name", "id")] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &path,
        schema_ref! { nullable "kept": INTEGER, nullable "dropped": INTEGER },
        engine.as_ref(),
        &[("delta.columnMapping.mode", mapping)],
    )?;
    let original_schema = snapshot.schema();
    let dropped = original_schema.field("dropped").unwrap();
    let original_max = snapshot
        .table_configuration()
        .table_properties()
        .column_mapping_max_column_id
        .unwrap();
    assert_eq!(dropped.column_mapping_id(), Some(original_max));
    let post = snapshot
        .overwrite(schema_ref! { nullable "kept": INTEGER }, vec![])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        post.table_configuration()
            .table_properties()
            .column_mapping_max_column_id,
        Some(original_max)
    );
    let post = post
        .overwrite(
            schema_ref! { nullable "kept": INTEGER, nullable "dropped": INTEGER },
            vec![],
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let new_schema = post.schema();
    let reintroduced = new_schema.field("dropped").unwrap();
    assert!(reintroduced.column_mapping_id().unwrap() > original_max);
    assert_ne!(
        reintroduced.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName),
        dropped.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    );
    assert_eq!(
        new_schema.field("kept").unwrap().metadata,
        original_schema.field("kept").unwrap().metadata
    );
    Ok(())
}
