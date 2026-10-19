//! Collation metadata passthrough through table creation, writes and log reconstruction.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::{col, column_name, lit};
use delta_kernel::schema::{
    schema_ref, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
    StructType,
};
use delta_kernel::snapshot::{CheckpointWriteResult, ChecksumWriteResult};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::{
    add_commit, assert_result_error_with_message, read_add_infos, read_scan, test_table_setup,
    test_table_setup_mt, write_batch_to_table,
};

use super::{
    annotated_field, assert_rows, committer, create_fixture, generated_table, mapped_bounds,
    physical_name, physical_path, rewrite_as_preview_table,
};
use crate::common::write_utils::{resolve_json_path, rewrite_commit};

fn assert_annotations(logical: &StructType, physical: &StructType, mapped: bool) {
    for (logical, physical) in logical.fields().zip(physical.fields()) {
        assert_eq!(
            physical.name(),
            if mapped {
                physical_name(logical)
            } else {
                logical.name()
            }
        );
        let expected = logical
            .get_config_value(&ColumnMetadataKey::Collations)
            .map(|annotation| match annotation {
                MetadataValue::Other(Value::Object(collations)) if mapped => {
                    MetadataValue::Other(Value::Object(
                        collations
                            .iter()
                            .map(|(path, identifier)| {
                                let suffix = path.strip_prefix(logical.name()).unwrap();
                                (format!("{}{suffix}", physical.name()), identifier.clone())
                            })
                            .collect(),
                    ))
                }
                annotation => annotation.clone(),
            });
        assert_eq!(
            physical.get_config_value(&ColumnMetadataKey::Collations),
            expected.as_ref(),
        );
        if let (DataType::Struct(left), DataType::Struct(right)) =
            (logical.data_type(), physical.data_type())
        {
            assert_annotations(left, right, mapped);
        }
    }
}

#[rstest]
#[tokio::test]
async fn reads_all_collation_bounds_and_annotations(
    #[values(false, true)] nested: bool,
    #[values("none", "name", "id")] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fields: &[&str] = if nested {
        &["first", "last", "email"]
    } else {
        &["name"]
    };
    let fixture = generated_table(nested, fields);
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &fixture,
        &[("delta.columnMapping.mode", mapping)],
    )
    .await?;
    let config = snapshot.table_configuration();
    assert!(config.is_feature_supported(&TableFeature::Collations));
    assert!(!config.is_feature_supported(&TableFeature::CollationsPreview));
    assert!(config.is_feature_supported(&TableFeature::DomainMetadata));
    assert!(!config
        .protocol()
        .reader_features()
        .unwrap()
        .contains(&TableFeature::Collations));
    let scan = snapshot.clone().scan_builder().build()?;
    assert_annotations(&fixture.schema, &snapshot.schema(), false);
    assert_annotations(&snapshot.schema(), scan.physical_schema(), true);
    let batches = read_scan(&scan, engine.clone())?;
    assert_rows(&fixture, &batches);
    for batch in &batches {
        let recovered = StructType::try_from_arrow(batch.schema())?;
        assert_annotations(&snapshot.schema(), &recovered, false);
    }
    let adds = read_add_infos(&snapshot, engine.as_ref())?;
    assert_eq!(adds.len(), fixture.files.len());
    for add in adds {
        let stats = add.stats.unwrap();
        let expected = fixture
            .files
            .iter()
            .find(|(batch, _)| {
                let id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<delta_kernel::arrow::array::Int32Array>()
                    .unwrap();
                stats["minValues"][physical_name(snapshot.schema().field("id").unwrap())]
                    == id.value(0)
            })
            .unwrap();
        assert_eq!(stats["numRecords"], expected.0.num_rows());
        let collation_stats = expected.1.as_ref().unwrap();
        assert_eq!(
            stats["statsWithCollation"][collation_stats.identifier],
            mapped_bounds(&collation_stats.bounds, &snapshot.schema())
        );
        if nested && expected.0.num_rows() == 2 {
            let email = physical_path(&snapshot, &column_name!("email"));
            // Binary bounds put uppercase B before lowercase a; the fixture's collation bounds
            // reverse that order.
            assert_eq!(
                resolve_json_path(&stats["minValues"], &email),
                "Bob.Dylan@Example.Com"
            );
            assert_eq!(
                resolve_json_path(&stats["maxValues"], &email),
                "alice.johnson@example.com"
            );
        }
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn predicates_use_binary_semantics_on_collated_columns(
    #[values(false, true)] nested: bool,
    #[values("none", "name", "id")] mapping: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = generated_table(
        nested,
        if nested {
            &["first", "last", "email"]
        } else {
            &["name"]
        },
    );
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &fixture,
        &[("delta.columnMapping.mode", mapping)],
    )
    .await?;
    let (column, value, expected) = if nested {
        (col!("name.first"), "charlie", 0)
    } else {
        (col!("name"), "M\u{fc}ller", 1)
    };
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(column.eq(lit(value))))
        .build()?;
    assert_eq!(
        read_scan(&scan, engine.clone())?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        expected
    );
    if nested {
        let scan = snapshot
            .scan_builder()
            .with_predicate(Arc::new(col!("name.first").eq(lit("alice"))))
            .build()?;
        let batches = read_scan(&scan, engine)?;
        let expected = pretty_format_batches(&[fixture.files[1].0.clone()])?.to_string();
        let mut expected: Vec<_> = expected.lines().collect();
        sort_lines!(expected);
        assert_batches_sorted_eq!(expected, &batches);
    }
    Ok(())
}

#[rstest]
#[case::automatic(None, TableFeature::Collations, TableFeature::CollationsPreview)]
#[case::stable(
    Some("collations"),
    TableFeature::Collations,
    TableFeature::CollationsPreview
)]
#[case::existing_preview(None, TableFeature::CollationsPreview, TableFeature::Collations)]
#[tokio::test]
async fn create_stable_tables_and_preserve_existing_features_on_append(
    #[case] signal: Option<&str>,
    #[case] feature: TableFeature,
    #[case] absent: TableFeature,
    #[values(false, true)] nested: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = generated_table(
        nested,
        if nested {
            &["first", "last", "email"]
        } else {
            &["name"]
        },
    );
    let properties: Vec<_> = signal
        .map(|signal| (format!("delta.feature.{signal}"), "supported".to_string()))
        .into_iter()
        .collect();
    let properties: Vec<_> = properties
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let mut snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    if feature == TableFeature::CollationsPreview {
        rewrite_as_preview_table(&path)?;
        snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    }
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&feature));
    assert!(!snapshot.table_configuration().is_feature_supported(&absent));
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::DomainMetadata));
    let protocol = snapshot.table_configuration().protocol().clone();
    let schema = snapshot.schema();
    let snapshot = write_batch_to_table(
        &snapshot,
        engine.as_ref(),
        fixture.files[0].0.clone(),
        HashMap::new(),
    )
    .await?;
    let reloaded = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), snapshot.version());
    assert_eq!(reloaded.schema(), schema);
    assert_eq!(reloaded.table_configuration().protocol(), &protocol);
    let adds = read_add_infos(&reloaded, engine.as_ref())?;
    assert_eq!(adds.len(), fixture.files.len() + 1);
    assert_eq!(
        adds.iter()
            .filter(|add| {
                add.stats
                    .as_ref()
                    .unwrap()
                    .get("statsWithCollation")
                    .is_none()
            })
            .count(),
        1
    );
    let initial_rows: usize = fixture
        .files
        .iter()
        .map(|(batch, _)| batch.num_rows())
        .sum();
    assert_eq!(
        read_scan(&reloaded.scan_builder().build()?, engine)?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        initial_rows + fixture.files[0].0.num_rows()
    );
    Ok(())
}

#[rstest]
#[case::string(DataType::STRING, "value")]
#[case::array(ArrayType::new(DataType::STRING, true).into(), "value.element")]
#[case::map(MapType::new(DataType::STRING, DataType::STRING, true).into(), "value.value")]
#[tokio::test]
async fn arrow_round_trip_schema_can_create_table(
    #[case] data_type: DataType,
    #[case] target: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let schema = schema_ref! { (annotated_field("value", data_type, target, "test.ASCII_CI")) };
    let arrow: ArrowSchema = schema.as_ref().try_into_arrow()?;
    let recovered = Arc::new(StructType::try_from_arrow(&arrow)?);
    assert_eq!(schema, recovered);
    let snapshot = create_table(&path, recovered, "test", false)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(snapshot.schema(), schema);
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::Collations));
    Ok(())
}

#[rstest]
#[case::missing_feature(None, false)]
#[case::stable(Some("collations"), true)]
#[case::preview(Some("collations-preview"), true)]
#[tokio::test]
async fn alter_requires_existing_feature_without_upgrade(
    #[case] signal: Option<&str>,
    #[case] supported: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let mut builder = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test", false);
    if signal.is_some() {
        builder = builder.with_table_properties([("delta.feature.collations", "supported")]);
    }
    let mut snapshot = builder
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    if signal == Some("collations-preview") {
        rewrite_as_preview_table(&path)?;
        snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    }
    let protocol = snapshot.table_configuration().protocol().clone();
    let collated_field = annotated_field("name", DataType::STRING, "name", "test.ASCII_CI");
    let result = snapshot
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .add_column(collated_field.clone())
        .build(engine.as_ref(), committer());
    if supported {
        let updated = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let reloaded = Snapshot::builder_for(&path).build(engine.as_ref())?;
        assert_eq!(reloaded.version(), updated.version());
        assert_eq!(reloaded.table_configuration().protocol(), &protocol);
        assert_eq!(reloaded.schema().field("name"), Some(&collated_field));
    } else {
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("requires the 'collations'"));
        assert_eq!(
            Snapshot::builder_for(&path)
                .build(engine.as_ref())?
                .version(),
            0
        );
    }
    let malformed = StructField::nullable("bad", DataType::INTEGER).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        MetadataValue::Other(json!({ "bad": "test.ASCII_CI" })),
    )]);
    let error = snapshot
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .add_column(malformed)
        .build(engine.as_ref(), committer())
        .err()
        .unwrap();
    assert!(error.to_string().contains("STRING target"));
    Ok(())
}

#[rstest]
#[case::array(DataType::from(ArrayType::new(
    schema_ref! { (annotated_field("name", DataType::STRING, "name", "test.ASCII_CI")) }.as_ref().clone(),
    true,
)))]
#[case::map(DataType::from(MapType::new(
    DataType::STRING,
    schema_ref! { (annotated_field("name", DataType::STRING, "name", "test.ASCII_CI")) }.as_ref().clone(),
    true,
)))]
#[tokio::test]
async fn annotations_inside_container_structs_auto_enable_feature(
    #[case] container: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "items": (container) };
    assert!(schema
        .field("items")
        .unwrap()
        .get_config_value(&ColumnMetadataKey::Collations)
        .is_none());
    let snapshot = create_table(&path, schema.clone(), "test", false)
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    assert_eq!(snapshot.schema(), schema);
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::Collations));
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::DomainMetadata));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn crc_and_time_travel_keep_schema_and_protocol_at_each_version(
    #[values(false, true)] nested: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let fixture = generated_table(nested, &[]);
    let before = create_fixture(&path, engine.as_ref(), &fixture, &[]).await?;
    let old_schema = before.schema();
    let old_protocol = before.table_configuration().protocol().clone();
    assert!(!before
        .table_configuration()
        .is_feature_supported(&TableFeature::Collations));
    assert_eq!(
        before.write_checksum(engine.as_ref())?.0,
        ChecksumWriteResult::Written
    );
    let collated = generated_table(
        nested,
        if nested {
            &["first", "last", "email"]
        } else {
            &["name"]
        },
    );
    let mut metadata = serde_json::to_value(before.table_configuration().metadata())?;
    metadata["schemaString"] = Value::String(serde_json::to_string(&collated.schema)?);
    let version = before.version() + 1;
    let table_root = before.table_root().as_str();
    let store = engine
        .get_object_store_for_url(before.table_root())
        .unwrap();
    // Simulate a writer upgrading the protocol; ALTER cannot enable table features.
    add_commit(
        table_root,
        store.as_ref(),
        version,
        format!(
            "{}\n{}",
            json!({
                "protocol": { "minReaderVersion": 3, "minWriterVersion": 7,
                    "readerFeatures": [], "writerFeatures": ["collations", "domainMetadata"] },
            }),
            json!({ "metaData": metadata })
        ),
    )
    .await?;
    let after = Snapshot::builder_for(&path).build(engine.as_ref())?;
    assert_eq!(after.schema(), collated.schema);
    assert!(after
        .table_configuration()
        .is_feature_supported(&TableFeature::Collations));
    assert_eq!(
        after.write_checksum(engine.as_ref())?.0,
        ChecksumWriteResult::Written
    );
    for (version, schema, protocol) in [
        (before.version(), old_schema, old_protocol),
        (
            after.version(),
            after.schema(),
            after.table_configuration().protocol().clone(),
        ),
    ] {
        let reloaded = Snapshot::builder_for(&path)
            .at_version(version)
            .build(engine.as_ref())?;
        assert_eq!(reloaded.schema(), schema);
        assert_eq!(reloaded.table_configuration().protocol(), &protocol);
        let crc = reloaded.crc_at_version().unwrap();
        assert_eq!(crc.protocol, protocol);
        let crc_schema = crc.metadata.parse_schema()?;
        assert_eq!(&crc_schema, schema.as_ref());
        assert_rows(
            &fixture,
            &read_scan(&reloaded.scan_builder().build()?, engine.clone())?,
        );
    }
    Ok(())
}

#[rstest]
#[case::text(json!("test.ASCII_CI"), "JSON object")]
#[case::non_string(json!({ "bad": 1 }), "string identifier")]
#[case::bad_identifier(json!({ "bad": "ASCII_CI" }), "invalid identifier")]
#[case::wrong_path(json!({ "other": "test.ASCII_CI" }), "STRING target")]
#[tokio::test]
async fn malformed_annotations_rejected_before_create_and_alter(
    #[case] metadata: Value,
    #[case] message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    let invalid = StructField::nullable("bad", DataType::STRING).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        MetadataValue::Other(metadata),
    )]);
    let create = create_table(&path, schema_ref! { (invalid.clone()) }, "test", false)
        .build(engine.as_ref(), committer());
    assert_result_error_with_message(create, message);
    let snapshot = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test", false)
        .with_table_properties([("delta.feature.collations", "supported")])
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let alter = snapshot
        .update_table(UpdateTableOperation::AlterTable, "test", false)
        .add_column(invalid)
        .build(engine.as_ref(), committer());
    assert_result_error_with_message(alter, message);
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .version(),
        0
    );
    Ok(())
}

#[rstest]
#[case::non_object(json!("test.ASCII_CI"), "JSON object")]
#[case::non_string(json!({ "name": 1 }), "string identifier")]
#[case::invalid_identifier(json!({ "name": "ASCII_CI" }), "invalid identifier")]
#[case::versioned_identifier(json!({ "name": "test.ASCII_CI.1" }), "invalid identifier")]
#[case::default_binary(json!({ "name": "spark.UTF8_BINARY" }), "invalid identifier")]
#[case::stale_path(json!({ "other": "test.ASCII_CI" }), "STRING target")]
#[tokio::test]
async fn snapshot_loading_rejects_malformed_collation_annotations(
    #[case] annotation: Value,
    #[case] message: &str,
    #[values("collations", "collations-preview")] feature: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    create_table(
        &path,
        schema_ref! { nullable "name": STRING },
        "test",
        false,
    )
    .with_table_properties([("delta.feature.collations", "supported")])
    .build(engine.as_ref(), committer())?
    .commit(engine.as_ref())?
    .unwrap_committed();
    if feature == "collations-preview" {
        rewrite_as_preview_table(&path)?;
    }
    let schema = schema_ref! {
        (StructField::nullable("name", DataType::STRING).with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            MetadataValue::Other(annotation),
        )]))
    };
    rewrite_commit(&path, 0, |action| {
        if let Some(metadata) = action.get_mut("metaData") {
            metadata["schemaString"] = Value::String(serde_json::to_string(&schema)?);
        }
        Ok(())
    })?;
    let error = Snapshot::builder_for(&path)
        .build(engine.as_ref())
        .err()
        .unwrap();
    assert!(error.to_string().contains(message), "{error}");
    Ok(())
}

#[rstest]
#[case::legacy(json!({ "minReaderVersion": 1, "minWriterVersion": 2 }))]
#[case::writer_features(json!({
    "minReaderVersion": 3, "minWriterVersion": 7,
    "readerFeatures": [], "writerFeatures": []
}))]
#[tokio::test]
async fn snapshot_loading_requires_collation_feature_for_annotated_schema(
    #[case] protocol: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup()?;
    create_table(
        &path,
        schema_ref! { nullable "name": STRING },
        "test",
        false,
    )
    .build(engine.as_ref(), committer())?
    .commit(engine.as_ref())?
    .unwrap_committed();
    let schema = schema_ref! {
        (annotated_field("name", DataType::STRING, "name", "test.ASCII_CI"))
    };
    rewrite_commit(&path, 0, |action| {
        if let Some(target) = action.get_mut("protocol") {
            *target = protocol.clone();
        }
        if let Some(metadata) = action.get_mut("metaData") {
            metadata["schemaString"] = Value::String(serde_json::to_string(&schema)?);
        }
        Ok(())
    })?;
    let error = Snapshot::builder_for(&path)
        .build(engine.as_ref())
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("requires the 'collations'"),
        "{error}"
    );
    Ok(())
}

#[rstest]
#[case::v1(CheckpointSpec::V1, false)]
#[case::v2_inline(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar), true)]
#[case::v2_sidecars(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
    file_actions_per_sidecar_hint: Some(1),
}), true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_repeatedly_preserves_multiple_identifiers_and_sparse_bounds(
    #[case] spec: CheckpointSpec,
    #[case] v2: bool,
    #[values("none", "name", "id")] mapping: &str,
    #[values((true, false), (false, true), (true, true))] stats_mode: (bool, bool),
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let (write_json, write_struct) = stats_mode;
    let fixture = generated_table(true, &["first", "last", "email"]);
    let mut properties = vec![
        (
            "delta.checkpoint.writeStatsAsStruct",
            if write_struct { "true" } else { "false" },
        ),
        (
            "delta.checkpoint.writeStatsAsJson",
            if write_json { "true" } else { "false" },
        ),
        ("delta.columnMapping.mode", mapping),
    ];
    if v2 {
        properties.push(("delta.feature.v2Checkpoint", "supported"));
    }
    let mut snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    let leaf = physical_path(&snapshot, &column_name!("name.first"));
    for version in 1..=snapshot.version() {
        rewrite_commit(&path, version, |action| {
            if let Some(add) = action.get_mut("add") {
                let mut stats: Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
                let mut value = json!("Alice");
                for segment in leaf.iter().rev() {
                    value = json!({ segment: value });
                }
                stats["statsWithCollation"]["test.ASCII_CI.1"] = json!({ "minValues": value });
                stats["statsWithCollation"]["test.ASCII_CI.2"] = json!({ "maxValues": value });
                add["stats"] = Value::String(stats.to_string());
            }
            Ok(())
        })?;
    }
    snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let expected: HashMap<_, _> = read_add_infos(&snapshot, engine.as_ref())?
        .into_iter()
        .map(|add| (add.path, add.stats.unwrap()))
        .collect();
    for round in 0..3 {
        assert_eq!(
            snapshot.checkpoint(engine.as_ref(), Some(&spec))?.0,
            CheckpointWriteResult::Written
        );
        snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
        assert_eq!(
            snapshot.log_segment().checkpoint_version,
            Some(snapshot.version())
        );
        let actual = read_add_infos(&snapshot, engine.as_ref())?;
        assert_eq!(actual.len(), expected.len());
        for add in actual {
            let mut actual = add.stats.unwrap();
            let expected = &expected[&add.path];
            if write_json {
                assert_eq!(&actual, expected);
            } else {
                // Struct-only JSON synthesis preserves collation bounds without binary bounds.
                for bounds in actual["statsWithCollation"]
                    .as_object_mut()
                    .unwrap()
                    .values_mut()
                {
                    remove_null_padding(bounds);
                }
                assert_eq!(actual["statsWithCollation"], expected["statsWithCollation"]);
                assert_eq!(actual["numRecords"], expected["numRecords"]);
            }
        }
        assert_rows(
            &fixture,
            &read_scan(&snapshot.clone().scan_builder().build()?, engine.clone())?,
        );
        if round < 2 {
            snapshot = snapshot
                .update_table(UpdateTableOperation::AlterTable, "test", false)
                .set_nullable(column_name!("id"))
                .build(engine.as_ref(), committer())?
                .commit(engine.as_ref())?
                .unwrap_post_commit_snapshot();
        }
    }
    Ok(())
}

fn remove_null_padding(value: &mut Value) {
    if let Value::Object(fields) = value {
        fields.retain(|_, value| {
            remove_null_padding(value);
            !value.is_null() && value.as_object().is_none_or(|fields| !fields.is_empty())
        });
    }
}
