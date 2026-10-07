//! Collation metadata passthrough through table creation, writes and log reconstruction.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, StructArray};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::expressions::{col, column_name, lit, ColumnName};
use delta_kernel::schema::{
    schema_ref, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, SchemaRef,
    StructField, StructType,
};
use delta_kernel::snapshot::{CheckpointWriteResult, ChecksumWriteResult};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Snapshot, SnapshotRef};
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, generate_batch, read_add_infos, read_scan, test_table_setup, test_table_setup_mt,
    write_batch_to_table, IntoArray,
};

use crate::common::write_utils::{resolve_json_path, rewrite_commit};

mod connector;

struct CollationTable {
    schema: SchemaRef,
    files: Vec<(RecordBatch, Value)>,
    collation_id: &'static str,
}

fn annotated_field(
    name: &str,
    data_type: impl Into<DataType>,
    path: &str,
    id: &str,
) -> StructField {
    StructField::nullable(name, data_type).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        MetadataValue::Other(json!({ path: id })),
    )])
}

fn generated_table(nested: bool, collated_fields: &[&str]) -> CollationTable {
    let annotation = if nested {
        "spark.UTF8_LCASE"
    } else {
        "icu.UNICODE_CI"
    };
    let field = |name: &str| {
        if collated_fields.contains(&name) {
            annotated_field(name, DataType::STRING, name, annotation)
        } else {
            StructField::nullable(name, DataType::STRING)
        }
    };
    let mut fixture = if nested {
        CollationTable {
            schema: schema_ref! {
                nullable "id": INTEGER,
                nullable "name": { (field("first")), (field("last")) },
                (field("email")),
                nullable "department": STRING,
            },
            files: vec![
                (
                    nested_batch(&[
                        (
                            1,
                            "Alice",
                            "Johnson",
                            "Alice.Johnson@example.com",
                            "Engineering",
                        ),
                        (2, "Bob", "Smith", "bob.smith@example.com", "Marketing"),
                        (3, "Charlie", "Brown", "charlie@example.com", "Engineering"),
                    ]),
                    json!({
                        "minValues": { "name": { "first": "Alice", "last": "Brown" },
                            "email": "Alice.Johnson@example.com" },
                        "maxValues": { "name": { "first": "Charlie", "last": "Smith" },
                            "email": "charlie@example.com" },
                    }),
                ),
                (
                    nested_batch(&[
                        (
                            4,
                            "alice",
                            "johnson",
                            "alice.johnson@example.com",
                            "Engineering",
                        ),
                        (5, "bob", "dylan", "Bob.Dylan@Example.Com", "Sales"),
                    ]),
                    json!({
                        "minValues": { "name": { "first": "alice", "last": "dylan" },
                            "email": "alice.johnson@example.com" },
                        "maxValues": { "name": { "first": "bob", "last": "johnson" },
                            "email": "Bob.Dylan@Example.Com" },
                    }),
                ),
            ],
            collation_id: "spark.UTF8_LCASE.75.1",
        }
    } else {
        CollationTable {
            schema: schema_ref! { nullable "id": INTEGER, (field("name")) },
            files: [(1, "M\u{fc}ller"), (2, "M\u{dc}LLER"), (3, "m\u{fc}ller")]
                .into_iter()
                .map(|(id, name)| {
                    (
                        simple_batch(vec![id], vec![name]),
                        json!({ "minValues": { "name": name }, "maxValues": { "name": name } }),
                    )
                })
                .collect(),
            collation_id: "icu.UNICODE_CI.75.1",
        }
    };
    if collated_fields.is_empty() {
        for (_, bounds) in &mut fixture.files {
            *bounds = Value::Null;
        }
    }
    fixture
}

fn simple_batch(ids: Vec<i32>, names: Vec<&'static str>) -> RecordBatch {
    generate_batch(vec![
        ("id", ids.into_arrow_array()),
        ("name", names.into_arrow_array()),
    ])
    .unwrap()
}

fn nested_batch(
    rows: &[(i32, &'static str, &'static str, &'static str, &'static str)],
) -> RecordBatch {
    let name = generate_batch(vec![
        (
            "first",
            rows.iter()
                .map(|row| row.1)
                .collect::<Vec<_>>()
                .into_arrow_array(),
        ),
        (
            "last",
            rows.iter()
                .map(|row| row.2)
                .collect::<Vec<_>>()
                .into_arrow_array(),
        ),
    ])
    .unwrap();
    generate_batch(vec![
        (
            "id",
            rows.iter()
                .map(|row| row.0)
                .collect::<Vec<_>>()
                .into_arrow_array(),
        ),
        ("name", Arc::new(StructArray::from(name)) as ArrayRef),
        (
            "email",
            rows.iter()
                .map(|row| row.3)
                .collect::<Vec<_>>()
                .into_arrow_array(),
        ),
        (
            "department",
            rows.iter()
                .map(|row| row.4)
                .collect::<Vec<_>>()
                .into_arrow_array(),
        ),
    ])
    .unwrap()
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

fn physical_name(field: &StructField) -> &str {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(name)) => name,
        _ => field.name(),
    }
}

fn physical_bounds(bounds: &Value, schema: &StructType) -> Value {
    Value::Object(
        bounds
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| {
                let field = schema.field(name).unwrap();
                let value = match field.data_type() {
                    DataType::Struct(child) => physical_bounds(value, child),
                    _ => value.clone(),
                };
                (physical_name(field).to_string(), value)
            })
            .collect(),
    )
}

fn physical_path(snapshot: &Snapshot, logical: &ColumnName) -> Vec<String> {
    snapshot
        .schema()
        .fields_of_path(logical)
        .unwrap()
        .iter()
        .map(|field| physical_name(field).to_string())
        .collect()
}

fn mapped_bounds(bounds: &Value, schema: &StructType) -> Value {
    Value::Object(
        bounds
            .as_object()
            .unwrap()
            .iter()
            .map(|(bound, values)| (bound.clone(), physical_bounds(values, schema)))
            .collect(),
    )
}

async fn create_fixture(
    table_path: &str,
    engine: &DefaultEngine<impl TaskExecutor>,
    fixture: &CollationTable,
    properties: &[(&str, &str)],
) -> Result<SnapshotRef, Box<dyn std::error::Error>> {
    let mut snapshot = create_table(table_path, fixture.schema.clone(), "collation-test")
        .with_table_properties(properties.iter().copied())
        .build(engine, committer())?
        .commit(engine)?
        .unwrap_post_commit_snapshot();
    for (batch, bounds) in &fixture.files {
        snapshot = write_batch_to_table(&snapshot, engine, batch.clone(), HashMap::new()).await?;
        if bounds.is_null() {
            continue;
        }
        let bounds = mapped_bounds(bounds, &snapshot.schema());
        // Simulate a writer supplying collation stats; Kernel itself computes only binary stats.
        let mut adds = 0;
        rewrite_commit(table_path, snapshot.version(), |action| {
            if let Some(add) = action.get_mut("add") {
                let mut stats: Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
                assert!(stats.get("statsWithCollation").is_none());
                stats["statsWithCollation"] = json!({ fixture.collation_id: bounds });
                add["stats"] = Value::String(stats.to_string());
                adds += 1;
            }
            Ok(())
        })?;
        assert_eq!(adds, 1);
        snapshot = Snapshot::builder_for(table_path).build(engine)?;
    }
    Ok(snapshot)
}

fn assert_rows(fixture: &CollationTable, actual: &[RecordBatch]) {
    let expected: Vec<_> = fixture
        .files
        .iter()
        .map(|(batch, _)| batch.clone())
        .collect();
    let expected = pretty_format_batches(&expected).unwrap().to_string();
    let mut expected: Vec<_> = expected.lines().collect();
    sort_lines!(expected);
    assert_batches_sorted_eq!(expected, actual);
}

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
        assert_eq!(
            logical.get_config_value(&ColumnMetadataKey::Collations),
            physical.get_config_value(&ColumnMetadataKey::Collations),
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
        assert_eq!(
            stats["statsWithCollation"][fixture.collation_id],
            mapped_bounds(&expected.1, &snapshot.schema())
        );
        if nested && expected.0.num_rows() == 2 {
            let email = physical_path(&snapshot, &column_name!("email"));
            // Uppercase B sorts before lowercase a in binary order; these supplied bounds reverse
            // it.
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
#[case::preview(
    Some("collations-preview"),
    TableFeature::CollationsPreview,
    TableFeature::Collations
)]
#[tokio::test]
async fn create_and_append_preserve_selected_feature(
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
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
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
    let snapshot = create_table(&path, recovered, "test")
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
    let mut builder = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test");
    if let Some(signal) = signal {
        builder = builder.with_table_properties([(format!("delta.feature.{signal}"), "supported")]);
    }
    let snapshot = builder
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let protocol = snapshot.table_configuration().protocol().clone();
    let result = snapshot
        .clone()
        .alter_table()
        .add_column(annotated_field(
            "name",
            DataType::STRING,
            "name",
            "test.ASCII_CI",
        ))
        .build(engine.as_ref(), committer());
    if supported {
        let updated = result?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        assert_eq!(updated.table_configuration().protocol(), &protocol);
        assert!(updated
            .schema()
            .field("name")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations)
            .is_some());
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
        .alter_table()
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
    let snapshot = create_table(&path, schema.clone(), "test")
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
    let create = create_table(&path, schema_ref! { (invalid.clone()) }, "test")
        .build(engine.as_ref(), committer());
    assert!(create.err().unwrap().to_string().contains(message));
    let snapshot = create_table(&path, schema_ref! { nullable "id": INTEGER }, "test")
        .with_table_properties([("delta.feature.collations", "supported")])
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let alter = snapshot
        .alter_table()
        .add_column(invalid)
        .build(engine.as_ref(), committer());
    assert!(alter.err().unwrap().to_string().contains(message));
    assert_eq!(
        Snapshot::builder_for(&path)
            .build(engine.as_ref())?
            .version(),
        0
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
    #[values(false, true)] write_json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let fixture = generated_table(true, &["first", "last", "email"]);
    let mut properties = vec![
        ("delta.checkpoint.writeStatsAsStruct", "true"),
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
            let actual = add.stats.unwrap();
            let expected = &expected[&add.path];
            for (identifier, bounds) in expected["statsWithCollation"].as_object().unwrap() {
                for (bound, values) in bounds.as_object().unwrap() {
                    assert_eq!(&actual["statsWithCollation"][identifier][bound], values);
                }
            }
            for name in ["numRecords", "minValues", "maxValues", "nullCount"] {
                assert_eq!(actual[name], expected[name]);
            }
        }
        assert_rows(
            &fixture,
            &read_scan(&snapshot.clone().scan_builder().build()?, engine.clone())?,
        );
        if round < 2 {
            snapshot = snapshot
                .alter_table()
                .set_nullable(column_name!("id"))
                .build(engine.as_ref(), committer())?
                .commit(engine.as_ref())?
                .unwrap_post_commit_snapshot();
        }
    }
    Ok(())
}
