//! Collation table-feature passthrough integration tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array as _, ArrayRef, Int32Array, StringArray, StructArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::{col, column_name, lit};
use delta_kernel::schema::{
    schema_ref, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Error};
use rstest::rstest;
use serde_json::{Map, Value};
use tempfile::TempDir;
use test_utils::{
    copy_directory, create_default_engine, read_add_infos, read_scan, test_table_setup,
    write_batch_to_table,
};
use url::Url;

const COLLATION_NAME: &str = "spark.UTF8_LCASE";

fn collation_metadata(path: &str) -> MetadataValue {
    MetadataValue::Other(Value::Object(Map::from_iter([(
        path.to_string(),
        Value::String(COLLATION_NAME.to_string()),
    )])))
}

fn collated_field(name: &str) -> StructField {
    StructField::nullable(name, DataType::STRING).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        collation_metadata(name),
    )])
}

fn committer() -> Box<FileSystemCommitter> {
    Box::new(FileSystemCommitter::new())
}

fn fixture_url(name: &str) -> DeltaResult<Url> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data").join(name))
        .map_err(|err| Error::generic(err.to_string()))?;
    path_to_url(&path)
}

fn copy_fixture(name: &str) -> Result<(TempDir, Url), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let destination = temp_dir.path().join(name);
    copy_directory(&PathBuf::from("./tests/data").join(name), &destination)?;
    let url = path_to_url(&destination)?;
    Ok((temp_dir, url))
}

fn path_to_url(path: &Path) -> DeltaResult<Url> {
    Url::from_directory_path(path)
        .map_err(|_| Error::generic(format!("Invalid table path: {}", path.display())))
}

fn simple_rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), names.value(row).to_string()));
        }
    }
    rows.sort();
    rows
}

fn nested_first_values(batches: &[RecordBatch]) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let first = names
            .column_by_name("first")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        values.extend(first.iter().flatten().map(str::to_string));
    }
    values.sort();
    values
}

fn simple_append_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap()
}

fn complex_append_batch() -> RecordBatch {
    let name = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("first", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["Test"])) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("last", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["User"])) as ArrayRef,
        ),
    ]);
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", name.data_type().clone(), true),
            ArrowField::new("email", ArrowDataType::Utf8, true),
            ArrowField::new("department", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![6])),
            Arc::new(name),
            Arc::new(StringArray::from(vec!["test.user@example.com"])),
            Arc::new(StringArray::from(vec!["Engineering"])),
        ],
    )
    .unwrap()
}

#[test]
fn reads_external_collation_table_with_binary_predicate_semantics(
) -> Result<(), Box<dyn std::error::Error>> {
    let table_url = fixture_url("collations")?;
    let engine = create_default_engine(&table_url)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_config = snapshot.table_configuration();
    assert!(table_config.is_feature_supported(&TableFeature::Collations));
    assert!(!table_config.is_feature_supported(&TableFeature::CollationsPreview));
    assert!(table_config.is_feature_supported(&TableFeature::DomainMetadata));
    assert_eq!(
        snapshot
            .schema()
            .field("name")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&MetadataValue::Other(serde_json::json!({
            "name": "icu.UNICODE_CI"
        })))
    );

    let batches = read_scan(&snapshot.clone().scan_builder().build()?, engine.clone())?;
    assert_eq!(
        simple_rows(&batches),
        vec![
            (1, "Müller".to_string()),
            (2, "MÜLLER".to_string()),
            (3, "müller".to_string()),
        ]
    );
    for batch in &batches {
        assert_eq!(
            batch
                .schema()
                .field(1)
                .metadata()
                .get(ColumnMetadataKey::Collations.as_ref())
                .map(String::as_str),
            Some(r#"{"name":"icu.UNICODE_CI"}"#)
        );
    }

    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(col!("name").eq(lit("Müller"))))
        .build()?;
    assert_eq!(
        simple_rows(&read_scan(&scan, engine)?),
        vec![(1, "Müller".to_string())]
    );
    Ok(())
}

#[test]
fn reads_external_nested_collations_with_binary_predicate_semantics(
) -> Result<(), Box<dyn std::error::Error>> {
    let table_url = fixture_url("collations-complex")?;
    let engine = create_default_engine(&table_url)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let schema = snapshot.schema();
    let DataType::Struct(name) = schema.field("name").unwrap().data_type() else {
        panic!("name must be a struct")
    };
    assert_eq!(
        name.field("first")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&MetadataValue::Other(serde_json::json!({
            "first": COLLATION_NAME
        })))
    );
    assert_eq!(
        name.field("last")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&MetadataValue::Other(serde_json::json!({
            "last": COLLATION_NAME
        })))
    );
    assert_eq!(
        schema
            .field("email")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&MetadataValue::Other(serde_json::json!({
            "email": COLLATION_NAME
        })))
    );

    let batches = read_scan(&snapshot.clone().scan_builder().build()?, engine.clone())?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    assert_eq!(
        nested_first_values(&batches),
        vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
            "alice".to_string(),
            "bob".to_string(),
        ]
    );
    for batch in &batches {
        let batch_schema = batch.schema();
        let ArrowDataType::Struct(name_fields) = batch_schema.field(1).data_type() else {
            panic!("name must be a struct")
        };
        for (field_name, expected) in [
            ("first", r#"{"first":"spark.UTF8_LCASE"}"#),
            ("last", r#"{"last":"spark.UTF8_LCASE"}"#),
        ] {
            assert_eq!(
                name_fields
                    .iter()
                    .find(|field| field.name() == field_name)
                    .unwrap()
                    .metadata()
                    .get(ColumnMetadataKey::Collations.as_ref())
                    .map(String::as_str),
                Some(expected)
            );
        }
        assert_eq!(
            batch_schema
                .field(2)
                .metadata()
                .get(ColumnMetadataKey::Collations.as_ref())
                .map(String::as_str),
            Some(r#"{"email":"spark.UTF8_LCASE"}"#)
        );
    }
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(col!("name.first").eq(lit("charlie"))))
        .build()?;
    assert_eq!(
        read_scan(&scan, engine.clone())?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(col!("name.first").eq(lit("alice"))))
        .build()?;
    assert_eq!(
        nested_first_values(&read_scan(&scan, engine)?),
        vec!["alice".to_string(), "bob".to_string()]
    );
    Ok(())
}

#[rstest]
#[case::simple("collations", 3, simple_append_batch)]
#[case::nested("collations-complex", 5, complex_append_batch)]
#[tokio::test]
async fn appends_to_external_collation_table_without_changing_schema_or_protocol(
    #[case] fixture: &str,
    #[case] initial_rows: usize,
    #[case] batch: fn() -> RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_url) = copy_fixture(fixture)?;
    let engine = create_default_engine(&table_url)?;
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let initial_version = snapshot.version();
    let initial_protocol = snapshot.table_configuration().protocol().clone();
    let initial_schema = snapshot.schema();

    write_batch_to_table(&snapshot, engine.as_ref(), batch(), HashMap::new()).await?;

    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), initial_version + 1);
    assert_eq!(reloaded.table_configuration().protocol(), &initial_protocol);
    assert_eq!(reloaded.schema().as_ref(), initial_schema.as_ref());
    assert_eq!(
        read_scan(&reloaded.clone().scan_builder().build()?, engine)?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        initial_rows + 1
    );
    Ok(())
}

#[rstest]
#[case::stable(None, TableFeature::Collations, TableFeature::CollationsPreview)]
#[case::preview(
    Some("collations-preview"),
    TableFeature::CollationsPreview,
    TableFeature::Collations
)]
#[tokio::test]
async fn collations_read_write_passthrough(
    #[case] feature_signal: Option<&str>,
    #[case] expected_feature: TableFeature,
    #[case] absent_feature: TableFeature,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let expected_metadata = collation_metadata("value");
    let schema = schema_ref! { (collated_field("value")) };
    let mut builder = create_table(&table_path, schema, "test");
    if let Some(feature) = feature_signal {
        builder = builder
            .with_table_properties([(format!("delta.feature.{feature}"), "supported".to_string())]);
    }
    let snapshot = builder
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let table_config = snapshot.table_configuration();
    assert!(table_config.is_feature_supported(&expected_feature));
    assert!(!table_config.is_feature_supported(&absent_feature));
    assert!(table_config.is_feature_supported(&TableFeature::DomainMetadata));
    assert_eq!(
        snapshot
            .schema()
            .field("value")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&expected_metadata)
    );

    let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Utf8,
        true,
    )]));
    assert!(input_schema.field(0).metadata().is_empty());
    let batch = RecordBatch::try_new(
        input_schema,
        vec![Arc::new(StringArray::from(vec!["a", "A"]))],
    )?;
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), snapshot.version());
    assert_eq!(
        reloaded
            .schema()
            .field("value")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&expected_metadata)
    );

    let batches = read_scan(&reloaded.clone().scan_builder().build()?, engine.clone())?;
    assert_eq!(batches.len(), 1);
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some("a"), Some("A")]
    );
    assert_eq!(
        batches[0]
            .schema()
            .field(0)
            .metadata()
            .get(ColumnMetadataKey::Collations.as_ref())
            .map(String::as_str),
        Some(r#"{"value":"spark.UTF8_LCASE"}"#)
    );

    let add_infos = read_add_infos(&reloaded, engine.as_ref())?;
    let stats = add_infos[0].stats.as_ref().unwrap();
    assert_eq!(stats["minValues"]["value"], "A");
    assert_eq!(stats["maxValues"]["value"], "a");
    assert!(stats.get("statsWithCollation").is_none());
    Ok(())
}

#[tokio::test]
async fn create_preserves_container_collation_metadata() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let array_metadata = collation_metadata("array_value.element");
    let map_metadata = collation_metadata("map_value.value");
    let schema = schema_ref! {
        (StructField::nullable(
            "array_value",
            ArrayType::new(DataType::STRING, true),
        )
        .with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            array_metadata.clone(),
        )])),
        (StructField::nullable(
            "map_value",
            MapType::new(DataType::STRING, DataType::STRING, true),
        )
        .with_metadata([(
            ColumnMetadataKey::Collations.as_ref(),
            map_metadata.clone(),
        )])),
    };
    let snapshot = create_table(&table_path, schema, "test")
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::Collations));
    assert_eq!(
        snapshot
            .schema()
            .field("array_value")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&array_metadata)
    );
    assert_eq!(
        snapshot
            .schema()
            .field("map_value")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&map_metadata)
    );
    Ok(())
}

#[rstest]
#[case::stable(
    "collations",
    TableFeature::Collations,
    TableFeature::CollationsPreview
)]
#[case::preview(
    "collations-preview",
    TableFeature::CollationsPreview,
    TableFeature::Collations
)]
#[tokio::test]
async fn alter_add_collated_column_with_enabled_feature(
    #[case] feature_signal: &str,
    #[case] expected_feature: TableFeature,
    #[case] absent_feature: TableFeature,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table(&table_path, schema_ref! { nullable "id": INTEGER }, "test")
        .with_table_properties([(
            format!("delta.feature.{feature_signal}"),
            "supported".to_string(),
        )])
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let initial_protocol = snapshot.table_configuration().protocol().clone();

    snapshot
        .alter_table()
        .add_column(collated_field("value"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let table_config = reloaded.table_configuration();
    assert_eq!(table_config.protocol(), &initial_protocol);
    assert!(table_config.is_feature_supported(&expected_feature));
    assert!(!table_config.is_feature_supported(&absent_feature));
    assert!(table_config.is_feature_supported(&TableFeature::DomainMetadata));
    assert_eq!(
        reloaded
            .schema()
            .field("value")
            .unwrap()
            .get_config_value(&ColumnMetadataKey::Collations),
        Some(&collation_metadata("value"))
    );
    Ok(())
}

#[tokio::test]
async fn alter_add_collated_column_requires_enabled_feature() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let snapshot = create_table(&table_path, schema_ref! { nullable "id": INTEGER }, "test")
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let error = snapshot
        .alter_table()
        .add_column(collated_field("value"))
        .build(engine.as_ref(), committer())
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires the 'collations' or 'collations-preview' table feature"));
    Ok(())
}

#[tokio::test]
async fn alter_preserves_existing_collation_metadata() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let expected_metadata = collation_metadata("value");
    let field = StructField::not_null("value", DataType::STRING).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        expected_metadata.clone(),
    )]);
    let snapshot = create_table(&table_path, schema_ref! { (field) }, "test")
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    snapshot
        .alter_table()
        .set_nullable(column_name!("value"))
        .build(engine.as_ref(), committer())?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let reloaded = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let schema = reloaded.schema();
    let field = schema.field("value").unwrap();
    assert!(field.is_nullable());
    assert_eq!(
        field.get_config_value(&ColumnMetadataKey::Collations),
        Some(&expected_metadata)
    );
    Ok(())
}
