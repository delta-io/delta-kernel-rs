//! Shared table fixtures for collation passthrough and connector-contract tests.
//!
//! Generates flat and nested annotated schemas, then seeds versioned collation statistics for
//! preservation and file-skipping checks under the [collation RFC].
//!
//! [collation RFC]: https://github.com/delta-io/delta/blob/master/protocol_rfcs/collated-string-type.md

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, StructArray};
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{
    schema_ref, ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Snapshot, SnapshotRef};
use serde_json::{json, Value};
use test_utils::delta_kernel_default_engine::executor::TaskExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{generate_batch, write_batch_to_table, IntoArray};

use crate::common::write_utils::rewrite_commit;

mod connector;
mod passthrough;

struct CollationStats {
    identifier: &'static str,
    bounds: Value,
}

struct CollationTable {
    schema: SchemaRef,
    files: Vec<(RecordBatch, Option<CollationStats>)>,
}

fn annotated_field(
    name: &str,
    data_type: impl Into<DataType>,
    path: &str,
    identifier: &str,
) -> StructField {
    StructField::nullable(name, data_type).with_metadata([(
        ColumnMetadataKey::Collations.as_ref(),
        MetadataValue::Other(json!({ path: identifier })),
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
    let (schema, files, identifier) = if nested {
        (
            schema_ref! {
                nullable "id": INTEGER,
                nullable "name": { (field("first")), (field("last")) },
                (field("email")),
                nullable "department": STRING,
            },
            vec![
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
            "spark.UTF8_LCASE.75.1",
        )
    } else {
        (
            schema_ref! { nullable "id": INTEGER, (field("name")) },
            [(1, "M\u{fc}ller"), (2, "M\u{dc}LLER"), (3, "m\u{fc}ller")]
                .into_iter()
                .map(|(id, name)| {
                    (
                        simple_batch(vec![id], vec![name]),
                        json!({ "minValues": { "name": name }, "maxValues": { "name": name } }),
                    )
                })
                .collect(),
            "icu.UNICODE_CI.75.1",
        )
    };
    CollationTable {
        schema,
        files: files
            .into_iter()
            .map(|(batch, bounds)| {
                (
                    batch,
                    (!collated_fields.is_empty()).then_some(CollationStats { identifier, bounds }),
                )
            })
            .collect(),
    }
}

fn simple_batch(ids: Vec<i32>, names: Vec<&'static str>) -> RecordBatch {
    generate_batch(vec![
        ("id", ids.into_arrow_array()),
        ("name", names.into_arrow_array()),
    ])
    .unwrap()
}

type Row = (i32, &'static str, &'static str, &'static str, &'static str);

fn nested_batch(rows: &[Row]) -> RecordBatch {
    let strings = |extract: fn(&Row) -> &'static str| {
        rows.iter()
            .map(extract)
            .collect::<Vec<_>>()
            .into_arrow_array()
    };
    let name = generate_batch(vec![
        ("first", strings(|row| row.1)),
        ("last", strings(|row| row.2)),
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
        ("email", strings(|row| row.3)),
        ("department", strings(|row| row.4)),
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
    let mut snapshot = create_table(table_path, fixture.schema.clone(), "collation-test", false)
        .with_table_properties(properties.iter().copied())
        .build(engine, committer())?
        .commit(engine)?
        .unwrap_post_commit_snapshot();
    for (batch, collation_stats) in &fixture.files {
        snapshot = write_batch_to_table(&snapshot, engine, batch.clone(), HashMap::new()).await?;
        if let Some(collation_stats) = collation_stats {
            let bounds = mapped_bounds(&collation_stats.bounds, &snapshot.schema());
            let mut adds = 0;
            rewrite_commit(table_path, snapshot.version(), |action| {
                if let Some(add) = action.get_mut("add") {
                    let mut stats: Value = serde_json::from_str(add["stats"].as_str().unwrap())?;
                    assert!(stats.get("statsWithCollation").is_none());
                    stats["statsWithCollation"] = json!({ collation_stats.identifier: bounds });
                    add["stats"] = Value::String(stats.to_string());
                    adds += 1;
                }
                Ok(())
            })?;
            assert_eq!(adds, 1);
            snapshot = Snapshot::builder_for(table_path).build(engine)?;
        }
    }
    Ok(snapshot)
}

fn rewrite_as_preview_table(table_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    rewrite_commit(table_path, 0, |action| {
        if let Some(protocol) = action.get_mut("protocol") {
            let feature = protocol["writerFeatures"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|feature| feature.as_str() == Some("collations"))
                .unwrap();
            *feature = json!("collations-preview");
        }
        Ok(())
    })
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
