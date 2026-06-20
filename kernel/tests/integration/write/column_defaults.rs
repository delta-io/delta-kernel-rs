//! Integration tests for the `allowColumnDefaults` writer feature.

use std::sync::Arc;

#[cfg(feature = "column-defaults-in-dev")]
use delta_kernel::arrow::array::{ArrayRef, Int64Array, StringArray};
#[cfg(feature = "column-defaults-in-dev")]
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
#[cfg(feature = "column-defaults-in-dev")]
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
#[cfg(feature = "column-defaults-in-dev")]
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{schema_ref, DataType, StructField, StructType};
#[cfg(feature = "column-defaults-in-dev")]
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::{create_table, engine_store_setup, test_table_setup};
#[cfg(feature = "column-defaults-in-dev")]
use test_utils::{insert_data, test_read};

// TODO(#2630): Allow create table to support column defaults
#[test]
fn test_create_table_rejects_col_defaults() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": LONG };

    let err = kernel_create_table(&table_path, schema, "Test/1.0")
        .with_table_properties([("delta.feature.allowColumnDefaults", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .expect_err("kernel create_table must reject allowColumnDefaults")
        .to_string();
    assert!(
        err.contains("allowColumnDefaults"),
        "error must name the unsupported feature; got: {err}",
    );
    Ok(())
}

#[tokio::test]
#[cfg(not(feature = "column-defaults-in-dev"))]
async fn test_col_defaults_blocked_when_cargo_feature_off() -> Result<(), Box<dyn std::error::Error>>
{
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?);

    let (store, engine, table_location) = engine_store_setup("test_col_defaults_off", None);
    let table_url = create_table(
        store.clone(),
        table_location,
        schema,
        &[],
        true,
        vec![],
        vec!["allowColumnDefaults"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let err = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .expect_err("write must be blocked when allowColumnDefaults is unsupported");
    let msg = err.to_string();
    assert!(
        msg.contains("allowColumnDefaults"),
        "error must name the unsupported feature; got: {msg}",
    );

    Ok(())
}

#[tokio::test]
#[cfg(feature = "column-defaults-in-dev")]
async fn test_blind_append_to_col_defaults_table_supported_when_cargo_feature_on(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?);

    let (store, engine, table_location) = engine_store_setup("test_table_col_defaults", None);
    // Use the JSON helper instead of the kernel `create_table` builder because the
    // latter does not whitelist this feature in `ALLOWED_DELTA_FEATURES`.
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        true,
        vec![],
        vec!["allowColumnDefaults"],
    )
    .await?;
    let engine = Arc::new(engine);

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let writer_features = snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .expect("writer_features must be present on a writer v7 table");
    assert!(
        writer_features.contains(&TableFeature::AllowColumnDefaults),
        "writer_features must include AllowColumnDefaults; got {writer_features:?}",
    );

    // Blind-append a small batch. No column has a `CURRENT_DEFAULT` yet, so this is a plain
    // append; we only assert the feature does not block the write path.
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    ];
    assert!(insert_data(snapshot, &engine, columns.clone())
        .await?
        .is_committed());

    // Round-trip read.
    let data = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), columns)?;
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}
