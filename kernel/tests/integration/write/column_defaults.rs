//! Integration test for the `allowColumnDefaults` writer feature flag.
//!
//! This module is gated by the `column-defaults` cargo feature. With the cargo feature on,
//! kernel recognizes `allowColumnDefaults` as a known WriterOnly `TableFeature` with
//! `KernelSupport::Supported`, so reads and blind appends against such tables succeed. With
//! the cargo feature off, the variant does not exist and the feature parses as
//! `TableFeature::Unknown`, blocking writes.
//!
//! This test only exercises the positive case (cargo feature on). The negative case is
//! implicit: the test file itself is excluded from compilation when the cargo feature is
//! off, so the existence of this passing test attests that the gate works.

use std::sync::Arc;

use delta_kernel::arrow::array::{Int64Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup, load_and_begin_transaction, test_read};

#[tokio::test]
async fn test_blind_append_to_allow_column_defaults_table() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt::try_init();

    // Sanity-check that with the cargo feature on, "allowColumnDefaults" parses as the
    // dedicated variant and not as `TableFeature::Unknown`. Parsing is infallible (see the
    // `#[strum(default)]` Unknown variant), so unwrap is safe.
    let parsed: TableFeature = "allowColumnDefaults".parse().unwrap();
    assert_eq!(
        parsed,
        TableFeature::AllowColumnDefaults,
        "with the `column-defaults` cargo feature enabled, the protocol name must parse \
         to the dedicated variant (got {parsed:?})",
    );

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?);

    let (store, engine, table_location) =
        engine_store_setup("test_table_allow_column_defaults", None);

    // Hand-roll the protocol with `allowColumnDefaults` in writerFeatures. We use the JSON
    // test helper instead of the kernel `create_table` builder because the latter does not
    // whitelist this feature in `ALLOWED_DELTA_FEATURES`.
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

    // Loading a snapshot must succeed: `allowColumnDefaults` is writer-only and
    // `KernelSupport::Supported` when the cargo feature is on.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let writer_features = snapshot
        .table_configuration()
        .protocol()
        .writer_features()
        .expect("writer_features must be present on a writer v7 table");
    assert!(
        writer_features.contains(&TableFeature::AllowColumnDefaults),
        "writer_features must include the AllowColumnDefaults variant; got {writer_features:?}",
    );

    // Blind-append a small batch. No column has a `CURRENT_DEFAULT` yet, so this is a plain
    // append; we only assert the feature does not block the write path.
    // TODO(#2630): extend to cover defaults substitution once writer enablement lands.
    let mut txn =
        load_and_begin_transaction(table_url.clone(), &engine)?.with_engine_info("default engine");

    let data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow()?),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;

    let engine = Arc::new(engine);
    let write_context = Arc::new(txn.unpartitioned_write_context()?);
    let add_files_metadata = engine
        .write_parquet(&ArrowEngineData::new(data.clone()), write_context.as_ref())
        .await?;
    txn.add_files(add_files_metadata);
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Round-trip read.
    test_read(&ArrowEngineData::new(data), &table_url, engine)?;

    Ok(())
}
