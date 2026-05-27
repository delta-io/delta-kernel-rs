//! Integration tests for the `allowColumnDefaults` writer feature flag.
//!
//! These tests cover both states of the `column-defaults` cargo feature:
//! - With the cargo feature on, `"allowColumnDefaults"` parses as the dedicated
//!   `TableFeature::AllowColumnDefaults` variant with `KernelSupport::Supported`, so snapshot
//!   loading and blind appends succeed.
//! - With the cargo feature off, the variant does not exist and `"allowColumnDefaults"` parses as
//!   `TableFeature::Unknown`. Snapshot loading still succeeds (writer-only features do not block
//!   reads), but beginning a write transaction must fail because unknown writer features are
//!   `KernelSupport::NotSupported`.
//!
//! TODO(#2630): once full column-defaults support ships and the cargo gate is removed,
//! delete the negative test, drop the `#[cfg(...)]` on the positive test, and simplify
//! the variant-recognition assertion (which becomes trivially true).

use std::sync::Arc;

#[cfg(feature = "column-defaults")]
use delta_kernel::arrow::array::{Int64Array, StringArray};
#[cfg(feature = "column-defaults")]
use delta_kernel::arrow::record_batch::RecordBatch;
#[cfg(feature = "column-defaults")]
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
#[cfg(feature = "column-defaults")]
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::TableFeature;
use delta_kernel::Snapshot;
#[cfg(feature = "column-defaults")]
use test_utils::test_read;
use test_utils::{create_table, engine_store_setup, load_and_begin_transaction};

// ============================================
// Negative: cargo feature `column-defaults` OFF
// ============================================

/// With the `column-defaults` cargo feature disabled, `"allowColumnDefaults"` must parse as
/// `TableFeature::Unknown`. Snapshot loading still succeeds (writer-only features don't
/// block reads), but starting a write transaction must fail because unknown writer features
/// are `KernelSupport::NotSupported`.
///
/// TODO(#2630): delete this test when the cargo gate is removed.
#[tokio::test]
#[cfg(not(feature = "column-defaults"))]
async fn test_allow_column_defaults_blocked_when_cargo_feature_off(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Parsing is infallible (Unknown is the strum default variant), so unwrap is safe.
    let parsed: TableFeature = "allowColumnDefaults".parse().unwrap();
    assert!(
        matches!(&parsed, TableFeature::Unknown(s) if s == "allowColumnDefaults"),
        "with the `column-defaults` cargo feature off, the protocol name must fall back \
         to `Unknown`; got {parsed:?}",
    );

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("name", DataType::STRING),
    ])?);

    let (store, engine, table_location) =
        engine_store_setup("test_allow_column_defaults_off", None);
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

    // Snapshot loading succeeds: writer-only features don't block reads.
    let _snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;

    // Beginning a write transaction must fail with an error naming the unsupported feature.
    let err = load_and_begin_transaction(table_url.clone(), &engine)
        .expect_err("write must be blocked when allowColumnDefaults parses as Unknown");
    let msg = err.to_string();
    assert!(
        msg.contains("allowColumnDefaults"),
        "error must name the unsupported feature; got: {msg}",
    );

    Ok(())
}

// ===========================================
// Positive: cargo feature `column-defaults` ON
// ===========================================

/// With the `column-defaults` cargo feature enabled, `"allowColumnDefaults"` parses as the
/// dedicated `TableFeature::AllowColumnDefaults` variant with `KernelSupport::Supported`,
/// so snapshot loading and blind appends both succeed.
///
/// TODO(#2630): drop the `#[cfg(...)]` and remove the variant-recognition assertion (which
/// becomes trivially true) when the cargo gate is removed. Extend to cover defaults
/// substitution once writer support lands.
#[tokio::test]
#[cfg(feature = "column-defaults")]
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
