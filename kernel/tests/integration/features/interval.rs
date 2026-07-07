//! Reader-side behavior for the `intervalType` reader-writer feature.
//!
//! A table that declares `intervalType` is readable only when kernel support is compiled in (the
//! `interval-type-in-dev` gate). Tables that carry interval columns without declaring the feature
//! (e.g. legacy DBR-written tables) read regardless of the gate, since the read-support check only
//! inspects features the protocol declares. That featureless case is covered directly below and
//! end-to-end by the `intv_*` acceptance workloads.

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

/// Builds a `(3,7)` table that declares `intervalType` in both feature lists and carries a single
/// interval column, then returns the result of building a scan over it.
async fn build_scan_over_interval_table(
    name: &str,
    interval: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv", interval,
    )])?);

    let (store, engine, table_location) = engine_store_setup(name, None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],
        true,
        vec!["intervalType"],
        vec!["intervalType"],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    // The reader-feature support gate fires here, in `Scan::build`, not at snapshot load.
    snapshot.scan_builder().build()?;
    Ok(())
}

#[cfg(feature = "interval-type-in-dev")]
#[tokio::test]
async fn test_scan_interval_feature_table_succeeds_when_enabled(
) -> Result<(), Box<dyn std::error::Error>> {
    build_scan_over_interval_table("interval_read_ym", DataType::INTERVAL_YEAR_MONTH).await?;
    build_scan_over_interval_table("interval_read_dt", DataType::INTERVAL_DAY_TIME).await?;
    Ok(())
}

#[cfg(not(feature = "interval-type-in-dev"))]
#[tokio::test]
async fn test_scan_interval_feature_table_blocked_when_kernel_support_off(
) -> Result<(), Box<dyn std::error::Error>> {
    let err = build_scan_over_interval_table("interval_read_off", DataType::INTERVAL_DAY_TIME)
        .await
        .expect_err("scanning an intervalType table must be blocked when kernel support is off")
        .to_string();
    assert!(
        err.contains("intervalType") && err.contains("not supported"),
        "error must name the unsupported feature; got: {err}",
    );
    Ok(())
}

/// A featureless table carrying an interval column reads regardless of feature gate (mirrors DBR)
#[tokio::test]
async fn test_scan_featureless_interval_table_succeeds_regardless_of_kernel_support(
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv",
        DataType::INTERVAL_DAY_TIME,
    )])?);

    let (store, engine, table_location) = engine_store_setup("interval_read_featureless", None);
    // A (3,7) table that declares no reader/writer features, so the read path never sees
    // `intervalType` even though the schema carries an interval column.
    let table_url = create_table(store, table_location, schema, &[], true, vec![], vec![]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    snapshot.scan_builder().build()?;
    Ok(())
}
