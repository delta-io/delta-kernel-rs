//! Reader-side behavior for the `intervalType-preview` table feature.

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

async fn build_scan_over_interval_table(
    name: &str,
    interval: DataType,
    declare_feature: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv", interval,
    )])?);
    let features = declare_feature
        .then_some(vec!["intervalType-preview"])
        .unwrap_or_default();
    let (store, engine, table_location) = engine_store_setup(name, None);
    let table_url = create_table(
        store,
        table_location,
        schema,
        &[],
        true,
        features.clone(),
        features,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    snapshot.scan_builder().build()?;
    Ok(())
}

#[tokio::test]
async fn test_scan_interval_feature_table_succeeds_without_write_support(
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, interval) in [
        ("interval_read_ym", DataType::INTERVAL_YEAR_MONTH),
        ("interval_read_dt", DataType::INTERVAL_DAY_TIME),
    ] {
        build_scan_over_interval_table(name, interval, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_scan_featureless_interval_table_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    build_scan_over_interval_table(
        "interval_read_featureless",
        DataType::INTERVAL_DAY_TIME,
        false,
    )
    .await
}
