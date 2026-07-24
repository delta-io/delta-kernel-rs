//! Reader-side behavior for ANSI interval columns.

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

async fn build_scan_over_interval_table(
    name: &str,
    interval: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv", interval,
    )])?);
    let (store, engine, table_location) = engine_store_setup(name, None);
    let table_url = create_table(store, table_location, schema, &[], true, vec![], vec![]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    snapshot.scan_builder().build()?;
    Ok(())
}

#[tokio::test]
async fn test_scan_interval_table_is_not_gated_by_cargo_feature(
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, interval) in [
        ("interval_read_ym", DataType::INTERVAL_YEAR_MONTH),
        ("interval_read_dt", DataType::INTERVAL_DAY_TIME),
    ] {
        build_scan_over_interval_table(name, interval).await?;
    }
    Ok(())
}
