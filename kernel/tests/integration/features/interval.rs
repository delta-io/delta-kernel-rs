//! Reader-side behavior for ANSI interval columns.

use delta_kernel::schema::{schema_ref, DataType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

#[rstest::rstest]
#[case::year_month("interval_read_ym", DataType::INTERVAL_YEAR_MONTH)]
#[case::day_time("interval_read_dt", DataType::INTERVAL_DAY_TIME)]
#[tokio::test]
async fn test_build_scan_over_interval_table(
    #[case] name: &str,
    #[case] interval: DataType,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema_ref! { nullable "iv": (interval) };
    let (store, engine, table_location) = engine_store_setup(name, None);
    let table_url = create_table(store, table_location, schema, &[], true, vec![], vec![]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    snapshot.scan_builder().build()?;
    Ok(())
}
