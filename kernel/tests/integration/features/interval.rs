//! Reader-side behavior for the `intervalType-preview` reader-writer feature.
//!
//! A table that declares `intervalType-preview` is readable only when kernel support is compiled
//! in (the `interval-type-in-dev` gate). Tables that carry interval columns without declaring the
//! feature (e.g. legacy tables) read regardless of the gate, since the read-support check only
//! inspects features the protocol declares. That featureless case is covered directly below and
//! end-to-end by the `intv_*` acceptance workloads.

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

/// Builds a `(3,7)` table carrying a single interval column and builds a scan over it. When
/// `declare_feature` is set, the table declares `intervalType-preview` in both feature lists;
/// otherwise it declares no features (a featureless interval table).
async fn build_scan_over_interval_table(
    name: &str,
    interval: DataType,
    declare_feature: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv", interval,
    )])?);
    let features = if declare_feature {
        vec!["intervalType-preview"]
    } else {
        vec![]
    };

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
    // The reader-feature support gate fires here, in `Scan::build`, not at snapshot load.
    snapshot.scan_builder().build()?;
    Ok(())
}

/// A table declaring `intervalType-preview` is scannable exactly when the `interval-type-in-dev`
/// gate is compiled in; otherwise the scan is refused, naming the unsupported feature.
#[tokio::test]
async fn test_scan_interval_feature_table_respects_kernel_support_gate(
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, interval) in [
        ("interval_read_ym", DataType::INTERVAL_YEAR_MONTH),
        ("interval_read_dt", DataType::INTERVAL_DAY_TIME),
    ] {
        let result = build_scan_over_interval_table(name, interval, true).await;
        if cfg!(feature = "interval-type-in-dev") {
            result?;
        } else {
            let err = result
                .expect_err(
                    "scanning an interval feature table must be blocked when support is off",
                )
                .to_string();
            assert!(
                err.contains("intervalType-preview"),
                "error must name the unsupported feature; got: {err}",
            );
        }
    }
    Ok(())
}

/// A featureless table carrying an interval column reads regardless of the gate: the read-support
/// check only inspects declared features, and this table declares none (mirrors legacy tables).
#[tokio::test]
async fn test_scan_featureless_interval_table_succeeds_regardless_of_kernel_support(
) -> Result<(), Box<dyn std::error::Error>> {
    build_scan_over_interval_table(
        "interval_read_featureless",
        DataType::INTERVAL_DAY_TIME,
        false,
    )
    .await
}
