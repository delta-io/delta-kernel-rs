//! Integration tests for writing ANSI interval columns (the `intervalType-preview` feature).
//!
//! Covers the unpartitioned blind-append round-trip and the write-time feature enforcement.
//! Partitioned interval writes are out of scope here (they require `Scalar::Interval*`, planning
//! to add in a later PR).

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use test_utils::{create_table, engine_store_setup, load_and_begin_transaction};

/// Writing interval data is gated by the `interval-type-in-dev` cargo feature, not by the
/// `intervalType-preview` table feature.
#[tokio::test]
async fn test_write_interval_featureless_table_gate() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv",
        DataType::INTERVAL_DAY_TIME,
    )])?);

    let (store, engine, table_location) =
        engine_store_setup("test_interval_requires_feature", None);
    // A (3,7) table that has an interval column but does NOT list the intervalType feature.
    let table_url = create_table(store, table_location, schema, &[], true, vec![], vec![]).await?;

    let result = load_and_begin_transaction(table_url, &engine);
    if cfg!(feature = "interval-type-in-dev") {
        result.expect("interval writes should be allowed when the cargo feature is enabled");
    } else {
        let err = result
            .expect_err("interval writes should be blocked when the cargo feature is disabled")
            .to_string();
        assert!(
            err.contains("interval-type-in-dev"),
            "error must explain the missing cargo feature; got: {err}",
        );
    }
    Ok(())
}

#[cfg(not(feature = "interval-type-in-dev"))]
mod feature_disabled {
    use super::*;

    /// With the cargo feature off, kernel does not support `intervalType-preview`, so starting a
    /// write transaction on a table that lists the feature is blocked.
    #[tokio::test]
    async fn test_write_interval_blocked_when_feature_off() -> Result<(), Box<dyn std::error::Error>>
    {
        let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
            "iv",
            DataType::INTERVAL_DAY_TIME,
        )])?);

        let (store, engine, table_location) = engine_store_setup("test_interval_off", None);
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],
            true,
            vec!["intervalType-preview"],
            vec!["intervalType-preview"],
        )
        .await?;

        let err = load_and_begin_transaction(table_url, &engine)
            .expect_err("write must be blocked when intervalType is unsupported")
            .to_string();
        assert!(
            err.contains("intervalType") && err.contains("not supported"),
            "error must name the unsupported feature; got: {err}",
        );
        Ok(())
    }
}

#[cfg(feature = "interval-type-in-dev")]
mod feature_enabled {
    use delta_kernel::arrow::array::{ArrayRef, Int32Array, Int64Array};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStoreExt as _;
    use itertools::Itertools as _;
    use rstest::rstest;
    use serde_json::Deserializer;
    use test_utils::test_read;

    use super::*;

    /// Unpartitioned blind-append round-trip for both interval families, covering null / zero /
    /// negative values. Also asserts the committed `add` action carries no `minValues`/`maxValues`
    /// for the interval column (intervals collect no min/max stats and are not skipping-eligible).
    #[rstest]
    #[case::year_month(
        "test_interval_append_ym",
        DataType::INTERVAL_YEAR_MONTH,
        Arc::new(Int32Array::from(vec![Some(0), Some(30), None, Some(-18)])) as ArrayRef,
    )]
    #[case::day_time(
        "test_interval_append_dt",
        DataType::INTERVAL_DAY_TIME,
        Arc::new(Int64Array::from(vec![Some(0), Some(131_445_000_000), None, Some(-5)])) as ArrayRef,
    )]
    #[tokio::test]
    async fn test_append_interval_roundtrip(
        #[case] name: &str,
        #[case] interval: DataType,
        #[case] column: ArrayRef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
            "iv", interval,
        )])?);

        let (store, engine, table_location) = engine_store_setup(name, None);
        let table_url = create_table(
            store.clone(),
            table_location,
            schema.clone(),
            &[],
            true,
            vec!["intervalType-preview"],
            vec!["intervalType-preview"],
        )
        .await?;

        let mut txn = load_and_begin_transaction(table_url.clone(), &engine)?
            .with_engine_info("default engine");
        let data = RecordBatch::try_new(Arc::new(schema.as_ref().try_into_arrow()?), vec![column])?;

        let engine = Arc::new(engine);
        let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
        let add_files_metadata = engine
            .write_parquet(&ArrowEngineData::new(data.clone()), write_context.as_ref())
            .await?;
        txn.add_files(add_files_metadata);
        assert!(txn.commit(engine.as_ref())?.is_committed());

        // Interval columns must not carry min/max stats in the committed `add` action.
        let commit = store
            .get(&Path::from(format!(
                "/{name}/_delta_log/00000000000000000001.json"
            )))
            .await?;
        let parsed: Vec<serde_json::Value> = Deserializer::from_slice(&commit.bytes().await?)
            .into_iter::<serde_json::Value>()
            .try_collect()?;
        let add = parsed
            .iter()
            .find_map(|v| v.get("add"))
            .expect("commit must contain an add action");
        // Match DBR exactly: interval columns get nullCount but no min/max.
        let stats = add
            .get("stats")
            .and_then(|s| s.as_str())
            .expect("add action must carry stats");
        let stats: serde_json::Value = serde_json::from_str(stats)?;
        assert_eq!(
            stats.get("nullCount").and_then(|v| v.get("iv")),
            Some(&serde_json::json!(1)),
            "interval column must have nullCount; got: {stats}",
        );
        for category in ["minValues", "maxValues"] {
            if let Some(obj) = stats.get(category).and_then(|v| v.as_object()) {
                assert!(
                    !obj.contains_key("iv"),
                    "interval column must not have {category} stats; got: {stats}",
                );
            }
        }

        // Round-trip read back the raw physical integer values.
        test_read(&ArrowEngineData::new(data), &table_url, engine)?;
        Ok(())
    }
}
