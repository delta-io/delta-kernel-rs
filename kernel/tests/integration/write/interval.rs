//! Integration tests for writing ANSI interval columns.

use std::sync::Arc;

use delta_kernel::schema::{DataType, StructField, StructType};
use test_utils::load_and_begin_transaction;
#[cfg(not(feature = "interval-type-in-dev"))]
use test_utils::{create_table, engine_store_setup};

/// Writing interval data is gated by the `interval-type-in-dev` cargo feature.
#[cfg(not(feature = "interval-type-in-dev"))]
#[tokio::test]
async fn test_write_interval_table_gate() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "iv",
        DataType::INTERVAL_DAY_TIME,
    )])?);

    let (store, engine, table_location) =
        engine_store_setup("test_interval_requires_feature", None);
    let table_url = create_table(store, table_location, schema, &[], true, vec![], vec![]).await?;

    let transaction = load_and_begin_transaction(table_url, &engine)?;
    let err = transaction
        .unpartitioned_write_context()
        .expect_err("interval write contexts should be blocked when the cargo feature is disabled")
        .to_string();
    assert!(
        err.contains("interval-type-in-dev"),
        "error must explain the missing cargo feature; got: {err}",
    );
    Ok(())
}

#[cfg(feature = "interval-type-in-dev")]
mod feature_enabled {
    use std::collections::HashMap;

    use delta_kernel::actions::{MAX_VALUES, MIN_VALUES, NULL_COUNT, STATS_PARSED};
    use delta_kernel::arrow::array::{
        Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray,
    };
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::transaction::create_table::create_table as create_table_transaction;
    use rstest::rstest;
    use test_utils::{
        get_column, read_actions_from_commit, test_read, test_table_setup, test_table_setup_mt,
        write_batch_to_table,
    };

    use super::*;
    use crate::common::read_utils::read_parquet_file;
    use crate::common::write_utils::load_existing_single_file_checkpoint_path;

    /// Unpartitioned blind-append round-trip for both interval families, covering null / zero /
    /// negative values in a nested schema. Also verifies that ordinary columns retain their full
    /// statistics while the interval column gets only `nullCount`.
    #[rstest]
    #[case::year_month(
        DataType::INTERVAL_YEAR_MONTH,
        Arc::new(Int32Array::from(vec![Some(0), Some(30), None, Some(-18)])) as ArrayRef,
    )]
    #[case::day_time(
        DataType::INTERVAL_DAY_TIME,
        Arc::new(Int64Array::from(vec![Some(0), Some(131_445_000_000), None, Some(-5)])) as ArrayRef,
    )]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_append_interval_roundtrip(
        #[case] interval: DataType,
        #[case] column: ArrayRef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::try_new(vec![
            StructField::not_null("id", DataType::LONG),
            StructField::nullable(
                "nested",
                StructType::new_unchecked([
                    StructField::nullable("iv", interval),
                    StructField::nullable("label", DataType::STRING),
                ]),
            ),
        ])?);

        let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
        let snapshot = create_table_transaction(&table_path, schema.clone(), "Test/1.0")
            .with_table_properties([("delta.checkpoint.writeStatsAsStruct", "true")])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let table_url = snapshot.table_root().clone();

        let mut txn = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
            .with_engine_info("default engine");
        let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
        let arrow_schema = Arc::new(arrow_schema);
        let nested_fields = match arrow_schema.field_with_name("nested").unwrap().data_type() {
            ArrowDataType::Struct(fields) => fields.clone(),
            data_type => panic!("expected nested struct, got {data_type:?}"),
        };
        let nested = StructArray::new(
            nested_fields,
            vec![
                column,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef,
            ],
            None,
        );
        let data = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(nested) as ArrayRef,
            ],
        )?;

        let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
        let add_files_metadata = engine
            .write_parquet(&ArrowEngineData::new(data.clone()), write_context.as_ref())
            .await?;
        txn.add_files(add_files_metadata);
        let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

        let add_actions = read_actions_from_commit(&table_url, 1, "add")?;
        let add = &add_actions[0];
        let stats = add
            .get("stats")
            .and_then(|s| s.as_str())
            .expect("add action must carry stats");
        let stats: serde_json::Value = serde_json::from_str(stats)?;
        assert_eq!(
            stats.pointer("/nullCount/id"),
            Some(&serde_json::json!(0)),
            "id must have nullCount; got: {stats}",
        );
        assert_eq!(
            stats.pointer("/nullCount/nested/iv"),
            Some(&serde_json::json!(1)),
            "interval column must have nullCount; got: {stats}",
        );
        assert_eq!(
            stats.pointer("/nullCount/nested/label"),
            Some(&serde_json::json!(0)),
            "label must have nullCount; got: {stats}",
        );
        for category in ["minValues", "maxValues"] {
            let nested_stats = stats
                .get(category)
                .and_then(|v| v.get("nested"))
                .and_then(|v| v.as_object())
                .expect("ordinary nested columns must have min/max stats");
            assert!(
                !nested_stats.contains_key("iv"),
                "interval column must not have {category} stats; got: {stats}",
            );
            assert!(
                nested_stats.contains_key("label"),
                "label must have {category} stats; got: {stats}",
            );
        }
        assert_eq!(stats.pointer("/minValues/id"), Some(&serde_json::json!(1)));
        assert_eq!(stats.pointer("/maxValues/id"), Some(&serde_json::json!(4)));
        assert_eq!(
            stats.pointer("/minValues/nested/label"),
            Some(&serde_json::json!("a"))
        );
        assert_eq!(
            stats.pointer("/maxValues/nested/label"),
            Some(&serde_json::json!("d"))
        );

        snapshot.checkpoint(engine.as_ref(), None)?;
        let checkpoint_path =
            load_existing_single_file_checkpoint_path(&table_path, snapshot.version());
        let checkpoint_batch = read_parquet_file(&checkpoint_path);
        let checkpoint_add = get_column!(checkpoint_batch, "add", StructArray);
        let add_row = (0..checkpoint_batch.num_rows())
            .find(|&row| checkpoint_add.is_valid(row))
            .expect("checkpoint must contain an add action");
        let stats_parsed = get_column!(checkpoint_add, STATS_PARSED, StructArray);
        assert!(stats_parsed.is_valid(add_row));

        let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);
        let nested_null_count = get_column!(null_count, "nested", StructArray);
        assert_eq!(get_column!(null_count, "id", Int64Array).value(add_row), 0);
        assert_eq!(
            get_column!(nested_null_count, "iv", Int64Array).value(add_row),
            1
        );
        assert_eq!(
            get_column!(nested_null_count, "label", Int64Array).value(add_row),
            0
        );

        for (category, expected_id, expected_label) in [(MIN_VALUES, 1, "a"), (MAX_VALUES, 4, "d")]
        {
            let values = get_column!(stats_parsed, category, StructArray);
            let nested_values = get_column!(values, "nested", StructArray);
            assert_eq!(
                get_column!(values, "id", Int64Array).value(add_row),
                expected_id
            );
            assert!(nested_values.column_by_name("iv").is_none());
            assert_eq!(
                get_column!(nested_values, "label", StringArray).value(add_row),
                expected_label
            );
        }

        test_read(&ArrowEngineData::new(data), &table_url, engine)?;
        Ok(())
    }

    /// Interval columns omitted by the configured stats selection produce no statistics.
    #[rstest]
    #[case::stats_columns("delta.dataSkippingStatsColumns", "value")]
    #[case::num_indexed_cols("delta.dataSkippingNumIndexedCols", "1")]
    #[tokio::test]
    async fn test_interval_stats_excluded_by_column_selection(
        #[values(DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME)] interval: DataType,
        #[case] property_name: &str,
        #[case] property_value: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::try_new(vec![
            StructField::not_null("value", DataType::LONG),
            StructField::nullable("iv", interval.clone()),
        ])?);
        let interval_column: ArrayRef = if interval == DataType::INTERVAL_YEAR_MONTH {
            Arc::new(Int32Array::from(vec![Some(12), None, Some(-6)]))
        } else {
            Arc::new(Int64Array::from(vec![Some(86_400_000_000), None, Some(-1)]))
        };

        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let snapshot = create_table_transaction(&table_path, schema.clone(), "Test/1.0")
            .with_table_properties([(property_name, property_value)])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();

        let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                interval_column,
            ],
        )?;
        let _ = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

        let add_actions = read_actions_from_commit(snapshot.table_root(), 1, "add")?;
        let stats: serde_json::Value = serde_json::from_str(
            add_actions[0]
                .get("stats")
                .and_then(|value| value.as_str())
                .expect("add action should have stats"),
        )?;

        assert_eq!(
            stats.pointer("/nullCount/value"),
            Some(&serde_json::json!(0))
        );
        assert_eq!(
            stats.pointer("/minValues/value"),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            stats.pointer("/maxValues/value"),
            Some(&serde_json::json!(3))
        );
        for category in ["nullCount", "minValues", "maxValues"] {
            assert!(
                stats.get(category).and_then(|value| value.get("iv")).is_none(),
                "interval column excluded by {property_name} must not have {category}; got: {stats}",
            );
        }
        Ok(())
    }
}
