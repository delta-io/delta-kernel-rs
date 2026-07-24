//! Interval-type integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::test_table_setup;

#[cfg(not(feature = "interval-type-in-dev"))]
#[test]
fn test_create_table_interval_blocked_when_feature_off() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::INTEGER),
        StructField::nullable("iv", DataType::INTERVAL_DAY_TIME),
    ])?);

    let result = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    test_utils::assert_result_error_with_message(result, "interval-type-in-dev");
    Ok(())
}

#[rstest::rstest]
fn test_create_table_rejects_interval_clustering(
    #[values(DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME)] interval: DataType,
    #[values(false, true)] nested: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let (interval_field, clustering_column) = if nested {
        (
            StructField::nullable(
                "nested",
                StructType::new_unchecked([StructField::nullable("iv", interval)]),
            ),
            ColumnName::new(["nested", "iv"]),
        )
    } else {
        (
            StructField::nullable("iv", interval),
            ColumnName::new(["iv"]),
        )
    };
    let schema = Arc::new(StructType::try_new(vec![
        StructField::not_null("id", DataType::INTEGER),
        interval_field,
    ])?);

    let result = create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::Clustered {
            columns: vec![clustering_column],
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    test_utils::assert_result_error_with_message(result, "unsupported type");
    Ok(())
}

#[cfg(feature = "interval-type-in-dev")]
mod feature_enabled {
    use delta_kernel::schema::SchemaRef;
    use delta_kernel::snapshot::Snapshot;
    use delta_kernel::table_features::ColumnMappingMode;
    use rstest::rstest;
    use test_utils::cm_properties;

    use super::super::column_mapping::{
        assert_column_mapping_config, strip_column_mapping_metadata,
    };
    use super::*;

    /// Top-level schema carrying the given interval `DataType`.
    fn top_level_interval_schema(interval: DataType) -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("iv", interval, true),
        ]))
    }

    /// Schema with the given interval `DataType` nested inside a struct.
    fn nested_interval_schema(interval: DataType) -> SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                StructType::new_unchecked([StructField::new("inner_iv", interval, true)]),
                true,
            ),
        ]))
    }

    /// Creating a table with interval columns preserves its schema across column mapping modes.
    #[rstest]
    fn test_create_table_with_interval_round_trips_schema(
        #[values(DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME)] interval: DataType,
        #[values(top_level_interval_schema, nested_interval_schema)] make_schema: fn(
            DataType,
        )
            -> SchemaRef,
        #[values("none", "name", "id")] cm_mode: &str,
    ) -> DeltaResult<()> {
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let schema = make_schema(interval);

        let _ = create_table(&table_path, schema.clone(), "Test/1.0")
            .with_table_properties(cm_properties(cm_mode))
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

        let table_url = delta_kernel::try_parse_uri(&table_path)?;
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

        let expected_cm_mode = match cm_mode {
            "none" => ColumnMappingMode::None,
            "name" => ColumnMappingMode::Name,
            "id" => ColumnMappingMode::Id,
            _ => unreachable!(),
        };
        assert_column_mapping_config(&snapshot, expected_cm_mode);

        let read_schema = snapshot.schema();
        let stripped_schema = strip_column_mapping_metadata(&read_schema);
        assert_eq!(
            &stripped_schema,
            schema.as_ref(),
            "logical schema should round-trip through create table"
        );
        Ok(())
    }
}
