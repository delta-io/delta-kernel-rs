//! Interval-type integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use test_utils::test_table_setup;

/// A schema without interval columns must not add the `intervalType-preview` feature. This holds
/// regardless of the cargo gate, since no interval column is present.
#[test]
fn test_create_table_no_interval_no_feature() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, true),
        StructField::new("name", DataType::STRING, true),
    ])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert!(
        !snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::IntervalTypePreview),
        "intervalType feature should NOT be in protocol for a non-interval schema"
    );
    Ok(())
}

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
    use delta_kernel::table_features::{
        TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
    };
    use rstest::rstest;

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

    /// Creating a table whose schema contains an interval column does not auto-enable
    /// `intervalType-preview`, and the schema round-trips. The write path requires kernel support,
    /// so this test is gated behind the `interval-type-in-dev` cargo feature.
    #[rstest]
    fn test_create_table_with_interval_does_not_auto_enable_feature(
        #[values(DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME)] interval: DataType,
        #[values(top_level_interval_schema, nested_interval_schema)] make_schema: fn(
            DataType,
        )
            -> SchemaRef,
    ) -> DeltaResult<()> {
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let schema = make_schema(interval);

        let _ = create_table(&table_path, schema.clone(), "Test/1.0")
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

        let table_url = delta_kernel::try_parse_uri(&table_path)?;
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
        let table_config = snapshot.table_configuration();

        assert!(
            !table_config.is_feature_supported(&TableFeature::IntervalTypePreview),
            "intervalType feature should not be auto-enabled"
        );
        let protocol = table_config.protocol();
        assert!(!protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::IntervalTypePreview)));
        assert!(!protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::IntervalTypePreview)));

        assert_eq!(
            snapshot.schema().as_ref(),
            schema.as_ref(),
            "schema should round-trip through create table"
        );
        Ok(())
    }

    #[test]
    fn test_create_table_interval_feature_signal_supported() -> DeltaResult<()> {
        let (_temp_dir, table_path, engine) = test_table_setup()?;
        let schema = top_level_interval_schema(DataType::INTERVAL_DAY_TIME);

        let _ = create_table(&table_path, schema, "Test/1.0")
            .with_table_properties([("delta.feature.intervalType-preview", "supported")])
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;

        let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        let table_config = snapshot.table_configuration();
        assert!(
            table_config.is_feature_supported(&TableFeature::IntervalTypePreview),
            "intervalType feature signal should be supported"
        );
        let protocol = table_config.protocol();
        assert!(protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION);
        assert!(protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION);
        assert!(protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::IntervalTypePreview)));
        assert!(protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::IntervalTypePreview)));
        Ok(())
    }
}
