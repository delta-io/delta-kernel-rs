//! Geospatial type integration tests for the CreateTable API.
//!
//! Tests that creating a table with geometry or geography columns automatically adds the
//! `geospatial` feature to the protocol, and that geospatial columns are rejected as partition
//! and clustering columns.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    schema_ref, DataType, EdgeInterpolationAlgorithm, GeographyType, GeometryType, SchemaRef,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::KernelResult;
use rstest::rstest;
use test_utils::{assert_result_error_with_message, cm_properties, test_table_setup};

use super::column_mapping::strip_column_mapping_metadata;

fn geometry() -> DataType {
    GeometryType::try_new("OGC:CRS84").unwrap().into()
}

fn geography() -> DataType {
    GeographyType::try_new("OGC:CRS84", EdgeInterpolationAlgorithm::Spherical)
        .unwrap()
        .into()
}

fn top_level_schema(geo: DataType) -> SchemaRef {
    schema_ref! {
        not_null "id": INTEGER,
        nullable "geo": (geo),
    }
}

fn nested_struct_schema(geo: DataType) -> SchemaRef {
    schema_ref! {
        not_null "id": INTEGER,
        nullable "nested": { nullable "geo": (geo) },
    }
}

fn array_schema(geo: DataType) -> SchemaRef {
    schema_ref! {
        not_null "id": INTEGER,
        nullable "geos": [ nullable (geo) ],
    }
}

fn map_value_schema(geo: DataType) -> SchemaRef {
    schema_ref! {
        not_null "id": INTEGER,
        nullable "geos": { STRING => nullable (geo) },
    }
}

/// Asserts the snapshot's protocol includes geospatial with table-features reader/writer versions.
fn assert_geospatial_protocol(snapshot: &Snapshot) {
    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::GeospatialType),
        "geospatial feature should be supported"
    );
    let protocol = table_config.protocol();
    assert!(protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION);
    assert!(protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION);
}

/// Geospatial columns auto-enable the feature across schema shapes and column mapping modes.
#[rstest]
fn test_create_table_with_geospatial(
    #[values(geometry(), geography())] geo: DataType,
    #[values(top_level_schema, nested_struct_schema, array_schema, map_value_schema)]
    make_schema: fn(DataType) -> SchemaRef,
    #[values("none", "name", "id")] cm_mode: &str,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = make_schema(geo);

    let _ = create_table(&table_path, schema.clone(), "Test/1.0", true)
        .with_table_properties(cm_properties(cm_mode))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    assert_geospatial_protocol(&snapshot);
    let read_schema = snapshot.schema();
    assert_eq!(
        &strip_column_mapping_metadata(&read_schema),
        schema.as_ref(),
        "schema should round-trip through create table"
    );
    Ok(())
}

/// The explicit feature signal enables geospatial on a table that has no geospatial columns yet.
#[test]
fn test_create_table_geospatial_feature_signal() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };

    let _ = create_table(&table_path, schema, "Test/1.0", true)
        .with_table_properties([("delta.feature.geospatial", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_geospatial_protocol(&snapshot);
    Ok(())
}

/// A schema without geospatial columns does not add the feature.
#[test]
fn test_create_table_no_geospatial_no_feature() -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = schema_ref! { nullable "id": INTEGER };

    let _ = create_table(&table_path, schema, "Test/1.0", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert!(!snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::GeospatialType));
    Ok(())
}

/// The Delta protocol defines no partition-value serialization for geospatial types.
#[rstest]
fn test_create_table_geospatial_partition_rejected(
    #[values(geometry(), geography())] geo: DataType,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let result = create_table(&table_path, top_level_schema(geo), "Test/1.0", true)
        .with_data_layout(DataLayout::partitioned(["geo"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "Geospatial columns cannot be partition columns");
    Ok(())
}

/// Clustering requires min/max statistics, which kernel does not collect for geospatial types.
#[rstest]
fn test_create_table_geospatial_clustering_rejected(
    #[values(geometry(), geography())] geo: DataType,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let result = create_table(&table_path, top_level_schema(geo), "Test/1.0", true)
        .with_data_layout(DataLayout::clustered(["geo"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, "unsupported type");
    Ok(())
}
