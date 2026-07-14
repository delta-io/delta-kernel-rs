//! Geospatial (geometry / geography) integration tests for the CreateTable and read paths.
//!
//! Kernel represents geometry / geography schema types but does not yet support the
//! `geospatial` table feature. A table whose schema contains a geospatial column is therefore
//! rejected on both the create/write path and the snapshot-load (read) path, since both build
//! a `TableConfiguration`.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    DataType, EdgeInterpolationAlgorithm, GeographyType, GeometryType, PrimitiveType, StructField,
    StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use rstest::rstest;
use test_utils::{
    add_commit, assert_result_error_with_message, engine_store_setup, test_table_setup,
};

const UNSUPPORTED_MSG: &str = "geometry or geography columns, which are not yet supported";

fn geometry() -> DataType {
    DataType::Primitive(PrimitiveType::Geometry(Box::new(
        GeometryType::try_new("EPSG:4326").unwrap(),
    )))
}

fn geography() -> DataType {
    DataType::Primitive(PrimitiveType::Geography(Box::new(
        GeographyType::try_new("EPSG:4326", EdgeInterpolationAlgorithm::Spherical).unwrap(),
    )))
}

/// Creating a table whose schema contains a geometry or geography column is rejected, whether
/// the column is top-level or nested.
#[rstest]
#[case::geometry_top_level(StructType::new_unchecked([
    StructField::nullable("id", DataType::INTEGER),
    StructField::nullable("g", geometry()),
]))]
#[case::geography_nested(StructType::new_unchecked([
    StructField::nullable("id", DataType::INTEGER),
    StructField::nullable(
        "nested",
        DataType::Struct(Box::new(StructType::new_unchecked([StructField::nullable(
            "inner_geo",
            geography(),
        )]))),
    ),
]))]
#[tokio::test]
async fn test_create_table_with_geospatial_column_rejected(
    #[case] schema: StructType,
) -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let result = create_table(&table_path, Arc::new(schema), "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    assert_result_error_with_message(result, UNSUPPORTED_MSG);
    Ok(())
}

/// Loading a snapshot of a pre-existing table whose schema contains a geospatial column is
/// rejected. The commit is hand-written to bypass create-table validation, and lists the
/// `geospatial` feature so the rejection comes from the schema-type guard rather than an
/// unknown-feature check.
#[tokio::test]
async fn test_snapshot_load_with_geospatial_column_rejected(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup("geo_read_reject", None);
    let schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("g", geometry()),
    ]);
    let schema_json = serde_json::to_string(&schema)?;
    let escaped = serde_json::to_string(&schema_json)?;
    let v0 = format!(
        r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["geospatial"],"writerFeatures":["geospatial"]}}}}
{{"metaData":{{"id":"geo-id","format":{{"provider":"parquet","options":{{}}}},"schemaString":{escaped},"partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}
"#
    );
    add_commit(table_url.as_str(), store.as_ref(), 0, v0).await?;

    let msg = Snapshot::builder_for(table_url)
        .build(&engine)
        .expect_err("geospatial column must be rejected at snapshot load")
        .to_string();
    assert!(
        msg.contains(UNSUPPORTED_MSG),
        "expected geospatial-unsupported error, got: {msg}"
    );
    Ok(())
}
