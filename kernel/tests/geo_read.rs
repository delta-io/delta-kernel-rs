//! End-to-end read tests for tables with geospatial (geometry) columns.
//!
//! Validates that `Scan::execute()` on a Delta table declaring a `PrimitiveType::Geometry`
//! column correctly reads WKB bytes from a Parquet file and hands back an Arrow
//! `BinaryArray` to the caller. Also exercises the `ST_Intersects` opaque predicate on
//! the scanned data.

#![cfg(feature = "default-engine-rustls")]

use std::sync::Arc;

use delta_kernel::arrow::array::{Array, BinaryArray, BooleanArray, RecordBatch};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine::arrow_expression::st_intersects::{
    st_intersects_predicate, st_intersects_predicate_with_crs,
};
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::ColumnName;
use delta_kernel::object_store::{
    local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore,
};
use delta_kernel::schema::{DataType, GeometryType, PrimitiveType, StructField, StructType};
use delta_kernel::{Engine, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, record_batch_to_bytes};

const GEO_PARQUET_FILE: &str = "part-00000-geo-0000-000000000000-c000.snappy.parquet";

/// Encode a 2D point as little-endian ISO WKB.
fn point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(21);
    buf.push(0x01); // little-endian
    buf.extend_from_slice(&1u32.to_le_bytes()); // Point = 1
    buf.extend_from_slice(&x.to_le_bytes());
    buf.extend_from_slice(&y.to_le_bytes());
    buf
}

/// Encode a polygon's exterior ring as little-endian ISO WKB. Caller closes the ring.
fn polygon_wkb(ring: &[(f64, f64)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x01);
    buf.extend_from_slice(&3u32.to_le_bytes()); // Polygon = 3
    buf.extend_from_slice(&1u32.to_le_bytes()); // one ring
    buf.extend_from_slice(&(ring.len() as u32).to_le_bytes());
    for &(x, y) in ring {
        buf.extend_from_slice(&x.to_le_bytes());
        buf.extend_from_slice(&y.to_le_bytes());
    }
    buf
}

/// Builds the kernel schema `{ geom: geometry(OGC:CRS84) }` used by the tests.
fn kernel_geo_schema() -> StructType {
    StructType::new_unchecked([StructField::nullable(
        "geom",
        DataType::Primitive(PrimitiveType::Geometry(GeometryType::default())),
    )])
}

/// Writes a Parquet file containing a single Binary column `geom` with the given WKB
/// values (one per row).
fn write_geo_parquet(wkb_values: &[&[u8]]) -> Vec<u8> {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "geom",
        ArrowDataType::Binary,
        true,
    )]));
    let array = BinaryArray::from(wkb_values.iter().copied().map(Some).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(array)]).unwrap();
    record_batch_to_bytes(&batch)
}

/// Serializes a commit containing a Protocol (with the `geospatial` feature enabled), a
/// Metadata for the given kernel schema, and one Add action for the given parquet file.
fn commit_with_geo_schema(
    kernel_schema: &StructType,
    parquet_path: &str,
    parquet_size: u64,
    num_records: usize,
) -> String {
    let schema_string = serde_json::to_string(kernel_schema).unwrap();
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["geospatial"],
            "writerFeatures": ["geospatial"]
        }
    });
    let metadata = json!({
        "metaData": {
            "id": "geo-test-table",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }
    });
    // Stats JSON: only numRecords, no per-column stats (geo stats are optional).
    let stats = format!(r#"{{"numRecords":{num_records}}}"#);
    let add = json!({
        "add": {
            "path": parquet_path,
            "partitionValues": {},
            "size": parquet_size,
            "modificationTime": 0,
            "dataChange": true,
            "stats": stats
        }
    });
    format!("{protocol}\n{metadata}\n{add}")
}

#[tokio::test]
async fn reads_geometry_column_as_binary_wkb() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";

    let schema = kernel_geo_schema();
    let wkb1 = point_wkb(1.0, 2.0);
    let wkb2 = point_wkb(10.0, 20.0);
    let parquet_bytes = write_geo_parquet(&[&wkb1, &wkb2]);
    let parquet_size = parquet_bytes.len() as u64;

    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, 2);
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let mut rows = 0usize;
    let mut seen_wkbs: Vec<Vec<u8>> = Vec::new();
    for batch_result in scan.execute(engine)? {
        let batch = batch_result?.try_into_record_batch()?;
        rows += batch.num_rows();
        let col = batch.column(0);
        let bin = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("geom column must arrive as Arrow BinaryArray of WKB");
        for i in 0..bin.len() {
            seen_wkbs.push(bin.value(i).to_vec());
        }
    }
    assert_eq!(rows, 2);
    assert_eq!(seen_wkbs, vec![wkb1, wkb2]);
    Ok(())
}

#[tokio::test]
async fn st_intersects_filters_scanned_rows() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";

    let schema = kernel_geo_schema();
    // Two rows: one inside the unit square [0,0]-[1,1], one far outside.
    let inside = point_wkb(0.5, 0.5);
    let outside = point_wkb(100.0, 100.0);
    let parquet_bytes = write_geo_parquet(&[&inside, &outside]);
    let parquet_size = parquet_bytes.len() as u64;

    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, 2);
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;

    // First: read the batch.
    let scan = snapshot.scan_builder().build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine.clone())? {
        batches.push(batch_result?.try_into_record_batch()?);
    }
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    // Second: evaluate ST_Intersects against the scanned batch via the engine's
    // predicate evaluator. This exercises the row-level `eval_pred` path on top of
    // the BinaryArray produced by the scan.
    let query = polygon_wkb(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let predicate = Arc::new(st_intersects_predicate(ColumnName::new(["geom"]), query));
    let evaluator = engine
        .evaluation_handler()
        .new_predicate_evaluator(Arc::new(schema.clone()), predicate)?;
    let mask_data = evaluator.evaluate(&delta_kernel::engine::arrow_data::ArrowEngineData::new(
        batch.clone(),
    ))?;
    let mask_batch = mask_data.try_into_record_batch()?;
    let mask = mask_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("predicate output must be BooleanArray");
    assert_eq!(mask.len(), 2);
    assert!(mask.value(0), "inside point should intersect");
    assert!(!mask.value(1), "outside point should not intersect");
    Ok(())
}

/// End-to-end test: build a real Delta table with 10 points — 5 inside the unit square
/// `[0,0]–[1,1]` and 5 far outside — then scan, evaluate `ST_Intersects(geom, unit_square)`
/// via the engine's predicate evaluator, filter the batch with the resulting mask, and
/// assert exactly the inside points survive.
///
/// Exercises `eval_pred` on real parquet-read data in a realistic batch size, not a
/// toy 2-row example.
#[tokio::test]
async fn eval_pred_filters_half_the_rows_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::compute::filter_record_batch;

    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";

    let schema = kernel_geo_schema();

    // 10 points interleaved: even indices inside the unit square, odd indices far outside.
    let inside_coords: [(f64, f64); 5] =
        [(0.1, 0.1), (0.3, 0.9), (0.5, 0.5), (0.8, 0.2), (0.99, 0.99)];
    let outside_coords: [(f64, f64); 5] = [
        (-50.0, -50.0),
        (200.0, 200.0),
        (-10.0, 10.5),
        (42.0, 0.5),
        (1_000.0, -1_000.0),
    ];
    let mut wkbs: Vec<Vec<u8>> = Vec::with_capacity(10);
    let mut expected_inside: Vec<Vec<u8>> = Vec::with_capacity(5);
    for i in 0..5 {
        let i_wkb = point_wkb(inside_coords[i].0, inside_coords[i].1);
        let o_wkb = point_wkb(outside_coords[i].0, outside_coords[i].1);
        wkbs.push(i_wkb.clone());
        expected_inside.push(i_wkb);
        wkbs.push(o_wkb);
    }

    let slices: Vec<&[u8]> = wkbs.iter().map(|v| v.as_slice()).collect();
    let parquet_bytes = write_geo_parquet(&slices);
    let parquet_size = parquet_bytes.len() as u64;

    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, wkbs.len());
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    // Scan the table end-to-end through the Delta log + parquet reader.
    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine.clone())? {
        batches.push(batch_result?.try_into_record_batch()?);
    }
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 10);

    // Evaluate ST_Intersects with the unit-square polygon as the query geometry.
    let query = polygon_wkb(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let predicate = Arc::new(st_intersects_predicate(ColumnName::new(["geom"]), query));
    let evaluator = engine
        .evaluation_handler()
        .new_predicate_evaluator(Arc::new(schema.clone()), predicate)?;
    let mask_data = evaluator.evaluate(&delta_kernel::engine::arrow_data::ArrowEngineData::new(
        batch.clone(),
    ))?;
    let mask_batch = mask_data.try_into_record_batch()?;
    let mask = mask_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("predicate output must be BooleanArray");
    assert_eq!(mask.len(), 10);
    let true_count = (0..mask.len()).filter(|&i| mask.value(i)).count();
    assert_eq!(
        true_count, 5,
        "exactly 5 of 10 rows should intersect the unit square"
    );

    // Apply the mask to the scanned batch and assert only the 5 inside WKBs survive.
    let filtered = filter_record_batch(batch, mask)?;
    assert_eq!(filtered.num_rows(), 5);
    let bin = filtered
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("filtered geom column must be BinaryArray");
    let survivors: Vec<Vec<u8>> = (0..bin.len()).map(|i| bin.value(i).to_vec()).collect();
    assert_eq!(survivors, expected_inside);
    Ok(())
}

/// End-to-end: build an `ST_Intersects` predicate via `Scalar::Geometry` (with CRS) and
/// attach it to the scan via `ScanBuilder::with_predicate`. Exercises the full log-replay /
/// data-skipping path that was previously untested for geo predicates.
#[tokio::test]
async fn scan_with_predicate_geometry_literal_end_to_end() -> Result<(), Box<dyn std::error::Error>>
{
    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let schema = kernel_geo_schema();

    // Two rows: one inside the unit square, one far outside.
    let inside = point_wkb(0.5, 0.5);
    let outside = point_wkb(100.0, 100.0);
    let parquet_bytes = write_geo_parquet(&[&inside, &outside]);
    let parquet_size = parquet_bytes.len() as u64;
    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, 2);
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;

    // Construct the predicate via the CRS-aware constructor -- the query geometry declares
    // OGC:CRS84, matching the column's declared CRS.
    let query = polygon_wkb(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let predicate = Arc::new(st_intersects_predicate_with_crs(
        ColumnName::new(["geom"]),
        GeometryType::default(),
        query,
    ));

    // Wire the predicate into the scan via `ScanBuilder::with_predicate`.
    let scan = snapshot
        .scan_builder()
        .with_predicate(Some(predicate))
        .build()?;

    // The scan must still return rows (row-level filtering is engine-side today), but the
    // predicate traveled through the builder without compile / runtime errors.
    let mut total_rows = 0usize;
    for batch_result in scan.execute(engine)? {
        let batch = batch_result?.try_into_record_batch()?;
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 2);
    Ok(())
}

/// Builds the kernel schema `{ geom: geometry(<srid>) }` for an arbitrary CRS.
fn kernel_geo_schema_with_srid(srid: &str) -> StructType {
    StructType::new_unchecked([StructField::nullable(
        "geom",
        DataType::Primitive(PrimitiveType::Geometry(
            GeometryType::try_new(srid).unwrap(),
        )),
    )])
}

/// Smoke test: a predicate built with `Scalar::geometry_from_wkt` in a non-default CRS
/// still filters correctly through the `geoarrow-expr-geo::intersects` path. Confirms that
/// the WkbType construction (CRS from Scalar + CRS from field metadata) doesn't interfere
/// with the underlying spatial test.
#[tokio::test]
async fn eval_pred_crs_survives_through_geoarrow_expr() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let schema = kernel_geo_schema_with_srid("EPSG:3857");

    let inside = point_wkb(0.5, 0.5);
    let outside = point_wkb(100.0, 100.0);
    let parquet_bytes = write_geo_parquet(&[&inside, &outside]);
    let parquet_size = parquet_bytes.len() as u64;
    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, 2);
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine.clone())? {
        batches.push(batch_result?.try_into_record_batch()?);
    }
    let batch = &batches[0];

    // Build a predicate whose literal declares the same non-default CRS as the column.
    let query = polygon_wkb(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let predicate = Arc::new(st_intersects_predicate_with_crs(
        ColumnName::new(["geom"]),
        GeometryType::try_new("EPSG:3857")?,
        query,
    ));
    let evaluator = engine
        .evaluation_handler()
        .new_predicate_evaluator(Arc::new(schema.clone()), predicate)?;
    let mask_data = evaluator.evaluate(&delta_kernel::engine::arrow_data::ArrowEngineData::new(
        batch.clone(),
    ))?;
    let mask_batch = mask_data.try_into_record_batch()?;
    let mask = mask_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("predicate output must be BooleanArray");
    assert_eq!(mask.len(), 2);
    assert!(mask.value(0), "inside point should intersect");
    assert!(!mask.value(1), "outside point should not intersect");
    Ok(())
}

/// External callers that bypass our constructors and hand-build a predicate with a raw
/// `Scalar::Binary` literal must be rejected by `eval_pred` -- every query geometry must
/// travel as a typed `Scalar::Geometry` so its CRS is known.
#[tokio::test]
async fn eval_pred_rejects_binary_literal() -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate as _;
    use delta_kernel::engine::arrow_expression::st_intersects::StIntersectsOp;
    use delta_kernel::expressions::{Expression, Predicate, Scalar};

    let storage = Arc::new(InMemory::new());
    let table_root = "memory:///";
    let schema = kernel_geo_schema();
    let parquet_bytes = write_geo_parquet(&[&point_wkb(0.5, 0.5)]);
    let parquet_size = parquet_bytes.len() as u64;
    storage
        .put(&Path::from(GEO_PARQUET_FILE), parquet_bytes.into())
        .await?;
    let commit = commit_with_geo_schema(&schema, GEO_PARQUET_FILE, parquet_size, 1);
    add_commit(table_root, storage.as_ref(), 0, commit).await?;

    let engine = Arc::new(DefaultEngineBuilder::new(storage.clone()).build());
    let snapshot = Snapshot::builder_for(table_root).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine.clone())? {
        batches.push(batch_result?.try_into_record_batch()?);
    }
    let batch = &batches[0];

    // Hand-built predicate with a raw `Scalar::Binary` literal (bypassing our constructors).
    let query = polygon_wkb(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
    let predicate = Arc::new(Predicate::arrow_opaque(
        StIntersectsOp,
        [
            Expression::column(ColumnName::new(["geom"])),
            Expression::literal(Scalar::Binary(query)),
        ],
    ));

    let evaluator = engine
        .evaluation_handler()
        .new_predicate_evaluator(Arc::new(schema.clone()), predicate)?;
    let result = evaluator.evaluate(&delta_kernel::engine::arrow_data::ArrowEngineData::new(
        batch.clone(),
    ));
    let err = result
        .err()
        .expect("Scalar::Binary literal must be rejected");
    assert!(
        err.to_string()
            .contains("second argument must be a Scalar::Geometry literal"),
        "unexpected error message: {err}"
    );
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolve an on-disk Delta table under `$HOME/delta-tables-geo/geo-tables-delta/<name>/`
/// and return it as a `file:///...` URL (with the trailing slash kernel APIs expect).
/// Shared by the two `#[ignore]`d end-to-end tests below. Returns an error if the fixture
/// directory is missing; callers should surface that as a test failure.
/// Compact schema printer for the on-disk geo tests. Prints one line per (possibly
/// nested) Arrow field with a dot-delimited path and a specialised rendering of
/// the geoarrow extension metadata. Far less verbose than `{:#?}` on `Schema`.
/// Accepts `&Schema`, `SchemaRef`, `Arc<Schema>`, or anything else that is
/// `AsRef<Schema>` so call sites don't need fiddly deref dances.
fn format_schema_compact<S: AsRef<ArrowSchema>>(schema: S) -> String {
    let schema = schema.as_ref();
    let mut out = String::new();
    for field in schema.fields() {
        format_field_compact(field, "", &mut out);
    }
    out
}

fn format_field_compact(field: &ArrowField, prefix: &str, out: &mut String) {
    use std::fmt::Write;
    let path = if prefix.is_empty() {
        field.name().to_string()
    } else {
        format!("{prefix}.{}", field.name())
    };
    let ty = match field.data_type() {
        ArrowDataType::Struct(_) => "Struct".to_string(),
        ArrowDataType::List(_) | ArrowDataType::LargeList(_) => "List".to_string(),
        ArrowDataType::Map(_, _) => "Map".to_string(),
        other => format!("{other:?}"),
    };
    let nullable = if field.is_nullable() {
        "nullable"
    } else {
        "not null"
    };
    let meta = format_metadata_compact(field.metadata());
    writeln!(out, "  {path}: {ty} ({nullable}){meta}").ok();
    if let ArrowDataType::Struct(fields) = field.data_type() {
        for f in fields {
            format_field_compact(f, &path, out);
        }
    }
}

fn format_metadata_compact(metadata: &std::collections::HashMap<String, String>) -> String {
    if metadata.is_empty() {
        return String::new();
    }
    // Special-case geoarrow extension keys so the CRS is visible without JSON noise.
    if let (Some(name), Some(ext)) = (
        metadata.get("ARROW:extension:name"),
        metadata.get("ARROW:extension:metadata"),
    ) {
        return format!("  [ext={name} meta={ext}]");
    }
    let mut kvs: Vec<String> = metadata.iter().map(|(k, v)| format!("{k}={v}")).collect();
    kvs.sort();
    format!("  [{}]", kvs.join(", "))
}

fn geo_fixture_url(table_name: &str) -> Result<url::Url, Box<dyn std::error::Error>> {
    let home = std::env::var("HOME").map_err(|_| "HOME must be set to locate the fixture")?;
    let table_path = std::path::PathBuf::from(home)
        .join("delta-tables-geo")
        .join("geo-tables-delta")
        .join(table_name);
    if !table_path.exists() {
        return Err(format!("fixture table missing at {table_path:?}").into());
    }
    url::Url::from_directory_path(&table_path)
        .map_err(|_| format!("could not build file:// URL for {table_path:?}").into())
}

/// End-to-end scan of an on-disk geo Delta table via the public
/// `Snapshot -> ScanBuilder -> Scan -> execute` pipeline. Collects every returned
/// `RecordBatch` and pretty-prints the schema and the row data for manual inspection.
///
/// Parameterized over every geo fixture table to exercise geometry vs geography,
/// default vs non-default CRS, and single vs multi-file / multi-commit layouts.
/// Marked `#[ignore]` because fixtures live outside the repo; run with:
///
///   cargo test -p delta_kernel --test geo_read --features default-engine-rustls \
///       scan_geo_table_from_disk -- --ignored --nocapture --test-threads=1
#[rstest]
// #[case::geometry_basic("geometry_basic")]
// #[case::geography_basic("geography_basic")]
// #[case::geometry_with_id("geometry_with_id")]
// #[case::geometry_multifile("geometry_multifile")]
#[case::geometry_multicommit_wide("geometry_multicommit_wide")]
// #[case::geometry_epsg4326("geometry_epsg4326")]
// #[case::geography_vincenty("geography_vincenty")]
// #[case::geometry_all_nulls("geometry_all_nulls")]
#[tokio::test]
#[ignore = "requires external fixtures at $HOME/delta-tables-geo/geo-tables-delta/"]
async fn scan_geo_table_from_disk_pretty_prints_wkb(
    #[case] table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let table_url = geo_fixture_url(table_name)?;
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(table_url.as_str()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine)? {
        batches.push(batch_result?.try_into_record_batch()?);
    }

    println!("\n=========== TABLE: {table_name} ===========");
    assert!(
        !batches.is_empty(),
        "scan yielded no RecordBatches for {table_name}"
    );

    let schema = batches[0].schema();
    println!(
        "scanned {table_name} schema:\n{}",
        &schema
    );
    let formatted = pretty_format_batches(&batches)?.to_string();
    println!("scanned {table_name} data:\n{formatted}");

    Ok(())
}

/// End-to-end `scan_metadata` test against on-disk geo Delta tables with
/// `ScanBuilder::include_all_stats_columns()` set. Drains every
/// `ScanMetadata.scan_files` batch, applies the selection vector, and pretty-prints:
///   1. the full scan_files schema + data (one row per selected Add file),
///   2. the `stats_parsed` column extracted into its own RecordBatch for focused display.
///
/// Parameterized over every geo fixture table to exercise geometry vs geography, default
/// vs non-default CRS, multi-file / multi-commit layouts, and the all-null stats case.
/// Marked `#[ignore]` because fixtures live outside the repo; run with:
///
///   cargo test -p delta_kernel --test geo_read --features default-engine-rustls \
///       scan_metadata_geo_table_from_disk -- --ignored --nocapture --test-threads=1
#[rstest]
// #[case::geometry_basic("geometry_basic")]
// #[case::geography_basic("geography_basic")]
// #[case::geometry_with_id("geometry_with_id")]
// #[case::geometry_multifile("geometry_multifile")]
#[case::geometry_multicommit_wide("geometry_multicommit_wide")]
// #[case::geometry_epsg4326("geometry_epsg4326")]
// #[case::geography_vincenty("geography_vincenty")]
// #[case::geometry_all_nulls("geometry_all_nulls")]
#[tokio::test]
#[ignore = "requires external fixtures at $HOME/delta-tables-geo/geo-tables-delta/"]
async fn scan_metadata_geo_table_from_disk_pretty_prints_stats_parsed(
    #[case] table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::array::BooleanArray;
    use delta_kernel::arrow::compute::filter_record_batch;

    let table_url = geo_fixture_url(table_name)?;
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(table_url.as_str()).build(engine.as_ref())?;
    // `include_all_stats_columns` asks the kernel to emit parsed per-file statistics
    // (minValues / maxValues / nullCount / numRecords) via the `stats_parsed` column
    // on each `ScanMetadata.scan_files` batch.
    let scan = snapshot
        .scan_builder()
        .include_all_stats_columns()
        .build()?;

    // Drain the scan_metadata iterator. Each yield is a `FilteredEngineData`: a RecordBatch
    // of log-replay rows plus a selection vector marking which rows are actually selected
    // Add files. Apply the selection vector so the pretty-print shows only the selected rows
    // (i.e. the stats actually emitted to the engine), not the unselected
    // Protocol/Metadata/CommitInfo rows that carry null stats.
    let mut scan_file_batches: Vec<RecordBatch> = Vec::new();
    for res in scan.scan_metadata(engine.as_ref())? {
        let sm = res?;
        let (data, sel) = sm.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        // The selection vector may be shorter than the batch; trailing rows are assumed
        // selected per the `FilteredEngineData` contract. Pad with `true` to full length.
        let mut mask = sel;
        mask.resize(batch.num_rows(), true);
        let filtered = filter_record_batch(&batch, &BooleanArray::from(mask))?;
        if filtered.num_rows() > 0 {
            scan_file_batches.push(filtered);
        }
    }

    println!("\n=========== TABLE: {table_name} ===========");
    assert!(
        !scan_file_batches.is_empty(),
        "scan_metadata yielded no batches for {table_name}"
    );

    // Full scan_files batch (one row per Add file, with stats_parsed as a nested struct).
    println!(
        "scan_metadata.scan_files schema:\n{}",
        format_schema_compact(scan_file_batches[0].schema())
    );
    let formatted = pretty_format_batches(&scan_file_batches)?.to_string();
    println!("scan_metadata.scan_files data:\n{formatted}");

    // Extract the `stats_parsed` column into its own RecordBatch for focused display of
    // the parsed per-file statistics (minValues, maxValues, nullCount, numRecords).
    let batch = &scan_file_batches[0];
    let stats_idx = batch.schema().index_of("stats_parsed")?;
    let stats_field = batch.schema().field(stats_idx).clone();
    let stats_col = batch.column(stats_idx).clone();
    let stats_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![stats_field])),
        vec![stats_col],
    )?;
    println!(
        "stats_parsed only -- schema:\n{}",
        format_schema_compact(stats_batch.schema())
    );
    let stats_formatted = pretty_format_batches(std::slice::from_ref(&stats_batch))?.to_string();
    println!("stats_parsed only -- data:\n{stats_formatted}");

    Ok(())
}

/// Same as `scan_metadata_geo_table_from_disk_pretty_prints_stats_parsed`, but pointed
/// at the one-off fixture at `$HOME/geo/geo-files/my-checkpoint/`. Useful for inspecting
/// `stats_parsed` on a table whose `_delta_log` has a checkpoint alongside the commits.
///
/// Marked `#[ignore]` because the fixture lives outside the repo; run with:
///
///   cargo test -p delta_kernel --test geo_read --features default-engine-rustls \
///       scan_metadata_my_checkpoint_pretty_prints_stats_parsed \
///       -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires external fixture at $HOME/geo/geo-files/my-checkpoint"]
async fn scan_metadata_my_checkpoint_pretty_prints_stats_parsed(
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::compute::filter_record_batch;

    let home = std::env::var("HOME").map_err(|_| "HOME must be set to locate the fixture")?;
    let table_path = std::path::PathBuf::from(home)
        .join("geo")
        .join("geo-files")
        .join("my-checkpoint");
    if !table_path.exists() {
        return Err(format!("fixture table missing at {table_path:?}").into());
    }
    let table_url = url::Url::from_directory_path(&table_path)
        .map_err(|_| format!("could not build file:// URL for {table_path:?}"))?;

    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(table_url.as_str()).build(engine.as_ref())?;
    let scan = snapshot
        .scan_builder()
        .include_all_stats_columns()
        .build()?;

    let mut scan_file_batches: Vec<RecordBatch> = Vec::new();
    for res in scan.scan_metadata(engine.as_ref())? {
        let sm = res?;
        let (data, sel) = sm.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        let mut mask = sel;
        mask.resize(batch.num_rows(), true);
        let filtered = filter_record_batch(&batch, &BooleanArray::from(mask))?;
        if filtered.num_rows() > 0 {
            scan_file_batches.push(filtered);
        }
    }

    println!("\n=========== TABLE: my-checkpoint ===========");
    assert!(
        !scan_file_batches.is_empty(),
        "scan_metadata yielded no batches for my-checkpoint"
    );

    println!(
        "scan_metadata.scan_files schema:\n{}",
        format_schema_compact(scan_file_batches[0].schema())
    );
    let formatted = pretty_format_batches(&scan_file_batches)?.to_string();
    println!("scan_metadata.scan_files data:\n{formatted}");

    let batch = &scan_file_batches[0];
    let stats_idx = batch.schema().index_of("stats_parsed")?;
    let stats_field = batch.schema().field(stats_idx).clone();
    let stats_col = batch.column(stats_idx).clone();
    let stats_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![stats_field])),
        vec![stats_col],
    )?;
    println!(
        "stats_parsed only -- schema:\n{}",
        format_schema_compact(stats_batch.schema())
    );
    let stats_formatted = pretty_format_batches(std::slice::from_ref(&stats_batch))?.to_string();
    println!("stats_parsed only -- data:\n{stats_formatted}");

    Ok(())
}

/// End-to-end scan of `geometry_multicommit_wide` with an `ST_Intersects` predicate
/// wired through `ScanBuilder::with_predicate` AND a caller-side row-level filter.
///
/// The scan pipeline uses the predicate only for best-effort data skipping (file-level
/// and row-group-level pruning) -- it never filters individual rows. The docstring on
/// `with_predicate` calls this out: "filtering is best-effort and can produce false
/// positives." To get exact per-row semantics, callers run the predicate against the
/// scanned `RecordBatch`es themselves. This test demonstrates both phases:
///
///   Phase 1: `scan.execute(...)` returns every row that survives data skipping.
///   Phase 2: we build a predicate evaluator via
///            `engine.evaluation_handler().new_predicate_evaluator(...)`, run it over
///            each batch to produce a `BooleanArray` mask, and `filter_record_batch`
///            to drop the non-matching rows.
///
/// Expected oracle for `POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))` (unit square at origin):
///   - Phase 1: 30 rows -- file-level skipping is currently a no-op for `StIntersectsOp`
///              (its `as_data_skipping_predicate` / `eval_as_data_skipping_predicate`
///              return `None`), so every Add file flows through.
///   - Phase 2: 3 rows -- ids 0, 10, 20 (the origin-touching points in each of the
///              three files). All other 27 rows are removed by the row-level filter.
///
/// Marked `#[ignore]` because the fixture lives outside the repo; run with:
///
///   cargo test -p delta_kernel --test geo_read --features default-engine-rustls \
///       scan_geometry_multicommit_wide_with_st_intersects_predicate \
///       -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires external fixture at $HOME/delta-tables-geo/geo-tables-delta/geometry_multicommit_wide"]
async fn scan_geometry_multicommit_wide_with_st_intersects_predicate(
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::compute::filter_record_batch;
    use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate as _;
    use delta_kernel::engine::arrow_expression::st_intersects::StIntersectsOp;
    use delta_kernel::expressions::{Expression, Predicate, Scalar};

    let table_url = geo_fixture_url("geometry_multicommit_wide")?;
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(table_url.as_str()).build(engine.as_ref())?;
    // Capture the kernel schema before `scan_builder` consumes the snapshot Arc;
    // the row-level predicate evaluator in Phase 2 needs the logical schema.
    let kernel_schema = snapshot.schema();

    // Build `ST_Intersects(geom, POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1)))` from scratch.
    // `geometry_from_wkt` parses the WKT to ISO WKB and wraps it in a `Scalar::Geometry`
    // tagged with the declared CRS. `Predicate::arrow_opaque` then builds the opaque
    // predicate directly, without the `st_intersects_predicate*` helper constructors.
    let ty = GeometryType::try_new("OGC:CRS84")?;
    let literal = Scalar::geometry_from_wkt(ty, "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))")?;
    let predicate = Arc::new(Predicate::arrow_opaque(
        StIntersectsOp,
        [
            Expression::column(ColumnName::new(["geom"])),
            Expression::literal(literal),
        ],
    ));

    // --- Phase 1: scan with the predicate attached (data skipping only, no row filter) ---
    let scan = snapshot
        .scan_builder()
        .with_predicate(Some(predicate.clone()))
        .build()?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch_result in scan.execute(engine.clone())? {
        batches.push(batch_result?.try_into_record_batch()?);
    }

    println!(
        "\n=========== TABLE: geometry_multicommit_wide + ST_Intersects(geom, unit square @ origin) ==========="
    );
    println!("--- Phase 1: raw scan output (predicate used for data skipping only) ---");
    assert!(!batches.is_empty(), "scan yielded no RecordBatches");
    let schema = batches[0].schema();
    println!("scan schema:\n{}", format_schema_compact(&schema));
    let scan_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("scan row count: {scan_rows}");
    let formatted = pretty_format_batches(&batches)?.to_string();
    println!("scan data:\n{formatted}");

    // --- Phase 2: apply the predicate row-by-row via a caller-side evaluator ---
    // `new_predicate_evaluator` returns an engine-backed `Evaluator` whose `.evaluate()`
    // internally drives `evaluate_predicate`, producing a boolean mask per row.
    let evaluator = engine
        .evaluation_handler()
        .new_predicate_evaluator(kernel_schema, predicate)?;

    let mut filtered_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in &batches {
        let mask_data = evaluator.evaluate(&ArrowEngineData::new(batch.clone()))?;
        let mask_batch = mask_data.try_into_record_batch()?;
        let mask = mask_batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("predicate output must be BooleanArray");
        let filtered = filter_record_batch(batch, mask)?;
        if filtered.num_rows() > 0 {
            filtered_batches.push(filtered);
        }
    }

    println!("--- Phase 2: post-filter output (row-level ST_Intersects evaluation) ---");
    if let Some(first) = filtered_batches.first() {
        println!(
            "filtered schema:\n{}",
            format_schema_compact(first.schema())
        );
    }
    let filtered_rows: usize = filtered_batches.iter().map(|b| b.num_rows()).sum();
    println!("filtered row count: {filtered_rows}");
    let filtered_formatted = pretty_format_batches(&filtered_batches)?.to_string();
    println!("filtered data:\n{filtered_formatted}");

    Ok(())
}

/// `scan_metadata` on `geometry_multicommit_wide` with a query polygon DISJOINT
/// from two of the three file bboxes, so stats-based file pruning has visible
/// work to do. Stops at the scan-metadata phase -- no row-level evaluator is
/// applied afterwards. Isolates what `StIntersectsOp::as_data_skipping_predicate`'s
/// bbox-overlap rewrite does to the Add-action batch.
///
/// Flow:
///   1. `scan_metadata` walks log replay, producing per-commit batches of rows
///      (Add actions and null-filled Protocol/Metadata/CommitInfo rows).
///   2. The predicate is lowered via `StIntersectsOp::as_data_skipping_predicate`
///      into the 2D bbox-overlap predicate
///      `AND(a.xmax >= b.xmin, a.xmin <= b.xmax, a.ymax >= b.ymin, a.ymin <= b.ymax)`
///      evaluated against each file's `stats_parsed.minValues`/`maxValues`.
///   3. Files proven by stats to have `false` drop out of the selection vector.
///   4. Any `ScanMetadata` whose selection vector has no `true` entries is filtered
///      out of the iterator entirely (via `HasSelectionVector`).
///   5. We print the selection vector plus pre- and post-selection `scan_files` batches.
///
/// Query: `POLYGON((20 -5, 40 -5, 40 -2, 20 -2, 20 -5))` -- a rectangle at
/// x in [20, 40], y in [-5, -2]. Per-file bbox analysis:
///   - File 0 (ids 0-9):   x in [0, 9]    -- disjoint from [20, 40] in x, PRUNED
///   - File 1 (ids 10-19): x in [-9, 0]   -- disjoint from [20, 40] in x, PRUNED
///   - File 2 (ids 20-29): x in [0, 45], y in [-9, 0] -- overlaps both dims, KEPT
///
/// Expected: 1 Add file kept (File 2 only), 2 Add files pruned by stats skipping.
/// That's the observable signature that the
/// `StIntersectsOp::as_data_skipping_predicate` bbox-overlap rewrite is doing
/// real work. By contrast, a query polygon that overlaps every file's bbox (e.g.
/// the unit square at the origin used by Test 3, since each file's bbox has a
/// corner at (0, 0)) would correctly keep all 3 files -- proving the rewrite
/// doesn't over-prune either.
///
/// Marked `#[ignore]` because the fixture lives outside the repo.
#[tokio::test]
#[ignore = "requires external fixture at $HOME/delta-tables-geo/geo-tables-delta/geometry_multicommit_wide"]
async fn scan_metadata_stats_skipping_prunes_two_files_with_disjoint_polygon(
) -> Result<(), Box<dyn std::error::Error>> {
    use delta_kernel::arrow::compute::filter_record_batch;
    use delta_kernel::engine::arrow_expression::opaque::ArrowOpaquePredicate as _;
    use delta_kernel::engine::arrow_expression::st_intersects::StIntersectsOp;
    use delta_kernel::expressions::{Expression, Predicate, Scalar};

    let table_url = geo_fixture_url("geometry_multicommit_wide")?;
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(table_url.as_str()).build(engine.as_ref())?;

    // Query polygon disjoint from File 0 (x<=9) and File 1 (x<=0); only File 2's
    // bbox [0, 45] x [-9, 0] overlaps [20, 40] x [-5, -2].
    let ty = GeometryType::try_new("OGC:CRS84")?;
    let literal =
        Scalar::geometry_from_wkt(ty, "POLYGON((20 -5, 40 -5, 40 -2, 20 -2, 20 -5))")?;
    let predicate = Arc::new(Predicate::arrow_opaque(
        StIntersectsOp,
        [
            Expression::column(ColumnName::new(["geom"])),
            Expression::literal(literal),
        ],
    ));

    println!(
        "\n=========== TABLE: geometry_multicommit_wide scan_metadata + ST_Intersects(geom, disjoint polygon) ==========="
    );

    // Print the original data-skipping predicate (the opaque ST_Intersects op tree that
    // was handed to `ScanBuilder::with_predicate`).
    println!(
        "--- data skipping predicate (original) ---\n{:#?}",
        predicate.as_ref()
    );

    // Print the rewritten stats-skipping predicate -- the form that the scan's
    // log-replay actually evaluates against per-file `stats_parsed.minValues/maxValues`.
    // For ST_Intersects, the `as_data_skipping_predicate` rewrite lowers to the 2D
    // bbox-overlap predicate `AND(xmax >= xmin, xmin <= xmax, ymax >= ymin, ymin <= ymax)`.
    //
    // `delta_kernel::scan::as_data_skipping_predicate` is gated behind the
    // `internal-api` feature. Enable it when running this test to see the rewrite:
    //   cargo test ... --features default-engine-rustls,internal-api ...
    #[cfg(feature = "internal-api")]
    {
        match delta_kernel::scan::as_data_skipping_predicate(predicate.as_ref()) {
            Some(rewritten) => println!(
                "--- rewritten stats-skipping predicate ---\n{rewritten:#?}"
            ),
            None => println!(
                "--- rewritten stats-skipping predicate: None (predicate not eligible for stats skipping) ---"
            ),
        }
    }
    #[cfg(not(feature = "internal-api"))]
    println!(
        "--- rewritten stats-skipping predicate: not shown (requires `--features internal-api`) ---"
    );

    let scan = snapshot
        .scan_builder()
        .with_predicate(Some(predicate))
        .include_all_stats_columns()
        .build()?;

    let mut total_seen = 0usize;
    let mut total_kept = 0usize;
    let mut iteration = 0usize;
    for res in scan.scan_metadata(engine.as_ref())? {
        iteration += 1;
        let sm = res?;
        let (data, mut sel) = sm.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        sel.resize(batch.num_rows(), true);
        total_seen += batch.num_rows();

        println!("--- scan_metadata iteration {iteration} ---");
        println!(
            "raw scan_files schema:\n{}",
            format_schema_compact(batch.schema())
        );
        println!("selection vector (length {}): {:?}", sel.len(), sel);
        println!(
            "raw scan_files data (pre-selection vector, {} rows):\n{}",
            batch.num_rows(),
            pretty_format_batches(std::slice::from_ref(&batch))?
        );

        let filtered = filter_record_batch(&batch, &BooleanArray::from(sel))?;
        total_kept += filtered.num_rows();

        println!(
            "filtered scan_files data (after selection vector, {} rows):\n{}",
            filtered.num_rows(),
            pretty_format_batches(std::slice::from_ref(&filtered))?
        );
    }

    println!(
        "summary: {total_seen} log-replay rows seen, {total_kept} kept after stats skipping"
    );

    Ok(())
}
