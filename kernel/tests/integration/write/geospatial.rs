//! Integration tests for writing tables with geospatial (geometry and geography) columns.
//!
//! The default engine cannot write geospatial Parquet data, so these tests stage add-file metadata
//! directly, with the WKT bounding-box statistics a geospatial-aware engine would produce.

use std::sync::Arc;

use delta_kernel::actions::{
    MAX_VALUES, MIN_VALUES, NULL_COUNT, NUM_RECORDS, STATS_PARSED, TIGHT_BOUNDS,
};
use delta_kernel::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, StructArray,
};
use delta_kernel::arrow::datatypes::{Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{
    schema_ref, DataType, EdgeInterpolationAlgorithm, GeographyType, GeometryType, SchemaRef,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::EngineData;
use rstest::rstest;
use serde_json::{json, Value};
use test_utils::{
    create_add_files_metadata, get_column, into_record_batch, read_actions_from_commit,
    test_table_setup_mt,
};

use crate::common::read_utils::read_parquet_file;
use crate::common::write_utils::load_existing_single_file_checkpoint_path;

const LOWER_LEFT: &str = "POINT(-122.42 37.77)";
const UPPER_RIGHT: &str = "POINT(-73.99 40.75)";

fn struct_array(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns
        .into_iter()
        .map(|(name, array)| (Field::new(name, array.data_type().clone(), true), array))
        .unzip();
    Arc::new(StructArray::new(fields.into(), arrays, None))
}

/// Add-file metadata for one file whose statistics carry WKT bounds for both geospatial columns.
fn add_file_with_wkt_bounds(
    add_files_schema: &SchemaRef,
) -> Result<Box<dyn EngineData>, Box<dyn std::error::Error>> {
    let batch = into_record_batch(create_add_files_metadata(
        add_files_schema,
        vec![("geo.parquet", 1024, 1_000_000, Some(3))],
    )?);
    let bounds = |id: i64, point: &str| {
        struct_array(vec![
            ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
            ("geom", Arc::new(StringArray::from(vec![point]))),
            ("geog", Arc::new(StringArray::from(vec![point]))),
        ])
    };
    let stats = struct_array(vec![
        (NUM_RECORDS, Arc::new(Int64Array::from(vec![3])) as ArrayRef),
        (
            NULL_COUNT,
            struct_array(vec![
                ("id", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
                ("geom", Arc::new(Int64Array::from(vec![1]))),
                ("geog", Arc::new(Int64Array::from(vec![0]))),
            ]),
        ),
        (MIN_VALUES, bounds(1, LOWER_LEFT)),
        (MAX_VALUES, bounds(3, UPPER_RIGHT)),
        (TIGHT_BOUNDS, Arc::new(BooleanArray::from(vec![true]))),
    ]);

    let stats_index = batch.schema().index_of("stats")?;
    let mut fields = batch.schema().fields().to_vec();
    fields[stats_index] = Arc::new(Field::new("stats", stats.data_type().clone(), true));
    let mut columns = batch.columns().to_vec();
    columns[stats_index] = stats;
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
    Ok(Box::new(ArrowEngineData::new(batch)))
}

/// Commits a file with WKT bounds to a geospatial table, either in the CREATE TABLE transaction
/// or in a later append, and checkpoints the result. Kernel commits the engine's statistics
/// unchanged and the checkpoint keeps them in `add.stats`, while `stats_parsed` holds only the
/// geospatial null counts because kernel's stats schema has no geospatial min/max.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_commit_and_checkpoint_geospatial_table(
    #[values(false, true)] in_create_transaction: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let geometry: DataType = GeometryType::try_new("OGC:CRS84")?.into();
    let geography: DataType =
        GeographyType::try_new("OGC:CRS84", EdgeInterpolationAlgorithm::Spherical)?.into();
    let schema = schema_ref! {
        not_null "id": LONG,
        nullable "geom": (geometry),
        nullable "geog": (geography),
    };

    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let mut create = create_table(&table_path, schema, "Test/1.0", true)
        .with_table_properties([("delta.checkpoint.writeStatsAsStruct", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let snapshot = if in_create_transaction {
        create.add_files(add_file_with_wkt_bounds(create.add_files_schema())?);
        create
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot()
    } else {
        let snapshot = create
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let mut txn = snapshot
            .update_table(UpdateTableOperation::Write, "Test/1.0", true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        txn.add_files(add_file_with_wkt_bounds(txn.add_files_schema())?);
        txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot()
    };
    let table_url = snapshot.table_root().clone();

    let adds = read_actions_from_commit(&table_url, snapshot.version(), "add")?;
    assert_eq!(adds.len(), 1);
    let commit_stats = adds[0]["stats"]
        .as_str()
        .expect("add action must carry stats");
    let expected_stats = json!({
        "numRecords": 3,
        "nullCount": { "id": 0, "geom": 1, "geog": 0 },
        "minValues": { "id": 1, "geom": LOWER_LEFT, "geog": LOWER_LEFT },
        "maxValues": { "id": 3, "geom": UPPER_RIGHT, "geog": UPPER_RIGHT },
        "tightBounds": true,
    });
    assert_eq!(serde_json::from_str::<Value>(commit_stats)?, expected_stats);

    snapshot.checkpoint(engine.as_ref(), None)?;
    let checkpoint_path =
        load_existing_single_file_checkpoint_path(&table_path, snapshot.version());
    let checkpoint_batch = read_parquet_file(&checkpoint_path);
    let checkpoint_add = get_column!(checkpoint_batch, "add", StructArray);
    let add_row = (0..checkpoint_batch.num_rows())
        .find(|&row| checkpoint_add.is_valid(row))
        .expect("checkpoint must contain an add action");
    let checkpoint_stats = get_column!(checkpoint_add, "stats", StringArray).value(add_row);
    assert_eq!(
        serde_json::from_str::<Value>(checkpoint_stats)?,
        expected_stats
    );

    let stats_parsed = get_column!(checkpoint_add, STATS_PARSED, StructArray);
    let null_count = get_column!(stats_parsed, NULL_COUNT, StructArray);
    assert_eq!(
        get_column!(null_count, "geom", Int64Array).value(add_row),
        1
    );
    assert_eq!(
        get_column!(null_count, "geog", Int64Array).value(add_row),
        0
    );
    for category in [MIN_VALUES, MAX_VALUES] {
        let values = get_column!(stats_parsed, category, StructArray);
        assert!(values.column_by_name("id").is_some());
        assert!(values.column_by_name("geom").is_none());
        assert!(values.column_by_name("geog").is_none());
    }

    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(reloaded.version(), snapshot.version());
    Ok(())
}
