//! End-to-end integration test for IcebergCompatV3:
//!   1. Create a V3 table with a schema containing Array<int> and Map<int, int> columns.
//!   2. Append a row of data (currently `Array<int>` + `Map<int, int>` columns empty/null ok).
//!   3. Read the written parquet file directly and assert that:
//!       - Top-level fields carry `field_id` in the parquet schema.
//!       - Array `element` fields carry `field_id`.
//!       - Map `key`/`value` fields carry `field_id`.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StructArray};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::Scalar;
use delta_kernel::parquet::file::reader::{FileReader, SerializedFileReader};
use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::Snapshot;
use rstest::rstest;
use tempfile::tempdir;
use test_utils::{create_default_engine, write_batch_to_table};
use url::Url;

/// Walks the parquet schema and returns every leaf/group field's `field_id` keyed by its full
/// dot-separated path (e.g. `"col1.list.element"`, `"col2.key_value.key"`). Fields without a
/// `field_id` set are omitted.
fn collect_parquet_field_ids(
    parquet_file: &std::path::Path,
) -> std::collections::HashMap<String, i32> {
    fn walk(
        ty: &delta_kernel::parquet::schema::types::Type,
        path: Vec<String>,
        out: &mut std::collections::HashMap<String, i32>,
    ) {
        // Skip the synthetic root. parquet-rs assigns the root no id by convention.
        if !path.is_empty() {
            let info = ty.get_basic_info();
            if info.has_id() {
                out.insert(path.join("."), info.id());
            }
        }
        if ty.is_group() {
            for child in ty.get_fields() {
                let mut p = path.clone();
                p.push(child.name().to_string());
                walk(child, p, out);
            }
        }
    }

    let file = std::fs::File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let root = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .clone();
    let mut out = std::collections::HashMap::new();
    walk(&root, Vec::new(), &mut out);
    out
}

#[tokio::test]
async fn test_v3_create_write_read_parquet_field_ids() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // --- Schema: one primitive + one Array<int> + one Map<int, int> column ---
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("value", DataType::INTEGER),
        StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
        ),
        StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::INTEGER,
                DataType::INTEGER,
                true,
            ))),
        ),
    ])?);

    // --- Setup temp dir + engine ---
    let tmp = tempdir()?;
    let table_url = Url::from_directory_path(tmp.path()).unwrap();
    let engine = create_default_engine(&table_url)?;

    // --- Create V3 table using the real CreateTableTransactionBuilder so the V3 dependency
    // auto-enable runs end-to-end and nested field ids are written into the Metadata action. ---
    let _ = create_table(tmp.path().to_str().unwrap(), schema.clone(), "Test/1.0")
        .with_table_properties(vec![("delta.enableIcebergCompatV3", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Load the snapshot and verify V3 came out as expected (sanity-check the auto-enable).
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let props = snapshot.table_configuration().table_properties();
    assert_eq!(props.enable_iceberg_compat_v3, Some(true));
    assert_eq!(props.enable_row_tracking, Some(true));

    // --- Build an input RecordBatch matching the logical schema. The arrow schema used here
    // is whatever the arrow builders produce; apply_schema_to will coerce it to the table's
    // physical schema (with `PARQUET:field_id` stamped per our fix) before the writer runs. ---
    let value_array: ArrayRef = Arc::new(Int32Array::from(vec![42i32]));

    // One-element list: [[1, 2, 3]]. Use kernel's arrow conversion conventions:
    // list inner field is named "element", map inner struct is named "key_value" with
    // children "key" and "value". These match `TryFromKernel<&ArrayType>/<&MapType>`.
    let list_values = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let list_field = Arc::new(ArrowField::new("element", ArrowDataType::Int32, true));
    let list_array = Arc::new(ListArray::try_new(
        list_field,
        OffsetBuffer::new(vec![0, 3].into()),
        list_values,
        None,
    )?);

    // One-element map: [{10: 20, 30: 40}]
    let map_keys = Arc::new(Int32Array::from(vec![10, 30])) as ArrayRef;
    let map_vals = Arc::new(Int32Array::from(vec![20, 40])) as ArrayRef;
    let map_struct = Arc::new(StructArray::try_new(
        vec![
            ArrowField::new("key", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]
        .into(),
        vec![map_keys, map_vals],
        Option::<NullBuffer>::None,
    )?);
    let map_entries_field = Arc::new(ArrowField::new(
        "key_value",
        map_struct.data_type().clone(),
        false,
    ));
    let map_array = Arc::new(MapArray::try_new(
        map_entries_field,
        OffsetBuffer::new(vec![0, 2].into()),
        map_struct.as_ref().clone(),
        None,
        false,
    )?);

    // Build the RecordBatch using the logical schema converted to arrow. kernel's
    // `apply_schema_to` pipeline renames logical → physical and stamps PARQUET:field_id
    // (including nested ids) before handing it to the parquet writer.
    let arrow_schema: delta_kernel::arrow::datatypes::Schema = schema.as_ref().try_into_arrow()?;
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![value_array, list_array, map_array],
    )?;

    // --- Commit a data-change append. ---
    let _new_snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, Default::default()).await?;

    // --- Find the written parquet file and inspect its schema. ---
    fn collect_parquet(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                // Skip the _delta_log dir -- we want data files only.
                if p.file_name().and_then(|n| n.to_str()) == Some("_delta_log") {
                    continue;
                }
                collect_parquet(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    let mut parquet_files = Vec::new();
    collect_parquet(tmp.path(), &mut parquet_files);
    assert_eq!(
        parquet_files.len(),
        1,
        "expected exactly one parquet data file under {:?}; found: {parquet_files:?}",
        tmp.path()
    );
    let parquet_file = parquet_files.pop().unwrap();

    let ids_by_path = collect_parquet_field_ids(&parquet_file);
    assert!(
        !ids_by_path.is_empty(),
        "no field_ids on any parquet field; V3 nested id propagation is broken"
    );

    // --- Assertions ---
    // Pull the IDs kernel assigned from the snapshot's logical schema, then assert each one
    // shows up in the parquet file at the expected nested path.
    use delta_kernel::schema::{ColumnMetadataKey, MetadataValue};
    let logical = snapshot.table_configuration().logical_schema();

    let phys = |field_name: &str| -> String {
        let field = logical.field(field_name).unwrap();
        match field
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap()
        {
            MetadataValue::String(s) => s.clone(),
            v => panic!("physicalName on {field_name} not a string: {v:?}"),
        }
    };
    let nested_id = |field_name: &str, role: &str| -> i32 {
        let field = logical.field(field_name).unwrap();
        let nested = match field
            .metadata
            .get(ColumnMetadataKey::ParquetFieldNestedIds.as_ref())
            .unwrap_or_else(|| panic!("field '{field_name}' missing parquet.field.nested.ids"))
        {
            MetadataValue::Other(v) => v,
            v => panic!("parquet.field.nested.ids on {field_name} not a JSON object: {v:?}"),
        };
        let key = format!("{}.{role}", phys(field_name));
        nested
            .get(&key)
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| panic!("missing nested id at '{key}': {nested}"))
            as i32
    };

    // Parquet paths use physical column names; 3-level list/map encodings add `.list.element`
    // and `.key_value.{key,value}` under the column.
    let arr_phys = phys("arr");
    let m_phys = phys("m");
    let expected_arr_element = nested_id("arr", "element");
    let expected_m_key = nested_id("m", "key");
    let expected_m_value = nested_id("m", "value");

    assert_eq!(
        ids_by_path.get(&format!("{arr_phys}.list.element")).copied(),
        Some(expected_arr_element),
        "parquet field_id at {arr_phys}.list.element must match the id kernel wrote to \
         parquet.field.nested.ids. all ids: {ids_by_path:?}"
    );
    assert_eq!(
        ids_by_path.get(&format!("{m_phys}.key_value.key")).copied(),
        Some(expected_m_key),
        "parquet field_id at {m_phys}.key_value.key must match the id kernel wrote to \
         parquet.field.nested.ids. all ids: {ids_by_path:?}"
    );
    assert_eq!(
        ids_by_path
            .get(&format!("{m_phys}.key_value.value"))
            .copied(),
        Some(expected_m_value),
        "parquet field_id at {m_phys}.key_value.value must match the id kernel wrote to \
         parquet.field.nested.ids. all ids: {ids_by_path:?}"
    );

    Ok(())
}

/// Verifies that on an IcebergCompatV3 table, partition column values are materialized into
/// the parquet data file (instead of only being recorded in the partition path / Add action),
/// and that on a non-V3 partitioned table they are NOT materialized.
///
/// This guards the `should_materialize_partition_columns()` branch added for V3 (see PR
/// #2504): without that branch, V3 writes would skip the partition column from the physical
/// write schema and produce parquet files an Iceberg reader could not consume.
#[rstest]
#[case::v3_enabled_materializes(true, true)]
#[case::v3_disabled_does_not_materialize(false, false)]
#[tokio::test]
async fn test_partition_column_materialization_in_parquet(
    #[case] enable_v3: bool,
    #[case] expect_pcol_in_parquet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // === Schema: one data column + one partition column ===
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("value", DataType::INTEGER),
        StructField::nullable("pcol", DataType::INTEGER),
    ])?);

    // === Setup ===
    let tmp = tempdir()?;
    let table_url = Url::from_directory_path(tmp.path()).unwrap();
    let engine = create_default_engine(&table_url)?;

    // === Create the table, partitioned by `pcol`, with V3 toggled per case ===
    let mut builder = create_table(tmp.path().to_str().unwrap(), schema.clone(), "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["pcol"]));
    if enable_v3 {
        builder = builder.with_table_properties(vec![("delta.enableIcebergCompatV3", "true")]);
    }
    let _ = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(
        snapshot.table_configuration().table_properties().enable_iceberg_compat_v3,
        if enable_v3 { Some(true) } else { None },
    );

    // === Write a single-row batch with pcol=7 ===
    let value_array: ArrayRef = Arc::new(Int32Array::from(vec![42i32]));
    let pcol_array: ArrayRef = Arc::new(Int32Array::from(vec![7i32]));
    let arrow_schema: delta_kernel::arrow::datatypes::Schema = schema.as_ref().try_into_arrow()?;
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![value_array, pcol_array])?;

    let mut partition_values = HashMap::new();
    partition_values.insert("pcol".to_string(), Scalar::Integer(7));

    let _ = write_batch_to_table(&snapshot, engine.as_ref(), batch, partition_values).await?;

    // === Find the parquet data file ===
    fn collect_parquet(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                if p.file_name().and_then(|n| n.to_str()) == Some("_delta_log") {
                    continue;
                }
                collect_parquet(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    let mut parquet_files = Vec::new();
    collect_parquet(tmp.path(), &mut parquet_files);
    assert_eq!(
        parquet_files.len(),
        1,
        "expected exactly one parquet data file under {:?}; found: {parquet_files:?}",
        tmp.path()
    );
    let parquet_file = parquet_files.pop().unwrap();

    // === Read the raw parquet schema and check whether `pcol`'s physical name is present ===
    let logical = snapshot.table_configuration().logical_schema();
    let pcol_phys_name = physical_name_of(logical.as_ref(), "pcol");
    let value_phys_name = physical_name_of(logical.as_ref(), "value");

    let top_level_names: Vec<String> = top_level_parquet_field_names(&parquet_file);

    assert!(
        top_level_names.contains(&value_phys_name),
        "data column '{value_phys_name}' must always be present in parquet; got {top_level_names:?}"
    );
    assert_eq!(
        top_level_names.contains(&pcol_phys_name),
        expect_pcol_in_parquet,
        "partition column '{pcol_phys_name}' presence in parquet (V3 enabled = {enable_v3}) \
         did not match expectation; got {top_level_names:?}"
    );

    Ok(())
}

/// Returns the physical column name kernel assigned to `field_name` (logical name).
/// Falls back to the logical name when column mapping is disabled.
fn physical_name_of(logical: &StructType, field_name: &str) -> String {
    use delta_kernel::schema::{ColumnMetadataKey, MetadataValue};
    let field = logical.field(field_name).unwrap();
    match field
        .metadata
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        Some(MetadataValue::String(s)) => s.clone(),
        _ => field_name.to_string(),
    }
}

/// Returns the names of the top-level fields of the parquet file's schema, in declaration order.
fn top_level_parquet_field_names(parquet_file: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let root = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .clone();
    root.get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}
