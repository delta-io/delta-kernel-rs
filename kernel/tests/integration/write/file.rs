//! Write-path integration tests for `file` columns.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int32Array, Int64Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::parquet::basic::LogicalType;
use delta_kernel::parquet::schema::types::Type as ParquetType;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::KernelResult;
use rstest::rstest;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    create_table_and_load_snapshot, read_add_infos, read_scan, test_table_setup_mt,
    write_batch_to_table,
};
use url::Url;

use crate::common::write_utils::read_parquet_root_schema;

/// The `(uri, offset, size, inline)` fields of a non-null `file` value.
type Value<'a> = (Option<&'a str>, Option<i64>, Option<i64>, Option<&'a [u8]>);

/// Builds a `file` array from `rows`; `None` is a null `file` value.
fn file_array(rows: &[Option<Value<'_>>]) -> StructArray {
    let ArrowDataType::Struct(fields) = (&DataType::file_type()).try_into_arrow().unwrap() else {
        panic!("a file is physically a struct");
    };
    let content_type = |row: &Option<Value<'_>>| row.and(Some("image/png"));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter(
            rows.iter().map(|r| r.and_then(|v| v.0)),
        )),
        Arc::new(Int64Array::from_iter(
            rows.iter().map(|r| r.and_then(|v| v.1)),
        )),
        Arc::new(Int64Array::from_iter(
            rows.iter().map(|r| r.and_then(|v| v.2)),
        )),
        Arc::new(StringArray::from_iter(rows.iter().map(content_type))),
        Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
        Arc::new(BinaryArray::from_iter(
            rows.iter().map(|r| r.and_then(|v| v.3)),
        )),
    ];
    let nulls = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
    StructArray::try_new(fields, columns, Some(nulls)).unwrap()
}

/// Three valid `file` values: an external byte range, an inline value, and a null.
fn file_values() -> StructArray {
    file_array(&[
        Some((Some("s3://bucket/a.png"), Some(4), Some(10), None)),
        Some((None, None, None, Some(b"xyz".as_slice()))),
        None,
    ])
}

/// A `file` column `c` at top level, in a struct (`c.f`), as an array element, or as a map value,
/// holding `files`.
fn file_column(
    layout: &str,
    files: StructArray,
    struct_nulls: Option<NullBuffer>,
) -> (DataType, ArrayRef) {
    match layout {
        "top_level" => (DataType::file_type(), Arc::new(files)),
        "in_struct" => {
            let field = ArrowField::new("f", files.data_type().clone(), true);
            let array = StructArray::try_new(
                vec![field].into(),
                vec![Arc::new(files) as ArrayRef],
                struct_nulls,
            )
            .unwrap();
            let data_type =
                StructType::try_new([StructField::nullable("f", DataType::file_type())])
                    .unwrap()
                    .into();
            (data_type, Arc::new(array))
        }
        "map_value" => {
            let entries = StructArray::from(vec![
                (
                    Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                    Arc::new(StringArray::from_iter_values(
                        (0..files.len()).map(|i| i.to_string()),
                    )) as ArrayRef,
                ),
                (
                    Arc::new(ArrowField::new("value", files.data_type().clone(), true)),
                    Arc::new(files.clone()) as ArrayRef,
                ),
            ]);
            let entries_field = ArrowField::new("key_value", entries.data_type().clone(), false);
            let array = MapArray::new(
                Arc::new(entries_field),
                OffsetBuffer::from_lengths(vec![1; files.len()]),
                entries,
                None,
                false,
            );
            let data_type = MapType::new(DataType::STRING, DataType::file_type(), true).into();
            (data_type, Arc::new(array))
        }
        "array_element" => {
            let element = ArrowField::new("element", files.data_type().clone(), true);
            let lengths = vec![1; files.len()];
            let array = ListArray::new(
                Arc::new(element),
                OffsetBuffer::from_lengths(lengths),
                Arc::new(files),
                None,
            );
            (
                ArrayType::new(DataType::file_type(), true).into(),
                Arc::new(array),
            )
        }
        _ => unreachable!("unknown layout {layout}"),
    }
}

/// A table written by [`write_table`]: its directory (kept alive), path, engine, and the result of
/// the write.
type WrittenTable = (
    tempfile::TempDir,
    String,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    KernelResult<SnapshotRef>,
);

/// Creates a table with columns `id` and `c` in `cm_mode` and writes one batch holding `column`.
async fn write_table(
    cm_mode: &str,
    data_type: DataType,
    column: ArrayRef,
) -> Result<WrittenTable, Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("c", data_type),
    ])?);
    let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema.clone(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", cm_mode)],
    )?;
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
    let ids: Vec<i32> = (0..column.len() as i32).collect();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int32Array::from(ids)), column],
    )?;
    let result = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await;
    Ok((tmp_dir, table_path, engine, result))
}

/// Returns the sub-field ids of each `FILE`-annotated group in `schema`, with the group's own id.
fn file_groups(schema: &ParquetType) -> Vec<(Option<i32>, Vec<Option<i32>>)> {
    let id = |field: &ParquetType| {
        let info = field.get_basic_info();
        info.has_id().then(|| info.id())
    };
    let mut out = Vec::new();
    let mut stack = vec![schema];
    while let Some(field) = stack.pop() {
        if !field.is_group() {
            continue;
        }
        if field.get_basic_info().logical_type_ref() == Some(&LogicalType::File) {
            let sub_field_ids = field.get_fields().iter().map(|f| id(f)).collect();
            out.push((id(field), sub_field_ids));
        }
        stack.extend(field.get_fields().iter().map(|f| f.as_ref()));
    }
    out
}

/// A `file` column at top level, in a struct, as an array element, or as a map value is written as
/// a `FILE`-annotated group whose sub-field ids follow its own id, and reads back unchanged.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_and_read_nested_file_column(
    #[values("top_level", "in_struct", "array_element", "map_value")] layout: &str,
    #[values("none", "id", "name")] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (data_type, column) = file_column(layout, file_values(), None);
    let (_tmp_dir, table_path, engine, snapshot) =
        write_table(cm_mode, data_type, column.clone()).await?;
    let snapshot = snapshot?;

    let add_files = read_add_infos(&snapshot, engine.as_ref())?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let parquet_path = table_url.join(&add_files[0].path)?.to_file_path().unwrap();
    let root = read_parquet_root_schema(&parquet_path);
    let groups = file_groups(&root);
    assert_eq!(groups.len(), 1);
    let (id, sub_field_ids) = &groups[0];
    let column = snapshot.schema().field("c").unwrap().clone();
    // The column is written under its physical name.
    let physical_name = match column.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    {
        Some(MetadataValue::String(name)) => name.clone(),
        _ => "c".to_string(),
    };
    assert!(root
        .get_fields()
        .iter()
        .any(|field| field.name() == physical_name));
    // Under column mapping, a `file` struct field has its column mapping id. This test does not
    // enable Iceberg compatibility, so an array element or map value carries no id.
    let file_field = match (cm_mode, layout) {
        ("none", _) | (_, "array_element" | "map_value") => None,
        (_, "top_level") => Some(column),
        _ => {
            let DataType::Struct(inner) = column.data_type() else {
                panic!("`c` is a struct");
            };
            Some(inner.field("f").unwrap().clone())
        }
    };
    let expected_id = file_field.map(|field| {
        let Some(MetadataValue::Number(id)) =
            field.get_config_value(&ColumnMetadataKey::ColumnMappingId)
        else {
            panic!("a column-mapped field has an id");
        };
        i32::try_from(*id).unwrap()
    });
    assert_eq!(*id, expected_id);
    let expected: Vec<_> = (1..=6).map(|offset| id.map(|id| id + offset)).collect();
    assert_eq!(sub_field_ids, &expected);

    let batches = read_scan(&snapshot.scan_builder().build()?, engine)?;
    assert_eq!(batches.len(), 1);
    let read = batches[0].column_by_name("c").expect("the column is read");
    // Compare the `file` values: a nested struct field also carries column-mapping metadata.
    let read_files = match layout {
        "top_level" => read.as_struct().clone(),
        "in_struct" => read.as_struct().column(0).as_struct().clone(),
        "map_value" => read.as_map().values().as_struct().clone(),
        _ => read.as_list::<i32>().values().as_struct().clone(),
    };
    assert_eq!(read_files, file_values());
    Ok(())
}

/// The default writer rejects an invalid `file` value before writing a data file, wherever the
/// column appears.
#[rstest]
#[case::no_referent((None, None, None, None), "neither `uri` nor `inline` is set")]
#[case::offset_without_uri((None, Some(4), Some(10), Some(b"x".as_slice())), "`offset` is set without `uri`")]
#[case::offset_without_size((Some("s3://b/a"), Some(4), None, None), "`offset` is set without `size`")]
#[case::negative_size((Some("s3://b/a"), None, Some(-1), None), "`size` is negative")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_rejects_invalid_file_value(
    #[case] value: Value<'static>,
    #[case] problem: &str,
    #[values("top_level", "in_struct", "array_element", "map_value")] layout: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let files = file_array(&[Some((Some("s3://b/a"), None, None, None)), Some(value)]);
    let (data_type, column) = file_column(layout, files, None);
    let (_tmp_dir, table_path, _engine, result) = write_table("none", data_type, column).await?;

    let error = result
        .expect_err("an invalid `file` value is rejected")
        .to_string();
    assert!(error.contains(problem), "got: {error}");
    let data_files = walkdir::WalkDir::new(&table_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(data_files, 0, "no data file is written");
    Ok(())
}

/// The default writer rejects a `file` value whose `checksum` is malformed, writing no data file.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_rejects_malformed_checksum() -> Result<(), Box<dyn std::error::Error>> {
    let (fields, mut columns, nulls) =
        file_array(&[Some((Some("s3://b/a"), None, None, None))]).into_parts();
    let index = fields.iter().position(|f| f.name() == "checksum").unwrap();
    columns[index] = Arc::new(StringArray::from(vec![Some("MD5:xyz")]));
    let files = StructArray::try_new(fields, columns, nulls)?;
    let (data_type, column) = file_column("top_level", files, None);
    let (_tmp_dir, table_path, _engine, result) = write_table("none", data_type, column).await?;

    let error = result
        .expect_err("a malformed checksum is rejected")
        .to_string();
    assert!(error.contains("`checksum` digest"), "got: {error}");
    let data_files = walkdir::WalkDir::new(&table_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .count();
    assert_eq!(data_files, 0, "no data file is written");
    Ok(())
}

/// A `file` value under a null struct is not validated, as on reads.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_ignores_file_value_under_null_struct() -> Result<(), Box<dyn std::error::Error>>
{
    let files = file_array(&[
        Some((Some("s3://b/a"), None, None, None)),
        Some((None, None, None, None)),
    ]);
    let struct_nulls = Some(NullBuffer::from(vec![true, false]));
    let (data_type, column) = file_column("in_struct", files, struct_nulls);
    let (_tmp_dir, _table_path, _engine, result) = write_table("none", data_type, column).await?;
    result?;
    Ok(())
}
