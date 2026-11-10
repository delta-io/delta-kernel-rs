//! Write-path integration tests for `file` columns.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Int32Array, Int64Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Int64Type, Schema as ArrowSchema,
};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::{FileGroup, file_groups};
use delta_kernel::expressions::{
    ColumnName, Predicate as Pred, Scalar, column_expr, column_name, expr,
};
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField, StructType,
};
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::table_changes::TableChanges;
use delta_kernel::transaction::create_table::{CreateTableTransaction, create_table};
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{Engine, KernelResult, resolve_file_uri};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::{
    add_commit, create_table_and_load_snapshot, read_add_infos, read_scan, test_table_setup,
    test_table_setup_mt, write_batch_to_table,
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

/// [`file_values`] with a valid `checksum` on the first value, so that statistics would show a
/// `checksum` bound if one were collected.
fn file_values_with_checksum() -> StructArray {
    let (fields, mut columns, nulls) = file_values().into_parts();
    let index = fields.iter().position(|f| f.name() == "checksum").unwrap();
    columns[index] = Arc::new(StringArray::from(vec![Some("CRC32:0a1b2c3d"), None, None]));
    StructArray::try_new(fields, columns, nulls).unwrap()
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
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..column.len() as i32));
    write_columns(
        vec![
            (StructField::nullable("id", DataType::INTEGER), ids),
            (StructField::nullable("c", data_type), column),
        ],
        &[("delta.columnMapping.mode", cm_mode)],
    )
    .await
}

/// Creates a table with `columns` and `properties` and writes one batch holding their values.
async fn write_columns(
    columns: Vec<(StructField, ArrayRef)>,
    properties: &[(&str, &str)],
) -> Result<WrittenTable, Box<dyn std::error::Error>> {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    let schema = Arc::new(StructType::try_new(fields)?);
    let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), properties)?;
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), arrays)?;
    let result = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await;
    Ok((tmp_dir, table_path, engine, result))
}

/// Returns the statistics of the only data file of `snapshot`.
fn data_file_stats(
    snapshot: &SnapshotRef,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let add_files = read_add_infos(snapshot, engine)?;
    assert_eq!(add_files.len(), 1);
    Ok(add_files[0]
        .stats
        .clone()
        .expect("the data file has statistics"))
}

/// Returns the columns with an entry in `stats[kind]`, such as `minValues`, as sorted paths.
fn stats_columns(stats: &serde_json::Value, kind: &str) -> Vec<String> {
    fn visit(value: &serde_json::Value, path: &str, out: &mut Vec<String>) {
        match value.as_object() {
            Some(object) => {
                for (name, child) in object {
                    let child_path = match path {
                        "" => name.clone(),
                        _ => format!("{path}.{name}"),
                    };
                    visit(child, &child_path, out);
                }
            }
            None => out.push(path.to_string()),
        }
    }
    let mut out = Vec::new();
    if let Some(value) = stats.get(kind) {
        visit(value, "", &mut out);
    }
    out.sort();
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
    let FileGroup {
        id, sub_field_ids, ..
    } = &groups[0];
    let column = snapshot.schema().field("c").unwrap().clone();
    // The column is written under its physical name.
    let physical_name = match column.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    {
        Some(MetadataValue::String(name)) => name.clone(),
        _ => "c".to_string(),
    };
    assert!(
        root.get_fields()
            .iter()
            .any(|field| field.name() == physical_name)
    );
    // Under column mapping, a `file` struct field has its column mapping id. An array element or
    // map value has an id only under Iceberg compatibility, which does not allow `file` (see
    // `test_create_table_with_iceberg_compat_rejects_file_column`), so here it has none.
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

/// IcebergCompat V2 and V3 do not allow the `file` type, so creating a table with a `file` column
/// and either of them fails, wherever the column appears.
#[rstest]
fn test_create_table_with_iceberg_compat_rejects_file_column(
    #[values("top_level", "in_struct", "array_element", "map_value")] layout: &str,
    #[values("delta.enableIcebergCompatV2", "delta.enableIcebergCompatV3")] property: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (data_type, _) = file_column(layout, file_values(), None);
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("c", data_type),
    ])?);
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let error = create_table_and_load_snapshot(
        &table_path,
        schema,
        engine.as_ref(),
        &[(property, "true"), ("delta.columnMapping.mode", "name")],
    )
    .expect_err("IcebergCompat does not allow `file`")
    .to_string();
    assert!(
        error.contains("does not support type at column") && error.contains("file"),
        "got: {error}"
    );
    Ok(())
}

/// The sub-fields of a `file` column named `prefix`, sorted: all six for `nullCount`, or the four
/// with min/max statistics.
fn file_stats_columns(prefix: &str, null_count: bool) -> Vec<String> {
    let names: &[&str] = match null_count {
        true => &[
            "checksum",
            "content_type",
            "inline",
            "offset",
            "size",
            "uri",
        ],
        false => &["content_type", "offset", "size", "uri"],
    };
    names
        .iter()
        .map(|name| format!("{prefix}.{name}"))
        .collect()
}

/// A `file` column gets `nullCount` for all six sub-fields and `minValues`/`maxValues` for all but
/// `checksum` and `inline`, at top level or in a struct. In an array or a map it is part of a
/// single leaf that gets only `nullCount`, as for any array or map.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_collects_stats_for_file_column(
    #[values("top_level", "in_struct", "array_element", "map_value")] layout: &str,
    #[values("none", "id", "name")] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (data_type, column) = file_column(layout, file_values(), None);
    let (_tmp_dir, _table_path, engine, snapshot) = write_table(cm_mode, data_type, column).await?;
    let snapshot = snapshot?;
    let stats = data_file_stats(&snapshot, engine.as_ref())?;

    // Statistics are keyed by physical name; a `file` sub-field keeps its own name.
    let physical = |field: &StructField| match field
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    {
        Some(MetadataValue::String(name)) => name.clone(),
        _ => field.name().clone(),
    };
    let schema = snapshot.schema();
    let (id, c) = (schema.field("id").unwrap(), schema.field("c").unwrap());
    let file_path = match (layout, c.data_type()) {
        ("in_struct", DataType::Struct(inner)) => Some(format!(
            "{}.{}",
            physical(c),
            physical(inner.field("f").unwrap())
        )),
        ("top_level", _) => Some(physical(c)),
        _ => None,
    };
    let sorted = |mut columns: Vec<String>| {
        columns.sort();
        columns
    };
    let (min_max, null_count) = match &file_path {
        Some(path) => (
            sorted(
                [
                    vec![physical(id)],
                    file_stats_columns(path, false /* null_count */),
                ]
                .concat(),
            ),
            sorted(
                [
                    vec![physical(id)],
                    file_stats_columns(path, true /* null_count */),
                ]
                .concat(),
            ),
        ),
        None => (vec![physical(id)], sorted(vec![physical(c), physical(id)])),
    };

    assert_eq!(stats["numRecords"], 3);
    for kind in ["minValues", "maxValues"] {
        assert_eq!(stats_columns(&stats, kind), min_max, "{kind}: {stats}");
    }
    assert_eq!(stats_columns(&stats, "nullCount"), null_count, "{stats}");
    // The values of a top-level or nested `file` column's statistics.
    if let Some(path) = &file_path {
        let stat = |kind: &str, name: &str| {
            let pointer = format!("/{kind}/{}/{name}", path.replace('.', "/"));
            stats.pointer(&pointer).cloned()
        };
        assert_eq!(stat("minValues", "size"), Some(10.into()), "{stats}");
        assert_eq!(
            stat("maxValues", "uri"),
            Some("s3://bucket/a.png".into()),
            "{stats}"
        );
        assert_eq!(stat("nullCount", "checksum"), Some(3.into()), "{stats}");
    }
    Ok(())
}

/// A sub-field's `nullCount` also counts rows whose whole `file` value is null, even when the
/// sub-field's own array holds a value at that row, as the `file` RFC requires.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_null_count_counts_null_file_values() -> Result<(), Box<dyn std::error::Error>> {
    // The second value is null, but its sub-field arrays still hold values.
    let (fields, columns, _) = file_array(&[
        Some((Some("s3://b/a"), None, Some(10), None)),
        Some((Some("s3://b/x"), Some(1), Some(5), Some(b"q".as_slice()))),
        Some((None, None, None, Some(b"xyz".as_slice()))),
    ])
    .into_parts();
    let nulls = NullBuffer::from(vec![true, false, true]);
    let files = StructArray::try_new(fields, columns, Some(nulls))?;
    let (_tmp_dir, _table_path, engine, snapshot) =
        write_table("none", DataType::file_type(), Arc::new(files)).await?;
    let stats = data_file_stats(&snapshot?, engine.as_ref())?;

    // Rows without the sub-field plus the null `file` value.
    let null_count = &stats["nullCount"]["c"];
    for (name, expected) in [
        ("uri", 2),
        ("offset", 3),
        ("size", 2),
        ("content_type", 1),
        ("checksum", 3),
        ("inline", 2),
    ] {
        assert_eq!(null_count[name], expected, "{name}: {stats}");
    }
    // The bounds ignore the null `file` value's sub-field values.
    assert_eq!(stats["minValues"]["c"]["size"], 10, "{stats}");
    assert_eq!(stats["maxValues"]["c"]["uri"], "s3://b/a", "{stats}");
    Ok(())
}

/// Each `file` sub-field takes one slot against `delta.dataSkippingNumIndexedCols`, and
/// `delta.dataSkippingStatsColumns` selects a whole `file` column or single sub-fields.
#[rstest]
#[case::num_indexed_cols(("delta.dataSkippingNumIndexedCols", "2"), &["c.offset", "c.uri"], &["c.offset", "c.uri"])]
#[case::whole_column(
    ("delta.dataSkippingStatsColumns", "c,x"),
    &["c.content_type", "c.offset", "c.size", "c.uri", "x"],
    &["c.checksum", "c.content_type", "c.inline", "c.offset", "c.size", "c.uri", "x"],
)]
#[case::sub_fields(("delta.dataSkippingStatsColumns", "c.size,c.checksum"), &["c.size"], &["c.checksum", "c.size"])]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_file_column_with_stats_column_settings(
    #[case] property: (&str, &str),
    #[case] min_max: &[&str],
    #[case] null_count: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let ints = || Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let (_tmp_dir, _table_path, engine, snapshot) = write_columns(
        vec![
            (
                StructField::nullable("c", DataType::file_type()),
                Arc::new(file_values_with_checksum()) as ArrayRef,
            ),
            (StructField::nullable("id", DataType::INTEGER), ints()),
            (StructField::nullable("x", DataType::INTEGER), ints()),
        ],
        &[property],
    )
    .await?;
    let stats = data_file_stats(&snapshot?, engine.as_ref())?;

    assert_eq!(stats["numRecords"], 3);
    for kind in ["minValues", "maxValues"] {
        assert_eq!(stats_columns(&stats, kind), min_max, "{kind}: {stats}");
    }
    assert_eq!(stats_columns(&stats, "nullCount"), null_count, "{stats}");
    if null_count.contains(&"c.checksum") {
        assert_eq!(stats["nullCount"]["c"]["checksum"], 2, "{stats}");
    }
    Ok(())
}

/// The schema of the clustering tests: `id` and a `file` column `c`.
fn clustering_test_schema() -> Arc<StructType> {
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("c", DataType::file_type()),
    ]))
}

/// Builds the creation of a table at `table_path` clustered on `column`, which indexes statistics
/// for only one column.
fn create_clustered_table(
    table_path: &str,
    engine: &dyn Engine,
    cm_mode: &str,
    column: ColumnName,
) -> KernelResult<CreateTableTransaction> {
    create_table(table_path, clustering_test_schema(), "Test/1.0", true)
        .with_table_properties([
            ("delta.columnMapping.mode", cm_mode),
            ("delta.dataSkippingNumIndexedCols", "1"),
        ])
        .with_data_layout(DataLayout::Clustered {
            columns: vec![column],
        })
        .build(engine, Box::new(FileSystemCommitter::new()))
}

/// `checksum` and `inline` have no min/max statistics, so clustering on either is rejected.
#[rstest]
fn test_clustering_on_null_count_only_file_sub_field_is_rejected(
    #[values("c.checksum", "c.inline")] column: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let column = ColumnName::from_naive_str_split(column);
    let Err(error) = create_clustered_table(&table_path, engine.as_ref(), "none", column) else {
        panic!("clustering on a null-count-only sub-field should be rejected");
    };
    let error = error.to_string();
    assert!(error.contains("without min/max statistics"), "got: {error}");
    Ok(())
}

/// A table can be clustered on a `file` sub-field with min/max statistics, named in any case, and
/// its data files then carry statistics for it, even past `delta.dataSkippingNumIndexedCols`.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_clustering_on_file_sub_field(
    #[values("none", "id", "name")] cm_mode: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let _ = create_clustered_table(
        &table_path,
        engine.as_ref(),
        cm_mode,
        column_name!("c.Size"),
    )?
    .commit(engine.as_ref())?;
    let snapshot = Snapshot::builder_for(Url::from_directory_path(&table_path).unwrap())
        .build(engine.as_ref())?;
    let arrow_schema: ArrowSchema = clustering_test_schema().as_ref().try_into_arrow()?;
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(file_values()),
        ],
    )?;
    let snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    let stats = data_file_stats(&snapshot, engine.as_ref())?;
    let c = match snapshot
        .schema()
        .field("c")
        .unwrap()
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
    {
        Some(MetadataValue::String(name)) => name.clone(),
        _ => "c".to_string(),
    };
    assert_eq!(stats["minValues"][&c]["size"], 10, "{stats}");
    Ok(())
}

/// Returns the number of data files a scan of `snapshot` with `predicate` keeps, after data
/// skipping.
fn scan_file_count(
    snapshot: &SnapshotRef,
    engine: &DefaultEngine<TokioMultiThreadExecutor>,
    predicate: Pred,
) -> Result<usize, Box<dyn std::error::Error>> {
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(predicate))
        .build()?;
    let mut count = 0usize;
    for scan_metadata in scan.scan_metadata(engine)? {
        count = scan_metadata?.visit_scan_files(count, |count, _| *count += 1)?;
    }
    Ok(count)
}

/// Writes a table with columns `id` and `c` as two data files, the first from [`file_values`] and
/// the second with one file `s3://bucket/b.png` of size 1000, and checkpoints it with
/// `checkpoint` if given.
async fn write_file_skipping_table(
    properties: &[(&str, &str)],
    checkpoint: Option<CheckpointSpec>,
) -> Result<
    (
        tempfile::TempDir,
        Arc<DefaultEngine<TokioMultiThreadExecutor>>,
        SnapshotRef,
    ),
    Box<dyn std::error::Error>,
> {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..3));
    let (tmp_dir, _table_path, engine, snapshot) = write_columns(
        vec![
            (StructField::nullable("id", DataType::INTEGER), ids),
            (
                StructField::nullable("c", DataType::file_type()),
                Arc::new(file_values()),
            ),
        ],
        properties,
    )
    .await?;
    let snapshot = snapshot?;
    let files = file_array(&[Some((Some("s3://bucket/b.png"), None, Some(1000), None))]);
    let schema = StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("c", DataType::file_type()),
    ])?;
    let schema: ArrowSchema = (&schema).try_into_arrow()?;
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(vec![10])), Arc::new(files)],
    )?;
    let mut snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    if let Some(spec) = checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&spec))?;
        snapshot = Snapshot::builder_for(snapshot.table_root().clone()).build(engine.as_ref())?;
    }
    Ok((tmp_dir, engine, snapshot))
}

/// A predicate on a `file` sub-field also skips data files in an incremental scan, a parallel
/// scan, and a Change Data Feed scan. Only the second data file has a file larger than 100.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_predicate_on_file_sub_field_skips_files_in_other_scans(
    #[values("incremental", "parallel", "change_data_feed")] scan: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // An incremental scan and a Change Data Feed scan read commits. A parallel scan reads file
    // actions in its parallel phase only from a checkpoint's sidecars.
    let (properties, checkpoint) = match scan {
        "parallel" => (
            &[("delta.feature.v2Checkpoint", "supported")][..],
            Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(1),
            })),
        ),
        _ => (&[("delta.enableChangeDataFeed", "true")][..], None),
    };
    let (_tmp_dir, engine, snapshot) = write_file_skipping_table(properties, checkpoint).await?;
    let predicate = || Arc::new(Pred::from_expr(expr!(col("c.size") > 100i64)));
    let kept = match scan {
        "incremental" => {
            let listing = snapshot
                .incremental_scan_builder(0 /* base_version */)
                .with_predicate(predicate())
                .build(engine.as_ref())?
                .expect("the commits after version 0 are available")
                .into_listing()?;
            listing
                .add_files
                .iter()
                .map(|f| f.selection_vector().iter().filter(|s| **s).count())
                .sum()
        }
        "parallel" => {
            let scan = snapshot
                .scan_builder()
                .with_predicate(predicate())
                .build()?;
            let count = |count: &mut usize, _| *count += 1;
            let engine: Arc<dyn Engine> = engine.clone();
            let mut sequential = scan.parallel_scan_metadata(engine.clone())?;
            let mut kept = 0usize;
            for scan_metadata in sequential.by_ref() {
                kept = scan_metadata?.visit_scan_files(kept, count)?;
            }
            let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()?
            else {
                panic!("the checkpoint sidecars should be read in the parallel phase");
            };
            let state = Arc::from(state);
            for file in files {
                let parallel =
                    ParallelScanMetadata::try_new(engine.clone(), Arc::clone(&state), vec![file])?;
                for scan_metadata in parallel {
                    kept = scan_metadata?.visit_scan_files(kept, count)?;
                }
            }
            kept
        }
        _ => {
            // Change Data Feed scans return the rows of the data files they keep.
            let table_changes =
                TableChanges::try_new(snapshot.table_root().clone(), engine.as_ref(), 1, None)?;
            let scan = table_changes
                .into_scan_builder()
                .with_predicate(predicate())
                .build()?;
            let mut rows = 0;
            for data in scan.execute(engine.clone())? {
                rows += data?.len();
            }
            rows
        }
    };
    assert_eq!(kept, 1, "{scan}");
    Ok(())
}

/// A predicate on a `file` sub-field with min/max statistics skips data files, like one on any
/// column, with or without column mapping, and with statistics read from commits or from a
/// checkpoint's struct statistics. `inline` has only `nullCount`, which `IS NULL` predicates use;
/// predicates on `checksum` skip nothing. Of the two data files, only the second has the file
/// `s3://bucket/b.png` of size 1000, and only the first has an inline value.
///
/// (Predicates resolve only leaf columns, so a predicate on the whole `file` column, like one on a
/// whole struct, is rejected when the scan is built.)
#[rstest]
#[case::uri_eq(Pred::from_expr(expr!(col("c.uri") == "s3://bucket/b.png")), 1)]
#[case::uri_missing(Pred::from_expr(expr!(col("c.uri") == "s3://bucket/missing.png")), 0)]
#[case::size_gt(Pred::from_expr(expr!(col("c.size") > 100i64)), 1)]
#[case::size_too_large(Pred::from_expr(expr!(col("c.size") > 1_000_000i64)), 0)]
#[case::content_type(Pred::from_expr(expr!(col("c.content_type") == "text/plain")), 0)]
#[case::inline_not_null(Pred::from_expr(expr!(is_not_null(col("c.inline")))), 1)]
// `inline` has no min/max statistics, so a comparison skips only the file whose `inline` values
// are all null.
#[case::inline_eq(Pred::eq(column_expr!("c.inline"), Scalar::Binary(b"missing".to_vec())), 1)]
// Both files have only null checksums, but `checksum` predicates are not used to skip.
#[case::checksum_not_null(Pred::from_expr(expr!(is_not_null(col("c.checksum")))), 2)]
#[case::checksum_eq(Pred::from_expr(expr!(col("c.checksum") == "MD5:x")), 2)]
#[tokio::test(flavor = "multi_thread")]
async fn test_predicate_on_file_sub_field_skips_files(
    #[case] predicate: Pred,
    #[case] expected: usize,
    #[values("none", "id", "name")] cm_mode: &str,
    #[values(false /* checkpoint */, true /* checkpoint */)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let properties = [
        ("delta.columnMapping.mode", cm_mode),
        // A checkpoint then holds only struct statistics, so skipping must use them.
        ("delta.checkpoint.writeStatsAsStruct", "true"),
        ("delta.checkpoint.writeStatsAsJson", "false"),
    ];
    let checkpoint = checkpoint.then_some(CheckpointSpec::V1);
    let (_tmp_dir, engine, snapshot) = write_file_skipping_table(&properties, checkpoint).await?;
    let files = scan_file_count(&snapshot, engine.as_ref(), predicate.clone())?;
    assert_eq!(files, expected, "{predicate:?}");
    Ok(())
}

/// Kernel reads statistics for `file` sub-fields written by other engines, keyed by the `file`
/// column and the sub-field name, and skips data files with them. `a.parquet` and `b.parquet`
/// differ only in their `uri` and `size` (10 and 20).
#[rstest]
#[case::uri_eq(Pred::from_expr(expr!(col("c.uri") == "b.parquet")), 1)]
#[case::offset_gt(Pred::from_expr(expr!(col("c.offset") > 100i64)), 0)]
#[case::size_lt(Pred::from_expr(expr!(col("c.size") < 15i64)), 1)]
#[case::content_type(Pred::from_expr(expr!(col("c.content_type") == "text/plain")), 0)]
#[case::uri_is_null(Pred::from_expr(expr!(is_null(col("c.uri")))), 0)]
#[case::inline_not_null(Pred::from_expr(expr!(is_not_null(col("c.inline")))), 0)]
// `checksum` statistics say every value is null, but they are not used to skip.
#[case::checksum_not_null(Pred::from_expr(expr!(is_not_null(col("c.checksum")))), 2)]
#[tokio::test(flavor = "multi_thread")]
async fn test_predicate_on_file_sub_field_uses_written_stats(
    #[case] predicate: Pred,
    #[case] expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(StructType::try_new([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("c", DataType::file_type()),
    ])?);
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), &[])?;
    let add = |path: &str, size: i64| {
        let stats = serde_json::json!({
            "numRecords": 1,
            "minValues": {"id": 1, "c": {"uri": path, "offset": 0, "size": size, "content_type": "image/png"}},
            "maxValues": {"id": 1, "c": {"uri": path, "offset": 0, "size": size, "content_type": "image/png"}},
            "nullCount": {"id": 0, "c": {"uri": 0, "offset": 0, "size": 0, "content_type": 0, "checksum": 1, "inline": 1}},
        });
        serde_json::json!({"add": {
            "path": path, "size": 1024, "modificationTime": 1700000000000i64, "dataChange": true,
            "partitionValues": {}, "stats": stats.to_string(),
        }})
        .to_string()
    };
    let commit = [
        r#"{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE"}}"#.to_string(),
        add("a.parquet", 10),
        add("b.parquet", 20),
    ]
    .join("\n");
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let store: Arc<DynObjectStore> = Arc::new(LocalFileSystem::new());
    add_commit(table_url.as_str(), store.as_ref(), 1, commit).await?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let files = scan_file_count(&snapshot, engine.as_ref(), predicate.clone())?;
    assert_eq!(files, expected, "{predicate:?}");
    Ok(())
}

/// A connector reads the bytes a `file` value references by resolving its `uri` with
/// [`resolve_file_uri`]: a relative `uri` against the table root, an absolute one as-is.
#[tokio::test(flavor = "multi_thread")]
async fn test_read_bytes_at_resolved_file_uris() -> Result<(), Box<dyn std::error::Error>> {
    let external_dir = tempfile::tempdir()?;
    let external_path = external_dir.path().join("b.png");
    std::fs::write(&external_path, b"external bytes")?;
    let external_uri = Url::from_file_path(&external_path).unwrap().to_string();
    let files = file_array(&[
        Some((Some("_files/a.png"), Some(9), Some(5), None)),
        Some((Some(external_uri.as_str()), None, None, None)),
    ]);
    let (_tmp_dir, table_path, engine, snapshot) =
        write_table("none", DataType::file_type(), Arc::new(files)).await?;
    let snapshot = snapshot?;
    let files_dir = Path::new(&table_path).join("_files");
    std::fs::create_dir(&files_dir)?;
    std::fs::write(files_dir.join("a.png"), b"relative bytes")?;

    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let read = batches[0].column_by_name("c").unwrap().as_struct();
    let uris = read.column_by_name("uri").unwrap().as_string::<i32>();
    let offsets = read
        .column_by_name("offset")
        .unwrap()
        .as_primitive::<Int64Type>();
    let sizes = read
        .column_by_name("size")
        .unwrap()
        .as_primitive::<Int64Type>();
    let mut bytes = Vec::new();
    for row in 0..read.len() {
        let location = resolve_file_uri(scan.table_root(), uris.value(row))?;
        let range = (!offsets.is_null(row)).then(|| {
            let start = offsets.value(row) as u64;
            start..start + sizes.value(row) as u64
        });
        let mut read = engine
            .storage_handler()
            .read_files(vec![(location, range)])?;
        bytes.push(read.next().unwrap()?);
    }
    assert_eq!(bytes, [&b"bytes"[..], &b"external bytes"[..]]);
    Ok(())
}
