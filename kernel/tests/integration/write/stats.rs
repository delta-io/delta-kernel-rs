//! Integration tests verifying stats collection for complex-typed columns.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, BinaryArray, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{column_expr, ColumnName};
use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
use delta_kernel::table_features::{get_any_level_column_physical_name, ColumnMappingMode};
use delta_kernel::test_utils::{
    create_table_and_load_snapshot, read_actions_from_commit, test_table_setup,
    test_table_setup_mt, write_batch_to_table,
};
use delta_kernel::{Expression as Expr, Predicate as Pred, Snapshot};
use url::Url;

use crate::common::write_utils::set_table_properties;

/// Builds a RecordBatch with schema (id: long, tags: array<string>, props: map<string, long>,
/// v: variant). Each row gets one entry in tags, one entry in props, and a simple integer variant.
/// If `nulls` is provided, it sets the null buffer for tags, props, and variant columns.
fn complex_type_batch(schema: &StructType, ids: &[i64], nulls: Option<&[bool]>) -> RecordBatch {
    let arrow_schema: ArrowSchema = schema.try_into_arrow().unwrap();
    let arrow_schema = Arc::new(arrow_schema);
    let n = ids.len();

    let id_array = Int64Array::from(ids.to_vec());

    // tags: one string element per row
    let tag_values = StringArray::from((0..n).map(|i| format!("t{i}")).collect::<Vec<_>>());
    let tag_offsets = OffsetBuffer::new((0..=n).map(|i| i as i32).collect::<Vec<_>>().into());
    let list_field = match arrow_schema.field_with_name("tags").unwrap().data_type() {
        ArrowDataType::List(f) => f.clone(),
        other => panic!("expected List, got {other:?}"),
    };
    let tag_array = ListArray::new(
        list_field,
        tag_offsets,
        Arc::new(tag_values),
        nulls.map(NullBuffer::from),
    );

    // props: one {"k": id} entry per row
    let map_keys = StringArray::from(vec!["k"; n]);
    let map_vals = Int64Array::from(ids.to_vec());
    let (map_entries_field, map_sorted) =
        match arrow_schema.field_with_name("props").unwrap().data_type() {
            ArrowDataType::Map(f, sorted) => (f.clone(), *sorted),
            other => panic!("expected Map, got {other:?}"),
        };
    let entries_fields = match map_entries_field.data_type() {
        ArrowDataType::Struct(f) => f.clone(),
        other => panic!("expected Struct, got {other:?}"),
    };
    let entries = StructArray::new(
        entries_fields,
        vec![
            Arc::new(map_keys) as ArrayRef,
            Arc::new(map_vals) as ArrayRef,
        ],
        None,
    );
    let map_offsets = OffsetBuffer::new((0..=n).map(|i| i as i32).collect::<Vec<_>>().into());
    let map_array = MapArray::new(
        map_entries_field,
        map_offsets,
        entries,
        nulls.map(NullBuffer::from),
        map_sorted,
    );

    // v: simple integer variant per row (metadata=[0x01,0x00,0x00], value=[0x0C, low_byte])
    let variant_meta = BinaryArray::from(
        ids.iter()
            .map(|_| Some(&[0x01u8, 0x00, 0x00][..]))
            .collect::<Vec<_>>(),
    );
    let variant_val_data: Vec<[u8; 2]> = ids.iter().map(|&id| [0x0Cu8, id as u8]).collect();
    let variant_val = BinaryArray::from(
        variant_val_data
            .iter()
            .map(|v| Some(&v[..]))
            .collect::<Vec<_>>(),
    );
    let variant_fields = match arrow_schema.field_with_name("v").unwrap().data_type() {
        ArrowDataType::Struct(fields) => fields.clone(),
        other => panic!("expected Struct, got {other:?}"),
    };
    let variant_array = StructArray::new(
        variant_fields,
        vec![
            Arc::new(variant_meta) as ArrayRef,
            Arc::new(variant_val) as ArrayRef,
        ],
        nulls.map(NullBuffer::from),
    );

    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(tag_array) as ArrayRef,
            Arc::new(map_array) as ArrayRef,
            Arc::new(variant_array) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Verifies that writing a table with array, map, and variant columns produces nullCount
/// statistics for those columns while excluding them from minValues/maxValues. Then writes a
/// second file and scans with a predicate to verify data skipping works e2e. Parameterized
/// over column mapping mode and whether a checkpoint is taken before the scan.
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_stats_for_complex_type_columns(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Id,
        ColumnMappingMode::Name
    )]
    cm_mode: ColumnMappingMode,
    #[values(false, true)] use_checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable(
            "tags",
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
        ),
        StructField::nullable(
            "props",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::LONG,
                true,
            ))),
        ),
        StructField::nullable("v", DataType::unshredded_variant()),
    ])?);

    let mode_str = match cm_mode {
        ColumnMappingMode::None => "none",
        ColumnMappingMode::Id => "id",
        ColumnMappingMode::Name => "name",
    };
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let snapshot = create_table_and_load_snapshot(
        &table_path,
        schema.clone(),
        engine.as_ref(),
        &[("delta.columnMapping.mode", mode_str)],
    )?;
    let table_url = Url::from_directory_path(&table_path).unwrap();

    // Resolve physical column names for stats JSON verification
    let cm = snapshot
        .table_properties()
        .column_mapping_mode
        .unwrap_or(ColumnMappingMode::None);
    let physical_name = |logical: &str| -> String {
        get_any_level_column_physical_name(
            snapshot.schema().as_ref(),
            &ColumnName::new([logical]),
            cm,
        )
        .unwrap()
        .into_inner()
        .into_iter()
        .next()
        .unwrap()
    };
    let id_phys = physical_name("id");
    let tags_phys = physical_name("tags");
    let props_phys = physical_name("props");
    let v_phys = physical_name("v");

    // Batch 1: ids [1,2,3] with one null per complex column
    let batch1 = complex_type_batch(&schema, &[1, 2, 3], Some(&[true, false, true]));
    let _snapshot =
        write_batch_to_table(&snapshot, engine.as_ref(), batch1, HashMap::new()).await?;

    // Read the commit and verify stats use correct (physical) column names
    let add_actions = read_actions_from_commit(&table_url, 1, "add")?;
    assert_eq!(add_actions.len(), 1);

    let stats: serde_json::Value = serde_json::from_str(
        add_actions[0]
            .get("stats")
            .and_then(|s| s.as_str())
            .expect("add action should have stats"),
    )?;

    assert_eq!(stats["numRecords"], 3);

    // nullCount should be present for all columns including array, map, and variant
    assert_eq!(stats["nullCount"][&id_phys], 0);
    assert_eq!(stats["nullCount"][&tags_phys], 1);
    assert_eq!(stats["nullCount"][&props_phys], 1);
    assert_eq!(stats["nullCount"][&v_phys], 1);

    // minValues/maxValues should have id but NOT complex types
    assert!(stats["minValues"][&id_phys].is_number());
    assert!(stats["maxValues"][&id_phys].is_number());
    for col in [&tags_phys, &props_phys, &v_phys] {
        assert!(
            stats["minValues"].get(col).is_none(),
            "minValues should not contain {col}"
        );
        assert!(
            stats["maxValues"].get(col).is_none(),
            "maxValues should not contain {col}"
        );
    }

    // Batch 2: ids [10,11,12] with no nulls, written to a separate file
    let batch2 = complex_type_batch(&schema, &[10, 11, 12], None);
    let snapshot2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let snapshot2 =
        write_batch_to_table(&snapshot2, engine.as_ref(), batch2, HashMap::new()).await?;

    // Optionally checkpoint to verify stats survive the checkpoint round-trip
    let scan_snapshot = if use_checkpoint {
        snapshot2.checkpoint(engine.as_ref(), None)?;
        Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?
    } else {
        snapshot2
    };

    // Scan with predicate id > 5. File 1 has id in [1,3], file 2 has id in [10,12].
    // Data skipping should skip file 1 entirely and return only file 2's rows.
    let scan = scan_snapshot
        .scan_builder()
        .with_predicate(Arc::new(Pred::gt(column_expr!("id"), Expr::literal(5_i64))))
        .build()?;
    let batches: Vec<RecordBatch> = scan
        .execute(engine.clone())?
        .map(|r| {
            let data = r.unwrap();
            ArrowEngineData::try_from_engine_data(data)
                .unwrap()
                .record_batch()
                .clone()
        })
        .collect();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "predicate id > 5 should return only the second file's 3 rows"
    );

    let result_schema = batches[0].schema();
    let combined = delta_kernel::arrow::compute::concat_batches(&result_schema, &batches)?;
    let ids: Vec<i64> = combined
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .iter()
        .copied()
        .collect();
    assert_eq!(ids, vec![10, 11, 12]);

    Ok(())
}

/// Verifies that complex types in a nested schema count against `dataSkippingNumIndexedCols`.
/// Schema: (id: long, data: struct<name: string, tags: array<string>, props: map<string, long>>).
/// With numIndexedCols=3, the first 3 leaf columns (id, data.name, data.tags) get stats. The
/// 4th leaf (data.props) is excluded because the limit is reached.
#[tokio::test]
async fn test_write_stats_nested_complex_types_respect_column_limit(
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::LONG),
        StructField::nullable(
            "data",
            DataType::try_struct_type(vec![
                StructField::nullable("name", DataType::STRING),
                StructField::nullable(
                    "tags",
                    DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))),
                ),
                StructField::nullable(
                    "props",
                    DataType::Map(Box::new(MapType::new(
                        DataType::STRING,
                        DataType::LONG,
                        true,
                    ))),
                ),
            ])?,
        ),
    ])?);

    let (_tmp_dir, table_path, engine) = test_table_setup()?;
    let table_url = Url::from_directory_path(&table_path).unwrap();
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;
    let snapshot = set_table_properties(
        &table_path,
        &table_url,
        engine.as_ref(),
        snapshot.version(),
        &[("delta.dataSkippingNumIndexedCols", "3")],
    )?;

    // Build a batch with 3 rows
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
    let arrow_schema = Arc::new(arrow_schema);

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["a", "b", "c"]);

    // tags: [["x"], null, ["y"]]
    let data_field = arrow_schema.field_with_name("data").unwrap();
    let data_struct_fields = match data_field.data_type() {
        ArrowDataType::Struct(f) => f,
        other => panic!("expected Struct, got {other:?}"),
    };
    let tags_field = &data_struct_fields[1];
    let list_field = match tags_field.data_type() {
        ArrowDataType::List(f) => f.clone(),
        other => panic!("expected List, got {other:?}"),
    };
    let tag_values = StringArray::from(vec!["x", "y"]);
    let tag_offsets = OffsetBuffer::new(vec![0, 1, 1, 2].into());
    let tag_array = ListArray::new(
        list_field,
        tag_offsets,
        Arc::new(tag_values),
        Some(NullBuffer::from_iter([true, false, true])),
    );

    // props: [{"k": 1}, {"k": 2}, null]
    let props_field = &data_struct_fields[2];
    let (map_entries_field, map_sorted) = match props_field.data_type() {
        ArrowDataType::Map(f, sorted) => (f.clone(), *sorted),
        other => panic!("expected Map, got {other:?}"),
    };
    let entries_fields = match map_entries_field.data_type() {
        ArrowDataType::Struct(f) => f.clone(),
        other => panic!("expected Struct, got {other:?}"),
    };
    let map_keys = StringArray::from(vec!["k", "k"]);
    let map_vals = Int64Array::from(vec![1i64, 2]);
    let entries = StructArray::new(
        entries_fields,
        vec![
            Arc::new(map_keys) as ArrayRef,
            Arc::new(map_vals) as ArrayRef,
        ],
        None,
    );
    let map_offsets = OffsetBuffer::new(vec![0, 1, 2, 2].into());
    let map_array = MapArray::new(
        map_entries_field,
        map_offsets,
        entries,
        Some(NullBuffer::from_iter([true, true, false])),
        map_sorted,
    );

    // Assemble the data struct
    let data_array = StructArray::new(
        data_struct_fields.clone(),
        vec![
            Arc::new(name_array) as ArrayRef,
            Arc::new(tag_array) as ArrayRef,
            Arc::new(map_array) as ArrayRef,
        ],
        None,
    );

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(data_array) as ArrayRef,
        ],
    )?;

    let _snapshot = write_batch_to_table(&snapshot, engine.as_ref(), batch, HashMap::new()).await?;

    // Version 0=create, 1=set properties, 2=data write
    let add_actions = read_actions_from_commit(&table_url, 2, "add")?;
    let stats: serde_json::Value = serde_json::from_str(
        add_actions[0]
            .get("stats")
            .and_then(|s| s.as_str())
            .expect("add action should have stats"),
    )?;

    assert_eq!(stats["numRecords"], 3);

    // First 3 leaves: id, data.name, data.tags all get nullCount
    assert_eq!(stats["nullCount"]["id"], 0);
    assert_eq!(stats["nullCount"]["data"]["name"], 0);
    assert_eq!(stats["nullCount"]["data"]["tags"], 1);

    // 4th leaf data.props is excluded by the column limit
    assert!(
        stats["nullCount"]["data"].get("props").is_none(),
        "props should be excluded by numIndexedCols=3"
    );

    // id and data.name get min/max; data.tags does not (complex type)
    assert!(stats["minValues"]["id"].is_number());
    assert!(stats["minValues"]["data"]["name"].is_string());
    assert!(stats["minValues"]["data"].get("tags").is_none());

    Ok(())
}
