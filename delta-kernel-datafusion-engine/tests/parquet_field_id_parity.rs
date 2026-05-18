//! Integration tests for parquet column matching when the kernel logical schema attaches
//! parquet field IDs (`parquet.field.id` → Arrow `PARQUET:field_id` when lowering logical schemas).
//!
//! Mirrors Delta Kernel parquet-handler semantics: match by native parquet field ID first,
//! then fall back to column name. The `nested_struct_*` tests prove the same matching
//! reaches into nested struct/list/map paths via [`FieldIdPhysicalExprAdapterFactory`].

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use common::run_to_batches_with as run_scan;
use delta_kernel::arrow::array::{Int64Array, ListArray, StringArray, StructArray};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field, Fields, Schema as ArrowSchema,
};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::plans::ir::PlanBuilder;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType as KernelDataType, MetadataValue, StructField,
    StructType,
};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use test_utils::parquet::file_meta;

fn parquet_field_id_meta(id: i64) -> HashMap<String, String> {
    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
}

fn write_parquet(path: &std::path::Path, schema: Arc<ArrowSchema>, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn flatten_string_col(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let idx = batches[0].schema().column_with_name(name).unwrap().0;
    batches
        .iter()
        .flat_map(|b| {
            b.column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or("").to_string())
        })
        .collect()
}

fn flatten_i64_col(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let idx = batches[0].schema().column_with_name(name).unwrap().0;
    batches
        .iter()
        .flat_map(|b| {
            b.column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

#[tokio::test]
async fn parquet_scan_matches_renamed_logical_columns_by_parquet_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("renamed.parquet");

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("legacy_id", ArrowDataType::Int64, false)
            .with_metadata(parquet_field_id_meta(1)),
        Field::new("legacy_name", ArrowDataType::Utf8, false)
            .with_metadata(parquet_field_id_meta(2)),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["p", "q"])),
        ],
    )
    .unwrap();
    write_parquet(&path, arrow_schema, batch);

    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("user_id", KernelDataType::LONG, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(1),
            )]),
            StructField::new("user_name", KernelDataType::STRING, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(2),
            )]),
        ])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = run_scan(
        &ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    assert_eq!(flatten_i64_col(&batches, "user_id"), vec![10, 20]);
    assert_eq!(
        flatten_string_col(&batches, "user_name"),
        vec!["p".to_string(), "q".to_string()]
    );
}

#[tokio::test]
async fn parquet_scan_orders_output_by_logical_schema_when_physical_column_order_differs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reordered.parquet");

    // Physical column order B then A (legacy names); logical scan lists A then B.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("phys_b", ArrowDataType::Utf8, false).with_metadata(parquet_field_id_meta(2)),
        Field::new("phys_a", ArrowDataType::Int64, false).with_metadata(parquet_field_id_meta(1)),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["right"])),
            Arc::new(Int64Array::from(vec![99])),
        ],
    )
    .unwrap();
    write_parquet(&path, arrow_schema, batch);

    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("logical_a", KernelDataType::LONG, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(1),
            )]),
            StructField::new("logical_b", KernelDataType::STRING, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(2),
            )]),
        ])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = run_scan(
        &ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    assert_eq!(batches[0].schema().field(0).name(), "logical_a");
    assert_eq!(batches[0].schema().field(1).name(), "logical_b");
    assert_eq!(flatten_i64_col(&batches, "logical_a"), vec![99]);
    assert_eq!(flatten_string_col(&batches, "logical_b"), vec!["right"]);
}

#[tokio::test]
async fn parquet_scan_mixes_field_id_match_with_name_fallback_per_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.parquet");

    // Physical order: renamed typed column first, plain column second (logical order will invert).
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("phys_metric", ArrowDataType::Int64, false)
            .with_metadata(parquet_field_id_meta(42)),
        Field::new("plain_label", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1000])),
            Arc::new(StringArray::from(vec!["alpha"])),
        ],
    )
    .unwrap();
    write_parquet(&path, arrow_schema, batch);

    // Only `logical_metric` carries a parquet field ID — triggers ID-first adaptation while
    // `plain_label` stays purely name-aligned (no IDs anywhere on that column).
    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("plain_label", KernelDataType::STRING, false),
            StructField::new("logical_metric", KernelDataType::LONG, false).with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(42),
            )]),
        ])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = run_scan(
        &ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    assert_eq!(
        flatten_string_col(&batches, "plain_label"),
        vec!["alpha".to_string()]
    );
    assert_eq!(flatten_i64_col(&batches, "logical_metric"), vec![1000]);
}

// --- Nested struct / list rename via field-id adapter --------------------------------
//
// These tests exercise the FieldIdPhysicalExprAdapterFactory end-to-end through the
// engine. They write parquet files whose nested fields use *physical* names, then scan
// against a kernel schema using *logical* names, linked by `parquet.field.id` at every
// nesting level. The emitted batches must have logical names at every level (and the
// declared StructArray inner field names must match the kernel-declared shape exactly).

#[tokio::test]
async fn parquet_scan_renames_nested_struct_fields_by_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested_struct.parquet");

    // Physical: Struct{phys_outer: Struct{phys_inner: Int64}} with field-ids 1, 2.
    let phys_inner_field = Arc::new(
        Field::new("phys_inner", ArrowDataType::Int64, false)
            .with_metadata(parquet_field_id_meta(2)),
    );
    let phys_outer_field = Field::new(
        "phys_outer",
        ArrowDataType::Struct(Fields::from(vec![phys_inner_field.clone()])),
        true,
    )
    .with_metadata(parquet_field_id_meta(1));
    let arrow_schema = Arc::new(ArrowSchema::new(vec![phys_outer_field]));

    let inner_values = Arc::new(Int64Array::from(vec![Some(7), Some(11), Some(13)]));
    let outer_struct = StructArray::new(
        Fields::from(vec![phys_inner_field]),
        vec![inner_values as _],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(outer_struct) as _],
    )
    .unwrap();
    write_parquet(&path, arrow_schema, batch);

    // Logical: Struct{logical_outer: Struct{logical_inner: Long}} with the same field-ids.
    let kernel_schema = Arc::new(
        StructType::try_new(vec![StructField::new(
            "logical_outer",
            KernelDataType::try_struct_type(vec![StructField::new(
                "logical_inner",
                KernelDataType::LONG,
                false,
            )
            .with_metadata([(
                ColumnMetadataKey::ParquetFieldId.as_ref(),
                MetadataValue::Number(2),
            )])])
            .unwrap(),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(1),
        )])])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = run_scan(
        &ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    // The emitted top-level field is the kernel-declared logical name.
    assert_eq!(batches[0].schema().field(0).name(), "logical_outer");
    // CRITICAL: the inner struct field must also carry the logical name. If the adapter
    // weren't doing nested rename, this would still be "phys_inner".
    let outer = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("outer must be StructArray");
    assert_eq!(
        outer.fields()[0].name(),
        "logical_inner",
        "nested struct inner field must be renamed by field-id"
    );
    let inner = outer
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("inner must be Int64Array");
    assert_eq!(inner.values(), &[7i64, 11, 13][..]);
}

#[tokio::test]
async fn parquet_scan_renames_list_of_struct_element_fields_by_field_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("list_of_struct.parquet");

    // Physical: List<Struct{phys_x: Int64}>; element=field-id 2, inner=field-id 3.
    let phys_inner_field = Arc::new(
        Field::new("phys_x", ArrowDataType::Int64, false)
            .with_metadata(parquet_field_id_meta(3)),
    );
    let phys_element_field = Arc::new(
        Field::new(
            "element",
            ArrowDataType::Struct(Fields::from(vec![phys_inner_field.clone()])),
            true,
        )
        .with_metadata(parquet_field_id_meta(2)),
    );
    let phys_root_field = Field::new(
        "phys_arr",
        ArrowDataType::List(Arc::clone(&phys_element_field)),
        true,
    )
    .with_metadata(parquet_field_id_meta(1));
    let arrow_schema = Arc::new(ArrowSchema::new(vec![phys_root_field]));

    // Two rows: [{x:1},{x:2}], [{x:3}]
    let inner = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let element_struct = StructArray::new(
        Fields::from(vec![phys_inner_field]),
        vec![inner as _],
        None,
    );
    let offsets = OffsetBuffer::<i32>::new(vec![0, 2, 3].into());
    let list = ListArray::new(
        phys_element_field,
        offsets,
        Arc::new(element_struct) as _,
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(list) as _],
    )
    .unwrap();
    write_parquet(&path, arrow_schema, batch);

    // Logical: List<Struct{logical_x: Long}> with matching field-ids.
    let kernel_schema = Arc::new(
        StructType::try_new(vec![StructField::new(
            "logical_arr",
            KernelDataType::Array(Box::new(ArrayType::new(
                KernelDataType::try_struct_type(vec![StructField::new(
                    "logical_x",
                    KernelDataType::LONG,
                    false,
                )
                .with_metadata([(
                    ColumnMetadataKey::ParquetFieldId.as_ref(),
                    MetadataValue::Number(3),
                )])])
                .unwrap(),
                true,
            ))),
            true,
        )
        .with_metadata([(
            ColumnMetadataKey::ParquetFieldId.as_ref(),
            MetadataValue::Number(1),
        )])])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = run_scan(
        &ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    assert_eq!(batches[0].schema().field(0).name(), "logical_arr");
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("must be ListArray");
    assert_eq!(list.offsets().as_ref(), &[0i32, 2, 3]);
    let element_struct = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("element must be StructArray");
    // CRITICAL: nested-in-list rename worked.
    assert_eq!(element_struct.fields()[0].name(), "logical_x");
    let xs = element_struct
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(xs.values(), &[1i64, 2, 3][..]);
}
