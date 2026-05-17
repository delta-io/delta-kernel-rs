//! Integration tests for parquet **root** column matching when the kernel logical schema attaches
//! parquet field IDs (`parquet.field.id` → Arrow `PARQUET:field_id` when lowering logical schemas).
//!
//! This mirrors Delta Kernel parquet-handler semantics for flat tables: match by native parquet
//! field ID first, then fall back to column name. Nested struct paths are **not** field-ID-resolved
//! here.

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use common::run_to_batches_with as run_scan;
use delta_kernel::arrow::array::{Int64Array, StringArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{
    ColumnMetadataKey, DataType as KernelDataType, MetadataValue, StructField, StructType,
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
        DeclarativePlanNode::scan_parquet(vec![file_meta(&path)], kernel_schema),
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
        DeclarativePlanNode::scan_parquet(vec![file_meta(&path)], kernel_schema),
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
        DeclarativePlanNode::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap();

    assert_eq!(
        flatten_string_col(&batches, "plain_label"),
        vec!["alpha".to_string()]
    );
    assert_eq!(flatten_i64_col(&batches, "logical_metric"), vec![1000]);
}
