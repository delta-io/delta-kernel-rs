//! Integration tests for parquet column matching when the kernel logical schema attaches
//! parquet field IDs (`parquet.field.id` → Arrow `PARQUET:field_id` when lowering logical schemas).
//!
//! Mirrors Delta Kernel parquet-handler semantics: match by native parquet field ID first,
//! then fall back to column name. The `nested_struct_*` tests prove the same matching
//! reaches into nested struct/list/map paths via [`FieldIdPhysicalExprAdapterFactory`].
//!
//! Assertions go through [`datafusion_common::assert_batches_sorted_eq!`] -- the pretty-printed
//! table reveals both the kernel-declared logical column NAMES (top-level + nested) and the
//! row values in one shot, so the adapter's rename + value-preservation contract is checked
//! end-to-end without bespoke `as_any().downcast_ref::<StructArray>()` chains.

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use common::run_to_batches_with as run_scan;
use datafusion_common::assert_batches_sorted_eq;
use delta_kernel::arrow::array::{Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use delta_kernel::arrow::buffer::OffsetBuffer;
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field, FieldRef, Fields, Schema as ArrowSchema,
};
use delta_kernel::plans::ir::PlanBuilder;
use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType as KernelDataType, MetadataValue, StructField,
    StructType,
};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use test_utils::parquet::file_meta;

fn arrow_id_meta(id: i64) -> HashMap<String, String> {
    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
}

fn kernel_id_meta(id: i64) -> Vec<(&'static str, MetadataValue)> {
    vec![(
        ColumnMetadataKey::ParquetFieldId.as_ref(),
        MetadataValue::Number(id),
    )]
}

/// Arrow `Field` carrying a `PARQUET:field_id`. Used for building physical schemas in tests.
fn aid(name: &str, ty: ArrowDataType, nullable: bool, id: i64) -> Field {
    Field::new(name, ty, nullable).with_metadata(arrow_id_meta(id))
}

/// Arrow `Field` of `Struct(inner_fields)` carrying a `PARQUET:field_id`. Used for the nested
/// physical schemas (the struct-wrap pattern repeats 2-3 times across the nested tests).
fn aid_struct(name: &str, inner_fields: Vec<FieldRef>, nullable: bool, id: i64) -> Field {
    aid(
        name,
        ArrowDataType::Struct(Fields::from(inner_fields)),
        nullable,
        id,
    )
}

/// Kernel `StructField` carrying a `parquet.field.id`. Used for building logical schemas
/// in tests.
fn kid(name: &str, ty: KernelDataType, nullable: bool, id: i64) -> StructField {
    StructField::new(name, ty, nullable).with_metadata(kernel_id_meta(id))
}

fn write_parquet(path: &std::path::Path, schema: Arc<ArrowSchema>, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Run a parquet scan plan through the executor, anchoring the file in a tempdir so the
/// pretty-printed `path` column never appears in assertions.
async fn scan_file(
    ex: &DataFusionExecutor,
    dir: &std::path::Path,
    file_name: &str,
    kernel_schema: Arc<StructType>,
) -> Vec<RecordBatch> {
    let path = dir.join(file_name);
    // Caller has already written the parquet file at `path`.
    run_scan(
        ex,
        PlanBuilder::scan_parquet(vec![file_meta(&path)], kernel_schema),
    )
    .await
    .unwrap()
}

/// Fixture for the three "flat" field-id parity scenarios. Each builds an Arrow file with a
/// physical schema, plus a kernel logical schema linked by `parquet.field.id`. The expected
/// pretty-printed table doubles as the assertion on (column names, column order, row values).
struct FlatFixture {
    file_name: &'static str,
    arrow_schema: Arc<ArrowSchema>,
    batch: RecordBatch,
    kernel_schema: Arc<StructType>,
    expected: &'static [&'static str],
}

fn renamed_logical_columns_fixture() -> FlatFixture {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        aid("legacy_id", ArrowDataType::Int64, false, 1),
        aid("legacy_name", ArrowDataType::Utf8, false, 2),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["p", "q"])),
        ],
    )
    .unwrap();
    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            kid("user_id", KernelDataType::LONG, false, 1),
            kid("user_name", KernelDataType::STRING, false, 2),
        ])
        .unwrap(),
    );
    FlatFixture {
        file_name: "renamed.parquet",
        arrow_schema,
        batch,
        kernel_schema,
        expected: &[
            "+---------+-----------+",
            "| user_id | user_name |",
            "+---------+-----------+",
            "| 10      | p         |",
            "| 20      | q         |",
            "+---------+-----------+",
        ],
    }
}

fn reordered_logical_columns_fixture() -> FlatFixture {
    // Physical column order B then A (legacy names); logical scan lists A then B.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        aid("phys_b", ArrowDataType::Utf8, false, 2),
        aid("phys_a", ArrowDataType::Int64, false, 1),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["right"])),
            Arc::new(Int64Array::from(vec![99])),
        ],
    )
    .unwrap();
    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            kid("logical_a", KernelDataType::LONG, false, 1),
            kid("logical_b", KernelDataType::STRING, false, 2),
        ])
        .unwrap(),
    );
    FlatFixture {
        file_name: "reordered.parquet",
        arrow_schema,
        batch,
        kernel_schema,
        expected: &[
            "+-----------+-----------+",
            "| logical_a | logical_b |",
            "+-----------+-----------+",
            "| 99        | right     |",
            "+-----------+-----------+",
        ],
    }
}

fn mixed_field_id_and_name_fallback_fixture() -> FlatFixture {
    // Physical order: renamed typed column first, plain column second (logical order inverts).
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        aid("phys_metric", ArrowDataType::Int64, false, 42),
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
    // Only `logical_metric` carries a parquet field ID -- triggers ID-first adaptation while
    // `plain_label` stays purely name-aligned (no IDs anywhere on that column).
    let kernel_schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("plain_label", KernelDataType::STRING, false),
            kid("logical_metric", KernelDataType::LONG, false, 42),
        ])
        .unwrap(),
    );
    FlatFixture {
        file_name: "mixed.parquet",
        arrow_schema,
        batch,
        kernel_schema,
        expected: &[
            "+-------------+----------------+",
            "| plain_label | logical_metric |",
            "+-------------+----------------+",
            "| alpha       | 1000           |",
            "+-------------+----------------+",
        ],
    }
}

#[rstest::rstest]
#[case::renamed_columns(renamed_logical_columns_fixture())]
#[case::reordered_columns(reordered_logical_columns_fixture())]
#[case::mixed_id_and_name_fallback(mixed_field_id_and_name_fallback_fixture())]
#[tokio::test]
async fn parquet_scan_with_field_ids(#[case] fx: FlatFixture) {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join(fx.file_name), fx.arrow_schema, fx.batch);

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = scan_file(&ex, dir.path(), fx.file_name, fx.kernel_schema).await;

    assert_batches_sorted_eq!(fx.expected, &batches);
}

// --- Nested struct / list rename via field-id adapter --------------------------------
//
// These tests exercise the FieldIdPhysicalExprAdapterFactory end-to-end through the engine.
// They write parquet files whose nested fields use *physical* names, then scan against a
// kernel schema using *logical* names, linked by `parquet.field.id` at every nesting level.
// The pretty-printed cells (`{logical_inner: 7}`, `[{logical_x: 1}, {logical_x: 2}]`)
// confirm both the rename at every level AND value preservation in one assertion.

#[tokio::test]
async fn parquet_scan_renames_nested_struct_fields_by_field_id() {
    let dir = tempfile::tempdir().unwrap();

    // Physical: Struct{phys_outer: Struct{phys_inner: Int64}} with field-ids 1, 2.
    let phys_inner_field = Arc::new(aid("phys_inner", ArrowDataType::Int64, false, 2));
    let phys_outer_field = aid_struct("phys_outer", vec![phys_inner_field.clone()], true, 1);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![phys_outer_field]));

    let inner_values = Arc::new(Int64Array::from(vec![Some(7), Some(11), Some(13)]));
    let outer_struct = StructArray::new(
        Fields::from(vec![phys_inner_field]),
        vec![inner_values as _],
        None,
    );
    let batch =
        RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(outer_struct) as _]).unwrap();
    write_parquet(
        &dir.path().join("nested_struct.parquet"),
        arrow_schema,
        batch,
    );

    // Logical: Struct{logical_outer: Struct{logical_inner: Long}} with the same field-ids.
    let kernel_schema = Arc::new(
        StructType::try_new(vec![kid(
            "logical_outer",
            KernelDataType::try_struct_type(vec![kid(
                "logical_inner",
                KernelDataType::LONG,
                false,
                2,
            )])
            .unwrap(),
            true,
            1,
        )])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = scan_file(&ex, dir.path(), "nested_struct.parquet", kernel_schema).await;

    // Pretty-printed cell shape `{logical_inner: N}` proves the nested rename: if the
    // adapter weren't doing nested rename, this would render as `{phys_inner: N}`.
    assert_batches_sorted_eq!(
        &[
            "+---------------------+",
            "| logical_outer       |",
            "+---------------------+",
            "| {logical_inner: 11} |",
            "| {logical_inner: 13} |",
            "| {logical_inner: 7}  |",
            "+---------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn parquet_scan_renames_list_of_struct_element_fields_by_field_id() {
    let dir = tempfile::tempdir().unwrap();

    // Physical: List<Struct{phys_x: Int64}>; element=field-id 2, inner=field-id 3.
    let phys_inner_field = Arc::new(aid("phys_x", ArrowDataType::Int64, false, 3));
    let phys_element_field = Arc::new(aid_struct(
        "element",
        vec![phys_inner_field.clone()],
        true,
        2,
    ));
    let phys_root_field = aid(
        "phys_arr",
        ArrowDataType::List(Arc::clone(&phys_element_field)),
        true,
        1,
    );
    let arrow_schema = Arc::new(ArrowSchema::new(vec![phys_root_field]));

    // Two rows: [{x:1},{x:2}], [{x:3}]
    let inner = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let element_struct =
        StructArray::new(Fields::from(vec![phys_inner_field]), vec![inner as _], None);
    let offsets = OffsetBuffer::<i32>::new(vec![0, 2, 3].into());
    let list = ListArray::new(
        phys_element_field,
        offsets,
        Arc::new(element_struct) as _,
        None,
    );
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(list) as _]).unwrap();
    write_parquet(
        &dir.path().join("list_of_struct.parquet"),
        arrow_schema,
        batch,
    );

    // Logical: List<Struct{logical_x: Long}> with matching field-ids.
    let kernel_schema = Arc::new(
        StructType::try_new(vec![kid(
            "logical_arr",
            KernelDataType::Array(Box::new(ArrayType::new(
                KernelDataType::try_struct_type(vec![kid(
                    "logical_x",
                    KernelDataType::LONG,
                    false,
                    3,
                )])
                .unwrap(),
                true,
            ))),
            true,
            1,
        )])
        .unwrap(),
    );

    let ex = DataFusionExecutor::try_new().unwrap();
    let batches = scan_file(&ex, dir.path(), "list_of_struct.parquet", kernel_schema).await;

    // Pretty-printed cell shape `[{logical_x: 1}, ...]` proves both the list-of-struct
    // rename and offset preservation in one assertion.
    assert_batches_sorted_eq!(
        &[
            "+----------------------------------+",
            "| logical_arr                      |",
            "+----------------------------------+",
            "| [{logical_x: 1}, {logical_x: 2}] |",
            "| [{logical_x: 3}]                 |",
            "+----------------------------------+",
        ],
        &batches
    );
}
