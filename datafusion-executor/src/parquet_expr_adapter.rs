//! Parquet adaptation for legacy checkpoints whose nested fields have weaker nullability than the
//! schema requested by kernel.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray, MapArray, StructArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, internal_err, Result as DataFusionResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column as DFColumn;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapter, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;

/// Creates [`PhysicalExprAdapter`]s that reconcile a checkpoint's physical Parquet schema with
/// the schema requested by a kernel scan.
///
/// DataFusion calls this factory after reading each file's physical schema, since files in the
/// same scan may have different schemas.
///
/// Each adapter delegates ordinary schema evolution to DataFusion and additionally validates
/// required nested fields that legacy checkpoint schemas mark nullable.
#[derive(Debug)]
pub(crate) struct KernelParquetExprAdapterFactory;

impl PhysicalExprAdapterFactory for KernelParquetExprAdapterFactory {
    fn create(
        &self,
        kernel_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DataFusionResult<Arc<dyn PhysicalExprAdapter>> {
        let aligned_physical_schema = Arc::new(align_nested_nullability(
            &kernel_schema,
            &physical_file_schema,
        ));
        let default_adapter =
            DefaultPhysicalExprAdapter::new(kernel_schema, Arc::clone(&aligned_physical_schema));
        let adapter = KernelParquetExprAdapter {
            default_adapter,
            physical_file_schema,
            aligned_physical_schema,
        };
        Ok(Arc::new(adapter))
    }
}

#[derive(Debug)]
struct KernelParquetExprAdapter {
    default_adapter: DefaultPhysicalExprAdapter,
    physical_file_schema: SchemaRef,
    aligned_physical_schema: SchemaRef,
}

// DataFusion's default adapter handles ordinary schema evolution. It compares each Kernel field
// with the physical field and adds a cast when they differ. Consider this column:
//
//   physical: action?: struct<required?: i32>
//   aligned:  action?: struct<required: i32>
//   Kernel:   action?: struct<required: i64, added?: utf8>
//
// Given the raw physical schema, the default adapter rejects nullable `required?` -> required
// `required` during rewriting, without checking whether the array actually contains nulls. We
// instead give the default adapter the aligned schema before inserting reconciliation.
//
// Alignment changes only nested nullability; it leaves `required` as i32 and leaves `added`
// missing. The default adapter can then add a cast for the remaining conversion: widening i32 to
// i64 and filling `added` with nulls.
//
//   column(action) -> cast(column(action) to Kernel's action type)
//
// Passing the aligned schema does not change the actual Parquet array read by `column(action)`.
//
// After the default rewrite, our bottom-up pass replaces that column with reconciliation:
//
//   cast(column(action))
//     -> cast(reconcile_kernel_nullability(column(action)))
//
// At execution, the child expression runs first. Reconciliation rebuilds the physical array with
// the aligned type and verifies that `required` contains no unmasked nulls. The cast then performs
// the remaining conversion to Kernel's type. If nullability is the only difference, the default
// adapter adds no cast and the final expression is just reconciliation around the column.
impl PhysicalExprAdapter for KernelParquetExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let adapted = self.default_adapter.rewrite(expr)?;
        let reconcile_column = |expr: Arc<dyn PhysicalExpr>| {
            let Some(column): Option<&DFColumn> = expr.downcast_ref() else {
                return Ok(Transformed::no(expr));
            };
            let index = self.physical_file_schema.index_of(column.name())?;
            let physical_field = self.physical_file_schema.field(index);
            let aligned_field = self.aligned_physical_schema.field(index);
            if physical_field.data_type() == aligned_field.data_type() {
                return Ok(Transformed::no(expr));
            }

            let reconciled: Arc<dyn PhysicalExpr> = Arc::new(ReconcileKernelNullabilityExpr {
                expr,
                field: Arc::new(aligned_field.clone()),
            });
            Ok(Transformed::yes(reconciled))
        };
        adapted.transform_up(reconcile_column).data()
    }
}

/// Builds an intermediate physical schema with Kernel's required nested fields.
///
/// The intermediate schema starts from the physical schema and changes only nullable flags on
/// matched nested fields. Names, metadata, primitive types, absent fields, and top-level
/// nullability stay as declared by the file. DataFusion performs all other conversions to the
/// requested Kernel schema.
fn align_nested_nullability(kernel_schema: &Schema, physical_schema: &Schema) -> Schema {
    let mut aligned_fields = Vec::with_capacity(physical_schema.fields().len());
    for physical_field in physical_schema.fields() {
        let Some((_, kernel_field)) = kernel_schema.fields().find(physical_field.name()) else {
            aligned_fields.push(Arc::clone(physical_field));
            continue;
        };
        let data_type =
            align_nullable_data_types(kernel_field.data_type(), physical_field.data_type());
        aligned_fields.push(Arc::new(
            physical_field.as_ref().clone().with_data_type(data_type),
        ));
    }
    Schema::new_with_metadata(aligned_fields, physical_schema.metadata().clone())
}

fn align_nullable_data_types(kernel_type: &DataType, physical_type: &DataType) -> DataType {
    match (kernel_type, physical_type) {
        (DataType::Struct(kernel_fields), DataType::Struct(physical_fields)) => {
            let mut aligned_fields = Vec::with_capacity(physical_fields.len());
            for physical_field in physical_fields {
                let Some((_, kernel_field)) = kernel_fields.find(physical_field.name()) else {
                    aligned_fields.push(Arc::clone(physical_field));
                    continue;
                };
                let data_type =
                    align_nullable_data_types(kernel_field.data_type(), physical_field.data_type());
                let field = align_nullable_field(kernel_field, physical_field, data_type);
                aligned_fields.push(field);
            }
            DataType::Struct(aligned_fields.into())
        }
        (DataType::List(kernel_field), DataType::List(physical_field)) => {
            let data_type =
                align_nullable_data_types(kernel_field.data_type(), physical_field.data_type());
            let field = align_nullable_field(kernel_field, physical_field, data_type);
            DataType::List(field)
        }
        (DataType::Map(kernel_field, _), DataType::Map(physical_field, physical_keys_sorted)) => {
            let data_type =
                align_nullable_data_types(kernel_field.data_type(), physical_field.data_type());
            let field = align_nullable_field(kernel_field, physical_field, data_type);
            DataType::Map(field, *physical_keys_sorted)
        }
        // Only align nullability for matching container shapes. Preserve other type differences so
        // DataFusion can cast compatible types or reject incompatible ones.
        _ => physical_type.clone(),
    }
}

fn align_nullable_field(
    kernel_field: &FieldRef,
    physical_field: &FieldRef,
    data_type: DataType,
) -> FieldRef {
    // A field is required if either schema marks it required. This normally tightens a nullable
    // physical field to match Kernel, while also preserving requirements declared by the file.
    let is_nullable = physical_field.is_nullable() && kernel_field.is_nullable();
    let field = physical_field
        .as_ref()
        .clone()
        .with_data_type(data_type)
        .with_nullable(is_nullable);
    Arc::new(field)
}

/// Reconciles nested fields with Kernel's schema after verifying their values satisfy the tighter
/// nullability.
///
/// DataFusion's cast expression rejects nullable-to-non-nullable nested fields from schema
/// metadata alone. This expression defers that decision to Arrow's array constructors, which
/// validate the actual values.
#[derive(Debug, Eq)]
struct ReconcileKernelNullabilityExpr {
    expr: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl PartialEq for ReconcileKernelNullabilityExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.field == other.field
    }
}

impl Hash for ReconcileKernelNullabilityExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.field.hash(state);
    }
}

impl std::fmt::Display for ReconcileKernelNullabilityExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reconcile_kernel_nullability({})", self.expr)
    }
}

// DataFusion calls this implementation to evaluate the inserted reconciliation expr against each
// physical record batch.
impl PhysicalExpr for ReconcileKernelNullabilityExpr {
    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let ColumnarValue::Array(array) = self.expr.evaluate(batch)? else {
            return exec_err!("Kernel nullability reconciliation requires an array value");
        };
        reconcile_array_nullability(&array, self.field.data_type()).map(ColumnarValue::Array)
    }

    fn return_field(&self, _input_schema: &Schema) -> DataFusionResult<FieldRef> {
        Ok(Arc::clone(&self.field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let [expr] = children.as_slice() else {
            return internal_err!(
                "ReconcileKernelNullabilityExpr expected one child, got {}",
                children.len()
            );
        };
        let reconciled = Self {
            expr: Arc::clone(expr),
            field: Arc::clone(&self.field),
        };
        Ok(Arc::new(reconciled))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reconcile_kernel_nullability(")?;
        self.expr.fmt_sql(f)?;
        write!(f, ")")
    }
}

// Recursively rebuild nested Struct, List, and Map arrays. Their constructors verify that fields
// being marked non-nullable contain no unmasked nulls.
fn reconcile_array_nullability(
    array: &ArrayRef,
    target_type: &DataType,
) -> DataFusionResult<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(Arc::clone(array));
    }

    match target_type {
        DataType::Struct(target_fields) => {
            let Some(source) = array.as_struct_opt() else {
                return exec_err!("Arrow struct downcast failed");
            };
            if source.num_columns() != target_fields.len() {
                return exec_err!(
                    "Cannot reconcile struct with {} fields as struct with {} fields",
                    source.num_columns(),
                    target_fields.len()
                );
            }

            let mut columns = Vec::with_capacity(source.num_columns());
            for (column, field) in source.columns().iter().zip(target_fields) {
                columns.push(reconcile_array_nullability(column, field.data_type())?);
            }
            let reconciled =
                StructArray::try_new(target_fields.clone(), columns, source.nulls().cloned())?;
            Ok(Arc::new(reconciled))
        }
        DataType::List(target_field) => {
            let Some(source) = array.as_list_opt::<i32>() else {
                return exec_err!("Arrow list downcast failed");
            };
            let values = reconcile_array_nullability(source.values(), target_field.data_type())?;
            let reconciled = ListArray::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )?;
            Ok(Arc::new(reconciled))
        }
        DataType::Map(target_field, keys_sorted) => {
            let Some(source) = array.as_map_opt() else {
                return exec_err!("Arrow map downcast failed");
            };
            let entries: ArrayRef = Arc::new(source.entries().clone());
            let entries = reconcile_array_nullability(&entries, target_field.data_type())?;
            let Some(entries) = entries.as_struct_opt() else {
                return exec_err!("Arrow map entries downcast failed");
            };
            let reconciled = MapArray::try_new(
                Arc::clone(target_field),
                source.offsets().clone(),
                entries.clone(),
                source.nulls().cloned(),
                *keys_sorted,
            )?;
            Ok(Arc::new(reconciled))
        }
        _ => exec_err!(
            "Cannot reconcile Parquet nullability from {} to {target_type}",
            array.data_type()
        ),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::physical_expr::expressions::CastExpr;

    use super::*;

    // === Tests ===

    // A `?` after a field name marks that field nullable in the schema summaries below.

    // Physical:
    //   action?: struct<
    //     required?: i32,
    //     nested?: struct<required?: i32>,
    //     items?: list<item?: i32>,
    //     properties?: map<entries: struct<key: utf8, value?: i32>>
    //   >
    // Requested Kernel:
    //   action?: struct<
    //     required: i32,
    //     nested?: struct<required: i32>,
    //     items?: list<element: i32>,
    //     properties?: map<key_value: struct<key: utf8, value: i32>>
    //   >
    // Expected:
    //   action?: struct<
    //     required: i32,
    //     nested?: struct<required: i32>,
    //     items?: list<item: i32>,
    //     properties?: map<entries: struct<key: utf8, value: i32>>
    //   >
    #[test]
    fn alignment_tightens_nested_fields() {
        let kernel = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, false),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, false)],
                    true,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
                    true,
                ),
                Field::new("properties", map_data_type("key_value", false), true),
            ],
            true,
        )]);
        let physical = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, true),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, true)],
                    true,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                ),
                Field::new("properties", map_data_type("entries", true), true),
            ],
            true,
        )]);

        let aligned = align_nested_nullability(&kernel, &physical);

        let DataType::Struct(fields) = aligned.field(0).data_type() else {
            panic!("expected a struct")
        };
        assert!(!fields[0].is_nullable());
        let DataType::Struct(nested_fields) = fields[1].data_type() else {
            panic!("expected a nested struct")
        };
        assert!(!nested_fields[0].is_nullable());
        let DataType::List(element) = fields[2].data_type() else {
            panic!("expected a list")
        };
        assert_eq!(element.name(), "item");
        assert!(!element.is_nullable());
        let DataType::Map(entries, _) = fields[3].data_type() else {
            panic!("expected a map")
        };
        assert_eq!(entries.name(), "entries");
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("expected map entries")
        };
        assert!(!entry_fields.find("value").unwrap().1.is_nullable());
    }

    // Physical: struct<nested?: struct<child?: i32>>
    // Requested Kernel: struct<nested?: struct<child: i32>>
    // Expected: struct<nested?: struct<child: i32>>, or an error for an unmasked null child.
    #[test]
    fn reconciling_nested_required_child_checks_parent_validity() {
        let source_children: Fields = vec![Field::new("child", DataType::Int32, true)].into();
        let source_fields: Fields =
            vec![Field::new_struct("nested", source_children.clone(), true)].into();
        let target_children: Fields = vec![Field::new("child", DataType::Int32, false)].into();
        let target_type =
            DataType::Struct(vec![Field::new_struct("nested", target_children, true)].into());
        let nested = |nulls| -> ArrayRef {
            let child: ArrayRef = Arc::new(Int32Array::from(vec![None]));
            let nested = StructArray::try_new(source_children.clone(), vec![child], nulls).unwrap();
            Arc::new(nested)
        };
        let outer = |nested| -> ArrayRef {
            let outer = StructArray::try_new(source_fields.clone(), vec![nested], None).unwrap();
            Arc::new(outer)
        };

        let masked = outer(nested(Some(NullBuffer::from(vec![false]))));
        let _ = reconcile_array_nullability(&masked, &target_type).unwrap();

        let unmasked = outer(nested(None));
        let error = reconcile_array_nullability(&unmasked, &target_type).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(r#"Found unmasked nulls for non-nullable StructArray field "child""#),
            "{error}"
        );
    }

    // Physical: list<item?: i32>
    // Requested Kernel: list<item: i32>
    // Expected: list<item: i32>, or an error if an item is null.
    #[test]
    fn reconciling_required_list_elements_checks_values() {
        let list = |values: Vec<Option<i32>>| -> ArrayRef {
            let len = values.len() as i32;
            let values: ArrayRef = Arc::new(Int32Array::from(values));
            let field = Arc::new(Field::new("item", DataType::Int32, true));
            Arc::new(
                ListArray::try_new(field, OffsetBuffer::new(vec![0, len].into()), values, None)
                    .unwrap(),
            )
        };
        let target_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));

        let valid = list(vec![Some(1), Some(2)]);
        let reconciled = reconcile_array_nullability(&valid, &target_type).unwrap();
        assert_eq!(reconciled.data_type(), &target_type);

        let invalid = list(vec![Some(1), None]);
        let error = reconcile_array_nullability(&invalid, &target_type).unwrap_err();
        assert!(
            error.to_string().contains("cannot contain nulls"),
            "{error}"
        );
    }

    // Physical: map<entries: struct<key: utf8, value?: i32>, keys_sorted=false>
    // Requested Kernel: map<entries: struct<key: utf8, value: i32>, keys_sorted=false>
    // Expected:
    //   map<entries: struct<key: utf8, value: i32>, keys_sorted=false>,
    //   or an error if a value is null.
    #[test]
    fn reconciling_required_map_values_checks_values() {
        let map = |values: Vec<Option<i32>>| -> ArrayRef {
            let len = values.len();
            let fields: Fields = vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ]
            .into();
            let keys: ArrayRef = Arc::new(StringArray::from_iter_values(
                (0..len).map(|index| index.to_string()),
            ));
            let values: ArrayRef = Arc::new(Int32Array::from(values));
            let entries = StructArray::try_new(fields.clone(), vec![keys, values], None).unwrap();
            let entries_field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
            Arc::new(
                MapArray::try_new(
                    entries_field,
                    OffsetBuffer::new(vec![0, len as i32].into()),
                    entries,
                    None,
                    false,
                )
                .unwrap(),
            )
        };
        let target_type = map_data_type("entries", false);

        let valid = map(vec![Some(1), Some(2)]);
        let reconciled = reconcile_array_nullability(&valid, &target_type).unwrap();
        assert_eq!(reconciled.data_type(), &target_type);

        let invalid = map(vec![Some(1), None]);
        let error = reconcile_array_nullability(&invalid, &target_type).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(r#"Found unmasked nulls for non-nullable StructArray field "value""#),
            "{error}"
        );
    }

    // Physical: action?: struct<required?: i32>
    // Requested Kernel: action?: struct<required: i64, added?: utf8>
    // Expected: an error because reconcile(cast(column)) executes the cast first.
    #[test]
    fn reconciling_before_default_rewrite_places_cast_inside_reconciliation() {
        let (kernel_schema, physical_schema, batch) = schema_evolution_case();
        let aligned_schema = Arc::new(align_nested_nullability(&kernel_schema, &physical_schema));
        let column: Arc<dyn PhysicalExpr> = Arc::new(DFColumn::new("action", 0));
        let reconciled: Arc<dyn PhysicalExpr> = Arc::new(ReconcileKernelNullabilityExpr {
            expr: column,
            field: Arc::clone(&aligned_schema.fields()[0]),
        });

        let adapted =
            DefaultPhysicalExprAdapter::new(kernel_schema, aligned_schema).rewrite(reconciled);
        let adapted = adapted.unwrap();

        let reconciliation = adapted
            .downcast_ref::<ReconcileKernelNullabilityExpr>()
            .expect("reconciliation should remain outside the rewritten child");
        assert!(reconciliation.expr.downcast_ref::<CastExpr>().is_some());
        let error = adapted.evaluate(&batch).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Cannot cast nullable struct field 'required' to non-nullable field"),
            "{error}"
        );
    }

    // Physical: action?: struct<required?: i32>
    // Requested Kernel: action?: struct<required: i64, added?: utf8>
    // Expected: action?: struct<required: i64, added?: utf8>, with added filled with nulls.
    #[test]
    fn default_rewrite_preserves_type_casts_and_fills_missing_fields() {
        let (kernel_schema, physical_schema, batch) = schema_evolution_case();
        let adapter = KernelParquetExprAdapterFactory
            .create(Arc::clone(&kernel_schema), physical_schema)
            .unwrap();
        let column: Arc<dyn PhysicalExpr> = Arc::new(DFColumn::new("action", 0));

        let adapted = adapter.rewrite(column).unwrap();

        let cast = adapted
            .downcast_ref::<CastExpr>()
            .expect("default rewrite should add a cast");
        let reconciliation = cast
            .expr()
            .downcast_ref::<ReconcileKernelNullabilityExpr>()
            .expect("reconciliation should run before the cast");
        let ColumnarValue::Array(reconciled_only) = reconciliation.evaluate(&batch).unwrap() else {
            panic!("expected an array")
        };
        let reconciled_action = reconciled_only.as_struct();
        assert_eq!(reconciled_action.num_columns(), 1);
        assert!(reconciled_action.column(0).as_any().is::<Int32Array>());
        assert!(reconciled_action.column_by_name("added").is_none());

        let ColumnarValue::Array(array) = adapted.evaluate(&batch).unwrap() else {
            panic!("expected an array")
        };
        assert_eq!(array.data_type(), kernel_schema.field(0).data_type());
        let action = array.as_struct();
        let required = action
            .column_by_name("required")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(required.values().as_ref(), &[1, 2]);
        assert_eq!(action.column_by_name("added").unwrap().null_count(), 2);
    }

    fn schema_evolution_case() -> (SchemaRef, SchemaRef, RecordBatch) {
        let physical_fields: Fields = vec![Field::new("required", DataType::Int32, true)].into();
        let kernel_fields: Fields = vec![
            Field::new("required", DataType::Int64, false),
            Field::new("added", DataType::Utf8, true),
        ]
        .into();
        let physical_schema = Arc::new(Schema::new(vec![Field::new_struct(
            "action",
            physical_fields.clone(),
            true,
        )]));
        let kernel_schema = Arc::new(Schema::new(vec![Field::new_struct(
            "action",
            kernel_fields,
            true,
        )]));
        let required: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let action: ArrayRef =
            Arc::new(StructArray::try_new(physical_fields, vec![required], None).unwrap());
        let batch = RecordBatch::try_new(Arc::clone(&physical_schema), vec![action]).unwrap();
        (kernel_schema, physical_schema, batch)
    }

    fn map_data_type(entries_name: &str, value_nullable: bool) -> DataType {
        let fields: Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, value_nullable),
        ]
        .into();
        DataType::Map(
            Arc::new(Field::new(entries_name, DataType::Struct(fields), false)),
            false,
        )
    }
}
