//! Parquet adaptation for legacy checkpoints whose struct fields have weaker nullability than
//! the schema requested by kernel.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StructArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, internal_err, Result as DataFusionResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column as DFColumn;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapter, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;

/// Creates [`PhysicalExprAdapter`]s that reconcile a checkpoint's physical Parquet schema with
/// the schema requested by a kernel scan.
///
/// Each adapter delegates ordinary schema evolution to DataFusion and additionally validates
/// required action fields that legacy checkpoint schemas mark nullable.
#[derive(Debug)]
pub(crate) struct KernelParquetExprAdapterFactory;

impl PhysicalExprAdapterFactory for KernelParquetExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DataFusionResult<Arc<dyn PhysicalExprAdapter>> {
        let aligned_physical_schema = Arc::new(align_nested_nullability(
            &logical_file_schema,
            &physical_file_schema,
        ));
        let default_adapter = DefaultPhysicalExprAdapter::new(
            logical_file_schema,
            Arc::clone(&aligned_physical_schema),
        );
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

impl PhysicalExprAdapter for KernelParquetExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        // The default adapter can return a tree, such as a cast around a column. Rewrite each
        // column leaf after that adaptation without revisiting the wrapper we introduce.
        let adapted = self.default_adapter.rewrite(expr)?;
        adapted
            .transform_up(|expr| self.relabel_column(expr))
            .data()
    }
}

impl KernelParquetExprAdapter {
    fn relabel_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Transformed<Arc<dyn PhysicalExpr>>> {
        let Some(column): Option<&DFColumn> = expr.downcast_ref() else {
            return Ok(Transformed::no(expr));
        };
        let index = self.physical_file_schema.index_of(column.name())?;
        let physical_field = self.physical_file_schema.field(index);
        let aligned_field = self.aligned_physical_schema.field(index);
        if physical_field.data_type() == aligned_field.data_type() {
            return Ok(Transformed::no(expr));
        }

        let relabeled = RelabelNullabilityExpr {
            expr,
            field: Arc::new(aligned_field.clone()),
        };
        let relabeled: Arc<dyn PhysicalExpr> = Arc::new(relabeled);
        Ok(Transformed::yes(relabeled))
    }
}

/// Tightens nested physical fields that the requested schema declares non-nullable.
///
/// Top-level nullability remains physical because an Arrow array does not carry its enclosing
/// field descriptor. Nested fields are checked when their struct wrappers are rebuilt during
/// expression evaluation.
fn align_nested_nullability(logical_schema: &Schema, physical_schema: &Schema) -> Schema {
    let fields = align_fields(logical_schema.fields(), physical_schema.fields(), false);
    Schema::new_with_metadata(fields, physical_schema.metadata().clone())
}

fn align_data_type(logical_type: &DataType, physical_type: &DataType) -> DataType {
    match (logical_type, physical_type) {
        (DataType::Struct(logical_fields), DataType::Struct(physical_fields)) => {
            DataType::Struct(align_fields(logical_fields, physical_fields, true))
        }
        _ => physical_type.clone(),
    }
}

fn align_fields(
    logical_fields: &Fields,
    physical_fields: &Fields,
    tighten_nullability: bool,
) -> Fields {
    physical_fields
        .iter()
        .map(|physical_field| {
            let Some((_, logical_field)) = logical_fields.find(physical_field.name()) else {
                return Arc::clone(physical_field);
            };
            let field = physical_field
                .as_ref()
                .clone()
                .with_data_type(align_data_type(
                    logical_field.data_type(),
                    physical_field.data_type(),
                ));
            let field = if tighten_nullability {
                // Tightening is safe only after `StructArray::try_new` verifies that any child
                // nulls are masked by a null parent.
                field.with_nullable(physical_field.is_nullable() && logical_field.is_nullable())
            } else {
                field
            };
            Arc::new(field)
        })
        .collect()
}

#[derive(Debug)]
struct RelabelNullabilityExpr {
    expr: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl PartialEq for RelabelNullabilityExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.field == other.field
    }
}

impl Eq for RelabelNullabilityExpr {}

impl Hash for RelabelNullabilityExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.field.hash(state);
    }
}

impl std::fmt::Display for RelabelNullabilityExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "relabel_nullability({})", self.expr)
    }
}

impl PhysicalExpr for RelabelNullabilityExpr {
    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let ColumnarValue::Array(array) = self.expr.evaluate(batch)? else {
            return exec_err!("Parquet nullability relabeling requires an array value");
        };
        relabel_nested_nullability(&array, self.field.data_type()).map(ColumnarValue::Array)
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
                "RelabelNullabilityExpr expected one child, got {}",
                children.len()
            );
        };
        let relabeled = Self {
            expr: Arc::clone(expr),
            field: Arc::clone(&self.field),
        };
        Ok(Arc::new(relabeled))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "relabel_nullability(")?;
        self.expr.fmt_sql(f)?;
        write!(f, ")")
    }
}

fn relabel_nested_nullability(
    array: &ArrayRef,
    target_type: &DataType,
) -> DataFusionResult<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(Arc::clone(array));
    }

    let DataType::Struct(target_fields) = target_type else {
        return exec_err!(
            "Cannot relabel Parquet nullability from {} to {target_type}",
            array.data_type()
        );
    };
    let Some(source) = array.as_struct_opt() else {
        return exec_err!("Arrow struct downcast failed");
    };
    if source.num_columns() != target_fields.len() {
        return exec_err!(
            "Cannot relabel struct with {} fields as struct with {} fields",
            source.num_columns(),
            target_fields.len()
        );
    }

    let source_columns = source.columns();
    let columns: Vec<_> = source_columns
        .iter()
        .zip(target_fields)
        .map(|(column, field)| relabel_nested_nullability(column, field.data_type()))
        .try_collect()?;
    let relabeled = StructArray::try_new(target_fields.clone(), columns, source.nulls().cloned())?;
    Ok(Arc::new(relabeled))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn alignment_only_tightens_nested_fields() {
        let logical = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, false),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, false)],
                    true,
                ),
            ],
            false,
        )]);
        let physical = Schema::new(vec![Field::new_struct(
            "action",
            vec![
                Field::new("required", DataType::Int32, true),
                Field::new_struct(
                    "nested",
                    vec![Field::new("required", DataType::Int32, true)],
                    false,
                ),
            ],
            true,
        )]);

        let aligned = align_nested_nullability(&logical, &physical);

        assert!(aligned.field(0).is_nullable());
        let DataType::Struct(fields) = aligned.field(0).data_type() else {
            panic!("expected a struct")
        };
        assert!(!fields[0].is_nullable());
        assert!(!fields[1].is_nullable());
        let DataType::Struct(nested_fields) = fields[1].data_type() else {
            panic!("expected a nested struct")
        };
        assert!(!nested_fields[0].is_nullable());
    }

    #[test]
    fn relabeling_nested_required_child_checks_parent_validity() {
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
        let _ = relabel_nested_nullability(&masked, &target_type).unwrap();

        let unmasked = outer(nested(None));
        let error = relabel_nested_nullability(&unmasked, &target_type).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(r#"Found unmasked nulls for non-nullable StructArray field "child""#),
            "{error}"
        );
    }
}
