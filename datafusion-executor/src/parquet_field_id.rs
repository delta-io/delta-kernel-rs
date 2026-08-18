//! Parquet field-ID adaptation for DataFusion physical expressions.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::nested_struct::{cast_column, validate_data_type_compatibility};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{self, CastExpr, Column, Literal};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;

use crate::arrow_utils::{
    align_nested_nullability, relabel_arrow_array, resolve_parquet_field_ids,
};

/// Creates per-file expression adapters that resolve Parquet fields by ID before name.
#[derive(Debug, Clone, Default)]
pub(crate) struct ParquetFieldIdAdapterFactory;

impl PhysicalExprAdapterFactory for ParquetFieldIdAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DataFusionResult<Arc<dyn PhysicalExprAdapter>> {
        let resolved_physical_schema =
            resolve_parquet_field_ids(&logical_file_schema, &physical_file_schema)?;
        let resolved_physical_schema = Arc::new(align_nested_nullability(
            &logical_file_schema,
            &resolved_physical_schema,
        ));
        Ok(Arc::new(ParquetFieldIdAdapter {
            logical_file_schema,
            physical_file_schema,
            resolved_physical_schema,
        }))
    }
}

#[derive(Debug)]
struct ParquetFieldIdAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    resolved_physical_schema: SchemaRef,
}

impl PhysicalExprAdapter for ParquetFieldIdAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        expr.transform_down(|expr| {
            if let Some(path) = get_field_path(expr.as_ref()) {
                let target_field = expr.return_field(&self.logical_file_schema)?;
                if let Some(rewritten) = self.rewrite_get_field(path, target_field)? {
                    return Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump));
                }
            }

            if let Some(column) = expr.downcast_ref::<Column>() {
                let rewritten = self.rewrite_column(column)?;
                return Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump));
            }

            Ok(Transformed::no(expr))
        })
        .data()
    }
}

impl ParquetFieldIdAdapter {
    fn rewrite_column(&self, column: &Column) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let logical_field = self
            .logical_file_schema
            .field_with_name(column.name())
            .map_err(DataFusionError::from)?;
        let Some(index) =
            unique_field_index(self.resolved_physical_schema.fields(), column.name())?
        else {
            return missing_field_expr(logical_field, logical_field);
        };

        let physical_field = self.physical_file_schema.field(index);
        let resolved_field = self.resolved_physical_schema.field(index);
        let physical_column: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(physical_field.name(), index));
        let expr = relabel_if_needed(physical_column, physical_field, resolved_field);
        cast_if_needed(expr, resolved_field, logical_field)
    }

    fn rewrite_get_field(
        &self,
        path: GetFieldPath<'_>,
        target_field: FieldRef,
    ) -> DataFusionResult<Option<Arc<dyn PhysicalExpr>>> {
        let logical_root = self
            .logical_file_schema
            .field_with_name(path.column.name())
            .map_err(DataFusionError::from)?;
        let Some(root_index) =
            unique_field_index(self.resolved_physical_schema.fields(), path.column.name())?
        else {
            return missing_field_expr(logical_root, &target_field).map(Some);
        };

        let mut logical_field = logical_root;
        let mut resolved_field = self.resolved_physical_schema.field(root_index);
        let mut physical_field = self.physical_file_schema.field(root_index);
        let mut physical_names = Vec::with_capacity(path.fields.len());

        for logical_name in path.fields {
            let DataType::Struct(logical_fields) = logical_field.data_type() else {
                return Ok(None);
            };
            let Some(logical_index) = unique_field_index(logical_fields, logical_name)? else {
                return Ok(None);
            };
            let next_logical_field = logical_fields[logical_index].as_ref();

            let DataType::Struct(resolved_fields) = resolved_field.data_type() else {
                return Ok(None);
            };
            let Some(resolved_index) = unique_field_index(resolved_fields, logical_name)? else {
                return missing_field_expr(next_logical_field, &target_field).map(Some);
            };
            let DataType::Struct(physical_fields) = physical_field.data_type() else {
                return Ok(None);
            };
            let Some(next_physical_field) = physical_fields.get(resolved_index) else {
                return Err(DataFusionError::Execution(format!(
                    "Resolved Parquet field `{logical_name}` has no physical field at index \
                     {resolved_index}"
                )));
            };

            physical_names.push(next_physical_field.name().clone());
            logical_field = next_logical_field;
            resolved_field = resolved_fields[resolved_index].as_ref();
            physical_field = next_physical_field;
        }

        let mut args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(physical_names.len() + 1);
        args.push(Arc::new(Column::new(
            self.physical_file_schema.field(root_index).name(),
            root_index,
        )));
        args.extend(
            physical_names
                .into_iter()
                .map(|name| expressions::lit(ScalarValue::Utf8(Some(name)))),
        );
        let get_field = ScalarFunctionExpr::try_new(
            Arc::new(path.function.fun().clone()),
            args,
            &self.physical_file_schema,
            Arc::new(path.function.config_options().clone()),
        )?;
        let physical_expr: Arc<dyn PhysicalExpr> = Arc::new(get_field);
        let source_field = physical_expr.return_field(&self.physical_file_schema)?;
        let resolved_field = Arc::new(
            resolved_field
                .clone()
                .with_nullable(source_field.is_nullable()),
        );
        let expr = relabel_if_needed(physical_expr, &source_field, &resolved_field);
        cast_if_needed(expr, &resolved_field, &target_field).map(Some)
    }
}

struct GetFieldPath<'a> {
    column: &'a Column,
    fields: Vec<&'a str>,
    function: &'a ScalarFunctionExpr,
}

fn get_field_path(expr: &dyn PhysicalExpr) -> Option<GetFieldPath<'_>> {
    let function = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr)?;
    let mut fields = Vec::new();
    let column = collect_get_field_path(expr, &mut fields)?;
    Some(GetFieldPath {
        column,
        fields,
        function,
    })
}

fn collect_get_field_path<'a>(
    expr: &'a dyn PhysicalExpr,
    fields: &mut Vec<&'a str>,
) -> Option<&'a Column> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(column);
    }
    let function = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr)?;
    let column = collect_get_field_path(function.args().first()?.as_ref(), fields)?;
    for field in function.args().iter().skip(1) {
        let literal = field.downcast_ref::<Literal>()?;
        fields.push(literal.value().try_as_str().flatten()?);
    }
    Some(column)
}

fn unique_field_index(fields: &[FieldRef], name: &str) -> DataFusionResult<Option<usize>> {
    let mut matches = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == name)
        .map(|(index, _)| index);
    let first = matches.next();
    if matches.next().is_some() {
        return Err(DataFusionError::Execution(format!(
            "Multiple physical Parquet fields resolve to requested field `{name}`"
        )));
    }
    Ok(first)
}

fn missing_field_expr(
    required_field: &Field,
    output_field: &Field,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    if !required_field.is_nullable() {
        return Err(DataFusionError::Execution(format!(
            "Non-nullable column '{}' is missing from the physical schema",
            required_field.name()
        )));
    }
    let value = ScalarValue::Null.cast_to(output_field.data_type())?;
    Ok(Arc::new(Literal::new_with_metadata(
        value,
        Some(FieldMetadata::from(output_field)),
    )))
}

fn relabel_if_needed(
    expr: Arc<dyn PhysicalExpr>,
    physical_field: &Field,
    resolved_field: &Field,
) -> Arc<dyn PhysicalExpr> {
    if physical_field.data_type() == resolved_field.data_type() {
        expr
    } else {
        Arc::new(RelabelFieldsExpr {
            expr,
            field: Arc::new(resolved_field.clone()),
        })
    }
}

fn cast_if_needed(
    expr: Arc<dyn PhysicalExpr>,
    source_field: &Field,
    target_field: &Field,
) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
    if source_field == target_field {
        return Ok(expr);
    }

    if matches!(
        (source_field.data_type(), target_field.data_type()),
        (DataType::Map(_, _), DataType::Map(_, _))
    ) {
        validate_map_cast(source_field.data_type(), target_field.data_type())?;
        return Ok(Arc::new(MapCastExpr {
            expr,
            field: Arc::new(target_field.clone()),
        }));
    }

    validate_data_type_compatibility(
        target_field.name(),
        source_field.data_type(),
        target_field.data_type(),
    )
    .map_err(|error| {
        DataFusionError::Execution(format!(
            "Cannot cast column '{}' from '{}' to '{}': {error}",
            target_field.name(),
            source_field.data_type(),
            target_field.data_type()
        ))
    })?;
    Ok(Arc::new(CastExpr::new_with_target_field(
        expr,
        Arc::new(target_field.clone()),
        None,
    )))
}

fn validate_map_cast(source_type: &DataType, target_type: &DataType) -> DataFusionResult<()> {
    let (_, source_ordered, source_key, source_value) = map_type_parts(source_type)?;
    let (_, target_ordered, target_key, target_value) = map_type_parts(target_type)?;
    if target_ordered && !source_ordered {
        return Err(DataFusionError::Execution(
            "Cannot cast an unsorted map to a sorted map".to_string(),
        ));
    }
    validate_map_child(source_key, target_key)?;
    validate_map_child(source_value, target_value)
}

fn validate_map_child(source: &Field, target: &Field) -> DataFusionResult<()> {
    if source.is_nullable() && !target.is_nullable() {
        return Err(DataFusionError::Execution(format!(
            "Cannot cast nullable map field '{}' to a non-nullable field",
            target.name()
        )));
    }
    validate_data_type_compatibility(target.name(), source.data_type(), target.data_type())
}

fn map_type_parts(
    data_type: &DataType,
) -> DataFusionResult<(&FieldRef, bool, &FieldRef, &FieldRef)> {
    let DataType::Map(entries, ordered) = data_type else {
        return Err(DataFusionError::Internal(format!(
            "Expected map type, got {data_type}"
        )));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(DataFusionError::Execution(
            "Map entries must be a struct".to_string(),
        ));
    };
    let [key, value] = fields.as_ref() else {
        return Err(DataFusionError::Execution(format!(
            "Map entries must contain exactly two fields, got {}",
            fields.len()
        )));
    };
    Ok((entries, *ordered, key, value))
}

#[derive(Debug)]
struct RelabelFieldsExpr {
    expr: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl PartialEq for RelabelFieldsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.field == other.field
    }
}

impl Eq for RelabelFieldsExpr {}

impl Hash for RelabelFieldsExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.field.hash(state);
    }
}

impl std::fmt::Display for RelabelFieldsExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "relabel_fields({})", self.expr)
    }
}

impl PhysicalExpr for RelabelFieldsExpr {
    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                relabel_arrow_array(&array, self.field.data_type()).map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Execution(
                "Parquet field relabeling requires an array value".to_string(),
            )),
        }
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
            return Err(DataFusionError::Internal(format!(
                "RelabelFieldsExpr expected one child, got {}",
                children.len()
            )));
        };
        Ok(Arc::new(Self {
            expr: Arc::clone(expr),
            field: Arc::clone(&self.field),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "relabel_fields(")?;
        self.expr.fmt_sql(f)?;
        write!(f, ")")
    }
}

#[derive(Debug)]
struct MapCastExpr {
    expr: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl PartialEq for MapCastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.field == other.field
    }
}

impl Eq for MapCastExpr {}

impl Hash for MapCastExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.field.hash(state);
    }
}

impl std::fmt::Display for MapCastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "map_cast({})", self.expr)
    }
}

impl PhysicalExpr for MapCastExpr {
    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                cast_map_array(&array, self.field.data_type()).map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::Execution(
                "Map casting requires an array value".to_string(),
            )),
        }
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
            return Err(DataFusionError::Internal(format!(
                "MapCastExpr expected one child, got {}",
                children.len()
            )));
        };
        Ok(Arc::new(Self {
            expr: Arc::clone(expr),
            field: Arc::clone(&self.field),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "map_cast(")?;
        self.expr.fmt_sql(f)?;
        write!(f, ")")
    }
}

fn cast_map_array(array: &ArrayRef, target_type: &DataType) -> DataFusionResult<ArrayRef> {
    let (target_entries, target_ordered, target_key, target_value) = map_type_parts(target_type)?;
    let source = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        DataFusionError::Execution(format!("Expected map array, got {}", array.data_type()))
    })?;
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let key = cast_column(source.entries().column(0), target_key.data_type(), &options)?;
    let value = cast_column(
        source.entries().column(1),
        target_value.data_type(),
        &options,
    )?;
    let DataType::Struct(target_fields) = target_entries.data_type() else {
        return Err(DataFusionError::Execution(
            "Map entries must be a struct".to_string(),
        ));
    };
    let entries = StructArray::try_new(
        target_fields.clone(),
        vec![key, value],
        source.entries().nulls().cloned(),
    )?;
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(target_entries),
        source.offsets().clone(),
        entries,
        source.nulls().cloned(),
        target_ordered,
    )?))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::functions::core::get_field;

    use super::*;

    fn field(name: &str, data_type: DataType, id: impl Into<String>) -> Field {
        Field::new(name, data_type, true)
            .with_metadata(HashMap::from([("PARQUET:field_id".to_string(), id.into())]))
    }

    fn nested_schema(parent: &str, child: &str) -> SchemaRef {
        let child = Arc::new(field(child, DataType::Int64, "2"));
        Arc::new(Schema::new(vec![field(
            parent,
            DataType::Struct(vec![child].into()),
            "1",
        )]))
    }

    #[test]
    fn nested_leaf_rewrite_stays_a_physical_get_field_path() {
        let logical = nested_schema("parent", "child");
        let physical = nested_schema("parent_physical", "child_physical");
        let logical_expr: Arc<dyn PhysicalExpr> = Arc::new(
            ScalarFunctionExpr::try_new(
                get_field(),
                vec![
                    Arc::new(Column::new("parent", 0)),
                    expressions::lit(ScalarValue::Utf8(Some("child".to_string()))),
                ],
                &logical,
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        );
        let adapter = ParquetFieldIdAdapterFactory
            .create(logical, physical)
            .unwrap();

        let rewritten = adapter.rewrite(logical_expr).unwrap();
        let function =
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(rewritten.as_ref()).unwrap();
        let column = function.args()[0].downcast_ref::<Column>().unwrap();
        let field_name = function.args()[1].downcast_ref::<Literal>().unwrap();
        assert_eq!(column.name(), "parent_physical");
        assert_eq!(
            field_name.value(),
            &ScalarValue::Utf8(Some("child_physical".to_string()))
        );
    }

    #[test]
    fn field_id_wins_over_conflicting_name_and_malformed_id_falls_back_to_name() {
        let requested = Schema::new(vec![
            field("by_name", DataType::Int64, "1"),
            field("by_id", DataType::Int64, "2"),
        ]);
        let conflicting = Schema::new(vec![field("by_name", DataType::Int64, "2")]);
        let resolved = resolve_parquet_field_ids(&requested, &conflicting).unwrap();
        assert_eq!(resolved.field(0).name(), "by_id");

        let malformed = Schema::new(vec![field("by_name", DataType::Int64, "not-an-id")]);
        let resolved = resolve_parquet_field_ids(&requested, &malformed).unwrap();
        assert_eq!(resolved.field(0).name(), "by_name");
    }

    #[test]
    fn duplicate_physical_ids_fail_closed() {
        let logical = Arc::new(Schema::new(vec![field("logical", DataType::Int64, "1")]));
        let physical = Arc::new(Schema::new(vec![
            field("first", DataType::Int64, "1"),
            field("second", DataType::Int64, "1"),
        ]));
        let adapter = ParquetFieldIdAdapterFactory
            .create(Arc::clone(&logical), physical)
            .unwrap();
        let error = adapter
            .rewrite(Arc::new(Column::new("logical", 0)))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("Multiple physical Parquet fields resolve to requested field `logical`"));
    }
}
