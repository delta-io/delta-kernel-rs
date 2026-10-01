//! Helpers for predicate pushdown and deletion-vector application during scan execution.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use roaring::RoaringTreemap;

use super::transform_spec::parse_partition_value_raw;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, Expression, ExpressionRef, ExpressionStructPatch};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef};
use crate::struct_patch::ExpressionFieldPatch;
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::{DeltaResult, EngineData, Error, PredicateRef};

/// Bind partition references to Add-action values, which are authoritative even when the
/// Parquet file contains materialized partition columns.
pub(super) fn bind_partition_values(
    predicate: &PredicateRef,
    partition_schema: Option<&SchemaRef>,
    partition_values: &HashMap<String, String>,
) -> DeltaResult<PredicateRef> {
    let Some(partition_schema) = partition_schema else {
        return Ok(predicate.clone());
    };
    let mut binder = BindPartitions {
        schema: partition_schema,
        values: partition_values,
    };
    Ok(match binder.transform_pred(predicate)? {
        Cow::Borrowed(_) => predicate.clone(),
        Cow::Owned(predicate) => Arc::new(predicate),
    })
}

/// Shared read setup for applying DVs using original Parquet positions.
pub(super) struct DeletionVectorFilter {
    pub(super) read_schema: SchemaRef,
    row_index_column: ColumnName,
    remove_row_index: Option<String>,
}

impl DeletionVectorFilter {
    /// Reuse an existing row-index column (including one needed for row tracking), or request
    /// an internal one to remove in the physical-to-logical transform.
    pub(super) fn try_new(
        physical_schema: &SchemaRef,
        table_physical_schema: &SchemaRef,
    ) -> DeltaResult<Self> {
        let (read_schema, name, remove_row_index) = if let Some(field) =
            physical_schema.metadata_column(&MetadataColumnSpec::RowIndex)
        {
            (physical_schema.clone(), field.name().clone(), None)
        } else {
            // Include unprojected table columns to avoid shadowing predicate-only fields.
            let mut name = "row_index_for_deletion_vector".to_string();
            while physical_schema
                .fields()
                .chain(table_physical_schema.fields())
                .any(|field| field.name().to_lowercase() == name)
            {
                name.push('_');
            }
            let schema =
                Arc::new(physical_schema.add_metadata_column(&name, MetadataColumnSpec::RowIndex)?);
            (schema, name.clone(), Some(name))
        };
        Ok(Self {
            read_schema,
            row_index_column: ColumnName::new([name]),
            remove_row_index,
        })
    }

    /// Filter a physical batch using the file's DV. Invalid or missing indexes are errors.
    pub(super) fn apply(
        &self,
        data: Box<dyn EngineData>,
        deleted: &RoaringTreemap,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let mut visitor = DeletionVectorVisitor {
            deleted,
            selection: Vec::with_capacity(data.len()),
        };
        data.visit_rows(std::slice::from_ref(&self.row_index_column), &mut visitor)?;
        data.apply_selection_vector(visitor.selection)
    }

    /// Include removal of the internal index in the file's logical transform. Scan transforms
    /// are top-level struct patches; any other shape is an internal error.
    pub(super) fn with_row_index_removed(
        &self,
        transform: Option<ExpressionRef>,
    ) -> DeltaResult<Option<ExpressionRef>> {
        let Some(name) = &self.remove_row_index else {
            return Ok(transform);
        };
        let mut patch = match transform.as_deref() {
            Some(Expression::StructPatch(patch)) if patch.input_path.is_none() => patch.clone(),
            None => ExpressionStructPatch::default(),
            _ => {
                return Err(Error::internal_error(
                    "Expected a top-level scan struct patch",
                ))
            }
        };
        if patch
            .field_patches
            .insert(name.clone(), ExpressionFieldPatch::default())
            .is_some()
        {
            return Err(Error::internal_error(
                "Internal row-index column overlaps scan transform",
            ));
        }
        Ok(Some(Arc::new(Expression::struct_patch(patch)?)))
    }
}

struct BindPartitions<'s> {
    schema: &'s SchemaRef,
    values: &'s HashMap<String, String>,
}

impl<'a> ExpressionTransform<'a> for BindPartitions<'_> {
    transform_output_type!(|'a, T| DeltaResult<Cow<'a, T>>);

    fn transform_expr(&mut self, expr: &'a Expression) -> DeltaResult<Cow<'a, Expression>> {
        if let Expression::Column(column) = expr {
            if let [name] = column.path() {
                if let Some(field) = self.schema.field(name) {
                    let value =
                        parse_partition_value_raw(self.values.get(name), field.data_type())?;
                    return Ok(Cow::Owned(value.into()));
                }
            }
        }
        self.recurse_into_expr(expr)
    }
}

struct DeletionVectorVisitor<'a> {
    deleted: &'a RoaringTreemap,
    selection: Vec<bool>,
}

impl RowVisitor for DeletionVectorVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("row_index")], vec![DataType::LONG]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        let [indexes] = getters else {
            return Err(Error::internal_error(
                "Expected one row-index column for DV application",
            ));
        };
        for row in 0..row_count {
            let index: i64 = indexes.get(row, "row_index")?;
            let index = u64::try_from(index).map_err(|_| {
                Error::internal_error(format!(
                    "ParquetHandler returned a negative row index: {index}"
                ))
            })?;
            self.selection.push(!self.deleted.contains(index));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{ArrayRef, Int64Array};
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{col, lit};
    use crate::schema::{schema_ref, StructField};
    use crate::Engine as _;

    #[rstest::rstest]
    fn unchanged_partition_binding_reuses_predicate(#[values(false, true)] partitioned: bool) {
        let predicate = Arc::new(col!("id").gt(lit(10i64)));
        let partition_schema = partitioned.then(|| schema_ref! { nullable "part": LONG });
        let bound =
            bind_partition_values(&predicate, partition_schema.as_ref(), &HashMap::new()).unwrap();
        assert!(Arc::ptr_eq(&bound, &predicate));
    }

    #[rstest::rstest]
    #[case::sparse(vec![Some(5), Some(8), Some(10)], Some(vec![false, true, false]))]
    #[case::empty(vec![], Some(vec![]))]
    #[case::negative(vec![Some(-1)], None)]
    #[case::null(vec![None], None)]
    fn deletion_vector_visitor_validates_original_indexes(
        #[case] indexes: Vec<Option<i64>>,
        #[case] expected: Option<Vec<bool>>,
    ) {
        let data = ArrowEngineData::new(
            RecordBatch::try_from_iter([("position", Arc::new(Int64Array::from(indexes)) as _)])
                .unwrap(),
        );
        let deleted = [5, 10].into_iter().collect();
        let mut visitor = DeletionVectorVisitor {
            deleted: &deleted,
            selection: vec![],
        };
        let result = data.visit_rows(&[column_name!("position")], &mut visitor);
        match expected {
            Some(expected) => {
                result.unwrap();
                assert_eq!(visitor.selection, expected);
            }
            None => assert!(result.is_err()),
        }
    }

    #[rstest::rstest]
    fn internal_row_index_does_not_shadow_table_column(
        #[values("row_index_for_deletion_vector", "ROW_INDEX_FOR_DELETION_VECTOR")] name: &str,
        #[values(false, true)] projected: bool,
    ) {
        let table_schema = schema_ref! {
            nullable "id": LONG,
            (StructField::nullable(name, DataType::LONG)),
        };
        let physical_schema = if projected {
            table_schema.clone()
        } else {
            schema_ref! { nullable "id": LONG }
        };
        let filter = DeletionVectorFilter::try_new(&physical_schema, &table_schema).unwrap();
        let index = filter
            .read_schema
            .metadata_column(&MetadataColumnSpec::RowIndex)
            .unwrap();
        assert!(table_schema
            .fields()
            .all(|field| { field.name().to_lowercase() != index.name().to_lowercase() }));
        let mut columns: Vec<(&str, ArrayRef)> = physical_schema
            .fields()
            .map(|field| {
                (
                    field.name().as_str(),
                    Arc::new(Int64Array::from(vec![42, 43])) as _,
                )
            })
            .collect();
        columns.push((index.name(), Arc::new(Int64Array::from(vec![0, 1]))));
        let data = ArrowEngineData::new(RecordBatch::try_from_iter(columns).unwrap());
        let deleted = [0].into_iter().collect();
        let filtered = filter.apply(Box::new(data), &deleted).unwrap();
        let transform = filter.with_row_index_removed(None).unwrap().unwrap();
        let filtered = SyncEngine::new()
            .evaluation_handler()
            .new_expression_evaluator(
                filter.read_schema.clone(),
                transform,
                physical_schema.as_ref().clone().into(),
            )
            .unwrap()
            .evaluate(filtered.as_ref())
            .unwrap();
        let filtered = filtered.try_into_record_batch().unwrap();
        assert_eq!(filtered.num_columns(), physical_schema.num_fields());
        if projected {
            let column = filtered
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(column.value(0), 43);
        }
    }
}
