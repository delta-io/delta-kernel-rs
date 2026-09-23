//! Helpers for predicate pushdown and deletion-vector application during scan execution.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use roaring::RoaringTreemap;

use super::transform_spec::parse_partition_value_raw;
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{column_name, Expression, ExpressionStructPatchBuilder};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef};
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::{DeltaResult, Engine, EngineData, Error, ExpressionEvaluator, PredicateRef};

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
    Ok(Arc::new(binder.transform_pred(predicate)?.into_owned()))
}

/// Applies a file's DV using original Parquet positions, independent of pruning and batching.
pub(super) struct DeletionVectorFilter {
    pub(super) read_schema: SchemaRef,
    row_index_column: ColumnName,
    deleted: RoaringTreemap,
    remove_row_index: Option<Arc<dyn ExpressionEvaluator>>,
}

impl DeletionVectorFilter {
    /// Reuse an existing row-index column (including one needed for row tracking), or request
    /// an internal one and arrange to remove it before the physical-to-logical transform.
    pub(super) fn try_new(
        engine: &dyn Engine,
        physical_schema: &SchemaRef,
        table_physical_schema: &SchemaRef,
        deleted: RoaringTreemap,
    ) -> DeltaResult<Self> {
        let (read_schema, name, remove_row_index) = if let Some(field) =
            physical_schema.metadata_column(&MetadataColumnSpec::RowIndex)
        {
            (physical_schema.clone(), field.name().clone(), None)
        } else {
            // Include unprojected table columns to avoid shadowing predicate-only fields.
            let mut name = "row_index_for_deletion_vector".to_string();
            while physical_schema.field(&name).is_some()
                || table_physical_schema.field(&name).is_some()
            {
                name.push('_');
            }
            let schema =
                Arc::new(physical_schema.add_metadata_column(&name, MetadataColumnSpec::RowIndex)?);
            let drop_column =
                Expression::struct_patch(ExpressionStructPatchBuilder::new().drop(name.clone()))?;
            let evaluator = engine.evaluation_handler().new_expression_evaluator(
                schema.clone(),
                Arc::new(drop_column),
                physical_schema.as_ref().clone().into(),
            )?;
            (schema, name, Some(evaluator))
        };
        Ok(Self {
            read_schema,
            row_index_column: ColumnName::new([name]),
            deleted,
            remove_row_index,
        })
    }

    /// Filter a physical batch and restore its original schema, retaining original row indexes
    /// if they were already part of that schema. Invalid or missing indexes are errors.
    pub(super) fn apply(&self, data: Box<dyn EngineData>) -> DeltaResult<Box<dyn EngineData>> {
        let mut visitor = DeletionVectorVisitor {
            deleted: &self.deleted,
            selection: Vec::with_capacity(data.len()),
        };
        data.visit_rows(std::slice::from_ref(&self.row_index_column), &mut visitor)?;
        let data = data.apply_selection_vector(visitor.selection)?;
        match &self.remove_row_index {
            Some(evaluator) => evaluator.evaluate(data.as_ref()),
            None => Ok(data),
        }
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
            let index = u64::try_from(index)
                .map_err(|_| Error::generic("Parquet row indexes must be non-negative"))?;
            self.selection.push(!self.deleted.contains(index));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::Int64Array;
    use crate::arrow::record_batch::RecordBatch;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::schema::schema_ref;

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

    #[test]
    fn internal_row_index_does_not_shadow_unprojected_table_column() {
        let physical_schema = schema_ref! { nullable "id": LONG };
        let table_schema = schema_ref! {
            nullable "id": LONG,
            nullable "row_index_for_deletion_vector": LONG,
        };
        let filter = DeletionVectorFilter::try_new(
            &SyncEngine::new(),
            &physical_schema,
            &table_schema,
            RoaringTreemap::new(),
        )
        .unwrap();
        let index = filter
            .read_schema
            .metadata_column(&MetadataColumnSpec::RowIndex)
            .unwrap();
        assert!(table_schema.field(index.name()).is_none());
    }
}
