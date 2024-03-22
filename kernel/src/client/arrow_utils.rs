//! Some utilities for working with arrow data types

use std::sync::Arc;

use crate::{DeltaResult, Error};

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use itertools::Itertools;
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};

/// Get the indicies in `parquet_schema` of the specified columns in `requested_schema`
pub(crate) fn get_requested_indicies(
    requested_schema: &ArrowSchema,
    parquet_schema: &ArrowSchemaRef,
) -> DeltaResult<Vec<usize>> {
    let indicies = requested_schema
        .fields
        .iter()
        .map(|field| {
            // todo: handle nested (then use `leaves` not `roots` below in generate_mask)
            parquet_schema.index_of(field.name())
        })
        .try_collect()?;
    Ok(indicies)
}

/// Create a mask that will only select the specified indicies from the parquet. Currently we only
/// handle "root" level columns, and hence use `ProjectionMask::roots`, but will support leaf
/// selection in the future
pub(crate) fn generate_mask(
    requested_schema: &ArrowSchema,
    parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indicies: &[usize],
) -> Option<ProjectionMask> {
    if parquet_schema.fields.size() == requested_schema.fields.size() {
        // we assume that in get_requested_indicies we will have caught any column name mismatches,
        // so here we can just say that if we request the same # of columns as the parquet file
        // actually has, we don't need to mask anything out
        None
    } else {
        Some(ProjectionMask::roots(
            parquet_physical_schema,
            indicies.to_owned(),
        ))
    }
}

/// Reorder a RecordBatch to match `requested_schema`. This method takes `indicies` as computed by
/// [`get_requested_indicies`] as an optimization. If the indicies are in order, then we don't need
/// to do any re-ordering.
pub(crate) fn reorder_record_batch(
    requested_schema: Arc<ArrowSchema>,
    input_data: RecordBatch,
    indicies: &[usize],
) -> DeltaResult<RecordBatch> {
    if indicies.windows(2).all(|is| is[0] <= is[1]) {
        // indicies is already sorted, meaning we requested in the order that the columns were
        // stored in the parquet
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        let reordered_columns = requested_schema
            .fields
            .iter()
            .map(|field| {
                input_data
                    .column_by_name(field.name())
                    .cloned() // cheap clones of `Arc`s
                    .ok_or(Error::generic(
                        "request_schema had a field that didn't exist in final result. This is a bug",
                    ))
            })
            .try_collect()?;
        Ok(RecordBatch::try_new(requested_schema, reordered_columns)?)
    }
}
