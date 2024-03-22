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

pub(crate) fn generate_mask(
    requested_schema: &ArrowSchema,
    parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indicies: &[usize],
) -> ProjectionMask {
    if parquet_schema.fields.size() == requested_schema.fields.size() {
        ProjectionMask::all()
    } else {
        ProjectionMask::roots(parquet_physical_schema, indicies.to_owned())
    }
}

pub(crate) fn reorder_record_batch(
    requested_schema: Arc<ArrowSchema>,
    input_data: RecordBatch,
    indicies: &[usize],
) -> DeltaResult<RecordBatch> {
    if indicies.windows(2).all(|is| is[0] <= is[1]) {
        // indicies is already sorted, so we don't need to do anything more
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        let mut cols = Vec::with_capacity(input_data.num_columns());
        for field in requested_schema.fields.iter() {
            let col = input_data
                .column_by_name(field.name())
                .ok_or(Error::generic(
                    "request_schema had a field that didn't exist in final result. This is a bug",
                ))?;
            cols.push(col.clone());
        }
        let reordered = RecordBatch::try_new(requested_schema, cols)?;
        Ok(reordered)
    }
}
