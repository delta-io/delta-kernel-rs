//! Some utilities for working with arrow data types

use std::sync::Arc;

use crate::{schema::SchemaRef, DeltaResult, Error};

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use parquet::{arrow::ProjectionMask, schema::types::SchemaDescriptor};

/// Get the indicies in `parquet_schema` of the specified columns in `requested_schema`. This
/// returns a tuples of (mask_indicies: Vec<parquet_schema_index>, reorder_indicies:
/// Vec<requested_index>). `mask_indicies` is used for generating the mask for reading from the
/// parquet file, and simply contains an entry for each index we wish to select from the parquet
/// file set to the index of the requested column in the parquet. `reorder_indicies` is used for
/// re-ordering and will be the same size as `requested_schema`. Each index in `reorder_indicies`
/// represents a column that will be in the read parquet data at that index. The value stored in
/// `reorder_indicies` is the position that the column should appear in the final output. For
/// example, if `reorder_indicies` is `[2,0,1]`, then the re-ordering code should take the third
/// column in the raw-read parquet data, and move it to the first column in the final output, the
/// first column to the second, and the second to the third.
pub(crate) fn get_requested_indices(
    requested_schema: &SchemaRef,
    parquet_schema: &ArrowSchemaRef,
) -> DeltaResult<(Vec<usize>, Vec<usize>)> {
    let (mask_indicies, reorder_indicies): (Vec<usize>, Vec<usize>) = parquet_schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(parquet_index, field)| {
            requested_schema
                .index_of(field.name())
                .map(|index| (parquet_index, index))
        })
        .unzip();
    if mask_indicies.len() != requested_schema.fields.len() {
        return Err(Error::generic(
            "Didn't find all requested columns in parquet schema",
        ));
    }
    Ok((mask_indicies, reorder_indicies))
}

/// Create a mask that will only select the specified indicies from the parquet. Currently we only
/// handle "root" level columns, and hence use `ProjectionMask::roots`, but will support leaf
/// selection in the future. See issues #86 and #96 as well.
pub(crate) fn generate_mask(
    requested_schema: &SchemaRef,
    parquet_schema: &ArrowSchemaRef,
    parquet_physical_schema: &SchemaDescriptor,
    indicies: &[usize],
) -> Option<ProjectionMask> {
    if parquet_schema.fields.size() == requested_schema.fields.len() {
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

/// Reorder a RecordBatch to match `requested_ordering`. This method takes `mask_indicies` as
/// computed by [`get_requested_indicies`] as an optimization. If the indicies are in order, then we
/// don't need to do any re-ordering. Otherwise, for each non-zero value in `requested_ordering`,
/// the column at that index will be added in order to returned batch
pub(crate) fn reorder_record_batch(
    input_data: RecordBatch,
    requested_ordering: &[usize],
) -> DeltaResult<RecordBatch> {
    if requested_ordering.windows(2).all(|is| is[0] < is[1]) {
        // indicies is already sorted, meaning we requested in the order that the columns were
        // stored in the parquet
        Ok(input_data)
    } else {
        // requested an order different from the parquet, reorder
        let input_schema = input_data.schema();
        let mut fields = Vec::with_capacity(requested_ordering.len());
        let reordered_columns = requested_ordering
            .iter()
            .map(|index| {
                fields.push(input_schema.field(*index).clone());
                input_data.column(*index).clone() // cheap Arc clone
            })
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        Ok(RecordBatch::try_new(schema, reordered_columns)?)
    }
}
