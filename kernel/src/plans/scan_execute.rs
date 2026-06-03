//! Scan execution helpers for [`PlanExecutor`](super::PlanExecutor) implementations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::engine::arrow_conversion::TryIntoArrow as _;
use crate::engine::arrow_data::{extract_record_batch, ArrowEngineData};
use crate::expressions::{ColumnName, Scalar};
use crate::plans::ir::ScanFile;
use crate::plans::validate::validate_scan_files_schema;
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{
    DeltaResult, EngineData, FileDataReadResultIterator, FileMeta, JsonHandler, ParquetHandler,
    PredicateRef,
};

/// Resolves output column indices for `file_constant_columns` in `schema` field order.
pub(crate) fn file_constant_output_indices(
    schema: &StructType,
    file_constant_columns: &[ColumnName],
) -> DeltaResult<Vec<usize>> {
    file_constant_columns
        .iter()
        .map(|col| {
            let fields = schema.walk_column_fields(col)?;
            let leaf_name = fields
                .last()
                .ok_or_else(|| {
                    crate::Error::generic(format!(
                        "File-constant column '{col}' has an empty path"
                    ))
                })?
                .name();
            schema.index_of(leaf_name).ok_or_else(|| {
                crate::Error::schema(format!(
                    "File-constant column '{col}' not found in scan schema"
                ))
            })
        })
        .collect()
}

/// Schema passed to the format reader: output schema without file-constant fields.
fn read_schema_without_file_constants(
    physical_schema: &StructType,
    constant_output_indices: &[usize],
) -> SchemaRef {
    let constant_set: HashSet<usize> = constant_output_indices.iter().copied().collect();
    let fields: Vec<StructField> = physical_schema
        .fields()
        .enumerate()
        .filter(|(idx, _)| !constant_set.contains(idx))
        .map(|(_, field)| field.clone())
        .collect();
    Arc::new(StructType::new_unchecked(fields))
}

/// Reads Parquet scan files, broadcasting file-constant columns when configured.
pub(crate) fn execute_scan_parquet<P: ParquetHandler>(
    parquet: &P,
    files: Vec<ScanFile>,
    file_constant_columns: Vec<ColumnName>,
    physical_schema: SchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<FileDataReadResultIterator> {
    validate_scan_files_schema(&files, &file_constant_columns, physical_schema.clone())?;
    if file_constant_columns.is_empty() {
        let metas: Vec<FileMeta> = files.into_iter().map(|f| f.meta).collect();
        return parquet.read_parquet_files(&metas, physical_schema, predicate);
    }

    let constant_indices =
        file_constant_output_indices(physical_schema.as_ref(), &file_constant_columns)?;
    let read_schema =
        read_schema_without_file_constants(physical_schema.as_ref(), &constant_indices);
    let mut outer: Vec<DeltaResult<Box<dyn EngineData>>> = Vec::new();
    for file in files {
        let constants = file.file_constants;
        let iter =
            parquet.read_parquet_files(&[file.meta], read_schema.clone(), predicate.clone())?;
        for batch_result in iter {
            let batch = batch_result?;
            let batch = merge_file_constants(
                batch,
                physical_schema.as_ref(),
                &constants,
                &constant_indices,
            )?;
            outer.push(Ok(batch));
        }
    }
    Ok(Box::new(outer.into_iter()))
}

/// Reads JSON scan files, broadcasting file-constant columns when configured.
pub(crate) fn execute_scan_json<J: JsonHandler>(
    json: &J,
    files: Vec<ScanFile>,
    file_constant_columns: Vec<ColumnName>,
    physical_schema: SchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<FileDataReadResultIterator> {
    validate_scan_files_schema(&files, &file_constant_columns, physical_schema.clone())?;
    if file_constant_columns.is_empty() {
        let metas: Vec<FileMeta> = files.into_iter().map(|f| f.meta).collect();
        return json.read_json_files(&metas, physical_schema, predicate);
    }

    let constant_indices =
        file_constant_output_indices(physical_schema.as_ref(), &file_constant_columns)?;
    let read_schema =
        read_schema_without_file_constants(physical_schema.as_ref(), &constant_indices);
    let mut outer: Vec<DeltaResult<Box<dyn EngineData>>> = Vec::new();
    for file in files {
        let constants = file.file_constants;
        let iter = json.read_json_files(&[file.meta], read_schema.clone(), predicate.clone())?;
        for batch_result in iter {
            let batch = batch_result?;
            let batch = merge_file_constants(
                batch,
                physical_schema.as_ref(),
                &constants,
                &constant_indices,
            )?;
            outer.push(Ok(batch));
        }
    }
    Ok(Box::new(outer.into_iter()))
}

fn merge_file_constants(
    data: Box<dyn EngineData>,
    physical_schema: &StructType,
    constants: &[Scalar],
    constant_output_indices: &[usize],
) -> DeltaResult<Box<dyn EngineData>> {
    let read_data = ArrowEngineData::try_from_engine_data(data)?;
    let read_batch = extract_record_batch(read_data.as_ref())?;
    let num_rows = read_batch.num_rows();
    let constant_map: HashMap<usize, &Scalar> = constant_output_indices
        .iter()
        .zip(constants)
        .map(|(&idx, scalar)| (idx, scalar))
        .collect();

    let full_arrow_schema = Arc::new(physical_schema.try_into_arrow()?);
    let mut read_col = 0;
    let mut columns = Vec::with_capacity(physical_schema.fields().len());
    for (out_idx, _) in physical_schema.fields().enumerate() {
        if let Some(scalar) = constant_map.get(&out_idx) {
            columns.push(scalar.to_array(num_rows)?);
        } else {
            columns.push(read_batch.column(read_col).clone());
            read_col += 1;
        }
    }
    let batch = crate::arrow::array::RecordBatch::try_new(full_arrow_schema, columns)?;
    Ok(Box::new(ArrowEngineData::new(batch)))
}
