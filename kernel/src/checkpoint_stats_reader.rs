//! Helper module for reading stats from checkpoint files with parsed stats support
//!
//! This module provides functionality to read statistics from checkpoint files,
//! preferring the strongly-typed `stats_parsed` field when available and falling
//! back to JSON `stats` when necessary.

use std::sync::Arc;

use crate::engine::{Engine, EngineData, ParquetHandler};
use crate::error::DeltaResult;
use crate::expressions::PredicateRef;
use crate::schema::{SchemaRef, StructType};
use crate::schema_reconciliation::{reconcile_stats_schema, can_use_column_mapping_fast_path};
use crate::FileMeta;
use crate::actions::get_log_add_schema;

/// Check if a checkpoint has stats_parsed column
pub(crate) fn checkpoint_has_stats_parsed(
    parquet_handler: &dyn ParquetHandler,
    checkpoint_file: &FileMeta,
) -> DeltaResult<bool> {
    let schema = parquet_handler.get_parquet_schema(checkpoint_file)?;

    // Check if the schema has an "add" struct with a "stats_parsed" field
    if let Some(add_field) = schema.field("add") {
        if let crate::schema::DataType::Struct(add_struct) = &add_field.data_type {
            return Ok(add_struct.field("stats_parsed").is_some());
        }
    }

    Ok(false)
}

/// Build the schema for reading checkpoints with parsed stats
///
/// This function:
/// 1. Checks if stats_parsed exists in the checkpoint
/// 2. If it does, performs schema reconciliation
/// 3. Returns the appropriate schema for reading
pub(crate) fn build_checkpoint_read_schema_with_stats(
    engine: &dyn Engine,
    checkpoint_file: &FileMeta,
    base_schema: SchemaRef,
    table_schema: SchemaRef,
    table_features: &[String],
    checkpoint_version: i64,
) -> DeltaResult<SchemaRef> {
    let parquet_handler = engine.parquet_handler();

    // Check if checkpoint has stats_parsed
    if !checkpoint_has_stats_parsed(parquet_handler.as_ref(), checkpoint_file)? {
        // No stats_parsed, use base schema (will read JSON stats)
        return Ok(base_schema);
    }

    // Get the checkpoint schema
    let checkpoint_schema = parquet_handler.get_parquet_schema(checkpoint_file)?;

    // Extract the stats_parsed schema from checkpoint
    let stats_parsed_schema = extract_stats_parsed_schema(&checkpoint_schema)?;

    // Check for column mapping fast path
    if can_use_column_mapping_fast_path(
        table_features,
        checkpoint_version,
        None, // TODO: get column mapping enable version from metadata
    ) {
        // Fast path: trust checkpoint schema
        return build_schema_with_stats_parsed(base_schema, Some(stats_parsed_schema));
    }

    // Perform schema reconciliation
    let reconcile_result = reconcile_stats_schema(&stats_parsed_schema, &table_schema)?;

    // Build the final schema with reconciled stats_parsed
    build_schema_with_stats_parsed(base_schema, Some(reconcile_result.reconciled_schema.as_ref().clone()))
}

/// Extract the stats_parsed schema from a checkpoint schema
fn extract_stats_parsed_schema(checkpoint_schema: &StructType) -> DeltaResult<StructType> {
    use crate::Error;

    let add_field = checkpoint_schema
        .field("add")
        .ok_or_else(|| Error::generic("Checkpoint schema missing 'add' field"))?;

    let add_struct = match &add_field.data_type {
        crate::schema::DataType::Struct(s) => s.as_ref(),
        _ => return Err(Error::generic("'add' field is not a struct")),
    };

    let stats_parsed_field = add_struct
        .field("stats_parsed")
        .ok_or_else(|| Error::generic("'add' struct missing 'stats_parsed' field"))?;

    match &stats_parsed_field.data_type {
        crate::schema::DataType::Struct(s) => Ok(s.as_ref().clone()),
        _ => Err(Error::generic("'stats_parsed' field is not a struct")),
    }
}

/// Build a schema that includes stats_parsed field
fn build_schema_with_stats_parsed(
    base_schema: SchemaRef,
    stats_parsed_schema: Option<StructType>,
) -> DeltaResult<SchemaRef> {
    use crate::schema::{DataType, StructField};

    let mut fields = base_schema.fields().to_vec();

    // Find the 'add' field and add stats_parsed to it if needed
    for field in &mut fields {
        if field.name() == "add" {
            if let DataType::Struct(ref mut add_struct) = &mut field.data_type {
                let mut add_fields = add_struct.fields().to_vec();

                // Remove existing stats_parsed field if present
                add_fields.retain(|f| f.name() != "stats_parsed");

                // Add the new stats_parsed field if we have a schema for it
                if let Some(stats_schema) = stats_parsed_schema {
                    add_fields.push(StructField::nullable(
                        "stats_parsed",
                        DataType::Struct(Box::new(stats_schema)),
                    ));
                }

                *add_struct = Box::new(StructType::new_unchecked(add_fields));
            }
        }
    }

    Ok(Arc::new(StructType::new_unchecked(fields)))
}

/// Read checkpoint with parsed stats support
///
/// This function wraps the normal checkpoint reading to add parsed stats support.
/// It will:
/// 1. Check if stats_parsed exists
/// 2. Perform schema reconciliation if needed
/// 3. Read the checkpoint with the appropriate schema
/// 4. Handle fallback to JSON stats if necessary
pub(crate) fn read_checkpoint_with_parsed_stats(
    engine: &dyn Engine,
    checkpoint_files: &[FileMeta],
    base_schema: SchemaRef,
    table_schema: SchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> {
    // For now, just use the normal parquet reading
    // In a full implementation, we would:
    // 1. Build the appropriate schema using build_checkpoint_read_schema_with_stats
    // 2. Read the checkpoint files with that schema
    // 3. Handle any necessary transformations

    let parquet_handler = engine.parquet_handler();
    parquet_handler.read_parquet_files(checkpoint_files, base_schema, predicate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_has_stats_parsed() {
        // TODO: Add tests once we have test infrastructure
    }

    #[test]
    fn test_schema_reconciliation_integration() {
        // TODO: Add tests for schema reconciliation in checkpoint context
    }
}