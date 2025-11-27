//! Schema reconciliation for parsed statistics.
//!
//! This module handles the comparison between checkpoint stats_parsed schema and
//! the expected table stats schema, determining which columns can be used for
//! data skipping and which should be nulled out due to schema incompatibility.

use std::collections::HashSet;
use std::sync::Arc;

use crate::schema::{DataType, PrimitiveType, SchemaRef, StructField, StructType, ToSchema};

/// Result of comparing checkpoint stats schema with table stats schema.
#[derive(Debug)]
pub(crate) struct StatsSchemaReconciliation {
    /// Schema to request from parquet reader (the compatible subset).
    pub(crate) read_schema: SchemaRef,
    /// Columns that are incompatible and should be nulled in the predicate.
    /// These are physical column names (from minValues/maxValues/nullCount).
    pub(crate) incompatible_columns: HashSet<String>,
    /// Whether stats_parsed is usable at all.
    pub(crate) can_use_parsed_stats: bool,
}

impl StatsSchemaReconciliation {
    /// Create a reconciliation result indicating stats_parsed cannot be used.
    pub(crate) fn not_usable() -> Self {
        Self {
            read_schema: Arc::new(StructType::new_unchecked([])),
            incompatible_columns: HashSet::new(),
            can_use_parsed_stats: false,
        }
    }
}

/// Check if a type widening is supported by Parquet.
///
/// Parquet readers can handle certain type widenings automatically:
/// - byte -> short -> int -> long
/// - float -> double
fn is_type_widening_supported(from: &PrimitiveType, to: &PrimitiveType) -> bool {
    use PrimitiveType::*;
    matches!(
        (from, to),
        (Byte, Short)
            | (Byte, Integer)
            | (Byte, Long)
            | (Short, Integer)
            | (Short, Long)
            | (Integer, Long)
            | (Float, Double)
    )
}

/// Check if two data types are compatible for stats reading.
///
/// Returns true if the checkpoint type can be read as the table type.
fn is_type_compatible(checkpoint_type: &DataType, table_type: &DataType) -> bool {
    match (checkpoint_type, table_type) {
        // Same type - always compatible
        (a, b) if a == b => true,

        // Primitive type widening
        (DataType::Primitive(from), DataType::Primitive(to)) => {
            is_type_widening_supported(from, to)
        }

        // Struct fields - recurse for nested stats
        (DataType::Struct(checkpoint_struct), DataType::Struct(table_struct)) => {
            // For stats, we check if table fields can be read from checkpoint
            // Fields missing in checkpoint will get NULL (handled separately)
            table_struct.fields().all(|table_field| {
                checkpoint_struct
                    .field(&table_field.name)
                    .map(|checkpoint_field| {
                        is_type_compatible(&checkpoint_field.data_type, &table_field.data_type)
                    })
                    .unwrap_or(true) // Missing field is OK (will be NULL)
            })
        }

        // Array types
        (DataType::Array(checkpoint_arr), DataType::Array(table_arr)) => {
            is_type_compatible(&checkpoint_arr.element_type, &table_arr.element_type)
        }

        // Map types
        (DataType::Map(checkpoint_map), DataType::Map(table_map)) => {
            is_type_compatible(&checkpoint_map.key_type, &table_map.key_type)
                && is_type_compatible(&checkpoint_map.value_type, &table_map.value_type)
        }

        // Everything else - incompatible
        _ => false,
    }
}

/// Find columns that are incompatible between checkpoint and table stats schemas.
///
/// A column is incompatible if:
/// 1. It exists in both schemas but with incompatible types (not just widening)
/// 2. It's in the table schema but missing in checkpoint schema
fn find_incompatible_columns(
    checkpoint_schema: &StructType,
    table_schema: &StructType,
) -> HashSet<String> {
    let mut incompatible = HashSet::new();

    for table_field in table_schema.fields() {
        match checkpoint_schema.field(&table_field.name) {
            Some(checkpoint_field) => {
                // Field exists in both - check type compatibility
                if !is_type_compatible(&checkpoint_field.data_type, &table_field.data_type) {
                    incompatible.insert(table_field.name.clone());
                }
            }
            None => {
                // Field missing in checkpoint - mark as incompatible
                // (will get NULL stats, can't be used for skipping)
                incompatible.insert(table_field.name.clone());
            }
        }
    }

    incompatible
}

/// Build a safe read schema for stats_parsed that includes only columns that are:
/// 1. Present in checkpoint (available)
/// 2. Needed by the predicate (wanted)
/// 3. Type-compatible (safe to read without errors)
///
/// Missing or incompatible columns are simply excluded from the read schema.
/// The parquet reader will return NULL for columns not requested, and data skipping
/// expressions will treat NULL stats as "unknown" (can't skip the file).
///
/// # Arguments
///
/// * `checkpoint_stats_schema` - The stats_parsed schema from the checkpoint parquet file
/// * `needed_stats_schema` - The stats schema built from predicate-referenced columns
///
/// # Returns
///
/// A `StructType` containing only the compatible subset of columns to read.
/// Returns None if stats_parsed cannot be used at all (e.g., missing numRecords).
pub(crate) fn build_safe_read_schema(
    checkpoint_stats_schema: &StructType,
    needed_stats_schema: &StructType,
) -> Option<StructType> {
    // numRecords is always needed and must exist
    let Some(checkpoint_num_records) = checkpoint_stats_schema.field("numRecords") else {
        return None;
    };

    // Build safe schemas for minValues, maxValues, nullCount
    let safe_min = build_safe_inner_schema(
        checkpoint_stats_schema.field("minValues"),
        needed_stats_schema.field("minValues"),
    );
    let safe_max = build_safe_inner_schema(
        checkpoint_stats_schema.field("maxValues"),
        needed_stats_schema.field("maxValues"),
    );
    let safe_null = build_safe_inner_schema(
        checkpoint_stats_schema.field("nullCount"),
        needed_stats_schema.field("nullCount"),
    );

    // Build the final stats schema with only compatible columns
    let mut fields = vec![StructField::nullable(
        "numRecords",
        checkpoint_num_records.data_type.clone(),
    )];

    if let Some(min_struct) = safe_min {
        fields.push(StructField::nullable(
            "minValues",
            DataType::Struct(Box::new(min_struct)),
        ));
    }
    if let Some(max_struct) = safe_max {
        fields.push(StructField::nullable(
            "maxValues",
            DataType::Struct(Box::new(max_struct)),
        ));
    }
    if let Some(null_struct) = safe_null {
        fields.push(StructField::nullable(
            "nullCount",
            DataType::Struct(Box::new(null_struct)),
        ));
    }

    Some(StructType::new_unchecked(fields))
}

/// Build a safe inner schema (e.g., for minValues) containing only compatible columns.
fn build_safe_inner_schema(
    checkpoint_field: Option<&StructField>,
    needed_field: Option<&StructField>,
) -> Option<StructType> {
    let checkpoint_field = checkpoint_field?;
    let needed_field = needed_field?;

    let checkpoint_struct = match &checkpoint_field.data_type {
        DataType::Struct(s) => s,
        _ => return None,
    };
    let needed_struct = match &needed_field.data_type {
        DataType::Struct(s) => s,
        _ => return None,
    };

    // For each needed column, check if it's available and compatible in checkpoint
    let safe_fields: Vec<StructField> = needed_struct
        .fields()
        .filter_map(|needed_col| {
            checkpoint_struct
                .field(&needed_col.name)
                .and_then(|checkpoint_col| {
                    if is_type_compatible(&checkpoint_col.data_type, &needed_col.data_type) {
                        // Use the checkpoint type (parquet reader expects it)
                        Some(StructField::nullable(
                            needed_col.name.clone(),
                            checkpoint_col.data_type.clone(),
                        ))
                    } else {
                        // Incompatible type - exclude from read schema
                        None
                    }
                })
        })
        .collect();

    if safe_fields.is_empty() {
        None
    } else {
        Some(StructType::new_unchecked(safe_fields))
    }
}

/// Compare checkpoint stats_parsed schema with expected table stats schema.
///
/// This function determines:
/// 1. Whether stats_parsed can be used at all
/// 2. Which schema to use for reading (compatible subset)
/// 3. Which columns should be nulled in the data skipping predicate
///
/// # Arguments
///
/// * `checkpoint_stats_schema` - The stats_parsed schema from the checkpoint parquet file
/// * `table_stats_schema` - The expected stats schema built from current table schema
///
/// # Returns
///
/// A `StatsSchemaReconciliation` containing the reconciled read schema,
/// incompatible columns, and whether stats_parsed is usable.
#[allow(dead_code)] // Keep for backward compatibility, may be removed later
pub(crate) fn reconcile_stats_schema(
    checkpoint_stats_schema: &StructType,
    table_stats_schema: &StructType,
) -> StatsSchemaReconciliation {
    // Check basic stats structure (numRecords, minValues, maxValues, nullCount)
    let Some(checkpoint_min) = checkpoint_stats_schema.field("minValues") else {
        return StatsSchemaReconciliation::not_usable();
    };
    let Some(checkpoint_max) = checkpoint_stats_schema.field("maxValues") else {
        return StatsSchemaReconciliation::not_usable();
    };
    let Some(checkpoint_null) = checkpoint_stats_schema.field("nullCount") else {
        return StatsSchemaReconciliation::not_usable();
    };

    let Some(table_min) = table_stats_schema.field("minValues") else {
        return StatsSchemaReconciliation::not_usable();
    };
    let Some(table_max) = table_stats_schema.field("maxValues") else {
        return StatsSchemaReconciliation::not_usable();
    };
    let Some(table_null) = table_stats_schema.field("nullCount") else {
        return StatsSchemaReconciliation::not_usable();
    };

    // Extract the inner schemas for minValues/maxValues/nullCount
    let (checkpoint_min_struct, checkpoint_max_struct, _checkpoint_null_struct) = match (
        &checkpoint_min.data_type,
        &checkpoint_max.data_type,
        &checkpoint_null.data_type,
    ) {
        (DataType::Struct(min), DataType::Struct(max), DataType::Struct(null)) => (min, max, null),
        _ => return StatsSchemaReconciliation::not_usable(),
    };

    let (table_min_struct, table_max_struct, _table_null_struct) = match (
        &table_min.data_type,
        &table_max.data_type,
        &table_null.data_type,
    ) {
        (DataType::Struct(min), DataType::Struct(max), DataType::Struct(null)) => (min, max, null),
        _ => return StatsSchemaReconciliation::not_usable(),
    };

    // Find incompatible columns from minValues and maxValues
    // (nullCount is always LONG, so type compatibility is less of an issue there)
    let mut incompatible_columns =
        find_incompatible_columns(checkpoint_min_struct, table_min_struct);
    incompatible_columns.extend(find_incompatible_columns(
        checkpoint_max_struct,
        table_max_struct,
    ));

    // For now, we use the checkpoint schema directly for reading
    // The Parquet reader will handle type widening and return NULL for missing columns
    let read_schema = Arc::new(checkpoint_stats_schema.clone());

    StatsSchemaReconciliation {
        read_schema,
        incompatible_columns,
        can_use_parsed_stats: true,
    }
}

/// Extract the stats_parsed schema from a checkpoint parquet schema.
///
/// Looks for `add.stats_parsed` field in the checkpoint schema.
pub(crate) fn extract_stats_parsed_schema(checkpoint_schema: &StructType) -> Option<&StructType> {
    let add_field = checkpoint_schema.field("add")?;
    let add_struct = match &add_field.data_type {
        DataType::Struct(s) => s,
        _ => return None,
    };
    let stats_parsed_field = add_struct.field("stats_parsed")?;
    match &stats_parsed_field.data_type {
        DataType::Struct(s) => Some(s),
        _ => None,
    }
}

/// Information about stats_parsed availability for a checkpoint.
///
/// This struct contains all the information needed to:
/// 1. Determine if stats_parsed should be used for data skipping
/// 2. Build the correct checkpoint read schema
///
/// Incompatible columns are handled by the safe read schema - they're simply not included,
/// so their stats will be NULL when accessed. The predicate evaluator handles NULL stats
/// correctly (treats them as unknown, so the file is not skipped).
#[derive(Debug, Clone)]
pub(crate) struct StatsParsedInfo {
    /// Whether to use stats_parsed for data skipping.
    pub(crate) use_stats_parsed: bool,
    /// The stats_parsed schema to include in the checkpoint read schema.
    /// Only contains columns that are both needed by the predicate AND compatible.
    /// Set only if use_stats_parsed is true.
    pub(crate) stats_parsed_schema: Option<SchemaRef>,
}

impl StatsParsedInfo {
    /// Create info indicating stats_parsed is not available or should not be used.
    pub(crate) fn not_available() -> Self {
        Self {
            use_stats_parsed: false,
            stats_parsed_schema: None,
        }
    }

    /// Create info for using stats_parsed with the given safe read schema.
    pub(crate) fn available(stats_parsed_schema: SchemaRef) -> Self {
        Self {
            use_stats_parsed: true,
            stats_parsed_schema: Some(stats_parsed_schema),
        }
    }
}

/// Build the Add action schema for checkpoint reading, optionally including stats_parsed.
///
/// When `stats_parsed_schema` is provided, the returned schema includes `add.stats_parsed`
/// in addition to the regular Add fields. This allows reading stats_parsed from checkpoints.
pub(crate) fn build_add_schema_with_stats_parsed(
    stats_parsed_schema: Option<&StructType>,
) -> StructType {
    use crate::actions::{Add, STATS_PARSED_NAME};

    let base_schema = Add::to_schema();

    match stats_parsed_schema {
        Some(stats) => {
            let mut fields: Vec<StructField> = base_schema.into_fields().collect();
            fields.push(StructField::nullable(
                STATS_PARSED_NAME,
                DataType::Struct(Box::new(stats.clone())),
            ));
            StructType::new_unchecked(fields)
        }
        None => base_schema,
    }
}
