//! Schema reconciliation for checkpoint stats_parsed reading
//!
//! This module handles reconciliation between checkpoint schema and table schema
//! when reading stats_parsed from checkpoints. This is necessary because:
//! - Schema evolution: checkpoint might have different schema than current table
//! - Type widening: int → long, float → double are compatible
//! - Column mapping: need to match by field ID or name

use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::{DataType, PrimitiveType, SchemaRef, StructField, StructType};
use crate::DeltaResult;
#[allow(unused_imports)]
use crate::Error;

/// Result of schema reconciliation containing compatible fields
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// The reconciled schema with only compatible fields
    pub reconciled_schema: SchemaRef,
    /// Map from field name to compatibility status
    pub compatibility_map: HashMap<String, FieldCompatibility>,
}

/// Compatibility status for a field during reconciliation
#[derive(Debug, Clone, PartialEq)]
pub enum FieldCompatibility {
    /// Field is compatible and can be read
    Compatible,
    /// Field types are incompatible (will be NULL)
    Incompatible {
        checkpoint_type: DataType,
        table_type: DataType
    },
    /// Field is missing from checkpoint (will be NULL)
    Missing,
}

/// Reconcile stats schema between checkpoint and table
///
/// This function compares the checkpoint's stats_parsed schema with the table schema
/// and returns a reconciled schema that contains only compatible fields.
///
/// # Arguments
/// * `checkpoint_stats_schema` - The stats_parsed schema from the checkpoint
/// * `table_schema` - The current table schema
///
/// # Returns
/// A `ReconcileResult` containing the reconciled schema and compatibility information
pub fn reconcile_stats_schema(
    checkpoint_stats_schema: &StructType,
    table_schema: &StructType,
) -> DeltaResult<ReconcileResult> {
    let mut reconciled_fields = Vec::new();
    let mut compatibility_map = HashMap::new();

    // Get the minValues/maxValues/nullCount subfields from checkpoint stats_parsed
    let min_values_field = checkpoint_stats_schema.field("minValues");
    let max_values_field = checkpoint_stats_schema.field("maxValues");
    let null_count_field = checkpoint_stats_schema.field("nullCount");

    // Extract the struct types for min/max/null if they exist
    let min_values_struct = min_values_field.and_then(|f| {
        if let DataType::Struct(s) = &f.data_type {
            Some(s.as_ref())
        } else {
            None
        }
    });

    let max_values_struct = max_values_field.and_then(|f| {
        if let DataType::Struct(s) = &f.data_type {
            Some(s.as_ref())
        } else {
            None
        }
    });

    let null_count_struct = null_count_field.and_then(|f| {
        if let DataType::Struct(s) = &f.data_type {
            Some(s.as_ref())
        } else {
            None
        }
    });

    // Process each field in the table schema
    for table_field in table_schema.fields() {
        let field_name = table_field.name();
        let physical_name = table_field.physical_name();

        // Try to find the field in checkpoint stats (by physical name for column mapping)
        let checkpoint_field = find_field_in_stats(
            physical_name,
            field_name,
            min_values_struct,
            max_values_struct,
        );

        match checkpoint_field {
            Some((checkpoint_type, _source)) => {
                // Check type compatibility
                if is_type_compatible(&checkpoint_type, &table_field.data_type) {
                    // Compatible - include in reconciled schema
                    reconciled_fields.push(table_field.clone());
                    compatibility_map.insert(field_name.to_string(), FieldCompatibility::Compatible);
                } else {
                    // Incompatible - will be NULL in results
                    compatibility_map.insert(
                        field_name.to_string(),
                        FieldCompatibility::Incompatible {
                            checkpoint_type: checkpoint_type.clone(),
                            table_type: table_field.data_type.clone(),
                        },
                    );
                }
            }
            None => {
                // Missing from checkpoint - will be NULL
                compatibility_map.insert(field_name.to_string(), FieldCompatibility::Missing);
            }
        }
    }

    // Build reconciled stats_parsed schema structure
    let reconciled_stats_schema = build_reconciled_stats_schema(
        reconciled_fields,
        checkpoint_stats_schema,
        min_values_struct,
        max_values_struct,
        null_count_struct,
    )?;

    Ok(ReconcileResult {
        reconciled_schema: Arc::new(reconciled_stats_schema),
        compatibility_map,
    })
}

/// Find a field in the checkpoint stats structures
fn find_field_in_stats(
    physical_name: &str,
    logical_name: &str,
    min_values: Option<&StructType>,
    max_values: Option<&StructType>,
) -> Option<(DataType, &'static str)> {
    // First try to match by physical name (for column mapping)
    if let Some(min_struct) = min_values {
        if let Some(field) = min_struct.field(physical_name) {
            return Some((field.data_type.clone(), "minValues"));
        }
    }

    if let Some(max_struct) = max_values {
        if let Some(field) = max_struct.field(physical_name) {
            return Some((field.data_type.clone(), "maxValues"));
        }
    }

    // Fallback to logical name if physical name not found
    if physical_name != logical_name {
        if let Some(min_struct) = min_values {
            if let Some(field) = min_struct.field(logical_name) {
                return Some((field.data_type.clone(), "minValues"));
            }
        }

        if let Some(max_struct) = max_values {
            if let Some(field) = max_struct.field(logical_name) {
                return Some((field.data_type.clone(), "maxValues"));
            }
        }
    }

    None
}

/// Check if two data types are compatible for stats reading
///
/// Compatible cases:
/// - Exact match
/// - Type widening (int → long, float → double)
/// - Both nullable (nullability doesn't affect stats compatibility)
fn is_type_compatible(checkpoint_type: &DataType, table_type: &DataType) -> bool {
    use DataType::Primitive;
    use PrimitiveType::*;

    match (checkpoint_type, table_type) {
        // Exact match
        (a, b) if a == b => true,

        // Type widening: byte/short/int → long
        (Primitive(Byte | Short | Integer), Primitive(Long)) => true,

        // Type widening: byte/short → int
        (Primitive(Byte | Short), Primitive(Integer)) => true,

        // Type widening: byte → short
        (Primitive(Byte), Primitive(Short)) => true,

        // Type widening: float → double
        (Primitive(Float), Primitive(Double)) => true,

        // TODO: Handle more complex types like decimals, nested structs
        _ => false,
    }
}

/// Build the reconciled stats_parsed schema structure
fn build_reconciled_stats_schema(
    compatible_fields: Vec<StructField>,
    original_stats_schema: &StructType,
    min_values_struct: Option<&StructType>,
    max_values_struct: Option<&StructType>,
    null_count_struct: Option<&StructType>,
) -> DeltaResult<StructType> {
    let mut stats_fields = Vec::new();

    // Always include numRecords if present
    if let Some(num_records_field) = original_stats_schema.field("numRecords") {
        stats_fields.push(num_records_field.clone());
    }

    // Build minValues struct with only compatible fields
    if min_values_struct.is_some() && !compatible_fields.is_empty() {
        let min_values = StructType::new_unchecked(
            compatible_fields
                .iter()
                .filter_map(|f| {
                    min_values_struct
                        .and_then(|s| s.field(f.physical_name()))
                        .cloned()
                })
                .collect::<Vec<_>>(),
        );
        stats_fields.push(StructField::nullable("minValues", DataType::Struct(Box::new(min_values))));
    }

    // Build maxValues struct with only compatible fields
    if max_values_struct.is_some() && !compatible_fields.is_empty() {
        let max_values = StructType::new_unchecked(
            compatible_fields
                .iter()
                .filter_map(|f| {
                    max_values_struct
                        .and_then(|s| s.field(f.physical_name()))
                        .cloned()
                })
                .collect::<Vec<_>>(),
        );
        stats_fields.push(StructField::nullable("maxValues", DataType::Struct(Box::new(max_values))));
    }

    // Build nullCount struct with only compatible fields
    if null_count_struct.is_some() && !compatible_fields.is_empty() {
        let null_count = StructType::new_unchecked(
            compatible_fields
                .iter()
                .filter_map(|f| {
                    null_count_struct
                        .and_then(|s| s.field(f.physical_name()))
                        .cloned()
                })
                .collect::<Vec<_>>(),
        );
        stats_fields.push(StructField::nullable("nullCount", DataType::Struct(Box::new(null_count))));
    }

    // Include tightBounds if present
    if let Some(tight_bounds_field) = original_stats_schema.field("tightBounds") {
        stats_fields.push(tight_bounds_field.clone());
    }

    Ok(StructType::new_unchecked(stats_fields))
}

/// Check if column mapping fast path can be used
///
/// The fast path can be used when:
/// 1. Column mapping is enabled on the table
/// 2. Column mapping was enabled at or before the checkpoint was written
/// 3. All subsequent table replaces honored column mapping requirements
///
/// When these conditions hold, we can trust the checkpoint schema without reconciliation.
pub fn can_use_column_mapping_fast_path(
    table_features: &[String],
    checkpoint_version: i64,
    column_mapping_enable_version: Option<i64>,
) -> bool {
    // Check if column mapping is enabled
    let has_column_mapping = table_features
        .iter()
        .any(|f| f == "columnMapping" || f == "columnMappingV2");

    if !has_column_mapping {
        return false;
    }

    // Check if column mapping was enabled before or at checkpoint version
    if let Some(enable_version) = column_mapping_enable_version {
        checkpoint_version >= enable_version
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_compatibility() {
        // Exact matches
        assert!(is_type_compatible(
            &DataType::Primitive(PrimitiveType::Integer),
            &DataType::Primitive(PrimitiveType::Integer)
        ));

        // Type widening
        assert!(is_type_compatible(
            &DataType::Primitive(PrimitiveType::Integer),
            &DataType::Primitive(PrimitiveType::Long)
        ));
        assert!(is_type_compatible(
            &DataType::Primitive(PrimitiveType::Float),
            &DataType::Primitive(PrimitiveType::Double)
        ));

        // Incompatible types
        assert!(!is_type_compatible(
            &DataType::Primitive(PrimitiveType::String),
            &DataType::Primitive(PrimitiveType::Integer)
        ));
        assert!(!is_type_compatible(
            &DataType::Primitive(PrimitiveType::Long),
            &DataType::Primitive(PrimitiveType::Integer)
        ));
    }

    #[test]
    fn test_column_mapping_fast_path() {
        // With column mapping enabled
        assert!(can_use_column_mapping_fast_path(
            &["columnMapping".to_string()],
            100,
            Some(50)
        ));

        // Without column mapping
        assert!(!can_use_column_mapping_fast_path(
            &[],
            100,
            Some(50)
        ));

        // Column mapping enabled after checkpoint
        assert!(!can_use_column_mapping_fast_path(
            &["columnMapping".to_string()],
            100,
            Some(150)
        ));
    }
}