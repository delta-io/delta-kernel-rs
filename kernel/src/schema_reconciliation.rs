//! Minimal schema reconciliation utilities for parsed statistics
//!
//! Provides essential helper functions for handling schema compatibility
//! when reading stats_parsed from checkpoints.

use crate::schema::{DataType, PrimitiveType};

/// Check if two data types are compatible for stats reading
///
/// Compatible cases:
/// - Exact match
/// - Type widening (int → long, float → double)
pub(crate) fn is_type_compatible(checkpoint_type: &DataType, table_type: &DataType) -> bool {
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

        _ => false,
    }
}

/// Check if column mapping fast path can be used
///
/// The fast path can be used when:
/// 1. Column mapping is enabled on the table
/// 2. Column mapping was enabled at or before the checkpoint was written
///
/// When these conditions hold, we can trust the checkpoint schema without reconciliation.
pub(crate) fn can_use_column_mapping_fast_path(
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
        assert!(!can_use_column_mapping_fast_path(&[], 100, Some(50)));

        // Column mapping enabled after checkpoint
        assert!(!can_use_column_mapping_fast_path(
            &["columnMapping".to_string()],
            100,
            Some(150)
        ));
    }
}
