//! Field classifier implementations for different scan types (regular and CDF scans)

use crate::schema::StructField;
use crate::table_changes::{
    CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
};
use crate::transforms::FieldTransformSpec;

/// Trait for classifying fields during StateInfo construction.
/// Allows different scan types (regular, CDF) to customize field handling.
pub(crate) trait TransformFieldClassifier {
    /// Classify a field and return its transform spec.
    /// Returns None if the field is physical (should be read from parquet).
    /// Returns Some(spec) if the field needs transformation (partition, metadata-derived, or dynamic).
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        partition_columns: &[String],
        last_physical_field: &Option<String>,
    ) -> Option<FieldTransformSpec>;
}

/// Regular scan field classifier for standard Delta table scans.
/// Handles partition columns as metadata-derived fields.
pub(crate) struct ScanTransformFieldClassifier;
impl TransformFieldClassifier for ScanTransformFieldClassifier {
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        partition_columns: &[String],
        last_physical_field: &Option<String>,
    ) -> Option<FieldTransformSpec> {
        if partition_columns.contains(field.name()) {
            // Partition column: needs transform to inject metadata
            Some(FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after: last_physical_field.clone(),
            })
        } else {
            // Regular physical field - no transform needed
            None
        }
    }
}

/// CDF-specific field classifier that handles Change Data Feed columns.
/// Handles _change_type as Dynamic and CDF metadata columns (_commit_version, _commit_timestamp).
pub(crate) struct CdfTransformFieldClassifier;
impl TransformFieldClassifier for CdfTransformFieldClassifier {
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        partition_columns: &[String],
        last_physical_field: &Option<String>,
    ) -> Option<FieldTransformSpec> {
        match field.name().as_str() {
            // _change_type is dynamic - physical in CDC files, metadata in Add/Remove files
            CHANGE_TYPE_COL_NAME => Some(FieldTransformSpec::DynamicColumn {
                field_index,
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                insert_after: last_physical_field.clone(),
            }),
            // _commit_version and _commit_timestamp are always derived from metadata
            COMMIT_VERSION_COL_NAME | COMMIT_TIMESTAMP_COL_NAME => {
                Some(FieldTransformSpec::MetadataDerivedColumn {
                    field_index,
                    insert_after: last_physical_field.clone(),
                })
            }
            // Defer to default classifier for partition columns and physical fields
            _ => ScanTransformFieldClassifier.classify_field(
                field,
                field_index,
                partition_columns,
                last_physical_field,
            ),
        }
    }
}
