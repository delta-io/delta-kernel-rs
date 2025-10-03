//! Field classifier implementations for different scan types (regular and CDF scans)

use crate::schema::StructField;
use crate::transforms::FieldTransformSpec;

/// Trait for classifying fields during StateInfo construction.
/// Allows different scan types (regular, CDF) to customize field handling.
pub(crate) trait FieldClassifier {
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
pub(crate) struct RegularFieldClassifier;

impl FieldClassifier for RegularFieldClassifier {
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
pub(crate) struct CdfFieldClassifier;
use crate::table_changes::{
    CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
};

impl FieldClassifier for CdfFieldClassifier {
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        partition_columns: &[String],
        last_physical_field: &Option<String>,
    ) -> Option<FieldTransformSpec> {
        if partition_columns.contains(field.name()) {
            // Partition column - metadata derived
            Some(FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after: last_physical_field.clone(),
            })
        } else if field.name() == CHANGE_TYPE_COL_NAME {
            // _change_type is dynamic - physical in CDC files, metadata in Add/Remove files
            Some(FieldTransformSpec::DynamicColumn {
                field_index,
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                insert_after: last_physical_field.clone(),
            })
        } else if field.name() == COMMIT_VERSION_COL_NAME
            || field.name() == COMMIT_TIMESTAMP_COL_NAME
        {
            // Other CDF metadata columns are always computed
            Some(FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after: last_physical_field.clone(),
            })
        } else {
            // Regular data field - read from parquet
            None
        }
    }
}
