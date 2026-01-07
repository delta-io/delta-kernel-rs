//! Field classifier implementations for different scan types (regular and CDF scans)

use crate::schema::StructField;
use crate::table_changes::{
    CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
};
use crate::transforms::FieldTransformSpec;

/// Trait for classifying fields during StateInfo construction.  Allows different scan types
/// (regular, CDF) to customize field handling. Note that the default set of field handling occurs
/// in [`StateInfo::try_new`](crate::scan::state_info::StateInfo::try_new). A
/// `TransformFieldClassifier` can be used to override the behavior implemented in that method.
pub(crate) trait TransformFieldClassifier {
    /// Classify a field and return its transform spec.
    /// Returns None if the field is physical (should be read from parquet).
    /// Returns Some(spec) if the field needs transformation (partition, metadata-derived, or dynamic).
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        last_physical_field: &Option<String>,
    ) -> Option<FieldTransformSpec>;
}

// Empty classifier, always returns None
impl TransformFieldClassifier for () {
    fn classify_field(
        &self,
        _: &StructField,
        _: usize,
        _: &Option<String>,
    ) -> Option<FieldTransformSpec> {
        None
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
            _ => None,
        }
    }
}
