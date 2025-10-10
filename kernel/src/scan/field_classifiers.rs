//! Field classifier implementations for different scan types (regular and CDF scans)

use crate::schema::{DataType, MetadataColumnSpec, StructField};
use crate::table_changes::{
    CHANGE_TYPE_COL_NAME, COMMIT_TIMESTAMP_COL_NAME, COMMIT_VERSION_COL_NAME,
};
use crate::table_configuration::TableConfiguration;
use crate::transforms::FieldTransformSpec;
use crate::{DeltaResult, Error};

/// If a field requires transformation(s) or extra read fields, we use this struct to communicate
/// them together
pub(crate) struct FieldRequirements {
    pub(crate) transform_specs: Vec<FieldTransformSpec>,
    pub(crate) extra_read_fields: Vec<StructField>,
}

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
        table_configuration: &TableConfiguration,
        last_physical_field: &Option<String>,
    ) -> DeltaResult<Option<FieldRequirements>>;
}

// Empty classifier, always returns None
impl TransformFieldClassifier for () {
    fn classify_field(
        &self,
        _: &StructField,
        _: usize,
        _: &TableConfiguration,
        _: &Option<String>,
    ) -> DeltaResult<Option<FieldRequirements>> {
        Ok(None)
    }
}

/// Regular scan field classifier for standard Delta table scans.
/// Handles partition columns as metadata-derived fields.
pub(crate) struct ScanTransformFieldClassifierieldClassifier;
impl TransformFieldClassifier for ScanTransformFieldClassifierieldClassifier {
    fn classify_field(
        &self,
        field: &StructField,
        field_index: usize,
        table_configuration: &TableConfiguration,
        last_physical_field: &Option<String>,
    ) -> DeltaResult<Option<FieldRequirements>> {
        if table_configuration
            .metadata()
            .partition_columns()
            .contains(field.name())
        {
            // Partition column: needs transform to inject metadata
            let transform_specs = vec![FieldTransformSpec::MetadataDerivedColumn {
                field_index,
                insert_after: last_physical_field.clone(),
            }];
            Ok(Some(FieldRequirements {
                transform_specs,
                extra_read_fields: vec![],
            }))
        } else {
            match field.get_metadata_column_spec() {
                Some(MetadataColumnSpec::RowId) => {
                    if table_configuration.table_properties().enable_row_tracking == Some(true) {
                        let mut transform_specs = vec![];
                        let mut extra_read_fields = vec![];
                        let row_id_col = table_configuration
                            .metadata()
                            .configuration()
                            .get("delta.rowTracking.materializedRowIdColumnName")
                            .ok_or(Error::generic("No delta.rowTracking.materializedRowIdColumnName key found in metadata configuration"))?;
                        // we can `take` as we should only have one RowId col
                        let mut selected_row_index_col_name = None;
                        let index_column_name = match selected_row_index_col_name.take() {
                            Some(index_column_name) => index_column_name,
                            None => {
                                // ensure we have a column name that isn't already in our schema
                                let index_column_name = (0..)
                                    .map(|i| format!("row_indexes_for_row_id_{}", i))
                                    .find(|_name| true) //logical_schema.field(name).is_none())
                                    .ok_or(Error::generic(
                                        "Couldn't generate row index column name",
                                    ))?;
                                extra_read_fields.push(StructField::create_metadata_column(
                                    &index_column_name,
                                    MetadataColumnSpec::RowIndex,
                                ));
                                transform_specs.push(FieldTransformSpec::StaticDrop {
                                    field_name: index_column_name.clone(),
                                });
                                index_column_name
                            }
                        };

                        extra_read_fields.push(StructField::nullable(row_id_col, DataType::LONG));
                        transform_specs.push(FieldTransformSpec::GenerateRowId {
                            field_name: row_id_col.to_string(),
                            row_index_field_name: index_column_name,
                        });
                        Ok(Some(FieldRequirements {
                            transform_specs,
                            extra_read_fields,
                        }))
                    } else {
                        Err(Error::unsupported("Row ids are not enabled on this table"))
                    }
                }
                Some(MetadataColumnSpec::RowIndex) => {
                    // handled in parquet reader, treat as regular physical field
                    Ok(None)
                }
                Some(MetadataColumnSpec::RowCommitVersion) => {
                    Err(Error::unsupported("Row commit versions not supported"))
                }
                _ => {
                    // Regular physical field - no transform needed
                    Ok(None)
                }
            }
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
        table_configuration: &TableConfiguration,
        last_physical_field: &Option<String>,
    ) -> DeltaResult<Option<FieldRequirements>> {
        let transform_spec = match field.name().as_str() {
            // _change_type is dynamic - physical in CDC files, metadata in Add/Remove files
            CHANGE_TYPE_COL_NAME => FieldTransformSpec::DynamicColumn {
                field_index,
                physical_name: CHANGE_TYPE_COL_NAME.to_string(),
                insert_after: last_physical_field.clone(),
            },
            // _commit_version and _commit_timestamp are always derived from metadata
            COMMIT_VERSION_COL_NAME | COMMIT_TIMESTAMP_COL_NAME => {
                FieldTransformSpec::MetadataDerivedColumn {
                    field_index,
                    insert_after: last_physical_field.clone(),
                }
            }
            // Defer to default classifier for partition columns and physical fields
            _ => {
                return ScanTransformFieldClassifierieldClassifier.classify_field(
                    field,
                    field_index,
                    table_configuration,
                    last_physical_field,
                )
            }
        };
        Ok(Some(FieldRequirements {
            transform_specs: vec![transform_spec],
            extra_read_fields: vec![],
        }))
    }
}
