//! Schema diffing implementation for Delta Lake schemas
//! 
//! This module provides functionality to compute differences between two schemas
//! using field IDs as the primary mechanism for identifying fields across schema versions.

use std::collections::{HashMap, HashSet};
use super::{StructField, StructType, MetadataValue, ColumnMetadataKey};

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDiff {
    /// Added in new
    pub added_fields: Vec<StructField>,
    /// Removed from old
    pub removed_fields: Vec<StructField>,
    pub updated_fields: Vec<FieldUpdate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldUpdate {
    pub before: StructField,
    pub after: StructField,
    pub change_type: FieldChangeType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldChangeType {
    Renamed,
    NullabilityChanged,
    TypeChanged,
    MetadataChanged,
    Multiple(Vec<FieldChangeType>),
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaDiffError {
    #[error("Field '{0}' is missing column mapping ID")]
    MissingFieldId(String),
    #[error("Duplicate field ID {0} found in schema")]
    DuplicateFieldId(i64),
    #[error("Schema contains fields without column mapping IDs")]
    MissingColumnMappingIds,
}

impl SchemaDiff {
    /// Returns true if there are no differences between the schemas
    pub fn is_empty(&self) -> bool {
        self.added_fields.is_empty() 
            && self.removed_fields.is_empty() 
            && self.updated_fields.is_empty()
    }

    /// Returns the total number of changes
    pub fn change_count(&self) -> usize {
        self.added_fields.len() + self.removed_fields.len() + self.updated_fields.len()
    }

    /// Returns true if there are any breaking changes (removed fields or type changes)
    pub fn has_breaking_changes(&self) -> bool {
        !self.removed_fields.is_empty() || 
        self.updated_fields.iter().any(|update| {
            matches!(update.change_type, 
                FieldChangeType::TypeChanged | 
                FieldChangeType::Multiple(ref changes) if changes.contains(&FieldChangeType::TypeChanged)
            )
        })
    }
}

/// Computes the difference between two schemas using field IDs for identification
///
/// This function requires that both schemas have column mapping enabled and all fields
/// have valid field IDs. Fields are matched by their field ID rather than name,
/// allowing detection of renames.
///
/// # Arguments
/// * `current` - The current/original schema
/// * `new` - The new schema to compare against
///
/// # Returns
/// A `SchemaDiff` describing all changes, or an error if the schemas are invalid
///
/// # Example
/// ```rust,ignore
/// use delta_kernel::schema::{StructType, StructField, DataType};
/// 
/// let current = StructType::try_new([
///     StructField::new("id", DataType::LONG, false)
///         .add_metadata([("delta.columnMapping.id", 1i64)]),
///     StructField::new("name", DataType::STRING, false)  
///         .add_metadata([("delta.columnMapping.id", 2i64)]),
/// ])?;
/// 
/// let new = StructType::try_new([
///     StructField::new("id", DataType::LONG, false)
///         .add_metadata([("delta.columnMapping.id", 1i64)]),
///     StructField::new("full_name", DataType::STRING, false) // Renamed!
///         .add_metadata([("delta.columnMapping.id", 2i64)]),
///     StructField::new("age", DataType::INTEGER, true) // Added!
///         .add_metadata([("delta.columnMapping.id", 3i64)]),
/// ])?;
/// 
/// let diff = compute_schema_diff(&current, &new)?;
/// assert_eq!(diff.added_fields.len(), 1);
/// assert_eq!(diff.updated_fields.len(), 1);
/// assert_eq!(diff.updated_fields[0].change_type, FieldChangeType::Renamed);
/// ```
pub fn compute_schema_diff(
    current: &StructType, 
    new: &StructType
) -> Result<SchemaDiff, SchemaDiffError> {
    // Build field ID maps for both schemas
    let current_by_id = build_field_id_map(current)?;
    let new_by_id = build_field_id_map(new)?;

    let current_field_ids: HashSet<i64> = current_by_id.keys().cloned().collect();
    let new_field_ids: HashSet<i64> = new_by_id.keys().cloned().collect();

    // Find added, removed, and potentially updated fields
    let added_ids: Vec<i64> = new_field_ids.difference(&current_field_ids).cloned().collect();
    let removed_ids: Vec<i64> = current_field_ids.difference(&new_field_ids).cloned().collect();
    let common_ids: Vec<i64> = current_field_ids.intersection(&new_field_ids).cloned().collect();

    // Collect added fields
    let added_fields: Vec<StructField> = added_ids
        .into_iter()
        .map(|id| new_by_id[&id].clone())
        .collect();

    // Collect removed fields  
    let removed_fields: Vec<StructField> = removed_ids
        .into_iter()
        .map(|id| current_by_id[&id].clone())
        .collect();

    // Check for updates in common fields
    let mut updated_fields = Vec::new();
    for id in common_ids {
        let current_field = &current_by_id[&id];
        let new_field = &new_by_id[&id];
        
        if let Some(field_update) = compare_fields(current_field, new_field) {
            updated_fields.push(field_update);
        }
    }

    Ok(SchemaDiff {
        added_fields,
        removed_fields,
        updated_fields,
    })
}

/// Builds a map from field ID to StructField for the given schema
fn build_field_id_map(schema: &StructType) -> Result<HashMap<i64, StructField>, SchemaDiffError> {
    let mut field_map = HashMap::new();
    
    for field in schema.fields() {
        let field_id = get_field_id(field)?;
        
        if field_map.insert(field_id, field.clone()).is_some() {
            return Err(SchemaDiffError::DuplicateFieldId(field_id));
        }
    }
    
    Ok(field_map)
}

/// Extracts the field ID from a StructField's metadata
fn get_field_id(field: &StructField) -> Result<i64, SchemaDiffError> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
        Some(MetadataValue::Number(id)) => Ok(*id),
        _ => Err(SchemaDiffError::MissingFieldId(field.name().clone())),
    }
}

/// Compares two fields with the same ID and returns a FieldUpdate if they differ
fn compare_fields(before: &StructField, after: &StructField) -> Option<FieldUpdate> {
    let mut changes = Vec::new();
    
    // Check for name change (rename)
    if before.name() != after.name() {
        changes.push(FieldChangeType::Renamed);
    }
    
    // Check for nullability change
    if before.nullable != after.nullable {
        changes.push(FieldChangeType::NullabilityChanged);
    }
    
    // Check for type change
    if before.data_type() != after.data_type() {
        changes.push(FieldChangeType::TypeChanged);
    }
    
    // Check for metadata changes (excluding column mapping metadata)
    if has_metadata_changes(before, after) {
        changes.push(FieldChangeType::MetadataChanged);
    }
    
    if changes.is_empty() {
        None
    } else {
        let change_type = match changes.len() {
            1 => changes.into_iter().next().unwrap(),
            _ => FieldChangeType::Multiple(changes),
        };
        
        Some(FieldUpdate {
            before: before.clone(),
            after: after.clone(),
            change_type,
        })
    }
}

/// Checks if two fields have different metadata (excluding column mapping metadata)
fn has_metadata_changes(before: &StructField, after: &StructField) -> bool {
    let filter_column_mapping_metadata = |metadata: &HashMap<String, MetadataValue>| {
        metadata.iter()
            .filter(|(key, _)| {
                !key.starts_with("delta.columnMapping") && !key.starts_with("parquet.field")
            })
            .collect::<HashMap<_, _>>()
    };
    
    let before_filtered = filter_column_mapping_metadata(&before.metadata);
    let after_filtered = filter_column_mapping_metadata(&after.metadata);
    
    before_filtered != after_filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    fn create_field_with_id(name: &str, data_type: DataType, nullable: bool, id: i64) -> StructField {
        StructField::new(name, data_type, nullable)
            .add_metadata([("delta.columnMapping.id", MetadataValue::Number(id))])
    }

    #[test]
    fn test_identical_schemas() {
        let schema = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let diff = compute_schema_diff(&schema, &schema).unwrap();
        assert!(diff.is_empty());
    }

    #[test]
    fn test_added_field() {
        let current = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.added_fields[0].name(), "name");
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
    }

    #[test]
    fn test_removed_field() {
        let current = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 1);
        assert_eq!(diff.removed_fields[0].name(), "name");
        assert_eq!(diff.updated_fields.len(), 0);
    }

    #[test]
    fn test_renamed_field() {
        let current = StructType::new_unchecked([
            create_field_with_id("name", DataType::STRING, false, 1),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("full_name", DataType::STRING, false, 1),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        
        let update = &diff.updated_fields[0];
        assert_eq!(update.before.name(), "name");
        assert_eq!(update.after.name(), "full_name");
        assert_eq!(update.change_type, FieldChangeType::Renamed);
    }

    #[test]
    fn test_nullability_change() {
        let current = StructType::new_unchecked([
            create_field_with_id("name", DataType::STRING, false, 1),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("name", DataType::STRING, true, 1),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].change_type, FieldChangeType::NullabilityChanged);
    }

    #[test]
    fn test_type_change() {
        let current = StructType::new_unchecked([
            create_field_with_id("value", DataType::INTEGER, false, 1),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("value", DataType::LONG, false, 1),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].change_type, FieldChangeType::TypeChanged);
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_multiple_changes() {
        let current = StructType::new_unchecked([
            create_field_with_id("name", DataType::STRING, false, 1),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("full_name", DataType::STRING, true, 1),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.updated_fields.len(), 1);
        
        let update = &diff.updated_fields[0];
        if let FieldChangeType::Multiple(changes) = &update.change_type {
            assert_eq!(changes.len(), 2);
            assert!(changes.contains(&FieldChangeType::Renamed));
            assert!(changes.contains(&FieldChangeType::NullabilityChanged));
        } else {
            panic!("Expected Multiple change type");
        }
    }

    #[test]
    fn test_missing_field_id_error() {
        let current = StructType::new_unchecked([
            StructField::new("id", DataType::LONG, false), // Missing field ID
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
        ]);

        let result = compute_schema_diff(&current, &new);
        assert!(matches!(result, Err(SchemaDiffError::MissingFieldId(_))));
    }

    #[test]
    fn test_duplicate_field_id_error() {
        let current = StructType::new_unchecked([
            create_field_with_id("id1", DataType::LONG, false, 1),
            create_field_with_id("id2", DataType::LONG, false, 1), // Duplicate ID
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id1", DataType::LONG, false, 1),
        ]);

        let result = compute_schema_diff(&current, &new);
        assert!(matches!(result, Err(SchemaDiffError::DuplicateFieldId(1))));
    }

    #[test]
    fn test_metadata_change() {
        let current = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1)
                .add_metadata([("custom.metadata", MetadataValue::String("old_value".to_string()))]),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1)
                .add_metadata([("custom.metadata", MetadataValue::String("new_value".to_string()))]),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].change_type, FieldChangeType::MetadataChanged);
    }

    #[test] 
    fn test_column_mapping_metadata_ignored() {
        let current = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1)
                .add_metadata([("delta.columnMapping.physicalName", MetadataValue::String("col_1".to_string()))]),
        ]);

        let new = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1)
                .add_metadata([("delta.columnMapping.physicalName", MetadataValue::String("col_2".to_string()))]),
        ]);

        let diff = compute_schema_diff(&current, &new).unwrap();
        // Column mapping metadata changes should be ignored
        assert!(diff.is_empty());
    }
}