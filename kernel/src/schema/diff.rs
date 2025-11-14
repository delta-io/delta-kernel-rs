//! Schema diffing implementation for Delta Lake schemas
//!
//! This module provides functionality to compute differences between two schemas
//! using field IDs as the primary mechanism for identifying fields across schema versions.
//! Supports nested field comparison within structs, arrays, and maps.

// Allow dead code warnings since this API is not yet used by other modules
#![allow(dead_code)]

use super::{ColumnMetadataKey, ColumnName, DataType, MetadataValue, StructField, StructType};
use std::collections::{HashMap, HashSet};

/// Represents the difference between two schemas
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SchemaDiff {
    /// Fields that were added in the new schema
    pub(crate) added_fields: Vec<FieldChange>,
    /// Fields that were removed from the original schema
    pub(crate) removed_fields: Vec<FieldChange>,
    /// Fields that were modified between schemas
    pub(crate) updated_fields: Vec<FieldUpdate>,
    /// Whether the diff contains breaking changes (computed once during construction)
    breaking_changes: bool,
}

/// Represents a field change (added or removed) at any nesting level
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldChange {
    /// The field that was added or removed
    pub(crate) field: StructField,
    /// The path to this field (e.g., ColumnName::new(["user", "address", "street"]))
    pub(crate) path: ColumnName,
}

/// Represents an update to a field between two schema versions
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldUpdate {
    /// The field as it existed in the original schema
    pub(crate) before: StructField,
    /// The field as it exists in the new schema
    pub(crate) after: StructField,
    /// The path to this field (e.g., ColumnName::new(["user", "address", "street"]))
    pub(crate) path: ColumnName,
    /// The types of changes that occurred (can be multiple, e.g. renamed + nullability changed)
    pub(crate) change_types: Vec<FieldChangeType>,
}

/// The types of changes that can occur to a field
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FieldChangeType {
    /// Field was renamed (logical name changed, but field ID stayed the same)
    Renamed,
    /// Field nullability was loosened (non-nullable -> nullable) - safe change
    NullabilityLoosened,
    /// Field nullability was tightened (nullable -> non-nullable) - breaking change
    NullabilityTightened,
    /// Field data type was changed
    TypeChanged,
    /// Field metadata was changed (excluding column mapping metadata)
    MetadataChanged,
    /// The container nullability was loosened (safe change)
    ContainerNullabilityLoosened,
    /// The container nullability was tightened (breaking change)
    ContainerNullabilityTightened,
}

/// Errors that can occur during schema diffing
#[derive(Debug, thiserror::Error)]
pub(crate) enum SchemaDiffError {
    #[error("Schema diffing is not yet implemented")]
    Unsupported,
    #[error("Field at path '{path}' is missing column mapping ID")]
    MissingFieldId { path: ColumnName },
    #[error("Duplicate field ID {id} found at paths '{path1}' and '{path2}'")]
    DuplicateFieldId {
        id: i64,
        path1: ColumnName,
        path2: ColumnName,
    },
    #[error(
        "Field at path '{path}' is missing physical name (required when column mapping is enabled)"
    )]
    MissingPhysicalName { path: ColumnName },
    #[error("Field with ID {field_id} at path '{path}' has inconsistent physical names: '{before}' -> '{after}'. Physical names must not change for the same field ID.")]
    PhysicalNameChanged {
        field_id: i64,
        path: ColumnName,
        before: String,
        after: String,
    },
}

impl SchemaDiff {
    /// Compute the difference between two schemas using field IDs
    pub(crate) fn new(before: &StructType, after: &StructType) -> Result<Self, SchemaDiffError> {
        compute_schema_diff(before, after)
    }

    /// Returns true if there are no differences between the schemas
    pub(crate) fn is_empty(&self) -> bool {
        self.added_fields.is_empty()
            && self.removed_fields.is_empty()
            && self.updated_fields.is_empty()
    }

    /// Returns the total number of changes
    pub(crate) fn change_count(&self) -> usize {
        self.added_fields.len() + self.removed_fields.len() + self.updated_fields.len()
    }

    /// Returns true if there are any breaking changes (removed fields, type changes, or tightened nullability)
    pub(crate) fn has_breaking_changes(&self) -> bool {
        self.breaking_changes
    }

    /// Get all changes (both top-level and nested)
    pub(crate) fn all_changes(&self) -> (&[FieldChange], &[FieldChange], &[FieldUpdate]) {
        (
            &self.added_fields,
            &self.removed_fields,
            &self.updated_fields,
        )
    }

    /// Get all changes at the top level only (fields with path length of 1)
    pub(crate) fn top_level_changes(
        &self,
    ) -> (Vec<&FieldChange>, Vec<&FieldChange>, Vec<&FieldUpdate>) {
        let added = self
            .added_fields
            .iter()
            .filter(|f| f.path.path().len() == 1)
            .collect();
        let removed = self
            .removed_fields
            .iter()
            .filter(|f| f.path.path().len() == 1)
            .collect();
        let updated = self
            .updated_fields
            .iter()
            .filter(|f| f.path.path().len() == 1)
            .collect();
        (added, removed, updated)
    }

    /// Get all changes at nested levels only (fields with path length > 1)
    pub(crate) fn nested_changes(
        &self,
    ) -> (Vec<&FieldChange>, Vec<&FieldChange>, Vec<&FieldUpdate>) {
        let added = self
            .added_fields
            .iter()
            .filter(|f| f.path.path().len() > 1)
            .collect();
        let removed = self
            .removed_fields
            .iter()
            .filter(|f| f.path.path().len() > 1)
            .collect();
        let updated = self
            .updated_fields
            .iter()
            .filter(|f| f.path.path().len() > 1)
            .collect();
        (added, removed, updated)
    }
}

/// Internal representation of a field with its path and ID
#[derive(Debug, Clone)]
struct FieldWithPath {
    field: StructField,
    path: ColumnName,
    field_id: i64,
}

/// Computes the difference between two schemas using field IDs for identification
///
/// This function requires that both schemas have column mapping enabled and all fields
/// have valid field IDs. Fields are matched by their field ID rather than name,
/// allowing detection of renames at any nesting level within structs, arrays, and maps.
///
/// # Note
/// It's recommended to use `SchemaDiff::new()` instead of calling this function directly:
///
/// ```rust,ignore
/// let diff = SchemaDiff::new(&old_schema, &new_schema)?;
/// ```
///
/// # Arguments
/// * `before` - The before/original schema
/// * `after` - The after/new schema to compare against
///
/// # Returns
/// A `SchemaDiff` describing all changes including nested fields, or an error if the schemas are invalid
fn compute_schema_diff(
    before: &StructType,
    after: &StructType,
) -> Result<SchemaDiff, SchemaDiffError> {
    // Collect all fields with their paths from both schemas
    let empty_path: Vec<String> = vec![];
    let before_fields =
        collect_all_fields_with_paths(before, &ColumnName::new(empty_path.clone()))?;
    let after_fields = collect_all_fields_with_paths(after, &ColumnName::new(empty_path))?;

    // Build maps by field ID
    let before_by_id = build_field_map_by_id(&before_fields)?;
    let after_by_id = build_field_map_by_id(&after_fields)?;

    let before_field_ids: HashSet<i64> = before_by_id.keys().cloned().collect();
    let after_field_ids: HashSet<i64> = after_by_id.keys().cloned().collect();

    // Find added, removed, and potentially updated fields
    let added_ids: Vec<i64> = after_field_ids
        .difference(&before_field_ids)
        .cloned()
        .collect();
    let removed_ids: Vec<i64> = before_field_ids
        .difference(&after_field_ids)
        .cloned()
        .collect();
    let common_ids: Vec<i64> = before_field_ids
        .intersection(&after_field_ids)
        .cloned()
        .collect();

    // Collect added fields
    let added_fields: Vec<FieldChange> = added_ids
        .into_iter()
        .map(|id| {
            let field_with_path = &after_by_id[&id];
            FieldChange {
                field: field_with_path.field.clone(),
                path: field_with_path.path.clone(),
            }
        })
        .collect();

    // Filter out nested fields whose parent was also added
    // Example: If "user" struct was added, don't also report "user.name", "user.email", etc.
    let added_fields = filter_ancestor_fields(added_fields);

    // Collect removed fields
    let removed_fields: Vec<FieldChange> = removed_ids
        .into_iter()
        .map(|id| {
            let field_with_path = &before_by_id[&id];
            FieldChange {
                field: field_with_path.field.clone(),
                path: field_with_path.path.clone(),
            }
        })
        .collect();

    // Filter out nested fields whose parent was also removed
    // Example: If "user" struct was removed, don't also report "user.name", "user.email", etc.
    let removed_fields = filter_ancestor_fields(removed_fields);

    // Check for updates in common fields
    let mut updated_fields = Vec::new();
    for id in common_ids {
        let before_field_with_path = &before_by_id[&id];
        let after_field_with_path = &after_by_id[&id];

        // Invariant: A field in common_ids must have existed in both schemas, which means
        // its parent path must also have existed in both schemas. Therefore, neither an
        // added nor removed ancestor should be a parent of an updated field.
        #[cfg(debug_assertions)]
        {
            let added_paths: HashSet<ColumnName> =
                added_fields.iter().map(|f| f.path.clone()).collect();
            let removed_paths: HashSet<ColumnName> =
                removed_fields.iter().map(|f| f.path.clone()).collect();

            debug_assert!(
                !has_added_ancestor(&after_field_with_path.path, &added_paths),
                "Field with ID {} at path '{}' is in common_ids but has an added ancestor. \
                 This violates the invariant that common fields must have existed in both schemas.",
                id,
                after_field_with_path.path
            );
            debug_assert!(
                !has_added_ancestor(&after_field_with_path.path, &removed_paths),
                "Field with ID {} at path '{}' is in common_ids but has a removed ancestor. \
                 This violates the invariant that common fields must have existed in both schemas.",
                id,
                after_field_with_path.path
            );
        }

        if let Some(field_update) =
            compute_field_update(before_field_with_path, after_field_with_path)?
        {
            updated_fields.push(field_update);
        }
    }

    // Compute whether there are breaking changes
    let has_breaking_changes =
        compute_has_breaking_changes(&added_fields, &removed_fields, &updated_fields);

    Ok(SchemaDiff {
        added_fields,
        removed_fields,
        updated_fields,
        breaking_changes: has_breaking_changes,
    })
}

/// Helper function to check if a change type is breaking
fn is_breaking_change_type(change_type: &FieldChangeType) -> bool {
    matches!(
        change_type,
        FieldChangeType::TypeChanged
            | FieldChangeType::NullabilityTightened
            | FieldChangeType::ContainerNullabilityTightened
            | FieldChangeType::MetadataChanged
    )
}

/// Computes whether the diff contains breaking changes
fn compute_has_breaking_changes(
    added_fields: &[FieldChange],
    _removed_fields: &[FieldChange],
    updated_fields: &[FieldUpdate],
) -> bool {
    // Removed fields are safe - existing data files remain valid, queries referencing
    // removed fields will fail at query time but data integrity is maintained
    // Adding a non-nullable (required) field is breaking - existing data won't have values
    added_fields.iter().any(|add| !add.field.nullable)
        // Certain update types are breaking (type changes, nullability tightening, etc.)
        || updated_fields.iter().any(|update| {
            update
                .change_types
                .iter()
                .any(is_breaking_change_type)
        })
}

/// Filters field changes to keep only the least common ancestors (LCA).
///
/// This filters out descendant fields when their parent is also in the set.
/// For example, if both "user" and "user.name" are in the input, this returns only "user"
/// since reporting "user.name" would be redundant.
///
/// The algorithm is O(n) where n is the number of fields:
/// 1. Put all paths in a HashSet for O(1) lookup
/// 2. For each field, check if its immediate parent is in the set
/// 3. Keep only fields whose parent is NOT in the set
fn filter_ancestor_fields(fields: Vec<FieldChange>) -> Vec<FieldChange> {
    // Build a set of all paths for O(1) lookup (owned to avoid lifetime issues)
    let all_paths: HashSet<ColumnName> = fields.iter().map(|f| f.path.clone()).collect();

    // Filter to keep only fields whose parent is NOT in the set
    fields
        .into_iter()
        .filter(|field_change| {
            let path_parts = field_change.path.path();

            // Top-level fields (length 1) have no parent, so keep them
            if path_parts.len() == 1 {
                return true;
            }

            // Construct parent path by removing the last component
            let parent_path = ColumnName::new(&path_parts[..path_parts.len() - 1]);

            // Keep this field only if its parent was NOT in the input set
            !all_paths.contains(&parent_path)
        })
        .collect()
}

/// Checks if a field path has a parent in the given set of ancestor paths.
///
/// Returns true if any path in `added_ancestor_paths` is a prefix of `path`.
/// For example, "user" is an ancestor of "user.name" and "user.address.street".
///
/// This implementation walks up the parent chain of `path`, checking at each level
/// if that parent exists in the set. This is O(depth) instead of O(N * depth) where
/// N is the number of ancestor paths.
fn has_added_ancestor(path: &ColumnName, added_ancestor_paths: &HashSet<ColumnName>) -> bool {
    let mut curr = path.parent();
    while let Some(parent) = curr {
        if added_ancestor_paths.contains(&parent) {
            return true;
        }
        curr = parent.parent();
    }
    false
}

/// Gets the physical name of a field if present
fn physical_name(field: &StructField) -> Option<&str> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Validates that physical names are consistent between two versions of the same field.
///
/// Since schema diffing requires column mapping (field IDs), physical names must be present
/// and must not change for the same field ID across schema versions.
///
/// # Errors
/// - `PhysicalNameChanged`: Physical name differs between before and after
/// - `MissingPhysicalName`: Physical name is missing in either version
fn validate_physical_name(
    before: &FieldWithPath,
    after: &FieldWithPath,
) -> Result<(), SchemaDiffError> {
    let before_physical = physical_name(&before.field);
    let after_physical = physical_name(&after.field);

    match (before_physical, after_physical) {
        (Some(b), Some(a)) if b == a => {
            // Valid: physical name is present and unchanged
            Ok(())
        }
        (Some(b), Some(a)) => {
            // Invalid: physical name changed for the same field ID
            Err(SchemaDiffError::PhysicalNameChanged {
                field_id: before.field_id,
                path: after.path.clone(),
                before: b.to_string(),
                after: a.to_string(),
            })
        }
        (Some(_), None) | (None, Some(_)) => {
            // Invalid: physical name was added or removed
            Err(SchemaDiffError::MissingPhysicalName {
                path: after.path.clone(),
            })
        }
        (None, None) => {
            // Invalid: physical name must be present when column mapping is enabled
            Err(SchemaDiffError::MissingPhysicalName {
                path: after.path.clone(),
            })
        }
    }
}

/// Recursively collects all struct fields from a data type with their paths
///
/// This helper function handles deep nesting like `array<array<struct<...>>>` or
/// `map<array<struct<...>>, array<struct<...>>>` by recursing through container layers.
fn collect_fields_from_datatype(
    data_type: &DataType,
    parent_path: &ColumnName,
) -> Result<Vec<FieldWithPath>, SchemaDiffError> {
    let mut fields = Vec::new();

    match data_type {
        DataType::Struct(struct_type) => {
            // Collect fields from this struct
            fields.extend(collect_all_fields_with_paths(struct_type, parent_path)?);
        }
        DataType::Array(array_type) => {
            // TODO: Add IcebergCompatV2 support - check that array nested field IDs remain stable
            // For IcebergCompatV2, arrays should have a field ID on the array itself and nested
            // field IDs must not be added or removed (they must stay the same across versions).
            // See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv2

            // For arrays, we use "element" as the path segment and recurse into element type
            let element_path = parent_path.join(&ColumnName::new(["element"]));
            fields.extend(collect_fields_from_datatype(
                array_type.element_type(),
                &element_path,
            )?);
        }
        DataType::Map(map_type) => {
            // TODO: Add IcebergCompatV2 support - check that map nested field IDs remain stable
            // For IcebergCompatV2, maps should have field IDs on key/value and nested field IDs
            // must not be added or removed (they must stay the same across versions).
            // See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv2

            // For maps, we use "key" and "value" as path segments and recurse into both types
            let key_path = parent_path.join(&ColumnName::new(["key"]));
            fields.extend(collect_fields_from_datatype(
                map_type.key_type(),
                &key_path,
            )?);

            let value_path = parent_path.join(&ColumnName::new(["value"]));
            fields.extend(collect_fields_from_datatype(
                map_type.value_type(),
                &value_path,
            )?);
        }
        _ => {
            // Primitive types don't have nested fields
        }
    }

    Ok(fields)
}

/// Recursively collects all struct fields with their paths from a schema
fn collect_all_fields_with_paths(
    schema: &StructType,
    parent_path: &ColumnName,
) -> Result<Vec<FieldWithPath>, SchemaDiffError> {
    let mut fields = Vec::new();

    for field in schema.fields() {
        let field_path = parent_path.join(&ColumnName::new([field.name()]));

        // Only struct fields can have field IDs in column mapping
        let field_id = get_field_id_for_path(field, &field_path)?;

        fields.push(FieldWithPath {
            field: field.clone(),
            path: field_path.clone(),
            field_id,
        });

        // Recursively collect nested struct fields from the field's data type
        fields.extend(collect_fields_from_datatype(
            field.data_type(),
            &field_path,
        )?);
    }

    Ok(fields)
}

/// Builds a map from field ID to FieldWithPath
fn build_field_map_by_id(
    fields: &[FieldWithPath],
) -> Result<HashMap<i64, FieldWithPath>, SchemaDiffError> {
    let mut field_map = HashMap::new();

    for field_with_path in fields {
        let field_id = field_with_path.field_id;

        if let Some(existing) = field_map.insert(field_id, field_with_path.clone()) {
            return Err(SchemaDiffError::DuplicateFieldId {
                id: field_id,
                path1: existing.path,
                path2: field_with_path.path.clone(),
            });
        }
    }

    Ok(field_map)
}

/// Extracts the field ID from a StructField's metadata with path for error reporting
fn get_field_id_for_path(field: &StructField, path: &ColumnName) -> Result<i64, SchemaDiffError> {
    match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
        Some(MetadataValue::Number(id)) => Ok(*id),
        _ => Err(SchemaDiffError::MissingFieldId { path: path.clone() }),
    }
}

/// Computes the update for two fields with the same ID, if they differ
fn compute_field_update(
    before: &FieldWithPath,
    after: &FieldWithPath,
) -> Result<Option<FieldUpdate>, SchemaDiffError> {
    let mut changes = Vec::new();

    // Check for name change (rename)
    if before.field.name() != after.field.name() {
        changes.push(FieldChangeType::Renamed);
    }

    // Check for nullability change - distinguish between tightening and loosening
    if let Some(change) =
        check_field_nullability_change(before.field.nullable, after.field.nullable)
    {
        changes.push(change);
    }

    // Validate physical name consistency
    validate_physical_name(before, after)?;

    // Check for type change (including container changes)
    changes.extend(classify_data_type_change(
        before.field.data_type(),
        after.field.data_type(),
    ));

    // Check for metadata changes (excluding column mapping metadata)
    if has_metadata_changes(&before.field, &after.field) {
        changes.push(FieldChangeType::MetadataChanged);
    }

    // If no changes detected, return None
    if changes.is_empty() {
        return Ok(None);
    }

    Ok(Some(FieldUpdate {
        before: before.field.clone(),
        after: after.field.clone(),
        path: after.path.clone(), // Use the new path in case of renames
        change_types: changes,
    }))
}

/// Checks for field nullability changes.
///
/// Returns:
/// - `Some(FieldChangeType::NullabilityLoosened)` if nullability was relaxed (false -> true)
/// - `Some(FieldChangeType::NullabilityTightened)` if nullability was restricted (true -> false)
/// - `None` if nullability didn't change
fn check_field_nullability_change(
    before_nullable: bool,
    after_nullable: bool,
) -> Option<FieldChangeType> {
    match (before_nullable, after_nullable) {
        (false, true) => Some(FieldChangeType::NullabilityLoosened),
        (true, false) => Some(FieldChangeType::NullabilityTightened),
        (true, true) | (false, false) => None,
    }
}

/// Checks for container nullability changes.
///
/// Returns:
/// - `Some(FieldChangeType::ContainerNullabilityLoosened)` if nullability was relaxed (false -> true)
/// - `Some(FieldChangeType::ContainerNullabilityTightened)` if nullability was restricted (true -> false)
/// - `None` if nullability didn't change
fn check_container_nullability_change(
    before_nullable: bool,
    after_nullable: bool,
) -> Option<FieldChangeType> {
    match (before_nullable, after_nullable) {
        (false, true) => Some(FieldChangeType::ContainerNullabilityLoosened),
        (true, false) => Some(FieldChangeType::ContainerNullabilityTightened),
        (true, true) | (false, false) => None,
    }
}

/// Classifies a type change between two data types.
///
/// Returns:
/// - A `Vec<FieldChangeType>` containing all changes detected (type changed or container nullability changed)
/// - An empty vec if the types are the same container with nested changes handled elsewhere
///
/// This function handles the following cases:
/// 1. **Struct containers**: Changes to nested fields are captured via field IDs, so return empty vec
/// 2. **Array containers**:
///    - If element types match and only nullability changed, return the specific nullability change
///    - If element types are both structs with same nullability, nested changes handled via field IDs (return empty vec)
///    - Otherwise, it's a type change
/// 3. **Map containers**: Similar logic to arrays, but for both key and value types
/// 4. **Different container types or primitives**: Type change
fn classify_data_type_change(before: &DataType, after: &DataType) -> Vec<FieldChangeType> {
    // Early return if types are identical - no change to report
    if before == after {
        return Vec::new();
    }

    match (before, after) {
        // Struct-to-struct: nested field changes are handled separately via field IDs
        (DataType::Struct(_), DataType::Struct(_)) => Vec::new(),

        // Array-to-array: check element types and nullability
        (DataType::Array(before_array), DataType::Array(after_array)) => {
            // Recursively check for element type changes
            let element_type_changes =
                match (before_array.element_type(), after_array.element_type()) {
                    // Both have struct elements - nested changes handled via field IDs
                    (DataType::Struct(_), DataType::Struct(_)) => Vec::new(),
                    // For non-struct elements, recurse to check for changes
                    (e1, e2) => classify_data_type_change(e1, e2),
                };

            // Check container nullability change
            let nullability_change = check_container_nullability_change(
                before_array.contains_null(),
                after_array.contains_null(),
            );

            // Combine both changes if present
            let mut changes = element_type_changes;
            if let Some(null_change) = nullability_change {
                changes.push(null_change);
            }
            changes
        }

        // Map-to-map: check key types, value types, and nullability
        (DataType::Map(before_map), DataType::Map(after_map)) => {
            // Recursively check for key type changes
            let key_type_changes = match (before_map.key_type(), after_map.key_type()) {
                // Both have struct keys - nested changes handled via field IDs
                (DataType::Struct(_), DataType::Struct(_)) => Vec::new(),
                // For non-struct keys (including arrays/maps containing structs), recurse
                (k1, k2) => classify_data_type_change(k1, k2),
            };

            // Recursively check for value type changes
            let value_type_changes = match (before_map.value_type(), after_map.value_type()) {
                // Both have struct values - nested changes handled via field IDs
                (DataType::Struct(_), DataType::Struct(_)) => Vec::new(),
                // For non-struct values (including arrays/maps containing structs), recurse
                (v1, v2) => classify_data_type_change(v1, v2),
            };

            // Check container nullability change
            let nullability_change = check_container_nullability_change(
                before_map.value_contains_null(),
                after_map.value_contains_null(),
            );

            // Combine all changes if present
            let mut changes = key_type_changes;
            changes.extend(value_type_changes);
            if let Some(null_change) = nullability_change {
                changes.push(null_change);
            }
            changes
        }

        // Different container types or primitive type changes
        _ => vec![FieldChangeType::TypeChanged],
    }
}

/// Checks if two fields have different metadata (excluding column mapping metadata)
fn has_metadata_changes(before: &StructField, after: &StructField) -> bool {
    // Instead of returning a HashMap of references, we'll compare directly
    let before_filtered: HashMap<String, MetadataValue> = before
        .metadata
        .iter()
        .filter(|(key, _)| {
            !key.starts_with("delta.columnMapping") && !key.starts_with("parquet.field")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let after_filtered: HashMap<String, MetadataValue> = after
        .metadata
        .iter()
        .filter(|(key, _)| {
            !key.starts_with("delta.columnMapping") && !key.starts_with("parquet.field")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    before_filtered != after_filtered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, DataType, StructField, StructType};

    fn create_field_with_id(
        name: &str,
        data_type: DataType,
        nullable: bool,
        id: i64,
    ) -> StructField {
        StructField::new(name, data_type, nullable).add_metadata([
            ("delta.columnMapping.id", MetadataValue::Number(id)),
            (
                "delta.columnMapping.physicalName",
                MetadataValue::String(format!("col_{}", id)),
            ),
        ])
    }

    #[test]
    fn test_identical_schemas() {
        let schema = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let diff = SchemaDiff::new(&schema, &schema).unwrap();
        assert!(diff.is_empty());
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_change_count() {
        let before = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let after = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, true, 1), // Changed
            create_field_with_id("email", DataType::STRING, false, 3), // Added
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // 1 removed (name), 1 added (email), 1 updated (id)
        assert_eq!(diff.change_count(), 3);
        assert_eq!(diff.removed_fields.len(), 1);
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.updated_fields.len(), 1);
    }

    #[test]
    fn test_top_level_added_field() {
        let before =
            StructType::new_unchecked([create_field_with_id("id", DataType::LONG, false, 1)]);

        let after = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
        assert_eq!(diff.added_fields[0].path, ColumnName::new(["name"]));
        assert_eq!(diff.added_fields[0].field.name(), "name");
        assert!(diff.has_breaking_changes()); // Adding non-nullable field is breaking
    }

    #[test]
    fn test_added_required_field_is_breaking() {
        // Adding a non-nullable (required) field is breaking
        let before =
            StructType::new_unchecked([create_field_with_id("id", DataType::LONG, false, 1)]);

        let after = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("required_field", DataType::STRING, false, 2), // Non-nullable
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_added_nullable_field_is_not_breaking() {
        // Adding a nullable (optional) field is NOT breaking
        let before =
            StructType::new_unchecked([create_field_with_id("id", DataType::LONG, false, 1)]);

        let after = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("optional_field", DataType::STRING, true, 2), // Nullable
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
        assert!(!diff.has_breaking_changes()); // Not breaking
    }

    #[test]
    fn test_physical_name_validation() {
        // Test: Physical names present and unchanged - valid schema evolution (just a rename)
        let before = StructType::new_unchecked([StructField::new("name", DataType::STRING, false)
            .add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(1)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col_1".to_string()),
                ),
            ])]);
        let after =
            StructType::new_unchecked([StructField::new("full_name", DataType::STRING, false)
                .add_metadata([
                    ("delta.columnMapping.id", MetadataValue::Number(1)),
                    (
                        "delta.columnMapping.physicalName",
                        MetadataValue::String("col_1".to_string()),
                    ),
                ])]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::Renamed]
        );
        assert!(!diff.has_breaking_changes()); // Rename is not breaking

        // Test: Physical name changed - INVALID (returns error)
        let before = StructType::new_unchecked([StructField::new("name", DataType::STRING, false)
            .add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(1)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col_001".to_string()),
                ),
            ])]);
        let after = StructType::new_unchecked([StructField::new("name", DataType::STRING, false)
            .add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(1)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col_002".to_string()),
                ),
            ])]);

        let result = SchemaDiff::new(&before, &after);
        assert!(matches!(
            result,
            Err(SchemaDiffError::PhysicalNameChanged { .. })
        ));

        // Test: Missing physical name in one schema - INVALID (returns error)
        let before = StructType::new_unchecked([StructField::new("name", DataType::STRING, false)
            .add_metadata([
                ("delta.columnMapping.id", MetadataValue::Number(1)),
                (
                    "delta.columnMapping.physicalName",
                    MetadataValue::String("col_1".to_string()),
                ),
            ])]);
        let after = StructType::new_unchecked([StructField::new("name", DataType::STRING, false)
            .add_metadata([("delta.columnMapping.id", MetadataValue::Number(1))])]);

        let result = SchemaDiff::new(&before, &after);
        assert!(matches!(
            result,
            Err(SchemaDiffError::MissingPhysicalName { .. })
        ));
    }

    #[test]
    fn test_multiple_change_types() {
        // Test that a field with multiple simultaneous changes produces FieldChangeType::Multiple
        let before = StructType::new_unchecked([create_field_with_id(
            "user_name",
            DataType::STRING,
            false,
            1,
        )
        .add_metadata([("custom", MetadataValue::String("old_value".to_string()))])]);

        let after = StructType::new_unchecked([
            create_field_with_id("userName", DataType::STRING, true, 1) // Renamed + nullability loosened
                .add_metadata([("custom", MetadataValue::String("new_value".to_string()))]), // Metadata changed
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        let update = &diff.updated_fields[0];

        // Should have 3 changes
        assert_eq!(update.change_types.len(), 3);
        assert!(update.change_types.contains(&FieldChangeType::Renamed));
        assert!(update
            .change_types
            .contains(&FieldChangeType::NullabilityLoosened));
        assert!(update
            .change_types
            .contains(&FieldChangeType::MetadataChanged));

        // Breaking because metadata changed (metadata changes can be unsafe, e.g., row tracking)
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_multiple_with_breaking_change() {
        // Test that Multiple changes are correctly identified as breaking when they contain breaking changes
        let before = StructType::new_unchecked([create_field_with_id(
            "user_name",
            DataType::STRING,
            true,
            1,
        )
        .add_metadata([("custom", MetadataValue::String("old_value".to_string()))])]);

        let after = StructType::new_unchecked([
            create_field_with_id("userName", DataType::STRING, false, 1) // Renamed + nullability TIGHTENED
                .add_metadata([("custom", MetadataValue::String("new_value".to_string()))]), // Metadata changed
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        let update = &diff.updated_fields[0];

        assert_eq!(update.change_types.len(), 3);
        assert!(update.change_types.contains(&FieldChangeType::Renamed));
        assert!(update
            .change_types
            .contains(&FieldChangeType::NullabilityTightened));
        assert!(update
            .change_types
            .contains(&FieldChangeType::MetadataChanged));

        // Breaking because nullability was tightened
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_duplicate_field_id_error() {
        // Test that duplicate field IDs in the same schema produce an error
        let schema_with_duplicates = StructType::new_unchecked([
            create_field_with_id("field1", DataType::STRING, false, 1),
            create_field_with_id("field2", DataType::STRING, false, 1), // Same ID!
        ]);

        let result = SchemaDiff::new(&schema_with_duplicates, &schema_with_duplicates);

        assert!(result.is_err());
        match result {
            Err(SchemaDiffError::DuplicateFieldId { id, path1, path2 }) => {
                assert_eq!(id, 1);
                assert_eq!(path1, ColumnName::new(["field1"]));
                assert_eq!(path2, ColumnName::new(["field2"]));
            }
            _ => panic!("Expected DuplicateFieldId error"),
        }
    }

    #[test]
    fn test_top_level_and_nested_change_filters() {
        // Test that top_level_changes and nested_changes correctly filter by path depth.
        // This test manually constructs a SchemaDiff to exercise the filtering logic.

        let top_level_field = create_field_with_id("name", DataType::STRING, false, 1);
        let nested_field = create_field_with_id("street", DataType::STRING, false, 2);
        let deeply_nested_field = create_field_with_id("city", DataType::STRING, false, 3);

        // Create a diff with mixed top-level and nested changes
        let diff = SchemaDiff {
            added_fields: vec![
                FieldChange {
                    field: top_level_field.clone(),
                    path: ColumnName::new(["name"]), // Top-level (depth 1)
                },
                FieldChange {
                    field: nested_field.clone(),
                    path: ColumnName::new(["address", "street"]), // Nested (depth 2)
                },
            ],
            removed_fields: vec![FieldChange {
                field: deeply_nested_field.clone(),
                path: ColumnName::new(["user", "address", "city"]), // Deeply nested (depth 3)
            }],
            updated_fields: vec![],
            breaking_changes: false,
        };

        // Test top_level_changes - should only return depth 1 fields
        let (top_added, top_removed, top_updated) = diff.top_level_changes();
        assert_eq!(top_added.len(), 1);
        assert_eq!(top_added[0].path, ColumnName::new(["name"]));
        assert_eq!(top_removed.len(), 0);
        assert_eq!(top_updated.len(), 0);

        // Test nested_changes - should only return depth > 1 fields
        let (nested_added, nested_removed, nested_updated) = diff.nested_changes();
        assert_eq!(nested_added.len(), 1);
        assert_eq!(nested_added[0].path, ColumnName::new(["address", "street"]));
        assert_eq!(nested_removed.len(), 1);
        assert_eq!(
            nested_removed[0].path,
            ColumnName::new(["user", "address", "city"])
        );
        assert_eq!(nested_updated.len(), 0);
    }

    #[test]
    fn test_ancestor_filtering() {
        // Test that when a parent struct is added/removed, its children aren't reported separately
        let without_user =
            StructType::new_unchecked([create_field_with_id("id", DataType::LONG, false, 1)]);

        let with_user = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id(
                "user",
                DataType::try_struct_type([
                    create_field_with_id("name", DataType::STRING, false, 3),
                    create_field_with_id("email", DataType::STRING, true, 4),
                    create_field_with_id(
                        "address",
                        DataType::try_struct_type([
                            create_field_with_id("street", DataType::STRING, false, 6),
                            create_field_with_id("city", DataType::STRING, false, 7),
                        ])
                        .unwrap(),
                        true,
                        5,
                    ),
                ])
                .unwrap(),
                false,
                2,
            ),
        ]);

        // CASE 1: Adding a parent struct - only parent should be reported, not nested fields
        let diff = SchemaDiff::new(&without_user, &with_user).unwrap();

        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.added_fields[0].path, ColumnName::new(["user"]));
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
        // The filtered paths would have been: user.name, user.email, user.address, user.address.street, user.address.city
        assert!(diff.has_breaking_changes()); // Adding non-nullable struct field is breaking

        // CASE 2: Removing a parent struct - only parent should be reported, not nested fields
        let diff = SchemaDiff::new(&with_user, &without_user).unwrap();

        assert_eq!(diff.removed_fields.len(), 1);
        assert_eq!(diff.removed_fields[0].path, ColumnName::new(["user"]));
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);
        assert!(!diff.has_breaking_changes()); // Removing fields is safe
    }

    #[test]
    fn test_container_with_nested_changes_not_reported_as_type_change() {
        // Test that when a struct's nested fields change, the struct itself isn't reported as TypeChanged
        let before = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([
                create_field_with_id("name", DataType::STRING, false, 2),
                create_field_with_id("email", DataType::STRING, true, 3),
            ])
            .unwrap(),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([
                create_field_with_id("full_name", DataType::STRING, false, 2), // Renamed
                create_field_with_id("email", DataType::STRING, true, 3),
                create_field_with_id("age", DataType::INTEGER, true, 4), // Added
            ])
            .unwrap(),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // Should see the nested field changes but NOT a type change on the parent struct
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.added_fields[0].path, ColumnName::new(["user", "age"]));
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(
            diff.updated_fields[0].path,
            ColumnName::new(["user", "full_name"])
        );
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::Renamed]
        );

        // Crucially, there should be NO update reported for the "user" field itself
        // even though its DataType::Struct contains different nested fields
        let top_level_updates: Vec<_> = diff
            .updated_fields
            .iter()
            .filter(|u| u.path.path().len() == 1)
            .collect();
        assert_eq!(top_level_updates.len(), 0);

        // Not a breaking change since it's just a rename and an added nullable field
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_actual_struct_type_change_still_reported() {
        // Test that actual type changes (not just nested content changes) are still reported
        let before =
            StructType::new_unchecked([create_field_with_id("data", DataType::STRING, false, 1)]);

        let after = StructType::new_unchecked([
            create_field_with_id(
                "data",
                DataType::try_struct_type([create_field_with_id(
                    "nested",
                    DataType::STRING,
                    false,
                    2,
                )])
                .unwrap(),
                false,
                1,
            ), // Changed from STRING to STRUCT
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // This IS a real type change from primitive to struct
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["data"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::TypeChanged]
        );
        assert!(diff.has_breaking_changes());

        // The new nested field should also be reported as added
        assert_eq!(
            diff.added_fields[0].path,
            ColumnName::new(["data", "nested"])
        );
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_array_with_struct_element_changes() {
        // Test that array containers aren't reported as changed when their struct elements change
        let before = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::try_struct_type([create_field_with_id(
                    "name",
                    DataType::STRING,
                    false,
                    2,
                )])
                .unwrap(),
                true,
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::try_struct_type([
                    create_field_with_id("title", DataType::STRING, false, 2), // Renamed
                ])
                .unwrap(),
                true,
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // Should only see the nested field rename, not a change to the array container
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(
            diff.updated_fields[0].path,
            ColumnName::new(["items", "element", "title"])
        );
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::Renamed]
        );

        // No change should be reported for the "items" array itself
        let array_updates: Vec<_> = diff
            .updated_fields
            .iter()
            .filter(|u| u.path == ColumnName::new(["items"]))
            .collect();
        assert_eq!(array_updates.len(), 0);
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_ancestor_filtering_with_mixed_changes() {
        let before = StructType::new_unchecked([
            create_field_with_id("existing", DataType::STRING, false, 1),
            create_field_with_id(
                "existing_struct",
                DataType::try_struct_type([create_field_with_id(
                    "old_name",
                    DataType::STRING,
                    false,
                    3,
                )])
                .unwrap(),
                false,
                2,
            ),
        ]);

        let after = StructType::new_unchecked([
            create_field_with_id("existing", DataType::STRING, true, 1), // Changed nullability
            create_field_with_id(
                "existing_struct",
                DataType::try_struct_type([
                    create_field_with_id("new_name", DataType::STRING, false, 3), // Renamed
                ])
                .unwrap(),
                true, // Changed nullability
                2,
            ),
            create_field_with_id(
                "new_struct", // Completely new struct
                DataType::try_struct_type([create_field_with_id(
                    "nested_field",
                    DataType::INTEGER,
                    false,
                    5,
                )])
                .unwrap(),
                false,
                4,
            ),
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // Should see: existing changed, existing_struct changed, existing_struct.old_name->new_name renamed, new_struct added
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.added_fields[0].path, ColumnName::new(["new_struct"]));

        assert_eq!(diff.updated_fields.len(), 3);
        let paths: HashSet<ColumnName> =
            diff.updated_fields.iter().map(|u| u.path.clone()).collect();
        assert!(paths.contains(&ColumnName::new(["existing"])));
        assert!(paths.contains(&ColumnName::new(["existing_struct"])));
        assert!(paths.contains(&ColumnName::new(["existing_struct", "new_name"])));

        // Added a non-nullable struct "new_struct"
        assert!(diff.has_breaking_changes());

        // nested_field should NOT appear as added since new_struct is its ancestor
    }

    #[test]
    fn test_nested_field_rename() {
        let before = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([create_field_with_id("name", DataType::STRING, false, 2)])
                .unwrap(),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([
                create_field_with_id("full_name", DataType::STRING, false, 2), // Renamed!
            ])
            .unwrap(),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);

        let update = &diff.updated_fields[0];
        assert_eq!(update.path, ColumnName::new(["user", "full_name"]));
        assert_eq!(update.change_types, vec![FieldChangeType::Renamed]);
        assert!(!diff.has_breaking_changes()); // Rename is not breaking
    }

    #[test]
    fn test_nested_field_added() {
        let before = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([create_field_with_id("name", DataType::STRING, false, 2)])
                .unwrap(),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "user",
            DataType::try_struct_type([
                create_field_with_id("name", DataType::STRING, false, 2),
                create_field_with_id("age", DataType::INTEGER, true, 3), // Added!
            ])
            .unwrap(),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 0);

        let added = &diff.added_fields[0];
        assert_eq!(added.path, ColumnName::new(["user", "age"]));
        assert_eq!(added.field.name(), "age");
        assert!(!diff.has_breaking_changes()); // Adding nullable field is not breaking
    }

    #[test]
    fn test_deeply_nested_changes() {
        let before = StructType::new_unchecked([create_field_with_id(
            "level1",
            DataType::try_struct_type([create_field_with_id(
                "level2",
                DataType::try_struct_type([create_field_with_id(
                    "deep_field",
                    DataType::STRING,
                    false,
                    3,
                )])
                .unwrap(),
                false,
                2,
            )])
            .unwrap(),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "level1",
            DataType::try_struct_type([create_field_with_id(
                "level2",
                DataType::try_struct_type([
                    create_field_with_id("very_deep_field", DataType::STRING, false, 3), // Renamed!
                ])
                .unwrap(),
                false,
                2,
            )])
            .unwrap(),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);

        let update = &diff.updated_fields[0];
        assert_eq!(
            update.path,
            ColumnName::new(["level1", "level2", "very_deep_field"])
        );
        assert_eq!(update.change_types, vec![FieldChangeType::Renamed]);
    }

    #[test]
    fn test_top_level_vs_nested_filtering() {
        let before = StructType::new_unchecked([
            create_field_with_id("top_field", DataType::STRING, false, 1),
            create_field_with_id(
                "user",
                DataType::try_struct_type([create_field_with_id(
                    "name",
                    DataType::STRING,
                    false,
                    3,
                )])
                .unwrap(),
                false,
                2,
            ),
        ]);

        let after = StructType::new_unchecked([
            create_field_with_id("renamed_top", DataType::STRING, false, 1), // Renamed top-level
            create_field_with_id(
                "user",
                DataType::try_struct_type([
                    create_field_with_id("full_name", DataType::STRING, false, 3), // Renamed nested
                    create_field_with_id("age", DataType::INTEGER, true, 4),       // Added nested
                ])
                .unwrap(),
                false,
                2,
            ),
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        let (top_added, _, top_updated) = diff.top_level_changes();
        let (nested_added, _, nested_updated) = diff.nested_changes();

        assert_eq!(top_added.len(), 0);
        assert_eq!(top_updated.len(), 1);
        assert_eq!(top_updated[0].path, ColumnName::new(["renamed_top"]));

        assert_eq!(nested_added.len(), 1);
        assert_eq!(nested_added[0].path, ColumnName::new(["user", "age"]));
        assert_eq!(nested_updated.len(), 1);
        assert_eq!(
            nested_updated[0].path,
            ColumnName::new(["user", "full_name"])
        );
    }

    #[test]
    fn test_mixed_changes() {
        let before = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id(
                "user",
                DataType::try_struct_type([
                    create_field_with_id("name", DataType::STRING, false, 3),
                    create_field_with_id("email", DataType::STRING, true, 4),
                ])
                .unwrap(),
                false,
                2,
            ),
        ]);

        let after = StructType::new_unchecked([
            create_field_with_id("identifier", DataType::LONG, false, 1), // Renamed top-level
            create_field_with_id(
                "user",
                DataType::try_struct_type([
                    create_field_with_id("full_name", DataType::STRING, false, 3), // Renamed nested
                    // email removed (id=4)
                    create_field_with_id("age", DataType::INTEGER, true, 5), // Added nested
                ])
                .unwrap(),
                false,
                2,
            ),
            create_field_with_id("created_at", DataType::TIMESTAMP, false, 6), // Added top-level
        ]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // Check totals
        assert_eq!(diff.added_fields.len(), 2);
        assert_eq!(diff.removed_fields.len(), 1);
        assert_eq!(diff.updated_fields.len(), 2);

        // Check specific changes
        let added_paths: HashSet<ColumnName> =
            diff.added_fields.iter().map(|f| f.path.clone()).collect();
        assert!(added_paths.contains(&ColumnName::new(["user", "age"])));
        assert!(added_paths.contains(&ColumnName::new(["created_at"])));

        let removed_paths: HashSet<ColumnName> =
            diff.removed_fields.iter().map(|f| f.path.clone()).collect();
        assert!(removed_paths.contains(&ColumnName::new(["user", "email"])));

        let updated_paths: HashSet<ColumnName> =
            diff.updated_fields.iter().map(|f| f.path.clone()).collect();
        assert!(updated_paths.contains(&ColumnName::new(["identifier"])));
        assert!(updated_paths.contains(&ColumnName::new(["user", "full_name"])));
    }
    #[test]
    fn test_array_element_struct_field_changes() {
        let before = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::try_struct_type([
                    create_field_with_id("name", DataType::STRING, false, 2),
                    create_field_with_id("removed_field", DataType::INTEGER, true, 3),
                ])
                .unwrap(),
                true,
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(
                DataType::try_struct_type([
                    create_field_with_id("title", DataType::STRING, false, 2), // Renamed!
                    create_field_with_id("added_field", DataType::STRING, true, 4), // Added!
                ])
                .unwrap(),
                true,
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.removed_fields.len(), 1);
        assert_eq!(diff.updated_fields.len(), 1);

        // Check added field
        assert_eq!(
            diff.added_fields[0].path,
            ColumnName::new(["items", "element", "added_field"])
        );

        // Check removed field
        assert_eq!(
            diff.removed_fields[0].path,
            ColumnName::new(["items", "element", "removed_field"])
        );

        // Check updated field (rename)
        let update = &diff.updated_fields[0];
        assert_eq!(update.path, ColumnName::new(["items", "element", "title"]));
        assert_eq!(update.change_types, vec![FieldChangeType::Renamed]);

        assert!(!diff.has_breaking_changes()); // Removal is safe, rename is safe
    }

    #[test]
    fn test_doubly_nested_array_type_change() {
        // Test that we can detect type changes in doubly nested arrays: array<array<int>> -> array<array<double>>
        let before = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                false,
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::DOUBLE, false))),
                false,
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        // The entire field should be reported as TypeChanged since we can't recurse into
        // non-struct array elements (no field IDs at intermediate levels)
        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["matrix"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::TypeChanged]
        );

        assert!(diff.has_breaking_changes()); // Type change is breaking
    }

    #[test]
    fn test_nested_array_nullability_loosened() {
        // Test: array<array<int> not null> -> array<array<int>>
        // Outer array element nullability loosened (safe change)
        let before = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                false, // Outer array elements are non-nullable
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                true, // Outer array elements now nullable
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["matrix"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityLoosened]
        );
        assert!(!diff.has_breaking_changes()); // Loosening is safe
    }

    #[test]
    fn test_nested_array_nullability_tightened() {
        // Test: array<array<int>> -> array<array<int> not null>
        // Outer array element nullability tightened (breaking change)
        let before = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                true, // Outer array elements are nullable
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))),
                false, // Outer array elements now non-nullable
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["matrix"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityTightened]
        );
        assert!(diff.has_breaking_changes()); // Tightening is breaking
    }

    #[test]
    fn test_nested_array_inner_nullability_loosened() {
        // Test: array<array<int not null>> -> array<array<int>>
        // Inner array element nullability loosened (safe change)
        let before = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))), // Inner elements non-nullable
                false,
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))), // Inner elements now nullable
                false,
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["matrix"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityLoosened]
        );
        assert!(!diff.has_breaking_changes()); // Loosening is safe
    }

    #[test]
    fn test_nested_array_inner_nullability_tightened() {
        // Test: array<array<int>> -> array<array<int not null>>
        // Inner array element nullability tightened (breaking change)
        let before = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))), // Inner elements nullable
                false,
            ))),
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "matrix",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, false))), // Inner elements now non-nullable
                false,
            ))),
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(diff.updated_fields[0].path, ColumnName::new(["matrix"]));
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityTightened]
        );
        assert!(diff.has_breaking_changes()); // Tightening is breaking
    }

    #[test]
    fn test_array_nullability_loosened() {
        let before = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))), // Non-nullable elements
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))), // Nullable elements now
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityLoosened]
        );
        assert!(!diff.has_breaking_changes()); // Loosening is safe
    }

    #[test]
    fn test_array_nullability_tightened() {
        let before = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, true))), // Nullable elements
            false,
            1,
        )]);

        let after = StructType::new_unchecked([create_field_with_id(
            "items",
            DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))), // Non-nullable now
            false,
            1,
        )]);

        let diff = SchemaDiff::new(&before, &after).unwrap();

        assert_eq!(diff.added_fields.len(), 0);
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.updated_fields.len(), 1);
        assert_eq!(
            diff.updated_fields[0].change_types,
            vec![FieldChangeType::ContainerNullabilityTightened]
        );
        assert!(diff.has_breaking_changes()); // Tightening is breaking
    }

    #[test]
    fn test_top_level_and_nested_change_filters() {
        // Test that top_level_changes and nested_changes correctly filter by path depth.
        // This test manually constructs a SchemaDiff to exercise the filtering logic.

        let top_level_field = create_field_with_id("name", DataType::STRING, false, 1);
        let nested_field = create_field_with_id("street", DataType::STRING, false, 2);
        let deeply_nested_field = create_field_with_id("city", DataType::STRING, false, 3);

        // Create a diff with mixed top-level and nested changes
        let diff = SchemaDiff {
            added_fields: vec![
                FieldChange {
                    field: top_level_field.clone(),
                    path: ColumnName::new(["name"]), // Top-level (depth 1)
                },
                FieldChange {
                    field: nested_field.clone(),
                    path: ColumnName::new(["address", "street"]), // Nested (depth 2)
                },
            ],
            removed_fields: vec![FieldChange {
                field: deeply_nested_field.clone(),
                path: ColumnName::new(["user", "address", "city"]), // Deeply nested (depth 3)
            }],
            updated_fields: vec![],
            has_breaking_changes: false,
        };

        // Test top_level_changes - should only return depth 1 fields
        let (top_added, top_removed, top_updated) = diff.top_level_changes();
        assert_eq!(top_added.len(), 1);
        assert_eq!(top_added[0].path, ColumnName::new(["name"]));
        assert_eq!(top_removed.len(), 0);
        assert_eq!(top_updated.len(), 0);

        // Test nested_changes - should only return depth > 1 fields
        let (nested_added, nested_removed, nested_updated) = diff.nested_changes();
        assert_eq!(nested_added.len(), 1);
        assert_eq!(nested_added[0].path, ColumnName::new(["address", "street"]));
        assert_eq!(nested_removed.len(), 1);
        assert_eq!(
            nested_removed[0].path,
            ColumnName::new(["user", "address", "city"])
        );
        assert_eq!(nested_updated.len(), 0);
    }
}
