//! Schema diffing implementation for Delta Lake schemas
//!
//! This module provides functionality to compute differences between two schemas
//! using field IDs as the primary mechanism for identifying fields across schema versions.
//! Supports nested field comparison within structs, arrays, and maps.

// Allow dead code warnings since this API is not yet used by other modules
#![allow(dead_code)]
// TEMPORARY: Allow unused imports in PR 1 - these will be used when the full implementation is added in PR 2
#![allow(unused_imports)]

use super::{ColumnMetadataKey, ColumnName, DataType, MetadataValue, StructField, StructType};
use std::collections::{HashMap, HashSet};

/// Feature gate for schema diff functionality.
/// Set to `false` to ensure incomplete implementations don't activate until all PRs are merged.
/// Will be removed in the final PR when all tests and implementation are complete.
const SCHEMA_DIFF_ENABLED: bool = false;

/// Arguments for computing a schema diff
#[derive(Debug, Clone)]
pub(crate) struct SchemaDiffArgs<'a> {
    /// The before/original schema
    pub before: &'a StructType,
    /// The after/new schema to compare against
    pub after: &'a StructType,
}

impl<'a> SchemaDiffArgs<'a> {
    /// Compute the difference between the two schemas
    pub(crate) fn compute_diff(self) -> Result<SchemaDiff, SchemaDiffError> {
        compute_schema_diff(self.before, self.after)
    }
}

/// Represents the difference between two schemas
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SchemaDiff {
    /// Fields that were added in the new schema
    pub added_fields: Vec<FieldChange>,
    /// Fields that were removed from the original schema
    pub removed_fields: Vec<FieldChange>,
    /// Fields that were modified between schemas
    pub updated_fields: Vec<FieldUpdate>,
    /// Whether the diff contains breaking changes (computed once during construction)
    has_breaking_changes: bool,
}

/// Represents a field change (added or removed) at any nesting level
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldChange {
    /// The field that was added or removed
    pub field: StructField,
    /// The path to this field (e.g., ColumnName::new(["user", "address", "street"]))
    pub path: ColumnName,
}

/// Represents an update to a field between two schema versions
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldUpdate {
    /// The field as it existed in the original schema
    pub before: StructField,
    /// The field as it exists in the new schema
    pub after: StructField,
    /// The path to this field (e.g., ColumnName::new(["user", "address", "street"]))
    pub path: ColumnName,
    /// The types of changes that occurred (can be multiple, e.g. renamed + nullability changed)
    pub change_types: Vec<FieldChangeType>,
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
        self.has_breaking_changes
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

// TEMPORARY: This is a stub implementation for PR 1 (data structures only).
// Will be replaced with the full implementation in PR 2.
// The full implementation from murali-db/schema-evol will be copied exactly in PR 2.
fn compute_schema_diff(
    _before: &StructType,
    _after: &StructType,
) -> Result<SchemaDiff, SchemaDiffError> {
    // Feature gate check - prevents activation of incomplete implementation in production
    // This gate will be removed in the final PR when all functionality is complete
    // Note: Gate is bypassed in test builds to allow tests to validate the implementation
    #[cfg(not(test))]
    {
        if !SCHEMA_DIFF_ENABLED {
            return Ok(SchemaDiff {
                added_fields: Vec::new(),
                removed_fields: Vec::new(),
                updated_fields: Vec::new(),
                has_breaking_changes: false,
            });
        }
    }

    // Stub implementation - actual implementation will be added in PR 2
    Ok(SchemaDiff {
        added_fields: Vec::new(),
        removed_fields: Vec::new(),
        updated_fields: Vec::new(),
        has_breaking_changes: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

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

        let diff = SchemaDiffArgs {
            before: &schema,
            after: &schema,
        }
        .compute_diff()
        .unwrap();
        assert!(diff.is_empty());
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_change_count() {
        // NOTE: This test uses the stub implementation and will pass trivially in PR 1.
        // In PR 2, when the real compute_schema_diff is added, this test will properly
        // verify the change counting logic.
        let before = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, false, 1),
            create_field_with_id("name", DataType::STRING, false, 2),
        ]);

        let after = StructType::new_unchecked([
            create_field_with_id("id", DataType::LONG, true, 1), // Changed
            create_field_with_id("email", DataType::STRING, false, 3), // Added
        ]);

        let diff = SchemaDiffArgs {
            before: &before,
            after: &after,
        }
        .compute_diff()
        .unwrap();

        // With stub implementation, this will be 0 in PR 1
        // In PR 2, this will correctly be 3 (1 removed, 1 added, 1 updated)
        assert_eq!(diff.change_count(), 0); // TEMPORARY: Will be 3 in PR 2
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
