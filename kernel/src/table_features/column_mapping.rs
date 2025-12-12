//! Code to handle column mapping, including modes and schema transforms
use super::TableFeature;
use crate::actions::Protocol;
use crate::schema::{
    ArrayType, ColumnName, DataType, MapType, MetadataValue, Schema, SchemaTransform, StructField,
    StructType,
};
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

use std::borrow::Cow;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::EnumString;
use uuid::Uuid;

/// Table property key for column mapping mode
pub(crate) const COLUMN_MAPPING_MODE_KEY: &str = "delta.columnMapping.mode";

/// Table property key for tracking the maximum column ID assigned
pub(crate) const COLUMN_MAPPING_MAX_COLUMN_ID_KEY: &str = "delta.columnMapping.maxColumnId";

/// Metadata key for the column mapping ID on a field
pub(crate) const COLUMN_MAPPING_ID_KEY: &str = "delta.columnMapping.id";

/// Metadata key for the physical name on a field
pub(crate) const COLUMN_MAPPING_PHYSICAL_NAME_KEY: &str = "delta.columnMapping.physicalName";

/// Modes of column mapping a table can be in
#[derive(Debug, EnumString, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    Name,
}

/// Determine the column mapping mode for a table based on the [`Protocol`] and [`TableProperties`]
pub(crate) fn column_mapping_mode(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> ColumnMappingMode {
    match (
        table_properties.column_mapping_mode,
        protocol.min_reader_version(),
    ) {
        // NOTE: The table property is optional even when the feature is supported, and is allowed
        // (but should be ignored) even when the feature is not supported. For details see
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping
        (Some(mode), 2) => mode,
        (Some(mode), 3) if protocol.has_table_feature(&TableFeature::ColumnMapping) => mode,
        _ => ColumnMappingMode::None,
    }
}

/// When column mapping mode is enabled, verify that each field in the schema is annotated with a
/// physical name and field_id; when not enabled, verify that no fields are annotated.
pub fn validate_schema_column_mapping(schema: &Schema, mode: ColumnMappingMode) -> DeltaResult<()> {
    let mut validator = ValidateColumnMappings {
        mode,
        path: vec![],
        err: None,
    };
    let _ = validator.transform_struct(schema);
    match validator.err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

struct ValidateColumnMappings<'a> {
    mode: ColumnMappingMode,
    path: Vec<&'a str>,
    err: Option<Error>,
}

impl<'a> ValidateColumnMappings<'a> {
    fn transform_inner_type(
        &mut self,
        data_type: &'a DataType,
        name: &'a str,
    ) -> Option<Cow<'a, DataType>> {
        if self.err.is_none() {
            self.path.push(name);
            let _ = self.transform(data_type);
            self.path.pop();
        }
        None
    }
    fn check_annotations(&mut self, field: &StructField) {
        // The iterator yields `&&str` but `ColumnName::new` needs `&str`
        let column_name = || ColumnName::new(self.path.iter().copied());
        let annotation = "delta.columnMapping.physicalName";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a physical name annotation; None mode forbids it.
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::String(_))) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "The {annotation} annotation on field '{}' must be a string",
                    column_name()
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                    column_name()
                )));
            }
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{annotation}' is annotated with {}",
                    column_name()
                )));
            }
        }

        let annotation = "delta.columnMapping.id";
        match (self.mode, field.metadata.get(annotation)) {
            // Both Id and Name modes require a field ID annotation; None mode forbids it.
            (ColumnMappingMode::None, None) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::Number(_))) => {}
            (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "The {annotation} annotation on field '{}' must be a number",
                    column_name()
                )));
            }
            (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                    column_name()
                )));
            }
            (ColumnMappingMode::None, Some(_)) => {
                self.err = Some(Error::invalid_column_mapping_mode(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                    column_name()
                )));
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for ValidateColumnMappings<'a> {
    // Override array element and map key/value for better error messages
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(etype, "<array element>")
    }
    fn transform_map_key(&mut self, ktype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(ktype, "<map key>")
    }
    fn transform_map_value(&mut self, vtype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner_type(vtype, "<map value>")
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if self.err.is_none() {
            self.path.push(&field.name);
            self.check_annotations(field);
            let _ = self.recurse_into_struct_field(field);
            self.path.pop();
        }
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        // don't recurse into variant's fields, as they are not expected to have column mapping
        // annotations
        // TODO: this changes with icebergcompat right? see issue#1125 for icebergcompat.
        None
    }
}

// ============================================================================
// Write-side column mapping functions
// ============================================================================

/// Get the column mapping mode from a table properties map.
///
/// This is used during table creation when we have raw properties from the builder,
/// not yet converted to [`TableProperties`].
pub(crate) fn get_column_mapping_mode_from_properties(
    properties: &HashMap<String, String>,
) -> DeltaResult<ColumnMappingMode> {
    match properties.get(COLUMN_MAPPING_MODE_KEY) {
        Some(mode_str) => mode_str.parse::<ColumnMappingMode>().map_err(|_| {
            Error::generic(format!(
                "Invalid column mapping mode '{}'. Must be one of: none, name, id",
                mode_str
            ))
        }),
        None => Ok(ColumnMappingMode::None),
    }
}

/// Find the maximum column mapping ID in a schema.
///
/// This scans all fields (including nested fields) to find the highest
/// `delta.columnMapping.id` value. Returns 0 if no IDs are found.
///
/// Note: This is primarily used for testing. Production code uses
/// `assign_column_mapping_metadata` which tracks max ID during assignment.
#[cfg(test)]
fn find_max_column_id(schema: &StructType) -> i64 {
    let mut max_id: i64 = 0;
    find_max_column_id_in_fields(schema.fields(), &mut max_id);
    max_id
}

#[cfg(test)]
fn find_max_column_id_in_fields<'a>(
    fields: impl Iterator<Item = &'a StructField>,
    max_id: &mut i64,
) {
    for field in fields {
        // Check this field's ID
        if let Some(MetadataValue::Number(id)) = field.metadata.get(COLUMN_MAPPING_ID_KEY) {
            *max_id = (*max_id).max(*id);
        }

        // Recurse into nested types
        match &field.data_type {
            DataType::Struct(inner) => {
                find_max_column_id_in_fields(inner.fields(), max_id);
            }
            DataType::Array(array_type) => {
                if let DataType::Struct(inner) = array_type.element_type() {
                    find_max_column_id_in_fields(inner.fields(), max_id);
                }
            }
            DataType::Map(map_type) => {
                if let DataType::Struct(inner) = map_type.key_type() {
                    find_max_column_id_in_fields(inner.fields(), max_id);
                }
                if let DataType::Struct(inner) = map_type.value_type() {
                    find_max_column_id_in_fields(inner.fields(), max_id);
                }
            }
            _ => {}
        }
    }
}

/// Assigns column mapping metadata (ID and physical name) to all fields in a schema.
///
/// This is called when creating a new table with column mapping enabled. Each field
/// that doesn't already have column mapping metadata will be assigned:
/// - A unique `delta.columnMapping.id` (incrementing from max_id)
/// - A unique `delta.columnMapping.physicalName` (format: `col-{uuid}`)
///
/// # Arguments
///
/// * `schema` - The schema to process
/// * `max_id` - The current maximum column ID (will be updated as IDs are assigned)
///
/// # Returns
///
/// A new schema with column mapping metadata assigned to all fields.
pub(crate) fn assign_column_mapping_metadata(
    schema: &StructType,
    max_id: &mut i64,
) -> DeltaResult<StructType> {
    let new_fields: Vec<StructField> = schema
        .fields()
        .map(|field| assign_field_column_mapping(field, max_id))
        .collect::<DeltaResult<Vec<_>>>()?;

    StructType::try_new(new_fields)
}

/// Assigns column mapping metadata to a single field, recursively processing nested types.
///
/// If the field already has column mapping metadata, updates `max_id` to track the highest
/// ID seen. If the field doesn't have metadata, assigns a new ID (incrementing `max_id`).
fn assign_field_column_mapping(field: &StructField, max_id: &mut i64) -> DeltaResult<StructField> {
    let has_id = field.metadata.contains_key(COLUMN_MAPPING_ID_KEY);
    let has_physical_name = field.metadata.contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY);

    // Validate: if one is present, both must be present
    if has_id != has_physical_name {
        return Err(Error::generic(format!(
            "Field '{}' has incomplete column mapping metadata. \
             Both delta.columnMapping.id and delta.columnMapping.physicalName must be present if one is present.",
            field.name
        )));
    }

    // Start with the existing field
    let mut new_field = field.clone();

    if has_id {
        // Field already has an ID - update max_id to track the highest seen
        if let Some(MetadataValue::Number(existing_id)) =
            field.metadata.get(COLUMN_MAPPING_ID_KEY)
        {
            *max_id = (*max_id).max(*existing_id);
        }
    } else {
        // Assign new ID
        *max_id += 1;
        new_field.metadata.insert(
            COLUMN_MAPPING_ID_KEY.to_string(),
            MetadataValue::Number(*max_id),
        );
    }

    // Assign physical name if missing
    if !has_physical_name {
        let physical_name = format!("col-{}", Uuid::new_v4());
        new_field.metadata.insert(
            COLUMN_MAPPING_PHYSICAL_NAME_KEY.to_string(),
            MetadataValue::String(physical_name),
        );
    }

    // Recursively process nested types
    new_field.data_type = process_nested_data_type(&field.data_type, max_id)?;

    Ok(new_field)
}

/// Process nested data types to assign column mapping metadata to any nested struct fields.
fn process_nested_data_type(data_type: &DataType, max_id: &mut i64) -> DeltaResult<DataType> {
    match data_type {
        DataType::Struct(inner) => {
            let new_inner = assign_column_mapping_metadata(inner, max_id)?;
            Ok(DataType::Struct(Box::new(new_inner)))
        }
        DataType::Array(array_type) => {
            let new_element_type =
                process_nested_data_type(array_type.element_type(), max_id)?;
            Ok(DataType::Array(Box::new(ArrayType::new(
                new_element_type,
                array_type.contains_null(),
            ))))
        }
        DataType::Map(map_type) => {
            let new_key_type = process_nested_data_type(map_type.key_type(), max_id)?;
            let new_value_type = process_nested_data_type(map_type.value_type(), max_id)?;
            Ok(DataType::Map(Box::new(MapType::new(
                new_key_type,
                new_value_type,
                map_type.value_contains_null(),
            ))))
        }
        // Primitive and Variant types don't have nested fields that need column mapping
        _ => Ok(data_type.clone()),
    }
}

/// Get the physical name from a field's metadata, falling back to the logical name.
pub(crate) fn get_physical_name(field: &StructField) -> &str {
    match field.metadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
        Some(MetadataValue::String(name)) => name.as_str(),
        _ => &field.name,
    }
}

/// Get the column mapping ID from a field's metadata.
#[cfg(test)]
pub(crate) fn get_column_id(field: &StructField) -> Option<i64> {
    match field.metadata.get(COLUMN_MAPPING_ID_KEY) {
        Some(MetadataValue::Number(id)) => Some(*id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::StructType;
    use std::collections::HashMap;

    #[test]
    fn test_column_mapping_mode() {
        let table_properties: HashMap<_, _> =
            [("delta.columnMapping.mode".to_string(), "id".to_string())]
                .into_iter()
                .collect();
        let table_properties = TableProperties::from(table_properties.iter());
        let empty_table_properties = TableProperties::from([] as [(String, String); 0]);

        let protocol = Protocol::try_new(2, 5, None::<Vec<String>>, None::<Vec<String>>).unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let empty_features = Some::<[String; 0]>([]);
        let protocol =
            Protocol::try_new(3, 7, empty_features.clone(), empty_features.clone()).unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::ColumnMapping]),
            Some([TableFeature::ColumnMapping]),
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors]),
            Some([TableFeature::DeletionVectors]),
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
            Some([TableFeature::DeletionVectors, TableFeature::ColumnMapping]),
        )
        .unwrap();

        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        assert_eq!(
            column_mapping_mode(&protocol, &empty_table_properties),
            ColumnMappingMode::None
        );
    }

    // Creates optional schema field annotations for column mapping id and physical name, as a string.
    fn create_annotations<'a>(
        id: impl Into<Option<&'a str>>,
        name: impl Into<Option<&'a str>>,
    ) -> String {
        let mut annotations = vec![];
        if let Some(id) = id.into() {
            annotations.push(format!("\"delta.columnMapping.id\": {id}"));
        }
        if let Some(name) = name.into() {
            annotations.push(format!("\"delta.columnMapping.physicalName\": {name}"));
        }
        annotations.join(", ")
    }

    // Creates a generic schema with optional field annotations for column mapping id and physical name.
    fn create_schema<'a>(
        inner_id: impl Into<Option<&'a str>>,
        inner_name: impl Into<Option<&'a str>>,
        outer_id: impl Into<Option<&'a str>>,
        outer_name: impl Into<Option<&'a str>>,
    ) -> StructType {
        let schema = format!(
            r#"
        {{
            "name": "e",
            "type": {{
                "type": "array",
                "elementType": {{
                    "type": "struct",
                    "fields": [
                        {{
                            "name": "d",
                            "type": "integer",
                            "nullable": false,
                            "metadata": {{ {} }}
                        }}
                    ]
                }},
                "containsNull": true
            }},
            "nullable": true,
            "metadata": {{ {} }}
        }}
        "#,
            create_annotations(inner_id, inner_name),
            create_annotations(outer_id, outer_name)
        );
        println!("{schema}");
        StructType::new_unchecked([serde_json::from_str(&schema).unwrap()])
    }

    #[test]
    fn test_column_mapping_enabled() {
        [ColumnMappingMode::Name, ColumnMappingMode::Id]
            .into_iter()
            .for_each(|mode| {
                let schema = create_schema("5", "\"col-a7f4159c\"", "4", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).unwrap();

                // missing annotation
                let schema = create_schema(None, "\"col-a7f4159c\"", "4", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("missing field id");
                let schema = create_schema("5", None, "4", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("missing field name");
                let schema = create_schema("5", "\"col-a7f4159c\"", None, "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("missing field id");
                let schema = create_schema("5", "\"col-a7f4159c\"", "4", None);
                validate_schema_column_mapping(&schema, mode).expect_err("missing field name");

                // wrong-type field id annotation (string instead of int)
                let schema = create_schema("\"5\"", "\"col-a7f4159c\"", "4", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("invalid field id");
                let schema = create_schema("5", "\"col-a7f4159c\"", "\"4\"", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("invalid field id");

                // wrong-type field name annotation (int instead of string)
                let schema = create_schema("5", "555", "4", "\"col-5f422f40\"");
                validate_schema_column_mapping(&schema, mode).expect_err("invalid field name");
                let schema = create_schema("5", "\"col-a7f4159c\"", "4", "444");
                validate_schema_column_mapping(&schema, mode).expect_err("invalid field name");
            });
    }

    #[test]
    fn test_column_mapping_disabled() {
        let schema = create_schema(None, None, None, None);
        validate_schema_column_mapping(&schema, ColumnMappingMode::None).unwrap();

        let schema = create_schema("5", None, None, None);
        validate_schema_column_mapping(&schema, ColumnMappingMode::None).expect_err("field id");
        let schema = create_schema(None, "\"col-a7f4159c\"", None, None);
        validate_schema_column_mapping(&schema, ColumnMappingMode::None).expect_err("field name");
        let schema = create_schema(None, None, "4", None);
        validate_schema_column_mapping(&schema, ColumnMappingMode::None).expect_err("field id");
        let schema = create_schema(None, None, None, "\"col-5f422f40\"");
        validate_schema_column_mapping(&schema, ColumnMappingMode::None).expect_err("field name");
    }

    // ========================================================================
    // Tests for write-side column mapping functions
    // ========================================================================

    #[test]
    fn test_get_column_mapping_mode_from_properties() {
        let mut props = HashMap::new();

        // No mode property -> None
        assert_eq!(
            get_column_mapping_mode_from_properties(&props).unwrap(),
            ColumnMappingMode::None
        );

        // Explicit none
        props.insert("delta.columnMapping.mode".to_string(), "none".to_string());
        assert_eq!(
            get_column_mapping_mode_from_properties(&props).unwrap(),
            ColumnMappingMode::None
        );

        // Name mode
        props.insert("delta.columnMapping.mode".to_string(), "name".to_string());
        assert_eq!(
            get_column_mapping_mode_from_properties(&props).unwrap(),
            ColumnMappingMode::Name
        );

        // Id mode
        props.insert("delta.columnMapping.mode".to_string(), "id".to_string());
        assert_eq!(
            get_column_mapping_mode_from_properties(&props).unwrap(),
            ColumnMappingMode::Id
        );

        // Invalid mode
        props.insert("delta.columnMapping.mode".to_string(), "invalid".to_string());
        assert!(get_column_mapping_mode_from_properties(&props).is_err());
    }

    #[test]
    fn test_find_max_column_id_empty_schema() {
        let schema = StructType::new_unchecked([]);
        assert_eq!(find_max_column_id(&schema), 0);
    }

    #[test]
    fn test_find_max_column_id_no_ids() {
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::STRING, true),
        ]);
        assert_eq!(find_max_column_id(&schema), 0);
    }

    #[test]
    fn test_find_max_column_id_with_ids() {
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false).add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(5)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("col-a".to_string()),
                ),
            ]),
            StructField::new("b", DataType::STRING, true).add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(10)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("col-b".to_string()),
                ),
            ]),
        ]);
        assert_eq!(find_max_column_id(&schema), 10);
    }

    #[test]
    fn test_find_max_column_id_nested_struct() {
        let inner = StructType::new_unchecked([StructField::new("x", DataType::INTEGER, false)
            .add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(15)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("col-x".to_string()),
                ),
            ])]);

        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false).add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(5)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("col-a".to_string()),
                ),
            ]),
            StructField::new("nested", DataType::Struct(Box::new(inner)), true).add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(10)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("col-nested".to_string()),
                ),
            ]),
        ]);
        assert_eq!(find_max_column_id(&schema), 15);
    }

    #[test]
    fn test_assign_column_mapping_metadata_simple() {
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::STRING, true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Check max_id was incremented
        assert_eq!(max_id, 2);

        // Check each field has column mapping metadata
        for (i, field) in result.fields().enumerate() {
            let expected_id = (i + 1) as i64;
            assert_eq!(
                field.metadata.get(COLUMN_MAPPING_ID_KEY),
                Some(&MetadataValue::Number(expected_id))
            );
            assert!(field
                .metadata
                .contains_key(COLUMN_MAPPING_PHYSICAL_NAME_KEY));

            // Verify physical name format (col-{uuid})
            if let Some(MetadataValue::String(name)) = field.metadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
                assert!(name.starts_with("col-"), "Physical name should start with 'col-'");
            }
        }
    }

    #[test]
    fn test_assign_column_mapping_metadata_preserves_existing() {
        let schema = StructType::new_unchecked([
            // Field with existing column mapping
            StructField::new("a", DataType::INTEGER, false).add_metadata([
                (COLUMN_MAPPING_ID_KEY, MetadataValue::Number(100)),
                (
                    COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                    MetadataValue::String("existing-physical".to_string()),
                ),
            ]),
            // Field without column mapping
            StructField::new("b", DataType::STRING, true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // max_id should be 101: 'a' has ID 100, 'b' gets ID 101
        // (max_id tracks the highest ID seen/assigned to prevent duplicates)
        assert_eq!(max_id, 101);

        // Check field 'a' preserved its existing metadata
        let field_a = result.field("a").unwrap();
        assert_eq!(
            field_a.metadata.get(COLUMN_MAPPING_ID_KEY),
            Some(&MetadataValue::Number(100))
        );
        assert_eq!(
            field_a.metadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY),
            Some(&MetadataValue::String("existing-physical".to_string()))
        );

        // Check field 'b' got new metadata (ID = 101, one more than the existing max of 100)
        let field_b = result.field("b").unwrap();
        assert_eq!(
            field_b.metadata.get(COLUMN_MAPPING_ID_KEY),
            Some(&MetadataValue::Number(101))
        );
    }

    #[test]
    fn test_assign_column_mapping_metadata_nested_struct() {
        let inner = StructType::new_unchecked([
            StructField::new("x", DataType::INTEGER, false),
            StructField::new("y", DataType::STRING, true),
        ]);

        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("nested", DataType::Struct(Box::new(inner)), true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should have 4 IDs: a, nested, x, y
        assert_eq!(max_id, 4);

        // Check outer field 'a'
        let field_a = result.field("a").unwrap();
        assert!(field_a.metadata.contains_key(COLUMN_MAPPING_ID_KEY));

        // Check outer field 'nested'
        let field_nested = result.field("nested").unwrap();
        assert!(field_nested.metadata.contains_key(COLUMN_MAPPING_ID_KEY));

        // Check nested fields
        if let DataType::Struct(inner) = &field_nested.data_type {
            let field_x = inner.field("x").unwrap();
            assert!(field_x.metadata.contains_key(COLUMN_MAPPING_ID_KEY));
            let field_y = inner.field("y").unwrap();
            assert!(field_y.metadata.contains_key(COLUMN_MAPPING_ID_KEY));
        } else {
            panic!("Expected struct type for 'nested' field");
        }
    }

    #[test]
    fn test_assign_column_mapping_incomplete_metadata_fails() {
        // Field with ID but no physical name
        let schema = StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)
            .add_metadata([(COLUMN_MAPPING_ID_KEY, MetadataValue::Number(1))])]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id);
        assert!(result.is_err());

        // Field with physical name but no ID
        let schema = StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)
            .add_metadata([(
                COLUMN_MAPPING_PHYSICAL_NAME_KEY,
                MetadataValue::String("col-a".to_string()),
            )])]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_physical_name() {
        // Field with physical name
        let field = StructField::new("logical", DataType::INTEGER, false).add_metadata([(
            COLUMN_MAPPING_PHYSICAL_NAME_KEY,
            MetadataValue::String("physical-name".to_string()),
        )]);
        assert_eq!(get_physical_name(&field), "physical-name");

        // Field without physical name - falls back to logical name
        let field = StructField::new("logical", DataType::INTEGER, false);
        assert_eq!(get_physical_name(&field), "logical");
    }

    #[test]
    fn test_get_column_id() {
        // Field with ID
        let field = StructField::new("a", DataType::INTEGER, false)
            .add_metadata([(COLUMN_MAPPING_ID_KEY, MetadataValue::Number(42))]);
        assert_eq!(get_column_id(&field), Some(42));

        // Field without ID
        let field = StructField::new("a", DataType::INTEGER, false);
        assert_eq!(get_column_id(&field), None);
    }
}
