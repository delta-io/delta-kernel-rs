//! Code to handle column mapping, including modes and schema transforms
//!
//! This module provides:
//! - Read-side: Mode detection and schema validation
//! - Write-side: Schema transformation for assigning IDs and physical names
use super::TableFeature;
use crate::actions::Protocol;
use crate::schema::{
    ArrayType, ColumnMetadataKey, ColumnName, DataType, MapType, MetadataValue, Schema,
    SchemaTransform, StructField, StructType,
};
use crate::table_configuration::TableConfiguration;
use crate::table_properties::{TableProperties, COLUMN_MAPPING_MODE};
use crate::{DeltaResult, Error};

use std::borrow::Cow;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::EnumString;
use uuid::Uuid;

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

/// Validates that the schema annotations are consistent with the column mapping mode.
pub(crate) fn validate_column_mapping(tc: &TableConfiguration) -> DeltaResult<()> {
    validate_schema_column_mapping(&tc.schema(), tc.column_mapping_mode())
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
///
/// Returns `ColumnMappingMode::None` if the property is not set.
pub(crate) fn get_column_mapping_mode_from_properties(
    properties: &HashMap<String, String>,
) -> DeltaResult<ColumnMappingMode> {
    match properties.get(COLUMN_MAPPING_MODE) {
        Some(mode_str) => mode_str.parse::<ColumnMappingMode>().map_err(|_| {
            Error::generic(format!(
                "Invalid column mapping mode '{}'. Must be one of: none, name, id",
                mode_str
            ))
        }),
        None => Ok(ColumnMappingMode::None),
    }
}

/// Assigns column mapping metadata (id and physicalName) to all fields in a schema.
///
/// This function recursively processes all fields in the schema, including nested structs,
/// arrays, and maps. Each field is assigned a new unique ID and physical name.
///
/// Fields with pre-existing column mapping metadata (id or physicalName) are rejected
/// to avoid conflicts. ALTER TABLE will need different handling in the future.
///
/// # Arguments
///
/// * `schema` - The schema to transform
/// * `max_id` - Tracks the highest column ID assigned. Updated in place. Should be initialized
///   to 0 for a new table.
///
/// # Returns
///
/// A new schema with column mapping metadata on all fields.
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
/// Rejects fields with pre-existing column mapping metadata. Otherwise, assigns a new
/// unique ID and physical name (incrementing `max_id`).
fn assign_field_column_mapping(field: &StructField, max_id: &mut i64) -> DeltaResult<StructField> {
    let has_id = field
        .get_config_value(&ColumnMetadataKey::ColumnMappingId)
        .is_some();
    let has_physical_name = field
        .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
        .is_some();

    // For CREATE TABLE, reject any pre-existing column mapping metadata.
    // This avoids conflicts between user-provided IDs/physical names and the ones we assign.
    // ALTER TABLE (adding columns) will need different handling in the future.
    // TODO: Also check for nested column IDs (`delta.columnMapping.nested.ids`) once
    // Iceberg compatibility (IcebergCompatV2+) is supported. See issue #1125.
    if has_id || has_physical_name {
        return Err(Error::generic(format!(
            "Field '{}' already has column mapping metadata. \
             Pre-existing column mapping metadata is not supported for CREATE TABLE.",
            field.name
        )));
    }

    // Start with the existing field and assign new ID
    let mut new_field = field.clone();
    *max_id += 1;
    new_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(*max_id),
    );

    // Assign physical name
    let physical_name = format!("col-{}", Uuid::new_v4());
    new_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingPhysicalName
            .as_ref()
            .to_string(),
        MetadataValue::String(physical_name),
    );

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
            let new_element_type = process_nested_data_type(array_type.element_type(), max_id)?;
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
        // Primitive and Variant types don't contain nested struct fields - return as-is
        DataType::Primitive(_) | DataType::Variant(_) => Ok(data_type.clone()),
    }
}

/// Translates a logical [`ColumnName`] to physical. It can be top level or nested.
///
/// Uses `StructType::walk_column_fields` to walk the column path through nested structs,
/// then maps each field to its physical name based on the column mapping mode.
///
/// Returns an error if the column name cannot be resolved in the schema, or if column mapping is
/// enabled but any field in the path lacks the required
/// [`ColumnMetadataKey::ColumnMappingPhysicalName`] or [`ColumnMetadataKey::ColumnMappingId`]
/// annotations.
#[delta_kernel_derive::internal_api]
pub(crate) fn get_any_level_column_physical_name(
    schema: &StructType,
    col_name: &ColumnName,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<ColumnName> {
    let fields = schema.walk_column_fields(col_name)?;
    let physical_path: Vec<String> = fields
        .iter()
        .map(|field| -> DeltaResult<String> {
            if column_mapping_mode != ColumnMappingMode::None {
                if !field.has_physical_name_annotation() {
                    return Err(Error::Schema(format!(
                        "Column mapping is enabled but field '{}' lacks the {} annotation",
                        field.name,
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()
                    )));
                }
                if !field.has_id_annotation() {
                    return Err(Error::Schema(format!(
                        "Column mapping is enabled but field '{}' lacks the {} annotation",
                        field.name,
                        ColumnMetadataKey::ColumnMappingId.as_ref()
                    )));
                }
            }

            Ok(field.physical_name(column_mapping_mode).to_string())
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    Ok(ColumnName::new(physical_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::ColumnName;
    use crate::schema::{DataType, StructType};
    use crate::utils::test_utils::make_test_tc;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    #[test]
    fn test_column_mapping_mode() {
        let annotated = create_schema("5", "\"col-a7f4159c\"", "4", "\"col-5f422f40\"");
        let plain = create_schema(None, None, None, None);
        let cmm_id = HashMap::from([("delta.columnMapping.mode".to_string(), "id".to_string())]);
        let no_props = HashMap::new();

        // v2 legacy + mode=id => Id (annotated schema required)
        let tc = make_test_tc(
            annotated.clone(),
            Protocol::try_new_legacy(2, 5).unwrap(),
            cmm_id.clone(),
        )
        .unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::Id);

        // v2 legacy + no mode => None
        let tc = make_test_tc(
            plain.clone(),
            Protocol::try_new_legacy(2, 5).unwrap(),
            no_props.clone(),
        )
        .unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + empty features + mode=id => None (mode ignored without CM feature)
        let protocol =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();
        let tc = make_test_tc(plain.clone(), protocol.clone(), cmm_id.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + empty features + no mode => None
        let tc = make_test_tc(plain.clone(), protocol, no_props.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + CM feature + mode=id => Id
        let protocol =
            Protocol::try_new_modern([TableFeature::ColumnMapping], [TableFeature::ColumnMapping])
                .unwrap();
        let tc = make_test_tc(annotated.clone(), protocol.clone(), cmm_id.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::Id);

        // v3 + CM feature + no mode => None
        let tc = make_test_tc(plain.clone(), protocol, no_props.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + DV feature (no CM) + mode=id => None (mode ignored)
        let protocol = Protocol::try_new_modern(
            [TableFeature::DeletionVectors],
            [TableFeature::DeletionVectors],
        )
        .unwrap();
        let tc = make_test_tc(plain.clone(), protocol.clone(), cmm_id.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + DV feature + no mode => None
        let tc = make_test_tc(plain.clone(), protocol, no_props.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);

        // v3 + DV + CM features + mode=id => Id
        let protocol = Protocol::try_new_modern(
            [TableFeature::DeletionVectors, TableFeature::ColumnMapping],
            [TableFeature::DeletionVectors, TableFeature::ColumnMapping],
        )
        .unwrap();
        let tc = make_test_tc(annotated.clone(), protocol.clone(), cmm_id.clone()).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::Id);

        // v3 + DV + CM features + no mode => None
        let tc = make_test_tc(plain.clone(), protocol, no_props).unwrap();
        assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);
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

    // =========================================================================
    // End-to-end TC-level tests for column mapping mode vs protocol vs schema.
    //
    // Column mapping is the only feature with BOTH a multi-valued toggle property
    // (delta.columnMapping.mode) AND schema-level presence (annotations). This
    // combination is not expressible in the generic test battery, so these tests
    // cover the {support mechanism} × {mode} × {annotations} matrix directly.
    // =========================================================================

    /// Helper: build a TC from a schema, protocol, and optional mode property.
    fn make_cm_tc(
        schema: Arc<StructType>,
        protocol: Protocol,
        mode: Option<&str>,
    ) -> crate::DeltaResult<crate::table_configuration::TableConfiguration> {
        use crate::actions::Metadata;
        let props: HashMap<String, String> = mode
            .map(|m| {
                HashMap::from([("delta.columnMapping.mode".to_string(), m.to_string())])
            })
            .unwrap_or_default();
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, props).unwrap();
        let table_root = url::Url::parse("file:///").unwrap();
        crate::table_configuration::TableConfiguration::try_new(metadata, protocol, table_root, 0)
    }

    fn plain_schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked([StructField::nullable(
            "value",
            DataType::INTEGER,
        )]))
    }

    fn annotated_schema() -> Arc<StructType> {
        Arc::new(create_schema(
            "5",
            "\"col-a7f4159c\"",
            "4",
            "\"col-5f422f40\"",
        ))
    }

    /// v3 + CM feature: mode=id/name without annotations → rejected (missing annotations).
    #[test]
    fn test_cm_v3_supported_mode_set_but_no_annotations() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::ColumnMapping]),
            Some([TableFeature::ColumnMapping]),
        )
        .unwrap();
        for mode in ["id", "name"] {
            assert!(
                make_cm_tc(plain_schema(), protocol.clone(), Some(mode)).is_err(),
                "v3+feat, mode={mode}, no annotations: should fail"
            );
        }
    }

    /// v3 + CM feature: mode=none with annotations → rejected (orphaned annotations).
    #[test]
    fn test_cm_v3_supported_mode_none_with_annotations() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([TableFeature::ColumnMapping]),
            Some([TableFeature::ColumnMapping]),
        )
        .unwrap();
        assert!(
            make_cm_tc(annotated_schema(), protocol, Some("none")).is_err(),
            "v3+feat, mode=none, annotations present: should fail"
        );
    }

    /// v3 + no CM feature: mode=id/name without annotations → silently downgrades to None.
    /// The mode property is ignored per spec when the feature is not supported.
    #[test]
    fn test_cm_v3_not_supported_mode_set_no_annotations() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<[String; 0]>([]),
            Some::<[String; 0]>([]),
        )
        .unwrap();
        for mode in ["id", "name"] {
            let tc = make_cm_tc(plain_schema(), protocol.clone(), Some(mode))
                .unwrap_or_else(|e| panic!("v3-feat, mode={mode}, no annotations: {e}"));
            assert!(!tc.is_feature_supported(&TableFeature::ColumnMapping));
            assert_eq!(tc.column_mapping_mode(), ColumnMappingMode::None);
        }
    }

    // =========================================================================
    // Tests for write-side column mapping functions
    // =========================================================================

    #[rstest::rstest]
    #[case::no_property(None, Some(ColumnMappingMode::None))]
    #[case::mode_name(Some("name"), Some(ColumnMappingMode::Name))]
    #[case::mode_id(Some("id"), Some(ColumnMappingMode::Id))]
    #[case::mode_none_explicit(Some("none"), Some(ColumnMappingMode::None))]
    #[case::invalid_mode(Some("invalid"), None)]
    fn test_get_column_mapping_mode_from_properties(
        #[case] mode_str: Option<&str>,
        #[case] expected: Option<ColumnMappingMode>,
    ) {
        let mut properties = HashMap::new();
        if let Some(mode) = mode_str {
            properties.insert(COLUMN_MAPPING_MODE.to_string(), mode.to_string());
        }
        match expected {
            Some(mode) => assert_eq!(
                get_column_mapping_mode_from_properties(&properties).unwrap(),
                mode
            ),
            None => assert!(get_column_mapping_mode_from_properties(&properties).is_err()),
        }
    }

    #[test]
    fn test_assign_column_mapping_metadata_simple() {
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::STRING, true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should have assigned IDs 1 and 2
        assert_eq!(max_id, 2);
        assert_eq!(result.fields().count(), 2);

        // Check both fields have metadata
        for (i, field) in result.fields().enumerate() {
            let expected_id = (i + 1) as i64;
            assert_eq!(
                field.get_config_value(&ColumnMetadataKey::ColumnMappingId),
                Some(&MetadataValue::Number(expected_id))
            );
            assert!(field
                .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
                .is_some());

            // Verify physical name format (col-{uuid})
            if let Some(MetadataValue::String(name)) =
                field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            {
                assert!(
                    name.starts_with("col-"),
                    "Physical name should start with 'col-'"
                );
            }
        }
    }

    #[test]
    fn test_assign_column_mapping_metadata_rejects_existing_id() {
        // Schema with pre-existing column mapping metadata should be rejected
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false).add_metadata([
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(100),
                ),
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("existing-physical".to_string()),
                ),
            ]),
            StructField::new("b", DataType::STRING, true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already has column mapping metadata"),
            "Expected error about existing column mapping metadata, got: {}",
            err_msg
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

        // Should have assigned IDs to all 4 fields
        assert_eq!(max_id, 4);

        let mut seen_ids = HashSet::new();
        let mut seen_physical_names = HashSet::new();

        // Check outer field 'a'
        let field_a = result.field("a").unwrap();
        assert_has_column_mapping_metadata(field_a, &mut seen_ids, &mut seen_physical_names);

        // Check outer field 'nested'
        let field_nested = result.field("nested").unwrap();
        assert_has_column_mapping_metadata(field_nested, &mut seen_ids, &mut seen_physical_names);

        // Check nested fields
        let inner = unwrap_struct(&field_nested.data_type, "nested");
        let field_x = inner.field("x").unwrap();
        assert_has_column_mapping_metadata(field_x, &mut seen_ids, &mut seen_physical_names);
        let field_y = inner.field("y").unwrap();
        assert_has_column_mapping_metadata(field_y, &mut seen_ids, &mut seen_physical_names);

        // All 4 fields should have unique IDs and physical names
        assert_eq!(seen_ids.len(), 4);
        assert_eq!(seen_physical_names.len(), 4);
    }

    // ========================================================================
    // "Cursed" nested type tests - verify column mapping metadata is assigned
    // correctly for complex nested structures (arrays, maps, deeply nested)
    // ========================================================================

    /// Helper to verify a struct field has column mapping metadata (id and physical name).
    /// Also collects the id and physical name into the provided sets for uniqueness checking.
    fn assert_has_column_mapping_metadata(
        field: &StructField,
        seen_ids: &mut HashSet<i64>,
        seen_physical_names: &mut HashSet<String>,
    ) {
        let id = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .unwrap_or_else(|| panic!("Field '{}' should have column mapping ID", field.name));
        let MetadataValue::Number(id_val) = id else {
            panic!(
                "Field '{}' column mapping ID should be a number",
                field.name
            );
        };
        assert!(
            seen_ids.insert(*id_val),
            "Duplicate column mapping ID {} on field '{}'",
            id_val,
            field.name
        );

        let physical = field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .unwrap_or_else(|| panic!("Field '{}' should have physical name", field.name));
        let MetadataValue::String(physical_name) = physical else {
            panic!("Field '{}' physical name should be a string", field.name);
        };
        assert!(
            seen_physical_names.insert(physical_name.clone()),
            "Duplicate physical name '{}' on field '{}'",
            physical_name,
            field.name
        );
    }

    /// Helper to extract struct from a DataType, panicking with context if not a struct
    fn unwrap_struct<'a>(data_type: &'a DataType, context: &str) -> &'a StructType {
        match data_type {
            DataType::Struct(s) => s,
            _ => panic!("Expected Struct for {}, got {:?}", context, data_type),
        }
    }

    #[test]
    fn test_assign_column_mapping_metadata_map_with_struct_key_and_value() {
        // Test: map<struct<k: int>, struct<v: int>>
        // Both key and value are structs that need column mapping metadata

        let key_struct =
            StructType::new_unchecked([StructField::new("k", DataType::INTEGER, false)]);
        let value_struct =
            StructType::new_unchecked([StructField::new("v", DataType::INTEGER, false)]);

        let map_type = MapType::new(
            DataType::Struct(Box::new(key_struct)),
            DataType::Struct(Box::new(value_struct)),
            true,
        );

        let schema = StructType::new_unchecked([StructField::new(
            "my_map",
            DataType::Map(Box::new(map_type)),
            true,
        )]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should assign IDs to: my_map (1), k (2), v (3)
        assert_eq!(max_id, 3);

        let mut seen_ids = HashSet::new();
        let mut seen_physical_names = HashSet::new();

        // Check top-level map field
        let map_field = result.field("my_map").unwrap();
        assert_has_column_mapping_metadata(map_field, &mut seen_ids, &mut seen_physical_names);

        // Check key struct field
        if let DataType::Map(inner_map) = &map_field.data_type {
            let key_struct = unwrap_struct(inner_map.key_type(), "map key");
            let field_k = key_struct.field("k").unwrap();
            assert_has_column_mapping_metadata(field_k, &mut seen_ids, &mut seen_physical_names);

            // Check value struct field
            let value_struct = unwrap_struct(inner_map.value_type(), "map value");
            let field_v = value_struct.field("v").unwrap();
            assert_has_column_mapping_metadata(field_v, &mut seen_ids, &mut seen_physical_names);
        } else {
            panic!("Expected map type");
        }

        assert_eq!(seen_ids.len(), 3);
        assert_eq!(seen_physical_names.len(), 3);
    }

    #[test]
    fn test_assign_column_mapping_metadata_array_with_struct_element() {
        // Test: array<struct<elem: int>>

        let elem_struct =
            StructType::new_unchecked([StructField::new("elem", DataType::INTEGER, false)]);

        let array_type = ArrayType::new(DataType::Struct(Box::new(elem_struct)), true);

        let schema = StructType::new_unchecked([StructField::new(
            "my_array",
            DataType::Array(Box::new(array_type)),
            true,
        )]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should assign IDs to: my_array (1), elem (2)
        assert_eq!(max_id, 2);

        let mut seen_ids = HashSet::new();
        let mut seen_physical_names = HashSet::new();

        // Check top-level array field
        let array_field = result.field("my_array").unwrap();
        assert_has_column_mapping_metadata(array_field, &mut seen_ids, &mut seen_physical_names);

        // Check element struct field
        if let DataType::Array(inner_array) = &array_field.data_type {
            let elem_struct = unwrap_struct(inner_array.element_type(), "array element");
            let field_elem = elem_struct.field("elem").unwrap();
            assert_has_column_mapping_metadata(field_elem, &mut seen_ids, &mut seen_physical_names);
        } else {
            panic!("Expected array type");
        }

        assert_eq!(seen_ids.len(), 2);
        assert_eq!(seen_physical_names.len(), 2);
    }

    #[test]
    fn test_assign_column_mapping_metadata_double_nested_array() {
        // Test: array<array<struct<deep: int>>>

        let deep_struct =
            StructType::new_unchecked([StructField::new("deep", DataType::INTEGER, false)]);

        let inner_array = ArrayType::new(DataType::Struct(Box::new(deep_struct)), true);
        let outer_array = ArrayType::new(DataType::Array(Box::new(inner_array)), true);

        let schema = StructType::new_unchecked([StructField::new(
            "nested_arrays",
            DataType::Array(Box::new(outer_array)),
            true,
        )]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should assign IDs to: nested_arrays (1), deep (2)
        assert_eq!(max_id, 2);

        let mut seen_ids = HashSet::new();
        let mut seen_physical_names = HashSet::new();

        // Check top-level field
        let outer_field = result.field("nested_arrays").unwrap();
        assert_has_column_mapping_metadata(outer_field, &mut seen_ids, &mut seen_physical_names);

        // Navigate: array -> array -> struct -> field
        let DataType::Array(outer) = &outer_field.data_type else {
            panic!("Expected outer array type");
        };
        let DataType::Array(inner) = outer.element_type() else {
            panic!("Expected inner array type");
        };
        let deep_struct = unwrap_struct(inner.element_type(), "inner array element");
        let field_deep = deep_struct.field("deep").unwrap();
        assert_has_column_mapping_metadata(field_deep, &mut seen_ids, &mut seen_physical_names);

        assert_eq!(seen_ids.len(), 2);
        assert_eq!(seen_physical_names.len(), 2);
    }

    #[test]
    fn test_assign_column_mapping_metadata_array_map_array_struct_nesting() {
        // Test: array<map<array<struct<k: int>>, array<struct<v: int>>>>
        // Deeply nested array-map-array-struct combination

        let key_struct =
            StructType::new_unchecked([StructField::new("k", DataType::INTEGER, false)]);
        let value_struct =
            StructType::new_unchecked([StructField::new("v", DataType::INTEGER, false)]);

        let key_array = ArrayType::new(DataType::Struct(Box::new(key_struct)), true);
        let value_array = ArrayType::new(DataType::Struct(Box::new(value_struct)), true);

        let inner_map = MapType::new(
            DataType::Array(Box::new(key_array)),
            DataType::Array(Box::new(value_array)),
            true,
        );

        let outer_array = ArrayType::new(DataType::Map(Box::new(inner_map)), true);

        let schema = StructType::new_unchecked([StructField::new(
            "cursed",
            DataType::Array(Box::new(outer_array)),
            true,
        )]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        // Should assign IDs to: cursed (1), k (2), v (3)
        assert_eq!(max_id, 3);

        let mut seen_ids = HashSet::new();
        let mut seen_physical_names = HashSet::new();

        // Check top-level field
        let cursed_field = result.field("cursed").unwrap();
        assert_has_column_mapping_metadata(cursed_field, &mut seen_ids, &mut seen_physical_names);

        // Navigate: array -> map -> key array -> struct -> field
        //                        -> value array -> struct -> field
        let DataType::Array(outer) = &cursed_field.data_type else {
            panic!("Expected outer array type");
        };
        let DataType::Map(inner_map) = outer.element_type() else {
            panic!("Expected map inside outer array");
        };

        // Check key path: array<struct<k>>
        let DataType::Array(key_arr) = inner_map.key_type() else {
            panic!("Expected array for map key");
        };
        let key_struct = unwrap_struct(key_arr.element_type(), "key array element");
        let field_k = key_struct.field("k").unwrap();
        assert_has_column_mapping_metadata(field_k, &mut seen_ids, &mut seen_physical_names);

        // Check value path: array<struct<v>>
        let DataType::Array(val_arr) = inner_map.value_type() else {
            panic!("Expected array for map value");
        };
        let val_struct = unwrap_struct(val_arr.element_type(), "value array element");
        let field_v = val_struct.field("v").unwrap();
        assert_has_column_mapping_metadata(field_v, &mut seen_ids, &mut seen_physical_names);

        assert_eq!(seen_ids.len(), 3);
        assert_eq!(seen_physical_names.len(), 3);
    }

    #[test]
    fn test_get_any_level_column_physical_name_success() {
        let inner = StructType::new_unchecked([StructField::new("y", DataType::INTEGER, false)
            .add_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("col-inner-y".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
            ])]);

        let schema = StructType::new_unchecked([StructField::new(
            "a",
            DataType::Struct(Box::new(inner)),
            true,
        )
        .add_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("col-outer-a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
        ])]);

        // Top-level column
        let result = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["a"]),
            ColumnMappingMode::Name,
        )
        .unwrap();
        assert_eq!(result, ColumnName::new(["col-outer-a"]));
        assert_eq!(result.path().len(), 1);

        // Nested column
        let result = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["a", "y"]),
            ColumnMappingMode::Name,
        )
        .unwrap();
        assert_eq!(result, ColumnName::new(["col-outer-a", "col-inner-y"]));
        assert_eq!(result.path().len(), 2);

        // No mapping mode returns logical names (annotations are ignored)
        let result = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["a", "y"]),
            ColumnMappingMode::None,
        )
        .unwrap();
        assert_eq!(result, ColumnName::new(["a", "y"]));
        assert_eq!(result.path().len(), 2);
    }

    #[test]
    fn test_get_any_level_column_physical_name_errors() {
        let schema = StructType::new_unchecked([StructField::new("a", DataType::INTEGER, false)]);

        // Non-existent top-level column
        let result = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["nonexistent"]),
            ColumnMappingMode::None,
        );
        assert!(result.is_err());

        // Nested path on a non-struct field
        let result = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["a", "b"]),
            ColumnMappingMode::None,
        );
        assert!(result.is_err());
    }

    #[rstest::rstest]
    // physicalName present, id missing → id error
    #[case::missing_id(true, false, "delta.columnMapping.id")]
    // id present, physicalName missing → physicalName error
    #[case::missing_physical_name(false, true, "delta.columnMapping.physicalName")]
    // both missing → physicalName checked first, so physicalName error
    #[case::missing_both(false, false, "delta.columnMapping.physicalName")]
    fn test_get_any_level_column_physical_name_missing_annotations(
        #[case] has_physical_name: bool,
        #[case] has_id: bool,
        #[case] expected_err: &str,
    ) {
        let mut inner_field = StructField::new("y", DataType::INTEGER, false);
        if has_physical_name {
            inner_field = inner_field.add_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("col-inner-y".to_string()),
            )]);
        }
        if has_id {
            inner_field = inner_field.add_metadata([(
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(2),
            )]);
        }

        let inner = StructType::new_unchecked([inner_field]);
        let schema = StructType::new_unchecked([StructField::new(
            "a",
            DataType::Struct(Box::new(inner)),
            true,
        )
        .add_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("col-outer-a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
        ])]);

        let err = get_any_level_column_physical_name(
            &schema,
            &ColumnName::new(["a", "y"]),
            ColumnMappingMode::Name,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(expected_err),
            "Expected error containing '{expected_err}', got: {err}"
        );
    }
}
