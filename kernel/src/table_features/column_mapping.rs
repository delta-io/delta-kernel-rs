//! Code to handle column mapping, including modes and schema transforms
//!
//! This module provides:
//! - Read-side: Mode detection and schema validation
//! - Write-side: Schema transformation for assigning IDs and physical names
use std::borrow::Cow;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::EnumString;
use uuid::Uuid;

use super::TableFeature;
use crate::actions::Protocol;
use crate::schema::{
    ArrayType, ColumnMetadataKey, ColumnName, DataType, MapType, MetadataValue, Schema,
    StructField, StructType,
};
use crate::table_properties::{TableProperties, COLUMN_MAPPING_MODE};
use crate::transforms::SchemaTransform;
use crate::{DeltaResult, Error};

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
/// physical name and field_id, and that no two fields share the same `delta.columnMapping.id`
/// value. When not enabled, verifies that no fields are annotated.
pub fn validate_schema_column_mapping(schema: &Schema, mode: ColumnMappingMode) -> DeltaResult<()> {
    let mut validator = ValidateColumnMappings {
        mode,
        path: vec![],
        seen: HashMap::new(),
        err: None,
    };
    let _ = validator.transform_struct(schema);
    match validator.err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Validates a field's column mapping annotations and extracts the physical name and column
/// mapping id. If `seen` is provided, also checks for duplicate column mapping IDs.
///
/// Metadata columns are not subject to column mapping and must not carry column mapping
/// annotations. Returns the logical field name and `None` for such fields.
///
/// When column mapping is enabled (`Id` or `Name`), the field must have a
/// `delta.columnMapping.physicalName` (string) and `delta.columnMapping.id` (number) annotation.
/// Returns the physical name and `Some(id)`.
///
/// When disabled (`None`), neither annotation should be present. Returns the logical field name
/// and `None`.
///
/// `path` identifies the field in error messages (e.g. `&["a", "b"]` renders as `a.b`).
pub(crate) fn get_field_column_mapping_info<'a>(
    field: &'a StructField,
    mode: ColumnMappingMode,
    path: &[&str],
    seen: Option<&mut HashMap<i64, &'a str>>,
) -> DeltaResult<(&'a str, Option<i64>)> {
    let field_path = || ColumnName::new(path.iter().copied());
    let physical_name_meta = field
        .metadata
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref());
    let id_meta = field
        .metadata
        .get(ColumnMetadataKey::ColumnMappingId.as_ref());

    if field.is_metadata_column() {
        if physical_name_meta.is_some() || id_meta.is_some() {
            return Err(Error::internal_error(format!(
                "Metadata column '{}' must not have column mapping annotations",
                field.name()
            )));
        }
        return Ok((field.name(), None));
    }

    let annotation = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    let physical_name = match (mode, physical_name_meta) {
        (ColumnMappingMode::None, None) => field.name(),
        (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::String(s))) => s,
        (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
            return Err(Error::schema(format!(
                "The {annotation} annotation on field '{}' must be a string",
                field_path(),
            )));
        }
        (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
            return Err(Error::schema(format!(
                "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                field_path(),
            )));
        }
        (ColumnMappingMode::None, Some(_)) => {
            return Err(Error::schema(format!(
                "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                field_path(),
            )));
        }
    };

    let annotation = ColumnMetadataKey::ColumnMappingId.as_ref();
    let id = match (mode, id_meta) {
        (ColumnMappingMode::None, None) => None,
        (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(MetadataValue::Number(n))) => {
            Some(*n)
        }
        (ColumnMappingMode::Name | ColumnMappingMode::Id, Some(_)) => {
            return Err(Error::schema(format!(
                "The {annotation} annotation on field '{}' must be a number",
                field_path(),
            )));
        }
        (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
            return Err(Error::schema(format!(
                "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                field_path(),
            )));
        }
        (ColumnMappingMode::None, Some(_)) => {
            return Err(Error::schema(format!(
                "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                field_path(),
            )));
        }
    };

    if let (Some(id), Some(seen)) = (id, seen) {
        seen.insert(id, field.name()).map_or(Ok(()), |prev| {
            Err(Error::schema(format!(
                "Duplicate column mapping ID {id} assigned to both '{prev}' and '{}'",
                field.name()
            )))
        })?;
    }

    Ok((physical_name, id))
}

struct ValidateColumnMappings<'a> {
    mode: ColumnMappingMode,
    path: Vec<&'a str>,
    seen: HashMap<i64, &'a str>, // column mapping id -> first field name that claimed it
    err: Option<Error>,
}

impl<'a> ValidateColumnMappings<'a> {
    fn transform_inner<R>(&mut self, field_name: &'a str, validate: impl FnOnce(&mut Self) -> R) {
        if self.err.is_none() {
            self.path.push(field_name);
            let _ = validate(self);
            self.path.pop();
        }
    }
}

impl<'a> SchemaTransform<'a> for ValidateColumnMappings<'a> {
    // Override array element and map key/value for better error messages
    fn transform_array_element(&mut self, etype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<array element>", |this| this.transform(etype));
        Some(Cow::Borrowed(etype))
    }
    fn transform_map_key(&mut self, ktype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<map key>", |this| this.transform(ktype));
        Some(Cow::Borrowed(ktype))
    }
    fn transform_map_value(&mut self, vtype: &'a DataType) -> Option<Cow<'a, DataType>> {
        self.transform_inner("<map value>", |this| this.transform(vtype));
        Some(Cow::Borrowed(vtype))
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.transform_inner(field.name(), |this| {
            get_field_column_mapping_info(field, this.mode, &this.path, Some(&mut this.seen))
                .map_err(|e| this.err = Some(e))
                .ok()?;
            this.recurse_into_struct_field(field)
        });
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
                "Invalid column mapping mode '{mode_str}'. Must be one of: none, name, id"
            ))
        }),
        None => Ok(ColumnMappingMode::None),
    }
}

/// Assigns column mapping metadata (id and physicalName) to all fields in a schema,
/// matching delta-spark's `DeltaColumnMapping.assignColumnIdAndPhysicalName` semantics.
///
/// This function recursively processes all fields in the schema, including nested structs,
/// arrays, and maps. For each field:
/// - If both `delta.columnMapping.id` and `delta.columnMapping.physicalName` are present, they are
///   preserved verbatim and `max_id` is advanced past the preserved id.
/// - If only `id` is present, `physicalName` is filled in as `col-<uuid>` and the id is preserved.
/// - If only `physicalName` is present, a new `id` is allocated as `*max_id + 1`.
/// - If neither is present, both are assigned.
///
/// Wrong-typed annotations (`id` not a number, `physicalName` not a string), an empty
/// `physicalName`, and a negative `id` error out. The latter two are stricter than
/// delta-spark: an empty physical name fails PROTOCOL.md's "globally unique identifier"
/// requirement and would break Parquet column resolution, and a negative id is rejected so
/// `(*max_id).max(n)` cannot silently no-op below the allocator's seed.
///
/// Callers should seed `max_id` from [`find_max_column_id_in_schema`] so newly assigned
/// IDs do not collide with preserved ones. Duplicate preserved IDs are detected by
/// [`crate::schema::StructType::make_physical`], which runs from
/// `TableConfiguration::try_new[_with_schema]` after assignment.
///
/// # Arguments
///
/// * `schema` - The schema to transform.
/// * `max_id` - Tracks the highest column ID. Read and updated in place. Should be seeded from
///   `find_max_column_id_in_schema(schema)` (or `0` for a brand-new table with no preserved IDs
///   anywhere).
///
/// # Returns
///
/// A new schema with column mapping metadata present on every field.
pub(crate) fn assign_column_mapping_metadata(
    schema: &StructType,
    max_id: &mut i64,
) -> DeltaResult<StructType> {
    let new_fields: Vec<StructField> = schema
        .fields()
        .map(|field| try_assign_field_column_mapping(field, max_id))
        .collect::<DeltaResult<Vec<_>>>()?;

    StructType::try_new(new_fields)
}

/// Assigns column mapping metadata to a single field, recursively processing nested types.
///
/// Implements the 3-way preserve/fill/assign behavior described on
/// [`assign_column_mapping_metadata`], matching delta-spark. The returned field always has
/// both `delta.columnMapping.id` and `delta.columnMapping.physicalName` present.
///
/// `max_id` is advanced as follows:
/// - When a `delta.columnMapping.id` is preserved, `*max_id = max(*max_id, preserved_id)`.
/// - When a new id is allocated, `*max_id += 1` and the new id is `*max_id`.
///
/// # Errors
///
/// Errors when any pre-existing `id` / `physicalName` annotation on `field` is wrong-typed,
/// when the `id` is negative, or when the `physicalName` is empty.
pub(crate) fn try_assign_field_column_mapping(
    field: &StructField,
    max_id: &mut i64,
) -> DeltaResult<StructField> {
    // TODO: Also check for nested column IDs (`delta.columnMapping.nested.ids`) once
    // Iceberg compatibility (IcebergCompatV2+) is supported. See issue #1125.
    field.validate_existing_column_mapping_annotations()?;

    let id_meta = field.get_config_value(&ColumnMetadataKey::ColumnMappingId);
    let name_meta = field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName);
    let mut new_field = field.clone();

    match (id_meta, name_meta) {
        // Both present with correct types: preserve verbatim and advance `max_id`.
        (Some(MetadataValue::Number(n)), Some(MetadataValue::String(_))) => {
            *max_id = (*max_id).max(*n);
        }
        // Only `id` present: preserve id, fill in physical name.
        (Some(MetadataValue::Number(n)), None) => {
            *max_id = (*max_id).max(*n);
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                MetadataValue::String(format!("col-{}", Uuid::new_v4())),
            );
        }
        // Only `physicalName` present: preserve name, allocate id.
        (None, Some(MetadataValue::String(_))) => {
            *max_id += 1;
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                MetadataValue::Number(*max_id),
            );
        }
        // Neither present: assign both.
        (None, None) => {
            *max_id += 1;
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                MetadataValue::Number(*max_id),
            );
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                MetadataValue::String(format!("col-{}", Uuid::new_v4())),
            );
        }
        _ => unreachable!("Invalid annotations should be rejected upstream."),
    }

    // Recursively process nested types (struct/array/map descend; primitive/variant pass through)
    new_field.data_type = process_nested_data_type(&field.data_type, max_id)?;

    Ok(new_field)
}

/// Returns the largest `delta.columnMapping.id` found anywhere in `schema`, including nested
/// data types. Returns `None` if no field carries a column mapping ID.
pub(crate) fn find_max_column_id_in_schema(schema: &StructType) -> Option<i64> {
    let mut visitor = MaxColumnId(None);
    visitor.transform_struct(schema);
    visitor.0
}

/// Visitor that walks a schema and records the largest `delta.columnMapping.id` seen on any
/// field (including nested struct, array, map, and variant fields).
struct MaxColumnId(Option<i64>);

impl<'a> SchemaTransform<'a> for MaxColumnId {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if let Some(n) = field.column_mapping_id() {
            self.0 = Some(self.0.map_or(n, |prev| prev.max(n)));
        }
        // Recurse into the field's data type so we also visit nested struct/array/map members.
        self.recurse_into_struct_field(field)
    }
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

/// Convert a physical column name to a logical column name by walking the schema.
///
/// For each path component in the physical column, finds the field in the schema whose
/// `physical_name(mode)` matches, and returns the field's logical name instead.
pub(crate) fn physical_to_logical_column_name(
    logical_schema: &StructType,
    physical_col: &ColumnName,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<ColumnName> {
    let fields = logical_schema.walk_column_fields_by(physical_col, |s, phys_name| {
        s.fields()
            .find(|f| f.physical_name(column_mapping_mode) == phys_name)
    })?;
    Ok(ColumnName::new(fields.iter().map(|f| f.name.clone())))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::expressions::ColumnName;
    use crate::schema::{DataType, MetadataValue, StructField, StructType};
    use crate::utils::test_utils::{make_test_tc, test_deep_nested_schema_missing_leaf_cm};

    fn expect_numeric_column_mapping_id(field: &StructField) -> i64 {
        match field.get_config_value(&ColumnMetadataKey::ColumnMappingId) {
            Some(MetadataValue::Number(n)) => *n,
            other => panic!("expected numeric column mapping id, got {other:?}"),
        }
    }

    fn expect_physical_name(field: &StructField) -> &str {
        match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
            Some(MetadataValue::String(s)) => s,
            other => panic!("expected string physicalName, got {other:?}"),
        }
    }

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

    // Creates optional schema field annotations for column mapping id and physical name, as a
    // string.
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

    // Creates a generic schema with optional field annotations for column mapping id and physical
    // name.
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

    #[test]
    fn test_annotation_validation_reaches_struct_fields_in_map_value() {
        let unannotated =
            StructType::new_unchecked([StructField::new("x", DataType::INTEGER, false)]);
        let schema = StructType::new_unchecked([make_cm_field(
            "b",
            1,
            MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(unannotated)),
                false,
            ),
        )]);
        validate_schema_column_mapping(&schema, ColumnMappingMode::Id)
            .expect_err("missing annotation on struct field inside map value");
    }

    fn make_cm_field(name: &str, id: i64, data_type: impl Into<DataType>) -> StructField {
        StructField::new(name, data_type, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(format!("col-{name}")),
            ),
        ])
    }

    fn cm_schema_same_level_duplicates() -> StructType {
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field("b", 1, DataType::INTEGER),
        ])
    }

    fn cm_schema_nested_duplicates() -> StructType {
        let nested = StructType::new_unchecked([
            make_cm_field("x", 5, DataType::INTEGER),
            make_cm_field("y", 5, DataType::INTEGER),
        ]);
        StructType::new_unchecked([make_cm_field(
            "outer",
            10,
            DataType::Struct(Box::new(nested)),
        )])
    }

    fn cm_schema_cross_level_duplicates() -> StructType {
        let nested = StructType::new_unchecked([make_cm_field("inner", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field("b", 2, DataType::Struct(Box::new(nested))),
        ])
    }

    fn cm_schema_array_duplicates() -> StructType {
        let element = StructType::new_unchecked([make_cm_field("x", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field(
                "b",
                2,
                ArrayType::new(DataType::Struct(Box::new(element)), false),
            ),
        ])
    }

    fn cm_schema_map_duplicates() -> StructType {
        let value = StructType::new_unchecked([make_cm_field("x", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field(
                "b",
                2,
                MapType::new(DataType::STRING, DataType::Struct(Box::new(value)), false),
            ),
        ])
    }

    #[rstest::rstest]
    #[case::same_level(cm_schema_same_level_duplicates())]
    #[case::nested_struct(cm_schema_nested_duplicates())]
    #[case::across_nesting_levels(cm_schema_cross_level_duplicates())]
    #[case::across_array(cm_schema_array_duplicates())]
    #[case::across_map(cm_schema_map_duplicates())]
    fn test_duplicate_column_mapping_ids_rejected(#[case] schema: StructType) {
        crate::utils::test_utils::assert_result_error_with_message(
            validate_schema_column_mapping(&schema, ColumnMappingMode::Id),
            "Duplicate column mapping ID",
        );
    }

    #[test]
    fn test_duplicate_column_mapping_ids_rejected_in_name_mode() {
        crate::utils::test_utils::assert_result_error_with_message(
            validate_schema_column_mapping(
                &cm_schema_same_level_duplicates(),
                ColumnMappingMode::Name,
            ),
            "Duplicate column mapping ID",
        );
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

    /// Schema shapes used to dimensionalize tests over field nesting depth.
    #[derive(Clone, Copy, Debug)]
    enum SchemaShape {
        /// Field under test is a top-level sibling of an unannotated field.
        Flat,
        /// Field under test lives inside a 1-level nested struct, with a top-level unannotated
        /// sibling.
        NestedStruct,
        /// Field under test lives 2 levels deep inside nested structs, with a top-level
        /// unannotated sibling.
        DeeplyNestedStruct,
        /// Field under test lives in an array element struct.
        ArrayOfStruct,
        /// Field under test lives in a map key struct.
        MapKeyStruct,
        /// Field under test lives in a map value struct.
        MapValueStruct,
    }

    const FIELD_UNDER_TEST: &str = "field_under_test";

    /// Builds the field whose pre-existing column-mapping annotations vary by test case.
    fn make_field_under_test(pre_id: Option<i64>, pre_name: Option<&str>) -> StructField {
        let mut metadata: Vec<(&str, MetadataValue)> = Vec::new();
        if let Some(id) = pre_id {
            metadata.push((
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ));
        }
        if let Some(name) = pre_name {
            metadata.push((
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(name.to_string()),
            ));
        }
        let field = StructField::not_null(FIELD_UNDER_TEST, DataType::INTEGER);
        if metadata.is_empty() {
            field
        } else {
            field.add_metadata(metadata)
        }
    }

    /// Wraps the field under test according to `shape`, alongside a top-level unannotated
    /// sibling. Returns the schema and path that resolves back to the field under test.
    fn build_schema_with_field_under_test(
        shape: SchemaShape,
        field_under_test: StructField,
    ) -> (StructType, Vec<String>) {
        let path = |segments: &[&str]| segments.iter().map(|s| s.to_string()).collect();
        match shape {
            SchemaShape::Flat => {
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    field_under_test,
                ]);
                (schema, path(&[FIELD_UNDER_TEST]))
            }
            SchemaShape::NestedStruct => {
                let inner = StructType::new_unchecked([field_under_test]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable("outer", DataType::Struct(Box::new(inner))),
                ]);
                (schema, path(&["outer", FIELD_UNDER_TEST]))
            }
            SchemaShape::DeeplyNestedStruct => {
                let innermost = StructType::new_unchecked([field_under_test]);
                let middle = StructType::new_unchecked([StructField::nullable(
                    "middle",
                    DataType::Struct(Box::new(innermost)),
                )]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable("outer", DataType::Struct(Box::new(middle))),
                ]);
                (schema, path(&["outer", "middle", FIELD_UNDER_TEST]))
            }
            SchemaShape::ArrayOfStruct => {
                let inner = StructType::new_unchecked([field_under_test]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable(
                        "outer",
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(inner)),
                            true,
                        ))),
                    ),
                ]);
                (schema, path(&["outer"]))
            }
            SchemaShape::MapKeyStruct => {
                let key = StructType::new_unchecked([field_under_test]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable(
                        "outer",
                        DataType::Map(Box::new(MapType::new(
                            DataType::Struct(Box::new(key)),
                            DataType::STRING,
                            true,
                        ))),
                    ),
                ]);
                (schema, path(&["outer"]))
            }
            SchemaShape::MapValueStruct => {
                let value = StructType::new_unchecked([field_under_test]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable(
                        "outer",
                        DataType::Map(Box::new(MapType::new(
                            DataType::STRING,
                            DataType::Struct(Box::new(value)),
                            true,
                        ))),
                    ),
                ]);
                (schema, path(&["outer"]))
            }
        }
    }

    fn find_field_under_test<'a>(
        schema: &'a StructType,
        shape: SchemaShape,
        path: &[String],
    ) -> &'a StructField {
        match shape {
            SchemaShape::Flat | SchemaShape::NestedStruct | SchemaShape::DeeplyNestedStruct => {
                schema.field_at_path(path)
            }
            SchemaShape::ArrayOfStruct => {
                let outer = schema.field_at_path(path);
                let DataType::Array(array) = &outer.data_type else {
                    panic!("expected array field for {shape:?}");
                };
                let DataType::Struct(inner) = array.element_type() else {
                    panic!("expected array element struct for {shape:?}");
                };
                inner.field(FIELD_UNDER_TEST).unwrap()
            }
            SchemaShape::MapKeyStruct => {
                let outer = schema.field_at_path(path);
                let DataType::Map(map) = &outer.data_type else {
                    panic!("expected map field for {shape:?}");
                };
                let DataType::Struct(key) = map.key_type() else {
                    panic!("expected map key struct for {shape:?}");
                };
                key.field(FIELD_UNDER_TEST).unwrap()
            }
            SchemaShape::MapValueStruct => {
                let outer = schema.field_at_path(path);
                let DataType::Map(map) = &outer.data_type else {
                    panic!("expected map field for {shape:?}");
                };
                let DataType::Struct(value) = map.value_type() else {
                    panic!("expected map value struct for {shape:?}");
                };
                value.field(FIELD_UNDER_TEST).unwrap()
            }
        }
    }

    /// Happy-path dispatch coverage for [`assign_column_mapping_metadata`]. Each `#[case]`
    /// exercises one of the 4 dispatch arms (preserve-both / preserve-id-fill-name /
    /// preserve-name-allocate-id / assign-both), including `id = 0` to verify that 0 is
    /// accepted as a preserved id (matches delta-spark; symmetric with `maxColumnId = 0`
    /// table property). The schema-shape `#[values]` axis confirms the same dispatch behavior
    /// at every nesting depth.
    #[rstest::rstest]
    #[case::preserve_both(Some(100), Some("preserved-name"))]
    #[case::preserve_id_only(Some(7), None)]
    #[case::preserve_name_only(None, Some("user-supplied"))]
    #[case::preserve_id_zero(Some(0), Some("zero-name"))]
    #[case::neither_preserved(None, None)]
    fn test_assign_column_mapping_metadata_dispatch(
        #[case] pre_id: Option<i64>,
        #[case] pre_name: Option<&str>,
        #[values(
            SchemaShape::Flat,
            SchemaShape::NestedStruct,
            SchemaShape::DeeplyNestedStruct,
            SchemaShape::ArrayOfStruct,
            SchemaShape::MapKeyStruct,
            SchemaShape::MapValueStruct
        )]
        shape: SchemaShape,
    ) {
        let field_under_test = make_field_under_test(pre_id, pre_name);
        let (schema, field_under_test_path) =
            build_schema_with_field_under_test(shape, field_under_test);

        let seed = find_max_column_id_in_schema(&schema).unwrap_or(0);
        let mut max_id = seed;
        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();

        let result_field = find_field_under_test(&result, shape, &field_under_test_path);
        let result_id = expect_numeric_column_mapping_id(result_field);
        let result_name = expect_physical_name(result_field);

        match pre_id {
            Some(id) => assert_eq!(result_id, id, "preserved id must round-trip"),
            None => assert!(
                result_id > seed,
                "allocated id {result_id} must exceed seed {seed}",
            ),
        }
        match pre_name {
            Some(expected) => {
                assert_eq!(result_name, expected, "preserved name must round-trip")
            }
            None => assert!(
                result_name.starts_with("col-"),
                "generated name should start with `col-`, got {result_name:?}",
            ),
        }
    }

    /// Validates delta-spark's seeding rule for [`assign_column_mapping_metadata`].
    ///
    /// delta-spark's `DeltaColumnMapping.assignColumnIdAndPhysicalName` seeds:
    ///     maxColumnId = max(currentMaxColumnId, findMaxColumnId(schema))
    /// then assigns each new id as `maxColumnId += 1`. Consequence: every newly assigned
    /// id is strictly greater than `findMaxColumnId(schema)`, and preserved ids round-trip
    /// unchanged.
    ///
    /// Each `#[case]` supplies a different set of preserved ids to confirm the seed advances
    /// correctly regardless of value sparsity (sparse positives, sequential, with `0`,
    /// single-max).
    #[rstest::rstest]
    #[case::sparse_positives(vec![1, 5, 100])]
    #[case::with_zero(vec![0, 5, 100])]
    #[case::single_max(vec![100])]
    #[case::sequential(vec![1, 2, 3])]
    #[case::single_zero(vec![0])]
    fn test_assign_column_mapping_metadata_seed_avoids_collisions(#[case] preserved_ids: Vec<i64>) {
        let preserved_max = *preserved_ids.iter().max().unwrap();
        const UNANNOTATED_FIELD_NAMES: [&str; 3] =
            ["unannotated_1", "unannotated_2", "unannotated_3"];

        // Build schema: unannotated_1, [preserved_0..N], unannotated_2, unannotated_3.
        let mut fields = vec![StructField::nullable(
            UNANNOTATED_FIELD_NAMES[0],
            DataType::STRING,
        )];
        for (i, id) in preserved_ids.iter().enumerate() {
            let name = format!("preserved_{i}");
            fields.push(
                StructField::not_null(&name, DataType::INTEGER).add_metadata([
                    (
                        ColumnMetadataKey::ColumnMappingId.as_ref(),
                        MetadataValue::Number(*id),
                    ),
                    (
                        ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                        MetadataValue::String(format!("p-{name}")),
                    ),
                ]),
            );
        }
        fields.extend(
            UNANNOTATED_FIELD_NAMES[1..]
                .iter()
                .map(|n| StructField::nullable(*n, DataType::STRING)),
        );
        let schema = StructType::new_unchecked(fields);

        let mut max_id = find_max_column_id_in_schema(&schema).unwrap_or(0);
        assert_eq!(max_id, preserved_max, "seed should equal the preserved max");

        let result = assign_column_mapping_metadata(&schema, &mut max_id).unwrap();
        // Three unannotated fields -> +3 ids above the seed.
        assert_eq!(max_id, preserved_max + UNANNOTATED_FIELD_NAMES.len() as i64);

        // Every assigned id on an unannotated field exceeds the preserved max.
        for name in UNANNOTATED_FIELD_NAMES {
            let field = result.field(name).unwrap();
            let id = expect_numeric_column_mapping_id(field);
            assert!(
                id > preserved_max,
                "newly assigned id {id} for '{name}' must exceed preserved max {preserved_max}",
            );
        }

        // Preserved ids round-trip.
        for (i, expected_id) in preserved_ids.iter().enumerate() {
            let name = format!("preserved_{i}");
            assert_eq!(
                expect_numeric_column_mapping_id(result.field(&name).unwrap()),
                *expected_id,
                "preserved id at '{name}' must round-trip",
            );
        }
    }

    /// Each case supplies a (possibly partial, possibly malformed) annotation pair and asserts
    /// the error names both the offending key and the kind of violation.
    #[rstest::rstest]
    #[case::id_wrong_type_string(
        Some(MetadataValue::String("not-a-number".to_string())),
        Some(MetadataValue::String("p".to_string())),
        "non-numeric",
        ColumnMetadataKey::ColumnMappingId,
    )]
    #[case::name_wrong_type_number(
        None,
        Some(MetadataValue::Number(7)),
        "non-string",
        ColumnMetadataKey::ColumnMappingPhysicalName
    )]
    #[case::name_empty_no_id(
        None,
        Some(MetadataValue::String(String::new())),
        "empty",
        ColumnMetadataKey::ColumnMappingPhysicalName
    )]
    #[case::name_empty_with_id(
        Some(MetadataValue::Number(5)),
        Some(MetadataValue::String(String::new())),
        "empty",
        ColumnMetadataKey::ColumnMappingPhysicalName
    )]
    #[case::id_negative_with_name(
        Some(MetadataValue::Number(-7)),
        Some(MetadataValue::String("p".to_string())),
        "negative",
        ColumnMetadataKey::ColumnMappingId,
    )]
    #[case::id_i64_min_no_name(
        Some(MetadataValue::Number(i64::MIN)),
        None,
        "negative",
        ColumnMetadataKey::ColumnMappingId
    )]
    fn test_assign_column_mapping_metadata_rejects_invalid_annotations(
        #[case] id: Option<MetadataValue>,
        #[case] name: Option<MetadataValue>,
        #[case] violation_kind: &str,
        #[case] offending_key: ColumnMetadataKey,
        #[values(
            SchemaShape::Flat,
            SchemaShape::NestedStruct,
            SchemaShape::DeeplyNestedStruct,
            SchemaShape::ArrayOfStruct,
            SchemaShape::MapKeyStruct,
            SchemaShape::MapValueStruct
        )]
        shape: SchemaShape,
    ) {
        let mut metadata = Vec::new();
        if let Some(id) = id {
            metadata.push((ColumnMetadataKey::ColumnMappingId.as_ref(), id));
        }
        if let Some(name) = name {
            metadata.push((ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(), name));
        }
        let bad = StructField::not_null(FIELD_UNDER_TEST, DataType::INTEGER).add_metadata(metadata);
        let (schema, _) = build_schema_with_field_under_test(shape, bad);

        let mut max_id = 0;
        let err = assign_column_mapping_metadata(&schema, &mut max_id)
            .unwrap_err()
            .to_string();
        let key = offending_key.as_ref();
        assert!(
            err.contains(violation_kind) && err.contains(key),
            "Expected '{violation_kind}' and '{key}' in error, got: {err}",
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
            _ => panic!("Expected Struct for {context}, got {data_type:?}"),
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

    #[test]
    fn validate_schema_column_mapping_error_includes_full_path() {
        let schema = test_deep_nested_schema_missing_leaf_cm();
        let err = validate_schema_column_mapping(&schema, ColumnMappingMode::Name)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("top.`<array element>`.mid_field.`<map value>`.leaf"),
            "Expected full nested path in error, got: {err}"
        );
    }

    #[test]
    fn physical_to_logical_no_mapping() {
        let schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let physical_col = ColumnName::new(["id"]);
        let result =
            physical_to_logical_column_name(&schema, &physical_col, ColumnMappingMode::None)
                .unwrap();
        assert_eq!(result, ColumnName::new(["id"]));
    }

    #[test]
    fn physical_to_logical_with_name_mapping() {
        let field = StructField::new("user_id", DataType::INTEGER, false).with_metadata([(
            "delta.columnMapping.physicalName".to_string(),
            MetadataValue::String("col-abc-123".to_string()),
        )]);
        let schema = StructType::new_unchecked(vec![field]);

        let physical_col = ColumnName::new(["col-abc-123"]);
        let result =
            physical_to_logical_column_name(&schema, &physical_col, ColumnMappingMode::Name)
                .unwrap();
        assert_eq!(result, ColumnName::new(["user_id"]));
    }

    #[test]
    fn physical_to_logical_not_found() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)]);
        let physical_col = ColumnName::new(["nonexistent"]);
        let result =
            physical_to_logical_column_name(&schema, &physical_col, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found in schema"));
    }

    #[test]
    fn physical_to_logical_nested_struct_with_mapping() {
        let inner_field = StructField::new("city", DataType::STRING, true).with_metadata([(
            "delta.columnMapping.physicalName".to_string(),
            MetadataValue::String("col-inner-456".to_string()),
        )]);
        let inner_struct = StructType::new_unchecked(vec![inner_field]);
        let outer_field =
            StructField::new("address", DataType::Struct(Box::new(inner_struct)), true)
                .with_metadata([(
                    "delta.columnMapping.physicalName".to_string(),
                    MetadataValue::String("col-outer-123".to_string()),
                )]);
        let schema = StructType::new_unchecked(vec![outer_field]);

        let physical_col = ColumnName::new(["col-outer-123", "col-inner-456"]);
        let result =
            physical_to_logical_column_name(&schema, &physical_col, ColumnMappingMode::Name)
                .unwrap();
        assert_eq!(result, ColumnName::new(["address", "city"]));
    }

    #[test]
    fn physical_to_logical_non_struct_intermediate_errors() {
        let schema =
            StructType::new_unchecked(vec![StructField::new("id", DataType::INTEGER, false)]);
        let physical_col = ColumnName::new(["id", "nested"]);
        let result =
            physical_to_logical_column_name(&schema, &physical_col, ColumnMappingMode::None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("is not a struct type"));
    }

    // === find_max_column_id_in_schema tests ===

    fn field_with_id(name: &str, ty: DataType, id: i64) -> StructField {
        let mut f = StructField::nullable(name, ty);
        f.metadata.insert(
            ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
            MetadataValue::Number(id),
        );
        f
    }

    #[test]
    fn find_max_column_id_empty_schema_is_none() {
        let schema =
            StructType::try_new(vec![StructField::nullable("a", DataType::STRING)]).unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), None);
    }

    #[test]
    fn find_max_column_id_top_level_only() {
        let schema = StructType::try_new(vec![
            field_with_id("a", DataType::STRING, 1),
            field_with_id("b", DataType::INTEGER, 3),
            field_with_id("c", DataType::STRING, 2),
        ])
        .unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(3));
    }

    #[test]
    fn find_max_column_id_nested_struct() {
        let inner = DataType::Struct(Box::new(
            StructType::try_new(vec![
                field_with_id("x", DataType::STRING, 7),
                field_with_id("y", DataType::STRING, 5),
            ])
            .unwrap(),
        ));
        let schema = StructType::try_new(vec![
            field_with_id("outer", inner, 2),
            field_with_id("sibling", DataType::STRING, 3),
        ])
        .unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(7));
    }

    #[test]
    fn find_max_column_id_array_and_map_recurse_into_element_types() {
        let array_elem_struct = DataType::Array(Box::new(ArrayType::new(
            DataType::Struct(Box::new(
                StructType::try_new(vec![field_with_id("deep", DataType::STRING, 42)]).unwrap(),
            )),
            true,
        )));
        let map_ty = DataType::Map(Box::new(MapType::new(
            DataType::STRING,
            DataType::Struct(Box::new(
                StructType::try_new(vec![field_with_id("inside", DataType::STRING, 9)]).unwrap(),
            )),
            false,
        )));
        let schema = StructType::try_new(vec![
            field_with_id("arr", array_elem_struct, 1),
            field_with_id("m", map_ty, 2),
        ])
        .unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(42));
    }

    #[test]
    fn find_max_column_id_map_with_struct_key_recurses() {
        let key_struct = DataType::Struct(Box::new(
            StructType::try_new(vec![field_with_id("key_id", DataType::INTEGER, 17)]).unwrap(),
        ));
        let value_struct = DataType::Struct(Box::new(
            StructType::try_new(vec![field_with_id("val_id", DataType::INTEGER, 11)]).unwrap(),
        ));
        let map_ty = DataType::Map(Box::new(MapType::new(key_struct, value_struct, false)));
        let schema = StructType::try_new(vec![field_with_id("m", map_ty, 1)]).unwrap();
        // Max should come from the key struct's `key_id = 17`, beating value's 11 and
        // top-level's 1.
        assert_eq!(find_max_column_id_in_schema(&schema), Some(17));
    }

    /// Every inner variant field carries a cm.id so the assertion proves all three are walked,
    /// not just one. The `shred=23` is the max and beats `metadata=10`, `value=20`, and
    /// top-level `v=4`.
    #[test]
    fn find_max_column_id_variant_recurses_into_inner_fields() {
        let variant = DataType::Variant(Box::new(
            StructType::try_new(vec![
                field_with_id("metadata", DataType::BINARY, 10),
                field_with_id("value", DataType::BINARY, 20),
                field_with_id("shred", DataType::STRING, 23),
            ])
            .unwrap(),
        ));
        let schema = StructType::try_new(vec![field_with_id("v", variant, 4)]).unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(23));
    }
}
