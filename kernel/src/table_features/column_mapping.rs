//! Code to handle column mapping, including modes and schema transforms
//!
//! This module provides:
//! - Read-side: Mode detection and schema validation
//! - Write-side: Schema transformation for assigning IDs and physical names
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
use crate::transforms::{transform_output_type, SchemaTransform};
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
    };
    validator.transform_struct(schema)
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
}

impl<'a> ValidateColumnMappings<'a> {
    fn transform_inner<V>(&mut self, field_name: &'a str, validate: V) -> DeltaResult<()>
    where
        V: FnOnce(&mut Self) -> DeltaResult<()>,
    {
        self.path.push(field_name);
        let result = validate(self);
        self.path.pop();
        result
    }
}

impl<'a> SchemaTransform<'a> for ValidateColumnMappings<'a> {
    transform_output_type!(|'a, T| DeltaResult<()>);

    // Override array element and map key/value for better error messages
    fn transform_array_element(&mut self, etype: &'a DataType) -> DeltaResult<()> {
        self.transform_inner("<array element>", |this| this.transform(etype))
    }
    fn transform_map_key(&mut self, ktype: &'a DataType) -> DeltaResult<()> {
        self.transform_inner("<map key>", |this| this.transform(ktype))
    }
    fn transform_map_value(&mut self, vtype: &'a DataType) -> DeltaResult<()> {
        self.transform_inner("<map value>", |this| this.transform(vtype))
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> DeltaResult<()> {
        self.transform_inner(field.name(), |this| {
            get_field_column_mapping_info(field, this.mode, &this.path, Some(&mut this.seen))?;
            this.recurse_into_struct_field(field)
        })
    }
    fn transform_variant(&mut self, _stype: &'a StructType) -> DeltaResult<()> {
        // don't recurse into variant's fields, as they are not expected to have column mapping
        // annotations
        // TODO: this changes with icebergcompat right? see issue#1125 for icebergcompat.
        Ok(())
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

/// Assigns column mapping metadata (id and physicalName) to all fields in `schema`, and also
/// assigns `delta.columnMapping.nested.ids` on Array/Map fields when
/// `assign_nested_field_ids` is `true`.
///
/// This function recursively processes all fields in the schema, including nested structs,
/// arrays, and maps. Each field is assigned a new unique ID and physical name. When
/// `assign_nested_field_ids` is true, each element/key/value in Array/Map is assigned a new
/// unique parquet field id and stored in the `delta.columnMapping.nested.ids` metadata of the
/// nearest ancestor `StructField`.
///
/// Fields with pre-existing column mapping metadata (id or physicalName) are rejected to avoid
/// conflicts.
///
/// # Arguments
///
/// * `schema` - The schema to transform.
/// * `max_id` - Tracks the highest column ID assigned. Updated in place. Should be initialized to
///   `0` for a new table.
/// * `assign_nested_field_ids` - When `true`, also allocates IDs for synthetic `Array.element` and
///   `Map.key`/`Map.value` fields, stores them in `delta.columnMapping.nested.ids`, and includes
///   them in `max_id`. Assigning IDs to these nested fields is a hard requirement for
///   IcebergCompatV2/3.
///
/// # Returns
///
/// A new schema with column mapping metadata on all fields.
///
/// # Example
///
/// Given a top-level field `m: map<list<int>, int>`, after calling with
/// `assign_nested_field_ids = true` the field gets (`<pname>` is the assigned UUID-based
/// physical name):
///
/// ```json
/// {
///   "delta.columnMapping.id": 1,
///   "delta.columnMapping.physicalName": "<pname>",
///   "delta.columnMapping.nested.ids": {
///     "<pname>.key":         2,
///     "<pname>.key.element": 3,
///     "<pname>.value":       4
///   }
/// }
/// ```
///
/// `max_id` ends at `4`. With `assign_nested_field_ids = false` the same field gets only the
/// CM metadata and `max_id` ends at `1`:
///
/// ```json
/// {
///   "delta.columnMapping.id": 1,
///   "delta.columnMapping.physicalName": "<pname>"
/// }
/// ```
pub(crate) fn assign_column_mapping_metadata(
    schema: &StructType,
    max_id: &mut i64,
    assign_nested_field_ids: bool,
) -> DeltaResult<StructType> {
    // Assign CM id + physical name to every StructField (top-level and nested).
    // `delta.columnMapping.nested.ids` is assigned later separately, this is to
    // align with delta-spark's behavior.
    let new_fields: Vec<StructField> = schema
        .fields()
        .map(|field| try_assign_flat_column_mapping_info(field, max_id))
        .collect::<DeltaResult<Vec<_>>>()?;
    let with_flat_cm_info = StructType::try_new(new_fields)?;
    if !assign_nested_field_ids {
        return Ok(with_flat_cm_info);
    }
    // Then populate `delta.columnMapping.nested.ids` per StructField.
    assign_nested_cm_ids(&with_flat_cm_info, max_id)
}

/// JSON object holding [`ColumnMetadataKey::ColumnMappingNestedIds`] entries for an Array/Map
/// field.
type NestedFieldIds = serde_json::Map<String, serde_json::Value>;

/// Assigns flat column mapping metadata(id and physical name) to a single field, recursively
/// processing nested types. Returns a new field with a fresh unique ID and a UUID-based physical
/// name, and increments `max_id` to reflect the assignment.
///
/// `delta.columnMapping.nested.ids` is not assigned here.
///
/// # Errors
///
/// - Field carries a pre-existing `delta.columnMapping.id`, `delta.columnMapping.physicalName`,
///   `delta.columnMapping.nested.ids`, or `parquet.field.nested.ids` annotation.
pub(crate) fn try_assign_flat_column_mapping_info(
    field: &StructField,
    max_id: &mut i64,
) -> DeltaResult<StructField> {
    for key in [
        ColumnMetadataKey::ColumnMappingId,
        ColumnMetadataKey::ColumnMappingPhysicalName,
        ColumnMetadataKey::ColumnMappingNestedIds,
        ColumnMetadataKey::ParquetFieldNestedIds,
    ] {
        if field.get_config_value(&key).is_some() {
            return Err(Error::generic(format!(
                "Field '{}' has pre-populated `{}` metadata; the caller must not provide \
                 column-mapping annotations on input fields.",
                field.name,
                key.as_ref(),
            )));
        }
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
    new_field.data_type = flat_cm_info_for_nested_data_type(&field.data_type, max_id)?;
    Ok(new_field)
}

/// Recurses through `data_type`, descending into struct/array/map members so any nested
/// StructFields receive CM id + physical name via [`assign_column_mapping_metadata`].
fn flat_cm_info_for_nested_data_type(
    data_type: &DataType,
    max_id: &mut i64,
) -> DeltaResult<DataType> {
    match data_type {
        DataType::Struct(inner) => {
            let new_inner = assign_column_mapping_metadata(
                inner, max_id, /* assign_nested_field_ids */ false,
            )?;
            Ok(DataType::Struct(Box::new(new_inner)))
        }
        DataType::Array(array_type) => {
            let new_element_type =
                flat_cm_info_for_nested_data_type(array_type.element_type(), max_id)?;
            Ok(DataType::Array(Box::new(ArrayType::new(
                new_element_type,
                array_type.contains_null(),
            ))))
        }
        DataType::Map(map_type) => {
            let new_key_type = flat_cm_info_for_nested_data_type(map_type.key_type(), max_id)?;
            let new_value_type = flat_cm_info_for_nested_data_type(map_type.value_type(), max_id)?;
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

/// Walks `schema` (already carrying CM id + physical name on every StructField) and populates
/// `delta.columnMapping.nested.ids` on Array/Map fields.
///
/// `delta.columnMapping.nested.ids` is a JSON object on a StructField that records the parquet
/// field ids for the synthetic Array `element` and Map `key`/`value` slots inside that field's
/// subtree. Keys are dotted paths anchored at the field's physical name; values are parquet field
/// ids.
///
/// Example for `m: Map<List<int>, int>` with physical name `<phys>`:
///
/// ```json
/// {
///   "<phys>.key":         2,
///   "<phys>.key.element": 3,
///   "<phys>.value":       4
/// }
/// ```
fn assign_nested_cm_ids(schema: &StructType, max_id: &mut i64) -> DeltaResult<StructType> {
    fn walk(
        data_type: &DataType,
        max_id: &mut i64,
        path: &str,
        nested_ids: &mut NestedFieldIds,
    ) -> DeltaResult<DataType> {
        match data_type {
            DataType::Struct(inner) => Ok(DataType::Struct(Box::new(assign_nested_cm_ids(
                inner, max_id,
            )?))),
            DataType::Array(array_type) => {
                let element_path = format!("{path}.element");
                *max_id += 1;
                nested_ids.insert(element_path.clone(), serde_json::Value::from(*max_id));
                let new_element =
                    walk(array_type.element_type(), max_id, &element_path, nested_ids)?;
                Ok(DataType::Array(Box::new(ArrayType::new(
                    new_element,
                    array_type.contains_null(),
                ))))
            }
            DataType::Map(map_type) => {
                let key_path = format!("{path}.key");
                let value_path = format!("{path}.value");
                *max_id += 1;
                nested_ids.insert(key_path.clone(), serde_json::Value::from(*max_id));
                let new_key = walk(map_type.key_type(), max_id, &key_path, nested_ids)?;
                *max_id += 1;
                nested_ids.insert(value_path.clone(), serde_json::Value::from(*max_id));
                let new_value = walk(map_type.value_type(), max_id, &value_path, nested_ids)?;
                Ok(DataType::Map(Box::new(MapType::new(
                    new_key,
                    new_value,
                    map_type.value_contains_null(),
                ))))
            }
            DataType::Primitive(_) | DataType::Variant(_) => Ok(data_type.clone()),
        }
    }

    let new_fields: Vec<StructField> = schema
        .fields()
        .map(|field| {
            let physical = expect_physical_name(field)?;
            let mut nested_ids = NestedFieldIds::new();
            let new_dt = walk(&field.data_type, max_id, &physical, &mut nested_ids)?;
            let mut new_field = field.clone();
            new_field.data_type = new_dt;
            if !nested_ids.is_empty() {
                insert_nested_field_ids_metadata(&mut new_field, nested_ids);
            }
            Ok(new_field)
        })
        .collect::<DeltaResult<Vec<_>>>()?;
    StructType::try_new(new_fields)
}

fn expect_physical_name(field: &StructField) -> DeltaResult<String> {
    match field
        .metadata
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        Some(MetadataValue::String(s)) => Ok(s.clone()),
        _ => Err(Error::internal_error(format!(
            "nested-id assignment requires every StructField to already carry a physical \
             name; '{}' is missing one",
            field.name,
        ))),
    }
}

/// Sets the collected nested ids on `field` under the `delta.columnMapping.nested.ids` key.
fn insert_nested_field_ids_metadata(field: &mut StructField, ids: NestedFieldIds) {
    field.metadata.insert(
        ColumnMetadataKey::ColumnMappingNestedIds
            .as_ref()
            .to_string(),
        MetadataValue::Other(serde_json::Value::Object(ids)),
    );
}

/// Returns the largest column mapping id found anywhere in `schema`. This includes both
/// per-field `delta.columnMapping.id` annotations and the nested ids in
/// `delta.columnMapping.nested.ids` metadata.
pub(crate) fn find_max_column_id_in_schema(schema: &StructType) -> Option<i64> {
    let mut visitor = MaxColumnId(None);
    visitor.transform_struct(schema);
    visitor.0
}

/// Visitor that walks a schema and records the largest column mapping id seen on any field,
/// counting both `delta.columnMapping.id` (per-field) and the integer values inside any
/// `delta.columnMapping.nested.ids` JSON map (for element/key/value of Array/Map).
struct MaxColumnId(Option<i64>);

impl MaxColumnId {
    fn observe(&mut self, n: i64) {
        self.0 = Some(self.0.map_or(n, |prev| prev.max(n)));
    }
}

impl<'a> SchemaTransform<'a> for MaxColumnId {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, field: &'a StructField) {
        if let Some(n) = field.column_mapping_id() {
            self.observe(n);
        }
        // `delta.columnMapping.nested.ids` is a JSON object set on Array/Map fields. Shape: {
        // "<phys>.key": <id>, "<phys>.value": <id>, "<phys>.key.element": <id>, ... }
        // (string paths -> nested column-mapping ids). See the definition of
        // `ColumnMetadataKey::ColumnMappingNestedIds` for more details.
        if let Some(MetadataValue::Other(serde_json::Value::Object(obj))) = field
            .metadata()
            .get(ColumnMetadataKey::ColumnMappingNestedIds.as_ref())
        {
            for v in obj.values() {
                if let Some(n) = v.as_i64() {
                    self.observe(n);
                }
            }
        }
        // Recurse into the field's data type so we also visit nested struct/array/map members.
        self.recurse_into_struct_field(field)
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

    #[test]
    fn test_assign_column_mapping_metadata_simple() {
        let schema = StructType::new_unchecked([
            StructField::new("a", DataType::INTEGER, false),
            StructField::new("b", DataType::STRING, true),
        ]);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("pre-populated") && err_msg.contains("delta.columnMapping.id"),
            "Expected error naming the pre-populated CM annotation, got: {err_msg}"
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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

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

    /// Nested-ids JSON entries (assigned to element/key/value of Array/Map) are real column-mapping
    /// ids and must beat the per-field `delta.columnMapping.id` when larger.
    #[test]
    fn find_max_column_id_picks_up_nested_ids_metadata() {
        let mut field = field_with_id(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::INTEGER,
                DataType::INTEGER,
                false,
            ))),
            1,
        );
        field.metadata.insert(
            ColumnMetadataKey::ColumnMappingNestedIds
                .as_ref()
                .to_string(),
            MetadataValue::Other(serde_json::json!({
                "m.key":   2,
                "m.value": 99,
            })),
        );
        let schema = StructType::try_new(vec![field]).unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(99));
    }
}
