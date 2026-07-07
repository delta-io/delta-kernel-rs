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
    ArrayType, ColumnMetadataKey, ColumnName, DataType, ExistingColumnMappingAnnotations,
    MakePhysical, MapType, MetadataValue, Schema, StructField, StructType,
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

/// Maximum legal `delta.columnMapping.id` value per the Delta protocol, which restricts the id
/// to a 32-bit non-negative integer
/// (see <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping>). Kernel
/// stores the id in `i64` end-to-end (`MetadataValue::Number` is `i64`) but enforces this bound
/// at the validation and allocation entry points so writers cannot persist out-of-range ids.
pub(crate) const MAX_COLUMN_MAPPING_ID: i64 = i32::MAX as i64;

/// Validates that a `delta.columnMapping.id` value lies in the protocol-permitted range
/// `0..=`[`MAX_COLUMN_MAPPING_ID`]. Out-of-range ids (negative or above `i32::MAX`) are rejected
/// with a single canonical message naming the offending value and the protocol bound. Callers
/// that want to add context (e.g. the offending field name or table-property name) wrap with
/// `.map_err(|e| Error::schema(format!("Field '{name}': {e}")))?` -- matching the kernel
/// convention used by sibling validators in `schema/validation.rs`.
pub(crate) fn validate_column_mapping_id(id: i64) -> DeltaResult<()> {
    if (0..=MAX_COLUMN_MAPPING_ID).contains(&id) {
        return Ok(());
    }
    let key = ColumnMetadataKey::ColumnMappingId.as_ref();
    Err(Error::schema(format!(
        "Invalid column mapping id {id}: the Delta protocol restricts \
         `{key}` to a 32-bit non-negative integer (max {MAX_COLUMN_MAPPING_ID}).",
    )))
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

/// Validates `delta.columnMapping.id` and `delta.columnMapping.physicalName` annotations across
/// every field in `schema`. Aligns with delta-spark's validation logic.
///
/// When `mode` is [`ColumnMappingMode::Id`] or [`ColumnMappingMode::Name`]: each field must
/// carry both annotations, no two fields may share a `delta.columnMapping.id`, and no two
/// fields may share a *full physical column path*. Two fields may share the same leaf
/// `physicalName` if they live at different physical column paths.
///
/// When `mode` is [`ColumnMappingMode::None`]: verifies no field carries either annotation.
///
/// Examples for physical name validation:
/// Rejected (two siblings share `delta.columnMapping.physicalName="x"`):
/// ```json
/// {"type":"struct","fields":[
///   {"name":"a","type":"long","nullable":true,
///    "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"x"}},
///   {"name":"b","type":"long","nullable":true,
///    "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"x"}}
/// ]}
/// ```
///
/// Accepted (same `delta.columnMapping.physicalName="x"` at different physical column paths):
/// ```json
/// {"type":"struct","fields":[
///   {"name":"a","type":"long","nullable":true,
///    "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"x"}},
///   {"name":"nested","nullable":true,
///    "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"nested"},
///    "type":{"type":"struct","fields":[
///      {"name":"a","type":"long","nullable":true,
///       "metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"x"}}
///   ]}}
/// ]}
/// ```
pub fn validate_schema_column_mapping(schema: &Schema, mode: ColumnMappingMode) -> DeltaResult<()> {
    MakePhysical::validate_schema_column_mapping(mode, schema)
}

/// How to treat a stale `delta.columnMapping.*` annotation found on a field while column mapping
/// is disabled ([`ColumnMappingMode::None`]).
///
/// A table can carry these annotations after column mapping was enabled and then disabled. They
/// are inert while mapping is off -- physical names resolve to logical names -- so the read path
/// tolerates them (matching delta-spark, which ignores them on read), while the write path rejects
/// them so kernel never persists a table in that shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StaleAnnotationPolicy {
    /// Ignore a stale annotation and resolve the field by its logical name.
    Ignore,
    /// Reject a stale annotation with a schema error.
    Reject,
}

/// Validates a field's column mapping annotations and extracts the physical name and column
/// mapping id. If `seen_ids` is provided, also checks that this field's
/// `delta.columnMapping.id` is globally unique. If `current_field_siblings` is provided, also
/// checks that this field's `delta.columnMapping.physicalName` is unique among its siblings.
///
/// Metadata columns are not subject to column mapping and must not carry column mapping
/// annotations. Returns the logical field name and `None` for such fields.
///
/// When column mapping is enabled (`Id` or `Name`), the field must have a
/// `delta.columnMapping.physicalName` (string) and `delta.columnMapping.id` (number) annotation.
/// Returns the physical name and `Some(id)`.
///
/// When disabled (`None`), a stale annotation is handled per `stale_policy`:
/// [`StaleAnnotationPolicy::Ignore`] resolves the field by its logical name (returning the logical
/// name and `None`), while [`StaleAnnotationPolicy::Reject`] errors. Either way, in `None` mode no
/// dedup is performed (the returned "physical name" is just the logical field name and is only
/// schema-unique within its parent struct, not globally).
///
/// # Parameters
///
/// - `field`: The field to validate.
/// - `mode`: Column mapping mode.
/// - `stale_policy`: In `None` mode, whether to ignore or reject a stale `delta.columnMapping.*`
///   annotation. Unused when mapping is enabled.
/// - `parent_field_logical_path`: The field's parent path, used to render full paths in error
///   messages (e.g. parent `&["a", "b"]` and field `c` renders as `a.b.c`).
/// - `seen_ids`: Global map of `delta.columnMapping.id` -> first claimer logical name. `None` skips
///   the ID-dedup check.
/// - `current_field_siblings`: Map of `delta.columnMapping.physicalName` -> first claimer logical
///   name *for the current field's siblings only*. `None` skips the sibling-dedup check.
///
/// # Errors
///
/// - The field is a metadata column carrying any CM annotation.
/// - CM is enabled but `delta.columnMapping.physicalName` is missing or non-string.
/// - CM is enabled but `delta.columnMapping.id` is missing or non-numeric.
/// - CM is disabled, `stale_policy` is [`StaleAnnotationPolicy::Reject`], and either annotation is
///   present.
/// - `seen_ids` is provided and the current field's `delta.columnMapping.id` is already in the map.
///   Example rejection (two fields share `delta.columnMapping.id=1`):
///
///   ```json
///   {"type":"struct","fields":[
///     {"name":"a","type":"long","nullable":true,
///      "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}},
///     {"name":"b","type":"long","nullable":true,
///      "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-b"}}
///   ]}
///   ```
/// - `current_field_siblings` is provided and the current field's
///   `delta.columnMapping.physicalName` is already claimed by a sibling. Example rejection (two
///   siblings share `delta.columnMapping.physicalName="x"`):
///
///   ```json
///   {"type":"struct","fields":[
///     {"name":"a","type":"long","nullable":true,
///      "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"x"}},
///     {"name":"b","type":"long","nullable":true,
///      "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"x"}}
///   ]}
///   ```
pub(crate) fn validate_and_extract_column_mapping_annotations<'a>(
    field: &'a StructField,
    mode: ColumnMappingMode,
    stale_policy: StaleAnnotationPolicy,
    parent_field_logical_path: &[&'a str],
    seen_ids: Option<&mut HashMap<i64, &'a str>>,
    current_field_siblings: Option<&mut HashMap<&'a str, &'a str>>,
) -> DeltaResult<(&'a str, Option<i64>)> {
    let logical_field_path = || {
        ColumnName::new(
            parent_field_logical_path
                .iter()
                .copied()
                .chain([field.name().as_str()]),
        )
    };
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
                logical_field_path(),
            )));
        }
        (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
            return Err(Error::schema(format!(
                "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                logical_field_path(),
            )));
        }
        (ColumnMappingMode::None, Some(_)) => match stale_policy {
            StaleAnnotationPolicy::Ignore => field.name(),
            StaleAnnotationPolicy::Reject => {
                return Err(Error::schema(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                    logical_field_path(),
                )));
            }
        },
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
                logical_field_path(),
            )));
        }
        (ColumnMappingMode::Name | ColumnMappingMode::Id, None) => {
            return Err(Error::schema(format!(
                "Column mapping is enabled but field '{}' lacks the {annotation} annotation",
                logical_field_path(),
            )));
        }
        (ColumnMappingMode::None, Some(_)) => match stale_policy {
            StaleAnnotationPolicy::Ignore => None,
            StaleAnnotationPolicy::Reject => {
                return Err(Error::schema(format!(
                    "Column mapping is not enabled but field '{}' is annotated with {annotation}",
                    logical_field_path(),
                )));
            }
        },
    };

    // CM-disabled mode synthesizes `physical_name = field.name()`, which only has to be unique
    // within its parent struct, not globally -- so dedup is gated on CM being enabled. ID dedup
    // additionally requires `Some(id)` because `id` is `None` outside CM-enabled mode.
    if mode != ColumnMappingMode::None {
        if let (Some(id), Some(seen_ids)) = (id, seen_ids) {
            seen_ids.insert(id, field.name()).map_or(Ok(()), |prev| {
                Err(Error::schema(format!(
                    "Duplicate column mapping ID {id} assigned to both '{prev}' and '{}'",
                    field.name()
                )))
            })?;
        }
        // Dedup `physicalName` among siblings. Two distinct columns with the same full
        // physical path must diverge at some ancestor struct, where two siblings share
        // the same `physicalName`.
        if let Some(siblings) = current_field_siblings {
            siblings
                .insert(physical_name.as_str(), field.name().as_str())
                .map_or(Ok(()), |prev| {
                    Err(Error::schema(format!(
                        "Duplicate `delta.columnMapping.physicalName` '{physical_name}' \
                         assigned to both '{}' and '{}'",
                        ColumnName::new(parent_field_logical_path.iter().copied().chain([prev])),
                        logical_field_path(),
                    )))
                })?;
        }
    }

    Ok((physical_name, id))
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
/// Pre-populated `delta.columnMapping.nested.ids` and `parquet.field.nested.ids` annotations
/// are always rejected: those payloads are kernel-managed and only emitted by the nested-ids
/// assignment pass below.
///
/// When `assign_nested_field_ids` is `true`, after the per-field flat assignment this function
/// also allocates fresh parquet field ids for the synthetic `Array.element`,
/// `Map.key`, and `Map.value` slots and stores them under
/// `delta.columnMapping.nested.ids` on the nearest ancestor `StructField`. Allocating these
/// nested ids is a hard requirement for IcebergCompatV2/3.
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
///   [`find_max_column_id_in_schema`] (or `0` for a brand-new table with no preserved IDs anywhere)
///   so newly assigned IDs cannot collide with preserved ones.
/// * `assign_nested_field_ids` - When `true`, also allocates IDs for synthetic `Array.element` and
///   `Map.key`/`Map.value` fields, stores them in `delta.columnMapping.nested.ids`, and includes
///   them in `max_id`. Assigning IDs to these nested fields is a hard requirement for
///   IcebergCompatV2/3.
///
/// # Returns
///
/// A new schema with column mapping metadata present on every field.
///
/// # Example
///
/// Given a top-level field `m: map<list<int>, int>` with no pre-existing annotations, after
/// calling with `assign_nested_field_ids = true` the field gets (`<pname>` is the assigned
/// UUID-based physical name):
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
/// flat CM metadata and `max_id` ends at `1`:
///
/// ```json
/// {
///   "delta.columnMapping.id": 1,
///   "delta.columnMapping.physicalName": "<pname>"
/// }
/// ```
#[delta_kernel_derive::internal_api]
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

/// Advances `max_id` by one and returns the new value, surfacing `i64` overflow as an error.
///
/// `assign_column_mapping_metadata` allocates fresh column-mapping IDs as `max_id + 1`. A
/// connector that preserves an `id` near `i64::MAX` and then asks kernel to allocate a fresh
/// sibling would silently wrap to `i64::MIN` without this guard, producing a negative id that
/// would then trip the negative-id check on the next round-trip.
///
/// In practice this should be unreachable: the Delta protocol restricts `columnMapping.id` to a
/// 32-bit non-negative integer (`i32::MAX` ~ 2.1B), so anything near `i64::MAX` (~ 9.2
/// quintillion) is over four billion times the protocol-permitted maximum and points at a bug
/// in the connector's id allocator rather than legitimate id exhaustion. The error message
/// reflects that diagnosis.
fn next_column_mapping_id(max_id: &mut i64) -> DeltaResult<i64> {
    let next = max_id.checked_add(1).ok_or_else(|| {
        Error::generic(format!(
            "Cannot allocate column mapping id: `max_id + 1` overflows `i64` \
             (max_id={current}). The Delta protocol caps `delta.columnMapping.id` at a \
             32-bit non-negative integer; fix the upstream id allocator producing \
             out-of-range preserved ids.",
            current = *max_id,
        ))
    })?;
    if next > MAX_COLUMN_MAPPING_ID {
        return Err(Error::generic(format!(
            "Cannot allocate column mapping id {next}: exceeds the Delta protocol's 32-bit \
             non-negative maximum ({MAX_COLUMN_MAPPING_ID}). A preserved \
             `delta.columnMapping.id` is at the protocol cap; lower the largest preserved \
             id so kernel can allocate a sibling within range.",
        )));
    }
    *max_id = next;
    Ok(next)
}

/// Assigns flat column mapping metadata (id and physical name) to a single field, recursively
/// processing nested types. Implements the 3-way preserve/fill/assign behavior described on
/// [`assign_column_mapping_metadata`], matching delta-spark. The returned field always has both
/// `delta.columnMapping.id` and `delta.columnMapping.physicalName` present.
///
/// `delta.columnMapping.nested.ids` is not assigned here; the separate nested-ids pass run by
/// [`assign_column_mapping_metadata`] handles that.
///
/// `max_id` is advanced as follows:
/// - When a `delta.columnMapping.id` is preserved, `*max_id = max(*max_id, preserved_id)`.
/// - When a new id is allocated, `*max_id` is incremented via [`next_column_mapping_id`] (which
///   uses `checked_add`) and the new id is the post-increment value.
///
/// # Errors
///
/// - Field carries a pre-existing `delta.columnMapping.nested.ids` or `parquet.field.nested.ids`
///   annotation (those payloads are kernel-managed).
/// - A pre-existing `delta.columnMapping.id` annotation is wrong-typed or negative.
/// - A pre-existing `delta.columnMapping.physicalName` annotation is wrong-typed or empty.
/// - Allocating a fresh id would overflow `i64`.
pub(crate) fn try_assign_flat_column_mapping_info(
    field: &StructField,
    max_id: &mut i64,
) -> DeltaResult<StructField> {
    for key in [
        ColumnMetadataKey::ColumnMappingNestedIds,
        ColumnMetadataKey::ParquetFieldNestedIds,
    ] {
        if field.get_config_value(&key).is_some() {
            return Err(Error::generic(format!(
                "Field '{}' has pre-populated `{}` metadata; this annotation is \
                 kernel-managed and must not be supplied on input fields.",
                field.name,
                key.as_ref(),
            )));
        }
    }

    let ExistingColumnMappingAnnotations { id, physical_name } =
        field.validate_and_extract_existing_column_mapping_annotations()?;

    let mut new_field = field.clone();

    match (id, physical_name) {
        // Both present: preserve verbatim and advance `max_id` past the preserved id.
        (Some(preserved_id), Some(_)) => {
            *max_id = (*max_id).max(preserved_id);
        }
        // Only `id` present: preserve id, fill in physical name.
        (Some(preserved_id), None) => {
            *max_id = (*max_id).max(preserved_id);
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                MetadataValue::String(format!("col-{}", Uuid::new_v4())),
            );
        }
        // Only `physicalName` present: preserve name, allocate id.
        (None, Some(_)) => {
            let new_id = next_column_mapping_id(max_id)?;
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                MetadataValue::Number(new_id),
            );
        }
        // Neither present: assign both.
        (None, None) => {
            let new_id = next_column_mapping_id(max_id)?;
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                MetadataValue::Number(new_id),
            );
            new_field.metadata.insert(
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                MetadataValue::String(format!("col-{}", Uuid::new_v4())),
            );
        }
    }

    // Recursively process nested types (struct/array/map descend; primitive/variant pass through).
    new_field.data_type = flat_cm_info_for_nested_data_type(&field.data_type, max_id)?;

    Ok(new_field)
}

/// Process nested data types to assign flat column mapping metadata to any nested struct fields.
fn flat_cm_info_for_nested_data_type(
    data_type: &DataType,
    max_id: &mut i64,
) -> DeltaResult<DataType> {
    match data_type {
        DataType::Struct(inner) => {
            let new_inner = assign_column_mapping_metadata(
                inner, max_id, /* assign_nested_field_ids */ false,
            )?;
            Ok(DataType::from(new_inner))
        }
        DataType::Array(array_type) => {
            let new_element_type =
                flat_cm_info_for_nested_data_type(array_type.element_type(), max_id)?;
            Ok(DataType::from(ArrayType::new(
                new_element_type,
                array_type.contains_null(),
            )))
        }
        DataType::Map(map_type) => {
            let new_key_type = flat_cm_info_for_nested_data_type(map_type.key_type(), max_id)?;
            let new_value_type = flat_cm_info_for_nested_data_type(map_type.value_type(), max_id)?;
            Ok(DataType::from(MapType::new(
                new_key_type,
                new_value_type,
                map_type.value_contains_null(),
            )))
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
            DataType::Struct(inner) => Ok(DataType::from(assign_nested_cm_ids(inner, max_id)?)),
            DataType::Array(array_type) => {
                let element_path = format!("{path}.element");
                *max_id += 1;
                nested_ids.insert(element_path.clone(), serde_json::Value::from(*max_id));
                let new_element =
                    walk(array_type.element_type(), max_id, &element_path, nested_ids)?;
                Ok(DataType::from(ArrayType::new(
                    new_element,
                    array_type.contains_null(),
                )))
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
                Ok(DataType::from(MapType::new(
                    new_key,
                    new_value,
                    map_type.value_contains_null(),
                )))
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

// Get the physical name for a field. Error if the field is missing the physical name annotation.
fn expect_physical_name(field: &StructField) -> DeltaResult<String> {
    match field
        .metadata
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
    {
        Some(MetadataValue::String(s)) => Ok(s.clone()),
        _ => Err(Error::internal_error(format!(
            "Expect field '{}' to have a physical name annotation",
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
#[delta_kernel_derive::internal_api]
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
    let fields = schema.fields_of_path(col_name)?;
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
    let mut fields = vec![];
    logical_schema.visit_fields_of_path_by(
        physical_col,
        |s, phys_name| {
            s.fields()
                .find(|f| f.physical_name(column_mapping_mode) == phys_name)
        },
        |field| fields.push(field),
    )?;
    Ok(ColumnName::new(fields.iter().map(|f| &f.name)))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::expressions::ColumnName;
    use crate::schema::{DataType, MetadataValue, StructField, StructType};
    use crate::utils::test_utils::{
        assert_result_error_with_message, column_mapping_physical_name_dedup_fixtures as fixtures,
        make_test_tc, test_deep_nested_schema_missing_leaf_cm,
    };
    use crate::utils::FoldWithOption as _;

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

    // `validate_schema_column_mapping` is the strict entry point (used by the write path), so a
    // stale annotation on a mapping-disabled table is rejected. The read path's leniency is
    // covered by `test_validate_and_extract_tolerates_stale_annotations_when_disabled`.
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

    /// A field carrying a stale `delta.columnMapping.*` annotation while mapping is disabled is
    /// tolerated under [`StaleAnnotationPolicy::Ignore`] (the read path) -- it resolves to the
    /// logical name with no id -- and rejected under [`StaleAnnotationPolicy::Reject`] (the write
    /// path).
    #[rstest::rstest]
    #[case::physical_name_only(None, Some("col-abc"))]
    #[case::id_only(Some(7), None)]
    #[case::both(Some(7), Some("col-abc"))]
    fn test_validate_and_extract_tolerates_stale_annotations_when_disabled(
        #[case] stale_id: Option<i64>,
        #[case] stale_physical_name: Option<&str>,
    ) {
        let mut metadata: Vec<(&str, MetadataValue)> = Vec::new();
        if let Some(id) = stale_id {
            metadata.push((
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(id),
            ));
        }
        if let Some(name) = stale_physical_name {
            metadata.push((
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String(name.to_string()),
            ));
        }
        let field = StructField::not_null("stale", DataType::INTEGER).with_metadata(metadata);

        // Ignore: resolves to the logical name, no id.
        let (physical_name, id) = validate_and_extract_column_mapping_annotations(
            &field,
            ColumnMappingMode::None,
            StaleAnnotationPolicy::Ignore,
            &[],
            None,
            None,
        )
        .unwrap();
        assert_eq!(physical_name, "stale");
        assert_eq!(id, None);

        // Reject: errors, naming the field.
        assert_result_error_with_message(
            validate_and_extract_column_mapping_annotations(
                &field,
                ColumnMappingMode::None,
                StaleAnnotationPolicy::Reject,
                &[],
                None,
                None,
            )
            .map(|_| ()),
            "Column mapping is not enabled but field 'stale'",
        );
    }

    #[test]
    fn test_annotation_validation_reaches_struct_fields_in_map_value() {
        let unannotated =
            StructType::new_unchecked([StructField::new("x", DataType::INTEGER, false)]);
        let schema = StructType::new_unchecked([make_cm_field(
            "b",
            1,
            MapType::new(DataType::STRING, unannotated, false),
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
        StructType::new_unchecked([make_cm_field("outer", 10, nested)])
    }

    fn cm_schema_cross_level_duplicates() -> StructType {
        let nested = StructType::new_unchecked([make_cm_field("inner", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field("b", 2, nested),
        ])
    }

    fn cm_schema_array_duplicates() -> StructType {
        let element = StructType::new_unchecked([make_cm_field("x", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field("b", 2, ArrayType::new(element, false)),
        ])
    }

    fn cm_schema_map_duplicates() -> StructType {
        let value = StructType::new_unchecked([make_cm_field("x", 1, DataType::INTEGER)]);
        StructType::new_unchecked([
            make_cm_field("a", 1, DataType::INTEGER),
            make_cm_field("b", 2, MapType::new(DataType::STRING, value, false)),
        ])
    }

    #[rstest::rstest]
    #[case::same_level(cm_schema_same_level_duplicates())]
    #[case::nested_struct(cm_schema_nested_duplicates())]
    #[case::across_nesting_levels(cm_schema_cross_level_duplicates())]
    #[case::across_array(cm_schema_array_duplicates())]
    #[case::across_map(cm_schema_map_duplicates())]
    fn test_duplicate_column_mapping_ids_rejected(#[case] schema: StructType) {
        assert_result_error_with_message(
            validate_schema_column_mapping(&schema, ColumnMappingMode::Id),
            "Duplicate column mapping ID",
        );
    }

    #[test]
    fn test_duplicate_column_mapping_ids_rejected_in_name_mode() {
        assert_result_error_with_message(
            validate_schema_column_mapping(
                &cm_schema_same_level_duplicates(),
                ColumnMappingMode::Name,
            ),
            "Duplicate column mapping ID",
        );
    }

    #[rstest::rstest]
    #[case::accepted_distinct_paths(fixtures::same_phy_name_different_paths(), None)]
    #[case::rejected_deeply_nested_repeat(
        fixtures::deeply_nested_repeat_physical_paths(),
        Some({
            let (a, b) =
                fixtures::deeply_nested_collider_paths();
            format!("assigned to both '{a}' and '{b}'")
        }),
    )]
    #[case::multiple_violations_reports_first(
        fixtures::multiple_physical_name_collisions(),
        Some("'p' assigned to both 'a' and 'b'".to_string()),
    )]
    fn test_dup_physical_name(
        #[case] schema: StructType,
        #[case] expected_error_substring: Option<String>,
    ) {
        // The same dedup rules should apply under both CM modes.
        for mode in [ColumnMappingMode::Name, ColumnMappingMode::Id] {
            let result = validate_schema_column_mapping(&schema, mode);
            match &expected_error_substring {
                None => {
                    result.expect("schema must validate");
                }
                Some(substr) => {
                    assert_result_error_with_message(result.as_ref().map(|_| ()), substr);
                    // For the multiple-violations case, also confirm the deeper site was never
                    // reported.
                    if let Err(e) = &result {
                        assert!(
                            !e.to_string().contains("'q'"),
                            "walker must short-circuit on first collision under {mode:?}; got: {e}"
                        );
                    }
                }
            }
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
                    StructField::nullable("outer", inner),
                ]);
                (schema, path(&["outer", FIELD_UNDER_TEST]))
            }
            SchemaShape::DeeplyNestedStruct => {
                let innermost = StructType::new_unchecked([field_under_test]);
                let middle =
                    StructType::new_unchecked([StructField::nullable("middle", innermost)]);
                let schema = StructType::new_unchecked([
                    StructField::nullable("unannotated_sibling", DataType::STRING),
                    StructField::nullable("outer", middle),
                ]);
                (schema, path(&["outer", "middle", FIELD_UNDER_TEST]))
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
            SchemaShape::DeeplyNestedStruct
        )]
        shape: SchemaShape,
    ) {
        let field_under_test = make_field_under_test(pre_id, pre_name);
        let (schema, field_under_test_path) =
            build_schema_with_field_under_test(shape, field_under_test);

        let seed = find_max_column_id_in_schema(&schema).unwrap_or(0);
        let mut max_id = seed;
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();

        let result_field = result.field_at_path(&field_under_test_path);
        let result_id = result_field
            .column_mapping_id()
            .expect("expected numeric column mapping id");
        let result_name = expect_physical_name(result_field).unwrap();

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

        let result = assign_column_mapping_metadata(&schema, &mut max_id, false).unwrap();
        // Three unannotated fields -> +3 ids above the seed.
        assert_eq!(max_id, preserved_max + UNANNOTATED_FIELD_NAMES.len() as i64);

        // Every assigned id on an unannotated field exceeds the preserved max.
        for name in UNANNOTATED_FIELD_NAMES {
            let field = result.field(name).unwrap();
            let id = field
                .column_mapping_id()
                .expect("expected numeric column mapping id");
            assert!(
                id > preserved_max,
                "newly assigned id {id} for '{name}' must exceed preserved max {preserved_max}",
            );
        }

        // Preserved ids round-trip.
        for (i, expected_id) in preserved_ids.iter().enumerate() {
            let name = format!("preserved_{i}");
            assert_eq!(
                result
                    .field(&name)
                    .unwrap()
                    .column_mapping_id()
                    .expect("expected numeric column mapping id"),
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
        "Invalid column mapping id",
        ColumnMetadataKey::ColumnMappingId,
    )]
    #[case::id_i64_min_no_name(
        Some(MetadataValue::Number(i64::MIN)),
        None,
        "Invalid column mapping id",
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
            SchemaShape::DeeplyNestedStruct
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
        let err = assign_column_mapping_metadata(&schema, &mut max_id, false)
            .unwrap_err()
            .to_string();
        let key = offending_key.as_ref();
        assert!(
            err.contains(violation_kind) && err.contains(key),
            "Expected '{violation_kind}' and '{key}' in error, got: {err}",
        );
    }

    /// `next_column_mapping_id` must reject `i64` overflow rather than wrapping to a negative
    /// id. Both fresh-allocation arms (`physicalName`-only and neither-preserved) flow through
    /// it, so each is exercised here. Without `checked_add` a connector that preserved an id
    /// near `i64::MAX` and asked kernel to allocate a sibling would wrap silently and produce
    /// a negative id that would then trip the negative-id validator on the next round-trip.
    #[rstest::rstest]
    #[case::physical_name_only(Some("preserved-name"))]
    #[case::neither_annotation(None)]
    fn test_assign_column_mapping_metadata_errors_when_id_allocation_overflows(
        #[case] preserved_physical_name: Option<&str>,
    ) {
        let field = make_field_under_test(None, preserved_physical_name);
        let schema = StructType::new_unchecked([field]);

        let mut max_id = i64::MAX;
        let err = assign_column_mapping_metadata(&schema, &mut max_id, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Cannot allocate column mapping id")
                && err.contains("overflows `i64`")
                && err.contains("32-bit non-negative integer")
                && err.contains(&i64::MAX.to_string()),
            "Expected overflow error citing the i64 ceiling, the protocol's 32-bit bound, \
             and the offending `max_id`; got: {err}",
        );
        // `max_id` must not be advanced past `i64::MAX` on failure.
        assert_eq!(max_id, i64::MAX);
    }

    /// `i32::MAX` is the protocol's largest legal `delta.columnMapping.id`. A connector that
    /// preserves it must round-trip; a fresh sibling allocated above it must be rejected with
    /// the protocol-bound message (not the `i64` overflow message). This pins both halves of
    /// the bound to one rstest so the negative space is contiguous.
    #[rstest::rstest]
    #[case::preserved_at_protocol_max_round_trips(i32::MAX as i64, true)]
    #[case::preserved_above_protocol_max_rejected(i32::MAX as i64 + 1, false)]
    fn test_assign_column_mapping_metadata_enforces_protocol_32bit_bound(
        #[case] preserved_id: i64,
        #[case] expect_ok: bool,
    ) {
        let field_under_test = make_field_under_test(Some(preserved_id), Some("preserved-name"));
        let (schema, _) = build_schema_with_field_under_test(SchemaShape::Flat, field_under_test);

        let mut max_id = 0;
        let result = assign_column_mapping_metadata(&schema, &mut max_id, false);
        if expect_ok {
            let schema = result.expect("preserved id at protocol max must round-trip");
            assert_eq!(
                schema
                    .field(FIELD_UNDER_TEST)
                    .unwrap()
                    .column_mapping_id()
                    .expect("expected numeric column mapping id"),
                i32::MAX as i64,
            );
            assert_eq!(max_id, i32::MAX as i64);
        } else {
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Invalid column mapping id")
                    && err.contains(&MAX_COLUMN_MAPPING_ID.to_string()),
                "Expected canonical out-of-range error citing the 32-bit max, got: {err}",
            );
        }
    }

    /// Allocating a sibling next to a preserved id at the protocol cap must error with the
    /// allocator-flavored bound message (distinct from the `i64::MAX` overflow message; the
    /// `checked_add` succeeds, then the bound check trips).
    #[test]
    fn test_assign_column_mapping_metadata_errors_when_allocation_would_exceed_protocol_max() {
        // Field 1 preserves id = i32::MAX; field 2 has only physicalName so allocation fires.
        let preserved = StructField::not_null("preserved", DataType::INTEGER).add_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(i32::MAX as i64),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("preserved-name".to_string()),
            ),
        ]);
        let needs_allocation = StructField::not_null("needs_alloc", DataType::INTEGER)
            .add_metadata([(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("user-supplied".to_string()),
            )]);
        let schema = StructType::new_unchecked([preserved, needs_allocation]);

        let mut max_id = find_max_column_id_in_schema(&schema).unwrap_or(0);
        let err = assign_column_mapping_metadata(&schema, &mut max_id, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Cannot allocate column mapping id")
                && err.contains("exceeds the Delta protocol's 32-bit non-negative maximum"),
            "Expected allocator-flavored protocol-bound error, got: {err}",
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
            StructField::new("nested", inner, true),
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
        let id_val = field.column_mapping_id().unwrap_or_else(|| {
            panic!(
                "Field '{}' should have a numeric column mapping ID",
                field.name
            )
        });
        assert!(
            seen_ids.insert(id_val),
            "Duplicate column mapping ID {id_val} on field '{}'",
            field.name
        );

        let physical_name = expect_physical_name(field)
            .unwrap_or_else(|e| panic!("Field '{}' should have physical name: {e}", field.name));
        assert!(
            seen_physical_names.insert(physical_name.clone()),
            "Duplicate physical name '{physical_name}' on field '{}'",
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

        let map_type = MapType::new(key_struct, value_struct, true);

        let schema = StructType::new_unchecked([StructField::new("my_map", map_type, true)]);

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

        let array_type = ArrayType::new(elem_struct, true);

        let schema = StructType::new_unchecked([StructField::new("my_array", array_type, true)]);

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

        let inner_array = ArrayType::new(deep_struct, true);
        let outer_array = ArrayType::new(inner_array, true);

        let schema =
            StructType::new_unchecked([StructField::new("nested_arrays", outer_array, true)]);

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

        let key_array = ArrayType::new(key_struct, true);
        let value_array = ArrayType::new(value_struct, true);

        let inner_map = MapType::new(key_array, value_array, true);

        let outer_array = ArrayType::new(inner_map, true);

        let schema = StructType::new_unchecked([StructField::new("cursed", outer_array, true)]);

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

        let schema =
            StructType::new_unchecked([StructField::new("a", inner, true).add_metadata([
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
        let schema =
            StructType::new_unchecked([StructField::new("a", inner, true).add_metadata([
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

    #[rstest::rstest]
    #[case::string_value(Some(MetadataValue::String("col-x".into())), Some("col-x"))]
    #[case::missing(None /* annotation */, None /* expected(None means error) */)]
    #[case::wrong_type_number(Some(MetadataValue::Number(7)), None /* expected(None means error) */)]
    #[case::wrong_type_boolean(Some(MetadataValue::Boolean(true)), None /* expected(None means error) */)]
    #[case::wrong_type_other(
        Some(MetadataValue::Other(serde_json::Value::from("col-x"))),
        None /* expected(None means error) */,
    )]
    fn test_expect_physical_name(
        #[case] annotation: Option<MetadataValue>,
        #[case] expected: Option<&str>,
    ) {
        let field =
            StructField::new("a", DataType::INTEGER, true).fold_with(annotation, |field, value| {
                field.add_metadata([(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(), value)])
            });
        let result = expect_physical_name(&field);
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => assert_result_error_with_message(
                result,
                "Expect field 'a' to have a physical name annotation",
            ),
        }
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
        let outer_field = StructField::new("address", inner_struct, true).with_metadata([(
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

    fn field_with_id(name: &str, ty: impl Into<DataType>, id: i64) -> StructField {
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
        let inner = DataType::from(
            StructType::try_new(vec![
                field_with_id("x", DataType::STRING, 7),
                field_with_id("y", DataType::STRING, 5),
            ])
            .unwrap(),
        );
        let schema = StructType::try_new(vec![
            field_with_id("outer", inner, 2),
            field_with_id("sibling", DataType::STRING, 3),
        ])
        .unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(7));
    }

    #[test]
    fn find_max_column_id_array_and_map_recurse_into_element_types() {
        let array_elem_struct = DataType::from(ArrayType::new(
            StructType::try_new(vec![field_with_id("deep", DataType::STRING, 42)]).unwrap(),
            true,
        ));
        let map_ty = DataType::from(MapType::new(
            DataType::STRING,
            StructType::try_new(vec![field_with_id("inside", DataType::STRING, 9)]).unwrap(),
            false,
        ));
        let schema = StructType::try_new(vec![
            field_with_id("arr", array_elem_struct, 1),
            field_with_id("m", map_ty, 2),
        ])
        .unwrap();
        assert_eq!(find_max_column_id_in_schema(&schema), Some(42));
    }

    #[test]
    fn find_max_column_id_map_with_struct_key_recurses() {
        let key_struct = DataType::from(
            StructType::try_new(vec![field_with_id("key_id", DataType::INTEGER, 17)]).unwrap(),
        );
        let value_struct = DataType::from(
            StructType::try_new(vec![field_with_id("val_id", DataType::INTEGER, 11)]).unwrap(),
        );
        let map_ty = DataType::from(MapType::new(key_struct, value_struct, false));
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
            MapType::new(DataType::INTEGER, DataType::INTEGER, false),
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
