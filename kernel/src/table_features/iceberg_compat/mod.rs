//! IcebergCompat invariant checks shared across versions.
//!
//! Each `delta.enableIcebergCompatV{N}` version owns a submodule (currently only
//! [`v3`]), and exposes a single
//! `pub(crate) const V{N}_VALIDATOR: IcebergCompatValidator`. Callers feed that
//! constant to [`validate_iceberg_compat_if_needed`].

pub(crate) mod v3;

use crate::schema::{ColumnMetadataKey, DataType, StructField};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error};

pub(crate) enum IcebergCompatVersion {
    // TODO: Add V1, V2 when kernel supports them.
    V3,
}

impl IcebergCompatVersion {
    pub(super) fn as_table_feature(&self) -> TableFeature {
        match self {
            Self::V3 => TableFeature::IcebergCompatV3,
        }
    }
}

/// A single invariant that must hold when an icebergCompat version is enabled.
pub(crate) type IcebergCompatCheck = fn(&TableConfiguration) -> DeltaResult<()>;

/// Pairs an [`IcebergCompatVersion`] with the ordered list of invariants that
/// run when that version is enabled.
pub(crate) struct IcebergCompatValidator {
    pub(super) version: IcebergCompatVersion,
    pub(super) checks: &'static [IcebergCompatCheck],
}

pub(crate) fn validate_iceberg_compat_if_needed(
    tc: &TableConfiguration,
    validator: &IcebergCompatValidator,
) -> DeltaResult<()> {
    if !tc.is_feature_enabled(&validator.version.as_table_feature()) {
        return Ok(());
    }
    for check in validator.checks {
        check(tc)?;
    }
    Ok(())
}

/// Walks `tc.logical_schema()` and errors on the first field whose data type
/// fails `is_supported`. Reports the offending column path + type in the error.
pub(super) fn check_only_supported_types(
    tc: &TableConfiguration,
    is_supported: fn(&DataType) -> bool,
    feature_label: &str,
) -> DeltaResult<()> {
    let mut v = TypeAllowListVisitor {
        is_supported,
        offender: None,
        path: vec![],
    };
    v.transform_struct(&tc.logical_schema());
    let Some(offender) = v.offender else {
        return Ok(());
    };
    Err(Error::generic(format!(
        "{feature_label} does not support type at column: {offender}",
    )))
}

struct TypeAllowListVisitor {
    is_supported: fn(&DataType) -> bool,
    /// The first unsupported field encountered, rendered as `<dotted.path> (<type>)`, or `None`
    /// if every field is supported.
    ///
    /// For example, given schema `struct<events: array<struct<ts: timestamp>>>` (and assuming
    /// `timestamp` is unsupported), the offender is `events.element.ts (timestamp)`.
    offender: Option<String>,
    path: Vec<String>,
}

impl TypeAllowListVisitor {
    /// If `dt` isn't allowed, record it as the offender. No-op if an offender is already set.
    fn check(&mut self, dt: &DataType) {
        if self.offender.is_none() && !(self.is_supported)(dt) {
            self.offender = Some(format!("{} ({})", self.path.join("."), dt));
        }
    }

    /// Descend into a nested element type (array element, map key, or map value).
    fn visit_nested(&mut self, seg: &str, ele_type: &DataType) {
        if self.offender.is_some() {
            return;
        }
        self.path.push(seg.to_string());
        self.check(ele_type);
        self.transform(ele_type);
        self.path.pop();
    }
}

impl<'a> SchemaTransform<'a> for TypeAllowListVisitor {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, f: &'a StructField) {
        if self.offender.is_some() {
            return;
        }
        self.path.push(f.name().to_string());
        self.check(f.data_type());
        self.recurse_into_struct_field(f);
        self.path.pop();
    }

    fn transform_array_element(&mut self, etype: &'a DataType) {
        self.visit_nested("element", etype);
    }

    fn transform_map_key(&mut self, etype: &'a DataType) {
        self.visit_nested("key", etype);
    }

    fn transform_map_value(&mut self, etype: &'a DataType) {
        self.visit_nested("value", etype);
    }
}

/// Rejects fields carrying the legacy `parquet.field.nested.ids` metadata.
///
/// See <https://github.com/delta-io/delta/issues/6688>.
pub(super) fn check_no_legacy_nested_ids(tc: &TableConfiguration) -> DeltaResult<()> {
    let mut v = LegacyNestedIdsVisitor {
        path: vec![],
        offender: None,
    };
    v.transform_struct(&tc.logical_schema());
    let Some(offender) = v.offender else {
        return Ok(());
    };
    Err(Error::generic(format!(
        "field `{offender}` carries deprecated `{}` metadata; use `{}` instead. \
         See https://github.com/delta-io/delta/issues/6688",
        ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
        ColumnMetadataKey::ColumnMappingNestedIds.as_ref(),
    )))
}

struct LegacyNestedIdsVisitor {
    path: Vec<String>,
    offender: Option<String>,
}

impl<'a> SchemaTransform<'a> for LegacyNestedIdsVisitor {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, f: &'a StructField) {
        if self.offender.is_some() {
            return;
        }
        self.path.push(f.name().to_string());
        if f.metadata()
            .contains_key(ColumnMetadataKey::ParquetFieldNestedIds.as_ref())
        {
            self.offender = Some(self.path.join("."));
            return;
        }
        self.recurse_into_struct_field(f);
        self.path.pop();
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{schema, ArrayType, MetadataValue, PrimitiveType, StructType};

    // === LegacyNestedIdsVisitor: parquet.field.nested.ids detection ===

    fn field_with_metadata(name: &str, dtype: impl Into<DataType>, key: &str) -> StructField {
        let mut f = StructField::nullable(name, dtype);
        f.metadata.insert(
            key.to_string(),
            MetadataValue::Other(serde_json::json!({ "x.element": 1 })),
        );
        f
    }

    // `array<integer>` fields carrying nested-id metadata under the new vs. legacy key.
    fn nested_ids_field(name: &str) -> StructField {
        let ty = ArrayType::new(DataType::INTEGER, true);
        field_with_metadata(name, ty, ColumnMetadataKey::ColumnMappingNestedIds.as_ref())
    }
    fn legacy_ids_field(name: &str) -> StructField {
        let ty = ArrayType::new(DataType::INTEGER, true);
        field_with_metadata(name, ty, ColumnMetadataKey::ParquetFieldNestedIds.as_ref())
    }

    fn simple_schema() -> StructType {
        schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        }
    }

    fn schema_with_good_nested_ids() -> StructType {
        schema! { (nested_ids_field("x")) }
    }

    fn schema_with_legacy_at(name: &str) -> StructType {
        schema! { (legacy_ids_field(name)) }
    }

    fn schema_struct_with_legacy_at_inner() -> StructType {
        schema! { nullable "parent": { (legacy_ids_field("inner")) } }
    }

    fn schema_array_struct_with_legacy_at_inner() -> StructType {
        schema! { nullable "arr": [ nullable { (legacy_ids_field("inner")) } ] }
    }

    fn schema_map_value_struct_with_legacy_at_inner() -> StructType {
        schema! { nullable "m": { STRING => nullable { (legacy_ids_field("inner")) } } }
    }

    fn schema_two_legacy_fields() -> StructType {
        schema! { (legacy_ids_field("a")), (legacy_ids_field("b")) }
    }

    #[rstest]
    #[case::clean_schema(simple_schema(), None)]
    #[case::column_mapping_nested_id_key_only(schema_with_good_nested_ids(), None)]
    #[case::top_level_legacy(schema_with_legacy_at("top"), Some("top".to_string()))]
    #[case::nested_struct_legacy(schema_struct_with_legacy_at_inner(), Some("parent.inner".to_string()))]
    #[case::array_struct_legacy(schema_array_struct_with_legacy_at_inner(), Some("arr.inner".to_string()))]
    #[case::map_value_struct_legacy(schema_map_value_struct_with_legacy_at_inner(), Some("m.inner".to_string()))]
    #[case::first_offender_wins(schema_two_legacy_fields(), Some("a".to_string()))]
    fn legacy_nested_ids_visitor_finds_first_offender(
        #[case] schema: StructType,
        #[case] expected: Option<String>,
    ) {
        let mut v = LegacyNestedIdsVisitor {
            path: vec![],
            offender: None,
        };
        v.transform_struct(&schema);
        assert_eq!(v.offender, expected);
    }

    // === TypeAllowlistVisitor tests ===

    /// Narrow allowlist: Integer, String, and Struct.
    fn narrow_allowlist(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Primitive(PrimitiveType::Integer | PrimitiveType::String)
                | DataType::Struct(_)
        )
    }

    /// Wide allowlist: Integer, String, Struct, Array, Map.
    fn wide_allowlist(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Primitive(PrimitiveType::Integer | PrimitiveType::String)
                | DataType::Struct(_)
                | DataType::Array(_)
                | DataType::Map(_)
        )
    }

    fn schema_struct_with_float_inner() -> StructType {
        schema! { nullable "s": { nullable "f": FLOAT } }
    }

    fn schema_array_of_float() -> StructType {
        schema! { nullable "arr": [ nullable FLOAT ] }
    }

    fn schema_map_key_float() -> StructType {
        schema! { nullable "m": { FLOAT => nullable STRING } }
    }

    fn schema_map_value_float() -> StructType {
        schema! { nullable "m": { STRING => nullable FLOAT } }
    }

    fn schema_float_long() -> StructType {
        schema! {
            nullable "a": LONG,
            nullable "b": FLOAT,
        }
    }

    /// `a: array<map<string, array<float>>>` followed by a sibling `b: array<float>`.
    fn schema_deep_nested_float() -> StructType {
        schema! {
            nullable "a": [ nullable { STRING => nullable [ nullable FLOAT ] } ],
            nullable "b": [ nullable FLOAT ],
        }
    }

    #[rstest]
    #[case::accept_int_string(narrow_allowlist, simple_schema(), None)]
    #[case::reject_top_level_array(
        narrow_allowlist,
        schema_with_legacy_at("top"),
        Some("top (array<integer>)".to_string()),
    )]
    #[case::reject_nested_primitive_in_struct(
        narrow_allowlist,
        schema_struct_with_float_inner(),
        Some("s.f (float)".to_string()),
    )]
    #[case::reject_primitive_inside_array(
        wide_allowlist,
        schema_array_of_float(),
        Some("arr.element (float)".to_string()),
    )]
    #[case::reject_primitive_inside_map_key(
        wide_allowlist,
        schema_map_key_float(),
        Some("m.key (float)".to_string()),
    )]
    #[case::reject_primitive_inside_map_value(
        wide_allowlist,
        schema_map_value_float(),
        Some("m.value (float)".to_string()),
    )]
    #[case::first_offender_wins(
        wide_allowlist,
        schema_float_long(),
        Some("a (long)".to_string()),
    )]
    #[case::reject_deep_then_short_circuits_sibling(
        wide_allowlist,
        schema_deep_nested_float(),
        Some("a.element.value.element (float)".to_string()),
    )]
    fn type_allowlist_visitor(
        #[case] is_supported: fn(&DataType) -> bool,
        #[case] schema: StructType,
        #[case] expected: Option<String>,
    ) {
        let mut v = TypeAllowListVisitor {
            is_supported,
            offender: None,
            path: vec![],
        };
        v.transform_struct(&schema);
        assert_eq!(v.offender, expected);
    }
}
