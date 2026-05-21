//! IcebergCompat invariant checks shared across versions.
//!
//! Each `delta.enableIcebergCompatV{N}` version owns a submodule (currently only
//! [`v3`]), and exposes a single
//! `pub(crate) const V{N}_VALIDATOR: IcebergCompatValidator`. Callers feed that
//! constant to [`validate_iceberg_compat_if_needed`], the shared dispatcher.

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
    /// Maps the version to the `TableFeature`.
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
pub(super) fn has_only_supported_types(
    tc: &TableConfiguration,
    is_supported: fn(&DataType) -> bool,
    feature_label: &str,
) -> DeltaResult<()> {
    let mut v = TypeAllowlistVisitor {
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

struct TypeAllowlistVisitor {
    is_supported: fn(&DataType) -> bool,
    offender: Option<String>,
    path: Vec<String>,
}

impl<'a> SchemaTransform<'a> for TypeAllowlistVisitor {
    transform_output_type!(|'a, T| ());

    fn transform_struct_field(&mut self, f: &'a StructField) {
        if self.offender.is_some() {
            return;
        }
        self.path.push(f.name().to_string());
        if !(self.is_supported)(f.data_type()) {
            self.offender = Some(format!("{} ({})", self.path.join("."), f.data_type()));
            return;
        }
        self.recurse_into_struct_field(f);
        self.path.pop();
    }
}

/// `parquet.field.nested.ids` is to be deprecated in favor of `delta.columnMapping.nested.ids`.
/// Validates that no fields in the schema have `parquet.field.nested.ids` metadata.
///
/// Tracking issue: <https://github.com/delta-io/delta/issues/6688>
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
    use crate::schema::{ArrayType, MapType, MetadataValue, StructType};

    fn field_with_metadata(name: &str, dtype: DataType, key: &str) -> StructField {
        let mut f = StructField::nullable(name, dtype);
        f.metadata.insert(
            key.to_string(),
            MetadataValue::Other(serde_json::json!({ "x.element": 1 })),
        );
        f
    }

    fn simple_schema() -> StructType {
        StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ])
    }

    fn schema_with_good_nested_ids() -> StructType {
        StructType::new_unchecked(vec![field_with_metadata(
            "x",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            ColumnMetadataKey::ColumnMappingNestedIds.as_ref(),
        )])
    }

    fn schema_with_legacy_at(name: &str) -> StructType {
        StructType::new_unchecked(vec![field_with_metadata(
            name,
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
        )])
    }

    fn schema_struct_with_legacy_at_inner() -> StructType {
        let inner = StructType::new_unchecked(vec![field_with_metadata(
            "inner",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
        )]);
        StructType::new_unchecked(vec![StructField::nullable(
            "parent",
            DataType::Struct(Box::new(inner)),
        )])
    }

    fn schema_array_struct_with_legacy_at_inner() -> StructType {
        let inner = StructType::new_unchecked(vec![field_with_metadata(
            "inner",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
        )]);
        StructType::new_unchecked(vec![StructField::nullable(
            "arr",
            DataType::Array(Box::new(ArrayType::new(
                DataType::Struct(Box::new(inner)),
                true,
            ))),
        )])
    }

    fn schema_map_value_struct_with_legacy_at_inner() -> StructType {
        let inner = StructType::new_unchecked(vec![field_with_metadata(
            "inner",
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
        )]);
        StructType::new_unchecked(vec![StructField::nullable(
            "m",
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::Struct(Box::new(inner)),
                true,
            ))),
        )])
    }

    fn schema_two_legacy_fields() -> StructType {
        StructType::new_unchecked(vec![
            field_with_metadata(
                "a",
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
                ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            ),
            field_with_metadata(
                "b",
                DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
                ColumnMetadataKey::ParquetFieldNestedIds.as_ref(),
            ),
        ])
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

    // === TypeAllowlistVisitor tests  ===
    //
    /// Narrow allowlist: Integer, String, and Struct.
    fn narrow_allowlist(dt: &DataType) -> bool {
        use crate::schema::PrimitiveType::*;
        matches!(
            dt,
            DataType::Primitive(Integer | String) | DataType::Struct(_)
        )
    }

    fn schema_struct_with_float_inner() -> StructType {
        let inner = StructType::new_unchecked(vec![StructField::new("f", DataType::FLOAT, true)]);
        StructType::new_unchecked(vec![StructField::new(
            "s",
            DataType::Struct(Box::new(inner)),
            true,
        )])
    }

    #[rstest]
    #[case::accept_int_string(simple_schema(), None)]
    #[case::reject_top_level_array(
        schema_with_legacy_at("top"),
        Some("top (array<integer>)".to_string()),
    )]
    #[case::reject_nested_primitive(
        schema_struct_with_float_inner(),
        Some("s.f (float)".to_string()),
    )]
    fn type_allowlist_visitor_under_narrow_allowlist(
        #[case] schema: StructType,
        #[case] expected: Option<String>,
    ) {
        let mut v = TypeAllowlistVisitor {
            is_supported: narrow_allowlist,
            offender: None,
            path: vec![],
        };
        v.transform_struct(&schema);
        assert_eq!(v.offender, expected);
    }
}
