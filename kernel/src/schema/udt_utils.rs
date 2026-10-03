//! Detection and write validation for user-defined types.

use super::{ColumnMetadataKey, DataType, StructField, StructType, UserDefinedType};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{KernelError, Result};

/// Rejects generated or identity metadata on UDT fields in `schema`.
///
/// Returns a schema error naming the field and prohibited metadata key. Physical fields inside
/// a UDT are outside this traversal.
pub(crate) fn validate_udt_write_metadata(schema: &StructType) -> Result<()> {
    UdtWriteMetadataValidator.transform_struct(schema)
}

struct ContainsUdt;

impl<'a> SchemaTransform<'a> for ContainsUdt {
    transform_output_type!(|'a, T| std::result::Result<(), ()>);

    fn transform_user_defined(&mut self, _: &'a UserDefinedType) -> std::result::Result<(), ()> {
        Err(())
    }
}

/// Returns whether `data_type` contains a UDT at any depth.
pub(super) fn contains_udt(data_type: &DataType) -> bool {
    ContainsUdt.transform(data_type).is_err()
}

struct UdtWriteMetadataValidator;

impl<'a> SchemaTransform<'a> for UdtWriteMetadataValidator {
    transform_output_type!(|'a, T| Result<()>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> Result<()> {
        if matches!(field.data_type(), DataType::UserDefined(_)) {
            if let Some(key) = field.metadata().keys().find(|key| {
                key.as_str() == ColumnMetadataKey::GenerationExpression.as_ref()
                    || key.starts_with("delta.identity.")
            }) {
                return Err(KernelError::schema(format!(
                    "UDT column '{}' cannot carry '{key}' metadata",
                    field.name(),
                )));
            }
        }
        self.recurse_into_struct_field(field)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::actions::Protocol;
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::EdgeInterpolationAlgorithm;
    use crate::schema::{schema, ArrayType, MapType, MetadataValue, UserDefinedType};
    use crate::table_features::{Operation, TableFeature};
    use crate::unit_test_utils::{assert_schema_feature_validation, MockTableConfigurationBuilder};
    #[cfg(feature = "geo-type-in-dev")]
    use crate::unit_test_utils::{geography_type, geometry_type};

    fn wrap_type(data_type: DataType, layout: &str) -> DataType {
        match layout {
            "scalar" => data_type,
            "struct" => schema! { nullable "inner": (data_type) }.into(),
            "array" => ArrayType::new(data_type, true).into(),
            "map_key" => MapType::new(data_type, DataType::LONG, true).into(),
            "map_value" => MapType::new(DataType::STRING, data_type, true).into(),
            _ => panic!("Unknown layout: {layout}"),
        }
    }

    #[rstest]
    #[case::generated("delta.generationExpression")]
    #[case::identity("delta.identity.start")]
    fn existing_udt_rejects_write_metadata(#[case] key: &str, #[values(false, true)] nested: bool) {
        let udt = UserDefinedType::try_new(DataType::LONG, Default::default()).unwrap();
        let field = StructField::nullable("value", udt)
            .add_metadata([(key.to_owned(), MetadataValue::String("1".to_owned()))]);
        let schema = schema! { (field), };
        let schema = if nested {
            schema! { nullable "outer": (schema) }
        } else {
            schema
        };
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .build();
        crate::unit_test_utils::assert_result_error_with_message(
            config.ensure_operation_supported(Operation::Write),
            "cannot carry",
        );
    }

    #[rstest]
    #[case::variant(
        DataType::unshredded_variant(),
        TableFeature::VariantType,
        "variantType"
    )]
    #[case::variant_preview(
        DataType::unshredded_variant(),
        TableFeature::VariantTypePreview,
        "variantType"
    )]
    #[case::timestamp_ntz(
        DataType::TIMESTAMP_NTZ,
        TableFeature::TimestampWithoutTimezone,
        "timestampNtz"
    )]
    #[cfg_attr(
        feature = "geo-type-in-dev",
        case::geometry(geometry_type("EPSG:4326"), TableFeature::GeospatialType, "geospatial")
    )]
    #[cfg_attr(
        feature = "geo-type-in-dev",
        case::geography(
            geography_type("EPSG:4326", EdgeInterpolationAlgorithm::Spherical),
            TableFeature::GeospatialType,
            "geospatial"
        )
    )]
    fn udt_physical_types_require_table_features(
        #[case] inner: DataType,
        #[case] feature: TableFeature,
        #[case] feature_name: &str,
        #[values("scalar", "struct", "array", "map_key", "map_value")] physical_layout: &str,
        #[values("scalar", "struct", "array", "map_key", "map_value")] outer_layout: &str,
    ) {
        let wrap_udt = |inner| {
            let udt = UserDefinedType::try_new(
                wrap_type(inner, physical_layout),
                [("class".to_owned(), Some("FeatureUDT".to_owned()))].into(),
            )
            .unwrap();
            wrap_type(udt.into(), outer_layout)
        };
        let schema_with = schema! { nullable "value": (wrap_udt(inner)) };
        let schema_without = schema! { nullable "value": (wrap_udt(DataType::LONG)) };
        let protocol_with = Protocol::try_new_modern([&feature], [&feature]).unwrap();
        let protocol_without =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();
        assert_schema_feature_validation(
            &schema_with,
            &schema_without,
            &protocol_with,
            &protocol_without,
            &[],
            feature_name,
        );
    }
}
