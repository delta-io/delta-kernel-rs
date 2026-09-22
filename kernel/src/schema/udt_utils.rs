//! Detection and feature-conformance tests for user-defined types.

use super::DataType;

/// Returns whether `data_type` contains a UDT at any depth.
pub(super) fn contains_udt(data_type: &DataType) -> bool {
    match data_type {
        DataType::UserDefined(_) => true,
        DataType::Struct(s) | DataType::Variant(s) => {
            s.fields().any(|f| contains_udt(f.data_type()))
        }
        DataType::Array(a) => contains_udt(a.element_type()),
        DataType::Map(m) => contains_udt(m.key_type()) || contains_udt(m.value_type()),
        DataType::Primitive(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::actions::Protocol;
    #[cfg(feature = "geo-type-in-dev")]
    use crate::schema::EdgeInterpolationAlgorithm;
    use crate::schema::{schema, ArrayType, MapType, UserDefinedType};
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::assert_schema_feature_validation;
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
            wrap_type(
                UserDefinedType {
                    sql_type: Box::new(wrap_type(inner, physical_layout)),
                    annotation: [("class".to_owned(), Some("FeatureUDT".to_owned()))].into(),
                }
                .into(),
                outer_layout,
            )
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
