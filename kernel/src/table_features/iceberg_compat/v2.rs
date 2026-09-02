//! IcebergCompatV2 checks.

use super::{
    check_no_legacy_nested_ids, check_only_supported_types, IcebergCompatCheck,
    IcebergCompatValidator, IcebergCompatVersion,
};
use crate::schema::DataType;
use crate::schema::PrimitiveType::*;
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::SchemaTransform as _;
use crate::DeltaResult;

/// V2 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V2_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V2,
    checks: V2_CHECKS,
};

const V2_CHECKS: &[IcebergCompatCheck] = &[
    IcebergCompatCheck::always(check_v2_supported_types),
    IcebergCompatCheck::always(check_no_legacy_nested_ids),
    IcebergCompatCheck::write_only(iceberg_compat_v2_type_changes_validation),
];

fn is_v2_supported_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Primitive(
            Byte | Short
                | Integer
                | Long
                | Float
                | Double
                | Boolean
                | Binary
                | String
                | Date
                | Timestamp
                | TimestampNtz
                | Decimal(_)
        ) | DataType::Array(_)
            | DataType::Map(_)
            | DataType::Struct(_)
    )
}

fn check_v2_supported_types(tc: &TableConfiguration) -> DeltaResult<()> {
    check_only_supported_types(
        tc,
        is_v2_supported_type,
        IcebergCompatVersion::V2.as_table_feature().as_ref(),
    )
}

/// Validates that historical type changes on an IcebergCompatV2 table are compatible with Iceberg
/// schema evolution rules.
///
/// This is a write-side guard because unsupported type changes do not prevent Delta reads. They
/// only violate the IcebergCompatV2 writer contract that the table remains convertible to Iceberg.
///
/// # Errors
///
/// Returns an error if `delta.typeChanges` metadata is malformed, or if any recorded type change
/// is outside Iceberg V2's allowed widening list.
pub(crate) fn iceberg_compat_v2_type_changes_validation(
    tc: &TableConfiguration,
) -> DeltaResult<()> {
    if !tc.is_feature_supported(&TableFeature::TypeWidening)
        && !tc.is_feature_supported(&TableFeature::TypeWideningPreview)
    {
        return Ok(());
    }

    let mut validator = super::TypeChangesValidator {
        path: vec![],
        feature_label: "icebergCompatV2",
    };
    validator.transform_struct(tc.logical_schema_ref())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::schema::{
        schema, ArrayType, ColumnMetadataKey, DataType, MapType, MetadataValue, StructField,
        StructType,
    };
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};

    #[test]
    fn is_v2_supported_type_accepted_datatypes() {
        let primitives = [
            DataType::STRING,
            DataType::LONG,
            DataType::INTEGER,
            DataType::SHORT,
            DataType::BYTE,
            DataType::FLOAT,
            DataType::DOUBLE,
            DataType::BOOLEAN,
            DataType::BINARY,
            DataType::DATE,
            DataType::TIMESTAMP,
            DataType::TIMESTAMP_NTZ,
            DataType::decimal(10, 2).unwrap(),
        ];
        for dt in primitives {
            assert!(
                is_v2_supported_type(&dt),
                "primitive {dt} should be V2-supported"
            );
        }
        let nested = [
            DataType::from(ArrayType::new(DataType::INTEGER, true)),
            DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
            DataType::from(schema! { nullable "x": INTEGER }),
        ];
        for dt in nested {
            assert!(
                is_v2_supported_type(&dt),
                "nested {dt} should be V2-supported"
            );
        }
    }

    #[test]
    fn is_v2_supported_type_rejects_variant_and_void() {
        // Variant is a V3+ type, and void is excluded from the V2 allowlist (by omission) to
        // match delta-spark, which cannot consume such columns on an icebergCompatV2 table.
        assert!(!is_v2_supported_type(&DataType::unshredded_variant()));
        assert!(!is_v2_supported_type(&DataType::VOID));
    }

    fn table_config_with_schema_and_features(
        schema: StructType,
        features: impl IntoIterator<Item = TableFeature>,
    ) -> TableConfiguration {
        MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .with_table_root("file:///t/")
            .build()
    }

    fn table_config_with_schema(schema: StructType) -> TableConfiguration {
        table_config_with_schema_and_features(
            schema,
            [TableFeature::IcebergCompatV2, TableFeature::TypeWidening],
        )
    }

    fn field_with_type_change(name: &str, from_type: &str, to_type: &str) -> StructField {
        StructField::nullable(name, DataType::STRING).add_metadata([(
            ColumnMetadataKey::TypeChanges.as_ref(),
            MetadataValue::Other(json!([{
                "fromType": from_type,
                "toType": to_type,
                "tableVersion": 2
            }])),
        )])
    }

    #[rstest]
    #[case::byte_short("byte", "short")]
    #[case::integer_long("integer", "long")]
    #[case::float_double("float", "double")]
    #[case::decimal_same_scale("decimal(10,2)", "decimal(20,2)")]
    fn v2_type_change_validation_allows_iceberg_promotions(
        #[case] from_type: &str,
        #[case] to_type: &str,
    ) {
        let table_configuration = table_config_with_schema(schema! {
            (field_with_type_change("a", from_type, to_type)),
        });

        iceberg_compat_v2_type_changes_validation(&table_configuration).unwrap();
    }

    #[rstest]
    #[case::integer_double("integer", "double")]
    #[case::integer_decimal("integer", "decimal(11,1)")]
    #[case::decimal_scale_change("decimal(10,2)", "decimal(20,5)")]
    #[case::date_timestamp_ntz("date", "timestamp_ntz")]
    #[case::long_double("long", "double")]
    fn v2_type_change_validation_rejects_non_iceberg_promotions(
        #[case] from_type: &str,
        #[case] to_type: &str,
    ) {
        let table_configuration = table_config_with_schema(schema! {
            (field_with_type_change("a", from_type, to_type)),
        });

        let err = iceberg_compat_v2_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("icebergCompatV2 does not support type change")
                && err.contains(from_type)
                && err.contains(to_type),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn v2_type_change_validation_reports_nested_field_path() {
        let table_configuration = table_config_with_schema(schema! {
            nullable "m": { STRING => nullable {
                (field_with_type_change("inner", "integer", "double")),
            } },
        });

        let err = iceberg_compat_v2_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(err.contains("m.value.inner"), "unexpected error: {err}");
    }

    #[rstest]
    #[case::wrong_metadata_type(
        MetadataValue::String("not an array".to_string()),
        "non-array",
    )]
    #[case::json_object(
        MetadataValue::Other(json!({"fromType": "integer", "toType": "long"})),
        "invalid",
    )]
    #[case::missing_to_type(
        MetadataValue::Other(json!([{"fromType": "integer"}])),
        "invalid",
    )]
    fn v2_type_change_validation_rejects_malformed_metadata(
        #[case] metadata: MetadataValue,
        #[case] expected_error: &str,
    ) {
        let field = StructField::nullable("a", DataType::LONG)
            .add_metadata([(ColumnMetadataKey::TypeChanges.as_ref(), metadata)]);
        let table_configuration = table_config_with_schema(schema! { (field) });

        let err = iceberg_compat_v2_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(expected_error) && err.contains("delta.typeChanges"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn v2_type_change_validation_skips_tables_without_type_widening_support() {
        let table_configuration = table_config_with_schema_and_features(
            schema! {
                (field_with_type_change("a", "integer", "double")),
            },
            [TableFeature::IcebergCompatV2],
        );

        iceberg_compat_v2_type_changes_validation(&table_configuration).unwrap();
    }
}
