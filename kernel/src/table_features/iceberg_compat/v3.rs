//! IcebergCompatV3 checks.

use tracing::warn;

use super::{
    check_no_legacy_nested_ids, check_only_supported_types, IcebergCompatCheck,
    IcebergCompatValidator, IcebergCompatVersion,
};
use crate::schema::PrimitiveType::{
    Binary, Boolean, Byte, Date, Decimal, Double, Float, Integer, Long, Short,
    String as StringType, Timestamp, TimestampNtz,
};
use crate::schema::{try_collect_column_defaults, DataType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::transforms::SchemaTransform;
use crate::DeltaResult;

/// V3 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V3_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V3,
    checks: V3_CHECKS,
};

const V3_CHECKS: &[IcebergCompatCheck] = &[
    IcebergCompatCheck::always(check_v3_supported_types),
    IcebergCompatCheck::always(check_no_legacy_nested_ids),
    IcebergCompatCheck::write_only(iceberg_compat_v3_type_changes_validation),
    IcebergCompatCheck::write_only(iceberg_compat_v3_column_defaults_validation),
];

fn is_v3_supported_type(dt: &DataType) -> bool {
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
                | StringType
                | Date
                | Timestamp
                | TimestampNtz
                | Decimal(_)
        ) | DataType::Array(_)
            | DataType::Map(_)
            | DataType::Struct(_)
            | DataType::Variant(_)
    )
}

fn check_v3_supported_types(tc: &TableConfiguration) -> DeltaResult<()> {
    check_only_supported_types(
        tc,
        is_v3_supported_type,
        IcebergCompatVersion::V3.as_table_feature().as_ref(),
    )
}

/// Validates that historical type changes on an IcebergCompatV3 table are compatible with Iceberg
/// schema evolution rules.
///
/// This is a write-side guard because unsupported type changes do not prevent Delta reads. They
/// only violate the IcebergCompatV3 writer contract that the table remains convertible to Iceberg.
///
/// # Errors
///
/// Returns an error if `delta.typeChanges` metadata is malformed, or if any recorded type change
/// is outside Iceberg V3's allowed widening list.
fn iceberg_compat_v3_type_changes_validation(tc: &TableConfiguration) -> DeltaResult<()> {
    if !tc.is_feature_supported(&TableFeature::TypeWidening)
        && !tc.is_feature_supported(&TableFeature::TypeWideningPreview)
    {
        return Ok(());
    }

    let mut validator = super::TypeChangesValidator {
        path: vec![],
        version: IcebergCompatVersion::V3,
    };
    validator.transform_struct(tc.logical_schema_ref())
}

/// Validates IcebergCompatV3 column defaults and logs warnings kernel cannot verify.
///
/// The IcebergCompatV3 spec requires column defaults to be literals. Kernel warns when its parser
/// cannot verify that requirement. This warning can be a false positive when the expression is a
/// literal that kernel's parser cannot parse.
///
/// This condition remains a warning because the table has already passed metadata validation and
/// rejecting a DML transaction could block valid tables based on kernel parser limitations. The
/// check provides defense in depth without treating an interoperability risk as definite
/// corruption.
///
/// # Errors
///
/// Propagates malformed column-default metadata errors from [`try_collect_column_defaults`].
pub(crate) fn iceberg_compat_v3_column_defaults_validation(
    table_configuration: &TableConfiguration,
) -> DeltaResult<()> {
    for (path, column_default) in
        try_collect_column_defaults(table_configuration.logical_schema_ref())?
    {
        if !column_default.is_kernel_parsable_literal() {
            warn!(
                "kernel could not verify that the icebergCompatV3 column default for '{path}' is a \
                 literal, got: {}",
                column_default.raw_sql()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{schema, ArrayType, MapType};

    #[test]
    fn is_v3_supported_type_accepted_datatypes() {
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
                is_v3_supported_type(&dt),
                "primitive {dt} should be V3-supported"
            );
        }
        let nested = [
            DataType::from(ArrayType::new(DataType::INTEGER, true)),
            DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
            DataType::from(schema! { nullable "x": INTEGER }),
            DataType::unshredded_variant(),
        ];
        for dt in nested {
            assert!(
                is_v3_supported_type(&dt),
                "nested {dt} should be V3-supported"
            );
        }
    }

    #[test]
    fn is_v3_supported_type_rejects_void() {
        // Void is excluded from the V3 allowlist (by omission) to match delta-spark, which
        // cannot consume an icebergCompatV3 table containing a void column.
        assert!(!is_v3_supported_type(&DataType::VOID));
    }
}

#[cfg(test)]
mod column_default_tests {
    use rstest::rstest;
    use test_utils::LoggingTest;

    use super::iceberg_compat_v3_column_defaults_validation;
    use crate::schema::ColumnMetadataKey::CurrentDefault;
    use crate::schema::{schema, ArrayType, DataType, MetadataValue, StructField, StructType};
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};

    /// Builds a `TableConfiguration` carrying `schema` with `allowColumnDefaults` enabled, so
    /// the IcebergCompatV3 column-default validation can be driven directly. The config does not
    /// enable IcebergCompatV3 itself (whose required dependencies are heavy to assemble here); the
    /// validation is invoked directly instead, and the end-to-end V3 path is covered by the
    /// integration tests.
    fn table_config_with_schema(schema: StructType) -> TableConfiguration {
        MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([TableFeature::AllowColumnDefaults])
                    .build(),
            )
            .with_table_root("file:///t/")
            .build()
    }

    fn field_with_default(
        name: &str,
        data_type: impl Into<DataType>,
        default_sql: &str,
    ) -> StructField {
        StructField::nullable(name, data_type).add_metadata([(
            CurrentDefault.as_ref().to_string(),
            MetadataValue::String(default_sql.to_string()),
        )])
    }

    #[rstest]
    #[case::primitive_literal(
        schema! {
            (field_with_default("a", DataType::INTEGER, "42")),
        },
        "a",
        None
    )]
    #[case::primitive_null(
        schema! {
            (field_with_default("a", DataType::INTEGER, "NULL")),
        },
        "a",
        None
    )]
    #[case::non_literal_primitive(
        schema! {
            (field_with_default("a", DataType::TIMESTAMP, "current_timestamp()")),
        },
        "a",
        Some("could not verify")
    )]
    #[case::null_on_non_primitive(
        schema! {
            (field_with_default("a", ArrayType::new(DataType::INTEGER, true), "NULL")),
        },
        "a",
        None
    )]
    #[case::non_null_on_non_primitive(
        schema! {
            (field_with_default("a", ArrayType::new(DataType::INTEGER, true), "ARRAY(1)")),
        },
        "a",
        Some("could not verify")
    )]
    #[case::nested_non_literal(
        schema! {
            nullable "s": {
                (field_with_default(
                "inner",
                DataType::TIMESTAMP,
                "current_timestamp()"
                )),
            },
        },
        "s.inner",
        Some("could not verify")
    )]
    fn v3_column_default_validation(
        #[case] schema: StructType,
        #[case] expected_path: &str,
        #[case] expected_warning: Option<&str>,
    ) {
        let table_configuration = table_config_with_schema(schema);
        let logging = LoggingTest::new();
        iceberg_compat_v3_column_defaults_validation(&table_configuration).unwrap();
        let logs = logging.logs();

        match expected_warning {
            None => assert!(
                !logs.contains("icebergCompatV3 column default"),
                "logs: {logs}"
            ),
            Some(needle) => {
                assert!(logs.contains(expected_path), "logs: {logs}");
                assert!(logs.contains(needle), "logs: {logs}");
            }
        }
    }
}

#[cfg(test)]
mod type_change_tests {
    use rstest::rstest;
    use serde_json::json;

    use super::iceberg_compat_v3_type_changes_validation;
    use crate::schema::{
        schema, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::{ColumnMappingMode, TableFeature};
    use crate::table_properties::{ENABLE_ICEBERG_COMPAT_V3, ENABLE_ROW_TRACKING};
    use crate::unit_test_utils::{MockProtocolBuilder, MockTableConfigurationBuilder};

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
            [TableFeature::IcebergCompatV3, TableFeature::TypeWidening],
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

    fn field_with_type_change_and_column_mapping(
        name: &str,
        from_type: &str,
        to_type: &str,
    ) -> StructField {
        field_with_type_change(name, from_type, to_type).add_metadata([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("col-1".to_string()),
            ),
        ])
    }

    #[rstest]
    #[case::byte_short("byte", "short")]
    #[case::byte_integer("byte", "integer")]
    #[case::byte_long("byte", "long")]
    #[case::short_integer("short", "integer")]
    #[case::short_long("short", "long")]
    #[case::integer_long("integer", "long")]
    #[case::float_double("float", "double")]
    #[case::decimal_same_scale("decimal(10,2)", "decimal(20,2)")]
    fn v3_type_change_validation_allows_iceberg_promotions(
        #[case] from_type: &str,
        #[case] to_type: &str,
    ) {
        let table_configuration = table_config_with_schema(schema! {
            (field_with_type_change("a", from_type, to_type)),
        });

        iceberg_compat_v3_type_changes_validation(&table_configuration).unwrap();
    }

    #[rstest]
    #[case::integer_double("integer", "double")]
    #[case::integer_decimal("integer", "decimal(11,1)")]
    #[case::decimal_scale_change("decimal(10,2)", "decimal(20,5)")]
    #[case::date_timestamp_ntz("date", "timestamp_ntz")]
    #[case::long_double("long", "double")]
    fn v3_type_change_validation_rejects_non_iceberg_promotions(
        #[case] from_type: &str,
        #[case] to_type: &str,
    ) {
        let table_configuration = table_config_with_schema(schema! {
            (field_with_type_change("a", from_type, to_type)),
        });

        let err = iceberg_compat_v3_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("icebergCompatV3 does not support type change")
                && err.contains("a")
                && err.contains(from_type)
                && err.contains(to_type),
            "unexpected error: {err}"
        );
    }

    #[rstest]
    #[case::nested_struct(
        schema! {
            nullable "s": {
                (field_with_type_change("inner", "integer", "double")),
            },
        },
        "s.inner",
    )]
    #[case::array_element_struct(
        schema! {
            nullable "arr": [ nullable {
                (field_with_type_change("inner", "integer", "double")),
            } ],
        },
        "arr.element.inner",
    )]
    #[case::map_value_struct(
        schema! {
            nullable "m": { STRING => nullable {
                (field_with_type_change("inner", "integer", "double")),
            } },
        },
        "m.value.inner",
    )]
    fn v3_type_change_validation_reports_field_path(
        #[case] schema: StructType,
        #[case] expected_path: &str,
    ) {
        let table_configuration = table_config_with_schema(schema);

        let err = iceberg_compat_v3_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(err.contains(expected_path), "unexpected error: {err}");
    }

    #[test]
    fn v3_type_change_validation_rejects_malformed_type_change_metadata() {
        let table_configuration = table_config_with_schema(schema! {
            (StructField::nullable("a", DataType::STRING).add_metadata([(
                ColumnMetadataKey::TypeChanges.as_ref(),
                MetadataValue::String("not an array".to_string()),
            )])),
        });

        let err = iceberg_compat_v3_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("non-array") && err.contains(ColumnMetadataKey::TypeChanges.as_ref()),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn v3_type_change_validation_skips_tables_without_type_widening_support() {
        let table_configuration = table_config_with_schema_and_features(
            schema! {
                (field_with_type_change("a", "integer", "double")),
            },
            [TableFeature::IcebergCompatV3],
        );

        iceberg_compat_v3_type_changes_validation(&table_configuration).unwrap();
    }

    #[test]
    fn v3_type_change_validation_blocks_writes_but_not_table_configuration() {
        let table_configuration = MockTableConfigurationBuilder::new()
            .with_schema(schema! {
                (field_with_type_change_and_column_mapping("a", "integer", "double")),
            })
            .with_properties([
                (ENABLE_ICEBERG_COMPAT_V3, "true"),
                (ENABLE_ROW_TRACKING, "true"),
            ])
            .with_column_mapping(ColumnMappingMode::Name)
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_features([
                        TableFeature::IcebergCompatV3,
                        TableFeature::ColumnMapping,
                        TableFeature::RowTracking,
                        TableFeature::DomainMetadata,
                        TableFeature::TypeWidening,
                    ])
                    .build(),
            )
            .build();

        assert!(table_configuration.is_feature_enabled(&TableFeature::IcebergCompatV3));
        let err = iceberg_compat_v3_type_changes_validation(&table_configuration)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("icebergCompatV3 does not support type change"),
            "unexpected error: {err}"
        );
    }
}
