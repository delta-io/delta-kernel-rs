//! IcebergCompatV3 checks.

#[cfg(feature = "column-defaults-in-dev")]
use tracing::warn;

use super::{
    check_no_legacy_nested_ids, check_only_supported_types, IcebergCompatCheck,
    IcebergCompatValidator, IcebergCompatVersion,
};
#[cfg(feature = "column-defaults-in-dev")]
use crate::schema::try_collect_column_defaults;
use crate::schema::DataType;
use crate::schema::PrimitiveType::*;
use crate::table_configuration::TableConfiguration;
use crate::DeltaResult;

/// V3 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V3_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V3,
    checks: V3_CHECKS,
};

const V3_CHECKS: &[IcebergCompatCheck] = &[check_v3_supported_types, check_no_legacy_nested_ids];

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
                | String
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

/// Validates IcebergCompatV3 column defaults and logs warnings kernel cannot verify.
///
/// The IcebergCompatV3 spec requires column defaults to be literals. Kernel warns when its parser
/// cannot verify that requirement. This warning can be a false positive because a connector's
/// parser may be able to parse the corresponding expression.
///
/// This condition remains a warning because the table has already passed metadata validation and
/// rejecting a DML transaction could block valid tables based on kernel parser limitations. The
/// check provides defense in depth without treating an interoperability risk as definite
/// corruption.
///
/// # Errors
///
/// Propagates malformed column-default metadata errors from [`try_collect_column_defaults`].
#[cfg(feature = "column-defaults-in-dev")]
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

#[cfg(all(test, feature = "column-defaults-in-dev"))]
mod column_default_tests {
    use std::collections::HashMap;

    use rstest::rstest;
    use test_utils::LoggingTest;
    use url::Url;

    use super::iceberg_compat_v3_column_defaults_validation;
    use crate::actions::{Metadata, Protocol};
    use crate::schema::ColumnMetadataKey::CurrentDefault;
    use crate::schema::{ArrayType, DataType, MetadataValue, StructField, StructType};
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::TableFeature;

    /// Builds a `TableConfiguration` carrying `schema` with `allowColumnDefaults` enabled, so
    /// the IcebergCompatV3 column-default validation can be driven directly. The config does not
    /// enable IcebergCompatV3 itself (whose required dependencies are heavy to assemble here); the
    /// validation is invoked directly instead, and the end-to-end V3 path is covered by the
    /// integration tests.
    fn table_config_with_schema(schema: StructType) -> TableConfiguration {
        let metadata = Metadata::try_new(
            None,
            None,
            std::sync::Arc::new(schema),
            vec![],
            0,
            HashMap::new(),
        )
        .unwrap();
        let protocol = Protocol::try_new_modern(
            TableFeature::EMPTY_LIST,
            [TableFeature::AllowColumnDefaults],
        )
        .unwrap();
        TableConfiguration::try_new(metadata, protocol, Url::parse("file:///t/").unwrap(), 0)
            .unwrap()
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
        StructType::try_new([field_with_default("a", DataType::INTEGER, "42")]).unwrap(),
        "a",
        None
    )]
    #[case::primitive_null(
        StructType::try_new([field_with_default("a", DataType::INTEGER, "NULL")]).unwrap(),
        "a",
        None
    )]
    #[case::non_literal_primitive(
        StructType::try_new([field_with_default(
            "a",
            DataType::TIMESTAMP,
            "current_timestamp()"
        )]).unwrap(),
        "a",
        Some("could not verify")
    )]
    #[case::null_on_non_primitive(
        StructType::try_new([field_with_default(
            "a",
            ArrayType::new(DataType::INTEGER, true),
            "NULL"
        )]).unwrap(),
        "a",
        None
    )]
    #[case::non_null_on_non_primitive(
        StructType::try_new([field_with_default(
            "a",
            ArrayType::new(DataType::INTEGER, true),
            "ARRAY(1)"
        )]).unwrap(),
        "a",
        Some("could not verify")
    )]
    #[case::nested_non_literal(
        StructType::try_new([StructField::nullable(
            "s",
            DataType::try_struct_type([field_with_default(
                "inner",
                DataType::TIMESTAMP,
                "current_timestamp()"
            )]).unwrap(),
        )]).unwrap(),
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
