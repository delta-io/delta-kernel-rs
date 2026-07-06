//! IcebergCompatV3 checks.

use super::{
    check_no_legacy_nested_ids, check_only_supported_types, IcebergCompatCheck,
    IcebergCompatValidator, IcebergCompatVersion,
};
use crate::schema::DataType;
use crate::schema::PrimitiveType::*;
use crate::table_configuration::TableConfiguration;
use crate::DeltaResult;
#[cfg(feature = "column-defaults-in-dev")]
use crate::Error;

/// V3 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V3_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V3,
    checks: V3_CHECKS,
};

const V3_CHECKS: &[IcebergCompatCheck] = &[
    check_v3_supported_types,
    check_no_legacy_nested_ids,
    // `check_column_defaults` only exists under `column-defaults-in-dev`, so it is gated as an
    // individual slice element rather than duplicating the whole list per cfg.
    #[cfg(feature = "column-defaults-in-dev")]
    check_column_defaults,
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

/// Validates column defaults on an IcebergCompatV3 table.
///
/// The IcebergCompatV3 spec requires only that a column default be a literal. Kernel imposes two
/// tighter limitations here:
/// - The default must be a *kernel-parsable* literal. A literal kernel's SQL parser cannot parse
///   (so [`ColumnDefault::is_literal`] is `false`, e.g. `current_timestamp()`) is rejected, since
///   kernel cannot materialize it into the Iceberg metadata.
/// - The column must be primitive. A `NULL` default on a non-primitive column is allowed on a
///   normal table but rejected here, matching Spark, which does not emit non-primitive defaults for
///   IcebergCompatV3.
///
/// [`ColumnDefault::is_literal`]: crate::schema::ColumnDefault::is_literal
#[cfg(feature = "column-defaults-in-dev")]
fn check_column_defaults(tc: &TableConfiguration) -> DeltaResult<()> {
    for (path, column_default) in crate::schema::collect_column_defaults(tc.logical_schema_ref())? {
        if column_default.data_type().as_primitive_opt().is_none() {
            return Err(Error::generic(format!(
                "kernel does not support a column default on non-primitive column '{path}' on \
                 icebergCompatV3 tables"
            )));
        }
        if !column_default.is_literal() {
            return Err(Error::generic(format!(
                "icebergCompatV3 requires the column default for '{path}' to be a literal kernel \
                 can parse, got: {}",
                column_default.raw_sql()
            )));
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
    use url::Url;

    use super::check_column_defaults;
    use crate::actions::{Metadata, Protocol};
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };
    use crate::table_configuration::TableConfiguration;
    use crate::table_features::TableFeature;

    /// Builds a `TableConfiguration` carrying `schema` with `allowColumnDefaults` enabled, so
    /// `check_column_defaults` can be driven directly. The config does not enable IcebergCompatV3
    /// itself (whose required dependencies are heavy to assemble here); the V3 check is invoked
    /// directly instead, and the end-to-end V3 path is covered by the integration tests.
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

    #[rstest]
    #[case::primitive_literal(DataType::INTEGER, "42", None)]
    #[case::primitive_null(DataType::INTEGER, "NULL", None)]
    #[case::non_literal_primitive(DataType::TIMESTAMP, "current_timestamp()", Some("literal"))]
    #[case::null_on_non_primitive(
        ArrayType::new(DataType::INTEGER, true).into(),
        "NULL",
        Some("non-primitive")
    )]
    fn check_v3_column_default(
        #[case] data_type: DataType,
        #[case] default_sql: &str,
        #[case] expected_error: Option<&str>,
    ) {
        let field = StructField::nullable("a", data_type).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::String(default_sql.to_string()),
        )]);
        let tc = table_config_with_schema(StructType::try_new([field]).unwrap());

        match (check_column_defaults(&tc), expected_error) {
            (Ok(()), None) => {}
            (Err(e), Some(needle)) => assert!(e.to_string().contains(needle), "got: {e}"),
            (result, expected) => {
                panic!("unexpected outcome for {default_sql:?}: {result:?} vs {expected:?}")
            }
        }
    }

    #[test]
    fn check_v3_column_default_rejects_nested_non_literal() {
        let inner = StructField::nullable("inner", DataType::TIMESTAMP).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::String("current_timestamp()".to_string()),
        )]);
        let schema = StructType::try_new([StructField::nullable(
            "s",
            DataType::try_struct_type([inner]).unwrap(),
        )])
        .unwrap();
        let tc = table_config_with_schema(schema);

        let err = check_column_defaults(&tc)
            .expect_err("non-literal default on a nested field must be rejected")
            .to_string();
        assert!(
            err.contains("s.inner"),
            "expected nested path in error: {err}"
        );
    }
}
