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

// `check_column_defaults` only exists under `column-defaults-in-dev`, so it is gated as an
// individual slice element rather than duplicating the whole list per cfg.
const V3_CHECKS: &[IcebergCompatCheck] = &[
    check_v3_supported_types,
    check_no_legacy_nested_ids,
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
/// The IcebergCompatV3 spec requires only that a column default be a literal, so non-literal
/// defaults (e.g. `current_timestamp()`) are rejected. The kernel additionally restricts defaults
/// to primitive columns: a `NULL` default on a non-primitive column is allowed on a normal table
/// but rejected here.
#[cfg(feature = "column-defaults-in-dev")]
fn check_column_defaults(tc: &TableConfiguration) -> DeltaResult<()> {
    validate_v3_column_defaults(tc.logical_schema_ref())
}

#[cfg(feature = "column-defaults-in-dev")]
fn validate_v3_column_defaults(schema: &crate::schema::StructType) -> DeltaResult<()> {
    for field in schema.fields() {
        let Some(column_default) = field.column_default()? else {
            continue;
        };
        if column_default.data_type().as_primitive_opt().is_none() {
            return Err(Error::generic(format!(
                "kernel does not support a column default on non-primitive column '{}' on \
                 icebergCompatV3 tables",
                field.name()
            )));
        }
        if !column_default.is_literal() {
            return Err(Error::generic(format!(
                "icebergCompatV3 requires the column default for '{}' to be a literal, got: {}",
                field.name(),
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
    use rstest::rstest;

    use super::validate_v3_column_defaults;
    use crate::schema::{
        ArrayType, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };

    #[rstest]
    #[case::primitive_literal(DataType::INTEGER, "42", None)]
    #[case::primitive_null(DataType::INTEGER, "NULL", None)]
    #[case::non_literal_primitive(DataType::TIMESTAMP, "current_timestamp()", Some("literal"))]
    #[case::null_on_non_primitive(
        ArrayType::new(DataType::INTEGER, true).into(),
        "NULL",
        Some("non-primitive")
    )]
    fn validate_v3_column_default(
        #[case] data_type: DataType,
        #[case] default_sql: &str,
        #[case] expected_error: Option<&str>,
    ) {
        let field = StructField::nullable("a", data_type).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::String(default_sql.to_string()),
        )]);
        let schema = StructType::try_new([field]).unwrap();

        match (validate_v3_column_defaults(&schema), expected_error) {
            (Ok(()), None) => {}
            (Err(e), Some(needle)) => assert!(e.to_string().contains(needle), "got: {e}"),
            (result, expected) => {
                panic!("unexpected outcome for {default_sql:?}: {result:?} vs {expected:?}")
            }
        }
    }
}
