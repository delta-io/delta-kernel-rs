//! IcebergCompatV2 checks.

use super::{
    check_no_legacy_nested_ids, check_only_supported_types, IcebergCompatCheck,
    IcebergCompatValidator, IcebergCompatVersion,
};
use crate::schema::DataType;
use crate::schema::PrimitiveType::*;
use crate::table_configuration::TableConfiguration;
use crate::DeltaResult;

/// V2 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V2_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V2,
    checks: V2_CHECKS,
};

const V2_CHECKS: &[IcebergCompatCheck] = &[check_v2_supported_types, check_no_legacy_nested_ids];

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{schema, ArrayType, MapType};

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
}
