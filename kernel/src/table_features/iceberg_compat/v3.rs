//! IcebergCompatV3 checks.

use super::{
    check_no_legacy_nested_ids, has_only_supported_types, IcebergCompatValidator,
    IcebergCompatVersion,
};
use crate::schema::DataType;
use crate::table_configuration::TableConfiguration;
use crate::DeltaResult;

/// V3 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V3_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::V3,
    checks: &[check_v3_supported_types, check_no_legacy_nested_ids],
};

/// Returns `true` if `dt` is allowed at any level of the schema under V3.
fn is_v3_supported_type(dt: &DataType) -> bool {
    use crate::schema::PrimitiveType::*;
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
    has_only_supported_types(
        tc,
        is_v3_supported_type,
        IcebergCompatVersion::V3.as_table_feature().as_ref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ArrayType, MapType, StructField, StructType};

    #[test]
    fn is_v3_supported_type_accepts_every_current_datatype() {
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
        ];
        for dt in primitives {
            assert!(
                is_v3_supported_type(&dt),
                "primitive {dt} should be V3-supported"
            );
        }
        let nested = [
            DataType::Array(Box::new(ArrayType::new(DataType::INTEGER, true))),
            DataType::Map(Box::new(MapType::new(
                DataType::STRING,
                DataType::INTEGER,
                true,
            ))),
            DataType::Struct(Box::new(StructType::new_unchecked([
                StructField::nullable("x", DataType::INTEGER),
            ]))),
            DataType::unshredded_variant(),
        ];
        for dt in nested {
            assert!(
                is_v3_supported_type(&dt),
                "nested {dt} should be V3-supported"
            );
        }
    }
}
