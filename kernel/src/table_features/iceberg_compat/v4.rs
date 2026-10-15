//! IcebergNativeV4 checks.

use super::v3::is_v3_supported_type;
use super::{
    check_only_supported_types, IcebergCompatCheck, IcebergCompatValidator, IcebergCompatVersion,
};
use crate::schema::DataType;
use crate::schema::PrimitiveType::{Byte, Short};
use crate::table_configuration::TableConfiguration;
use crate::KernelResult;

/// IcebergNativeV4 invariants paired with the version constant. Fed to
/// [`super::validate_iceberg_compat_if_needed`].
pub(crate) const V4_VALIDATOR: IcebergCompatValidator = IcebergCompatValidator {
    version: IcebergCompatVersion::NativeV4,
    checks: V4_CHECKS,
};

const V4_CHECKS: &[IcebergCompatCheck] = &[IcebergCompatCheck::always(check_v4_supported_types)];

/// IcebergNativeV4 supports the IcebergCompatV3 types except byte and short.
fn is_v4_supported_type(dt: &DataType) -> bool {
    !matches!(dt, DataType::Primitive(Byte | Short)) && is_v3_supported_type(dt)
}

fn check_v4_supported_types(tc: &TableConfiguration) -> KernelResult<()> {
    check_only_supported_types(
        tc,
        is_v4_supported_type,
        IcebergCompatVersion::NativeV4.as_table_feature().as_ref(),
    )
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::schema::{schema, StructType};
    use crate::unit_test_utils::{assert_result_error_with_message, MockTableConfigurationBuilder};

    #[rstest]
    #[case::integer(schema! { nullable "i": INTEGER }, None)]
    #[case::byte_top_level(schema! { nullable "b": BYTE }, Some("b (byte)"))]
    #[case::short_in_struct(
        schema! { nullable "s": { nullable "x": SHORT } },
        Some("s.x (short)"),
    )]
    #[case::byte_array_element(
        schema! { nullable "a": [ nullable BYTE ] },
        Some("a.element (byte)"),
    )]
    fn v4_supported_types_validation(#[case] schema: StructType, #[case] offender: Option<&str>) {
        let tc = MockTableConfigurationBuilder::new()
            .with_schema(schema)
            .build();
        let result = check_v4_supported_types(&tc);
        match offender {
            Some(offender) => assert_result_error_with_message(
                result,
                &format!("icebergNativeV4-preview does not support type at column: {offender}"),
            ),
            None => assert!(result.is_ok(), "expected Ok, got {result:?}"),
        }
    }
}
