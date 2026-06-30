//! Validation for the `intervalType` feature support

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Validates that if a table schema contains ANSI interval columns, the table must have the
/// `IntervalType` feature in both reader and writer features.
///
/// This enforces the protocol rule that writers must not write interval columns to a table whose
/// protocol does not declare the `intervalType` feature. Kernel-created interval tables carry the
/// feature (auto-enabled at create time); appending interval data to a pre-existing featureless
/// table (e.g. a legacy DBR table) is refused here rather than silently upgraded.
pub(crate) fn validate_interval_type_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::IntervalType) {
        require!(
            !schema_contains_interval_type(&tc.logical_schema()),
            Error::unsupported(
                "Table contains interval columns but does not have the required 'intervalType' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Checks if any column in the schema (including nested structs, arrays, maps) uses an ANSI
/// interval primitive type (`IntervalYearMonth` or `IntervalDayTime`).
pub(crate) fn schema_contains_interval_type(schema: &Schema) -> bool {
    UsesIntervalType.transform_struct(schema).is_err()
}

struct UsesIntervalType;

impl<'a> SchemaTransform<'a> for UsesIntervalType {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Result<(), ()> {
        match ptype {
            PrimitiveType::IntervalYearMonth | PrimitiveType::IntervalDayTime => Err(()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::validate_interval_type_feature_support;
    use crate::actions::Protocol;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::make_test_tc;

    fn modern_protocol(with_feature: bool) -> Protocol {
        if with_feature {
            Protocol::try_new_modern([TableFeature::IntervalType], [TableFeature::IntervalType])
                .unwrap()
        } else {
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap()
        }
    }

    /// Interval columns (top-level and nested) require the `intervalType` feature on the write
    /// path; without it, validation fails, with it, it passes. Validation is independent of the
    /// `interval-type-in-dev` cargo gate (it checks feature presence, not kernel support).
    #[rstest]
    fn test_interval_write_requires_feature(
        #[values(PrimitiveType::IntervalYearMonth, PrimitiveType::IntervalDayTime)]
        interval: PrimitiveType,
        #[values(false, true)] nested: bool,
    ) {
        let interval = DataType::Primitive(interval);
        let iv_field = if nested {
            StructField::new(
                "nested",
                StructType::new_unchecked([StructField::new("inner_iv", interval, true)]),
                true,
            )
        } else {
            StructField::new("iv", interval, true)
        };
        let schema =
            StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false), iv_field]);

        let tc = make_test_tc(schema.clone(), modern_protocol(true), []).unwrap();
        validate_interval_type_feature_support(&tc).expect("feature present -> ok");

        let tc = make_test_tc(schema, modern_protocol(false), []).unwrap();
        let err = validate_interval_type_feature_support(&tc)
            .expect_err("interval columns without the feature must be rejected")
            .to_string();
        assert!(
            err.contains("intervalType") && err.contains("interval columns"),
            "error must explain the missing feature; got: {err}",
        );
    }

    /// A schema with no interval columns passes regardless of whether the feature is present.
    #[test]
    fn test_no_interval_columns_needs_no_feature() {
        let schema = StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)]);
        let tc = make_test_tc(schema, modern_protocol(false), []).unwrap();
        validate_interval_type_feature_support(&tc).expect("no interval columns -> ok");
    }
}
