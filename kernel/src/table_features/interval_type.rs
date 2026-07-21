//! Validation for interval type support

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Validates that writes to tables with ANSI interval columns are allowed
pub(crate) fn validate_interval_type_feature_support_on_write(
    tc: &TableConfiguration,
) -> DeltaResult<()> {
    let interval_type_feature_enabled = tc
        .protocol()
        .has_table_feature(&TableFeature::IntervalTypePreview);
    require!(
        cfg!(feature = "interval-type-in-dev")
            || (!interval_type_feature_enabled
                && !schema_contains_interval_type(&tc.logical_schema())),
        Error::unsupported(
            "Writing interval columns or enabling 'intervalType-preview' requires the 'interval-type-in-dev' cargo feature"
        )
    );
    Ok(())
}

pub(crate) fn schema_contains_interval_type(schema: &Schema) -> bool {
    UsesIntervalType.transform_struct(schema).is_err()
}

struct UsesIntervalType;

impl<'a> SchemaTransform<'a> for UsesIntervalType {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Result<(), ()> {
        if ptype.is_interval() {
            return Err(());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::validate_interval_type_feature_support_on_write;
    use crate::actions::Protocol;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::make_test_tc;

    fn modern_protocol(with_feature: bool) -> Protocol {
        if with_feature {
            Protocol::try_new_modern(
                [TableFeature::IntervalTypePreview],
                [TableFeature::IntervalTypePreview],
            )
            .unwrap()
        } else {
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap()
        }
    }

    /// Interval columns (top-level and nested) and the `intervalType-preview` table feature require
    /// the `interval-type-in-dev` cargo feature on the write path.
    #[rstest]
    fn test_validate_interval_type_feature_support(
        #[values(PrimitiveType::IntervalYearMonth, PrimitiveType::IntervalDayTime)]
        interval: PrimitiveType,
        #[values(false, true)] nested: bool,
        #[values(false, true)] with_table_feature: bool,
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

        let tc = make_test_tc(schema, modern_protocol(with_table_feature), []).unwrap();
        let result = validate_interval_type_feature_support_on_write(&tc);
        if cfg!(feature = "interval-type-in-dev") {
            result.expect("interval writes must be allowed when the cargo feature is enabled");
        } else {
            let err = result
                .expect_err("interval writes must be blocked when the cargo feature is disabled")
                .to_string();
            assert!(err.contains("interval-type-in-dev"));
        }
    }

    /// A schema with no interval columns passes this write gate only when the table does not list
    /// the interval table feature, unless the cargo feature is enabled.
    #[rstest]
    fn test_validate_interval_type_feature_support_without_interval_columns(
        #[values(false, true)] with_table_feature: bool,
    ) {
        let schema = StructType::new_unchecked([StructField::new("id", DataType::INTEGER, false)]);
        let tc = make_test_tc(schema, modern_protocol(with_table_feature), []).unwrap();
        let result = validate_interval_type_feature_support_on_write(&tc);
        if cfg!(feature = "interval-type-in-dev") || !with_table_feature {
            result.expect("no interval write gate applies");
        } else {
            assert!(result
                .expect_err("intervalType-preview requires the cargo feature")
                .to_string()
                .contains("interval-type-in-dev"));
        }
    }
}
