//! Validation for the `intervalType-preview` table feature.

use super::TableFeature;
use crate::schema::schema_contains_interval_type;
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Validates that writes to schemas with ANSI interval columns declare `intervalType-preview`.
pub(crate) fn validate_interval_type_feature_support(
    table_config: &TableConfiguration,
) -> DeltaResult<()> {
    if schema_contains_interval_type(&table_config.logical_schema()) {
        require!(
            table_config
                .protocol()
                .has_table_feature(&TableFeature::IntervalTypePreview),
            Error::unsupported(
                "Table contains interval columns but does not have the required \
                 'intervalType-preview' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::validate_interval_type_feature_support;
    use crate::actions::Protocol;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{assert_result_error_with_message, make_test_tc};

    #[rstest]
    fn test_interval_type_feature_validation(
        #[values(DataType::INTERVAL_YEAR_MONTH, DataType::INTERVAL_DAY_TIME)] interval: DataType,
        #[values(false, true)] nested: bool,
        #[values(false, true)] with_feature: bool,
    ) {
        let interval_field = if nested {
            StructField::nullable(
                "nested",
                StructType::new_unchecked([StructField::nullable("iv", interval)]),
            )
        } else {
            StructField::nullable("iv", interval)
        };
        let schema = StructType::new_unchecked([
            StructField::not_null("id", DataType::INTEGER),
            interval_field,
        ]);
        let features = with_feature.then_some(TableFeature::IntervalTypePreview);
        let protocol = Protocol::try_new_modern(features.clone(), features).unwrap();
        let table_config = make_test_tc(schema, protocol, []).unwrap();

        let result = validate_interval_type_feature_support(&table_config);
        if with_feature {
            result.expect("interval table feature should permit writes");
        } else {
            assert_result_error_with_message(
                result,
                "required 'intervalType-preview' feature in reader and writer features",
            );
        }
    }
}
