//! Validation for the `intervalType-preview` table feature.

use super::TableFeature;
use crate::schema::schema_contains_interval_type;
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Validates that schemas with ANSI interval columns declare `intervalType-preview`.
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
    use crate::actions::Protocol;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::assert_schema_feature_validation;

    #[test]
    fn test_interval_type_feature_validation() {
        let schema_with = StructType::new_unchecked([
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("iv", DataType::INTERVAL_YEAR_MONTH),
        ]);
        let nested_schema_with = StructType::new_unchecked([
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable(
                "nested",
                StructType::new_unchecked([StructField::nullable(
                    "iv",
                    DataType::INTERVAL_DAY_TIME,
                )]),
            ),
        ]);
        let schema_without = StructType::new_unchecked([
            StructField::not_null("id", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
        ]);
        let protocol_with = Protocol::try_new_modern(
            [TableFeature::IntervalTypePreview],
            [TableFeature::IntervalTypePreview],
        )
        .unwrap();
        let protocol_without =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();

        assert_schema_feature_validation(
            &schema_with,
            &schema_without,
            &protocol_with,
            &protocol_without,
            &[&nested_schema_with],
            "Table contains interval columns but does not have the required \
             'intervalType-preview' feature in reader and writer features",
        );
    }
}
