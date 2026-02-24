//! Utility functions for the variant type and variant-related table features.

use crate::schema::{SchemaTransform, StructType};
use crate::table_configuration::TableConfiguration;
use crate::table_features::TableFeature;
use crate::utils::require;
use crate::{DeltaResult, Error};
use std::borrow::Cow;

/// Schema visitor that checks if any column in the schema uses VARIANT type
#[derive(Debug, Default)]
pub(crate) struct UsesVariant(pub(crate) bool);

impl<'a> SchemaTransform<'a> for UsesVariant {
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        self.0 = true;
        None
    }
}

pub(crate) fn validate_variant_type_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    // Both the reader and writer need to have either the VariantType or the VariantTypePreview
    // features.
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::VariantType)
        && !protocol.has_table_feature(&TableFeature::VariantTypePreview)
    {
        let mut uses_variant = UsesVariant::default();
        let _ = uses_variant.transform_struct(&tc.schema());
        require!(
            !uses_variant.0,
            Error::unsupported(
                "Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::Protocol;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{
        assert_result_error_with_message, assert_schema_feature_validation,
    };

    #[test]
    fn test_is_unshredded_variant() {
        fn is_unshredded_variant(s: &DataType) -> bool {
            s == &DataType::unshredded_variant()
        }
        assert!(!is_unshredded_variant(
            &DataType::variant_type([
                StructField::not_null("metadata", DataType::BINARY),
                StructField::nullable("value", DataType::BINARY),
                StructField::nullable("another_field", DataType::BINARY),
            ])
            .unwrap()
        ));
        assert!(is_unshredded_variant(
            &DataType::variant_type([
                StructField::not_null("metadata", DataType::BINARY),
                StructField::not_null("value", DataType::BINARY),
            ])
            .unwrap()
        ));
    }

    #[test]
    fn test_variant_feature_validation() {
        let schema_with = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("v", DataType::unshredded_variant(), true),
        ]);
        let schema_without = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let nested_schema_with = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::new(
                    "inner_v",
                    DataType::unshredded_variant(),
                    true,
                )]))),
                true,
            ),
        ]);
        let protocol_without =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();
        let err_msg = "Table contains VARIANT columns but does not have the required 'variantType' feature in reader and writer features";

        for (reader, writer) in [
            (TableFeature::VariantType, TableFeature::VariantType),
            (
                TableFeature::VariantTypePreview,
                TableFeature::VariantTypePreview,
            ),
        ] {
            let protocol_with = Protocol::try_new_modern([&reader], [&writer]).unwrap();

            // ReaderWriter features must be listed on both sides
            assert_result_error_with_message(
                Protocol::try_new_modern([&reader], TableFeature::EMPTY_LIST),
                "Reader features must contain only ReaderWriter features that are also listed in writer features",
            );
            assert_result_error_with_message(
                Protocol::try_new_modern(TableFeature::EMPTY_LIST, [&writer]),
                "Writer features must be Writer-only or also listed in reader features",
            );

            assert_schema_feature_validation(
                &schema_with,
                &schema_without,
                &protocol_with,
                &protocol_without,
                &[&nested_schema_with],
                err_msg,
            );
        }
    }
}
