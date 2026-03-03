//! Validation for TIMESTAMP_NTZ feature support

use super::TableFeature;
use crate::schema::{PrimitiveType, SchemaTransform};
use crate::table_configuration::TableConfiguration;
use crate::utils::require;
use crate::{DeltaResult, Error};

use std::borrow::Cow;

/// Validates that if a table schema contains TIMESTAMP_NTZ columns, the table must have the
/// TimestampWithoutTimezone feature in both reader and writer features.
pub(crate) fn validate_timestamp_ntz_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::TimestampWithoutTimezone) {
        let mut uses_timestamp_ntz = UsesTimestampNtz(false);
        let _ = uses_timestamp_ntz.transform_struct(&tc.schema());
        require!(
            !uses_timestamp_ntz.0,
            Error::unsupported(
                "Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Schema visitor that checks if any column in the schema uses TIMESTAMP_NTZ type
struct UsesTimestampNtz(bool);

impl<'a> SchemaTransform<'a> for UsesTimestampNtz {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if *ptype == PrimitiveType::TimestampNtz {
            self.0 = true;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::actions::Protocol;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::assert_schema_feature_validation;

    #[test]
    fn test_timestamp_ntz_feature_validation() {
        let schema_with = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("ts", DataType::Primitive(PrimitiveType::TimestampNtz), true),
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
                    "inner_ts",
                    DataType::Primitive(PrimitiveType::TimestampNtz),
                    true,
                )]))),
                true,
            ),
        ]);
        let protocol_with = Protocol::try_new_modern(
            [TableFeature::TimestampWithoutTimezone],
            [TableFeature::TimestampWithoutTimezone],
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
            "Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features",
        );
    }
}
