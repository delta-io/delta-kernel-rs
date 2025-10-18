//! Validation for TIMESTAMP_NTZ feature support

use super::TableFeature;
use crate::actions::Protocol;
use crate::schema::{PrimitiveType, Schema, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

use std::borrow::Cow;

/// Validates that if a table schema contains TIMESTAMP_NTZ columns, the table must have the
/// TimestampWithoutTimezone feature in both reader and writer features.
pub(crate) fn validate_timestamp_ntz_feature_support(
    schema: &Schema,
    protocol: &Protocol,
) -> DeltaResult<()> {
    if !protocol.has_table_feature(&TableFeature::TimestampWithoutTimezone) {
        let mut uses_timestamp_ntz = UsesTimestampNtz(false);
        let _ = uses_timestamp_ntz.transform_struct(schema);
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
    use super::*;
    use crate::actions::Protocol;
    use crate::schema::{DataType, PrimitiveType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::assert_result_error_with_message;

    #[test]
    fn test_timestamp_ntz_feature_validation() {
        let schema_with_timestamp_ntz = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("ts", DataType::Primitive(PrimitiveType::TimestampNtz), true),
        ]);

        let schema_without_timestamp_ntz = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        // Protocol with TimestampWithoutTimezone features
        let protocol_with_features = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampWithoutTimezone]),
            Some([TableFeature::TimestampWithoutTimezone]),
        )
        .unwrap();

        // Protocol without TimestampWithoutTimezone features
        let protocol_without_features = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some::<Vec<String>>(vec![]),
        )
        .unwrap();

        // Schema with TIMESTAMP_NTZ + Protocol with features = OK
        validate_timestamp_ntz_feature_support(&schema_with_timestamp_ntz, &protocol_with_features)
            .expect("Should succeed when features are present");

        // Schema without TIMESTAMP_NTZ + Protocol without features = OK
        validate_timestamp_ntz_feature_support(
            &schema_without_timestamp_ntz,
            &protocol_without_features,
        )
        .expect("Should succeed when no TIMESTAMP_NTZ columns are present");

        // Schema without TIMESTAMP_NTZ + Protocol with features = OK
        validate_timestamp_ntz_feature_support(
            &schema_without_timestamp_ntz,
            &protocol_with_features,
        )
        .expect("Should succeed when no TIMESTAMP_NTZ columns are present, even with features");

        // Schema with TIMESTAMP_NTZ + Protocol without features = ERROR
        let result = validate_timestamp_ntz_feature_support(
            &schema_with_timestamp_ntz,
            &protocol_without_features,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features");

        // Nested schema with TIMESTAMP_NTZ
        let nested_schema_with_timestamp_ntz = StructType::new_unchecked([
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

        let result = validate_timestamp_ntz_feature_support(
            &nested_schema_with_timestamp_ntz,
            &protocol_without_features,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NTZ columns but does not have the required 'timestampNtz' feature in reader and writer features");
    }
}
