//! Validation for TIMESTAMP_NANOS feature support

use super::TableFeature;
use crate::actions::Protocol;
use crate::schema::{PrimitiveType, Schema, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

use std::borrow::Cow;

/// Validates that if a table schema contains TIMESTAMP_NANOS columns, the table must have the
/// NanosecondTimestamp feature in both reader and writer features.
pub(crate) fn validate_timestamp_nanos_feature_support(
    schema: &Schema,
    protocol: &Protocol,
) -> DeltaResult<()> {
    if !protocol.has_table_feature(&TableFeature::TimestampNanos) {
        let mut uses_nanosecond_timestamp = UsesNanosecondTimestamp(false);
        let _ = uses_nanosecond_timestamp.transform_struct(schema);
        require!(
            !uses_nanosecond_timestamp.0,
            Error::unsupported(
                "Table contains TIMESTAMP_NANOS columns but does not have the required 'timestampNanos' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Schema visitor that checks if any column in the schema uses TIMESTAMP_NANOS type
struct UsesNanosecondTimestamp(bool);

impl<'a> SchemaTransform<'a> for UsesNanosecondTimestamp {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if *ptype == PrimitiveType::TimestampNanos {
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
    fn test_timestamp_nanos_feature_validation() {
        let schema_with_timestamp_nanos = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "ts",
                DataType::Primitive(PrimitiveType::TimestampNanos),
                true,
            ),
        ]);

        let schema_without_timestamp_nanos = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);

        // Protocol with NanosecondTimestamp features
        let protocol_with_features = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampNanos]),
            Some([TableFeature::TimestampNanos]),
        )
        .unwrap();

        // Protocol without NanosecondTimestamp features
        let protocol_without_features = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some::<Vec<String>>(vec![]),
        )
        .unwrap();

        // Schema with TIMESTAMP_NANOS + Protocol with features = OK
        validate_timestamp_nanos_feature_support(
            &schema_with_timestamp_nanos,
            &protocol_with_features,
        )
        .expect("Should succeed when features are present");

        // Schema without TIMESTAMP_NANOS + Protocol without features = OK
        validate_timestamp_nanos_feature_support(
            &schema_without_timestamp_nanos,
            &protocol_without_features,
        )
        .expect("Should succeed when no TIMESTAMP_NANOS columns are present");

        // Schema without TIMESTAMP_NANOS + Protocol with features = OK
        validate_timestamp_nanos_feature_support(
            &schema_without_timestamp_nanos,
            &protocol_with_features,
        )
        .expect("Should succeed when no TIMESTAMP_NANOS columns are present, even with features");

        // Schema with TIMESTAMP_NANOS + Protocol without features = ERROR
        let result = validate_timestamp_nanos_feature_support(
            &schema_with_timestamp_nanos,
            &protocol_without_features,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NANOS columns but does not have the required 'timestampNanos' feature in reader and writer features");

        // Nested schema with TIMESTAMP_NANOS
        let nested_schema_with_timestamp_nanos = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::new(
                    "inner_ts",
                    DataType::Primitive(PrimitiveType::TimestampNanos),
                    true,
                )]))),
                true,
            ),
        ]);

        let result = validate_timestamp_nanos_feature_support(
            &nested_schema_with_timestamp_nanos,
            &protocol_without_features,
        );
        assert_result_error_with_message(result, "Unsupported: Table contains TIMESTAMP_NANOS columns but does not have the required 'timestampNanos' feature in reader and writer features");
    }
}
