//! Validation for TIMESTAMP_NANOS feature support

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::SchemaTransform;
use crate::utils::require;
use crate::{DeltaResult, Error};

use std::borrow::Cow;

/// Validates that if a table schema contains TIMESTAMP_NANOS columns, the table must have the
/// TimestampNanos feature in both reader and writer features.
pub(crate) fn validate_timestamp_nanos_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::TimestampNanos) {
        require!(
            !schema_contains_timestamp_nanos(&tc.logical_schema()),
            Error::unsupported(
                "Table contains TIMESTAMP_NANOS columns but does not have the required 'timestampNanos' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

#[cfg(feature = "nanosecond-timestamps")]
/// Checks if any column in the schema (including nested structs, arrays, maps) uses
/// the TIMESTAMP_NANOS primitive type.
pub(crate) fn schema_contains_timestamp_nanos(schema: &Schema) -> bool {
    let mut uses_timestamp_nanos = UsesTimestampNanos(false);
    let _ = uses_timestamp_nanos.transform_struct(schema);
    uses_timestamp_nanos.0
}

struct UsesTimestampNanos(bool);

impl<'a> SchemaTransform<'a> for UsesTimestampNanos {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if *ptype == PrimitiveType::TimestampNanos {
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

        let nested_schema_with = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::new(
                    "inner_nanos",
                    DataType::Primitive(PrimitiveType::TimestampNanos),
                    true,
                )]))),
                true,
            ),
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

        assert_schema_feature_validation(
            &schema_with_timestamp_nanos,
            &schema_without_timestamp_nanos,
            &protocol_with_features,
            &protocol_without_features,
            &[&nested_schema_with],
            "Table contains TIMESTAMP_NANOS columns but does not have the required 'timestampNanos' feature in reader and writer features",
        );
    }
}
