//! Validation for TIMESTAMP_NANOS and TIMESTAMP_NANOS_NTZ feature support

use std::borrow::Cow;

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::SchemaTransform;
use crate::utils::require;
use crate::{transform_output_type, DeltaResult, Error};

/// Validates that if a table schema contains TIMESTAMP_NANOS or TIMESTAMP_NANOS_NTZ columns,
/// the table must have the TimestampNanos and TimestampNtz features in both reader and writer
/// features.
pub(crate) fn validate_timestamp_nanos_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::TimestampNanos)
        || !protocol.has_table_feature(&TableFeature::TimestampWithoutTimezone)
    {
        require!(
            !schema_contains_timestamp_nanos(&tc.logical_schema()),
            Error::unsupported(
                "Table contains TIMESTAMP_NANOS or TIMESTAMP_NANOS_NTZ columns but does not have the required 'timestampNanos' and 'timestampNtz' features in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Checks if any column in the schema (including nested structs, arrays, maps) uses
/// the TIMESTAMP_NANOS or TIMESTAMP_NANOS_NTZ primitive type.
pub(crate) fn schema_contains_timestamp_nanos(schema: &Schema) -> bool {
    let mut uses_timestamp_nanos = UsesTimestampNanos(false);
    let _ = uses_timestamp_nanos.transform_struct(schema);
    uses_timestamp_nanos.0
}

struct UsesTimestampNanos(bool);

impl<'a> SchemaTransform<'a> for UsesTimestampNanos {
    transform_output_type!(|'a, T| Option<Cow<'a, T>>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if *ptype == PrimitiveType::TimestampNanos || *ptype == PrimitiveType::TimestampNanosNtz {
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

    #[rstest::rstest]
    #[case::nanos(PrimitiveType::TimestampNanos)]
    #[case::nanos_ntz(PrimitiveType::TimestampNanosNtz)]
    fn test_timestamp_nanos_feature_validation(#[case] ptype: PrimitiveType) {
        let schema_with_timestamp_nanos = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("ts", DataType::Primitive(ptype.clone()), true),
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
                    DataType::Primitive(ptype),
                    true,
                )]))),
                true,
            ),
        ]);

        let protocol_with_features = Protocol::try_new(
            3,
            7,
            Some([
                TableFeature::TimestampNanos,
                TableFeature::TimestampWithoutTimezone,
            ]),
            Some([
                TableFeature::TimestampNanos,
                TableFeature::TimestampWithoutTimezone,
            ]),
        )
        .unwrap();

        let protocol_without_features = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some::<Vec<String>>(vec![]),
        )
        .unwrap();

        let protocol_without_nanos = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampWithoutTimezone]),
            Some([TableFeature::TimestampWithoutTimezone]),
        )
        .unwrap();

        let protocol_without_ntz = Protocol::try_new(
            3,
            7,
            Some([TableFeature::TimestampNanos]),
            Some([TableFeature::TimestampNanos]),
        )
        .unwrap();

        for protocol in &[
            protocol_without_features,
            protocol_without_nanos,
            protocol_without_ntz,
        ] {
            assert_schema_feature_validation(
                &schema_with_timestamp_nanos,
                &schema_without_timestamp_nanos,
                &protocol_with_features,
                protocol,
                &[&nested_schema_with],
                "Table contains TIMESTAMP_NANOS or TIMESTAMP_NANOS_NTZ columns but does not have the required 'timestampNanos' and 'timestampNtz' features in reader and writer features",
            );
        }
    }
}
