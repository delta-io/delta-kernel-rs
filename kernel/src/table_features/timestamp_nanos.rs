//! Validation for TIMESTAMP_NANOS and TIMESTAMP_NANOS_NTZ feature support

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

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
    UsesTimestampNanos.transform_struct(schema).is_err()
}

struct UsesTimestampNanos;

impl<'a> SchemaTransform<'a> for UsesTimestampNanos {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Result<(), ()> {
        match ptype {
            PrimitiveType::TimestampNanos | PrimitiveType::TimestampNanosNtz => Err(()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::actions::Protocol;
    use crate::schema::{schema, DataType};
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::assert_schema_feature_validation;

    // Nanos columns nested in structs, arrays, and map keys/values must all be detected: the
    // traversal has to short-circuit out of every container kind, not just the top level.
    #[rstest]
    fn test_timestamp_nanos_feature_validation(
        #[values(DataType::TIMESTAMP_NANOS, DataType::TIMESTAMP_NANOS_NTZ)] ts_type: DataType,
        #[values(
            TableFeature::EMPTY_LIST,
            vec![TableFeature::TimestampWithoutTimezone],
            vec![TableFeature::TimestampNanos]
        )]
        features_without: Vec<TableFeature>,
    ) {
        let schema_with = schema! {
            not_null "id": INTEGER,
            nullable "ts": (ts_type.clone()),
        };
        let schema_without = schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        };
        let nested_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "nested": {
                nullable "inner_ts": (ts_type.clone()),
            },
        };
        let array_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "arr": [ nullable (ts_type.clone()) ],
        };
        let map_key_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "map": { (ts_type.clone()) => nullable STRING },
        };
        let map_value_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "map": { STRING => nullable (ts_type) },
        };
        let features_with = [
            TableFeature::TimestampNanos,
            TableFeature::TimestampWithoutTimezone,
        ];
        let protocol_with = Protocol::try_new_modern(features_with.clone(), features_with).unwrap();
        let protocol_without =
            Protocol::try_new_modern(features_without.clone(), features_without).unwrap();

        assert_schema_feature_validation(
            &schema_with,
            &schema_without,
            &protocol_with,
            &protocol_without,
            &[
                &nested_schema_with,
                &array_schema_with,
                &map_key_schema_with,
                &map_value_schema_with,
            ],
            "Table contains TIMESTAMP_NANOS or TIMESTAMP_NANOS_NTZ columns but does not have the required 'timestampNanos' and 'timestampNtz' features in reader and writer features",
        );
    }
}
