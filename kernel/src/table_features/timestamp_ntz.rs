//! Validation for TIMESTAMP_NTZ feature support

use super::TableFeature;
use crate::error::delta_errors;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::DeltaResult;

/// Validates that if a table schema contains TIMESTAMP_NTZ columns, the table must have the
/// TimestampWithoutTimezone feature in both reader and writer features.
pub(crate) fn validate_timestamp_ntz_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::TimestampWithoutTimezone) {
        require!(
            !schema_contains_timestamp_ntz(&tc.logical_schema()),
            delta_errors::features_protocol_metadata_mismatch(["timestampNtz"])
        );
    }
    Ok(())
}

/// Checks if any column in the schema (including nested structs, arrays, maps) uses
/// the TIMESTAMP_NTZ primitive type.
pub(crate) fn schema_contains_timestamp_ntz(schema: &Schema) -> bool {
    UsesTimestampNtz.transform_struct(schema).is_err()
}

struct UsesTimestampNtz;

impl<'a> SchemaTransform<'a> for UsesTimestampNtz {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Result<(), ()> {
        match ptype {
            PrimitiveType::TimestampNtz => Err(()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::actions::Protocol;
    use crate::schema::schema;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::assert_schema_feature_validation;

    #[test]
    fn test_timestamp_ntz_feature_validation() {
        let schema_with = schema! {
            not_null "id": INTEGER,
            nullable "ts": TIMESTAMP_NTZ,
        };
        let schema_without = schema! {
            not_null "id": INTEGER,
            nullable "name": STRING,
        };
        let nested_schema_with = schema! {
            not_null "id": INTEGER,
            nullable "nested": {
                nullable "inner_ts": TIMESTAMP_NTZ,
            },
        };
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
            "timestampNtz",
        );
    }
}
