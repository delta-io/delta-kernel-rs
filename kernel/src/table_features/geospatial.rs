//! Validation logic for the `geospatial` table feature.

use std::borrow::Cow;

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::SchemaTransform;
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Validates that if a table schema contains geometry or geography columns, the table must have
/// the `geospatial` feature in both reader and writer features.
pub(crate) fn validate_geospatial_feature_support(tc: &TableConfiguration) -> DeltaResult<()> {
    let protocol = tc.protocol();
    if !protocol.has_table_feature(&TableFeature::GeospatialType) {
        require!(
            !schema_contains_geospatial(&tc.logical_schema()),
            Error::unsupported(
                "Table contains geometry or geography columns but does not have the required 'geospatial' feature in reader and writer features"
            )
        );
    }
    Ok(())
}

/// Returns `true` if the schema contains at least one geometry or geography column,
/// including nested structs, arrays, and maps.
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool {
    let mut detector = GeoDetector(false);
    let _ = detector.transform_struct(schema);
    detector.0
}

struct GeoDetector(bool);

impl<'a> SchemaTransform<'a> for GeoDetector {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if matches!(
            ptype,
            PrimitiveType::Geometry(_) | PrimitiveType::Geography(_)
        ) {
            self.0 = true;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::actions::Protocol;
    use crate::schema::{
        DataType, GeographyType, GeometryType, PrimitiveType, StructField, StructType,
    };
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::assert_schema_feature_validation;

    #[test]
    fn test_geospatial_feature_validation() {
        let schema_with = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "geom",
                DataType::Primitive(PrimitiveType::Geometry(GeometryType::default())),
                true,
            ),
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
                    "inner_geo",
                    DataType::Primitive(PrimitiveType::Geography(GeographyType::default())),
                    true,
                )]))),
                true,
            ),
        ]);
        let protocol_with = Protocol::try_new_modern(
            [TableFeature::GeospatialType],
            [TableFeature::GeospatialType],
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
            "Table contains geometry or geography columns but does not have the required 'geospatial' feature in reader and writer features",
        );
    }
}
