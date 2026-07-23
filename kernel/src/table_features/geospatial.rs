//! Validation logic for the `geospatial` table feature.

use super::TableFeature;
use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Returns `true` if the schema contains at least one geometry or geography column,
/// including nested structs, arrays, and maps.
fn schema_contains_geospatial(schema: &Schema) -> bool {
    UsesGeo.transform_struct(schema).is_err()
}

struct UsesGeo;

impl<'a> SchemaTransform<'a> for UsesGeo {
    transform_output_type!(|'a, T| Result<(), ()>);

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Result<(), ()> {
        match ptype {
            PrimitiveType::Geometry(_) | PrimitiveType::Geography(_) => Err(()),
            _ => Ok(()),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::actions::Protocol;
    use crate::schema::{
        ArrayType, DataType, EdgeInterpolationAlgorithm, MapType, StructField, StructType,
    };
    use crate::utils::test_utils::{
        assert_schema_feature_validation, geography_type, geometry_type,
    };

    fn schema_with_field(dt: DataType) -> StructType {
        StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("g", dt, true),
        ])
    }

    #[rstest]
    #[case::top_level_geometry(schema_with_field(geometry_type("EPSG:4326")), true)]
    #[case::nested_struct(
        schema_with_field(DataType::Struct(Box::new(StructType::new_unchecked([
            StructField::new("inner_geo", geography_type("EPSG:4326", EdgeInterpolationAlgorithm::Spherical), true),
        ])))),
        true
    )]
    #[case::in_array(schema_with_field(ArrayType::new(geometry_type("EPSG:4326"), true).into()), true)]
    #[case::in_map(
        schema_with_field(MapType::new(DataType::STRING, geography_type("EPSG:4326", EdgeInterpolationAlgorithm::Spherical), true).into()),
        true
    )]
    #[case::no_geo(schema_with_field(DataType::STRING), false)]
    fn test_schema_contains_geospatial(#[case] schema: StructType, #[case] expected: bool) {
        assert_eq!(schema_contains_geospatial(&schema), expected);
    }

    #[test]
    fn test_geospatial_feature_validation() {
        let schema_with_geotype = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("geom", geometry_type("EPSG:4326"), true),
        ]);
        let schema_without_geotype = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        let nested_schema_with_geotype = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::new(
                    "inner_geo",
                    geography_type("EPSG:4326", EdgeInterpolationAlgorithm::Spherical),
                    true,
                )]))),
                true,
            ),
        ]);
        let protocol_with_geotype = Protocol::try_new_modern(
            [TableFeature::GeospatialType],
            [TableFeature::GeospatialType],
        )
        .unwrap();
        let protocol_without_geotype =
            Protocol::try_new_modern(TableFeature::EMPTY_LIST, TableFeature::EMPTY_LIST).unwrap();

        assert_schema_feature_validation(
            &schema_with_geotype,
            &schema_without_geotype,
            &protocol_with_geotype,
            &protocol_without_geotype,
            &[&nested_schema_with_geotype],
            "Table contains geometry or geography columns but does not have the required 'geospatial' feature in reader and writer features",
        );
    }
}
