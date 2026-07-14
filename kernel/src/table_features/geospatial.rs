//! Validation for geometry / geography schema types.
//!
//! Kernel can represent geospatial types in a schema (so a table's metadata deserializes),
//! but does not yet support any engine operation on them. Until the `geospatial` table
//! feature lands, any table whose schema contains a geometry or geography column is rejected
//! on every path that builds a [`TableConfiguration`] -- create, snapshot load, and
//! incremental update.

use crate::schema::{PrimitiveType, Schema};
use crate::table_configuration::TableConfiguration;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Error};

/// Schema visitor that records whether any column uses a geometry or geography type.
struct UsesGeo(bool);

impl<'a> SchemaTransform<'a> for UsesGeo {
    transform_output_type!(|'a, T| ());
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) {
        if matches!(
            ptype,
            PrimitiveType::Geometry(_) | PrimitiveType::Geography(_)
        ) {
            self.0 = true;
        }
    }
}

/// Returns `true` if the schema contains at least one geometry or geography column,
/// including nested structs, arrays, and maps.
pub(crate) fn schema_contains_geospatial(schema: &Schema) -> bool {
    let mut visitor = UsesGeo(false);
    visitor.transform_struct(schema);
    visitor.0
}

/// Rejects any table whose schema contains a geometry or geography column.
///
/// Kernel does not yet support the `geospatial` table feature, so such tables cannot be
/// created, read, or written. See #2914.
pub(crate) fn validate_geospatial_unsupported(tc: &TableConfiguration) -> DeltaResult<()> {
    require!(
        !schema_contains_geospatial(&tc.logical_schema()),
        Error::unsupported(
            "Table contains geometry or geography columns, which are not yet supported by kernel"
        )
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        DataType, EdgeInterpolationAlgorithm, GeographyType, GeometryType, StructField, StructType,
    };

    fn geometry() -> DataType {
        DataType::Primitive(PrimitiveType::Geometry(Box::new(
            GeometryType::try_new("EPSG:4326").unwrap(),
        )))
    }

    fn geography() -> DataType {
        DataType::Primitive(PrimitiveType::Geography(Box::new(
            GeographyType::try_new("EPSG:4326", EdgeInterpolationAlgorithm::Spherical).unwrap(),
        )))
    }

    #[test]
    fn test_schema_contains_geospatial_top_level() {
        let schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("g", geometry(), true),
        ]);
        assert!(schema_contains_geospatial(&schema));
    }

    #[test]
    fn test_schema_contains_geospatial_nested() {
        let schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new(
                "nested",
                DataType::Struct(Box::new(StructType::new_unchecked([StructField::new(
                    "inner_geo",
                    geography(),
                    true,
                )]))),
                true,
            ),
        ]);
        assert!(schema_contains_geospatial(&schema));
    }

    #[test]
    fn test_schema_without_geospatial() {
        let schema = StructType::new_unchecked([
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
        ]);
        assert!(!schema_contains_geospatial(&schema));
    }
}
