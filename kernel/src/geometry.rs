use std::sync::Arc;

use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::array::WkbArray;
use geoarrow_array::cast::{from_wkb, to_wkb, AsGeoArrowArray};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_arrow_array_58::BinaryArray as GeoBinaryArray;
use geoarrow_schema::{
    Dimension, GeoArrowType, GeometryType as GeoArrowGeometryType, Metadata as GeoArrowMetadata,
    PointType,
};

use crate::schema::GeometryType;
use crate::{DeltaResult, Error};

pub(crate) fn normalize_geometry_wkb(_ty: &GeometryType, bytes: &[u8]) -> DeltaResult<Vec<u8>> {
    let geometry = parse_wkb_geometry(bytes)?;
    let wkb = to_wkb::<i32>(geometry.as_ref()).map_err(geoarrow_error)?;
    Ok(wkb.inner().value(0).to_vec())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn extract_geometry_stats_point_xy(
    _ty: &GeometryType,
    bytes: &[u8],
) -> DeltaResult<(f64, f64)> {
    let binary = GeoBinaryArray::from(vec![Some(bytes)]);
    let wkb = WkbArray::new(binary, Arc::new(GeoArrowMetadata::default()));
    let point_type = PointType::new(Dimension::XY, Arc::new(GeoArrowMetadata::default()));
    let point = from_wkb(&wkb, GeoArrowType::Point(point_type)).map_err(geoarrow_error)?;
    let point = point.as_point();
    let point = point
        .get(0)
        .map_err(geoarrow_error)?
        .ok_or_else(|| Error::generic("Geometry stats point may not be null"))?;
    let coord = point
        .coord()
        .ok_or_else(|| Error::generic("Geometry stats point may not be empty"))?;
    Ok((coord.x(), coord.y()))
}

fn parse_wkb_geometry(bytes: &[u8]) -> DeltaResult<Arc<dyn geoarrow_array::GeoArrowArray>> {
    let binary = GeoBinaryArray::from(vec![Some(bytes)]);
    let wkb = WkbArray::new(binary, Arc::new(GeoArrowMetadata::default()));
    let geometry_type = GeoArrowGeometryType::new(Arc::new(GeoArrowMetadata::default()));
    from_wkb(&wkb, GeoArrowType::Geometry(geometry_type)).map_err(geoarrow_error)
}

fn geoarrow_error(err: impl std::fmt::Display) -> Error {
    Error::generic(format!("Invalid geometry value: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::arrow_geometry::parse_geometry_stats_wkt;

    #[test]
    fn normalize_geometry_wkb_round_trips_point() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let parsed = parse_geometry_stats_wkt(&ty, "POINT(-122.419 37.774)").unwrap();
        let bytes = normalize_geometry_wkb(&ty, parsed.bytes()).unwrap();
        let (x, y) = extract_geometry_stats_point_xy(&ty, &bytes).unwrap();
        assert_eq!((x, y), (-122.419, 37.774));
    }

    #[test]
    fn normalize_geometry_wkb_rejects_invalid_bytes() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let err = normalize_geometry_wkb(&ty, &[0x01, 0x02, 0x03]).unwrap_err();
        assert!(err.to_string().contains("Invalid geometry value"));
    }
}
