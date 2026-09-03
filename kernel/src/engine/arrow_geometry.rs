use std::sync::Arc;

use geo_traits::GeometryTrait;
use geoarrow_array::array::WktArray;
use geoarrow_array::cast::{from_wkt, to_wkb, AsGeoArrowArray};
use geoarrow_array::GeoArrowArrayAccessor;
use geoarrow_arrow_array_58::StringArray as GeoStringArray;
use geoarrow_schema::{
    Dimension, GeoArrowType, GeometryType as GeoArrowGeometryType, Metadata as GeoArrowMetadata,
    PointType,
};

use crate::expressions::GeometryData;
use crate::schema::GeometryType;
use crate::{DeltaResult, Error};

pub(crate) fn parse_geometry_stats_wkt(ty: &GeometryType, raw: &str) -> DeltaResult<GeometryData> {
    let geometry = parse_wkt_geometry(raw)?;
    let point = geometry_point(geometry.as_ref())?;
    let wkb = to_wkb::<i32>(point.as_ref()).map_err(geoarrow_error)?;
    GeometryData::try_new(ty.clone(), wkb.inner().value(0).to_vec())
}

fn parse_wkt_geometry(raw: &str) -> DeltaResult<Arc<dyn geoarrow_array::GeoArrowArray>> {
    let wkt = WktArray::new(
        GeoStringArray::from(vec![Some(raw)]),
        Arc::new(GeoArrowMetadata::default()),
    );
    let geometry_type = GeoArrowGeometryType::new(Arc::new(GeoArrowMetadata::default()));
    from_wkt(&wkt, GeoArrowType::Geometry(geometry_type)).map_err(geoarrow_error)
}

fn geometry_point(
    geometry: &dyn geoarrow_array::GeoArrowArray,
) -> DeltaResult<Arc<dyn geoarrow_array::GeoArrowArray>> {
    let geometry = geometry.as_geometry();
    let value = unsafe { geometry.value_unchecked(0) }.map_err(geoarrow_error)?;
    match value.as_type() {
        geo_traits::GeometryType::Point(point) => {
            let point_type = PointType::new(Dimension::XY, Arc::new(GeoArrowMetadata::default()));
            let points = [point];
            let point =
                geoarrow_array::builder::PointBuilder::from_points(points.iter(), point_type)
                    .finish();
            Ok(Arc::new(point))
        }
        _ => Err(Error::generic(
            "Geometry stats must be bbox-corner POINT values",
        )),
    }
}

fn geoarrow_error(err: impl std::fmt::Display) -> Error {
    Error::generic(format!("Invalid geometry stats value: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::extract_geometry_stats_point_xy;

    #[test]
    fn parse_geometry_stats_wkt_round_trips_point() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let geometry = parse_geometry_stats_wkt(&ty, "POINT(-122.419 37.774)").unwrap();
        let (x, y) = extract_geometry_stats_point_xy(&ty, geometry.bytes()).unwrap();
        assert_eq!((x, y), (-122.419, 37.774));
    }

    #[test]
    fn parse_geometry_stats_wkt_accepts_whitespace_variants() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let geometry = parse_geometry_stats_wkt(&ty, " POINT ( -122.419 37.774 ) ").unwrap();
        let (x, y) = extract_geometry_stats_point_xy(&ty, geometry.bytes()).unwrap();
        assert_eq!((x, y), (-122.419, 37.774));
    }

    #[test]
    fn parse_geometry_stats_wkt_rejects_non_point() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let err = parse_geometry_stats_wkt(&ty, "LINESTRING(0 0, 1 1)").unwrap_err();
        assert!(err.to_string().contains("POINT"));
    }
}
