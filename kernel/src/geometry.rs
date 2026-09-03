use std::sync::Arc;

use geoarrow_array::array::WkbArray;
use geoarrow_array::cast::{from_wkb, to_wkb};
use geoarrow_arrow_array_58::BinaryArray as GeoBinaryArray;
use geoarrow_schema::{
    GeoArrowType, GeometryType as GeoArrowGeometryType, Metadata as GeoArrowMetadata,
};

use crate::schema::GeometryType;
use crate::{DeltaResult, Error};

pub(crate) fn normalize_geometry_wkb(_ty: &GeometryType, bytes: &[u8]) -> DeltaResult<Vec<u8>> {
    let geometry = parse_wkb_geometry(bytes)?;
    let wkb = to_wkb::<i32>(geometry.as_ref()).map_err(geoarrow_error)?;
    Ok(wkb.inner().value(0).to_vec())
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

    fn zero_point_wkb() -> Vec<u8> {
        vec![
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    }

    #[test]
    fn normalize_geometry_wkb_round_trips_point() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let bytes = zero_point_wkb();
        assert_eq!(normalize_geometry_wkb(&ty, &bytes).unwrap(), bytes);
    }

    #[test]
    fn normalize_geometry_wkb_rejects_invalid_bytes() {
        let ty = GeometryType::try_new("EPSG:4326").unwrap();
        let err = normalize_geometry_wkb(&ty, &[0x01, 0x02, 0x03]).unwrap_err();
        assert!(err.to_string().contains("Invalid geometry value"));
    }
}
