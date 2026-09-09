use std::sync::Arc;

use geoarrow_array::array::WktArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_arrow_array_58::StringArray as GeoStringArray;
use geoarrow_schema::Metadata as GeoArrowMetadata;

use crate::{DeltaResult, Error};

pub(crate) fn wkt_to_wkb_bytes(raw: &str) -> DeltaResult<Vec<u8>> {
    let wkt = WktArray::new(
        GeoStringArray::from(vec![Some(raw)]),
        Arc::new(GeoArrowMetadata::default()),
    );
    let wkb = to_wkb::<i32>(&wkt).map_err(geoarrow_error)?;
    Ok(wkb.inner().value(0).to_vec())
}

fn geoarrow_error(err: impl std::fmt::Display) -> Error {
    Error::generic(format!("Invalid geometry stats value: {err}"))
}
