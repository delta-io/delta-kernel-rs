//! Conversions between kernel geo types and GeoArrow WKB extension fields.

use std::collections::HashMap;
use std::sync::Arc;

use crate::arrow::datatypes::Field as ArrowField;
use crate::geoarrow_schema::crs::Crs;
use crate::geoarrow_schema::{Edges, GeoArrowType, Metadata, WkbType};
use crate::schema::EdgeInterpolationAlgorithm;

fn algorithm_to_edges(algo: &EdgeInterpolationAlgorithm) -> Edges {
    match algo {
        EdgeInterpolationAlgorithm::Spherical => Edges::Spherical,
        EdgeInterpolationAlgorithm::Vincenty => Edges::Vincenty,
        EdgeInterpolationAlgorithm::Thomas => Edges::Thomas,
        EdgeInterpolationAlgorithm::Andoyer => Edges::Andoyer,
        EdgeInterpolationAlgorithm::Karney => Edges::Karney,
    }
}

/// Builds an Arrow Field for a WKB-encoded geospatial column with GeoArrow extension
/// metadata encoding the CRS and (for geography) the edge interpolation algorithm.
///
/// Pass algorithm = None for Geometry (planar) and Some(...) for Geography. Any
/// extra_metadata is merged on top of the GeoArrow extension metadata.
pub(super) fn wkb_arrow_field(
    name: &str,
    srid: &str,
    algorithm: Option<&EdgeInterpolationAlgorithm>,
    nullable: bool,
    extra_metadata: HashMap<String, String>,
) -> ArrowField {
    let edges = algorithm.map(algorithm_to_edges);
    let geoarrow_metadata = Arc::new(Metadata::new(Crs::from_srid(srid.to_string()), edges));
    let wkb_type = WkbType::new(geoarrow_metadata);
    let field = GeoArrowType::Wkb(wkb_type).to_field(name, nullable);
    if extra_metadata.is_empty() {
        field
    } else {
        let mut merged = field.metadata().clone();
        merged.extend(extra_metadata);
        field.with_metadata(merged)
    }
}
