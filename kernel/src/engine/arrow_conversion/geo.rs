use std::collections::HashMap;

use serde_json::json;

use crate::schema::GeometryType;

pub(crate) const GEOARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub(crate) const GEOARROW_EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";
pub(crate) const GEOARROW_WKB_EXTENSION_NAME: &str = "geoarrow.wkb";

pub(crate) fn geometry_geoarrow_metadata(geometry: &GeometryType) -> HashMap<String, String> {
    HashMap::from([
        (
            GEOARROW_EXTENSION_NAME_KEY.to_string(),
            GEOARROW_WKB_EXTENSION_NAME.to_string(),
        ),
        (
            GEOARROW_EXTENSION_METADATA_KEY.to_string(),
            json!({
                "crs": geometry.crs(),
                "crs_type": "authority_code",
            })
            .to_string(),
        ),
    ])
}
