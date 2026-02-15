//! CRC file writing support.
//!
//! Serializes a [`Crc`] struct to JSON and writes it to `_delta_log/{version}.crc`.

use std::sync::Arc;

use url::Url;

use super::Crc;
use crate::engine_data::FilteredEngineData;
use crate::path::ParsedLogPath;
use crate::schema::ToSchema as _;
use crate::{DeltaResult, Engine, Error, IntoEngineData as _, Version};

/// Write a CRC file for the given version. The write is idempotent: if the CRC file already
/// exists, the write is silently ignored (another writer may have written it first).
///
/// Uses `PutMode::Create` (overwrite=false) so that concurrent writes don't clobber each other.
#[allow(dead_code)] // Called from Phase 5 (post-commit snapshot integration)
pub(crate) fn write_crc_file(
    engine: &dyn Engine,
    table_root: &Url,
    crc: &Crc,
    version: Version,
) -> DeltaResult<()> {
    let crc_path = ParsedLogPath::new_crc(table_root, version)?;
    let schema = Arc::new(Crc::to_schema());
    let engine_data = crc.clone().into_engine_data(schema, engine)?;
    let filtered = FilteredEngineData::with_all_rows_selected(engine_data);

    // Write with overwrite=false (PutMode::Create). If the file already exists, treat as success
    // (idempotent write).
    match engine.json_handler().write_json_file(
        &crc_path.location,
        Box::new(std::iter::once(Ok(filtered))),
        false,
    ) {
        Ok(()) => Ok(()),
        Err(Error::FileAlreadyExists(_)) => Ok(()),
        Err(e) => Err(e),
    }
}
