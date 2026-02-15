//! CRC file reading functionality.

use std::sync::Arc;

use super::{Crc, CrcVisitor};
use crate::actions::DomainMetadata;
use crate::engine::arrow_data::ArrowEngineData;
use crate::path::ParsedLogPath;
use crate::schema::ToSchema as _;
use crate::{DeltaResult, Engine, Error, RowVisitor as _};

/// Attempt to read and parse a CRC file.
///
/// Returns `Ok(Crc)` on success, `Err` on any failure (file not readable, corrupt JSON, missing
/// required fields). The caller should handle errors gracefully by falling back to log replay.
pub(crate) fn try_read_crc_file(engine: &dyn Engine, crc_path: &ParsedLogPath) -> DeltaResult<Crc> {
    let json_handler = engine.json_handler();
    let file_meta = crc_path.location.clone();
    let output_schema = Arc::new(Crc::to_schema());

    let mut batches = json_handler.read_json_files(&[file_meta], output_schema, None)?;

    // CRC file should have exactly one batch with one row
    let batch = batches
        .next()
        .ok_or_else(|| Error::generic("CRC file is empty"))??;

    if batch.len() != 1 {
        return Err(Error::generic(format!(
            "CRC file should have exactly 1 row, found {}",
            batch.len()
        )));
    }

    // Use visitor to extract scalar CRC fields (tableSizeBytes, numFiles, metadata, protocol, ICT)
    let mut visitor = CrcVisitor::default();
    visitor.visit_rows_of(batch.as_ref())?;
    let mut crc = visitor.into_crc()?;

    // Extract domain metadata from the already-loaded Arrow batch.
    crc.domain_metadata = extract_domain_metadata_from_batch(batch)?;

    Ok(crc)
}

/// Extract domain metadata from the already-loaded EngineData batch by downcasting to Arrow.
///
/// TODO: The proper fix is to extend the `EngineList` trait (in `engine_data.rs`) to support
/// array-of-struct access. Currently `EngineList::get()` only returns `String` (and the Arrow
/// impl panics on non-string list elements). To fix this properly:
///   1. Add a method like `fn get_getters(&self, row_index: usize) -> Vec<Vec<&dyn GetData>>`
///      to `EngineList` that returns per-element getters for struct fields.
///   2. Implement it in `arrow_get_data.rs` for `GenericListArray` by accessing the inner
///      `StructArray` and returning column arrays as getters for each list element.
///   3. Update `extract_leaf_column` in `arrow_data.rs` to handle `Array<Struct<...>>` types
///      (currently it only accepts `Array<String>` via the `value_type() == Utf8` check).
///   4. Then the `CrcVisitor` can request `domainMetadata` leaf columns and use the visitor
///      pattern to extract domain metadata, just like it does for metadata/protocol.
///
/// For now, we downcast to Arrow and extract directly since the data is already in memory.
fn extract_domain_metadata_from_batch(
    batch: Box<dyn crate::EngineData>,
) -> DeltaResult<Option<Vec<DomainMetadata>>> {
    use crate::arrow::array::cast::AsArray;
    use crate::arrow::array::Array;

    let arrow_data = ArrowEngineData::try_from_engine_data(batch)?;
    let rb = arrow_data.record_batch();

    // Find the domainMetadata column
    let col_idx = match rb.schema().index_of("domainMetadata") {
        Ok(idx) => idx,
        Err(_) => return Ok(None), // Column not present in schema
    };

    let col = rb.column(col_idx);
    if col.is_null(0) {
        return Ok(None);
    }

    // domainMetadata is Array<Struct<domain: String, configuration: String, removed: bool>>
    let list_array = col
        .as_list_opt::<i32>()
        .ok_or_else(|| Error::generic("domainMetadata column is not a list array"))?;

    let struct_array = list_array
        .value(0)
        .as_struct_opt()
        .ok_or_else(|| Error::generic("domainMetadata list elements are not structs"))?
        .clone();

    let domain_col = struct_array
        .column_by_name("domain")
        .ok_or_else(|| Error::generic("domainMetadata struct missing 'domain' field"))?
        .as_string_opt::<i32>()
        .ok_or_else(|| Error::generic("domainMetadata.domain is not a string"))?;

    let config_col = struct_array
        .column_by_name("configuration")
        .ok_or_else(|| Error::generic("domainMetadata struct missing 'configuration' field"))?
        .as_string_opt::<i32>()
        .ok_or_else(|| Error::generic("domainMetadata.configuration is not a string"))?;

    let removed_col = struct_array
        .column_by_name("removed")
        .ok_or_else(|| Error::generic("domainMetadata struct missing 'removed' field"))?
        .as_boolean_opt()
        .ok_or_else(|| Error::generic("domainMetadata.removed is not a boolean"))?;

    let mut result = Vec::with_capacity(struct_array.len());
    for i in 0..struct_array.len() {
        let domain = domain_col.value(i).to_string();
        let configuration = config_col.value(i).to_string();
        let removed = removed_col.value(i);
        let dm = if removed {
            DomainMetadata::remove(domain, configuration)
        } else {
            DomainMetadata::new(domain, configuration)
        };
        result.push(dm);
    }

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::actions::{Format, Metadata, Protocol};
    use crate::engine::sync::SyncEngine;
    use crate::path::ParsedLogPath;
    use crate::table_features::TableFeature;
    use test_utils::assert_result_error_with_message;

    fn test_table_root(dir: &str) -> url::Url {
        let path = std::fs::canonicalize(PathBuf::from(dir)).unwrap();
        url::Url::from_directory_path(path).unwrap()
    }

    #[test]
    fn test_read_crc_file() {
        let engine = SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-full/");
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);

        // Read and parse the CRC file
        let crc = try_read_crc_file(&engine, &crc_path).unwrap();

        // Verify basic fields
        assert_eq!(crc.table_size_bytes, 5259);
        assert_eq!(crc.num_files, 10);
        assert_eq!(crc.num_metadata, 1);
        assert_eq!(crc.num_protocol, 1);
        assert_eq!(crc.in_commit_timestamp_opt, Some(1694758257000));

        // Verify protocol
        let expected_protocol = Protocol::new_unchecked(
            3,
            7,
            Some(vec![TableFeature::DeletionVectors]),
            Some(vec![
                TableFeature::DomainMetadata,
                TableFeature::DeletionVectors,
                TableFeature::RowTracking,
            ]),
        );
        assert_eq!(crc.protocol, expected_protocol);

        // Verify metadata
        let expected_metadata = Metadata::new_unchecked(
            "6ca3020b-3cd9-4048-82e3-1417a0abb98f",
            None,
            None,
            Format::default(),
            r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#,
            vec![],
            Some(1694758256009),
            HashMap::from([
                (
                    "delta.enableDeletionVectors".to_string(),
                    "true".to_string(),
                ),
                (
                    "delta.checkpoint.writeStatsAsStruct".to_string(),
                    "true".to_string(),
                ),
                ("delta.enableRowTracking".to_string(), "true".to_string()),
                (
                    "delta.checkpoint.writeStatsAsJson".to_string(),
                    "false".to_string(),
                ),
                (
                    "delta.rowTracking.materializedRowCommitVersionColumnName".to_string(),
                    "_row-commit-version-col-2f60dcc1-9e36-4424-95e7-799b707e4ddb".to_string(),
                ),
                (
                    "delta.rowTracking.materializedRowIdColumnName".to_string(),
                    "_row-id-col-4cbc7924-f662-4db1-aa59-22c23f59eb5d".to_string(),
                ),
            ]),
        );
        assert_eq!(crc.metadata, expected_metadata);

        // Verify domain metadata was extracted from the already-loaded batch
        let domain_metadata = crc.domain_metadata.unwrap();
        assert_eq!(domain_metadata.len(), 1);
        assert_eq!(domain_metadata[0].domain(), "delta.rowTracking");
        assert!(!domain_metadata[0].is_removed());
        assert!(domain_metadata[0]
            .configuration()
            .contains("rowIdHighWaterMark"));
    }

    #[test]
    fn test_read_malformed_crc_file_fails() {
        let engine = SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-malformed/");
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);

        assert_result_error_with_message(try_read_crc_file(&engine, &crc_path), "Json error");
    }
}
