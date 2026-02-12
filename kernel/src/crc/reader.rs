//! CRC file reading functionality.

use std::sync::Arc;

use super::{Crc, CrcVisitor};
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

    // Use visitor to extract CRC fields
    let mut visitor = CrcVisitor::default();
    visitor.visit_rows_of(batch.as_ref())?;
    visitor.into_crc()
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
    }

    #[test]
    fn test_read_malformed_crc_file_fails() {
        let engine = SyncEngine::new();
        let table_root = test_table_root("./tests/data/crc-malformed/");
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);

        assert_result_error_with_message(try_read_crc_file(&engine, &crc_path), "Json error");
    }
}
