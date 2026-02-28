//! CRC file writing functionality.

use url::Url;

use super::Crc;
use crate::{DeltaResult, Engine};

/// Serialize and write a CRC file to storage.
///
/// Serializes the [`Crc`] struct to JSON via serde and writes the raw bytes using the storage
/// handler. If `overwrite` is false and the file already exists, returns
/// `Err(Error::FileAlreadyExists)`.
pub(crate) fn try_write_crc_file(
    engine: &dyn Engine,
    path: &Url,
    crc: &Crc,
    overwrite: bool,
) -> DeltaResult<()> {
    let data = serde_json::to_vec(crc)?;
    engine.storage_handler().put(path, data.into(), overwrite)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::actions::{DomainMetadata, Metadata, Protocol};
    use crate::crc::reader::try_read_crc_file;
    use crate::engine::default::DefaultEngineBuilder;
    use crate::path::ParsedLogPath;

    fn test_crc() -> Crc {
        let domain_metadata = vec![DomainMetadata::new(
            "test.domain".to_string(),
            r#"{"key":"val"}"#.to_string(),
        )];
        Crc {
            table_size_bytes: 1024,
            num_files: 5,
            num_metadata: 1,
            num_protocol: 1,
            metadata: Metadata::default(),
            protocol: Protocol::default(),
            txn_id: None,
            in_commit_timestamp_opt: Some(1234567890),
            set_transactions: None,
            domain_metadata: Some(domain_metadata),
            file_size_histogram: None,
            all_files: None,
            num_deleted_records_opt: None,
            num_deletion_vectors_opt: None,
            deleted_record_counts_histogram_opt: None,
        }
    }

    #[test]
    fn test_serde_round_trip() {
        let crc = test_crc();
        let json_bytes = serde_json::to_vec(&crc).unwrap();
        let round_tripped: Crc = serde_json::from_slice(&json_bytes).unwrap();

        assert_eq!(round_tripped, crc);
    }

    #[test]
    fn test_write_then_read_crc_file() {
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let table_root = url::Url::parse("memory:///test_table/").unwrap();
        let write_path = ParsedLogPath::new_crc(&table_root, 0).unwrap();
        let read_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let crc = test_crc();

        try_write_crc_file(&engine, &write_path.location, &crc, false).unwrap();

        let read_back = try_read_crc_file(&engine, &read_path).unwrap();
        assert_eq!(read_back, crc);
    }

    #[test]
    fn test_write_crc_file_already_exists() {
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let table_root = url::Url::parse("memory:///test_table/").unwrap();
        let crc_path = ParsedLogPath::new_crc(&table_root, 0).unwrap();
        let crc = test_crc();

        try_write_crc_file(&engine, &crc_path.location, &crc, false).unwrap();

        // Second write with overwrite=false should fail
        let result = try_write_crc_file(&engine, &crc_path.location, &crc, false);
        assert!(result.is_err());

        // Second write with overwrite=true should succeed
        try_write_crc_file(&engine, &crc_path.location, &crc, true).unwrap();
    }
}
