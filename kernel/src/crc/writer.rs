//! CRC file writing functionality.

use url::Url;

use super::{Crc, FileStatsValidity};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error};

/// Serialize and write a CRC file to storage.
///
/// Serializes the [`Crc`] struct to JSON via serde and writes the raw bytes using the storage
/// handler. Returns an error if file stats are not valid (a CRC file on disk must have correct
/// stats). If `overwrite` is false and the file already exists, returns
/// `Err(Error::FileAlreadyExists)`.
pub(crate) fn try_write_crc_file(
    engine: &dyn Engine,
    path: &Url,
    crc: &Crc,
    overwrite: bool,
) -> DeltaResult<()> {
    require!(
        crc.file_stats_validity == FileStatsValidity::Valid,
        Error::internal_error("Cannot write CRC file with invalid file stats")
    );
    let data = serde_json::to_vec(crc)?;
    engine.storage_handler().put(path, data.into(), overwrite)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use std::collections::HashMap;

    use super::*;
    use crate::actions::{DomainMetadata, Protocol, SetTransaction};
    use crate::crc::reader::try_read_crc_file;
    use crate::engine::default::DefaultEngineBuilder;
    use crate::path::{AsUrl, ParsedLogPath};
    use crate::table_features::TableFeature;

    fn test_crc() -> Crc {
        let protocol = Protocol::try_new_modern(
            [TableFeature::ColumnMapping],
            [
                TableFeature::ColumnMapping,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
                TableFeature::InCommitTimestamp,
            ],
        )
        .unwrap();
        // NOTE: Adding more entries here will break test_crc_serialized_json_content because
        // domain_metadata is backed by an unsorted HashMap -- the serialized array order is
        // non-deterministic. If you need multiple entries, either make the test order-independent
        // (e.g. sort both sides by domain name) or switch to a BTreeMap.
        let domain_metadata = HashMap::from([(
            "delta.rowTracking".to_string(),
            DomainMetadata::new(
                "delta.rowTracking".to_string(),
                r#"{"rowIdHighWaterMark":1048576}"#.to_string(),
            ),
        )]);
        let ict = 1234567890;
        let app_id = "testAppId".to_string();
        let set_transactions =
            HashMap::from([(app_id.clone(), SetTransaction::new(app_id, 1, Some(ict)))]);
        Crc {
            table_size_bytes: 1024,
            num_files: 5,
            num_metadata: 1,
            num_protocol: 1,
            protocol,
            txn_id: None,
            in_commit_timestamp_opt: Some(ict),
            set_transactions: Some(set_transactions),
            domain_metadata: Some(domain_metadata),
            ..Default::default()
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
        let write_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let read_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let crc = test_crc();

        try_write_crc_file(&engine, write_path.location.as_url(), &crc, false).unwrap();

        let read_back = try_read_crc_file(&engine, &read_path).unwrap();
        assert_eq!(read_back, crc);
    }

    /// Verify JSON content produced by CRC serialization via serde_json::Value comparison.
    #[test]
    fn test_crc_serialized_json_content() {
        let crc = test_crc();
        let actual: serde_json::Value = serde_json::to_value(&crc).unwrap();

        let expected = serde_json::json!({
            "tableSizeBytes": 1024,
            "numFiles": 5,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": "",
                "name": null,
                "description": null,
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "",
                "partitionColumns": [],
                "createdTime": null,
                "configuration": {}
            },
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": [
                    "columnMapping",
                    "rowTracking",
                    "domainMetadata",
                    "inCommitTimestamp"
                ]
            },
            "inCommitTimestampOpt": 1234567890,
            "domainMetadata": [
                {
                    "domain": "delta.rowTracking",
                    "configuration": "{\"rowIdHighWaterMark\":1048576}",
                    "removed": false
                }
            ],
            "setTransactions": [
                {
                    "appId": "testAppId",
                    "version": 1,
                    "lastUpdated": 1234567890
                }
            ]
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_write_crc_file_already_exists() {
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let table_root = url::Url::parse("memory:///test_table/").unwrap();
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let crc = test_crc();

        try_write_crc_file(&engine, crc_path.location.as_url(), &crc, false).unwrap();

        // Second write with overwrite=false should fail
        let result = try_write_crc_file(&engine, crc_path.location.as_url(), &crc, false);
        assert!(result.is_err());

        // Second write with overwrite=true should succeed
        try_write_crc_file(&engine, crc_path.location.as_url(), &crc, true).unwrap();
    }

    #[test]
    fn test_write_rejects_invalid_file_stats() {
        use crate::crc::FileStatsValidity;

        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let table_root = url::Url::parse("memory:///test_table/").unwrap();
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);

        for invalid_validity in [
            FileStatsValidity::RequiresCheckpointRead,
            FileStatsValidity::Indeterminate,
            FileStatsValidity::Untrackable,
        ] {
            let mut crc = test_crc();
            crc.file_stats_validity = invalid_validity;
            let result = try_write_crc_file(&engine, crc_path.location.as_url(), &crc, false);
            assert!(result.is_err(), "should reject {invalid_validity:?}");
        }
    }
}
