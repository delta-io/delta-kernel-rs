//! CRC file writing functionality.

use url::Url;

use super::{Crc, FileStatsValidity};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error};

/// Serialize and write a CRC file to storage.
///
/// Serializes the [`Crc`] struct to JSON via serde and writes the raw bytes using the storage
/// handler. Returns [`Error::ChecksumWriteUnsupported`] if file stats are not valid (a CRC file
/// on disk must have correct stats). Per the Delta protocol, writers MUST NOT overwrite existing
/// CRC files, so this always writes with `overwrite = false`. If the file already exists, returns
/// `Err(Error::FileAlreadyExists)`.
pub(crate) fn try_write_crc_file(engine: &dyn Engine, path: &Url, crc: &Crc) -> DeltaResult<()> {
    require!(
        crc.file_stats_validity == FileStatsValidity::Valid,
        Error::ChecksumWriteUnsupported(format!(
            "Cannot write CRC file with {:?} file stats",
            crc.file_stats_validity
        ))
    );
    let data = serde_json::to_vec(crc)?;
    engine
        .storage_handler()
        .put(path, data.into(), false /* overwrite */)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::collections::HashMap;

    use super::*;
    use crate::actions::{DomainMetadata, Protocol, SetTransaction};
    use crate::crc::reader::try_read_crc_file;
    use crate::crc::{FileSizeHistogram, FileStatsValidity};
    use crate::engine::default::DefaultEngineBuilder;
    use crate::object_store::memory::InMemory;
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
        // Build a histogram with 5 files totaling 1024 bytes, all in the first bin (< 8KB).
        let mut histogram = FileSizeHistogram::create_default();
        for size in [100, 200, 300, 150, 274] {
            histogram.insert(size).unwrap(); // 5 files, 1024 bytes total
        }
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
            file_size_histogram: Some(histogram),
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

        try_write_crc_file(&engine, write_path.location.as_url(), &crc).unwrap();

        let read_back = try_read_crc_file(&engine, &read_path).unwrap();
        assert_eq!(read_back, crc);
    }

    /// Verify JSON content produced by CRC serialization via serde_json::Value comparison.
    #[test]
    fn test_crc_serialized_json_content() {
        let crc = test_crc();
        let actual: serde_json::Value = serde_json::to_value(&crc).unwrap();

        // Verify non-histogram fields match exactly.
        let actual_obj = actual.as_object().unwrap();
        let expected_non_hist = serde_json::json!({
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
        for (key, expected_val) in expected_non_hist.as_object().unwrap() {
            assert_eq!(
                actual_obj.get(key).unwrap(),
                expected_val,
                "Mismatch for key: {key}"
            );
        }

        // Verify the histogram is present with correct camelCase keys and values.
        let hist = actual_obj.get("fileSizeHistogram").unwrap();
        let boundaries = hist.get("sortedBinBoundaries").unwrap().as_array().unwrap();
        let counts = hist.get("fileCounts").unwrap().as_array().unwrap();
        let bytes = hist.get("totalBytes").unwrap().as_array().unwrap();
        assert_eq!(boundaries.len(), 95);
        assert_eq!(counts.len(), 95);
        assert_eq!(bytes.len(), 95);
        // All 5 files are in bin 0 (< 8KB)
        assert_eq!(counts[0].as_i64().unwrap(), 5);
        assert_eq!(bytes[0].as_i64().unwrap(), 1024); // 100+200+300+150+274
    }

    #[test]
    fn test_write_crc_file_already_exists() {
        let store = Arc::new(InMemory::new());
        let engine = DefaultEngineBuilder::new(store).build();
        let table_root = url::Url::parse("memory:///test_table/").unwrap();
        let crc_path = ParsedLogPath::create_parsed_crc(&table_root, 0);
        let crc = test_crc();

        try_write_crc_file(&engine, crc_path.location.as_url(), &crc).unwrap();

        // Second write should fail (never overwrites)
        let result = try_write_crc_file(&engine, crc_path.location.as_url(), &crc);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_rejects_invalid_file_stats_with_checksum_write_unsupported() {
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
            let result = try_write_crc_file(&engine, crc_path.location.as_url(), &crc);
            assert!(
                matches!(result, Err(Error::ChecksumWriteUnsupported(_))),
                "should reject {invalid_validity:?} with ChecksumWriteUnsupported"
            );
        }
    }
}
