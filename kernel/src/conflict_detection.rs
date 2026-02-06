//! Conflict detection utilities for transaction retry scenarios.
//!
//! When a connector's transaction conflicts (e.g., read at v10, but other writers committed v11,
//! v12, v13), it may want to retry. This module provides utilities to check if previously computed
//! data (like add_files_meta) can be safely reused.

use std::sync::LazyLock;

use url::Url;

use crate::actions::{get_commit_schema, METADATA_NAME, PROTOCOL_NAME};
use crate::engine_data::{GetData, RowVisitor, TypedGetData};
use crate::log_segment::LogSegment;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType};
use crate::{DeltaResult, Engine, Error, Version};

/// Visitor that detects presence of protocol or metadata actions.
#[derive(Default)]
struct DetectProtocolMetadataChangeVisitor {
    found_protocol: bool,
    found_metadata: bool,
}

impl RowVisitor for DetectProtocolMetadataChangeVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            // Select required sub-fields: if non-null, the action exists
            let names = vec![
                column_name!("protocol.minReaderVersion"),
                column_name!("metaData.id"),
            ];
            let types = vec![DataType::INTEGER, DataType::STRING];
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            let protocol_val: Option<i32> = getters[0].get_opt(i, "protocol.minReaderVersion")?;
            if protocol_val.is_some() {
                self.found_protocol = true;
            }
            let metadata_val: Option<&str> = getters[1].get_opt(i, "metaData.id")?;
            if metadata_val.is_some() {
                self.found_metadata = true;
            }
            if self.found_protocol && self.found_metadata {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// Checks that no protocol or metadata action was committed in the given version range.
///
/// This is useful for transaction retry scenarios: when a connector's transaction conflicts,
/// it can check if protocol/metadata changed. If not, previously computed add_files_meta
/// can be safely reused.
///
/// # Arguments
/// * `engine` - The engine for reading log files
/// * `table_root` - The root URL of the delta table
/// * `start_version_exclusive` - Check commits after this version
/// * `end_version_inclusive` - Check commits up to and including this version
///
/// # Returns
/// * `Ok(())` - No protocol/metadata changes found in the version range
/// * `Err(ProtocolChanged)` - Protocol action detected in the version range
/// * `Err(MetadataChanged)` - Metadata action detected in the version range
pub fn check_no_protocol_or_metadata_changes(
    engine: &dyn Engine,
    table_root: &Url,
    start_version_exclusive: Version,
    end_version_inclusive: Version,
) -> DeltaResult<()> {
    // Nothing to check if versions are adjacent or reversed
    if start_version_exclusive >= end_version_inclusive {
        return Ok(());
    }

    let log_root = table_root.join("_delta_log/")?;
    let check_start = start_version_exclusive.saturating_add(1);

    let log_segment = LogSegment::for_table_changes(
        engine.storage_handler().as_ref(),
        log_root,
        check_start,
        Some(end_version_inclusive),
    )?;

    let schema = get_commit_schema().project(&[PROTOCOL_NAME, METADATA_NAME])?;
    let actions_iter = log_segment.read_actions(engine, schema, None)?;

    let mut visitor = DetectProtocolMetadataChangeVisitor::default();

    for actions_result in actions_iter {
        let batch = actions_result?;
        visitor.visit_rows_of(batch.actions.as_ref())?;

        // Check for protocol change first
        if visitor.found_protocol {
            return Err(Error::protocol_changed(format!(
                "protocol changed between versions {} and {}",
                start_version_exclusive, end_version_inclusive
            )));
        }

        // Check for metadata change
        if visitor.found_metadata {
            return Err(Error::metadata_changed(format!(
                "metadata changed between versions {} and {}",
                start_version_exclusive, end_version_inclusive
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sync::SyncEngine;
    use std::path::PathBuf;

    fn get_test_engine_and_url(table_name: &str) -> (SyncEngine, Url) {
        let engine = SyncEngine::new();
        let path = std::fs::canonicalize(PathBuf::from(format!("./tests/data/{}/", table_name)))
            .expect("Failed to canonicalize path");
        let url = Url::from_directory_path(path).expect("Failed to create URL");
        (engine, url)
    }

    #[test]
    fn test_adjacent_versions_succeeds() {
        let (engine, url) = get_test_engine_and_url("basic_partitioned");

        // start >= end should succeed immediately without reading any files
        let result = check_no_protocol_or_metadata_changes(&engine, &url, 0, 0);
        assert!(result.is_ok());

        let result = check_no_protocol_or_metadata_changes(&engine, &url, 5, 3);
        assert!(result.is_ok());
    }

    #[test]
    fn test_no_changes_between_add_only_commits() {
        // basic_partitioned: v0 has P+M, v1 has only add actions
        let (engine, url) = get_test_engine_and_url("basic_partitioned");

        // Check between v0 and v1 - should succeed since v1 only has add actions
        let result = check_no_protocol_or_metadata_changes(&engine, &url, 0, 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_detects_protocol_change() {
        // type-widening: v1 has protocol change (adding typeWidening-preview feature)
        let (engine, url) = get_test_engine_and_url("type-widening");

        // v0 -> v1 should detect the protocol change
        let result = check_no_protocol_or_metadata_changes(&engine, &url, 0, 1);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::ProtocolChanged(_)),
            "Expected ProtocolChanged, got {:?}",
            err
        );
    }

    #[test]
    fn test_detects_metadata_change() {
        // type-widening: v2 has metadata change (schema widening) but no protocol change
        let (engine, url) = get_test_engine_and_url("type-widening");

        // v1 -> v2 should detect the metadata change
        let result = check_no_protocol_or_metadata_changes(&engine, &url, 1, 2);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::MetadataChanged(_)),
            "Expected MetadataChanged, got {:?}",
            err
        );
    }
}
