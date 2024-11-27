//! Provides an API to read the table's change data feed between two versions.
use std::sync::{Arc, LazyLock};

use scan::TableChangesScanBuilder;
use url::Url;

use crate::log_segment::LogSegment;
use crate::path::AsUrl;
use crate::schema::{DataType, Schema, StructField, StructType};
use crate::snapshot::Snapshot;
use crate::table_features::ColumnMappingMode;
use crate::{DeltaResult, Engine, Error, Version};

pub mod scan;

static CDF_FIELDS: LazyLock<[StructField; 3]> = LazyLock::new(|| {
    [
        StructField::new("_change_type", DataType::STRING, false),
        StructField::new("_commit_version", DataType::LONG, false),
        StructField::new("_commit_timestamp", DataType::TIMESTAMP, false),
    ]
});

/// Represents a call to read the Change Data Feed (CDF) between two versions of a table. The schema of
/// `TableChanges` will be the schema of the table at the end version with three additional columns:
/// - `_change_type`: String representing the type of change that for that commit. This may be one
///   of `delete`, `insert`, `update_preimage`, or `update_postimage`.
/// - `_commit_version`: Long representing the commit the change occurred in.
/// - `_commit_timestamp`: Time at which the commit occurred. If In-commit timestamps is enabled,
///   this is retrieved from the [`CommitInfo`] action. Otherwise, the timestamp is the same as the
///   commit file's modification timestamp.
///
/// Three properties must hold for the entire CDF range:
/// - Reading must be supported for every commit in the range. This is determined using [`ensure_read_supported`]
/// - Change Data Feed must be enabled for the entire range with the `delta.enableChangeDataFeed`
///   table property set to 'true'.
/// - The schema for each commit must be compatible with the end schema. This means that all the
///   same fields and their nullability are the same. Schema compatibility will be expanded in the
///   future to allow compatible schemas that are not the exact same.
///   See issue [#523](https://github.com/delta-io/delta-kernel-rs/issues/523)
///
/// [`CommitInfo`]: crate::actions::CommitInfo
/// [`ensure_read_supported`]: crate::actions::Protocol::ensure_read_supported
///  # Examples
///  Get `TableChanges` for versions 0 to 1 (inclusive)
///  ```rust
///  # use delta_kernel::engine::sync::SyncEngine;
///  # use delta_kernel::{Table, Error};
///  # let engine = Box::new(SyncEngine::new());
///  # let path = "./tests/data/table-with-cdf";
///  let table = Table::try_from_uri(path).unwrap();
///  let table_changes = table.table_changes(engine.as_ref(), 0, 1)?;
///  # Ok::<(), Error>(())
///  ````
/// For more details, see the following sections of the protocol:
/// - [Add CDC File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)
/// - [Change Data Files](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-data-files).
#[derive(Debug)]
pub struct TableChanges {
    pub(crate) log_segment: LogSegment,
    table_root: Url,
    end_snapshot: Snapshot,
    start_version: Version,
    schema: Schema,
}

impl TableChanges {
    /// Creates a new [`TableChanges`] instance for the given version range. This function checks
    /// these two properties:
    /// - The change data feed table feature must be enabled in both the start or end versions.
    /// - The schema at the start and end versions are the same.
    ///
    /// Note that this does not check that change data feed is enabled for every commit in the
    /// range. It also does not check that the schema remains the same for the entire range.
    ///
    /// # Parameters
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `start_version`: The start version of the change data feed
    /// - `end_version`: The end version (inclusive) of the change data feed. If this is none, this
    ///   defaults to the newest table version.
    pub fn try_new(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        // Both snapshots ensure that reading is supported at the start and end version using
        // `ensure_read_supported`. Note that we must still verify that reading is
        // supported for every protocol action in the CDF range.
        let start_snapshot =
            Snapshot::try_new(table_root.as_url().clone(), engine, Some(start_version))?;
        let end_snapshot = Snapshot::new_from(&start_snapshot, engine, end_version)?;

        // Verify CDF is enabled at the beginning and end of the interval to fail early. We must
        // still check that CDF is enabled for every metadata action in the CDF range.
        let is_cdf_enabled = |snapshot: &Snapshot| {
            snapshot
                .table_properties()
                .enable_change_data_feed
                .unwrap_or(false)
        };
        if !is_cdf_enabled(&start_snapshot) {
            return Err(Error::change_data_feed_unsupported(start_version));
        } else if !is_cdf_enabled(&end_snapshot) {
            return Err(Error::change_data_feed_unsupported(end_snapshot.version()));
        }

        // Verify that the start and end schemas are compatible. We must still check schema
        // compatibility for each schema update in the CDF range.
        // Note: Schema compatibility check will be changed in the future to be more flexible.
        // See issue [#523](https://github.com/delta-io/delta-kernel-rs/issues/523)
        if start_snapshot.schema() != end_snapshot.schema() {
            return Err(Error::generic(format!(
                "Failed to build TableChanges: Start and end version schemas are different. Found start version schema {:?} and end version schema {:?}", start_snapshot.schema(), end_snapshot.schema(),
            )));
        }

        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.get_file_system_client().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;

        let schema = StructType::new(
            end_snapshot
                .schema()
                .fields()
                .cloned()
                .chain(CDF_FIELDS.clone()),
        );

        Ok(TableChanges {
            table_root,
            end_snapshot,
            log_segment,
            start_version,
            schema,
        })
    }

    /// The start version of the `TableChanges`.
    pub fn start_version(&self) -> Version {
        self.start_version
    }
    /// The end version (inclusive) of the [`TableChanges`]. If no `end_version` was specified in
    /// [`TableChanges::try_new`], this returns the newest version as of the call to `try_new`.
    pub fn end_version(&self) -> Version {
        self.log_segment.end_version
    }
    /// The logical schema of the change data feed. For details on the shape of the schema, see
    /// [`TableChanges`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Path to the root of the table that is being read.
    pub fn table_root(&self) -> &Url {
        &self.table_root
    }
    /// The partition columns that will be read.
    #[allow(unused)]
    pub(crate) fn partition_columns(&self) -> &Vec<String> {
        &self.end_snapshot.metadata().partition_columns
    }
    /// The column mapping mode at the end schema.
    #[allow(unused)]
    pub(crate) fn column_mapping_mode(&self) -> &ColumnMappingMode {
        &self.end_snapshot.column_mapping_mode
    }

    /// Create a [`TableChangesScanBuilder`] for an `Arc<TableChanges>`.
    pub fn scan_builder(self: Arc<Self>) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Consume this `TableChanges` to create a [`TableChangesScanBuilder`]
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField};
    use crate::table_changes::CDF_FIELDS;
    use crate::{Error, Table};
    use itertools::assert_equal;

    #[test]
    fn table_changes_checks_enable_cdf_flag() {
        // Table with CDF enabled, then disabled at version 2 and enabled at version 3
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();

        let valid_ranges = [(0, 1), (0, 0), (1, 1)];
        for (start_version, end_version) in valid_ranges {
            let table_changes = table
                .table_changes(engine.as_ref(), start_version, end_version)
                .unwrap();
            assert_eq!(table_changes.start_version, start_version);
            assert_eq!(table_changes.end_version(), end_version);
        }

        let invalid_ranges = [(0, 2), (1, 2), (2, 2), (2, 3)];
        for (start_version, end_version) in invalid_ranges {
            let res = table.table_changes(engine.as_ref(), start_version, end_version);
            assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))))
        }
    }
    #[test]
    fn schema_evolution_fails() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();
        let expected_msg = "Failed to build TableChanges: Start and end version schemas are different. Found start version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: true, metadata: {} }} } and end version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: false, metadata: {} }} }";

        // A field in the schema goes from being nullable to non-nullable
        let table_changes_res = table.table_changes(engine.as_ref(), 3, 4);
        assert!(matches!(table_changes_res, Err(Error::Generic(msg)) if msg == expected_msg));
    }

    #[test]
    fn table_changes_has_cdf_schema() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let table = Table::try_from_uri(path).unwrap();
        let expected_schema = [
            StructField::new("part", DataType::INTEGER, true),
            StructField::new("id", DataType::INTEGER, true),
        ]
        .into_iter()
        .chain(CDF_FIELDS.clone());

        let table_changes = table.table_changes(engine.as_ref(), 0, 0).unwrap();
        assert_equal(expected_schema, table_changes.schema().fields().cloned());
    }
}
