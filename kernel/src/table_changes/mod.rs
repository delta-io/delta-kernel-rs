//! Provides an API to read the table's change data feed between two versions.
//!
//! # Example
//! ```rust
//! # use std::sync::Arc;
//! # use test_utils::DefaultEngineExtension;
//! # use delta_kernel::engine::default::DefaultEngine;
//! # use delta_kernel::expressions::{column_expr, Scalar};
//! # use delta_kernel::{Predicate, Snapshot, SnapshotRef, Error, Engine};
//! # use delta_kernel::table_changes::TableChanges;
//! # let path = "./tests/data/table-with-cdf";
//! # let engine = DefaultEngine::new_local();
//! let url = delta_kernel::try_parse_uri(path)?;
//! // Get the table changes (change data feed) between version 0 and 1
//! let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1))?;
//!
//! // Optionally specify a schema and predicate to apply to the table changes scan
//! let schema = table_changes
//!     .schema()
//!     .project(&["id", "_commit_version"])?;
//! let predicate = Arc::new(Predicate::gt(column_expr!("id"), Scalar::from(10)));
//!
//! // Construct the table changes scan
//! let table_changes_scan = table_changes
//!     .into_scan_builder()
//!     .with_schema(schema)
//!     .with_predicate(predicate.clone())
//!     .build()?;
//!
//! // Execute the table changes scan to get a fallible iterator of `ScanResult`s
//! let table_change_batches = table_changes_scan.execute(engine.clone())?;
//! # Ok::<(), Error>(())
//! ```
use std::sync::{Arc, LazyLock};

use scan::TableChangesScanBuilder;
use url::Url;

use crate::actions::{ensure_supported_features, Protocol};
use crate::log_segment::LogSegment;
use crate::path::AsUrl;
use crate::schema::{DataType, Schema, StructField, StructType};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_features::{ColumnMappingMode, TableFeature};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Version};

mod log_replay;
mod physical_to_logical;
mod resolve_dvs;
pub mod scan;
mod scan_file;

pub(crate) const CHANGE_TYPE_COL_NAME: &str = "_change_type";
pub(crate) const COMMIT_VERSION_COL_NAME: &str = "_commit_version";
pub(crate) const COMMIT_TIMESTAMP_COL_NAME: &str = "_commit_timestamp";
static ADD_CHANGE_TYPE: &str = "insert";
static REMOVE_CHANGE_TYPE: &str = "delete";
static CDF_FIELDS: LazyLock<[StructField; 3]> = LazyLock::new(|| {
    [
        StructField::not_null(CHANGE_TYPE_COL_NAME, DataType::STRING),
        StructField::not_null(COMMIT_VERSION_COL_NAME, DataType::LONG),
        StructField::not_null(COMMIT_TIMESTAMP_COL_NAME, DataType::TIMESTAMP),
    ]
});

/// Represents a call to read the Change Data Feed (CDF) between two versions of a table. The schema of
/// `TableChanges` will be the schema of the table at the end version with three additional columns:
/// - `_change_type`: String representing the type of change that for that commit. This may be one
///   of `delete`, `insert`, `update_preimage`, or `update_postimage`.
/// - `_commit_version`: Long representing the commit the change occurred in.
/// - `_commit_timestamp`: Time at which the commit occurred. The timestamp is retrieved from the
///   file modification time of the log file. No timezone is associated with the timestamp.
///
///   Currently, in-commit timestamps (ICT) is not supported. In the future when ICT is enabled, the
///   timestamp will be retrieved from the `inCommitTimestamp` field of the CommitInfo` action.
///   See issue [#559](https://github.com/delta-io/delta-kernel-rs/issues/559)
///   For details on In-Commit Timestamps, see the [Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps).
///
///
/// Three properties must hold for the entire CDF range:
/// - Reading must be supported for every commit in the range. Currently the only read feature allowed
///   is deletion vectors. This will be expanded in the future to support more delta table features.
///   Because only deletion vectors are supported, reader version 2 will not be allowed. That is
//    because version 2 requires that column mapping is enabled. Reader versions 1 and 3 are allowed.
/// - Change Data Feed must be enabled for the entire range with the `delta.enableChangeDataFeed`
///   table property set to `true`. Performing change data feed on  tables with column mapping is
///   currently disallowed. We check that column mapping is disabled, or the column mapping mode is `None`.
/// - The schema for each commit must be compatible with the end schema. This means that all the
///   same fields and their nullability are the same. Schema compatibility will be expanded in the
///   future to allow compatible schemas that are not the exact same.
///   See issue [#523](https://github.com/delta-io/delta-kernel-rs/issues/523)
///
///  # Examples
///  Get `TableChanges` for versions 0 to 1 (inclusive)
///  ```rust
///  # use delta_kernel::engine::default::DefaultEngine;
///  # use test_utils::DefaultEngineExtension;
///  # use delta_kernel::{SnapshotRef, Error};
///  # use delta_kernel::table_changes::TableChanges;
///  # let engine = DefaultEngine::new_local();
///  # let path = "./tests/data/table-with-cdf";
///  let url = delta_kernel::try_parse_uri(path).unwrap();
///  let table_changes = TableChanges::try_new(url, engine.as_ref(), 0, Some(1))?;
///  # Ok::<(), Error>(())
///  ````
/// For more details, see the following sections of the protocol:
/// - [Add CDC File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-cdc-file)
/// - [Change Data Files](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-data-files).
#[derive(Debug)]
pub struct TableChanges {
    pub(crate) log_segment: LogSegment,
    table_root: Url,
    end_snapshot: SnapshotRef,
    start_version: Version,
    schema: Schema,
}

impl TableChanges {
    /// Creates a new [`TableChanges`] instance for the given version range. This function checks
    /// these properties:
    /// - The change data feed table feature must be enabled in both the start or end versions.
    /// - Other than the deletion vector reader feature, no other reader features are enabled for the table.
    /// - The schemas at the start and end versions are the same.
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
        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;

        // Both snapshots ensure that reading is supported at the start and end version using
        // `ensure_read_supported`. Note that we must still verify that reading is
        // supported for every protocol action in the CDF range.
        let start_snapshot = Snapshot::builder_for(table_root.as_url().clone())
            .at_version(start_version)
            .build(engine)?;
        let end_snapshot = match end_version {
            Some(version) => Snapshot::builder_from(start_snapshot.clone())
                .at_version(version)
                .build(engine)?,
            None => Snapshot::builder_from(start_snapshot.clone()).build(engine)?,
        };

        // we block reading catalog-managed tables with CDF for now. note this is best-effort just
        // checking that start/end snapshots are not catalog-managed.
        //
        // TODO: link issue
        #[cfg(feature = "catalog-managed")]
        require!(
            !start_snapshot
                .table_configuration()
                .protocol()
                .is_catalog_managed()
                && !end_snapshot
                    .table_configuration()
                    .protocol()
                    .is_catalog_managed(),
            Error::unsupported("Change data feed is not supported for catalog-managed tables")
        );

        // Verify CDF is enabled at the beginning and end of the interval using
        // [`check_cdf_table_properties`] to fail early. This also ensures that column mapping is
        // disabled.
        //
        // We also check the [`Protocol`] using [`ensure_cdf_read_supported`] to verify that
        // we support CDF with those features enabled.
        //
        // Note: We must still check each metadata and protocol action in the CDF range.
        let check_table_config = |snapshot: &Snapshot| {
            if snapshot.table_configuration().is_cdf_read_supported() {
                Ok(())
            } else {
                Err(Error::change_data_feed_unsupported(snapshot.version()))
            }
        };
        check_table_config(&start_snapshot)?;
        check_table_config(&end_snapshot)?;

        // Verify that the start and end schemas are compatible. We must still check schema
        // compatibility for each schema update in the CDF range.
        // Note: Schema compatibility check will be changed in the future to be more flexible.
        // See issue [#523](https://github.com/delta-io/delta-kernel-rs/issues/523)
        if start_snapshot.schema() != end_snapshot.schema() {
            return Err(Error::generic(format!(
                "Failed to build TableChanges: Start and end version schemas are different. Found start version schema {:?} and end version schema {:?}", start_snapshot.schema(), end_snapshot.schema(),
            )));
        }

        let schema = StructType::try_new(
            end_snapshot
                .schema()
                .fields()
                .cloned()
                .chain(CDF_FIELDS.clone()),
        )?;

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

    /// Create a [`TableChangesScanBuilder`] for an `Arc<TableChanges>`.
    pub fn scan_builder(self: Arc<Self>) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Consume this `TableChanges` to create a [`TableChangesScanBuilder`]
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }
}

/// Ensures that change data feed is enabled in `table_properties`. See the documentation
/// of [`TableChanges`] for more details.
fn check_cdf_table_properties(table_properties: &TableProperties) -> DeltaResult<()> {
    require!(
        table_properties.enable_change_data_feed.unwrap_or(false),
        Error::unsupported("Change data feed is not enabled")
    );
    require!(
        matches!(
            table_properties.column_mapping_mode,
            None | Some(ColumnMappingMode::None)
        ),
        Error::unsupported("Change data feed not supported when column mapping is enabled")
    );
    Ok(())
}

/// Ensures that Change Data Feed is supported for a table with this [`Protocol`] .
/// See the documentation of [`TableChanges`] for more details.
fn ensure_cdf_read_supported(protocol: &Protocol) -> DeltaResult<()> {
    static CDF_SUPPORTED_READER_FEATURES: LazyLock<Vec<TableFeature>> =
        LazyLock::new(|| vec![TableFeature::DeletionVectors]);
    match &protocol.reader_features() {
        // if min_reader_version = 3 and all reader features are subset of supported => OK
        Some(reader_features) if protocol.min_reader_version() == 3 => {
            ensure_supported_features(reader_features, &CDF_SUPPORTED_READER_FEATURES)
        }
        // if min_reader_version = 1 and there are no reader features => OK
        None if protocol.min_reader_version() == 1 => Ok(()),
        // any other protocol is not supported
        _ => Err(Error::unsupported(
            "Change data feed not supported on this protocol",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField};
    use crate::table_changes::CDF_FIELDS;
    use crate::Error;
    use itertools::assert_equal;

    #[test]
    fn table_changes_checks_enable_cdf_flag() {
        // Table with CDF enabled, then disabled at version 2 and enabled at version 3
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();

        let valid_ranges = [(0, 1), (0, 0), (1, 1)];
        for (start_version, end_version) in valid_ranges {
            let table_changes = TableChanges::try_new(
                url.clone(),
                engine.as_ref(),
                start_version,
                end_version.into(),
            )
            .unwrap();
            assert_eq!(table_changes.start_version, start_version);
            assert_eq!(table_changes.end_version(), end_version);
        }

        let invalid_ranges = [(0, 2), (1, 2), (2, 2), (2, 3)];
        for (start_version, end_version) in invalid_ranges {
            let res = TableChanges::try_new(
                url.clone(),
                engine.as_ref(),
                start_version,
                end_version.into(),
            );
            assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))))
        }
    }
    #[test]
    fn schema_evolution_fails() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let expected_msg = "Failed to build TableChanges: Start and end version schemas are different. Found start version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: true, metadata: {} }}, metadata_columns: {} } and end version schema StructType { type_name: \"struct\", fields: {\"part\": StructField { name: \"part\", data_type: Primitive(Integer), nullable: true, metadata: {} }, \"id\": StructField { name: \"id\", data_type: Primitive(Integer), nullable: false, metadata: {} }}, metadata_columns: {} }";

        // A field in the schema goes from being nullable to non-nullable
        let table_changes_res = TableChanges::try_new(url, engine.as_ref(), 3, Some(4));
        assert!(matches!(table_changes_res, Err(Error::Generic(msg)) if msg == expected_msg));
    }

    #[test]
    fn table_changes_has_cdf_schema() {
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let expected_schema = [
            StructField::nullable("part", DataType::INTEGER),
            StructField::nullable("id", DataType::INTEGER),
        ]
        .into_iter()
        .chain(CDF_FIELDS.clone());

        let table_changes =
            TableChanges::try_new(url.clone(), engine.as_ref(), 0, 0.into()).unwrap();
        assert_equal(expected_schema, table_changes.schema().fields().cloned());
    }
}
