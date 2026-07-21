//! Reads a table's change data feed between two versions.
//!
//! [`TableChanges::try_new`] reads change data recorded by writers and produces data through a
//! [`TableChangesScanBuilder`]. When the `internal-api` feature is enabled,
//! [`TableChanges::try_new_row_tracking_cdf_listing`] instead lists the files and row-tracking
//! metadata that a connector needs to reconstruct changes while reading the data files itself.
//!
//! # Example
//! ```rust
//! # use std::sync::Arc;
//! # use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
//! # use delta_kernel::expressions::{column_expr, Scalar};
//! # use delta_kernel::{Predicate, Snapshot, SnapshotRef, Error, Engine};
//! # use delta_kernel::table_changes::TableChanges;
//! # let path = "./tests/data/table-with-cdf";
//! let url = delta_kernel::try_parse_uri(path)?;
//! # use test_utils::delta_kernel_default_engine::storage::store_from_url;
//! # let engine = std::sync::Arc::new(DefaultEngineBuilder::new(store_from_url(&url)?).build());
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
//! // Execute the table changes scan to get a fallible iterator of `Box<dyn EngineData>`s
//! let table_change_batches = table_changes_scan.execute(engine.clone())?;
//! # Ok::<(), Error>(())
//! ```
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;
use log_replay::table_changes_action_iter_with_mode;
use scan::TableChangesScanBuilder;
use scan_file::scan_metadata_to_scan_file;
use url::Url;

use crate::log_segment::LogSegment;
use crate::path::AsUrl;
use crate::schema::compare::SchemaComparison as _;
use crate::schema::{DataType, Schema, StructField, StructType};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{Operation, TableFeature};
use crate::table_properties::{
    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, MATERIALIZED_ROW_ID_COLUMN_NAME,
};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Version};

mod log_replay;
mod net_changes;
mod physical_to_logical;
mod resolve_dvs;
pub mod scan;
mod scan_file;
#[cfg(test)]
mod test_utils;

#[internal_api]
pub(crate) use scan_file::{TableChangesFileAction, TableChangesScanFile};

/// Selects the history represented by [`TableChanges::scan_file_listing`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[internal_api]
pub(crate) enum TableChangesListingMode {
    /// Preserves each commit's file actions.
    ///
    /// An `add` and `remove` for the same path in one commit are grouped. Actions for the same
    /// path in different commits remain separate and require reconciliation by stable row ID.
    AllChanges,
    /// Reduces each path to its net effect between the start and end of the range.
    ///
    /// The remove side represents the earliest pre-image and the add side represents the latest
    /// post-image. Intermediate actions are omitted, but each surviving action still requires
    /// row-level reconciliation. Computing net changes buffers the full range.
    NetChanges,
}

/// Identifies the table feature that must remain enabled across the change-feed range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdfMode {
    /// Uses change data recorded by writers.
    ///
    /// This mode requires `delta.enableChangeDataFeed`. It reads `_change_data` files when a
    /// commit contains them and otherwise derives changes from `add` and `remove` actions. The
    /// data-reading [`scan`] and `execute` paths use these semantics.
    ChangeDataFeed,
    /// Uses row lineage to reconstruct changes while reading data files.
    ///
    /// This mode requires `delta.enableRowTracking`. It ignores `_change_data` files and returns
    /// metadata from `add` and `remove` actions for [`TableChanges::scan_file_listing`].
    RowTracking,
}

impl CdfMode {
    /// The table feature that must be enabled across the entire change-feed range for this mode.
    pub(crate) fn required_feature(self) -> TableFeature {
        match self {
            CdfMode::ChangeDataFeed => TableFeature::ChangeDataFeed,
            CdfMode::RowTracking => TableFeature::RowTracking,
        }
    }

    /// Returns the mode-specific error when [`CdfMode::required_feature`] is disabled at `version`.
    pub(crate) fn feature_disabled_error(self, version: Version) -> Error {
        match self {
            CdfMode::ChangeDataFeed => Error::change_data_feed_unsupported(version),
            CdfMode::RowTracking => Error::row_tracking_change_feed_unsupported(version),
        }
    }

    /// Whether data written with `candidate` can be read using the change feed's `read_schema`.
    ///
    /// [`CdfMode::ChangeDataFeed`] requires exact equality. Row-tracking CDF accepts additive
    /// nullable fields and wider nullability, but not datatype changes.
    pub(crate) fn schemas_compatible(
        self,
        candidate: &StructType,
        read_schema: &StructType,
    ) -> bool {
        match self {
            CdfMode::ChangeDataFeed => candidate == read_schema,
            CdfMode::RowTracking => candidate
                .can_read_as_without_type_widening(read_schema)
                .is_ok(),
        }
    }

    /// Whether `AddCDCFile` actions supersede `add` and `remove` actions.
    pub(crate) fn uses_change_data_files(self) -> bool {
        self == CdfMode::ChangeDataFeed
    }

    /// Returns the mode-specific error for incompatible range-boundary schemas.
    pub(crate) fn boundary_schema_error(self, start: &StructType, end: &StructType) -> Error {
        match self {
            CdfMode::ChangeDataFeed => Error::generic(format!(
                "Failed to build TableChanges: Start and end version schemas are different. Found start version schema {start:?} and end version schema {end:?}",
            )),
            CdfMode::RowTracking => Error::change_data_feed_incompatible_schema(end, start),
        }
    }

    /// Maps a reader-support failure on a protocol update to the mode-specific error.
    pub(crate) fn protocol_support_error(self, underlying: Error, version: Version) -> Error {
        match self {
            CdfMode::ChangeDataFeed => Error::change_data_feed_unsupported(version),
            CdfMode::RowTracking => underlying,
        }
    }
}

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

/// Represents a call to read the Change Data Feed (CDF) between two versions of a table. The schema
/// of `TableChanges` will be the schema of the table at the end version with three additional
/// columns:
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
/// - Reading must be supported for every commit in the range: every enabled reader feature must be
///   supported by the kernel. The supported read features will be expanded in the future to cover
///   more delta table features.
/// - [`TableChanges::try_new`] requires Change Data Feed to remain enabled and requires exact
///   schema equality.
/// - [`TableChanges::try_new_row_tracking_cdf_listing`] requires row tracking to remain enabled. It
///   allows additive nullable columns and relaxed nullability, but rejects datatype changes.
///
/// Construction validates the range boundaries. Intermediate metadata and protocol updates are
/// validated when the transaction log is replayed by the scan or listing operation.
///
///  # Examples
///  Get `TableChanges` for versions 0 to 1 (inclusive)
///  ```rust
///  # use test_utils::delta_kernel_default_engine::{storage::store_from_url, DefaultEngineBuilder};
///  # use delta_kernel::{SnapshotRef, Error};
///  # use delta_kernel::table_changes::TableChanges;
///  # let path = "./tests/data/table-with-cdf";
///  let url = delta_kernel::try_parse_uri(path)?;
///  # let engine = DefaultEngineBuilder::new(store_from_url(&url)?).build();
///  let table_changes = TableChanges::try_new(url, &engine, 0, Some(1))?;
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
    start_table_config: TableConfiguration,
    // Both feed types share range loading and log replay. The mode is fixed at construction, and
    // each consumer method verifies that it supports that mode before doing work.
    mode: CdfMode,
}

impl TableChanges {
    /// Creates a new [`TableChanges`] instance for the given version range. This function checks
    /// these properties:
    /// - The change data feed table feature must be enabled in both the start or end versions.
    /// - Every enabled reader feature must be supported by the kernel.
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
        Self::try_new_internal(
            table_root,
            engine,
            start_version,
            end_version,
            CdfMode::ChangeDataFeed,
        )
    }

    /// Creates a listing-only change feed from row-tracking metadata.
    ///
    /// This path requires `delta.enableRowTracking` and ignores `_change_data` files.
    /// [`TableChanges::scan_file_listing`] returns the `add` and `remove` actions whose data must
    /// be reconciled by row ID.
    ///
    /// Construction validates the range boundaries. [`TableChanges::scan_file_listing`] validates
    /// intermediate metadata and protocol updates while replaying the range. Every enabled reader
    /// feature must be supported by Kernel, and each schema must be readable using the end-version
    /// logical schema without datatype widening.
    ///
    /// # Parameters
    ///
    /// - `table_root`: URL of the table root containing `_delta_log`.
    /// - `engine`: Engine used to read the transaction log.
    /// - `start_version`: First version in the change feed.
    /// - `end_version`: The end version (inclusive) of the change data feed. If this is none, this
    ///   defaults to the newest table version.
    ///
    /// # Errors
    ///
    /// Returns an error if the range cannot be loaded or a boundary has unavailable row tracking,
    /// unsupported reader features, or an incompatible schema. Errors from intermediate versions
    /// are returned by [`TableChanges::scan_file_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn try_new_row_tracking_cdf_listing(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
    ) -> DeltaResult<Self> {
        Self::try_new_internal(
            table_root,
            engine,
            start_version,
            end_version,
            CdfMode::RowTracking,
        )
    }

    fn try_new_internal(
        table_root: Url,
        engine: &dyn Engine,
        start_version: Version,
        end_version: Option<Version>,
        mode: CdfMode,
    ) -> DeltaResult<Self> {
        let log_root = table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_table_changes(
            engine.storage_handler().as_ref(),
            log_root,
            start_version,
            end_version,
        )?;

        let start_snapshot = Snapshot::builder_for(table_root.as_url().clone())
            .at_version(start_version)
            .build(engine)?;
        start_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Cdf)?;

        let end_snapshot = match end_version {
            Some(version) => Snapshot::builder_from(start_snapshot.clone())
                .at_version(version)
                .build(engine)?,
            None => Snapshot::builder_from(start_snapshot.clone()).build(engine)?,
        };
        end_snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Cdf)?;

        // Verify the change feed is enabled at the beginning and end of the interval to fail early.
        // The `ensure_operation_supported` calls above already validate that every enabled reader
        // feature is supported by the kernel (e.g. deletion vectors and column mapping). The
        // feature that must additionally be *enabled* depends on the mode: ChangeDataFeed
        // for the cdc-file path, RowTracking for the row-tracking path.
        //
        // Note: We must still check each metadata and protocol action in the CDF range.
        let check_table_config = |snapshot: &Snapshot| -> DeltaResult<()> {
            require!(
                snapshot
                    .table_configuration()
                    .is_feature_enabled(&mode.required_feature()),
                mode.feature_disabled_error(snapshot.version())
            );
            Ok(())
        };
        check_table_config(&start_snapshot)?;
        check_table_config(&end_snapshot)?;

        if mode == CdfMode::RowTracking {
            let properties = end_snapshot.table_properties();
            require!(
                properties.materialized_row_id_column_name.is_some(),
                Error::generic(format!(
                    "Row tracking is enabled at version {}, but metadata property {} is missing",
                    end_snapshot.version(),
                    MATERIALIZED_ROW_ID_COLUMN_NAME
                ))
            );
            require!(
                properties
                    .materialized_row_commit_version_column_name
                    .is_some(),
                Error::generic(format!(
                    "Row tracking is enabled at version {}, but metadata property {} is missing",
                    end_snapshot.version(),
                    MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
                ))
            );
        }

        // Log replay validates only commits that contain metadata, so validate the range boundary
        // separately. Compatibility is evaluated against the logical schemas.
        let start_schema = start_snapshot.schema();
        let end_schema = end_snapshot.schema();
        if !mode.schemas_compatible(start_schema.as_ref(), end_schema.as_ref()) {
            return Err(mode.boundary_schema_error(start_schema.as_ref(), end_schema.as_ref()));
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
            start_table_config: start_snapshot.table_configuration().clone(),
            mode,
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

    /// Returns the physical Parquet column that stores materialized row IDs.
    ///
    /// # Errors
    ///
    /// Returns an error unless this value was created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn materialized_row_id_column_name(&self) -> DeltaResult<&str> {
        self.row_tracking_table_properties()?
            .materialized_row_id_column_name
            .as_deref()
            .ok_or_else(|| {
                Error::internal_error(
                    "A row-tracking TableChanges is missing its materialized row ID column name",
                )
            })
    }

    /// Returns the physical Parquet column that stores materialized row commit versions.
    ///
    /// # Errors
    ///
    /// Returns an error unless this value was created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn materialized_row_commit_version_column_name(&self) -> DeltaResult<&str> {
        self.row_tracking_table_properties()?
            .materialized_row_commit_version_column_name
            .as_deref()
            .ok_or_else(|| {
                Error::internal_error(
                    "A row-tracking TableChanges is missing its materialized row commit version \
                     column name",
                )
            })
    }

    fn row_tracking_table_properties(
        &self,
    ) -> DeltaResult<&crate::table_properties::TableProperties> {
        require!(
            self.mode == CdfMode::RowTracking,
            Error::unsupported(
                "Row-tracking column names are only available for row-tracking change feeds"
            )
        );
        Ok(self.end_snapshot.table_properties())
    }

    /// Create a [`TableChangesScanBuilder`] for an `Arc<TableChanges>`.
    pub fn scan_builder(self: Arc<Self>) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Consume this `TableChanges` to create a [`TableChangesScanBuilder`]
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Lists the files required to reconstruct a row-tracking change feed without reading data
    /// files.
    ///
    /// Each [`TableChangesFileAction`] contains an add side, a remove side, or both. Read each side
    /// using its own deletion vector. For every selected physical row, reconstruct the row ID as
    /// `coalesce(materialized_row_id, base_row_id + physical_row_index)`. Reconstruct the row
    /// commit version as
    /// `coalesce(materialized_row_commit_version, default_row_commit_version)`. The materialized
    /// column names are returned by [`TableChanges::materialized_row_id_column_name`] and
    /// [`TableChanges::materialized_row_commit_version_column_name`]. Assign physical row indexes
    /// before applying deletion vectors.
    ///
    /// Rows present only on the add side are inserts, rows present only on the remove side are
    /// deletes, and matching row IDs with different row commit versions are updates. Matching both
    /// the row ID and row commit version identifies a row carried forward without change.
    /// `AddCDCFile` actions are ignored.
    ///
    /// Both listing modes buffer the selected actions before returning. The iterator therefore
    /// reports preparation errors immediately and uses memory proportional to the range's action
    /// count. This method requires a [`TableChanges`] created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`].
    ///
    /// # Errors
    ///
    /// Returns an error if this value was not created by
    /// [`TableChanges::try_new_row_tracking_cdf_listing`] or if the log actions cannot be replayed
    /// into a valid listing.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn scan_file_listing(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        mode: TableChangesListingMode,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesFileAction>>> {
        if self.mode != CdfMode::RowTracking {
            return Err(Error::unsupported(
                "scan_file_listing is only supported for row-tracking change feeds; construct \
                 the TableChanges with TableChanges::try_new_row_tracking_cdf_listing",
            ));
        }

        let commits = self.log_segment.listed.ascending_commit_files.clone();
        let schema = self.end_snapshot.schema();
        let scan_metadata = table_changes_action_iter_with_mode(
            engine,
            &self.start_table_config,
            commits,
            schema,
            None,
            self.mode,
        )?;
        // Any listing error surfaces here rather than mid-iteration.
        let actions = scan_metadata_to_scan_file(scan_metadata)
            .map(|scan_file| TableChangesFileAction::try_from_scan_file(scan_file?))
            .collect::<DeltaResult<Vec<_>>>()?;
        let actions = match mode {
            TableChangesListingMode::AllChanges => actions,
            TableChangesListingMode::NetChanges => net_changes::collapse_net_changes(actions)?,
        };
        Ok(actions.into_iter().map(Ok))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use itertools::{assert_equal, Itertools};

    use super::*;
    use crate::actions::{Add, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::test_utils::{
        row_tracking_properties, row_tracking_protocol, row_tracking_setup_actions,
        test_deletion_vector, TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
        TEST_MATERIALIZED_ROW_ID_COLUMN_NAME,
    };
    use crate::table_changes::CDF_FIELDS;
    use crate::table_features::TableFeature;
    use crate::table_properties::{
        COLUMN_MAPPING_MODE, MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME,
        MATERIALIZED_ROW_ID_COLUMN_NAME,
    };
    use crate::utils::test_utils::{
        assert_result_error_with_message, test_schema_flat_with_column_mapping, Action,
        LocalMockTable,
    };
    use crate::{Engine, Error};

    fn listing_test_schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]))
    }

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

    #[test]
    fn scan_file_listing_rejects_cdc_file_table_changes() {
        // A change-data-file `TableChanges` (built via `try_new`) must not be consumed through the
        // row-tracking listing API.
        let path = "./tests/data/table-with-cdf";
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let table_changes =
            Arc::new(TableChanges::try_new(url, engine.as_ref(), 0, Some(1)).unwrap());
        assert_result_error_with_message(
            table_changes.materialized_row_id_column_name(),
            "only available for row-tracking change feeds",
        );
        assert_result_error_with_message(
            table_changes.materialized_row_commit_version_column_name(),
            "only available for row-tracking change feeds",
        );
        let res = table_changes.scan_file_listing(engine, TableChangesListingMode::AllChanges);
        assert!(
            matches!(res, Err(Error::Unsupported(_))),
            "scan_file_listing on a cdc-file TableChanges must return an unsupported error"
        );
    }

    #[rstest::rstest]
    #[case::row_id(MATERIALIZED_ROW_ID_COLUMN_NAME)]
    #[case::row_commit_version(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME)]
    #[tokio::test]
    async fn try_new_row_tracking_cdf_listing_rejects_missing_materialized_column_name(
        #[case] missing_property: &str,
    ) {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let mut properties = row_tracking_properties();
        properties.remove(missing_property);
        let metadata =
            Metadata::try_new(None, None, listing_test_schema(), vec![], 0, properties).unwrap();
        mock_table
            .commit([
                Action::Protocol(row_tracking_protocol()),
                Action::Metadata(metadata),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        assert_result_error_with_message(
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 0, Some(0)),
            missing_property,
        );
    }

    #[test]
    fn try_new_row_tracking_cdf_listing_fails_when_row_tracking_disabled() {
        // table-with-cdf enables change data feed but not row tracking, so the row-tracking feed
        // must be rejected at construction.
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let res = TableChanges::try_new_row_tracking_cdf_listing(url, engine.as_ref(), 0, Some(1));
        assert!(
            matches!(&res, Err(Error::RowTrackingChangeFeedUnsupported(_))),
            "expected a row-tracking-disabled error, got {res:?}"
        );
    }

    #[tokio::test]
    async fn try_new_row_tracking_cdf_listing_rejects_incompatible_start_schema() {
        // The start-version schema (in effect for in-range commits before any metadata update)
        // carries a column the end-version schema drops. No in-range commit re-declares the start
        // schema, so this boundary incompatibility is caught only by the start-vs-end check, not
        // the per-commit one.
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let start_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
            StructField::nullable("extra", DataType::INTEGER),
        ]));
        let end_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]));
        let rt_config = row_tracking_properties();

        // v0: start schema + row tracking. v1: a data commit (no metadata) using the start schema.
        // v2: a metadata commit that drops `extra` -- never re-declaring the start schema in range.
        mock_table
            .commit(row_tracking_setup_actions(start_schema))
            .await;
        mock_table
            .commit([Action::Add(Add {
                path: "f1".into(),
                data_change: true,
                size: 10,
                base_row_id: Some(0),
                default_row_commit_version: Some(1),
                ..Default::default()
            })])
            .await;
        mock_table
            .commit([Action::Metadata(
                Metadata::try_new(None, None, end_schema, vec![], 0, rt_config).unwrap(),
            )])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let res =
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 1, Some(2));
        assert!(
            matches!(&res, Err(Error::ChangeDataFeedIncompatibleSchema(_, _))),
            "expected an incompatible start schema to be rejected, got {res:?}"
        );
    }

    #[tokio::test]
    async fn scan_file_listing_surfaces_row_tracking_files_end_to_end() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();

        // version 0: protocol + metadata (row tracking enabled) + an insert.
        let add_v0 = Add {
            path: "path_0".into(),
            data_change: true,
            size: 100,
            base_row_id: Some(0),
            default_row_commit_version: Some(0),
            ..Default::default()
        };
        let add_v1 = Add {
            path: "path_1".into(),
            data_change: true,
            size: 100,
            base_row_id: Some(5),
            default_row_commit_version: Some(1),
            ..Default::default()
        };
        mock_table
            .commit(
                row_tracking_setup_actions(listing_test_schema())
                    .into_iter()
                    .chain([Action::Add(add_v0)]),
            )
            .await;
        mock_table.commit([Action::Add(add_v1)]).await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 0, Some(1))
                .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();

        // Both inserts surface (no cdc files) as single-sided add actions, each carrying its base
        // row id.
        assert_eq!(listing.len(), 2);
        let add_side = |path: &str| {
            listing
                .iter()
                .filter_map(|fa| fa.add.as_ref())
                .find(|a| a.path == path)
                .unwrap_or_else(|| panic!("{path} add side present"))
                .clone()
        };
        assert!(listing.iter().all(|fa| fa.remove.is_none()));
        assert_eq!(add_side("path_0").base_row_id, Some(0));
        assert_eq!(add_side("path_1").base_row_id, Some(5));
    }

    #[tokio::test]
    async fn scan_file_listing_preserves_column_mapped_partition_values() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let schema = test_schema_flat_with_column_mapping();
        let mut properties = row_tracking_properties();
        properties.insert(COLUMN_MAPPING_MODE.to_string(), "name".to_string());
        let metadata =
            Metadata::try_new(None, None, schema, vec!["name".to_string()], 0, properties).unwrap();
        let protocol = Protocol::try_new_modern(
            [TableFeature::ColumnMapping],
            [
                TableFeature::ColumnMapping,
                TableFeature::RowTracking,
                TableFeature::DomainMetadata,
            ],
        )
        .unwrap();
        let partition_values = HashMap::from([("phys_name".to_string(), "north".to_string())]);
        mock_table
            .commit([
                Action::Protocol(protocol),
                Action::Metadata(metadata),
                Action::Add(Add {
                    path: "name=north/file.parquet".into(),
                    partition_values: partition_values.clone(),
                    data_change: true,
                    size: 100,
                    base_row_id: Some(0),
                    default_row_commit_version: Some(0),
                    ..Default::default()
                }),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 0, Some(0))
                .unwrap(),
        );
        assert_eq!(
            table_changes.materialized_row_id_column_name().unwrap(),
            TEST_MATERIALIZED_ROW_ID_COLUMN_NAME
        );
        assert_eq!(
            table_changes
                .materialized_row_commit_version_column_name()
                .unwrap(),
            TEST_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(listing.len(), 1);
        assert_eq!(
            listing[0].add.as_ref().unwrap().partition_values,
            partition_values
        );
    }

    #[tokio::test]
    async fn scan_file_listing_net_changes_collapses_a_cross_commit_update() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let dv1 = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
        let dv2 = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 3);

        // v0: enable row tracking and add `f` with dv1.
        mock_table
            .commit(
                row_tracking_setup_actions(listing_test_schema())
                    .into_iter()
                    .chain([Action::Add(Add {
                        path: "f".into(),
                        data_change: true,
                        size: 100,
                        deletion_vector: Some(dv1.clone()),
                        base_row_id: Some(0),
                        default_row_commit_version: Some(0),
                        ..Default::default()
                    })]),
            )
            .await;
        // v1: remove `f` (dv1) with no paired add in the same commit -> an unpaired remove.
        mock_table
            .commit([Action::Remove(Remove {
                path: "f".into(),
                data_change: true,
                deletion_vector: Some(dv1.clone()),
                ..Default::default()
            })])
            .await;
        // v2: re-add `f` (dv2) with no paired remove in the same commit -> a bare add. Across the
        // range the remove@v1 and this add@v2 are the two boundaries of a net update.
        mock_table
            .commit([Action::Add(Add {
                path: "f".into(),
                data_change: true,
                size: 100,
                deletion_vector: Some(dv2.clone()),
                base_row_id: Some(0),
                default_row_commit_version: Some(2),
                ..Default::default()
            })])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let new_table_changes = || {
            Arc::new(
                TableChanges::try_new_row_tracking_cdf_listing(
                    table_root.clone(),
                    engine.as_ref(),
                    1,
                    Some(2),
                )
                .unwrap(),
            )
        };
        let listing: Vec<TableChangesFileAction> = new_table_changes()
            .scan_file_listing(engine.clone(), TableChangesListingMode::NetChanges)
            .unwrap()
            .try_collect()
            .unwrap();

        // `f`'s net effect over 1..=2 is an update from dv1 to dv2, surfaced as one grouped action:
        // the remove side is the pre-image (dv1) at the first remove (v1) and the add side the
        // post-image (dv2) at the last add (v2). Each side carries only its own DV; the connector
        // reconciles the two by row id.
        assert_eq!(
            listing.len(),
            1,
            "expected one grouped net update: {listing:?}"
        );
        let update = &listing[0];
        let net_remove = update.remove.as_ref().expect("net update remove side");
        assert_eq!(net_remove.commit_version, 1);
        assert_eq!(net_remove.deletion_vector, Some(dv1.clone()));
        let net_add = update.add.as_ref().expect("net update add side");
        assert_eq!(net_add.commit_version, 2);
        assert_eq!(net_add.deletion_vector, Some(dv2.clone()));

        // AllChanges does NOT group a cross-commit update: the same history surfaces as two
        // separate single-sided actions -- a delete at v1 and an insert at v2 -- since grouping is
        // same-commit only. This is the documented mode contrast.
        let all: Vec<TableChangesFileAction> = new_table_changes()
            .scan_file_listing(engine, TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(
            all.len(),
            2,
            "expected two ungrouped single-sided actions: {all:?}"
        );
        let all_remove = all
            .iter()
            .find(|fa| fa.remove.is_some())
            .expect("a standalone remove side");
        assert!(all_remove.add.is_none(), "remove must be single-sided");
        assert_eq!(all_remove.remove.as_ref().unwrap().commit_version, 1);
        let all_add = all
            .iter()
            .find(|fa| fa.add.is_some())
            .expect("a standalone add side");
        assert!(all_add.remove.is_none(), "add must be single-sided");
        assert_eq!(all_add.add.as_ref().unwrap().commit_version, 2);
    }

    #[rstest::rstest]
    #[case::all_changes(TableChangesListingMode::AllChanges)]
    #[case::net_changes(TableChangesListingMode::NetChanges)]
    #[tokio::test]
    async fn scan_file_listing_groups_same_commit_dv_update(#[case] mode: TableChangesListingMode) {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let dv1 = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
        let dv2 = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 3);

        // v0: enable row tracking and add `f` with dv1.
        mock_table
            .commit(
                row_tracking_setup_actions(listing_test_schema())
                    .into_iter()
                    .chain([Action::Add(Add {
                        path: "f".into(),
                        data_change: true,
                        size: 100,
                        deletion_vector: Some(dv1.clone()),
                        base_row_id: Some(0),
                        default_row_commit_version: Some(0),
                        ..Default::default()
                    })]),
            )
            .await;
        // v1: a same-commit DV update of `f` -- remove the dv1 baseline and re-add with dv2 in the
        // one commit. Log replay pairs these into a single grouped action carrying both sides.
        mock_table
            .commit([
                Action::Remove(Remove {
                    path: "f".into(),
                    data_change: true,
                    deletion_vector: Some(dv1.clone()),
                    ..Default::default()
                }),
                Action::Add(Add {
                    path: "f".into(),
                    data_change: true,
                    size: 100,
                    deletion_vector: Some(dv2.clone()),
                    base_row_id: Some(0),
                    default_row_commit_version: Some(1),
                    ..Default::default()
                }),
            ])
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 1, Some(1))
                .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, mode)
            .unwrap()
            .try_collect()
            .unwrap();

        assert_eq!(listing.len(), 1, "expected one grouped action: {listing:?}");
        let update = &listing[0];
        let add = update.add.as_ref().expect("add side");
        let remove = update.remove.as_ref().expect("remove side");
        assert_eq!(add.commit_version, 1);
        assert_eq!(add.deletion_vector, Some(dv2));
        assert_eq!(remove.commit_version, 1);
        assert_eq!(remove.deletion_vector, Some(dv1));
    }

    #[tokio::test]
    async fn scan_builder_rejects_row_tracking_table_changes() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        mock_table
            .commit(row_tracking_setup_actions(listing_test_schema()))
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes =
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 0, Some(0))
                .unwrap();
        let res = table_changes.into_scan_builder().build();
        assert_result_error_with_message(
            res,
            "A row-tracking TableChanges cannot be scanned for data",
        );
    }

    #[rstest::rstest]
    #[case::all_changes(TableChangesListingMode::AllChanges)]
    #[case::net_changes(TableChangesListingMode::NetChanges)]
    #[tokio::test]
    async fn scan_file_listing_empty_range_yields_no_actions(
        #[case] mode: TableChangesListingMode,
    ) {
        // A range whose only commit is the row-tracking setup (protocol + metadata, no data files)
        // has no add/remove actions to list, so both modes must return an empty listing.
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        mock_table
            .commit(row_tracking_setup_actions(listing_test_schema()))
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes = Arc::new(
            TableChanges::try_new_row_tracking_cdf_listing(table_root, engine.as_ref(), 0, Some(0))
                .unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, mode)
            .unwrap()
            .try_collect()
            .unwrap();
        assert!(listing.is_empty(), "expected an empty listing: {listing:?}");
    }
}
