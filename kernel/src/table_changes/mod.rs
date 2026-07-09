//! Provides an API to read the table's change data feed between two versions.
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

use log_replay::table_changes_action_iter_with_mode;
use scan::TableChangesScanBuilder;
use scan_file::scan_metadata_to_scan_file;
use url::Url;

use crate::log_segment::LogSegment;
use crate::path::AsUrl;
use crate::schema::compare::SchemaComparison;
use crate::schema::{DataType, Schema, StructField, StructType};
use crate::snapshot::{Snapshot, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{Operation, TableFeature};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, Version};

mod log_replay;
mod net_changes;
mod physical_to_logical;
mod resolve_dvs;
pub mod scan;
mod scan_file;

pub use scan_file::{TableChangesFileAction, TableChangesScanFile};

/// Selects how [`TableChanges::scan_file_listing`] presents the row-tracking change-feed files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TableChangesListingMode {
    /// One [`TableChangesFileAction`] per commit occurrence, with no cross-commit collapsing: a
    /// same-commit `add`+`remove` of one path groups into `{add, remove}` (an in-commit update),
    /// and an unpaired `add` or `remove` is single-sided. Like `NetChanges`, this buffers the full
    /// range before returning (see the method-level note on [`TableChanges::scan_file_listing`]).
    ///
    /// The `{add, remove}` grouping here is a *same-commit-only* convenience: a cross-commit
    /// update (a `remove` in one commit and the re-`add` in a later one) is **not** grouped --
    /// it surfaces as two separate single-sided actions. So `{add, remove}` present does not
    /// enumerate every update in this mode; the connector should reconcile pre-image and
    /// post-image rows by row id across the whole listing regardless of grouping (see
    /// [`TableChangesFileAction`]). To get each path's update grouped into one action, use
    /// [`NetChanges`](Self::NetChanges).
    AllChanges,
    /// Each file path reduced to its net effect across the whole range, as one
    /// [`TableChangesFileAction`]: a net insert (`{add: Some}`), a net delete (`{remove: Some}`),
    /// or -- when a path is removed then re-added with a different deletion vector -- a net update
    /// (`{add: Some, remove: Some}`) whose remove side is the pre-image (earliest remove, with its
    /// DV) and add side the post-image (latest add, with its DV); the connector reconciles them by
    /// row id. The result reflects the net effect, not the intermediate actions: a path added,
    /// removed, then re-added within the range still collapses to a single net-insert. Paths that
    /// net to no change are dropped; dropping is conservative (only structurally identical
    /// boundary deletion vectors are dropped), so a surviving path is not a guarantee its rows
    /// changed. Because the net effect is a property of the whole range, this mode buffers
    /// every per-commit action before emitting -- its memory is proportional to the range's
    /// action count and it is not streaming.
    NetChanges,
}

/// Selects which semantics a [`TableChanges`] uses to derive the change data feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdfMode {
    /// Write time CDF: requires `delta.enableChangeDataFeed`, reads `_change_data`
    /// (cdc) files when present, and falls back to add/remove + deletion-vector diffing. This is
    /// the semantics used by the data-reading [`scan`] / `execute` path.
    WriteTime,
    /// Read Time CDF: requires `delta.enableRowTracking` (not `enableChangeDataFeed`),
    /// ignores `_change_data` (cdc) files, and surfaces the row-tracking fields needed to
    /// reconstruct change events from add/remove actions. Used by the listing-only
    /// [`TableChanges::scan_file_listing`] path.
    ReadTime,
}

impl CdfMode {
    /// The table feature that must be enabled across the entire change-feed range for this mode.
    pub(crate) fn required_feature(self) -> TableFeature {
        match self {
            CdfMode::WriteTime => TableFeature::ChangeDataFeed,
            CdfMode::ReadTime => TableFeature::RowTracking,
        }
    }

    /// The error to return when [`CdfMode::required_feature`] is not enabled at `version`. Each
    /// mode has its own typed error variant ([`Error::ChangeDataFeedUnsupported`] /
    /// [`Error::RowTrackingChangeFeedUnsupported`]) so callers can match on the cause.
    pub(crate) fn feature_disabled_error(self, version: Version) -> Error {
        match self {
            CdfMode::WriteTime => Error::change_data_feed_unsupported(version),
            CdfMode::ReadTime => Error::row_tracking_change_feed_unsupported(version),
        }
    }

    /// Whether a file written with `candidate` schema can be read against the change feed's
    /// `read_schema`. The write time CDF currently requires exact schema equality. The read time
    /// path allows additive evolution (`candidate` must be readable as `read_schema`).
    pub(crate) fn schemas_compatible(
        self,
        candidate: &StructType,
        read_schema: &StructType,
    ) -> bool {
        match self {
            CdfMode::WriteTime => candidate == read_schema,
            CdfMode::ReadTime => candidate.can_read_as(read_schema).is_ok(),
        }
    }

    /// Maps a start-vs-end boundary schema mismatch to the per-mode error. The write time path
    /// reports a generic message; the read time path returns
    /// [`Error::change_data_feed_incompatible_schema`] with the arguments as `(end, start)` (the
    /// end schema is the read schema that the start schema must be readable as).
    pub(crate) fn boundary_schema_error(self, start: &StructType, end: &StructType) -> Error {
        match self {
            CdfMode::WriteTime => Error::generic(format!(
                "Failed to build TableChanges: Start and end version schemas are different. Found start version schema {start:?} and end version schema {end:?}",
            )),
            CdfMode::ReadTime => Error::change_data_feed_incompatible_schema(end, start),
        }
    }

    /// Maps a reader-support failure on a mid-range protocol update to the appropriate error. The
    /// change-data-file path reports [`Error::ChangeDataFeedUnsupported`] for backward
    /// compatibility; the row-tracking path propagates the `underlying` error so it names the
    /// actual unsupported feature.
    pub(crate) fn protocol_support_error(self, underlying: Error, version: Version) -> Error {
        match self {
            CdfMode::WriteTime => Error::change_data_feed_unsupported(version),
            CdfMode::ReadTime => underlying,
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
/// - Change Data Feed must be enabled for the entire range with the `delta.enableChangeDataFeed`
///   table property set to `true`.
/// - The schema for each commit must be compatible with the end schema. This means that all the
///   same fields and their nullability are the same. Schema compatibility will be expanded in the
///   future to allow compatible schemas that are not the exact same.
///   See issue [#523](https://github.com/delta-io/delta-kernel-rs/issues/523)
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
            CdfMode::WriteTime,
        )
    }

    /// Creates a new [`TableChanges`] that derives the change data feed from *row tracking* rather
    /// than change-data-file (`delta.enableChangeDataFeed`) semantics.
    ///
    /// Unlike [`TableChanges::try_new`], this:
    /// - requires `delta.enableRowTracking` (not `delta.enableChangeDataFeed`) to be enabled for
    ///   the entire range, and
    /// - ignores any `_change_data` (cdc) files, reconstructing change events from `add`/`remove`
    ///   actions and their row-tracking fields instead.
    ///
    /// This is intended for the listing-only [`TableChanges::scan_file_listing`] API, where the
    /// connector performs the data scan and the row-id matching that pairs an update's pre-image
    /// and post-image. The same reader support checks as [`TableChanges::try_new`] apply: every
    /// enabled reader feature must be supported by the kernel.
    ///
    /// Note: a table may have both `delta.enableChangeDataFeed` and `delta.enableRowTracking`
    /// enabled. The two paths derive the feed differently (cdc files vs. add/remove + row tracking)
    /// and may classify changes differently, so the caller chooses which semantics to use; this
    /// constructor always uses the row-tracking semantics.
    ///
    /// # Parameters
    /// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
    /// - `engine`: Implementation of [`Engine`] apis.
    /// - `start_version`: The start version of the change data feed
    /// - `end_version`: The end version (inclusive) of the change data feed. If this is none, this
    ///   defaults to the newest table version.
    pub fn try_new_row_tracking(
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
            CdfMode::ReadTime,
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

        // Validate that the start-version schema is compatible with the end-version (read) schema.
        // The per-commit check during log replay only fires for commits that carry a metadata
        // action, so a range whose schema was last set before `start_version` would otherwise go
        // unchecked at its boundary. The change-data-file path requires strict equality; the
        // row-tracking path allows additive evolution (the start schema must be readable as the end
        // schema), consistent with its per-commit check. See issue
        // [#523](https://github.com/delta-io/delta-kernel-rs/issues/523) for relaxing the
        // change-data-file equality check.
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

    /// Create a [`TableChangesScanBuilder`] for an `Arc<TableChanges>`.
    pub fn scan_builder(self: Arc<Self>) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Consume this `TableChanges` to create a [`TableChangesScanBuilder`]
    pub fn into_scan_builder(self) -> TableChangesScanBuilder {
        TableChangesScanBuilder::new(self)
    }

    /// Returns a *listing-only* iterator of the files that must be read to produce a row-tracking
    /// Change Data Feed over this `TableChanges`' version range, **without reading any data**.
    ///
    /// Each [`TableChangesFileAction`] groups a file's change into its `add` and `remove` sides;
    /// which sides are present encodes the change (insert / delete / update). It carries the
    /// per-file deletion-vector and row-tracking metadata the connector needs to (a) perform the
    /// data scan itself and (b) reconstruct change events by reading each side under its own DV and
    /// reconciling an update's pre-image and post-image rows by row id. `cdc` (`_change_data`)
    /// files are never referenced.
    ///
    /// `mode` selects how the files are presented ([`TableChangesListingMode`]):
    /// - [`TableChangesListingMode::AllChanges`] yields one action per commit occurrence (a
    ///   same-commit `add`+`remove` of one path groups into `{add, remove}`; an unpaired `add` or
    ///   `remove` is single-sided).
    /// - [`TableChangesListingMode::NetChanges`] collapses each path to its net effect across the
    ///   range.
    ///
    /// Both modes buffer the full range before returning (NetChanges must, to compute the net
    /// effect); the returned iterator walks that materialized listing. Its memory is proportional
    /// to the range's action count.
    ///
    /// This requires the `TableChanges` to have been constructed with
    /// [`TableChanges::try_new_row_tracking`]; calling it on a change-data-file `TableChanges`
    /// returns an error.
    ///
    /// Note: like the data-reading [`scan`] path, this currently reads each commit file in the
    /// range twice (a prepare pass and a scan pass). Collapsing the row-tracking listing path to a
    /// single read per commit is a future optimization.
    ///
    /// [`scan`]: crate::table_changes::scan
    pub fn scan_file_listing(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        mode: TableChangesListingMode,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<TableChangesFileAction>>> {
        if self.mode != CdfMode::ReadTime {
            return Err(Error::unsupported(
                "scan_file_listing is only supported for row-tracking change feeds; construct \
                 the TableChanges with TableChanges::try_new_row_tracking",
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
    use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
    use crate::actions::{Add, Metadata, Protocol, Remove};
    use crate::engine::sync::SyncEngine;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_changes::CDF_FIELDS;
    use crate::table_features::TableFeature;
    use crate::table_properties::ENABLE_ROW_TRACKING;
    use crate::utils::test_utils::{Action, LocalMockTable};
    use crate::{Engine, Error};

    // === Row-tracking listing e2e helpers ===

    /// The `(id, value)` schema shared by the row-tracking listing end-to-end tests.
    fn listing_test_schema() -> Arc<StructType> {
        Arc::new(StructType::new_unchecked([
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ]))
    }

    /// The v0 setup actions (protocol + row-tracking-enabled metadata) for `schema`, shared by the
    /// row-tracking listing end-to-end tests. Callers chain their v0 data actions onto these.
    fn row_tracking_setup_actions(schema: Arc<StructType>) -> [Action; 2] {
        let metadata = Metadata::try_new(
            None,
            None,
            schema,
            vec![],
            0,
            HashMap::from([(ENABLE_ROW_TRACKING.to_string(), "true".to_string())]),
        )
        .unwrap();
        let protocol = Protocol::try_new_modern(
            TableFeature::EMPTY_LIST,
            [TableFeature::RowTracking, TableFeature::DomainMetadata],
        )
        .unwrap();
        [Action::Protocol(protocol), Action::Metadata(metadata)]
    }

    /// Two distinct deletion-vector descriptors used as the before/after DVs of an update in the
    /// row-tracking listing end-to-end tests.
    fn listing_test_dvs() -> (DeletionVectorDescriptor, DeletionVectorDescriptor) {
        let dv1 = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        };
        let dv2 = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: "U5OWRz5k%CFT.Td}yCPW".to_string(),
            offset: Some(1),
            size_in_bytes: 38,
            cardinality: 3,
        };
        (dv1, dv2)
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
        let res = table_changes.scan_file_listing(engine, TableChangesListingMode::AllChanges);
        assert!(
            matches!(res, Err(Error::Unsupported(_))),
            "scan_file_listing on a cdc-file TableChanges must return an unsupported error"
        );
    }

    #[test]
    fn try_new_row_tracking_fails_when_row_tracking_disabled() {
        // table-with-cdf enables change data feed but not row tracking, so the row-tracking feed
        // must be rejected at construction.
        let path = "./tests/data/table-with-cdf";
        let engine = Box::new(SyncEngine::new());
        let url = delta_kernel::try_parse_uri(path).unwrap();
        let res = TableChanges::try_new_row_tracking(url, engine.as_ref(), 0, Some(1));
        assert!(
            matches!(&res, Err(Error::RowTrackingChangeFeedUnsupported(_))),
            "expected a row-tracking-disabled error, got {res:?}"
        );
    }

    #[tokio::test]
    async fn try_new_row_tracking_rejects_incompatible_start_schema() {
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
        let rt_config = HashMap::from([(ENABLE_ROW_TRACKING.to_string(), "true".to_string())]);

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
        let res = TableChanges::try_new_row_tracking(table_root, engine.as_ref(), 1, Some(2));
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
            TableChanges::try_new_row_tracking(table_root, engine.as_ref(), 0, Some(1)).unwrap(),
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
    async fn scan_file_listing_net_changes_collapses_a_cross_commit_update() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let (dv1, dv2) = listing_test_dvs();

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
                TableChanges::try_new_row_tracking(table_root.clone(), engine.as_ref(), 1, Some(2))
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

    /// A same-commit deletion-vector update (an `add` and a `remove` of the same path in one
    /// commit) flows through the real log-replay pipeline as a paired action. Both modes surface it
    /// identically: one grouped `{add, remove}` action at that commit, the add side carrying the
    /// post-image DV and the remove side the pre-image DV. (The two modes only diverge for a
    /// *cross-commit* update, covered by
    /// `scan_file_listing_net_changes_collapses_a_cross_commit_update`.)
    #[tokio::test]
    async fn scan_file_listing_same_commit_dv_update_grouped_in_both_modes() {
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        let (dv1, dv2) = listing_test_dvs();

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
        let new_table_changes = || {
            Arc::new(
                TableChanges::try_new_row_tracking(table_root.clone(), engine.as_ref(), 1, Some(1))
                    .unwrap(),
            )
        };

        // Both modes group the same-commit DV update into one `{add, remove}` action at v1: the add
        // side carries the post-image (dv2), the remove side the pre-image (dv1).
        let assert_grouped = |listing: Vec<TableChangesFileAction>| {
            assert_eq!(listing.len(), 1, "expected one grouped action: {listing:?}");
            let update = &listing[0];
            let add = update.add.as_ref().expect("add side");
            let remove = update.remove.as_ref().expect("remove side");
            assert_eq!(add.commit_version, 1);
            assert_eq!(add.deletion_vector, Some(dv2.clone()));
            assert_eq!(remove.commit_version, 1);
            assert_eq!(remove.deletion_vector, Some(dv1.clone()));
        };

        let all: Vec<TableChangesFileAction> = new_table_changes()
            .scan_file_listing(engine.clone(), TableChangesListingMode::AllChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_grouped(all);

        let net: Vec<TableChangesFileAction> = new_table_changes()
            .scan_file_listing(engine, TableChangesListingMode::NetChanges)
            .unwrap()
            .try_collect()
            .unwrap();
        assert_grouped(net);
    }

    #[tokio::test]
    async fn scan_builder_rejects_row_tracking_table_changes() {
        // Symmetric to `scan_file_listing_rejects_cdc_file_table_changes`: a row-tracking
        // (`ReadTime`) `TableChanges` carries different feed semantics than the data-reading scan
        // path, so building a scan from it must be rejected rather than silently mis-scanned.
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();
        mock_table
            .commit(row_tracking_setup_actions(listing_test_schema()))
            .await;

        let table_root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_changes =
            TableChanges::try_new_row_tracking(table_root, engine.as_ref(), 0, Some(0)).unwrap();
        let res = table_changes.into_scan_builder().build();
        assert!(
            matches!(res, Err(Error::Unsupported(_))),
            "building a data scan from a row-tracking TableChanges must be unsupported, got {res:?}"
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
            TableChanges::try_new_row_tracking(table_root, engine.as_ref(), 0, Some(0)).unwrap(),
        );
        let listing: Vec<TableChangesFileAction> = table_changes
            .scan_file_listing(engine, mode)
            .unwrap()
            .try_collect()
            .unwrap();
        assert!(listing.is_empty(), "expected an empty listing: {listing:?}");
    }
}
