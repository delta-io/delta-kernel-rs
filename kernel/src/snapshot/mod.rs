//! In-memory representation of snapshots of tables (snapshot is a table at given point in time, it
//! has schema etc.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::{debug, info, instrument, warn};
use url::Url;

use crate::action_reconciliation::calculate_transaction_expiration_timestamp;
use crate::actions::set_transaction::{is_set_txn_expired, SetTransactionScanner};
use crate::actions::{DomainMetadata, INTERNAL_DOMAIN_PREFIX};
use crate::checkpoint::{
    CheckpointSpec, CheckpointWriter, V2CheckpointConfig, DEFAULT_FILE_ACTIONS_PER_SIDECAR_HINT,
};
use crate::clustering::{parse_clustering_columns, CLUSTERING_DOMAIN_NAME};
use crate::committer::{Committer, PublishMetadata};
use crate::crc::{
    read_crc_file_or_none, try_write_crc_file, Crc, CrcDelta, DomainMetadataState, FileStats,
    SetTransactionState,
};
use crate::expressions::ColumnName;
use crate::incremental_scan::IncrementalScanBuilder;
use crate::log_segment::{DomainMetadataMap, LogSegment};
use crate::metrics::events::{DOMAIN_METADATA_LOADED_SPAN, SET_TRANSACTION_LOADED_SPAN};
use crate::metrics::MetricId;
use crate::path::ParsedLogPath;
use crate::scan::ScanBuilder;
use crate::schema::SchemaRef;
use crate::table_configuration::{InCommitTimestampEnablement, TableConfiguration};
use crate::table_features::{physical_to_logical_column_name, ColumnMappingMode, TableFeature};
use crate::table_properties::TableProperties;
use crate::transaction::builder::alter_table::AlterTableTransactionBuilder;
use crate::transaction::Transaction;
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, LogCompactionWriter, Version};

mod builder;
mod incremental;
pub use builder::SnapshotBuilder;

/// A shared, thread-safe reference to a [`Snapshot`].
pub type SnapshotRef = Arc<Snapshot>;

/// Result of attempting to write a version checksum (CRC) file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumWriteResult {
    /// A CRC file already exists at this version. Per the Delta protocol, writers MUST NOT
    /// overwrite existing version checksum files.
    AlreadyExists,
    /// The CRC file was successfully written to storage.
    Written,
}

/// Result of attempting to write a checkpoint file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointWriteResult {
    /// A checkpoint already exists at this version.
    AlreadyExists,
    /// The checkpoint was successfully written to storage.
    Written,
}

// TODO expose methods for accessing the files of a table (with file pruning).
/// In-memory representation of a specific snapshot of a Delta table. While a `DeltaTable` exists
/// throughout time, `Snapshot`s represent a view of a table at a specific point in time; they
/// have a defined schema (which may change over time for any given table), specific version, and
/// frozen log segment.
pub struct Snapshot {
    span: tracing::Span,
    log_segment: LogSegment,
    table_configuration: TableConfiguration,
    /// CRC at this snapshot's version, eagerly resolved at construction time. `Some(crc)`
    /// means `crc.version == self.version()` and the CRC can be queried at zero I/O. `None`
    /// means no CRC was loadable (no CRC on disk at this version, or the read failed).
    crc: Option<Arc<Crc>>,
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.log_segment == other.log_segment
            && self.table_configuration == other.table_configuration
    }
}

impl Eq for Snapshot {}

impl Drop for Snapshot {
    fn drop(&mut self) {
        debug!("Dropping snapshot");
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("path", &self.log_segment.log_root.as_str())
            .field("version", &self.version())
            .field("metadata", &self.table_configuration().metadata())
            .field("log_segment", &self.log_segment)
            .finish()
    }
}

impl Snapshot {
    // ============================================================================
    // Construction entry points
    // ============================================================================

    /// Create a new [`SnapshotBuilder`] to build a new [`Snapshot`] for a given table root. If you
    /// instead have an existing [`Snapshot`] you would like to do minimal work to update, consider
    /// using [`Snapshot::builder_from`] instead.
    pub fn builder_for(table_root: impl AsRef<str>) -> SnapshotBuilder {
        SnapshotBuilder::new_for(table_root)
    }

    /// Create a new [`SnapshotBuilder`] to incrementally update an existing [`Snapshot`] to a
    /// more recent version.
    ///
    /// See `Snapshot::try_new_from` for the case-by-case behavior.
    pub fn builder_from(existing_snapshot: SnapshotRef) -> SnapshotBuilder {
        SnapshotBuilder::new_from(existing_snapshot)
    }

    // ============================================================================
    // Internal constructors
    // ============================================================================

    /// Create a new [`Snapshot`] from a [`LogSegment`] and [`TableConfiguration`].
    #[internal_api]
    #[allow(unused)]
    pub(crate) fn new(
        log_segment: LogSegment,
        table_configuration: TableConfiguration,
    ) -> DeltaResult<Self> {
        Self::new_with_crc(log_segment, table_configuration, None)
    }

    /// Internal constructor that accepts an explicit pre-resolved CRC.
    ///
    /// A `Some(crc)` must be at the table configuration's version; otherwise this returns an
    /// internal error.
    pub(crate) fn new_with_crc(
        log_segment: LogSegment,
        table_configuration: TableConfiguration,
        crc: Option<Arc<Crc>>,
    ) -> DeltaResult<Self> {
        if let Some(crc) = crc.as_ref() {
            require!(
                crc.version == table_configuration.version(),
                Error::internal_error(format!(
                    "CRC version {} does not match snapshot version {}",
                    crc.version,
                    table_configuration.version()
                ))
            );
        }
        let span = tracing::info_span!(
            parent: tracing::Span::none(),
            "snap",
            path = %table_configuration.table_root(),
            version = table_configuration.version(),
        );
        info!(parent: &span, "Created snapshot");
        Ok(Self {
            span,
            log_segment,
            table_configuration,
            crc,
        })
    }

    /// Create a new [`Snapshot`] from a freshly-listed [`LogSegment`], eagerly resolving the
    /// CRC at the segment's end version when one is present on disk.
    #[instrument(err, fields(version, operation_id = %operation_id), skip(engine))]
    fn try_new_from_log_segment(
        location: Url,
        log_segment: LogSegment,
        engine: &dyn Engine,
        operation_id: MetricId,
    ) -> DeltaResult<Self> {
        let read_crc = log_segment
            .listed
            .latest_crc_file
            .as_ref()
            .and_then(|crc_file| read_crc_file_or_none(engine, crc_file));

        let (metadata, protocol) =
            log_segment.read_protocol_metadata(engine, read_crc.as_ref(), operation_id)?;

        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, location, log_segment.end_version)?;

        tracing::Span::current().record("version", table_configuration.version());

        let crc = read_crc.filter(|c| c.version == log_segment.end_version);
        Self::new_with_crc(log_segment, table_configuration, crc)
    }

    /// Creates a new [`Snapshot`] representing the table state immediately after a commit.
    ///
    /// Appends the newly committed file to this snapshot's log segment and bumps the version,
    /// producing a post-commit snapshot without a full log replay from storage.
    ///
    /// The `crc_delta` captures the CRC-relevant changes from the committed transaction
    /// (file stats, domain metadata, ICT, etc.). If this snapshot had a CRC at its version,
    /// the delta is applied to produce a precomputed in-memory CRC for the new version,
    /// avoiding re-reading metadata from storage. If no CRC was available, the new snapshot
    /// has no CRC either. CREATE TABLE handles CRC construction separately in
    /// `Transaction::into_committed`.
    pub(crate) fn new_post_commit(
        &self,
        commit: ParsedLogPath,
        crc_delta: CrcDelta,
    ) -> DeltaResult<Self> {
        require!(
            commit.is_commit(),
            Error::internal_error(format!(
                "Cannot create post-commit Snapshot. Log file is not a commit file. \
                Path: {}, Type: {:?}.",
                commit.location.location, commit.file_type
            ))
        );
        let read_version = self.version();
        let new_version = commit.version;
        require!(
            new_version == read_version.wrapping_add(1),
            Error::internal_error(format!(
                "Cannot create post-commit Snapshot. Log file version ({new_version}) does not \
                equal Snapshot version ({read_version}) + 1."
            ))
        );

        let new_table_configuration = TableConfiguration::new_post_commit(
            self.table_configuration(),
            new_version,
            crc_delta.metadata.clone(),
            crc_delta.protocol.clone(),
        )?;

        let new_log_segment = self.log_segment.new_with_commit_appended(commit)?;

        let new_crc = self
            .crc
            .as_deref()
            .map(|base| Arc::new(base.clone().apply(crc_delta, new_version)));

        Snapshot::new_with_crc(new_log_segment, new_table_configuration, new_crc)
    }

    // ============================================================================
    // Field accessors and state queries
    // ============================================================================

    /// Log segment this snapshot uses
    #[internal_api]
    pub(crate) fn log_segment(&self) -> &LogSegment {
        &self.log_segment
    }

    /// Returns the CRC for this snapshot, if one is resolved.
    ///
    /// When `Some(crc)`, `crc.version == self.version()` and queries backed by the CRC hit
    /// cache at zero I/O.
    #[internal_api]
    pub(crate) fn crc(&self) -> Option<&Arc<Crc>> {
        self.crc.as_ref()
    }

    pub fn table_root(&self) -> &Url {
        self.table_configuration.table_root()
    }

    /// Version of this `Snapshot` in the table.
    pub fn version(&self) -> Version {
        self.table_configuration().version()
    }

    /// Table [`Schema`] at this `Snapshot`s version.
    ///
    /// [`Schema`]: crate::schema::Schema
    pub fn schema(&self) -> SchemaRef {
        self.table_configuration.logical_schema()
    }

    /// Estimated owned heap size in bytes for this snapshot. Best-effort estimate
    /// for capacity tracking, not authoritative.
    ///
    /// Counts only the dominant per-snapshot heap contributors, normally > 70% of the snapshot's
    /// owned heap size:
    /// - For every listed log path (commit, compaction, checkpoint, latest CRC, latest commit): the
    ///   filename / extension / Url string heap.
    /// - Vec buffer capacity (`capacity * size_of::<ParsedLogPath>()`) for the three Vec fields on
    ///   `LogSegmentFiles`.
    /// - The log root Url string.
    /// - The raw `schemaString` JSON on table metadata.
    ///
    /// The Arc-shared variables (e.g. logical/physical schemas, `crc`) are not counted,
    /// as they can be shared between multiple snapshots and are not owned by a single snapshot.
    ///
    /// Other variables' contributions to heap size are relatively small, so they are not counted
    /// here.
    ///
    /// Runs in O(n) over listed log files.
    pub fn estimated_owned_heap_size_bytes(&self) -> usize {
        self.log_segment.listed.estimated_heap_size_bytes()
            + self.log_segment.log_root.as_str().len()
            + self
                .table_configuration()
                .metadata()
                .schema_string()
                .capacity()
    }

    /// Get the [`TableProperties`] for this [`Snapshot`].
    pub fn table_properties(&self) -> &TableProperties {
        self.table_configuration().table_properties()
    }

    /// Returns the protocol-derived table properties as a map of key-value pairs.
    ///
    /// This includes:
    /// - `delta.minReaderVersion` and `delta.minWriterVersion`
    /// - `delta.feature.<name> = "supported"` for each reader and writer feature (when using table
    ///   features protocol, i.e. reader version 3 / writer version 7)
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_protocol_derived_properties(&self) -> HashMap<String, String> {
        let protocol = self.table_configuration().protocol();

        let mut properties = HashMap::from([
            (
                "delta.minReaderVersion".into(),
                protocol.min_reader_version().to_string(),
            ),
            (
                "delta.minWriterVersion".into(),
                protocol.min_writer_version().to_string(),
            ),
        ]);

        let features = protocol
            .reader_features()
            .into_iter()
            .flatten()
            .chain(protocol.writer_features().into_iter().flatten());

        for feature in features {
            properties
                .entry(format!("delta.feature.{}", feature.as_ref()))
                .or_insert_with(|| "supported".to_string());
        }

        properties
    }

    /// Get the raw metadata configuration for this table.
    ///
    /// This returns the `Metadata.configuration` map as stored in the Delta log, containing
    /// user-defined properties, delta table properties (e.g., `delta.enableInCommitTimestamps`),
    /// and application-specific properties (e.g., `io.unitycatalog.tableId`).
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn metadata_configuration(&self) -> &HashMap<String, String> {
        self.table_configuration().metadata().configuration()
    }

    /// Get the [`TableConfiguration`] for this [`Snapshot`].
    #[internal_api]
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        &self.table_configuration
    }

    /// Fetch the latest version of the provided `application_id` for this snapshot. Filters the
    /// txn based on the delta.setTransactionRetentionDuration property and lastUpdated.
    ///
    /// Uses the CRC fast path when available, otherwise falls back to log replay.
    ///
    /// Reports metrics: `SetTransactionLoadSuccess` or `SetTransactionLoadFailure`.
    // TODO: add a get_app_id_versions to fetch all at once using SetTransactionScanner::get_all
    #[instrument(
        parent = &self.span,
        name = SET_TRANSACTION_LOADED_SPAN,
        skip_all,
        err,
        fields(report, from_cache, found)
    )]
    pub fn get_app_id_version(
        &self,
        application_id: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<i64>> {
        fn record_metric(from_cache: bool, found: bool) {
            let span = tracing::Span::current();
            span.record("from_cache", from_cache);
            span.record("found", found);
        }

        let expiration_timestamp =
            calculate_transaction_expiration_timestamp(self.table_properties())?;

        // Fast path: serve from CRC if available at this version.
        if let Some(crc) = self.crc.as_deref() {
            match &crc.set_transaction_state {
                SetTransactionState::Complete(map) => {
                    // Complete is authoritative: a miss means the app_id has no transaction.
                    let version = map
                        .get(application_id)
                        .filter(|txn| !is_set_txn_expired(expiration_timestamp, txn.last_updated))
                        .map(|txn| txn.version);
                    record_metric(true, version.is_some());
                    return Ok(version);
                }
                SetTransactionState::Partial(map) => {
                    // Hit is authoritative; miss falls through to log replay below.
                    if let Some(txn) = map.get(application_id) {
                        let version = (!is_set_txn_expired(expiration_timestamp, txn.last_updated))
                            .then_some(txn.version);
                        record_metric(true, version.is_some());
                        return Ok(version);
                    }
                }
            }
        }

        // Fallback: full log replay.
        let txn = SetTransactionScanner::get_one(
            self.log_segment(),
            application_id,
            engine,
            expiration_timestamp,
        )?;
        record_metric(false, txn.is_some());
        Ok(txn.map(|t| t.version))
    }

    /// Fetch the domainMetadata for a specific domain in this snapshot. This returns the latest
    /// configuration for the domain, or None if the domain does not exist.
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    pub fn get_domain_metadata(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<String>> {
        if domain.starts_with(INTERNAL_DOMAIN_PREFIX) {
            return Err(Error::generic(
                "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain",
            ));
        }

        self.get_domain_metadata_internal(domain, engine)
    }

    /// Get the logical clustering columns for this snapshot, if clustering is enabled.
    ///
    /// Returns `Ok(Some(columns))` if the ClusteredTable feature is enabled and clustering
    /// columns are defined, `Ok(None)` if clustering is not enabled, or an error if the
    /// clustering metadata is malformed.
    ///
    /// The columns are returned as logical [`ColumnName`]s. When column mapping is enabled,
    /// this converts the physical names stored in domain metadata back to logical names using
    /// the table schema.
    ///
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    ///
    /// # Errors
    ///
    /// Returns an error if the clustering domain metadata is malformed, or if a physical
    /// column name cannot be resolved to a logical name in the schema.
    ///
    /// [`ColumnName`]: crate::expressions::ColumnName
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_logical_clustering_columns(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        let physical_columns = match self.get_physical_clustering_columns(engine)? {
            Some(cols) => cols,
            None => return Ok(None),
        };
        let column_mapping_mode = self.table_configuration.column_mapping_mode();
        if column_mapping_mode == ColumnMappingMode::None {
            // No column mapping: physical = logical
            return Ok(Some(physical_columns));
        }
        // Convert physical column names to logical names by walking the schema
        let logical_schema = self.table_configuration.logical_schema();
        let logical_columns = physical_columns
            .iter()
            .map(|physical_col| {
                physical_to_logical_column_name(&logical_schema, physical_col, column_mapping_mode)
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Some(logical_columns))
    }

    /// Get the clustering columns for this snapshot, if the table has clustering enabled.
    ///
    /// Returns `Ok(Some(columns))` if the ClusteredTable feature is enabled and clustering
    /// columns are defined, `Ok(None)` if clustering is not enabled, or an error if the
    /// clustering metadata is malformed.
    ///
    /// The columns are returned as physical column names, respecting the column mapping mode.
    /// Note that this method performs log replay (fetches and processes metadata from storage).
    #[internal_api]
    pub(crate) fn get_physical_clustering_columns(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<Vec<ColumnName>>> {
        if !self
            .table_configuration
            .protocol()
            .has_table_feature(&TableFeature::ClusteredTable)
        {
            return Ok(None);
        }
        match self.get_domain_metadata_internal(CLUSTERING_DOMAIN_NAME, engine)? {
            Some(config) => Ok(Some(parse_clustering_columns(&config)?)),
            None => Ok(None),
        }
    }

    /// Load domain metadata: if Complete in the CRC, answer from the cache; else if every
    /// requested domain is in a Partial cache, also answer from the cache; else full log
    /// replay. `domains == None` means load all.
    ///
    /// Reports metrics: `DomainMetadataLoadSuccess` or `DomainMetadataLoadFailure`.
    #[instrument(
        parent = &self.span,
        name = DOMAIN_METADATA_LOADED_SPAN,
        skip_all,
        err,
        fields(report, from_cache, num_domains_returned)
    )]
    #[internal_api]
    pub(crate) fn get_domain_metadatas_internal(
        &self,
        engine: &dyn Engine,
        domains: Option<&HashSet<&str>>,
    ) -> DeltaResult<DomainMetadataMap> {
        fn record_metric(from_cache: bool, num_domains_returned: usize) {
            let span = tracing::Span::current();
            span.record("from_cache", from_cache);
            span.record("num_domains_returned", num_domains_returned as u64);
        }

        // Fast path: serve from CRC if it tracks domain metadata at this version.
        if let Some(crc) = self.crc.as_deref() {
            match &crc.domain_metadata_state {
                DomainMetadataState::Complete(map) => {
                    let hits: DomainMetadataMap = match domains {
                        None => map.clone(),
                        // Look up each filter key. A miss means the domain does not exist
                        // at this version (Complete is authoritative for misses), so skip
                        // it and return whatever hits we found.
                        Some(filter) => filter
                            .iter()
                            .filter_map(|&k| map.get(k).map(|v| (k.to_string(), v.clone())))
                            .collect(),
                    };
                    record_metric(true, hits.len());
                    return Ok(hits);
                }
                DomainMetadataState::Partial(map) => {
                    if let Some(filter) = domains {
                        // Look up each filter key. A miss means we do not know whether the
                        // domain exists, so abandon the cache and fall through to log
                        // replay. `collect::<Option<_>>` short-circuits on the first None.
                        // TODO(#2572): track tombstoned domains in `Partial` so removals
                        //              observed during apply can return authoritative
                        //              `None` instead of falling through.
                        let hits: Option<DomainMetadataMap> = filter
                            .iter()
                            .map(|&k| map.get(k).map(|v| (k.to_string(), v.clone())))
                            .collect();
                        if let Some(hits) = hits {
                            record_metric(true, hits.len());
                            return Ok(hits);
                        }
                    }
                }
            }
        }
        // Fallback: scan the log_segment from scratch.
        // TODO: a Partial cache already covers the commits read during snapshot load. A
        //       miss search could skip that range and only scan the older commits, then
        //       union those results with the Partial cache's entries to produce the final
        //       answer.
        let replayed = self.log_segment().scan_domain_metadatas(domains, engine)?;
        record_metric(false, replayed.len());
        Ok(replayed)
    }

    /// Fetch both user-controlled and system-controlled domain metadata for a specific domain
    /// in this snapshot.
    ///
    /// Returns the latest configuration for the domain, or `None` if the domain does not exist
    /// (or was removed). Unlike [`Snapshot::get_domain_metadata`], this does not reject `delta.*`
    /// domains.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_domain_metadata_internal(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> DeltaResult<Option<String>> {
        let mut map = self.get_domain_metadatas_internal(engine, Some(&HashSet::from([domain])))?;
        Ok(map.remove(domain).map(|dm| dm.configuration().to_owned()))
    }

    /// Fetch all non-internal domain metadata for this snapshot as a `Vec`.
    ///
    /// Internal (`delta.*`) domains are filtered out.
    #[allow(unused)]
    #[internal_api]
    pub(crate) fn get_all_domain_metadata(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<Vec<DomainMetadata>> {
        let all_metadata = self.get_domain_metadatas_internal(engine, None)?;
        Ok(all_metadata
            .into_values()
            .filter(|domain| !domain.is_internal())
            .collect())
    }

    /// Returns file-level statistics, or `None` if this snapshot has no CRC, or its CRC does
    /// not have `Complete` file stats. Performs no I/O (the CRC is resolved at construction).
    pub fn get_file_stats_if_present(&self) -> Option<FileStats> {
        self.crc.as_ref().and_then(|crc| crc.file_stats().cloned())
    }

    /// Get the In-Commit Timestamp (ICT) for this snapshot.
    ///
    /// Returns the `inCommitTimestamp` from the CommitInfo action of the commit that created this
    /// snapshot.
    ///
    /// # Returns
    /// - `Ok(Some(timestamp))` - ICT is enabled and available for this version
    /// - `Ok(None)` - ICT is not enabled
    /// - `Err(...)` - ICT is enabled but cannot be read, or enablement version is invalid
    #[instrument(parent = &self.span, name = "snap.get_ict", skip_all, err)]
    #[internal_api]
    pub(crate) fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        // Get ICT enablement info and check if we should read ICT for this version
        let enablement = self
            .table_configuration()
            .in_commit_timestamp_enablement()?;

        // Return None if ICT is not enabled at all
        if matches!(enablement, InCommitTimestampEnablement::NotEnabled) {
            return Ok(None);
        }

        // If ICT is enabled with an enablement version, verify the enablement version is not in the
        // future
        if let InCommitTimestampEnablement::Enabled {
            enablement: Some((enablement_version, _)),
        } = enablement
        {
            if self.version() < enablement_version {
                return Err(Error::generic(format!(
                    "Invalid state: snapshot at version {} has ICT enablement version {} in the future",
                    self.version(),
                    enablement_version
                )));
            }
        }

        // Fast path: serve ICT from CRC if available at this version.
        if let Some(crc) = self.crc.as_deref() {
            match crc.in_commit_timestamp_opt {
                Some(ict) => return Ok(Some(ict)),
                None => {
                    return Err(Error::generic(format!(
                        "In-Commit Timestamp not found in CRC file at version {}",
                        self.version()
                    )));
                }
            }
        }

        // Fallback: read the ICT from latest_commit_file
        match &self.log_segment.listed.latest_commit_file {
            Some(commit_file_meta) => {
                let ict = commit_file_meta.read_in_commit_timestamp(engine)?;
                Ok(Some(ict))
            }
            None => Err(Error::generic("Last commit file not found in log segment")),
        }
    }

    /// Get the timestamp for this snapshot's version, in milliseconds since the Unix epoch.
    ///
    /// When In-Commit Timestamp (ICT) are enabled, returns the In-Commit Timestamp value.
    /// Otherwise, falls back to the filesystem last-modified time of the latest commit file.
    ///
    /// Returns an error if the commit file is missing, the ICT configuration is invalid, or the
    /// ICT value cannot be read.
    ///
    /// See also [`get_in_commit_timestamp`] for ICT-only semantics.
    ///
    /// [`get_in_commit_timestamp`]: Self::get_in_commit_timestamp
    #[allow(unused)]
    #[instrument(parent = &self.span, name = "snap.get_ts", skip_all, err)]
    pub fn get_timestamp(&self, engine: &dyn Engine) -> DeltaResult<i64> {
        match self
            .table_configuration()
            .in_commit_timestamp_enablement()?
        {
            InCommitTimestampEnablement::NotEnabled => {
                match &self.log_segment.listed.latest_commit_file {
                    Some(commit_file_meta) => {
                        let ts = commit_file_meta.location.last_modified;
                        Ok(ts)
                    }
                    None => Err(Error::generic(format!(
                        "Last commit file not found in log segment for version {} \
                         (ICT disabled): cannot read filesystem modification timestamp",
                        self.version()
                    ))),
                }
            }
            InCommitTimestampEnablement::Enabled { .. } => self
                .get_in_commit_timestamp(engine)
                .map_err(|e| {
                    Error::generic(format!(
                        "Unable to read in-commit timestamp for version {}: {e}",
                        self.version()
                    ))
                })?
                .ok_or_else(|| {
                    Error::internal_error(format!(
                        "Invalid state: version {}, ICT is enabled \
                        but get_in_commit_timestamp returned None",
                        self.version()
                    ))
                }),
        }
    }

    // ============================================================================
    // Downstream operation builders
    // ============================================================================

    /// Create a [`ScanBuilder`] for an `SnapshotRef`.
    pub fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        ScanBuilder::new(self)
    }

    /// Create an [`IncrementalScanBuilder`] for the range `(base_version, self.version()]`.
    ///
    /// Use this to advance a cached file listing from `base_version` to this snapshot's
    /// version without doing a full scan. See [`IncrementalScanBuilder`] for details.
    pub fn incremental_scan_builder(
        self: Arc<Self>,
        base_version: Version,
    ) -> IncrementalScanBuilder {
        IncrementalScanBuilder::new(self, base_version)
    }

    /// Create a [`Transaction`] for this `SnapshotRef`. With the specified [`Committer`].
    ///
    /// Note: For tables with clustering enabled, this performs log replay to read clustering
    /// columns from domain metadata, which may have a performance cost.
    pub fn transaction(
        self: Arc<Self>,
        committer: Box<dyn Committer>,
        engine: &dyn Engine,
    ) -> DeltaResult<Transaction> {
        Transaction::try_new_existing_table(self, committer, engine)
    }

    /// Creates a builder for altering this table's metadata. Currently supports schema change
    /// operations.
    ///
    /// The returned builder allows chaining operations before building an
    /// [`AlterTableTransaction`] that can be committed.
    ///
    /// [`AlterTableTransaction`]: crate::transaction::AlterTableTransaction
    pub fn alter_table(self: Arc<Self>) -> AlterTableTransactionBuilder {
        AlterTableTransactionBuilder::new(self)
    }

    /// Creates a [`CheckpointWriter`] for generating a checkpoint from this snapshot.
    ///
    /// See the [`crate::checkpoint`] module documentation for more details on checkpoint types
    /// and the overall checkpoint process.
    pub fn create_checkpoint_writer(self: Arc<Self>) -> DeltaResult<CheckpointWriter> {
        CheckpointWriter::try_new(self)
    }

    /// Creates a [`LogCompactionWriter`] for generating a log compaction file.
    ///
    /// Log compaction aggregates commit files in a version range into a single compacted file,
    /// improving performance by reducing the number of files to process during log replay.
    ///
    /// # Parameters
    /// - `start_version`: The first version to include in the compaction (inclusive)
    /// - `end_version`: The last version to include in the compaction (inclusive)
    ///
    /// # Returns
    /// A [`LogCompactionWriter`] that can be used to generate the compaction file.
    ///
    /// NOTE: This method is currently a no-op because log compaction is disabled (#2337)
    pub fn log_compaction_writer(
        self: Arc<Self>,
        start_version: Version,
        end_version: Version,
    ) -> DeltaResult<LogCompactionWriter> {
        LogCompactionWriter::try_new(self, start_version, end_version)
    }

    // ============================================================================
    // Mutations
    // ============================================================================

    /// Writes a version checksum (CRC) file for this snapshot. Writers should call this after
    /// every commit because checksums enable faster snapshot loading and table state validation.
    ///
    /// Currently only supports writing from a post-commit snapshot that has pre-computed CRC
    /// information in memory (i.e. the snapshot returned by
    /// [`CommittedTransaction::post_commit_snapshot`]).
    ///
    /// Returns a tuple of [`ChecksumWriteResult`] and a [`SnapshotRef`]. On
    /// [`ChecksumWriteResult::Written`], the returned snapshot has the CRC file recorded in
    /// its log segment. On [`ChecksumWriteResult::AlreadyExists`], the original snapshot is
    /// returned unchanged.
    ///
    /// # Errors
    ///
    /// - [`Error::ChecksumWriteUnsupported`] if no in-memory CRC is available at this snapshot's
    ///   version (e.g. a snapshot loaded from disk that has no CRC file), if the CRC's
    ///   `file_stats_state` is `Indeterminate` (a non-incremental operation like ANALYZE STATS was
    ///   encountered, or a file action had a missing size; recoverable with a full state
    ///   reconstruction in the future), or if `delta.enableInCommitTimestamps` is `true` but
    ///   `inCommitTimestampOpt` is absent.
    /// - I/O errors from the engine's storage handler if the write fails.
    ///
    /// [`CommittedTransaction::post_commit_snapshot`]: crate::transaction::CommittedTransaction::post_commit_snapshot
    #[instrument(parent = &self.span, name = "snap.write_checksum", skip_all, err)]
    pub fn write_checksum(
        self: &SnapshotRef,
        engine: &dyn Engine,
    ) -> DeltaResult<(ChecksumWriteResult, SnapshotRef)> {
        let has_crc_on_disk = self
            .log_segment
            .listed
            .latest_crc_file
            .as_ref()
            .is_some_and(|f| f.version == self.version());

        if has_crc_on_disk {
            info!(
                "CRC file already exists on disk at version {}",
                self.version()
            );
            return Ok((ChecksumWriteResult::AlreadyExists, Arc::clone(self)));
        }

        let crc = self.crc.as_deref().ok_or_else(|| {
            Error::ChecksumWriteUnsupported(
                "No in-memory CRC available at this snapshot version.".to_string(),
            )
        })?;

        let crc_path = ParsedLogPath::new_crc(self.table_root(), self.version())?;

        match try_write_crc_file(engine, &crc_path.location, crc) {
            Ok(()) => {
                info!("Wrote CRC file at {}", crc_path.location);
                let new_log_segment = self.log_segment.try_new_with_crc_file(crc_path)?;
                let new_snapshot = Arc::new(Snapshot::new_with_crc(
                    new_log_segment,
                    self.table_configuration().clone(),
                    self.crc.clone(),
                )?);
                Ok((ChecksumWriteResult::Written, new_snapshot))
            }
            Err(Error::FileAlreadyExists(_)) => {
                info!(
                    "Another writer beat us to writing CRC file at {}",
                    crc_path.location
                );
                Ok((ChecksumWriteResult::AlreadyExists, Arc::clone(self)))
            }
            Err(e) => Err(e),
        }
    }

    /// Performs a complete checkpoint of this snapshot using the provided engine.
    ///
    /// If a checkpoint already exists at this version, returns
    /// [`CheckpointWriteResult::AlreadyExists`] with the original snapshot unchanged.
    /// Otherwise, writes a checkpoint parquet file and the `_last_checkpoint` file and returns
    /// [`CheckpointWriteResult::Written`] with an updated [`SnapshotRef`] whose log segment
    /// reflects the new checkpoint. Commits and compaction files subsumed by the checkpoint are
    /// dropped from the returned snapshot.
    ///
    /// # Parameters
    /// - `engine`: Engine for data processing and I/O
    /// - `spec`: Checkpoint format specification. `None` uses the default checkpoint settings
    ///   (auto-detecting V1/V2 from table features). For V2 checkpoints, the default is to not
    ///   write sidecar files.
    ///
    /// # Errors
    /// - If `CheckpointSpec::V2` is used but the table does not support the `v2Checkpoint` feature.
    /// - If `CheckpointSpec::V1` is used but the table supports `v2Checkpoint` feature. Note: the
    ///   Delta protocol permits writing V1 checkpoints to such tables; this is a kernel limitation.
    /// - If `file_actions_per_sidecar_hint` is `Some(0)`.
    /// - If the checkpoint write fails (e.g. I/O, parquet write). A `FileAlreadyExists` error is
    ///   not propagated; it returns [`CheckpointWriteResult::AlreadyExists`] instead. Note: this
    ///   also fires on the (unlikely) case of a sidecar UUID filename collision, where
    ///   it should ideally surface as an error. Tracked in
    ///   <https://github.com/delta-io/delta-kernel-rs/issues/2503>.
    ///
    /// Note:
    ///     - It is still possible that an existing checkpoint gets overwritten if that checkpoint
    ///       was written by a concurrent writer.
    ///     - This function uses [`crate::ParquetHandler::write_parquet_file`] and
    ///       [`crate::StorageHandler::head`], which may not be implemented by all engines. If you
    ///       are using the default engine, make sure to build it with the multi-threaded executor
    ///       if you want to use this method.
    ///
    /// [`CheckpointSpec`]: crate::checkpoint::CheckpointSpec
    ///
    /// Note: There is currently no public api for callers to determine whether a table supports V2
    /// checkpoints directly. Tracked in <https://github.com/delta-io/delta-kernel-rs/issues/2450>.
    #[instrument(parent = &self.span, name = "snap.checkpoint", skip_all, err)]
    pub fn checkpoint(
        self: &SnapshotRef,
        engine: &dyn Engine,
        spec: Option<&CheckpointSpec>,
    ) -> DeltaResult<(CheckpointWriteResult, SnapshotRef)> {
        if self.log_segment.checkpoint_version == Some(self.log_segment.end_version) {
            info!(
                "Checkpoint already exists for snapshot version {}",
                self.version()
            );
            return Ok((CheckpointWriteResult::AlreadyExists, Arc::clone(self)));
        }
        info!(
            "Writing checkpoint for snapshot version {} with spec {:?}",
            self.version(),
            spec
        );

        let v2_supported = self
            .table_configuration()
            .is_feature_supported(&TableFeature::V2Checkpoint);
        match spec {
            Some(CheckpointSpec::V2(cfg)) => {
                if !v2_supported {
                    return Err(Error::checkpoint_write(
                        "CheckpointSpec::V2 requires the v2Checkpoint table feature to be supported",
                    ));
                }
                if let V2CheckpointConfig::WithSidecar {
                    file_actions_per_sidecar_hint: Some(0),
                } = cfg
                {
                    return Err(Error::checkpoint_write(
                        "file_actions_per_sidecar_hint must be greater than 0",
                    ));
                }
            }
            Some(CheckpointSpec::V1) if v2_supported => {
                // TODO: remove this once we support writing V1 checkpoints even if table supports
                // v2Checkpoint See <https://github.com/delta-io/delta-kernel-rs/issues/2454>.
                return Err(Error::unsupported(
                    "Kernel does not support writing V1 checkpoints when the table supports v2Checkpoint",
                ));
            }
            _ => {}
        }

        let writer = Arc::clone(self).create_checkpoint_writer()?;

        let write_result = match spec {
            Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint,
            })) => {
                let hint =
                    file_actions_per_sidecar_hint.unwrap_or(DEFAULT_FILE_ACTIONS_PER_SIDECAR_HINT);
                writer.write_v2_checkpoint_with_sidecars(engine, hint)
            }
            _ => writer.write_checkpoint_without_sidecars(engine),
        };

        let info = match write_result {
            Ok(info) => info,
            Err(Error::FileAlreadyExists(_)) => {
                // NOTE: Per write_parquet_file's documentation, it should silently overwrite
                // existing files, so we log a warning but still return the correct result.
                warn!(
                    "ParquetHandler::write_parquet_file unexpectedly failed on \
                    FileAlreadyExists for version {}",
                    self.version()
                );
                return Ok((CheckpointWriteResult::AlreadyExists, Arc::clone(self)));
            }
            Err(e) => return Err(e),
        };

        writer.finalize(engine, &info.last_checkpoint_stats)?;

        let checkpoint_log_path = ParsedLogPath::try_from(info.file_meta)?.ok_or_else(|| {
            Error::internal_error("Checkpoint path could not be parsed as a log path")
        })?;
        let new_log_segment = self
            .log_segment
            .try_new_with_checkpoint(checkpoint_log_path)?;
        Ok((
            CheckpointWriteResult::Written,
            Arc::new(Snapshot::new_with_crc(
                new_log_segment,
                self.table_configuration().clone(),
                self.crc.clone(),
            )?),
        ))
    }

    /// Publishes all catalog commits at this table version. Applicable only to catalog-managed
    /// tables. This method is a no-op for filesystem-managed tables or if there are no catalog
    /// commits to publish.
    ///
    /// Publishing copies ratified catalog commits to the Delta log as published Delta files,
    /// reducing catalog storage requirements and enabling some table maintenance operations,
    /// like checkpointing.
    ///
    /// # Parameters
    ///
    /// - `engine`: The engine to use for publishing commits
    ///
    /// # Errors
    ///
    /// Returns an error if the publish operation fails, or if there are catalog commits that need
    /// publishing but the table or committer don't support publishing.
    ///
    /// # See Also
    ///
    /// - [`Committer::publish`]
    #[instrument(parent = &self.span, name = "snap.publish", skip_all, err)]
    pub fn publish(
        self: &SnapshotRef,
        engine: &dyn Engine,
        committer: &dyn Committer,
    ) -> DeltaResult<SnapshotRef> {
        let unpublished_catalog_commits = self.log_segment().get_unpublished_catalog_commits()?;

        if unpublished_catalog_commits.is_empty() {
            return Ok(Arc::clone(self));
        }

        require!(
            unpublished_catalog_commits
                .windows(2)
                .all(|commits| commits[0].version() + 1 == commits[1].version()),
            Error::generic(format!(
                "Expected ordered and contiguous unpublished catalog commits. \
                 Got: {unpublished_catalog_commits:?}"
            ))
        );

        require!(
            self.table_configuration().is_catalog_managed(),
            Error::generic(
                "There are catalog commits that need publishing, but the table is not catalog-managed.",
            )
        );

        require!(
            committer.is_catalog_committer(),
            Error::generic(
                "There are catalog commits that need publishing, but the committer is not a catalog committer.",
            )
        );

        let publish_metadata =
            PublishMetadata::try_new(self.version(), unpublished_catalog_commits)?;

        committer.publish(engine, publish_metadata)?;

        Ok(Arc::new(Snapshot::new_with_crc(
            self.log_segment().new_as_published()?,
            self.table_configuration().clone(),
            self.crc.clone(),
        )?))
    }
}

// TODO: unify this and lots of stuff in LogSegment tests and test_utils.
#[cfg(test)]
async fn commit(
    table_root: impl AsRef<str>,
    store: &crate::object_store::memory::InMemory,
    version: Version,
    commit: Vec<serde_json::Value>,
) {
    let commit_data = commit
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("\n");
    test_utils::add_commit(table_root, store, version, commit_data)
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::json;
    use test_utils::table_builder::{
        checkpoint_json_stats, unpartitioned, FeatureSet, LogState, TestTableBuilder, VersionTarget,
    };
    use test_utils::{add_commit, delta_path_for_version};

    use super::{commit, *};
    use crate::actions::{DomainMetadata, Protocol};
    use crate::arrow::array::StringArray;
    use crate::arrow::record_batch::RecordBatch;
    use crate::committer::FileSystemCommitter;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::sync::SyncEngine;
    use crate::last_checkpoint_hint::LastCheckpointHint;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::parquet::arrow::ArrowWriter;
    use crate::path::ParsedLogPath;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::{
        TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
    };
    use crate::table_properties::ENABLE_IN_COMMIT_TIMESTAMPS;
    use crate::transaction::create_table::create_table;
    use crate::utils::test_utils::{assert_result_error_with_message, string_array_to_engine_data};

    /// Helper function to create a commitInfo action with optional ICT
    fn create_commit_info(timestamp: i64, ict: Option<i64>) -> serde_json::Value {
        let mut commit_info = json!({
            "timestamp": timestamp,
            "operation": "WRITE",
        });

        if let Some(ict_value) = ict {
            commit_info["inCommitTimestamp"] = json!(ict_value);
        }

        json!({
            "commitInfo": commit_info
        })
    }

    fn create_protocol(ict_enabled: bool, min_reader_version: Option<u32>) -> serde_json::Value {
        let reader_version = min_reader_version.unwrap_or(1);

        if ict_enabled {
            let mut protocol = json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": TABLE_FEATURES_MIN_WRITER_VERSION,
                    "writerFeatures": ["inCommitTimestamp"]
                }
            });

            // Only include readerFeatures if minReaderVersion >= table-features minimum.
            if reader_version >= TABLE_FEATURES_MIN_READER_VERSION as u32 {
                protocol["protocol"]["readerFeatures"] = json!([]);
            }

            protocol
        } else {
            json!({
                "protocol": {
                    "minReaderVersion": reader_version,
                    "minWriterVersion": 2
                }
            })
        }
    }

    fn create_metadata(
        id: Option<&str>,
        schema_string: Option<&str>,
        created_time: Option<u64>,
        ict_config: Option<(String, String)>,
        ict_enabled_but_missing_version: bool,
    ) -> serde_json::Value {
        let config = if ict_enabled_but_missing_version {
            // Special case for testing ICT enabled but missing enablement info
            json!({
                "delta.enableInCommitTimestamps": "true"
            })
        } else if let Some((enablement_version, enablement_timestamp)) = ict_config {
            json!({
                "delta.enableInCommitTimestamps": "true",
                "delta.inCommitTimestampEnablementVersion": enablement_version,
                "delta.inCommitTimestampEnablementTimestamp": enablement_timestamp
            })
        } else {
            json!({})
        };

        json!({
            "metaData": {
                "id": id.unwrap_or("testId"),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string.unwrap_or("{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"),
                "partitionColumns": [],
                "configuration": config,
                "createdTime": created_time.unwrap_or(1587968586154u64)
            }
        })
    }

    fn create_basic_commit(ict_enabled: bool, ict_config: Option<(String, String)>) -> String {
        let protocol = create_protocol(ict_enabled, None);
        let metadata = create_metadata(None, None, None, ict_config, false);
        format!("{protocol}\n{metadata}")
    }

    fn create_snapshot_with_commit_file_absent_from_log_segment(
        url: &Url,
        table_cfg: TableConfiguration,
    ) -> DeltaResult<Snapshot> {
        // Create a log segment with only checkpoint and no commit file (simulating scenario
        // where a checkpoint exists but the commit file has been cleaned up)
        let checkpoint_parts = vec![ParsedLogPath::try_from(crate::FileMeta {
            location: url.join("_delta_log/00000000000000000000.checkpoint.parquet")?,
            last_modified: 0,
            size: 100,
        })?
        .unwrap()];

        let listed_files = LogSegmentFiles {
            checkpoint_parts,
            ..Default::default()
        };

        let log_segment =
            LogSegment::try_new(listed_files, url.join("_delta_log/")?, Some(0), None)?;

        Snapshot::new_with_crc(log_segment, table_cfg, None)
    }

    #[test]
    fn test_snapshot_read_metadata() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url)
            .at_version(1)
            .build(&engine)
            .unwrap();

        let expected = Protocol::try_new_modern(["deletionVectors"], ["deletionVectors"]).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    #[test]
    fn test_new_snapshot() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let expected = Protocol::try_new_modern(["deletionVectors"], ["deletionVectors"]).unwrap();
        assert_eq!(snapshot.table_configuration().protocol(), &expected);

        let schema_string = r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#;
        let expected: SchemaRef = serde_json::from_str(schema_string).unwrap();
        assert_eq!(snapshot.schema(), expected);
    }

    #[test]
    fn test_read_table_with_missing_last_checkpoint() {
        // this table doesn't have a _last_checkpoint file
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/table-with-dv-small/_delta_log/",
        ))
        .unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let storage = engine.storage_handler();
        let cp = LastCheckpointHint::try_read(storage.as_ref(), &url).unwrap();
        assert!(cp.is_none());
    }

    fn valid_last_checkpoint() -> (Vec<u8>, LastCheckpointHint) {
        let checkpoint = LastCheckpointHint {
            version: 1,
            size: 8,
            parts: None,
            size_in_bytes: Some(21857),
            num_of_add_files: None,
            checkpoint_schema: None,
            checksum: None,
            tags: None,
        };
        let data = checkpoint.to_json_bytes();
        (data, checkpoint)
    }

    fn valid_last_checkpoint_with_tags() -> (Vec<u8>, LastCheckpointHint) {
        use std::collections::HashMap;

        let (_, base_checkpoint) = valid_last_checkpoint();

        let mut tags = HashMap::new();
        tags.insert(
            "author".to_string(),
            "test_read_table_with_last_checkpoint".to_string(),
        );
        tags.insert("environment".to_string(), "snapshot_tests".to_string());
        tags.insert("created_by".to_string(), "delta-kernel-rs".to_string());

        let checkpoint = LastCheckpointHint {
            tags: Some(tags),
            ..base_checkpoint
        };

        let data = checkpoint.to_json_bytes();
        (data, checkpoint)
    }

    #[tokio::test]
    async fn test_read_table_with_empty_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // do a _last_checkpoint file with "{}" as content
        let empty = "{}".as_bytes().to_vec();
        let invalid_path = Path::from("invalid/_last_checkpoint");

        store
            .put(&invalid_path, empty.into())
            .await
            .expect("put _last_checkpoint");

        let engine = SyncEngine::new_with_store(store);
        let storage = engine.storage_handler();
        let url = Url::parse("memory:///invalid/").expect("valid url");
        let invalid =
            LastCheckpointHint::try_read(storage.as_ref(), &url).expect("read last checkpoint");
        assert!(invalid.is_none())
    }

    #[tokio::test]
    async fn test_read_table_with_last_checkpoint() {
        // in memory file system
        let store = Arc::new(InMemory::new());

        // Define test cases: (path, data, expected_result)
        let (data, expected) = valid_last_checkpoint();
        let (data_with_tags, expected_with_tags) = valid_last_checkpoint_with_tags();
        let test_cases = vec![
            ("valid", data, Some(expected)),
            ("invalid", "invalid".as_bytes().to_vec(), None),
            ("valid_with_tags", data_with_tags, Some(expected_with_tags)),
        ];

        // Write all test files to the in memory file system
        for (path_prefix, data, _) in &test_cases {
            let path = Path::from(format!("{path_prefix}/_last_checkpoint"));
            store
                .put(&path, data.clone().into())
                .await
                .expect("put _last_checkpoint");
        }

        let engine = SyncEngine::new_with_store(store);
        let storage = engine.storage_handler();

        // Test reading all checkpoints from the in memory file system for cases where the data is
        // valid, invalid and valid with tags.
        for (path_prefix, _, expected_result) in test_cases {
            let url = Url::parse(&format!("memory:///{path_prefix}/")).expect("valid url");
            let result =
                LastCheckpointHint::try_read(storage.as_ref(), &url).expect("read last checkpoint");
            assert_eq!(result, expected_result);
        }
    }

    #[test_log::test]
    fn test_read_table_with_checkpoint() {
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/",
        ))
        .unwrap();
        let location = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(location).build(&engine).unwrap();

        assert_eq!(snapshot.log_segment.listed.checkpoint_parts.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(
                snapshot.log_segment.listed.checkpoint_parts[0]
                    .location
                    .clone()
            )
            .unwrap()
            .unwrap()
            .version,
            2,
        );
        assert_eq!(snapshot.log_segment.listed.ascending_commit_files.len(), 1);
        assert_eq!(
            ParsedLogPath::try_from(
                snapshot.log_segment.listed.ascending_commit_files[0]
                    .location
                    .clone()
            )
            .unwrap()
            .unwrap()
            .version,
            3,
        );
    }

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        // commit0
        // - domain1: not removed
        // - domain2: not removed
        let commit = [
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain3",
                    "configuration": "domain3_commit0",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(table_root, store.clone().as_ref(), 0, commit)
            .await
            .unwrap();

        // commit1
        // - domain1: removed
        // - domain2: not-removed
        // - internal domain
        let commit = [
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit1",
                    "removed": true
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit1",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "delta.domain3",
                    "configuration": "domain3_commit1",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");
        add_commit(table_root, store.as_ref(), 1, commit)
            .await
            .unwrap();

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

        // Test get_domain_metadata

        assert_eq!(snapshot.get_domain_metadata("domain1", &engine)?, None);
        assert_eq!(
            snapshot.get_domain_metadata("domain2", &engine)?,
            Some("domain2_commit1".to_string())
        );
        assert_eq!(
            snapshot.get_domain_metadata("domain3", &engine)?,
            Some("domain3_commit0".to_string())
        );
        let err = snapshot
            .get_domain_metadata("delta.domain3", &engine)
            .unwrap_err();
        assert!(matches!(err, Error::Generic(msg) if
                msg == "User DomainMetadata are not allowed to use system-controlled 'delta.*' domain"));

        // Test get_domain_metadata_internal
        assert_eq!(
            snapshot.get_domain_metadata_internal("delta.domain3", &engine)?,
            Some("domain3_commit1".to_string())
        );

        // Test get_all_domain_metadata
        let mut metadata = snapshot.get_all_domain_metadata(&engine)?;
        metadata.sort_by(|a, b| a.domain().cmp(b.domain()));

        let mut expected = vec![
            DomainMetadata::new("domain2".to_string(), "domain2_commit1".to_string()),
            DomainMetadata::new("domain3".to_string(), "domain3_commit0".to_string()),
        ];
        expected.sort_by(|a, b| a.domain().cmp(b.domain()));

        assert_eq!(metadata, expected);

        Ok(())
    }

    #[test]
    #[ignore = "log compaction disabled (#2337)"]
    fn test_log_compaction_writer() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        // Test creating a log compaction writer
        let writer = snapshot.clone().log_compaction_writer(0, 1).unwrap();
        let path = writer.compaction_path();

        // Verify the path format is correct
        let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
        assert!(path.to_string().ends_with(expected_filename));

        // Test invalid version range (start >= end)
        let result = snapshot.clone().log_compaction_writer(2, 1);
        assert_result_error_with_message(result, "Invalid version range");

        // Test equal version range (also invalid)
        let result = snapshot.log_compaction_writer(1, 1);
        assert_result_error_with_message(result, "Invalid version range");
    }

    // TODO(#2337): remove this test when log compaction is re-enabled.
    #[test]
    fn test_log_compaction_writer_unsupported() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let result = snapshot.log_compaction_writer(0, 1);
        assert_result_error_with_message(result, "not currently supported");
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_disabled() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let table_root = "memory://test/";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create a basic commit without ICT enabled
        let commit0 = create_basic_commit(false, None);
        add_commit(table_root, store.as_ref(), 0, commit0).await?;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

        // When ICT is disabled, get_timestamp should return None
        let result = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(result, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_with_ict_enablement_timeline() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = Arc::new(InMemory::new());
        let table_root = "memory://test/";
        let engine = SyncEngine::new_with_store(store.clone());

        // Create initial commit without ICT
        let commit0 = create_basic_commit(false, None);
        add_commit(table_root, store.as_ref(), 0, commit0).await?;

        // Create commit that enables ICT (version 1 = enablement version)
        let commit1 =
            create_basic_commit(true, Some(("1".to_string(), "1587968586154".to_string())));
        add_commit(table_root, store.as_ref(), 1, commit1).await?;

        // Create commit with ICT enabled
        let expected_timestamp = 1587968586200i64;
        let commit2 = format!(
            r#"{{"commitInfo":{{"timestamp":1587968586154,"inCommitTimestamp":{expected_timestamp},"operation":"WRITE"}}}}"#,
        );
        add_commit(table_root, store.as_ref(), 2, commit2.to_string()).await?;

        // Read snapshot at version 0 (before ICT enablement)
        let snapshot_v0 = Snapshot::builder_for(table_root)
            .at_version(0)
            .build(&engine)?;
        // This snapshot version predates ICT enablement, so ICT is not available
        let result_v0 = snapshot_v0.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v0, None);

        // Read snapshot at version 2 (after ICT enabled)
        let snapshot_v2 = Snapshot::builder_for(table_root)
            .at_version(2)
            .build(&engine)?;
        // When ICT is enabled and available, timestamp() should return inCommitTimestamp
        let result_v2 = snapshot_v2.get_in_commit_timestamp(&engine)?;
        assert_eq!(result_v2, Some(expected_timestamp));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_enablement_version_in_future() -> DeltaResult<()> {
        // Test invalid state where snapshot has enablement version in the future - should error
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        let commit_data = [
            json!({
                "protocol": {
                    "minReaderVersion": TABLE_FEATURES_MIN_READER_VERSION,
                    "minWriterVersion": TABLE_FEATURES_MIN_WRITER_VERSION,
                    "readerFeatures": [],
                    "writerFeatures": ["inCommitTimestamp"]
                }
            }),
            json!({
                "metaData": {
                    "id": "test_id2",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                    "partitionColumns": [],
                    "configuration": {
                        "delta.enableInCommitTimestamps": "true",
                        "delta.inCommitTimestampEnablementVersion": "5", // Enablement after version 1
                        "delta.inCommitTimestampEnablementTimestamp": "1612345678"
                    },
                    "createdTime": 1677811175819u64
                }
            }),
        ];
        commit(table_root, store.as_ref(), 0, commit_data.to_vec()).await;

        // Create commit that predates ICT enablement (no inCommitTimestamp)
        let commit_predates = [create_commit_info(1234567890, None)];
        commit(table_root, store.as_ref(), 1, commit_predates.to_vec()).await;

        let snapshot_predates = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        let result_predates = snapshot_predates.get_in_commit_timestamp(&engine);

        // Version 1 with enablement at version 5 is invalid - should error
        assert_result_error_with_message(
            result_predates,
            "Invalid state: snapshot at version 1 has ICT enablement version 5 in the future",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_missing_ict_when_enabled() -> DeltaResult<()> {
        // Test missing ICT when it should be present - should error
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        let commit_data = [
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())),
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit_data.to_vec()).await; // ICT enabled from version 0

        // Create commit without ICT despite being enabled (corrupt case)
        let commit_missing_ict = [create_commit_info(1234567890, None)];
        commit(table_root, store.as_ref(), 1, commit_missing_ict.to_vec()).await;

        let snapshot_missing = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;
        let result = snapshot_missing.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "In-Commit Timestamp not found");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_fails_when_commit_missing() -> DeltaResult<()> {
        // When ICT is enabled but commit file is not found in log segment,
        // get_in_commit_timestamp should return an error

        let url = Url::parse("memory:///")?;
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        // Create initial commit with ICT enabled
        let commit_data = [
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())), // ICT enabled from version 0
                false,
            ),
        ];
        commit(url.as_str(), store.as_ref(), 0, commit_data.to_vec()).await;

        // Build snapshot to get table configuration
        let snapshot = Snapshot::builder_for(url.as_str())
            .at_version(0)
            .build(&engine)?;

        let snapshot_no_commit = create_snapshot_with_commit_file_absent_from_log_segment(
            &url,
            snapshot.table_configuration().clone(),
        )?;

        // Should return an error when commit file is missing
        let result = snapshot_no_commit.get_in_commit_timestamp(&engine);
        assert_result_error_with_message(result, "Last commit file not found in log segment");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_with_checkpoint_and_commit_same_version() -> DeltaResult<()> {
        // Test the scenario where both checkpoint and commit exist at the same version with ICT
        // enabled.
        let table_root = "memory:///test_table/";
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        // Create 00000000000000000000.json with ICT enabled
        let commit0_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit0_data.to_vec()).await;

        // Create 00000000000000000001.checkpoint.parquet
        let checkpoint_data = [
            create_commit_info(1587968586154, None),
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9"),
                Some("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"),
                Some(1587968585495),
                Some(("0".to_string(), "1587968586154".to_string())),
                false,
            ),
        ];

        let handler = engine.json_handler();
        let json_strings: StringArray = checkpoint_data
            .into_iter()
            .map(|json| json.to_string())
            .collect::<Vec<_>>()
            .into();
        let parsed = handler.parse_json(
            string_array_to_engine_data(json_strings),
            crate::actions::get_commit_schema().clone(),
        )?;
        let checkpoint = ArrowEngineData::try_from_engine_data(parsed)?;
        let checkpoint: RecordBatch = checkpoint.into();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, checkpoint.schema(), None)?;
        writer.write(&checkpoint)?;
        writer.close()?;

        let checkpoint_path = delta_path_for_version(1, "checkpoint.parquet");
        store.put(&checkpoint_path, buffer.into()).await?;

        // Create 00000000000000000001.json with ICT
        let expected_ict = 1587968586200i64;
        let commit1_data = [create_commit_info(1587968586200, Some(expected_ict))];
        commit(table_root, store.as_ref(), 1, commit1_data.to_vec()).await;

        // Build snapshot - LogSegment will filter out the commit file because checkpoint exists at
        // same version
        let snapshot = Snapshot::builder_for(table_root)
            .at_version(1)
            .build(&engine)?;

        // We should successfully read ICT by falling back to storage
        let timestamp = snapshot.get_in_commit_timestamp(&engine)?;
        assert_eq!(timestamp, Some(expected_ict));

        Ok(())
    }

    #[rstest]
    #[case::ict_disabled(false)]
    #[case::ict_enabled(true)]
    fn test_get_timestamp_returns_valid_timestamp(#[case] ict_enabled: bool) -> DeltaResult<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let table_path = Url::from_directory_path(temp_dir.path())
            .unwrap()
            .to_string();
        let engine = SyncEngine::new();

        let schema = Arc::new(StructType::try_new(vec![StructField::new(
            "id",
            DataType::INTEGER,
            true,
        )])?);

        let mut create_table_builder = create_table(&table_path, schema, "Test/1.0");
        if ict_enabled {
            create_table_builder = create_table_builder
                .with_table_properties(vec![(ENABLE_IN_COMMIT_TIMESTAMPS, "true")]);
        }

        let _ = create_table_builder
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?;

        let snapshot = Snapshot::builder_for(&table_path).build(&engine)?;
        let ts = snapshot.get_timestamp(&engine)?;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let two_days_ms = 2 * 24 * 60 * 60 * 1000_i64;
        assert!(
            (now_ms - two_days_ms..=now_ms).contains(&ts),
            "timestamp {ts} not within 2 days of now ({now_ms})"
        );

        if ict_enabled {
            let ict_ts = snapshot.get_in_commit_timestamp(&engine)?.unwrap();
            assert_eq!(ts, ict_ts);
        }
        Ok(())
    }

    #[rstest]
    #[case::ict_enabled(true)]
    #[case::ict_disabled(false)]
    #[tokio::test]
    async fn test_get_timestamp_errors_when_commit_file_missing(
        #[case] ict_enabled: bool,
    ) -> DeltaResult<()> {
        let url = Url::parse("memory:///")?;
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());

        // TODO: refactor `ict_config` from a raw tuple to a dedicated ICTConfig struct so the
        // enablement version and enablement timestamp fields are named and self-documenting.
        // The ict_config tuple is (inCommitTimestampEnablementVersion,
        // inCommitTimestampEnablementTimestamp): if ICT is enabled, the enablement version
        // is 0 with an arbitrary enablement timestamp.
        let ict_config = ict_enabled.then(|| ("0".to_string(), "1612345678".to_string()));
        let reader_version = ict_enabled.then_some(TABLE_FEATURES_MIN_READER_VERSION as u32);

        let mut commit_data = vec![];
        // When ICT is enabled, commitInfo must be the first action (protocol requirement)
        if ict_enabled {
            commit_data.push(create_commit_info(1677811175819, Some(1677811175999)));
        }
        commit_data.extend([
            create_protocol(ict_enabled, reader_version),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                ict_config,
                false,
            ),
        ]);
        commit(url.as_str(), store.as_ref(), 0, commit_data).await;

        let snapshot = Snapshot::builder_for(url.as_str())
            .at_version(0)
            .build(&engine)?;

        let snapshot_no_commit = create_snapshot_with_commit_file_absent_from_log_segment(
            &url,
            snapshot.table_configuration().clone(),
        )?;

        let result = snapshot_no_commit.get_timestamp(&engine);
        assert_result_error_with_message(result, "Last commit file not found in log segment");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_timestamp_errors_when_ict_missing_from_commit_info() -> DeltaResult<()> {
        // ICT is enabled and commit file IS present in the log segment, but the commitInfo
        // action does not carry an inCommitTimestamp value (corrupt/incomplete commit).
        let store = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";
        let engine = SyncEngine::new_with_store(store.clone());

        let commit0_data = vec![
            create_commit_info(1677811175819, None), // commitInfo without inCommitTimestamp
            create_protocol(true, Some(TABLE_FEATURES_MIN_READER_VERSION as u32)),
            create_metadata(
                Some("test_id"),
                Some("{\"type\":\"struct\",\"fields\":[]}"),
                Some(1677811175819),
                Some(("0".to_string(), "1612345678".to_string())), /* ict enabled at version 0, and an arbitrary timestamp */
                false,
            ),
        ];
        commit(table_root, store.as_ref(), 0, commit0_data).await;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        let result = snapshot.get_timestamp(&engine);
        assert_result_error_with_message(result, "In-Commit Timestamp not found in commit file");

        Ok(())
    }

    // Verifies the test_context! macro works from kernel/src/ unit tests
    // (crosses the crate type boundary via macro expansion). The kernel can't construct a
    // DefaultEngine here, so we pass a SyncEngine factory.
    #[test]
    fn test_context_macro_works_in_unit_test() {
        let (_engine, snap, _table) = test_utils::test_context!(
            LogState::with_latest_version(2),
            FeatureSet::empty(),
            unpartitioned(),
            checkpoint_json_stats(),
            VersionTarget::Latest,
            SyncEngine::new_with_store
        );
        assert_eq!(snap.version(), 2);
    }

    #[test]
    fn test_new_post_commit_simple() {
        // GIVEN
        let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();
        let engine = SyncEngine::new();
        let base_snapshot = Snapshot::builder_for(url.clone()).build(&engine).unwrap();
        let next_version = base_snapshot.version() + 1;

        // WHEN
        let fake_new_commit = ParsedLogPath::create_parsed_published_commit(&url, next_version);
        let post_commit_snapshot = base_snapshot
            .new_post_commit(fake_new_commit, CrcDelta::default())
            .unwrap();

        // THEN
        assert_eq!(post_commit_snapshot.version(), next_version);
        assert_eq!(post_commit_snapshot.log_segment().end_version, next_version);
    }

    #[test]
    fn test_get_protocol_derived_properties() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let url = url::Url::from_directory_path(path).unwrap();

        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(url).build(&engine).unwrap();

        let props = snapshot.get_protocol_derived_properties();
        assert_eq!(
            props.get("delta.minReaderVersion").unwrap(),
            &TABLE_FEATURES_MIN_READER_VERSION.to_string()
        );
        assert_eq!(
            props.get("delta.minWriterVersion").unwrap(),
            &TABLE_FEATURES_MIN_WRITER_VERSION.to_string()
        );
        assert_eq!(
            props.get("delta.feature.deletionVectors").unwrap(),
            "supported"
        );
    }

    #[tokio::test]
    async fn test_metadata_configuration() {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///";
        let engine = SyncEngine::new_with_store(storage.clone());

        // Create a commit with custom configuration
        let actions = vec![
            json!({"commitInfo": {"timestamp": 123, "operation": "CREATE TABLE"}}),
            json!({"protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": [],
                "writerFeatures": []
            }}),
            json!({"metaData": {
                "id": "test-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {
                    "io.unitycatalog.tableId": "abc-123",
                    "myapp.setting": "value"
                },
                "createdTime": 1234567890
            }}),
        ];
        commit(table_root, &storage, 0, actions).await;

        let snapshot = Snapshot::builder_for(table_root).build(&engine).unwrap();
        let config = snapshot.metadata_configuration();
        assert_eq!(
            config.get("io.unitycatalog.tableId"),
            Some(&"abc-123".to_string())
        );
        assert_eq!(config.get("myapp.setting"), Some(&"value".to_string()));
    }

    #[rstest::rstest]
    #[case::no_clustering(None, None, None)]
    #[case::clustered_no_column_mapping(
        Some(vec!["region"]),
        None,
        Some(vec![ColumnName::new(["region"])])
    )]
    #[case::clustered_with_column_mapping(
        Some(vec!["region"]),
        Some("name"),
        Some(vec![ColumnName::new(["region"])])
    )]
    fn test_get_logical_clustering_columns(
        #[case] clustering_cols: Option<Vec<&str>>,
        #[case] column_mapping_mode: Option<&str>,
        #[case] expected: Option<Vec<ColumnName>>,
    ) {
        use crate::transaction::create_table::create_table;
        use crate::transaction::data_layout::DataLayout;

        let storage = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(storage);
        let schema = Arc::new(
            crate::schema::StructType::try_new(vec![
                crate::schema::StructField::new("id", crate::schema::DataType::INTEGER, true),
                crate::schema::StructField::new("region", crate::schema::DataType::STRING, true),
            ])
            .unwrap(),
        );
        let mut builder = create_table("memory:///", schema, "test");
        if let Some(cols) = &clustering_cols {
            builder = builder.with_data_layout(DataLayout::clustered(cols.clone()));
        }
        if let Some(mode) = column_mapping_mode {
            builder = builder.with_table_properties([("delta.columnMapping.mode", mode)]);
        }
        let _ = builder
            .build(
                &engine,
                Box::new(crate::committer::FileSystemCommitter::new()),
            )
            .unwrap()
            .commit(&engine)
            .unwrap();
        let snapshot = Snapshot::builder_for("memory:///").build(&engine).unwrap();
        let result = snapshot.get_logical_clustering_columns(&engine).unwrap();
        assert_eq!(result, expected);
    }

    // === estimated_owned_heap_size ===
    /// Test that the estimated_owned_heap_size is correctly considering normal commit jsons,
    /// checkpoint parts, and log compaction files.
    #[test]
    fn estimated_owned_heap_size_on_table_with_many_log_files() {
        // Baseline: 101 commits (v0..=v100), no checkpoint, no compactions.
        let (_engine, baseline_snap, _table) = test_utils::test_context!(
            LogState::with_latest_version(100),
            FeatureSet::empty(),
            unpartitioned(),
            checkpoint_json_stats(),
            VersionTarget::Latest,
            SyncEngine::new_with_store
        );

        let baseline_heap = baseline_snap.estimated_owned_heap_size_bytes();
        let struct_size = size_of::<Snapshot>();
        // Heap size should be at least 5 times the stack size, to account for
        // the 100 commits file metadata.
        assert!(
            baseline_heap > 5 * struct_size,
            "baseline heap {baseline_heap} should exceed 5 * sizeof(Snapshot)={}",
            5 * struct_size
        );

        // 100 extra checkpoint parts: each contributes to heap.
        // Kernel doesn't yet support writing multi-part checkpoints, so we manually add them here.
        let snap_extra_checkpoints =
            snapshot_with_extra_files(&baseline_snap, |listed, log_root| {
                for i in 1..=100u32 {
                    let filename =
                        format!("00000000000000000099.checkpoint.{i:010}.0000000100.parquet");
                    let location = log_root.join(&filename).unwrap();
                    let part = ParsedLogPath::try_from(crate::FileMeta {
                        location,
                        last_modified: 0,
                        size: 100,
                    })
                    .unwrap()
                    .unwrap();
                    listed.checkpoint_parts.push(part);
                }
            });
        let delta_checkpoints =
            snap_extra_checkpoints.estimated_owned_heap_size_bytes() - baseline_heap;
        assert!(
            delta_checkpoints >= 15_000,
            "delta_checkpoints {delta_checkpoints} should be >= 15_000 for 100 checkpoint parts"
        );

        // 100 extra log compaction files: each contributes to heap.
        // Kernel disables writing log compaction files currently, so we manually add them here.
        let snap_extra_compactions =
            snapshot_with_extra_files(&baseline_snap, |listed, log_root| {
                for i in 0..100u64 {
                    let start = i * 10;
                    let end = start + 5;
                    let filename = format!("{start:020}.{end:020}.compacted.json");
                    let location = log_root.join(&filename).unwrap();
                    let comp = ParsedLogPath::try_from(crate::FileMeta {
                        location,
                        last_modified: 0,
                        size: 100,
                    })
                    .unwrap()
                    .unwrap();
                    listed.ascending_compaction_files.push(comp);
                }
            });
        let delta_compactions =
            snap_extra_compactions.estimated_owned_heap_size_bytes() - baseline_heap;
        assert!(
            delta_compactions >= 15_000,
            "delta_compactions {delta_compactions} should be >= 15_000 for 100 compaction files"
        );
    }

    /// Two tables that differ only in schema width: the wider schema should bump
    /// estimated_owned_heap_size by approximately the schemaString delta.
    #[test]
    fn estimated_owned_heap_size_reflects_schema_string() {
        fn snap_with_schema(schema: SchemaRef) -> SnapshotRef {
            let store = Arc::new(InMemory::new());
            let engine = SyncEngine::new_with_store(store);
            create_table("memory:///", schema, "test")
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .unwrap()
                .commit(&engine)
                .unwrap()
                .unwrap_committed();
            Snapshot::builder_for("memory:///").build(&engine).unwrap()
        }

        let small_schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("a", DataType::INTEGER)]).unwrap(),
        );
        let wide_schema = Arc::new(
            StructType::try_new(
                (0..50)
                    .map(|i| StructField::nullable(format!("field_{i:03}"), DataType::STRING))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );

        let snap_small = snap_with_schema(small_schema);
        let snap_wide = snap_with_schema(wide_schema);

        let schema_str_delta = snap_wide
            .table_configuration()
            .metadata()
            .schema_string()
            .capacity()
            - snap_small
                .table_configuration()
                .metadata()
                .schema_string()
                .capacity();
        let heap_delta = snap_wide.estimated_owned_heap_size_bytes()
            - snap_small.estimated_owned_heap_size_bytes();
        // Tables differ only in schemaString, so heap_delta should be approximately the
        // schema_str_delta.
        let ratio = heap_delta as f64 / schema_str_delta as f64;
        assert!(
            (0.8..=1.2).contains(&ratio),
            "heap_delta {heap_delta} should be within 20% of schema_str_delta {schema_str_delta} (ratio = {ratio:.3})"
        );
    }

    #[test]
    fn estimated_owned_heap_size_for_version_zero() {
        let table = TestTableBuilder::new().build().unwrap();
        let engine = SyncEngine::new_with_store(table.store().clone());
        let snapshot = Snapshot::builder_for(table.table_root())
            .build(&engine)
            .unwrap();

        let heap = snapshot.estimated_owned_heap_size_bytes();
        assert!(heap > 0, "heap size should be nonzero");
        assert!(
            heap < 10000,
            "heap size {heap} unexpectedly large for v0 snapshot"
        );
    }

    fn snapshot_with_extra_files<F>(baseline: &SnapshotRef, mutate: F) -> Snapshot
    where
        F: FnOnce(&mut LogSegmentFiles, &Url),
    {
        let mut new_log_segment = baseline.log_segment().clone();
        mutate(&mut new_log_segment.listed, &new_log_segment.log_root);
        Snapshot::new(new_log_segment, baseline.table_configuration().clone()).unwrap()
    }
}
