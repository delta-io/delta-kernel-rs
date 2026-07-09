use std::slice;
use std::sync::LazyLock;

use url::Url;

use crate::actions::visitors::InCommitTimestampVisitor;
use crate::actions::{Metadata, Protocol, COMMIT_INFO_FIELD, METADATA_FIELD, PROTOCOL_FIELD};
use crate::commit_range::with_version_context;
use crate::engine_data::RowVisitor as _;
use crate::path::ParsedLogPath;
use crate::schema::{lazy_schema_ref, SchemaRef};
use crate::table_configuration::{InCommitTimestampEnablement, TableConfiguration};
use crate::table_features::{ensure_table_can_be_read, Operation};
use crate::{DeltaResult, Engine, Error, FileDataReadResultIterator, Version};

/// A Delta log action kind.
///
/// Callers that need to read multiple action types pass a slice
/// (e.g. `&[DeltaAction::Add, DeltaAction::Remove]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaAction {
    Add,
    Remove,
    Metadata,
    Protocol,
    CommitInfo,
    Cdc,
    DomainMetadata,
    SetTxn,
    CheckpointMetadata,
    Sidecar,
}

/// Read schema for the per-commit header: protocol + metadata (for validation and the effective
/// table configuration) plus commitInfo (for the in-commit timestamp).
static HEADER_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    (&PROTOCOL_FIELD),
    (&METADATA_FIELD),
    (&COMMIT_INFO_FIELD),
};

/// Per-commit handle returned by [`super::CommitRange::commits`].
///
/// Carries the commit's version, timestamp, and the effective (extracted from this
/// commit overlaid onto the iterator's accumulated state) `Protocol` / `Metadata`.
/// Reading the commit's action batches is lazy and re-buildable via
/// [`Self::get_actions`], which issues a fresh JSON read on every call.
pub struct CommitAction {
    table_root: Url,
    log_path: ParsedLogPath,
    read_schema: SchemaRef,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
    /// Resolved commit timestamp (in-commit timestamp or file `last_modified`).
    timestamp: i64,
}

impl CommitAction {
    /// Construct and fully initialize a [`CommitAction`] for one commit JSON file.
    ///
    /// `seed_protocol` / `seed_metadata` carry the effective `(Protocol, Metadata)` inherited
    /// from prior commits (or from a start snapshot). This reads the commit's header
    /// (`[protocol, metadata, commitInfo]`), overlays any `Protocol` / `Metadata` the commit
    /// carries onto the seed, validates that the kernel can read the resulting configuration,
    /// and resolves the commit timestamp.
    pub(crate) fn try_new(
        engine: &dyn Engine,
        table_root: Url,
        log_path: ParsedLogPath,
        read_schema: SchemaRef,
        seed_protocol: Option<Protocol>,
        seed_metadata: Option<Metadata>,
    ) -> DeltaResult<Self> {
        let timestamp = log_path.location.last_modified;
        let mut this = Self {
            table_root,
            log_path,
            read_schema,
            protocol: seed_protocol,
            metadata: seed_metadata,
            timestamp,
        };
        let extracted_ict = this.read_commit_header(engine)?;
        // Build the effective table configuration once (when both protocol and metadata are
        // known) and reuse it for both validation and timestamp resolution.
        let table_config = match (&this.protocol, &this.metadata) {
            (Some(protocol), Some(metadata)) => Some(TableConfiguration::try_new(
                metadata.clone(),
                protocol.clone(),
                this.table_root.clone(),
                this.version(),
            )?),
            _ => None,
        };
        this.protocol_validation(&table_config)?;
        this.resolve_timestamp(&table_config, extracted_ict)?;
        Ok(this)
    }

    /// Commit version of this commit.
    pub fn version(&self) -> Version {
        self.log_path.version
    }

    /// Commit timestamp in milliseconds since epoch.
    ///
    /// For tables with in-commit timestamps enabled, this is the commit's
    /// `commitInfo.inCommitTimestamp` (for versions at or after the enablement version);
    /// otherwise it is the commit file's `last_modified` time. When the effective table
    /// configuration cannot be determined (e.g. a snapshot-less range that has not yet observed
    /// a `Metadata` action), the timestamp is best-effort: the in-commit timestamp if physically
    /// present, else `last_modified`. The value is not guaranteed monotonic across the
    /// enablement boundary or in such best-effort ranges.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub(crate) fn protocol(&self) -> Option<&Protocol> {
        self.protocol.as_ref()
    }

    pub(crate) fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    /// Read the commit header projected to `[protocol, metadata, commitInfo]`, overlay any
    /// `Protocol` / `Metadata` the commit carries onto `self` (a `None` extraction does NOT clear
    /// the inherited value), and return the commit's `inCommitTimestamp` if present.
    fn read_commit_header(&mut self, engine: &dyn Engine) -> DeltaResult<Option<i64>> {
        let json_iter = engine.json_handler().read_json_files(
            slice::from_ref(&self.log_path.location),
            HEADER_READ_SCHEMA.clone(),
            None,
        )?;

        let mut extracted_protocol: Option<Protocol> = None;
        let mut extracted_metadata: Option<Metadata> = None;
        let mut ict_visitor = InCommitTimestampVisitor::default();
        for (batch_index, batch_res) in json_iter.enumerate() {
            let batch = batch_res?;
            // The protocol requires commitInfo to be the first action when in-commit timestamps
            // are enabled, so it lives in the first batch (the visitor inspects only its first
            // row). Visiting only that batch matches the `table_changes` reference behavior.
            if batch_index == 0 {
                ict_visitor.visit_rows_of(batch.as_ref())?;
            }
            if extracted_protocol.is_none() {
                extracted_protocol = Protocol::try_new_from_data(batch.as_ref())?;
            }
            if extracted_metadata.is_none() {
                extracted_metadata = Metadata::try_new_from_data(batch.as_ref())?;
            }
            if extracted_protocol.is_some() && extracted_metadata.is_some() {
                break;
            }
        }

        if extracted_protocol.is_some() {
            self.protocol = extracted_protocol;
        }
        if extracted_metadata.is_some() {
            self.metadata = extracted_metadata;
        }
        Ok(ict_visitor.in_commit_timestamp)
    }

    /// Resolve and store the effective commit timestamp from the effective `(Protocol, Metadata)`
    /// and the extracted in-commit timestamp, following the in-commit timestamp protocol rules.
    ///
    /// When the configuration is known and ICT is enabled, commits at or after the enablement
    /// version use the in-commit timestamp (erroring if absent, which the protocol forbids);
    /// commits before it use `last_modified timestamp`. When the configuration is unknown (protocol
    /// or metadata not yet observed), this is best-effort: the in-commit timestamp if present,
    /// else `last_modified timestamp`.
    fn resolve_timestamp(
        &mut self,
        table_config: &Option<TableConfiguration>,
        extracted_ict: Option<i64>,
    ) -> DeltaResult<()> {
        let version = self.version();
        self.timestamp = match table_config {
            Some(table_config) => {
                let ict_applies = match table_config.in_commit_timestamp_enablement()? {
                    InCommitTimestampEnablement::NotEnabled => false,
                    InCommitTimestampEnablement::Enabled { enablement: None } => true,
                    InCommitTimestampEnablement::Enabled {
                        enablement: Some((enablement_version, _)),
                    } => version >= enablement_version,
                };
                if ict_applies {
                    extracted_ict.ok_or_else(|| {
                        with_version_context(version, Error::generic(
                            "in-commit timestamp is enabled but missing ICT timestamp field in commit"
                        ))
                    })?
                } else {
                    self.log_path.location.last_modified
                }
            }
            None => extracted_ict.unwrap_or(self.log_path.location.last_modified),
        };
        Ok(())
    }

    /// Validate that the kernel can read this commit, given the prebuilt effective `table_config`
    /// (present iff both protocol and metadata are known at this commit).
    fn protocol_validation(&self, table_config: &Option<TableConfiguration>) -> DeltaResult<()> {
        match (table_config, &self.protocol) {
            (Some(table_config), _) => table_config.ensure_operation_supported(Operation::Scan),
            (None, Some(protocol)) => ensure_table_can_be_read(protocol),
            (None, None) => Ok(()),
        }
    }

    /// Return an iterator over the commit's action batches projected to the
    /// caller-requested `read_schema`.
    ///
    /// Batches contain raw actions exactly as recorded in the commit JSON; no column-mapping
    /// translation is applied.
    pub fn get_actions(&self, engine: &dyn Engine) -> DeltaResult<FileDataReadResultIterator> {
        engine.json_handler().read_json_files(
            slice::from_ref(&self.log_path.location),
            self.read_schema.clone(),
            None,
        )
    }
}
