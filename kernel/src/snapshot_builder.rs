//! Basically a builder that builds TableConfiguration/Snapshot.
//! NOTE: might be renamed TableConfiguration -> ResolvedMetadata
//! Snapshot -> ResolvedTable

use crate::actions::{Metadata, Protocol};
use crate::table_configuration::TableConfiguration;
use crate::Engine;
use crate::{log_segment::LogSegment, snapshot::Snapshot, ParsedLogPath};
use crate::{DeltaResult, Version};

use url::Url;

enum SnapshotVersion {
    Latest,
    At(Version),
}

pub struct SnapshotBuilder {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
    target_version: SnapshotVersion,
}

pub struct ResolvedMetadata {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
    table_config: TableConfiguration,
}

// concern: where to do catalogManaged check? how does the engine say "yes I went to the catalog
// before building this snapshot"?

impl SnapshotBuilder {
    pub fn new_at(table_root: Url, version: Version) -> Self {
        Self {
            table_root,
            log_tail: Vec::new(),
            target_version: SnapshotVersion::At(version),
        }
    }

    pub fn new_latest(table_root: Url) -> Self {
        // FIXME: validate no catalogManaged
        Self {
            table_root,
            log_tail: Vec::new(),
            target_version: SnapshotVersion::Latest,
        }
    }

    // FIXME: don't like this
    pub fn with_log_tail(mut self, log_tail: Vec<ParsedLogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    // if you don't know PM @ version -> get back Snapshot
    // note this does (maybe)LIST + PM replay
    pub fn build_snapshot(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        let version = match self.target_version {
            SnapshotVersion::Latest => None,
            SnapshotVersion::At(v) => Some(v),
        };
        Snapshot::try_new(self.table_root, self.log_tail, engine, version)
    }

    // if you know PM @ version -> get back ResolvedMetadata (which you can then call
    // resolve_snapshot() on)
    //
    // skips PM replay
    pub fn build_metadata(
        self,
        protocol: Protocol,
        metadata: Metadata,
        version: Version,
    ) -> DeltaResult<ResolvedMetadata> {
        // FIXME: if target version is present it must match version here
        // OR we could remove version here and then just require that a version exists when you
        // call this API

        // TODO: fix table_root duplication
        Ok(ResolvedMetadata {
            table_root: self.table_root.clone(),
            log_tail: self.log_tail,
            table_config: TableConfiguration::try_new(
                metadata,
                protocol,
                self.table_root,
                version,
            )?,
        })
    }
}

impl ResolvedMetadata {
    // just does LIST (no PM replay)
    pub fn build_snapshot(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        let log_root = self.table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_snapshot(
            engine.storage_handler().as_ref(),
            log_root,
            self.log_tail,
            self.table_config.version(),
        )?;
        Ok(Snapshot::new(log_segment, self.table_config))
    }
}
