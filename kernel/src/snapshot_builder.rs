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

pub struct SnapshotBuilder<S: State> {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
    target_version: SnapshotVersion,
    state: S,
}

struct Empty {}
struct Pmv {
    // TODO: consider read/write generics or latest/time travel generic
    table_config: TableConfiguration,
    // optional log segment included here since we may 'end up' with a log segment on our way to
    // producing a Snapshot (e.g. if we do our own PM replay)
    log_segment: Option<LogSegment>,
}

trait State {}
impl State for Empty {}
impl State for Pmv {}

// concern: where to do catalogManaged check? how does the engine say "yes I went to the catalog
// before building this snapshot"?

impl SnapshotBuilder<Empty> {
    pub fn new_at(table_root: Url, version: Version) -> Self {
        Self {
            table_root,
            log_tail: Vec::new(),
            target_version: SnapshotVersion::At(version),
            state: Empty {},
        }
    }

    pub fn new_latest(table_root: Url) -> Self {
        // FIXME: validate no catalogManaged
        Self {
            table_root,
            log_tail: Vec::new(),
            target_version: SnapshotVersion::Latest,
            state: Empty {},
        }
    }

    // if you know PM @ version
    pub fn resolve_table_metadata(self, engine: &dyn Engine) -> DeltaResult<SnapshotBuilder<Pmv>> {
        let version = match self.target_version {
            SnapshotVersion::Latest => None,
            SnapshotVersion::At(v) => Some(v),
        };
        let log_root = self.table_root.join("_delta_log/")?;
        let log_segment = LogSegment::for_snapshot(
            engine.storage_handler().as_ref(),
            log_root,
            self.log_tail,
            version,
        )?;
        let resolved_version = log_segment.end_version;
        let (metadata, protocol) = log_segment.read_metadata(engine)?;
        Ok(SnapshotBuilder {
            table_root: self.table_root.clone(),
            log_tail: self.log_tail,
            // FIXME: remove - no target ver, resolved version
            target_version: SnapshotVersion::At(resolved_version),
            state: Pmv {
                table_config: TableConfiguration::try_new(
                    metadata,
                    protocol,
                    self.table_root,
                    resolved_version,
                )?,
                log_segment: Some(log_segment),
            },
        })
    }

    // if you know PM @ version
    pub fn with_protocol_metadata_version(
        self,
        protocol: Protocol,
        metadata: Metadata,
        version: Version,
    ) -> DeltaResult<SnapshotBuilder<Pmv>> {
        // FIXME: if target version is present it must match version here
        // OR we could remove version here and then just require that a version exists when you
        // call this API

        // TODO: make transition helper
        // TODO: fix table_root duplication
        Ok(SnapshotBuilder {
            table_root: self.table_root.clone(),
            log_tail: self.log_tail,
            target_version: Some(SnapshotVersion::At(version)),
            state: Pmv {
                table_config: TableConfiguration::try_new(
                    metadata,
                    protocol,
                    self.table_root,
                    version,
                )?,
                log_segment: None,
            },
        })
    }
}

impl<S: State> SnapshotBuilder<S> {
    pub fn with_log_tail(mut self, log_tail: Vec<ParsedLogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }
}
