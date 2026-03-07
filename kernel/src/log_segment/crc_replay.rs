// No consumers yet -- will be integrated in a follow-up PR.
#![allow(dead_code)]
//! Forward (ascending) CRC replay for [`LogSegment`].
//!
//! This module replays commit files oldest-first and accumulates a [`Crc`] that tracks
//! protocol, metadata, domain metadata, in-commit timestamp, and file stats.
//!
//! The replay engine ([`LogSegment::replay_ascending`]) reads all commits in a single
//! `read_json_files` call for engine-level parallelism, and uses [`CrcReplayVisitor`] to
//! extract CRC-relevant fields in a single pass per batch.

use std::sync::{Arc, LazyLock};

use tracing::{info, warn};

use crate::actions::visitors::{visit_metadata_at, visit_protocol_at, DomainMetadataVisitor};
use crate::actions::{
    DomainMetadata, Metadata, Protocol, ADD_NAME, COMMIT_INFO_NAME, DOMAIN_METADATA_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME,
};
use crate::crc::{try_read_crc_file, Crc, CrcDelta, FileStatsValidity};
use crate::engine_data::{GetData, TypedGetData as _};
use crate::schema::{
    ColumnNamesAndTypes, DataType, MetadataColumnSpec, SchemaRef, StructField, StructType,
    ToSchema as _,
};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error, RowVisitor, Version};

use super::LogSegment;

// ================================================================================
// Schema and getter layout
// ================================================================================

/// File path metadata column name. The engine injects the source file URL into each batch
/// when this column is present in the read schema.
const FILE_PATH_COLUMN: &str = "_file_path";

/// Number of getters expected by [`CrcReplayVisitor`].
///
/// Layout (22 total):
///   [0]      commitInfo.operation
///   [1]      commitInfo.inCommitTimestamp
///   [2..6]   protocol (4 leaves: minReaderVersion, minWriterVersion, readerFeatures, writerFeatures)
///   [6..15]  metaData (9 leaves: id, name, description, format.provider, format.options,
///            schemaString, partitionColumns, createdTime, configuration)
///   [15..18] domainMetadata (3 leaves: domain, configuration, removed)
///   [18]     add.size
///   [19]     remove.path  (non-null when remove is present; detects remove-with-null-size)
///   [20]     remove.size  (nullable; null on very old tables)
///   [21]     _file_path
const NUM_GETTERS: usize = 22;

// Getter index ranges for delegating to existing visitor functions.
const PROTOCOL_START: usize = 2;
const PROTOCOL_END: usize = 6;
const METADATA_START: usize = 6;
const METADATA_END: usize = 15;
const DM_START: usize = 15;
const DM_END: usize = 18;
const ADD_SIZE_IDX: usize = 18;
const REMOVE_PATH_IDX: usize = 19;
const REMOVE_SIZE_IDX: usize = 20;
const FILE_PATH_IDX: usize = 21;

/// Narrow schema for CRC replay, containing only the fields needed to build a [`Crc`].
///
/// Fields:
/// - `commitInfo { operation, inCommitTimestamp }` -- operation safety check and ICT
/// - `protocol { ... }` -- full Protocol via `Protocol::to_schema()`
/// - `metaData { ... }` -- full Metadata via `Metadata::to_schema()`
/// - `domainMetadata { ... }` -- full DomainMetadata via `DomainMetadata::to_schema()`
/// - `add { size }` -- file count/size tracking for adds
/// - `remove { path, size }` -- file count/size tracking for removes (path detects presence)
/// - `_file_path` -- metadata column for file boundary detection
fn crc_replay_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        let commit_info = StructField::nullable(
            COMMIT_INFO_NAME,
            StructType::new_unchecked([
                StructField::nullable("operation", DataType::STRING),
                StructField::nullable("inCommitTimestamp", DataType::LONG),
            ]),
        );
        let protocol = StructField::nullable(PROTOCOL_NAME, Protocol::to_schema());
        let metadata = StructField::nullable(METADATA_NAME, Metadata::to_schema());
        let domain_metadata =
            StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema());
        let add = StructField::nullable(
            ADD_NAME,
            StructType::new_unchecked([StructField::not_null("size", DataType::LONG)]),
        );
        let remove = StructField::nullable(
            REMOVE_NAME,
            StructType::new_unchecked([
                StructField::not_null("path", DataType::STRING),
                StructField::nullable("size", DataType::LONG),
            ]),
        );

        let base = StructType::new_unchecked([
            commit_info,
            protocol,
            metadata,
            domain_metadata,
            add,
            remove,
        ]);
        // SAFETY: The base schema has no metadata columns, so adding one always succeeds.
        #[allow(clippy::expect_used)]
        Arc::new(
            base.add_metadata_column(FILE_PATH_COLUMN, MetadataColumnSpec::FilePath)
                .expect("static schema construction cannot fail"),
        )
    });
    SCHEMA.clone()
}

/// Column names and types for [`CrcReplayVisitor`], derived from [`crc_replay_schema`].
fn crc_replay_names_and_types() -> &'static ColumnNamesAndTypes {
    static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
        LazyLock::new(|| crc_replay_schema().leaves(None));
    &NAMES_AND_TYPES
}

// ================================================================================
// CRC replay visitor
// ================================================================================

/// Single-pass visitor that extracts all CRC-relevant data from commit batches.
///
/// Each batch belongs to exactly one commit file (the engine preserves file order via
/// `try_flatten` on ordered file streams). The visitor detects file boundaries via the
/// `_file_path` metadata column and flushes the accumulated [`CrcDelta`] to the
/// [`Crc`] at each boundary.
struct CrcReplayVisitor<'a> {
    /// The CRC state being built up during replay. `None` when unseeded (no CRC file); the
    /// first complete delta (with P&M) creates the initial CRC via `CrcDelta::into_crc()`.
    crc: &'a mut Option<Crc>,
    /// Tracks the current file path for boundary detection.
    current_file: &'a mut Option<String>,
    /// Accumulates CRC changes for the current commit file.
    delta: &'a mut CrcDelta,
}

impl RowVisitor for CrcReplayVisitor<'_> {
    fn selected_column_names_and_types(
        &self,
    ) -> (&'static [crate::schema::ColumnName], &'static [DataType]) {
        crc_replay_names_and_types().as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == NUM_GETTERS,
            Error::InternalError(format!(
                "Wrong number of CrcReplayVisitor getters: {} (expected {})",
                getters.len(),
                NUM_GETTERS
            ))
        );

        if row_count == 0 {
            return Ok(());
        }

        // File boundary detection: read _file_path from the first row. Each batch belongs
        // to exactly one file, so checking row 0 is sufficient.
        let file_path: String = getters[FILE_PATH_IDX].get(0, FILE_PATH_COLUMN)?;
        if self.current_file.as_deref() != Some(&file_path) {
            // New file: flush the previous file's delta before starting a new one.
            if self.current_file.is_some() {
                flush_delta(self.crc, self.delta);
            }
            *self.current_file = Some(file_path);
        }

        // Process all rows in this batch.
        for i in 0..row_count {
            // commitInfo.operation (getter 0) -- take the first non-null value per file.
            if self.delta.operation.is_none() {
                if let Some(op) = getters[0].get_opt(i, "commitInfo.operation")? {
                    self.delta.operation = Some(op);
                }
            }

            // commitInfo.inCommitTimestamp (getter 1) -- take the first non-null value per file.
            if self.delta.in_commit_timestamp.is_none() {
                if let Some(ict) = getters[1].get_opt(i, "commitInfo.inCommitTimestamp")? {
                    self.delta.in_commit_timestamp = Some(ict);
                }
            }

            // protocol (getters 2..6) -- take the first non-null protocol per file.
            if let Some(p) = visit_protocol_at(i, &getters[PROTOCOL_START..PROTOCOL_END])? {
                if self.delta.protocol.is_some() {
                    warn!("Multiple protocol actions in a single commit file");
                } else {
                    self.delta.protocol = Some(p);
                }
            }

            // metaData (getters 6..15) -- take the first non-null metadata per file.
            if let Some(m) = visit_metadata_at(i, &getters[METADATA_START..METADATA_END])? {
                if self.delta.metadata.is_some() {
                    warn!("Multiple metadata actions in a single commit file");
                } else {
                    self.delta.metadata = Some(m);
                }
            }

            // domainMetadata (getters 15..18) -- accumulate all domain metadata changes.
            let domain: Option<String> = getters[DM_START].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = domain {
                let dm = DomainMetadataVisitor::visit_domain_metadata(
                    i,
                    domain,
                    &getters[DM_START..DM_END],
                )?;
                self.delta.domain_metadata_changes.push(dm);
            }

            // add.size (getter 18) -- count added files and bytes.
            let add_size: Option<i64> = getters[ADD_SIZE_IDX].get_opt(i, "add.size")?;
            if let Some(size) = add_size {
                self.delta.file_stats.net_files += 1;
                self.delta.file_stats.net_bytes += size;
            }

            // remove.path (getter 19) -- non-null when this row is a remove action.
            // remove.size (getter 20) -- nullable; null on very old tables.
            // We need remove.path to distinguish "not a remove" (path=null) from
            // "remove with missing size" (path=Some, size=null).
            let remove_path: Option<String> = getters[REMOVE_PATH_IDX].get_opt(i, "remove.path")?;
            if remove_path.is_some() {
                let remove_size: Option<i64> =
                    getters[REMOVE_SIZE_IDX].get_opt(i, "remove.size")?;
                if let Some(size) = remove_size {
                    self.delta.file_stats.net_files -= 1;
                    self.delta.file_stats.net_bytes -= size;
                } else {
                    // Remove action present but size is null -- byte stats are unrecoverable.
                    self.delta.file_stats.net_files -= 1;
                    self.delta.has_missing_file_size = true;
                }
            }
        }

        Ok(())
    }
}

/// Flush a completed delta into the CRC state. If the CRC exists, apply the delta. If it
/// doesn't exist yet (unseeded replay), attempt to create the initial CRC from the delta.
fn flush_delta(crc: &mut Option<Crc>, delta: &mut CrcDelta) {
    let delta = std::mem::take(delta);
    match crc {
        Some(c) => c.apply(delta),
        None => *crc = delta.into_crc(),
    }
}

// ================================================================================
// Replay orchestration
// ================================================================================

impl LogSegment {
    /// Orchestrate forward CRC replay: seed from the best available source and replay commits.
    ///
    /// Tries seeding strategies in order:
    /// 1. CRC file -- if a CRC file exists and loads successfully, seed from it (validity =
    ///    Valid) and replay only commits after the CRC version.
    /// 2. No usable CRC (zero-rooted or checkpoint-rooted) -- replay all commits from
    ///    scratch. The first commit with Protocol + Metadata creates the initial CRC via
    ///    `CrcDelta::into_crc()`. For checkpoint-rooted tables, file stats validity is overridden
    ///    to [`RequiresCheckpointRead`](FileStatsValidity::RequiresCheckpointRead) because the
    ///    checkpoint's add actions are not scanned for baseline counts.
    ///
    /// CRC read failures are logged and silently fall through to unseeded replay.
    pub(crate) fn forward_crc_replay(&self, engine: &dyn Engine) -> DeltaResult<Crc> {
        // Try to seed from an existing CRC file.
        if let Some(crc_path) = &self.listed.latest_crc_file {
            match try_read_crc_file(engine, crc_path) {
                Ok(seed) => {
                    if crc_path.version == self.end_version {
                        info!(
                            "CRC at target version {}, no replay needed",
                            self.end_version
                        );
                        return Ok(seed);
                    }
                    info!(
                        "CRC-rooted replay: use CRC from v{}, replay from v{} to v{}",
                        crc_path.version,
                        crc_path.version + 1,
                        self.end_version
                    );
                    return self.replay_ascending(engine, Some(seed), Some(crc_path.version));
                }
                Err(e) => {
                    warn!(
                        "CRC read failed at v{}: {e}, falling back",
                        crc_path.version
                    );
                }
            }
        }

        // No usable CRC -- replay all commits from scratch.
        let label = if self.listed.checkpoint_parts.is_empty() {
            "Zero-rooted"
        } else {
            "Checkpoint-rooted"
        };
        info!(
            "{label} replay: replay all commits to v{}",
            self.end_version
        );

        let mut crc = self.replay_ascending(engine, None, None)?;

        // Checkpoint-rooted: file stats are relative deltas (no checkpoint baseline).
        // Override validity unless replay already degraded it further.
        if !self.listed.checkpoint_parts.is_empty() && crc.validity == FileStatsValidity::Valid {
            crc.validity = FileStatsValidity::RequiresCheckpointRead;
        }

        Ok(crc)
    }

    /// Replay ascending commit files and accumulate CRC state, returning the updated CRC.
    ///
    /// The caller provides an optional pre-seeded `Crc`:
    /// - `Some(crc)`: seeded replay (from a CRC file). Deltas are applied to the existing CRC.
    /// - `None`: unseeded replay. The first commit with both Protocol and Metadata creates the
    ///   initial CRC via [`CrcDelta::into_crc()`]. Returns an error if no commit provides P&M.
    ///
    /// When `after_version` is `Some(v)`, only commits with version > v are replayed. This is
    /// used for CRC-rooted replay where the CRC already covers versions up to v.
    ///
    /// All matching commit files are read in a single `read_json_files` call for engine-level
    /// I/O parallelism. The engine returns a stream of batches, where each batch belongs to
    /// exactly one commit file. The visitor accumulates a per-file [`CrcDelta`] and detects
    /// file boundaries via the `_file_path` metadata column:
    ///
    /// - First file: `current_file` is None, so the visitor just starts accumulating into
    ///   the delta. No apply happens yet.
    /// - File boundary: The visitor sees a new `_file_path`. For seeded replay, applies
    ///   the accumulated delta via `crc.apply(delta)`. For unseeded replay, the first complete
    ///   delta creates the CRC via `into_crc()`; subsequent deltas use `apply`.
    /// - Last file: No subsequent file triggers a boundary, so the caller flushes the
    ///   final delta after the batch loop ends.
    ///
    /// No compaction file support: individual commit files are always available and compaction
    /// files span multiple commits, making per-commit delta construction impossible.
    pub(crate) fn replay_ascending(
        &self,
        engine: &dyn Engine,
        mut crc: Option<Crc>,
        after_version: Option<Version>,
    ) -> DeltaResult<Crc> {
        let commit_files: Vec<_> = self
            .listed
            .ascending_commit_files
            .iter()
            .filter(|f| after_version.is_none_or(|v| f.version > v))
            .collect();

        if commit_files.is_empty() {
            return crc.ok_or_else(|| {
                Error::generic("CRC replay found no protocol or metadata (no commits to replay)")
            });
        }

        let file_metas: Vec<_> = commit_files.iter().map(|f| f.location.clone()).collect();

        if let (Some(first), Some(last)) = (commit_files.first(), commit_files.last()) {
            info!(
                "CRC replay_ascending: {} commit files (v{} to v{})",
                file_metas.len(),
                first.version,
                last.version,
            );
        }

        let batches =
            engine
                .json_handler()
                .read_json_files(&file_metas, crc_replay_schema(), None)?;

        let mut current_file: Option<String> = None;
        let mut delta = CrcDelta::default();

        for batch in batches {
            let batch = batch?;
            CrcReplayVisitor {
                crc: &mut crc,
                current_file: &mut current_file,
                delta: &mut delta,
            }
            .visit_rows_of(batch.as_ref())?;
        }

        // Flush the last file's delta (no boundary was triggered for it).
        if current_file.is_some() {
            flush_delta(&mut crc, &mut delta);
        }

        crc.ok_or_else(|| Error::generic("CRC replay found no protocol or metadata in any commit"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use serde_json::json;
    use url::Url;

    use crate::actions::{Format, Metadata, Protocol};
    use crate::crc::{Crc, FileStatsValidity};
    use crate::engine::default::DefaultEngineBuilder;
    use crate::Engine;

    use test_utils::delta_path_for_version;

    // ========================================================================
    // Test helpers
    // ========================================================================

    const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

    fn test_protocol() -> Protocol {
        Protocol::try_new(1, 2, None::<Vec<String>>, None::<Vec<String>>).unwrap()
    }

    fn test_metadata() -> Metadata {
        Metadata::new_unchecked(
            "test-id",
            None,
            None,
            Format::default(),
            SCHEMA_STRING,
            vec![],
            Some(1000),
            std::collections::HashMap::new(),
        )
    }

    /// Write a commit JSON file to the in-memory store at the given version.
    async fn write_commit(store: &InMemory, version: u64, lines: &[serde_json::Value]) {
        let content = lines
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        store
            .put(
                &delta_path_for_version(version, "json"),
                content.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
    }

    fn protocol_json(p: &Protocol) -> serde_json::Value {
        json!({"protocol": serde_json::to_value(p).unwrap()})
    }

    fn metadata_json(m: &Metadata) -> serde_json::Value {
        json!({"metaData": serde_json::to_value(m).unwrap()})
    }

    fn commit_info_json(operation: &str) -> serde_json::Value {
        json!({"commitInfo": {"operation": operation, "timestamp": 1000}})
    }

    fn commit_info_json_with_ict(operation: &str, ict: i64) -> serde_json::Value {
        json!({"commitInfo": {"operation": operation, "timestamp": 1000, "inCommitTimestamp": ict}})
    }

    fn add_json(path: &str, size: i64) -> serde_json::Value {
        json!({"add": {"path": path, "size": size, "dataChange": true}})
    }

    fn remove_json(path: &str, size: i64) -> serde_json::Value {
        json!({"remove": {"path": path, "size": size, "dataChange": true, "deletionTimestamp": 1000}})
    }

    fn remove_json_no_size(path: &str) -> serde_json::Value {
        json!({"remove": {"path": path, "dataChange": true, "deletionTimestamp": 1000}})
    }

    fn domain_metadata_json(domain: &str, config: &str) -> serde_json::Value {
        json!({"domainMetadata": {"domain": domain, "configuration": config, "removed": false}})
    }

    fn domain_metadata_remove_json(domain: &str) -> serde_json::Value {
        json!({"domainMetadata": {"domain": domain, "configuration": "", "removed": true}})
    }

    /// Write a CRC JSON file to the in-memory store at the given version.
    async fn write_crc(store: &InMemory, version: u64, crc: &super::Crc) {
        let content = serde_json::to_string(crc).unwrap();
        store
            .put(
                &delta_path_for_version(version, "crc"),
                content.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
    }

    /// Write corrupt (non-JSON) data as a CRC file at the given version.
    async fn write_corrupt_crc(store: &InMemory, version: u64) {
        store
            .put(
                &delta_path_for_version(version, "crc"),
                b"not valid json".to_vec().into(),
            )
            .await
            .unwrap();
    }

    /// Build an engine + log segment from an in-memory store with commits at specified versions.
    /// Optionally includes a CRC file at a specific version.
    fn build_log_segment(
        store: Arc<InMemory>,
        versions: &[u64],
        crc_version: Option<u64>,
    ) -> (impl Engine, super::LogSegment) {
        use crate::log_segment_files::LogSegmentFiles;
        use crate::path::ParsedLogPath;
        use crate::FileMeta;

        let url = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(store).build();

        let log_root = url.join("_delta_log/").unwrap();
        let ascending_commit_files: Vec<ParsedLogPath> = versions
            .iter()
            .map(|&v| {
                let file_url = log_root.join(&format!("{v:020}.json")).unwrap();
                ParsedLogPath::try_from(FileMeta::new(file_url, 0, 0))
                    .unwrap()
                    .unwrap()
            })
            .collect();

        let latest_crc_file = crc_version.map(|v| ParsedLogPath::create_parsed_crc(&url, v));

        let end_version = *versions.last().unwrap();
        let segment = super::LogSegment {
            end_version,
            checkpoint_version: None,
            log_root,
            checkpoint_schema: None,
            listed: LogSegmentFiles {
                ascending_commit_files,
                ascending_compaction_files: vec![],
                checkpoint_parts: vec![],
                latest_crc_file,
                latest_commit_file: None,
                max_published_version: None,
            },
        };

        (engine, segment)
    }

    // ============================================================================================
    // Tests for replay_ascending (the replay engine).
    //
    // All tests use Crc::default() as the seed (zero stats, Valid). Seeding from a CRC file
    // or checkpoint is the orchestrator's job (forward_crc_replay) and is tested separately.
    // ============================================================================================

    #[tokio::test]
    async fn test_replay_ascending_accumulates_pm_dm_file_stats_and_ict() {
        let store = Arc::new(InMemory::new());

        // v0: table creation with P + M, 2 adds, DM, ICT
        //   -> P(1,2), M("test-id"), files=2/300B, DM={domainA:v1}, ICT=5000
        write_commit(
            &store,
            0,
            &[
                commit_info_json_with_ict("CREATE TABLE", 5000),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
                add_json("file2.parquet", 200),
                domain_metadata_json("domainA", "configA_v1"),
            ],
        )
        .await;

        // v1: add a file, upsert domainA, add domainB, update ICT
        //   -> P(1,2), M("test-id"), files=3/600B, DM={domainA:v2, domainB}, ICT=6000
        write_commit(
            &store,
            1,
            &[
                commit_info_json_with_ict("WRITE", 6000),
                add_json("file3.parquet", 300),
                domain_metadata_json("domainA", "configA_v2"),
                domain_metadata_json("domainB", "configB"),
            ],
        )
        .await;

        // v2: remove a file, remove domainA
        //   -> P(1,2), M("test-id"), files=2/500B, DM={domainB}, ICT=6000
        write_commit(
            &store,
            2,
            &[
                commit_info_json("DELETE"),
                remove_json("file1.parquet", 100),
                domain_metadata_remove_json("domainA"),
            ],
        )
        .await;

        // v3: protocol upgrade, add a file
        //   -> P(1,3), M("test-id"), files=3/900B, DM={domainB}, ICT=7000
        let updated_protocol =
            Protocol::try_new(1, 3, None::<Vec<String>>, None::<Vec<String>>).unwrap();
        write_commit(
            &store,
            3,
            &[
                commit_info_json_with_ict("WRITE", 7000),
                protocol_json(&updated_protocol),
                add_json("file4.parquet", 400),
            ],
        )
        .await;

        // v4: metadata change, add a file
        //   -> P(1,3), M("updated-id"), files=4/1300B, DM={domainB}, ICT=8000
        let updated_metadata = Metadata::new_unchecked(
            "updated-id",
            Some("my_table".to_string()),
            None,
            Format::default(),
            SCHEMA_STRING,
            vec![],
            Some(2000),
            std::collections::HashMap::new(),
        );
        write_commit(
            &store,
            4,
            &[
                commit_info_json_with_ict("WRITE", 8000),
                metadata_json(&updated_metadata),
                add_json("file5.parquet", 400),
            ],
        )
        .await;

        let (engine, segment) = build_log_segment(store, &[0, 1, 2, 3, 4], None);
        let crc = segment
            .replay_ascending(&engine, Some(Crc::default()), None)
            .unwrap();

        // Protocol: set in v0, upgraded in v3.
        assert_eq!(crc.protocol, updated_protocol);

        // Metadata: set in v0, replaced in v4.
        assert_eq!(crc.metadata, updated_metadata);

        // File stats: +2 (v0) +1 (v1) -1 (v2) +1 (v3) +1 (v4) = 4 files
        // Bytes: 100+200 (v0) +300 (v1) -100 (v2) +400 (v3) +400 (v4) = 1300
        assert_eq!(crc.num_files, 4);
        assert_eq!(crc.table_size_bytes, 1300);
        assert_eq!(crc.validity, FileStatsValidity::Valid);

        // Domain metadata: domainA added (v0), upserted (v1), removed (v2). domainB added (v1).
        let dm = crc.domain_metadata.as_ref().unwrap();
        assert_eq!(dm.len(), 1);
        assert!(!dm.contains_key("domainA"), "domainA was removed in v2");
        assert_eq!(dm["domainB"].configuration(), "configB");

        // ICT: last value wins -> v4's 8000.
        assert_eq!(crc.in_commit_timestamp_opt, Some(8000));
    }

    #[tokio::test]
    async fn test_replay_ascending_empty_segment_is_noop() {
        let store = Arc::new(InMemory::new());
        let (engine, segment) = build_log_segment(store, &[0], None);

        let mut segment = segment;
        segment.listed.ascending_commit_files = vec![];

        let crc = segment
            .replay_ascending(&engine, Some(Crc::default()), None)
            .unwrap();

        assert_eq!(crc.num_files, 0);
        assert_eq!(crc.table_size_bytes, 0);
    }

    #[tokio::test]
    async fn test_replay_ascending_unsafe_op_makes_stats_indeterminate() {
        let store = Arc::new(InMemory::new());
        write_commit(
            &store,
            0,
            &[
                commit_info_json("WRITE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;
        write_commit(
            &store,
            1,
            &[
                commit_info_json("ANALYZE STATS"),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        let (engine, segment) = build_log_segment(store, &[0, 1], None);
        let crc = segment
            .replay_ascending(&engine, Some(Crc::default()), None)
            .unwrap();

        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
    }

    #[tokio::test]
    async fn test_replay_ascending_remove_with_null_size_makes_stats_untrackable() {
        let store = Arc::new(InMemory::new());
        write_commit(
            &store,
            0,
            &[
                commit_info_json("WRITE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;
        write_commit(
            &store,
            1,
            &[
                commit_info_json("DELETE"),
                remove_json_no_size("file1.parquet"),
            ],
        )
        .await;

        let (engine, segment) = build_log_segment(store, &[0, 1], None);
        let crc = segment
            .replay_ascending(&engine, Some(Crc::default()), None)
            .unwrap();

        assert_eq!(crc.validity, FileStatsValidity::Untrackable);
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
    }

    // ============================================================================================
    // Tests for replay_ascending with after_version filtering.
    // ============================================================================================

    #[tokio::test]
    async fn test_replay_ascending_after_version_skips_earlier_commits() {
        let store = Arc::new(InMemory::new());

        // v0: table creation
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
                add_json("file2.parquet", 200),
            ],
        )
        .await;

        // v1: add a file
        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file3.parquet", 300)],
        )
        .await;

        // v2: add a file
        write_commit(
            &store,
            2,
            &[commit_info_json("WRITE"), add_json("file4.parquet", 400)],
        )
        .await;

        // Seed as if CRC covered v0: 2 files, 300 bytes.
        let seed = super::Crc {
            num_files: 2,
            table_size_bytes: 300,
            num_metadata: 1,
            num_protocol: 1,
            protocol: test_protocol(),
            metadata: test_metadata(),
            ..Default::default()
        };

        // Replay only v1 and v2 (after v0).
        let (engine, segment) = build_log_segment(store, &[0, 1, 2], None);
        let crc = segment
            .replay_ascending(&engine, Some(seed), Some(0))
            .unwrap();

        // v1: +1 file, +300B. v2: +1 file, +400B.
        assert_eq!(crc.num_files, 4); // 2 + 1 + 1
        assert_eq!(crc.table_size_bytes, 1000); // 300 + 300 + 400
        assert_eq!(crc.validity, FileStatsValidity::Valid);
    }

    #[tokio::test]
    async fn test_replay_ascending_after_version_beyond_all_commits_is_noop() {
        let store = Arc::new(InMemory::new());
        write_commit(
            &store,
            0,
            &[
                commit_info_json("WRITE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        let seed = super::Crc {
            num_files: 5,
            table_size_bytes: 500,
            ..Default::default()
        };
        let (engine, segment) = build_log_segment(store, &[0], None);
        let crc = segment
            .replay_ascending(&engine, Some(seed), Some(0))
            .unwrap();

        // No commits after v0, seed returned unchanged.
        assert_eq!(crc.num_files, 5);
        assert_eq!(crc.table_size_bytes, 500);
    }

    // ============================================================================================
    // Tests for forward_crc_replay (the orchestrator).
    // ============================================================================================

    #[tokio::test]
    async fn test_forward_crc_replay_zero_rooted() {
        let store = Arc::new(InMemory::new());

        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file2.parquet", 200)],
        )
        .await;

        // No CRC, no checkpoint -> zero-rooted.
        let (engine, segment) = build_log_segment(store, &[0, 1], None);
        let crc = segment.forward_crc_replay(&engine).unwrap();

        assert_eq!(crc.num_files, 2);
        assert_eq!(crc.table_size_bytes, 300);
        assert_eq!(crc.validity, FileStatsValidity::Valid);
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
    }

    #[tokio::test]
    async fn test_forward_crc_replay_crc_rooted_exact_match() {
        let store = Arc::new(InMemory::new());

        // Write a commit (needed for the log segment) and a CRC at the same version.
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        let seed_crc = super::Crc {
            num_files: 1,
            table_size_bytes: 100,
            num_metadata: 1,
            num_protocol: 1,
            protocol: test_protocol(),
            metadata: test_metadata(),
            ..Default::default()
        };
        write_crc(&store, 0, &seed_crc).await;

        // CRC at target version (v0) -> no replay needed, return CRC directly.
        let (engine, segment) = build_log_segment(store, &[0], Some(0));
        let crc = segment.forward_crc_replay(&engine).unwrap();

        assert_eq!(crc.num_files, 1);
        assert_eq!(crc.table_size_bytes, 100);
        assert_eq!(crc.validity, FileStatsValidity::Valid);
        assert_eq!(crc.protocol, test_protocol());
    }

    #[tokio::test]
    async fn test_forward_crc_replay_crc_rooted_with_commits_after() {
        let store = Arc::new(InMemory::new());

        // v0: table creation
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        // v1: add a file
        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file2.parquet", 200)],
        )
        .await;

        // v2: add a file
        write_commit(
            &store,
            2,
            &[commit_info_json("WRITE"), add_json("file3.parquet", 300)],
        )
        .await;

        // CRC at v0 captures the table at v0.
        let seed_crc = super::Crc {
            num_files: 1,
            table_size_bytes: 100,
            num_metadata: 1,
            num_protocol: 1,
            protocol: test_protocol(),
            metadata: test_metadata(),
            ..Default::default()
        };
        write_crc(&store, 0, &seed_crc).await;

        // Segment covers v0..v2, CRC at v0 -> replay v1 and v2 on top of CRC seed.
        let (engine, segment) = build_log_segment(store, &[0, 1, 2], Some(0));
        let crc = segment.forward_crc_replay(&engine).unwrap();

        assert_eq!(crc.num_files, 3); // 1 (CRC) + 1 (v1) + 1 (v2)
        assert_eq!(crc.table_size_bytes, 600); // 100 (CRC) + 200 (v1) + 300 (v2)
        assert_eq!(crc.validity, FileStatsValidity::Valid);
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
    }

    #[tokio::test]
    async fn test_forward_crc_replay_corrupt_crc_falls_back_to_zero_rooted() {
        let store = Arc::new(InMemory::new());

        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file2.parquet", 200)],
        )
        .await;

        // Write corrupt CRC at v0.
        write_corrupt_crc(&store, 0).await;

        // CRC fails to load -> fall through to zero-rooted (no checkpoint).
        let (engine, segment) = build_log_segment(store, &[0, 1], Some(0));
        let crc = segment.forward_crc_replay(&engine).unwrap();

        assert_eq!(crc.num_files, 2);
        assert_eq!(crc.table_size_bytes, 300);
        assert_eq!(crc.validity, FileStatsValidity::Valid);
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
    }

    // ============================================================================================
    // Tests for replay_ascending with None seed (unseeded).
    // ============================================================================================

    #[tokio::test]
    async fn test_replay_ascending_unseeded_creates_crc_from_first_complete_delta() {
        let store = Arc::new(InMemory::new());

        // v0: table creation with P + M + adds
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
                add_json("file2.parquet", 200),
            ],
        )
        .await;

        // v1: add a file (no P or M)
        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file3.parquet", 300)],
        )
        .await;

        let (engine, segment) = build_log_segment(store, &[0, 1], None);
        let crc = segment.replay_ascending(&engine, None, None).unwrap();

        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
        assert_eq!(crc.num_files, 3); // 2 (v0) + 1 (v1)
        assert_eq!(crc.table_size_bytes, 600); // 300 (v0) + 300 (v1)
        assert_eq!(crc.validity, FileStatsValidity::Valid);
    }

    #[tokio::test]
    async fn test_replay_ascending_unseeded_empty_commits_returns_error() {
        let store = Arc::new(InMemory::new());
        let (engine, segment) = build_log_segment(store, &[0], None);

        let mut segment = segment;
        segment.listed.ascending_commit_files = vec![];

        let result = segment.replay_ascending(&engine, None, None);
        assert!(result.is_err());
    }

    // ============================================================================================
    // Tests for forward_crc_replay with checkpoint-rooted tables.
    // ============================================================================================

    #[tokio::test]
    async fn test_forward_crc_replay_checkpoint_rooted_overrides_validity() {
        let store = Arc::new(InMemory::new());

        // v0: table creation with P + M + adds
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        write_commit(
            &store,
            1,
            &[commit_info_json("WRITE"), add_json("file2.parquet", 200)],
        )
        .await;

        // Build segment with a fake checkpoint part to simulate checkpoint-rooted.
        let (engine, mut segment) = build_log_segment(store, &[0, 1], None);
        let checkpoint_path = {
            use crate::path::ParsedLogPath;
            use crate::FileMeta;
            let url = segment
                .log_root
                .join("00000000000000000000.checkpoint.parquet")
                .unwrap();
            ParsedLogPath::try_from(FileMeta::new(url, 0, 0))
                .unwrap()
                .unwrap()
        };
        segment.listed.checkpoint_parts = vec![checkpoint_path];

        let crc = segment.forward_crc_replay(&engine).unwrap();

        // P&M come from commit replay, but validity is overridden for checkpoint-rooted.
        assert_eq!(crc.protocol, test_protocol());
        assert_eq!(crc.metadata, test_metadata());
        assert_eq!(crc.validity, FileStatsValidity::RequiresCheckpointRead);
    }

    #[tokio::test]
    async fn test_forward_crc_replay_checkpoint_rooted_preserves_worse_validity() {
        let store = Arc::new(InMemory::new());

        // v0: table creation
        write_commit(
            &store,
            0,
            &[
                commit_info_json("CREATE TABLE"),
                protocol_json(&test_protocol()),
                metadata_json(&test_metadata()),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        // v1: unsafe op degrades to Indeterminate
        write_commit(
            &store,
            1,
            &[
                commit_info_json("ANALYZE STATS"),
                add_json("file1.parquet", 100),
            ],
        )
        .await;

        let (engine, mut segment) = build_log_segment(store, &[0, 1], None);
        let checkpoint_path = {
            use crate::path::ParsedLogPath;
            use crate::FileMeta;
            let url = segment
                .log_root
                .join("00000000000000000000.checkpoint.parquet")
                .unwrap();
            ParsedLogPath::try_from(FileMeta::new(url, 0, 0))
                .unwrap()
                .unwrap()
        };
        segment.listed.checkpoint_parts = vec![checkpoint_path];

        let crc = segment.forward_crc_replay(&engine).unwrap();

        // Replay degraded to Indeterminate, which is worse than RequiresCheckpointRead.
        // The orchestrator should NOT override it back to RequiresCheckpointRead.
        assert_eq!(crc.validity, FileStatsValidity::Indeterminate);
    }
}
