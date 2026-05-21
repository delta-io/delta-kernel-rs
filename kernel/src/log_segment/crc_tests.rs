//! Tests for P&M replay and ICT reads with CRC files.
//!
//! Each test sets up an in-memory Delta log with V2 checkpoint JSONs, commit files, and CRC files,
//! then verifies that Protocol & Metadata loading and ICT reads resolve correctly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::json;
use test_utils::{assert_result_error_with_message, delta_path_for_version};
use url::Url;

use super::LogSegment;
use crate::actions::{CommitInfo, Format, Metadata, Protocol};
use crate::crc::{try_read_crc_file, Crc, FileSizeHistogram};
use crate::engine::sync::SyncEngine;
use crate::object_store::memory::InMemory;
use crate::object_store::ObjectStoreExt as _;
use crate::path::ParsedLogPath;
use crate::{DeltaResult, Engine, Snapshot};

// ============================================================================
// Expected values
// ============================================================================

const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

pub(super) fn protocol_v2() -> Protocol {
    Protocol::try_new_modern(["v2Checkpoint"], ["v2Checkpoint"]).unwrap()
}

pub(super) fn protocol_v2_dv() -> Protocol {
    Protocol::try_new_modern(
        ["v2Checkpoint", "deletionVectors"],
        ["v2Checkpoint", "deletionVectors"],
    )
    .unwrap()
}

pub(super) fn protocol_v2_dv_ntz() -> Protocol {
    Protocol::try_new_modern(
        ["v2Checkpoint", "deletionVectors", "timestampNtz"],
        ["v2Checkpoint", "deletionVectors", "timestampNtz"],
    )
    .unwrap()
}

pub(super) fn protocol_v2_ict() -> Protocol {
    Protocol::try_new(
        3,
        7,
        Some(["v2Checkpoint"]),
        Some(["v2Checkpoint", "inCommitTimestamp"]),
    )
    .unwrap()
}

pub(super) fn metadata_a() -> Metadata {
    Metadata::new_unchecked(
        "aaa",
        None,
        None,
        Format::default(),
        SCHEMA_STRING,
        vec![],
        Some(1587968585495),
        HashMap::new(),
    )
}

pub(super) fn metadata_b() -> Metadata {
    Metadata::new_unchecked(
        "bbb",
        None,
        None,
        Format::default(),
        SCHEMA_STRING,
        vec![],
        Some(1587968585495),
        HashMap::new(),
    )
}

pub(super) fn metadata_ict() -> Metadata {
    Metadata::new_unchecked(
        "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
        None,
        None,
        Format::default(),
        SCHEMA_STRING,
        vec![],
        Some(1587968585495),
        HashMap::from([(
            "delta.enableInCommitTimestamps".to_string(),
            "true".to_string(),
        )]),
    )
}

fn commit_info() -> CommitInfo {
    CommitInfo {
        timestamp: Some(1587968586154),
        operation: Some("WRITE".to_string()),
        ..Default::default()
    }
}

// ============================================================================
// JSON builders
// ============================================================================

const V2_CKPT_SUFFIX: &str = "checkpoint.00000000-0000-0000-0000-000000000000.json";

fn protocol_action_json(protocol: &Protocol) -> serde_json::Value {
    json!({"protocol": serde_json::to_value(protocol).unwrap()})
}

fn metadata_action_json(metadata: &Metadata) -> serde_json::Value {
    json!({"metaData": serde_json::to_value(metadata).unwrap()})
}

fn commit_info_json() -> serde_json::Value {
    json!({"commitInfo": serde_json::to_value(commit_info()).unwrap()})
}

fn commit_info_json_with_ict(ict: i64) -> serde_json::Value {
    json!({"commitInfo": {
        "timestamp": 1587968586154i64,
        "operation": "WRITE",
        "inCommitTimestamp": ict,
    }})
}

fn crc_json(
    protocol: &Protocol,
    metadata: &Metadata,
    ict: Option<i64>,
    file_sizes: &[i64],
) -> serde_json::Value {
    let total_bytes: i64 = file_sizes.iter().sum();
    let num_files = file_sizes.len() as i64;
    let mut v = json!({
        "tableSizeBytes": total_bytes,
        "numFiles": num_files,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": serde_json::to_value(metadata).unwrap(),
        "protocol": serde_json::to_value(protocol).unwrap(),
    });
    if !file_sizes.is_empty() {
        let mut hist = FileSizeHistogram::create_default();
        for &s in file_sizes {
            hist.insert(s).unwrap();
        }
        v["fileSizeHistogram"] = serde_json::to_value(&hist).unwrap();
    }
    if let Some(ict) = ict {
        v["inCommitTimestampOpt"] = json!(ict);
    }
    v
}

fn add_action_json(path: &str, size: i64) -> serde_json::Value {
    json!({"add": {
        "path": path, "size": size, "partitionValues": {},
        "modificationTime": 0, "dataChange": true,
    }})
}

fn remove_action_json(path: &str, size: Option<i64>) -> serde_json::Value {
    match size {
        Some(s) => json!({"remove": {
            "path": path, "size": s,
            "dataChange": true, "deletionTimestamp": 0,
        }}),
        None => json!({"remove": {
            "path": path,
            "dataChange": true, "deletionTimestamp": 0,
        }}),
    }
}

fn domain_metadata_action_json(
    domain: &str,
    configuration: &str,
    removed: bool,
) -> serde_json::Value {
    json!({"domainMetadata": {
        "domain": domain, "configuration": configuration, "removed": removed,
    }})
}

fn set_transaction_action_json(
    app_id: &str,
    txn_version: i64,
    last_updated: Option<i64>,
) -> serde_json::Value {
    let mut v = json!({"txn": {"appId": app_id, "version": txn_version}});
    if let Some(lu) = last_updated {
        v["txn"]["lastUpdated"] = json!(lu);
    }
    v
}

fn commit_info_json_with_op(operation: &str) -> serde_json::Value {
    json!({"commitInfo": {"timestamp": 1587968586154i64, "operation": operation}})
}

// ============================================================================
// CrcReadTest builder
// ============================================================================

/// Operations that can be applied to an in-memory Delta log.
pub(super) enum Op {
    V2Checkpoint {
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
    },
    Delta(u64),
    DeltaWithPM {
        version: u64,
        protocol: Option<Protocol>,
        metadata: Option<Metadata>,
    },
    DeltaWithIct {
        version: u64,
        ict: i64,
    },
    DeltaWithAdd {
        version: u64,
        path: String,
        size: i64,
    },
    DeltaWithAddAndOperation {
        version: u64,
        path: String,
        size: i64,
        operation: String,
    },
    DeltaWithRemove {
        version: u64,
        path: String,
        size: Option<i64>,
    },
    DeltaWithDomainMetadata {
        version: u64,
        domain: String,
        configuration: String,
        removed: bool,
    },
    DeltaWithTxn {
        version: u64,
        app_id: String,
        txn_version: i64,
        last_updated: Option<i64>,
    },
    Crc {
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
        ict: Option<i64>,
        file_sizes: Vec<i64>,
    },
    CorruptCrc(u64),
}

/// Declarative test builder: accumulate log operations, then build and assert.
pub(super) struct CrcReadTest {
    ops: Vec<Op>,
}

impl CrcReadTest {
    pub(super) fn new() -> Self {
        Self { ops: vec![] }
    }

    /// Write a V2 checkpoint at the given version.
    pub(super) fn v2_checkpoint(
        mut self,
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
    ) -> Self {
        self.ops.push(Op::V2Checkpoint {
            version,
            protocol,
            metadata,
        });
        self
    }

    /// Write a plain delta (commitInfo only, no protocol or metadata).
    pub(super) fn delta(mut self, version: u64) -> Self {
        self.ops.push(Op::Delta(version));
        self
    }

    /// Write a delta with optional protocol and/or metadata overrides.
    pub(super) fn delta_with_p_m(
        mut self,
        version: u64,
        protocol: impl Into<Option<Protocol>>,
        metadata: impl Into<Option<Metadata>>,
    ) -> Self {
        self.ops.push(Op::DeltaWithPM {
            version,
            protocol: protocol.into(),
            metadata: metadata.into(),
        });
        self
    }

    /// Write a delta with an in-commit timestamp in commitInfo.
    pub(super) fn delta_with_ict(mut self, version: u64, ict: i64) -> Self {
        self.ops.push(Op::DeltaWithIct { version, ict });
        self
    }

    /// Write a delta containing an `add` action.
    pub(super) fn delta_with_add(mut self, version: u64, path: &str, size: i64) -> Self {
        self.ops.push(Op::DeltaWithAdd {
            version,
            path: path.to_string(),
            size,
        });
        self
    }

    /// Write a delta containing an `add` action AND a commitInfo with the given operation
    /// (e.g., `"ANALYZE STATS"`). Single commit, two action rows.
    pub(super) fn delta_with_add_and_op(
        mut self,
        version: u64,
        path: &str,
        size: i64,
        operation: &str,
    ) -> Self {
        self.ops.push(Op::DeltaWithAddAndOperation {
            version,
            path: path.to_string(),
            size,
            operation: operation.to_string(),
        });
        self
    }

    /// Write a delta containing a `remove` action. `size = None` omits the field on disk.
    pub(super) fn delta_with_remove(mut self, version: u64, path: &str, size: Option<i64>) -> Self {
        self.ops.push(Op::DeltaWithRemove {
            version,
            path: path.to_string(),
            size,
        });
        self
    }

    /// Write a delta containing a `domainMetadata` action.
    pub(super) fn delta_with_dm(
        mut self,
        version: u64,
        domain: &str,
        configuration: &str,
        removed: bool,
    ) -> Self {
        self.ops.push(Op::DeltaWithDomainMetadata {
            version,
            domain: domain.to_string(),
            configuration: configuration.to_string(),
            removed,
        });
        self
    }

    /// Write a delta containing a `txn` action.
    pub(super) fn delta_with_txn(
        mut self,
        version: u64,
        app_id: &str,
        txn_version: i64,
        last_updated: Option<i64>,
    ) -> Self {
        self.ops.push(Op::DeltaWithTxn {
            version,
            app_id: app_id.to_string(),
            txn_version,
            last_updated,
        });
        self
    }

    /// Write a CRC file with the given protocol, metadata, and optional ICT. No file stats.
    pub(super) fn crc(
        mut self,
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
        ict: impl Into<Option<i64>>,
    ) -> Self {
        self.ops.push(Op::Crc {
            version,
            protocol,
            metadata,
            ict: ict.into(),
            file_sizes: vec![],
        });
        self
    }

    /// Write a CRC file with a real on-disk `FileSizeHistogram` computed from `file_sizes`.
    pub(super) fn crc_with_files(
        mut self,
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
        ict: impl Into<Option<i64>>,
        file_sizes: &[i64],
    ) -> Self {
        self.ops.push(Op::Crc {
            version,
            protocol,
            metadata,
            ict: ict.into(),
            file_sizes: file_sizes.to_vec(),
        });
        self
    }

    /// Write a corrupt CRC file.
    pub(super) fn corrupt_crc(mut self, version: u64) -> Self {
        self.ops.push(Op::CorruptCrc(version));
        self
    }

    /// Execute all operations, returning a built test that can be asserted against.
    pub(super) async fn build(self) -> BuiltCrcTest {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///").unwrap();
        let engine = SyncEngine::new_with_store(store.clone());

        // Catch the footgun where two ops resolve to the same `(version, suffix)` on disk,
        // because `put` is last-writer-wins and would silently clobber the earlier write.
        let mut seen: HashSet<(u64, &'static str)> = HashSet::new();
        let mut claim = |v: u64, suffix: &'static str| {
            assert!(
                seen.insert((v, suffix)),
                "CrcReadTest: two ops both write to version={v} suffix={suffix:?}; \
                 the later one silently overwrites the earlier one"
            );
        };

        for op in self.ops {
            match op {
                Op::V2Checkpoint {
                    version: v,
                    ref protocol,
                    ref metadata,
                } => {
                    claim(v, V2_CKPT_SUFFIX);
                    let content = format!(
                        "{}\n{}\n{}",
                        protocol_action_json(protocol),
                        metadata_action_json(metadata),
                        json!({"checkpointMetadata": {"version": 2}})
                    );
                    put(&store, v, V2_CKPT_SUFFIX, &content).await;
                }
                Op::Delta(v) => {
                    claim(v, "json");
                    put(&store, v, "json", &commit_info_json().to_string()).await;
                }
                Op::DeltaWithPM {
                    version: v,
                    protocol,
                    metadata,
                } => {
                    claim(v, "json");
                    let mut lines = vec![commit_info_json().to_string()];
                    if let Some(ref p) = protocol {
                        lines.push(protocol_action_json(p).to_string());
                    }
                    if let Some(ref m) = metadata {
                        lines.push(metadata_action_json(m).to_string());
                    }
                    put(&store, v, "json", &lines.join("\n")).await;
                }
                Op::DeltaWithIct { version: v, ict } => {
                    claim(v, "json");
                    put(
                        &store,
                        v,
                        "json",
                        &commit_info_json_with_ict(ict).to_string(),
                    )
                    .await;
                }
                Op::DeltaWithAdd {
                    version: v,
                    ref path,
                    size,
                } => {
                    claim(v, "json");
                    let content =
                        format!("{}\n{}", add_action_json(path, size), commit_info_json());
                    put(&store, v, "json", &content).await;
                }
                Op::DeltaWithAddAndOperation {
                    version: v,
                    ref path,
                    size,
                    ref operation,
                } => {
                    claim(v, "json");
                    let content = format!(
                        "{}\n{}",
                        add_action_json(path, size),
                        commit_info_json_with_op(operation)
                    );
                    put(&store, v, "json", &content).await;
                }
                Op::DeltaWithRemove {
                    version: v,
                    ref path,
                    size,
                } => {
                    claim(v, "json");
                    let content =
                        format!("{}\n{}", remove_action_json(path, size), commit_info_json());
                    put(&store, v, "json", &content).await;
                }
                Op::DeltaWithDomainMetadata {
                    version: v,
                    ref domain,
                    ref configuration,
                    removed,
                } => {
                    claim(v, "json");
                    let content = format!(
                        "{}\n{}",
                        domain_metadata_action_json(domain, configuration, removed),
                        commit_info_json()
                    );
                    put(&store, v, "json", &content).await;
                }
                Op::DeltaWithTxn {
                    version: v,
                    ref app_id,
                    txn_version,
                    last_updated,
                } => {
                    claim(v, "json");
                    // No commitInfo: an ICT replay test exercises "newest commit has no
                    // commitInfo" via a txn-only commit.
                    let content =
                        set_transaction_action_json(app_id, txn_version, last_updated).to_string();
                    put(&store, v, "json", &content).await;
                }
                Op::Crc {
                    version: v,
                    ref protocol,
                    ref metadata,
                    ict,
                    ref file_sizes,
                } => {
                    claim(v, "crc");
                    put(
                        &store,
                        v,
                        "crc",
                        &crc_json(protocol, metadata, ict, file_sizes).to_string(),
                    )
                    .await;
                }
                Op::CorruptCrc(v) => {
                    claim(v, "crc");
                    put(&store, v, "crc", "CORRUPT_CRC_DATA").await;
                }
            }
        }

        BuiltCrcTest { engine, url }
    }
}

pub(super) struct BuiltCrcTest {
    pub(super) engine: SyncEngine,
    pub(super) url: Url,
}

impl BuiltCrcTest {
    /// Construct a `LogSegment` directly from the store state (no `Snapshot`) and run
    /// `build_crc_from_stale` against `base`. `target_version = None` means latest.
    pub(super) fn incrementally_build_crc(
        &self,
        base: &Crc,
        target_version: impl Into<Option<u64>>,
    ) -> DeltaResult<Crc> {
        let storage = self.engine.storage_handler();
        let log_root = self.url.join("_delta_log/").unwrap();
        let log_segment = LogSegment::for_snapshot_impl(
            storage.as_ref(),
            log_root,
            vec![],
            None,
            target_version.into(),
        )?;
        log_segment.build_crc_from_stale(&self.engine, base)
    }

    /// Load the on-disk CRC at `crc_version` and use it as the replay base.
    pub(super) fn incrementally_build_crc_from_disk_crc_at(
        &self,
        crc_version: u64,
        target_version: impl Into<Option<u64>>,
    ) -> DeltaResult<Crc> {
        let crc_path = ParsedLogPath::create_parsed_crc(&self.url, crc_version);
        let base = try_read_crc_file(&self.engine, &crc_path)?;
        self.incrementally_build_crc(&base, target_version)
    }

    fn assert_p_m(
        &self,
        version: impl Into<Option<u64>>,
        expected_protocol: &Protocol,
        expected_metadata: &Metadata,
    ) {
        let version = version.into();
        let mut builder = Snapshot::builder_for(self.url.clone());
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        let snapshot = builder.build(&self.engine).unwrap();
        let table_config = snapshot.table_configuration();

        let version_label = version.map_or("latest".to_string(), |v| format!("v{v}"));
        assert_eq!(
            table_config.protocol(),
            expected_protocol,
            "Protocol mismatch at {version_label}"
        );

        assert_eq!(
            table_config.metadata(),
            expected_metadata,
            "Metadata mismatch at {version_label}"
        );
    }

    fn assert_ict(&self, version: impl Into<Option<u64>>, expected_ict: Option<i64>) {
        let version = version.into();
        let mut builder = Snapshot::builder_for(self.url.clone());
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        let snapshot = builder.build(&self.engine).unwrap();
        let ict = snapshot.get_in_commit_timestamp(&self.engine).unwrap();

        let version_label = version.map_or("latest".to_string(), |v| format!("v{v}"));
        assert_eq!(ict, expected_ict, "ICT mismatch at {version_label}");
    }
}

async fn put(store: &InMemory, version: u64, suffix: &str, content: &str) {
    store
        .put(
            &delta_path_for_version(version, suffix),
            content.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
}

// TODO: Time travel tests
// TODO: Log compaction tests
// TODO: build_from tests
// TODO: _last_checkpoint tests

// ============================================================================
// Tests: Baseline (no CRC)
// ============================================================================

#[tokio::test]
async fn test_get_p_m_from_delta_no_checkpoint() {
    CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .delta(1)
        .delta(2)
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_get_p_and_m_from_different_deltas() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta_with_p_m(1, protocol_v2_dv(), None) // <-- P from here
        .delta_with_p_m(2, None, metadata_b()) // <-- M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_m_from_checkpoint() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .delta(1)
        .delta(2)
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_get_p_m_from_delta_after_checkpoint() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta_with_p_m(1, protocol_v2_dv(), metadata_b()) // <-- P & M from here
        .delta(2)
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

// ============================================================================
// Tests: CRC at target version
// ============================================================================

#[tokio::test]
async fn test_get_p_m_from_crc_at_target() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .delta(2)
        .crc(2, protocol_v2_dv(), metadata_b(), None) // <-- P & M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_crc_preferred_over_delta_at_target() {
    // The P & M for the 002.crc and 002.json should NOT be different in practice.
    // We only do this for this test so we can differentiate which P & M is used.
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .delta_with_p_m(2, protocol_v2_dv(), metadata_a())
        .crc(2, protocol_v2_dv_ntz(), metadata_b(), None) // <-- P & M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv_ntz(), &metadata_b());
}

#[tokio::test]
async fn test_corrupt_crc_at_target_falls_back() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .delta(1)
        .delta(2)
        .corrupt_crc(2) // <-- Corrupt! Fall back to replay.
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_crc_wins_over_checkpoint() {
    // The P & M for the 002.crc and the v2 checkpoint should NOT be different in practice.
    // We only do this for this test so we can differentiate which P & M is used.
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .delta(2)
        .v2_checkpoint(2, protocol_v2(), metadata_a())
        .crc(2, protocol_v2_dv(), metadata_b(), None) // <-- P & M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_checkpoint_on_corrupt_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .delta(2)
        .v2_checkpoint(2, protocol_v2(), metadata_a()) // <-- P & M from here
        .corrupt_crc(2) // <-- Corrupt! Fall back to replay.
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

// ============================================================================
// Tests: CRC at version < target
// ============================================================================

#[tokio::test]
async fn test_crc_at_earlier_version() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P & M from here
        .delta(2)
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- M from here
        .delta_with_p_m(2, protocol_v2_dv_ntz(), None) // <-- P from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv_ntz(), &metadata_b());
}

#[tokio::test]
async fn test_get_m_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .delta(1)
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P from here
        .delta_with_p_m(2, None, metadata_a()) // <-- M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_a());
}

#[tokio::test]
async fn test_corrupt_crc_at_non_target_version_falls_back() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .delta(1)
        .corrupt_crc(1) // <-- Corrupt! Fall back to replay.
        .delta(2)
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_crc_before_checkpoint_is_ignored() {
    CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta(1)
        .crc(1, protocol_v2_dv_ntz(), metadata_b(), None)
        .v2_checkpoint(2, protocol_v2_dv(), metadata_a()) // <-- P & M from here
        .delta(3)
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_a());
}

// ============================================================================
// Tests: ICT from CRC
// ============================================================================

#[tokio::test]
async fn test_ict_from_crc_at_snapshot_version() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2_ict(), metadata_ict())
        .delta_with_ict(1, 2000)
        .crc(1, protocol_v2_ict(), metadata_ict(), 1000) // <-- ICT from here
        .build()
        .await
        .assert_ict(None, Some(1000));
}

#[tokio::test]
async fn test_ict_errors_when_crc_has_no_ict() {
    let setup = CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2_ict(), metadata_ict())
        .delta_with_ict(1, 2000)
        .crc(1, protocol_v2_ict(), metadata_ict(), None)
        .build()
        .await;

    let snapshot = Snapshot::builder_for(setup.url.clone())
        .build(&setup.engine)
        .unwrap();

    let result = snapshot.get_in_commit_timestamp(&setup.engine);

    assert_result_error_with_message(
        result,
        "In-Commit Timestamp not found in CRC file at version 1",
    );
}
