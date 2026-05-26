//! Tests for P&M replay and ICT reads with CRC files.
//!
//! Each test sets up an in-memory Delta log with V2 checkpoint JSONs, commit files, and CRC files,
//! then verifies that Protocol & Metadata loading and ICT reads resolve correctly.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use test_utils::{assert_result_error_with_message, delta_path_for_version};
use url::Url;

use crate::actions::{Format, Metadata, Protocol};
use crate::engine::sync::SyncEngine;
use crate::object_store::memory::InMemory;
use crate::object_store::ObjectStoreExt as _;
use crate::Snapshot;

// ============================================================================
// Expected values
// ============================================================================

const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

const COMMIT_INFO_TIMESTAMP: i64 = 1587968586154;
const DEFAULT_OPERATION: &str = "WRITE";

fn protocol_v2() -> Protocol {
    Protocol::try_new_modern(["v2Checkpoint"], ["v2Checkpoint"]).unwrap()
}

fn protocol_v2_dv() -> Protocol {
    Protocol::try_new_modern(
        ["v2Checkpoint", "deletionVectors"],
        ["v2Checkpoint", "deletionVectors"],
    )
    .unwrap()
}

fn protocol_v2_dv_ntz() -> Protocol {
    Protocol::try_new_modern(
        ["v2Checkpoint", "deletionVectors", "timestampNtz"],
        ["v2Checkpoint", "deletionVectors", "timestampNtz"],
    )
    .unwrap()
}

fn protocol_v2_ict() -> Protocol {
    Protocol::try_new(
        3,
        7,
        Some(["v2Checkpoint"]),
        Some(["v2Checkpoint", "inCommitTimestamp"]),
    )
    .unwrap()
}

fn metadata_a() -> Metadata {
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

fn metadata_b() -> Metadata {
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

fn metadata_ict() -> Metadata {
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

// ============================================================================
// Action JSON helpers
// ============================================================================

/// Default commitInfo: operation `"WRITE"`, no ICT.
fn commit_info() -> serde_json::Value {
    json!({"commitInfo": {"timestamp": COMMIT_INFO_TIMESTAMP, "operation": DEFAULT_OPERATION}})
}

/// commitInfo with the default `"WRITE"` operation and an in-commit timestamp.
fn commit_info_with_ict(ict: i64) -> serde_json::Value {
    json!({"commitInfo": {
        "timestamp": COMMIT_INFO_TIMESTAMP,
        "operation": DEFAULT_OPERATION,
        "inCommitTimestamp": ict,
    }})
}

fn protocol(p: Protocol) -> serde_json::Value {
    json!({"protocol": serde_json::to_value(&p).unwrap()})
}

fn metadata(m: Metadata) -> serde_json::Value {
    json!({"metaData": serde_json::to_value(&m).unwrap()})
}

#[allow(dead_code)]
fn add(path: &str, size: i64) -> serde_json::Value {
    json!({"add": {
        "path": path,
        "size": size,
        "partitionValues": {},
        "modificationTime": 0,
        "dataChange": true,
    }})
}

#[allow(dead_code)]
fn remove(path: &str, size: Option<i64>) -> serde_json::Value {
    let mut v = json!({"remove": {
        "path": path,
        "dataChange": true,
        "deletionTimestamp": 0,
    }});
    if let Some(s) = size {
        v["remove"]["size"] = json!(s);
    }
    v
}

#[allow(dead_code)]
fn dm(domain: &str, configuration: &str, removed: bool) -> serde_json::Value {
    json!({"domainMetadata": {
        "domain": domain,
        "configuration": configuration,
        "removed": removed,
    }})
}

#[allow(dead_code)]
fn set_txn(app_id: &str, version: i64, last_updated: Option<i64>) -> serde_json::Value {
    let mut v = json!({"txn": {"appId": app_id, "version": version}});
    if let Some(lu) = last_updated {
        v["txn"]["lastUpdated"] = json!(lu);
    }
    v
}

fn crc_json(protocol: &Protocol, metadata: &Metadata, ict: Option<i64>) -> serde_json::Value {
    let mut v = json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": serde_json::to_value(metadata).unwrap(),
        "protocol": serde_json::to_value(protocol).unwrap(),
    });
    if let Some(ict) = ict {
        v["inCommitTimestampOpt"] = json!(ict);
    }
    v
}

// ============================================================================
// CrcReadTest builder
// ============================================================================

/// Operations that can be applied to an in-memory Delta log.
enum Op {
    V2Checkpoint {
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
    },
    Commit {
        version: u64,
        actions: Vec<serde_json::Value>,
    },
    Crc {
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
        ict: Option<i64>,
    },
    CorruptCrc {
        version: u64,
    },
}

/// Declarative test builder: accumulate log operations, then build and assert.
struct CrcReadTest {
    ops: Vec<Op>,
}

impl CrcReadTest {
    fn new() -> Self {
        Self { ops: vec![] }
    }

    fn v2_checkpoint(mut self, version: u64, protocol: Protocol, metadata: Metadata) -> Self {
        self.ops.push(Op::V2Checkpoint {
            version,
            protocol,
            metadata,
        });
        self
    }

    /// Write a commit file containing exactly the given actions, in order.
    fn commit(
        mut self,
        version: u64,
        actions: impl IntoIterator<Item = serde_json::Value>,
    ) -> Self {
        let actions: Vec<serde_json::Value> = actions.into_iter().collect();
        self.ops.push(Op::Commit { version, actions });
        self
    }

    fn crc(
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
        });
        self
    }

    fn corrupt_crc(mut self, version: u64) -> Self {
        self.ops.push(Op::CorruptCrc { version });
        self
    }

    /// Execute all operations, returning a built test that can be asserted against.
    async fn build(self) -> BuiltCrcTest {
        Self::validate_ops(&self.ops);

        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///").unwrap();
        let engine = SyncEngine::new_with_store(store.clone());

        for op in self.ops {
            match op {
                Op::Commit { version, actions } => {
                    let content = actions
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join("\n");
                    put(&store, version, "json", &content).await;
                }
                Op::Crc {
                    version,
                    ref protocol,
                    ref metadata,
                    ict,
                } => {
                    put(
                        &store,
                        version,
                        "crc",
                        &crc_json(protocol, metadata, ict).to_string(),
                    )
                    .await;
                }
                Op::CorruptCrc { version } => {
                    put(&store, version, "crc", "CORRUPT_CRC_DATA").await;
                }
                Op::V2Checkpoint {
                    version,
                    protocol: p,
                    metadata: m,
                } => {
                    const V2_CKPT_SUFFIX: &str =
                        "checkpoint.00000000-0000-0000-0000-000000000000.json";
                    let content = format!(
                        "{}\n{}\n{}",
                        protocol(p),
                        metadata(m),
                        json!({"checkpointMetadata": {"version": 2}})
                    );
                    put(&store, version, V2_CKPT_SUFFIX, &content).await;
                }
            }
        }

        BuiltCrcTest { engine, url }
    }

    fn validate_ops(ops: &[Op]) {
        let mut prev: Option<u64> = None;
        for op in ops {
            if let Op::Commit { version, actions } = op {
                assert!(
                    !actions.is_empty(),
                    "commit(v{version}, []): a commit must contain at least one action",
                );
                if let Some(p) = prev {
                    assert!(
                        *version > p,
                        "commit(v{version}): commits must be added in strictly increasing \
                         version order (previous commit was at v{p})",
                    );
                }
                prev = Some(*version);
            }
        }
    }
}

struct BuiltCrcTest {
    engine: SyncEngine,
    url: Url,
}

impl BuiltCrcTest {
    /// Build a snapshot at `version` (or latest if `None`) and return it
    /// alongside a human-readable label like `"v2"` or `"latest"`.
    fn snapshot_at(&self, version: Option<u64>) -> (crate::snapshot::SnapshotRef, String) {
        let mut builder = Snapshot::builder_for(self.url.clone());
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        let snapshot = builder.build(&self.engine).unwrap();
        let label = version.map_or("latest".to_string(), |v| format!("v{v}"));
        (snapshot, label)
    }

    fn assert_p_m(
        &self,
        version: impl Into<Option<u64>>,
        expected_protocol: &Protocol,
        expected_metadata: &Metadata,
    ) {
        let (snapshot, label) = self.snapshot_at(version.into());
        let table_config = snapshot.table_configuration();
        assert_eq!(
            table_config.protocol(),
            expected_protocol,
            "Protocol mismatch at {label}"
        );
        assert_eq!(
            table_config.metadata(),
            expected_metadata,
            "Metadata mismatch at {label}"
        );
    }

    fn assert_ict(&self, version: impl Into<Option<u64>>, expected_ict: Option<i64>) {
        let (snapshot, label) = self.snapshot_at(version.into());
        let ict = snapshot.get_in_commit_timestamp(&self.engine).unwrap();
        assert_eq!(ict, expected_ict, "ICT mismatch at {label}");
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
        .commit(
            0, // <-- P & M from here
            [
                commit_info(),
                protocol(protocol_v2()),
                metadata(metadata_a()),
            ],
        )
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_get_p_and_m_from_different_deltas() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info(), protocol(protocol_v2_dv())]) // <-- P from here
        .commit(2, [commit_info(), metadata(metadata_b())]) // <-- M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_m_from_checkpoint() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_get_p_m_from_delta_after_checkpoint() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(
            1, // <-- P & M from here
            [
                commit_info(),
                protocol(protocol_v2_dv()),
                metadata(metadata_b()),
            ],
        )
        .commit(2, [commit_info()])
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
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
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
        .commit(1, [commit_info()])
        .commit(
            2,
            [
                commit_info(),
                protocol(protocol_v2_dv()),
                metadata(metadata_a()),
            ],
        )
        .crc(2, protocol_v2_dv_ntz(), metadata_b(), None) // <-- P & M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv_ntz(), &metadata_b());
}

#[tokio::test]
async fn test_corrupt_crc_at_target_falls_back() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
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
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
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
        .commit(1, [commit_info()])
        .commit(2, [commit_info()])
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
        .commit(1, [commit_info()])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P & M from here
        .commit(2, [commit_info()])
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info()])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- M from here
        .commit(2, [commit_info(), protocol(protocol_v2_dv_ntz())]) // <-- P from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv_ntz(), &metadata_b());
}

#[tokio::test]
async fn test_get_m_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info()])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P from here
        .commit(2, [commit_info(), metadata(metadata_a())]) // <-- M from here
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_a());
}

#[tokio::test]
async fn test_corrupt_crc_at_non_target_version_falls_back() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .commit(1, [commit_info()])
        .corrupt_crc(1) // <-- Corrupt! Fall back to replay.
        .commit(2, [commit_info()])
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_crc_before_checkpoint_is_ignored() {
    CrcReadTest::new()
        .commit(
            0,
            [
                commit_info(),
                protocol(protocol_v2()),
                metadata(metadata_a()),
            ],
        )
        .commit(1, [commit_info()])
        .crc(1, protocol_v2_dv_ntz(), metadata_b(), None)
        .v2_checkpoint(2, protocol_v2_dv(), metadata_a()) // <-- P & M from here
        .commit(3, [commit_info()])
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
        .commit(1, [commit_info_with_ict(2000)])
        .crc(1, protocol_v2_ict(), metadata_ict(), 1000) // <-- ICT from here
        .build()
        .await
        .assert_ict(None, Some(1000));
}

#[tokio::test]
async fn test_ict_errors_when_crc_has_no_ict() {
    let setup = CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2_ict(), metadata_ict())
        .commit(1, [commit_info_with_ict(2000)])
        .crc(1, protocol_v2_ict(), metadata_ict(), None)
        .build()
        .await;

    let (snapshot, _) = setup.snapshot_at(None);
    let result = snapshot.get_in_commit_timestamp(&setup.engine);

    assert_result_error_with_message(
        result,
        "In-Commit Timestamp not found in CRC file at version 1",
    );
}

// ============================================================================
// Tests: CrcReadTest framework itself
// ============================================================================

/// Build a `CrcReadTest` containing one commit per version, in the given order.
async fn build_with_commit_versions(versions: &[u64]) -> BuiltCrcTest {
    let mut t = CrcReadTest::new();
    for &v in versions {
        t = t.commit(v, [commit_info()]);
    }
    t.build().await
}

#[rstest::rstest]
#[case::single(&[0])]
#[case::consecutive(&[0, 1, 2])]
#[case::with_gaps(&[0, 5, 88, 1000])]
#[tokio::test]
async fn test_commit_versions_strictly_increasing_succeeds(#[case] versions: &[u64]) {
    build_with_commit_versions(versions).await;
}

#[rstest::rstest]
#[case::immediate_duplicate(&[0, 0])]
#[case::later_duplicate(&[0, 1, 1])]
#[case::immediate_decrease(&[5, 3])]
#[case::later_decrease(&[0, 5, 4])]
#[case::decrease_to_zero(&[2, 0])]
#[tokio::test]
#[should_panic(expected = "strictly increasing")]
async fn test_commit_versions_not_increasing_panics(#[case] versions: &[u64]) {
    build_with_commit_versions(versions).await;
}

#[tokio::test]
#[should_panic(expected = "at least one action")]
async fn test_commit_empty_actions_panics() {
    CrcReadTest::new().commit(0, []).build().await;
}
