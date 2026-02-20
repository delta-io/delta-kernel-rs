//! Tests for P&M replay with CRC files.
//!
//! Each test sets up an in-memory Delta log with V2 checkpoint JSONs, commit files, and CRC files,
//! then verifies that Protocol & Metadata loading resolves correctly.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use serde_json::json;
use url::Url;

use crate::actions::{CommitInfo, Format, Metadata, Protocol};
use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::{DefaultEngine, DefaultEngineBuilder};
use crate::Snapshot;

use test_utils::delta_path_for_version;

// ============================================================================
// Expected values
// ============================================================================

const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

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

fn crc_json(protocol: &Protocol, metadata: &Metadata) -> serde_json::Value {
    json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": serde_json::to_value(metadata).unwrap(),
        "protocol": serde_json::to_value(protocol).unwrap(),
    })
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
    Delta(u64),
    DeltaWithPM {
        version: u64,
        protocol: Option<Protocol>,
        metadata: Option<Metadata>,
    },
    Crc {
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
    },
    CorruptCrc(u64),
}

/// Declarative test builder: accumulate log operations, then assert P&M.
struct CrcReadTest {
    ops: Vec<Op>,
}

impl CrcReadTest {
    fn new() -> Self {
        Self { ops: vec![] }
    }

    /// Write a V2 checkpoint at the given version.
    fn v2_checkpoint(mut self, version: u64, protocol: Protocol, metadata: Metadata) -> Self {
        self.ops.push(Op::V2Checkpoint {
            version,
            protocol,
            metadata,
        });
        self
    }

    /// Write a plain delta (commitInfo only, no protocol or metadata).
    fn delta(mut self, version: u64) -> Self {
        self.ops.push(Op::Delta(version));
        self
    }

    /// Write a delta with optional protocol and/or metadata overrides.
    fn delta_with_p_m(
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

    /// Write a CRC file with the given protocol and metadata.
    fn crc(mut self, version: u64, protocol: Protocol, metadata: Metadata) -> Self {
        self.ops.push(Op::Crc {
            version,
            protocol,
            metadata,
        });
        self
    }

    /// Write a corrupt CRC file.
    fn corrupt_crc(mut self, version: u64) -> Self {
        self.ops.push(Op::CorruptCrc(version));
        self
    }

    /// Execute all operations, returning a built test that can be asserted against.
    async fn build(self) -> BuiltCrcTest {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        for op in self.ops {
            match op {
                Op::V2Checkpoint {
                    version: v,
                    ref protocol,
                    ref metadata,
                } => {
                    let content = format!(
                        "{}\n{}\n{}",
                        protocol_action_json(protocol),
                        metadata_action_json(metadata),
                        json!({"checkpointMetadata": {"version": 2}})
                    );
                    put(&store, v, V2_CKPT_SUFFIX, &content).await;
                }
                Op::Delta(v) => {
                    put(&store, v, "json", &commit_info_json().to_string()).await;
                }
                Op::DeltaWithPM {
                    version: v,
                    protocol,
                    metadata,
                } => {
                    let mut lines = vec![commit_info_json().to_string()];
                    if let Some(ref p) = protocol {
                        lines.push(protocol_action_json(p).to_string());
                    }
                    if let Some(ref m) = metadata {
                        lines.push(metadata_action_json(m).to_string());
                    }
                    put(&store, v, "json", &lines.join("\n")).await;
                }
                Op::Crc {
                    version: v,
                    ref protocol,
                    ref metadata,
                } => {
                    put(&store, v, "crc", &crc_json(protocol, metadata).to_string()).await;
                }
                Op::CorruptCrc(v) => {
                    put(&store, v, "crc", "CORRUPT_CRC_DATA").await;
                }
            }
        }

        BuiltCrcTest { engine, url }
    }
}

struct BuiltCrcTest {
    engine: DefaultEngine<TokioBackgroundExecutor>,
    url: Url,
}

impl BuiltCrcTest {
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
        .crc(2, protocol_v2_dv(), metadata_b()) // <-- P & M from here
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
        .crc(2, protocol_v2_dv_ntz(), metadata_b()) // <-- P & M from here
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
        .crc(2, protocol_v2_dv(), metadata_b()) // <-- P & M from here
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
        .crc(1, protocol_v2_dv(), metadata_b()) // <-- P & M from here
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
        .crc(1, protocol_v2_dv(), metadata_b()) // <-- M from here
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
        .crc(1, protocol_v2_dv(), metadata_b()) // <-- P from here
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
        .crc(1, protocol_v2_dv_ntz(), metadata_b())
        .v2_checkpoint(2, protocol_v2_dv(), metadata_a()) // <-- P & M from here
        .delta(3)
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_a());
}
