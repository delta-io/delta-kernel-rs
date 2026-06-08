//! Tests for P&M replay and ICT reads with CRC files.
//!
//! Each test sets up an in-memory Delta log with V2 checkpoint JSONs, commit files, and CRC files,
//! then verifies that Protocol & Metadata loading and ICT reads resolve correctly.

use std::collections::HashMap;
use std::sync::Arc;

use rstest::rstest;
use serde_json::{json, Value};
use test_utils::{assert_result_error_with_message, delta_path_for_version};
use url::Url;

use super::LogSegment;
use crate::actions::{DomainMetadata, Format, Metadata, Protocol, SetTransaction};
use crate::crc::{
    try_read_crc_file, Crc, DomainMetadataState, FileSizeHistogram, SetTransactionState,
};
use crate::engine::sync::SyncEngine;
use crate::object_store::memory::InMemory;
use crate::object_store::ObjectStoreExt as _;
use crate::path::ParsedLogPath;
use crate::snapshot::IncrementalReplay;
use crate::{DeltaResult, Engine, Snapshot};

// ============================================================================
// Expected values
// ============================================================================

const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

const COMMIT_INFO_TIMESTAMP: i64 = 1587968586154;
const DEFAULT_OPERATION: &str = "TEST_OP";

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
    Protocol::try_new_modern(["v2Checkpoint"], ["v2Checkpoint", "inCommitTimestamp"]).unwrap()
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

fn create_table_actions() -> Vec<serde_json::Value> {
    vec![
        commit_info("CREATE", None),
        protocol(protocol_v2()),
        metadata(metadata_a()),
    ]
}

fn create_table_actions_with_ict() -> Vec<serde_json::Value> {
    vec![
        commit_info("CREATE", None),
        protocol(protocol_v2_ict()),
        metadata(metadata_a()),
    ]
}

/// A commitInfo action with the given operation and optional in-commit timestamp.
fn commit_info(op: &str, ict: Option<i64>) -> serde_json::Value {
    let mut obj = json!({"commitInfo": {"timestamp": COMMIT_INFO_TIMESTAMP, "operation": op}});
    if let Some(ict) = ict {
        obj["commitInfo"]["inCommitTimestamp"] = json!(ict);
    }
    obj
}

fn protocol(p: Protocol) -> serde_json::Value {
    json!({"protocol": serde_json::to_value(&p).unwrap()})
}

fn metadata(m: Metadata) -> serde_json::Value {
    json!({"metaData": serde_json::to_value(&m).unwrap()})
}

fn add(path: &str, size: i64) -> serde_json::Value {
    json!({"add": {
        "path": path,
        "size": size,
        "partitionValues": {},
        "modificationTime": 0,
        "dataChange": true,
    }})
}

fn remove(path: &str, size: Option<i64>) -> serde_json::Value {
    let mut obj = json!({"remove": {
        "path": path,
        "dataChange": true,
        "deletionTimestamp": 0,
    }});
    if let Some(s) = size {
        obj["remove"]["size"] = json!(s);
    }
    obj
}

fn domain_metadata(domain: &str, configuration: &str) -> serde_json::Value {
    json!({"domainMetadata": {
        "domain": domain,
        "configuration": configuration,
        "removed": false,
    }})
}

fn domain_metadata_tombstone(domain: &str) -> serde_json::Value {
    json!({"domainMetadata": {
        "domain": domain,
        "configuration": "",
        "removed": true,
    }})
}

fn set_txn(app_id: &str, version: i64, last_updated: Option<i64>) -> serde_json::Value {
    let mut obj = json!({"txn": {"appId": app_id, "version": version}});
    if let Some(lu) = last_updated {
        obj["txn"]["lastUpdated"] = json!(lu);
    }
    obj
}

fn crc_json(
    protocol: &Protocol,
    metadata: &Metadata,
    ict: Option<i64>,
    file_sizes: &[i64],
) -> serde_json::Value {
    let total_bytes: i64 = file_sizes.iter().sum();
    let num_files = file_sizes.len() as i64;
    let mut obj = json!({
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
        obj["fileSizeHistogram"] = serde_json::to_value(&hist).unwrap();
    }
    if let Some(ict) = ict {
        obj["inCommitTimestampOpt"] = json!(ict);
    }
    obj
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
        file_sizes: Vec<i64>,
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
        self,
        version: u64,
        protocol: Protocol,
        metadata: Metadata,
        ict: impl Into<Option<i64>>,
    ) -> Self {
        self.crc_with_files(version, protocol, metadata, ict, &[])
    }

    /// Write a CRC file whose on-disk `fileSizeHistogram` is built from `file_sizes`.
    fn crc_with_files(
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
                    ref file_sizes,
                } => {
                    put(
                        &store,
                        version,
                        "crc",
                        &crc_json(protocol, metadata, ict, file_sizes).to_string(),
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

    /// Asserts the accumulated ops are valid before `build()` writes anything.
    fn validate_ops(ops: &[Op]) {
        let mut prev: Option<u64> = None;
        for op in ops {
            if let Op::Commit { version, actions } = op {
                // Each commit must contain at least one action.
                assert!(
                    !actions.is_empty(),
                    "commit(v{version}, []): a commit must contain at least one action",
                );
                // Commit versions must be strictly increasing (and therefore unique).
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
    /// Construct a `LogSegment` directly from the store state (no `Snapshot`) and run
    /// `build_incremental_crc_from_base` against `base`.
    fn incrementally_build_crc(&self, base: &Crc) -> DeltaResult<Crc> {
        let storage = self.engine.storage_handler();
        let log_root = self.url.join("_delta_log/").unwrap();
        let log_segment =
            LogSegment::for_snapshot_impl(storage.as_ref(), log_root, vec![], None, None)?;
        log_segment.build_incremental_crc_from_base(&self.engine, base)
    }

    /// Read the on-disk CRC at `version` from this test's log.
    fn read_crc_at(&self, version: u64) -> DeltaResult<Crc> {
        try_read_crc_file(
            &self.engine,
            &ParsedLogPath::create_parsed_crc(&self.url, version),
        )
    }

    /// Build a snapshot at `version` (or latest if `None`) and return it
    /// alongside a human-readable label like `"v2"` or `"latest"`.
    fn snapshot_at(&self, version: Option<u64>) -> (crate::snapshot::SnapshotRef, String) {
        let mut builder = Snapshot::builder_for(self.url.clone())
            .with_incremental_crc_replay(IncrementalReplay::Unlimited);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        let snapshot = builder.build(&self.engine).unwrap();
        let label = version.map_or("latest".to_string(), |v| format!("v{v}"));
        (snapshot, label)
    }

    fn snapshot_latest_with_replay(&self, mode: IncrementalReplay) -> crate::snapshot::SnapshotRef {
        Snapshot::builder_for(self.url.clone())
            .with_incremental_crc_replay(mode)
            .build(&self.engine)
            .unwrap()
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
                commit_info(DEFAULT_OPERATION, None),
                protocol(protocol_v2()),
                metadata(metadata_a()),
            ],
        )
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
        .build()
        .await
        .assert_p_m(None, &protocol_v2(), &metadata_a());
}

#[tokio::test]
async fn test_get_p_and_m_from_different_deltas() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(
            1, // <-- P from here
            [
                commit_info(DEFAULT_OPERATION, None),
                protocol(protocol_v2_dv()),
            ],
        )
        .commit(
            2, // <-- M from here
            [commit_info(DEFAULT_OPERATION, None), metadata(metadata_b())],
        )
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_m_from_checkpoint() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
                commit_info(DEFAULT_OPERATION, None),
                protocol(protocol_v2_dv()),
                metadata(metadata_b()),
            ],
        )
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(
            2, // <-- P from here
            [
                commit_info(DEFAULT_OPERATION, None),
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P & M from here
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_b());
}

#[tokio::test]
async fn test_get_p_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- M from here
        .commit(
            2,
            [
                commit_info(DEFAULT_OPERATION, None),
                protocol(protocol_v2_dv_ntz()),
            ],
        )
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv_ntz(), &metadata_b());
}

#[tokio::test]
async fn test_get_m_from_newer_delta_over_older_crc() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .crc(1, protocol_v2_dv(), metadata_b(), None) // <-- P from here
        .commit(
            2, // <-- M from here
            [commit_info(DEFAULT_OPERATION, None), metadata(metadata_a())],
        )
        .build()
        .await
        .assert_p_m(None, &protocol_v2_dv(), &metadata_a());
}

#[tokio::test]
async fn test_corrupt_crc_at_non_target_version_falls_back() {
    CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a()) // <-- P & M from here
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .corrupt_crc(1) // <-- Corrupt! Fall back to replay.
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
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
                commit_info(DEFAULT_OPERATION, None),
                protocol(protocol_v2()),
                metadata(metadata_a()),
            ],
        )
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .crc(1, protocol_v2_dv_ntz(), metadata_b(), None)
        .v2_checkpoint(2, protocol_v2_dv(), metadata_a()) // <-- P & M from here
        .commit(3, [commit_info(DEFAULT_OPERATION, None)])
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
        .commit(1, [commit_info(DEFAULT_OPERATION, Some(2000))])
        .crc(1, protocol_v2_ict(), metadata_ict(), 1000) // <-- ICT from here
        .build()
        .await
        .assert_ict(None, Some(1000));
}

#[tokio::test]
async fn test_ict_errors_when_crc_has_no_ict() {
    let setup = CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2_ict(), metadata_ict())
        .commit(1, [commit_info(DEFAULT_OPERATION, Some(2000))])
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
        t = t.commit(v, [commit_info(DEFAULT_OPERATION, None)]);
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

// ============================================================================
// Integration tests for incremental CRC replay
// ============================================================================

fn crc_with_dm_and_txn_states(dm: DomainMetadataState, txn: SetTransactionState) -> Crc {
    Crc {
        domain_metadata_state: dm,
        set_transaction_state: txn,
        ..Default::default()
    }
}

fn crc_complete_empty_dm_set_txn() -> Crc {
    crc_with_dm_and_txn_states(
        DomainMetadataState::Complete(HashMap::new()),
        SetTransactionState::Complete(HashMap::new()),
    )
}

fn dm_entry(domain: &str, configuration: &str) -> (String, DomainMetadata) {
    (
        domain.to_string(),
        DomainMetadata::new(domain.to_string(), configuration.to_string()),
    )
}

fn txn_entry(app_id: &str, version: i64, last_updated: Option<i64>) -> (String, SetTransaction) {
    (
        app_id.to_string(),
        SetTransaction::new(app_id.to_string(), version, last_updated),
    )
}

// === Protocol / metadata ===

#[rstest]
#[case::newest_p_wins(vec![protocol(protocol_v2_dv()), commit_info("WRITE", None)], protocol_v2_dv(), metadata_a())]
#[case::newest_m_wins(vec![metadata(metadata_b()), commit_info("WRITE", None)], protocol_v2(), metadata_b())]
#[case::base_preserved_when_segment_has_none(vec![commit_info("WRITE", None)], protocol_v2(), metadata_a())]
#[tokio::test]
async fn test_p_m_propagation(
    #[case] v1_actions: Vec<Value>,
    #[case] expected_p: Protocol,
    #[case] expected_m: Metadata,
) {
    let base = Crc {
        protocol: protocol_v2(),
        metadata: metadata_a(),
        ..crc_complete_empty_dm_set_txn()
    };
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(1, v1_actions)
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    assert_eq!(crc.protocol, expected_p);
    assert_eq!(crc.metadata, expected_m);
}

// === ICT ===

#[rstest]
#[case::captures_newest(vec![commit_info("WRITE", Some(2000))], Some(2000))]
#[case::none_when_newest_has_no_commit_info(vec![set_txn("app", 1, None)], None)]
#[tokio::test]
async fn test_ict_from_newest_commit_only_replaces_base(
    #[case] v2_actions: Vec<Value>,
    #[case] expected_ict: Option<i64>,
) {
    // Base carries an ICT so each case proves the delta's ICT replaces the base's
    let base = Crc {
        in_commit_timestamp_opt: Some(500),
        ..crc_complete_empty_dm_set_txn()
    };
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions_with_ict())
        .commit(1, [commit_info("WRITE", Some(1000))])
        .commit(2, v2_actions)
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    assert_eq!(crc.in_commit_timestamp_opt, expected_ict);
}

// === Domain metadata ===

#[rstest]
#[case::complete_base(DomainMetadataState::Complete(
    HashMap::from([dm_entry("a", "base_a"), dm_entry("b", "base_b")])
))]
#[case::partial_base(DomainMetadataState::Partial(
    HashMap::from([dm_entry("a", "base_a"), dm_entry("b", "base_b")])
))]
#[tokio::test]
async fn test_dm_overrides_base_and_preserves_completeness(#[case] base_dm: DomainMetadataState) {
    let base = crc_with_dm_and_txn_states(base_dm, SetTransactionState::Complete(HashMap::new()));
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(
            1, // <-- updates "a"; "b" stays at base value
            [commit_info("WRITE", None), domain_metadata("a", "new_a")],
        )
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    // Variant preservation: `expect_*` panics if the base's variant did not survive.
    let map = match &base.domain_metadata_state {
        DomainMetadataState::Complete(_) => crc.domain_metadata_state.expect_complete(),
        DomainMetadataState::Partial(_) => crc.domain_metadata_state.expect_partial(),
    };
    assert_eq!(map["a"].configuration(), "new_a");
    assert_eq!(map["b"].configuration(), "base_b");
}

#[rstest]
#[case::complete_base(DomainMetadataState::Complete(
    HashMap::from([dm_entry("a", "base_a"), dm_entry("b", "base_b")])
))]
#[case::partial_base(DomainMetadataState::Partial(
    HashMap::from([dm_entry("a", "base_a"), dm_entry("b", "base_b")])
))]
#[tokio::test]
async fn test_dm_tombstone_removes_entry_from_base(#[case] base_dm: DomainMetadataState) {
    let base = crc_with_dm_and_txn_states(base_dm, SetTransactionState::Complete(HashMap::new()));
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(
            1, // <-- tombstone removes "a" from the base map
            [commit_info("WRITE", None), domain_metadata_tombstone("a")],
        )
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    let map = match &base.domain_metadata_state {
        DomainMetadataState::Complete(_) => crc.domain_metadata_state.expect_complete(),
        DomainMetadataState::Partial(_) => crc.domain_metadata_state.expect_partial(),
    };
    assert!(!map.contains_key("a"));
    assert_eq!(map["b"].configuration(), "base_b"); // <-- untouched entry preserved
}

#[tokio::test]
async fn test_dm_newer_commit_wins_over_older_commit_for_same_domain() {
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(1, [commit_info("WRITE", None), domain_metadata("d", "old")])
        .commit(
            2, // <-- same domain in two commits; newest wins
            [commit_info("WRITE", None), domain_metadata("d", "new")],
        )
        .build()
        .await
        .incrementally_build_crc(&crc_complete_empty_dm_set_txn())
        .unwrap();
    let map = crc.domain_metadata_state.expect_complete();
    assert_eq!(map["d"].configuration(), "new");
}

// === SetTransaction ===

#[rstest]
#[case::complete_base(SetTransactionState::Complete(HashMap::from([
    txn_entry("app_a", 1, Some(100)),
    txn_entry("app_b", 2, Some(200)),
])))]
#[case::partial_base(SetTransactionState::Partial(HashMap::from([
    txn_entry("app_a", 1, Some(100)),
    txn_entry("app_b", 2, Some(200)),
])))]
#[tokio::test]
async fn test_txn_overrides_base_and_preserves_completeness(#[case] base_txn: SetTransactionState) {
    let base = crc_with_dm_and_txn_states(DomainMetadataState::Complete(HashMap::new()), base_txn);
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(
            1,
            [set_txn("app_a", 42, Some(999)), commit_info("WRITE", None)],
        )
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    let map = match &base.set_transaction_state {
        SetTransactionState::Complete(_) => crc.set_transaction_state.expect_complete(),
        SetTransactionState::Partial(_) => crc.set_transaction_state.expect_partial(),
    };
    assert_eq!(map["app_a"].version, 42);
    assert_eq!(map["app_a"].last_updated, Some(999));
    assert_eq!(map["app_b"].version, 2);
    assert_eq!(map["app_b"].last_updated, Some(200));
}

#[tokio::test]
async fn test_txn_newer_commit_wins_over_older_commit_for_same_app() {
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(
            1,
            [set_txn("app", 1, Some(100)), commit_info("WRITE", None)],
        )
        .commit(
            2, // <-- same app in two commits; newest wins
            [set_txn("app", 42, Some(999)), commit_info("WRITE", None)],
        )
        .build()
        .await
        .incrementally_build_crc(&crc_complete_empty_dm_set_txn())
        .unwrap();
    let map = crc.set_transaction_state.expect_complete();
    assert_eq!(map["app"].version, 42);
    assert_eq!(map["app"].last_updated, Some(999));
}

// === File stats (in-memory base) ===

#[tokio::test]
async fn test_adds_and_removes_accumulate() {
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(1, [commit_info("WRITE", None), add("a", 100)])
        .commit(2, [commit_info("WRITE", None), add("b", 200)])
        .commit(3, [commit_info("WRITE", None), add("c", 300)])
        .commit(4, [remove("a", Some(100)), commit_info("WRITE", None)])
        .commit(5, [commit_info("WRITE", None), add("d", 400)])
        .commit(6, [remove("b", Some(200)), commit_info("WRITE", None)])
        .build()
        .await
        .incrementally_build_crc(&crc_complete_empty_dm_set_txn())
        .unwrap();
    assert!(crc.file_stats_state().is_complete());
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 2); // a and b removed; c and d remain
    assert_eq!(stats.table_size_bytes(), 700); // c (300) + d (400)
}

// === Indeterminate trips ===

// Three distinct ways a single commit can lose incremental safety.
#[rstest]
#[case::remove_no_size(vec![commit_info("WRITE", None), remove("orphan", None)])]
#[case::add_with_unsafe_op(vec![commit_info("ANALYZE STATS", None), add("a", 100)])]
#[case::add_with_no_commit_info(vec![add("a", 100)])]
#[tokio::test]
async fn test_trips_indeterminate(#[case] v1_actions: Vec<Value>) {
    // Base carries DM + txn entries so we can verify the indeterminate trip is scoped
    // to file stats and leaves other state alone.
    let base = crc_with_dm_and_txn_states(
        DomainMetadataState::Complete(HashMap::from([dm_entry("d", "base_d")])),
        SetTransactionState::Complete(HashMap::from([txn_entry("app", 5, Some(100))])),
    );
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(1, v1_actions)
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();
    assert!(crc.file_stats_state().is_indeterminate());
    assert_eq!(
        crc.domain_metadata_state.expect_complete()["d"].configuration(),
        "base_d",
    );
    assert_eq!(
        crc.set_transaction_state.expect_complete()["app"].version,
        5
    );
}

// === On-disk base CRC (exercises serde round-trip) ===

#[tokio::test]
async fn test_from_disk_advances_file_stats() {
    let built = CrcReadTest::new()
        .commit(0, create_table_actions())
        // Base CRC: a (100 B, bin 0) + b (10_000 B, bin 1).
        .crc_with_files(0, protocol_v2(), metadata_a(), None, &[100, 10_000])
        .commit(1, [commit_info("WRITE", None), add("c", 20_000)]) // c lands in bin 2
        .commit(2, [remove("a", Some(100)), commit_info("WRITE", None)]) // a removed from bin 0
        .build()
        .await;
    let base = built.read_crc_at(0).unwrap();
    let crc = built.incrementally_build_crc(&base).unwrap();
    assert!(crc.file_stats_state().is_complete());
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 2);
    assert_eq!(stats.table_size_bytes(), 30_000); // b (10_000) + c (20_000)

    let hist = stats.file_size_histogram().unwrap();
    assert_eq!(hist.file_counts()[0], 0); // a was removed
    assert_eq!(hist.file_counts()[1], 1); // b
    assert_eq!(hist.total_bytes()[1], 10_000);
    assert_eq!(hist.file_counts()[2], 1); // c
    assert_eq!(hist.total_bytes()[2], 20_000);
}

#[tokio::test]
async fn test_from_disk_no_histogram_means_no_result_histogram() {
    // Empty file_sizes => no histogram persisted on disk.
    let built = CrcReadTest::new()
        .commit(0, create_table_actions())
        .crc(0, protocol_v2(), metadata_a(), None)
        .commit(1, [commit_info("WRITE", None), add("a", 100)])
        .build()
        .await;
    let base = built.read_crc_at(0).unwrap();
    let crc = built.incrementally_build_crc(&base).unwrap();
    assert!(crc.file_stats().unwrap().file_size_histogram().is_none());
}

#[tokio::test]
async fn test_from_disk_with_non_zero_crc_version() {
    // Stale CRC is at v2, not v0. Replay covers (2, 3].
    let built = CrcReadTest::new()
        .commit(0, create_table_actions())
        .commit(1, [commit_info("WRITE", None), add("a", 100)])
        .commit(2, [commit_info("WRITE", None), add("b", 200)])
        .crc_with_files(2, protocol_v2(), metadata_a(), None, &[100, 200])
        .commit(3, [commit_info("WRITE", None), add("c", 300)])
        .build()
        .await;
    let base = built.read_crc_at(2).unwrap();
    let crc = built.incrementally_build_crc(&base).unwrap();
    assert!(crc.file_stats_state().is_complete());
    let stats = crc.file_stats().unwrap();
    // Base CRC at v=2 has a (100), b (200); replay over (2, 3] adds c (300).
    assert_eq!(stats.num_files(), 3); // a, b, c
    assert_eq!(stats.table_size_bytes(), 600); // a (100) + b (200) + c (300)
}

#[tokio::test]
async fn test_full_replay_across_two_commits_propagates_all_state() {
    // Realistic shape: a table evolves over two commits. v=1 initial activity; v=2 adds
    // more, removes an older file, updates metadata, and stamps an ICT. The create_table_actions
    // (and the base) use an ICT-enabled protocol so the v=2 ICT is well-formed.
    let base = Crc {
        protocol: protocol_v2_ict(),
        metadata: metadata_a(),
        ..crc_complete_empty_dm_set_txn()
    };
    let crc = CrcReadTest::new()
        .commit(0, create_table_actions_with_ict())
        .commit(
            1,
            [
                commit_info("WRITE", None),
                add("a", 100),
                domain_metadata("kept", "v1_value"),
                set_txn("app_a", 1, Some(100)),
            ],
        )
        .commit(
            2,
            [
                commit_info("WRITE", Some(9999)),
                metadata(metadata_b()),
                add("b", 200),
                remove("a", Some(100)),
                domain_metadata("d", "x"),
                set_txn("app_b", 5, Some(200)),
            ],
        )
        .build()
        .await
        .incrementally_build_crc(&base)
        .unwrap();

    // Newest commit's M and ICT win; protocol stays at create_table_actions's ICT-enabled choice.
    assert_eq!(crc.protocol, protocol_v2_ict());
    assert_eq!(crc.metadata, metadata_b());
    assert_eq!(crc.in_commit_timestamp_opt, Some(9999));

    // File stats accumulated across both commits: a added then removed; b remains.
    assert!(crc.file_stats_state().is_complete());
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 1); // only b remains
    assert_eq!(stats.table_size_bytes(), 200); // b (200)

    // DM and txn entries from BOTH commits land in the result map.
    let dm = crc.domain_metadata_state.expect_complete();
    assert_eq!(dm["kept"].configuration(), "v1_value");
    assert_eq!(dm["d"].configuration(), "x");
    let txn = crc.set_transaction_state.expect_complete();
    assert_eq!(txn["app_a"].version, 1);
    assert_eq!(txn["app_b"].version, 5);
}

// ============================================================================
// Tests: incremental-replay budget gate
// ============================================================================

// Commits 0-3 (target v3) with a CRC at `crc_version`, so distance = 3 - crc_version.
#[rstest]
// CRC already at the target (distance 0): used regardless of mode, even Disabled.
#[case::at_target_disabled(3, IncrementalReplay::Disabled, Some(3))]
// Stale CRC (distance >= 1): advancement depends on the budget.
#[case::disabled_stale(1, IncrementalReplay::Disabled, None)]
#[case::up_to_zero(1, IncrementalReplay::UpToCommits(0), None)]
#[case::interior(2, IncrementalReplay::UpToCommits(5), Some(3))] // distance 1 < 5
#[case::at_budget(1, IncrementalReplay::UpToCommits(2), Some(3))] // distance 2 == 2
#[case::over_budget(1, IncrementalReplay::UpToCommits(1), None)] // distance 2 > 1
#[case::unlimited(0, IncrementalReplay::Unlimited, Some(3))] // distance 3
#[tokio::test]
async fn test_stale_crc_advanced_only_within_replay_budget(
    #[case] crc_version: u64,
    #[case] mode: IncrementalReplay,
    #[case] expected_crc_version: Option<u64>,
) {
    let built = CrcReadTest::new()
        .v2_checkpoint(0, protocol_v2(), metadata_a())
        .commit(1, [commit_info(DEFAULT_OPERATION, None)])
        .commit(2, [commit_info(DEFAULT_OPERATION, None)])
        .commit(3, [commit_info(DEFAULT_OPERATION, None)])
        .crc(crc_version, protocol_v2(), metadata_a(), None)
        .build()
        .await;
    let snapshot = built.snapshot_latest_with_replay(mode);
    assert_eq!(snapshot.version(), 3);
    assert_eq!(snapshot.crc().map(|c| c.version), expected_crc_version);
}
