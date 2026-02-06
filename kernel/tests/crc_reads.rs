//! Tests for P&M replay with CRC files.
//!
//! Each test puts simple JSON strings into an in-memory store (v2 checkpoint JSONs, commit files,
//! and CRC files), then verifies that Protocol & Metadata loading resolves correctly.
//!
//! All protocols are (3, 7) with v2Checkpoint, distinguished by a marker feature:
//!   - Protocol A: v2Checkpoint only (checkpoint base)
//!   - Protocol B: + deletionVectors (commit override)
//!   - Protocol C: + timestampNtz (CRC)
//!
//! Two metadata IDs:
//!   - Metadata Z (base): `METADATA_Z`
//!   - Metadata Y (alt):  `METADATA_Y`

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::table_features::TableFeature;
use delta_kernel::Snapshot;

use test_utils::delta_path_for_version;

// ─── Expected values ───────────────────────────────────────────────────────────

/// Which protocol source was used (identified by marker feature).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Proto {
    /// Checkpoint base: only v2Checkpoint
    A,
    /// Commit override: v2Checkpoint + deletionVectors
    B,
    /// CRC: v2Checkpoint + timestampNtz
    C,
}

const METADATA_Z: &str = "5fba94ed-9794-4965-ba6e-6ee3c0d22af9";
const METADATA_Y: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

// ─── JSON building blocks (macros for compile-time concat!) ────────────────────

macro_rules! schema_escaped {
    () => {
        r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"#
    };
}

macro_rules! protocol_a_action {
    () => {
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}"#
    };
}

macro_rules! protocol_b_action {
    () => {
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint","deletionVectors"],"writerFeatures":["v2Checkpoint","deletionVectors"]}}"#
    };
}

macro_rules! commit_info_action {
    () => {
        r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isBlindAppend":true}}"#
    };
}

macro_rules! metadata_z_action {
    () => {
        concat!(
            r#"{"metaData":{"id":""#,
            "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
            r#"","format":{"provider":"parquet","options":{}},"schemaString":""#,
            schema_escaped!(),
            r#"","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#
        )
    };
}

macro_rules! metadata_y_action {
    () => {
        concat!(
            r#"{"metaData":{"id":""#,
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            r#"","format":{"provider":"parquet","options":{}},"schemaString":""#,
            schema_escaped!(),
            r#"","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#
        )
    };
}

// ─── File content constants ────────────────────────────────────────────────────

/// V2 checkpoint JSON: protocol A, metadata Z.
const V2_CHECKPOINT: &str = concat!(
    protocol_a_action!(),
    "\n",
    metadata_z_action!(),
    "\n",
    r#"{"checkpointMetadata":{"version":2}}"#
);

/// Commit: commitInfo only (no protocol or metadata).
const DELTA: &str = commit_info_action!();

/// Commit: commitInfo + protocol B.
const DELTA_PROTOCOL_B: &str = concat!(commit_info_action!(), "\n", protocol_b_action!());

/// Commit: commitInfo + metadata Y.
const DELTA_METADATA_Y: &str = concat!(commit_info_action!(), "\n", metadata_y_action!());

/// CRC: protocol C (v2Checkpoint + timestampNtz marker), metadata Z.
const CHECKSUM: &str = concat!(
    r#"{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":1,"metadata":{"id":""#,
    "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
    r#"","format":{"provider":"parquet","options":{}},"schemaString":""#,
    schema_escaped!(),
    r#"","partitionColumns":[],"configuration":{},"createdTime":1587968585495},"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint","timestampNtz"],"writerFeatures":["v2Checkpoint","timestampNtz"]}}"#
);

/// CRC: protocol C (v2Checkpoint + timestampNtz marker), metadata Y.
const CHECKSUM_METADATA_Y: &str = concat!(
    r#"{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":1,"metadata":{"id":""#,
    "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    r#"","format":{"provider":"parquet","options":{}},"schemaString":""#,
    schema_escaped!(),
    r#"","partitionColumns":[],"configuration":{},"createdTime":1587968585495},"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint","timestampNtz"],"writerFeatures":["v2Checkpoint","timestampNtz"]}}"#
);

/// Corrupt CRC content.
const CORRUPT_CHECKSUM: &str = "CORRUPT_CRC_DATA";

/// V2 checkpoint file suffix (fixed UUID).
const V2_CKPT: &str = "checkpoint.00000000-0000-0000-0000-000000000000.json";

// ─── Helpers ───────────────────────────────────────────────────────────────────

fn setup() -> (Arc<InMemory>, DefaultEngine<TokioBackgroundExecutor>, Url) {
    let store = Arc::new(InMemory::new());
    let url = Url::parse("memory:///").unwrap();
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    (store, engine, url)
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

async fn assert_pm(
    engine: &DefaultEngine<TokioBackgroundExecutor>,
    url: &Url,
    version: u64,
    expected_proto: Proto,
    expected_metadata_id: &str,
) {
    let snapshot = Snapshot::builder_for(url.clone())
        .at_version(version)
        .build(engine)
        .unwrap();
    let protocol = snapshot.table_configuration().protocol();

    assert_eq!(
        protocol.min_reader_version(),
        3,
        "reader version at v{version}"
    );
    assert_eq!(
        protocol.min_writer_version(),
        7,
        "writer version at v{version}"
    );

    let writer_feats = protocol
        .writer_features()
        .expect("expected writer features");
    let has_dv = writer_feats.contains(&TableFeature::DeletionVectors);
    let has_tntz = writer_feats.contains(&TableFeature::TimestampWithoutTimezone);
    match expected_proto {
        Proto::A => assert!(!has_dv && !has_tntz, "Expected Protocol A at v{version}"),
        Proto::B => assert!(has_dv && !has_tntz, "Expected Protocol B at v{version}"),
        Proto::C => assert!(!has_dv && has_tntz, "Expected Protocol C at v{version}"),
    }

    assert_eq!(
        snapshot.table_configuration().metadata().id(),
        expected_metadata_id,
        "Metadata ID mismatch at v{version}"
    );
}

// ─── Tests ─────────────────────────────────────────────────────────────────────
//
// Legend:
//   v0.ckpt     = V2 checkpoint (protocol A, metadata Z)
//   v1          = plain commit (DELTA)
//   v1.crc      = CRC (CHECKSUM: protocol C, metadata Z)
//   v2(B)       = commit with protocol B
//   v2(Y)       = commit with metadata Y
//   v2.crc!     = corrupt CRC

// ── Baseline (no CRC) ──

/// v0.ckpt, v1, v2 => protocol A, metadata Z
#[tokio::test]
async fn test_no_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 2, "json", DELTA).await;
    assert_pm(&engine, &url, 2, Proto::A, METADATA_Z).await;
}

// ── CRC at target version ──

/// v0.ckpt, v1, v2, v2.crc => protocol C from CRC
#[tokio::test]
async fn test_crc_at_target() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 2, "json", DELTA).await;
    put(&store, 2, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 2, Proto::C, METADATA_Z).await;
}

/// v0.ckpt, v1, v2, v2.crc! => protocol A (corrupt CRC, falls back to replay)
#[tokio::test]
async fn test_corrupt_crc_at_target_falls_back() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 2, "json", DELTA).await;
    put(&store, 2, "crc", CORRUPT_CHECKSUM).await;
    assert_pm(&engine, &url, 2, Proto::A, METADATA_Z).await;
}

// ── CRC at earlier version ──

/// v0.ckpt, v1, v1.crc, v2 => protocol C (no P/M change after v1)
#[tokio::test]
async fn test_crc_at_earlier_version() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CHECKSUM).await;
    put(&store, 2, "json", DELTA).await;
    assert_pm(&engine, &url, 2, Proto::C, METADATA_Z).await;
}

/// v0.ckpt, v1, v1.crc, v2(B) => protocol B (commit overrides CRC)
#[tokio::test]
async fn test_commit_protocol_overrides_earlier_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CHECKSUM).await;
    put(&store, 2, "json", DELTA_PROTOCOL_B).await;
    assert_pm(&engine, &url, 2, Proto::B, METADATA_Z).await;
}

/// v0.ckpt, v1, v1.crc!, v2 => protocol A (corrupt CRC, falls back to full replay)
#[tokio::test]
async fn test_corrupt_crc_at_earlier_falls_back() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CORRUPT_CHECKSUM).await;
    put(&store, 2, "json", DELTA).await;
    assert_pm(&engine, &url, 2, Proto::A, METADATA_Z).await;
}

// ── CRC with checkpoint at target ──

/// v0.ckpt, v1, v2, v2.ckpt, v2.crc => protocol C (CRC wins over checkpoint)
#[tokio::test]
async fn test_crc_wins_over_checkpoint() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 2, "json", DELTA).await;
    put(&store, 2, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 2, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 2, Proto::C, METADATA_Z).await;
}

/// v0.ckpt, v1, v2, v2.ckpt, v2.crc! => protocol A (corrupt CRC, checkpoint replay)
#[tokio::test]
async fn test_checkpoint_on_corrupt_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 2, "json", DELTA).await;
    put(&store, 2, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 2, "crc", CORRUPT_CHECKSUM).await;
    assert_pm(&engine, &url, 2, Proto::A, METADATA_Z).await;
}

// ── Large gap (checkpoint at v10, commits v11..v20) ──

/// v10.ckpt, v11..v20, v20.crc => protocol C from CRC
#[tokio::test]
async fn test_gap_crc_at_target() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        put(&store, v, "json", DELTA).await;
    }
    put(&store, 20, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::C, METADATA_Z).await;
}

/// v10.ckpt, v11..v20, v15.crc => protocol C (no P/M change after v15)
#[tokio::test]
async fn test_gap_crc_at_middle() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        put(&store, v, "json", DELTA).await;
    }
    put(&store, 15, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::C, METADATA_Z).await;
}

/// v10.ckpt, v11..v20, v20.crc! => protocol A (corrupt CRC, checkpoint replay)
#[tokio::test]
async fn test_gap_corrupt_crc_falls_back() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        put(&store, v, "json", DELTA).await;
    }
    put(&store, 20, "crc", CORRUPT_CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::A, METADATA_Z).await;
}

/// v10.ckpt, v11..v20 => protocol A (no CRC, checkpoint replay)
#[tokio::test]
async fn test_gap_no_crc() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        put(&store, v, "json", DELTA).await;
    }
    assert_pm(&engine, &url, 20, Proto::A, METADATA_Z).await;
}

/// v10.ckpt, v11..v20, v15.crc, v18(B) => protocol B (commit overrides CRC)
#[tokio::test]
async fn test_gap_commit_protocol_overrides_crc() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        if v == 18 {
            put(&store, v, "json", DELTA_PROTOCOL_B).await;
        } else {
            put(&store, v, "json", DELTA).await;
        }
    }
    put(&store, 15, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::B, METADATA_Z).await;
}

// ── Split P/M changes after CRC ──

/// v0.ckpt, v1, v1.crc, v2(Y), v3 => protocol C from CRC, metadata Y from commit
#[tokio::test]
async fn test_metadata_change_after_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CHECKSUM).await;
    put(&store, 2, "json", DELTA_METADATA_Y).await;
    put(&store, 3, "json", DELTA).await;
    assert_pm(&engine, &url, 3, Proto::C, METADATA_Y).await;
}

/// v0.ckpt, v1, v1.crc, v2(B), v3(Y), v4 => protocol B, metadata Y from commits
#[tokio::test]
async fn test_protocol_then_metadata_after_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CHECKSUM).await;
    put(&store, 2, "json", DELTA_PROTOCOL_B).await;
    put(&store, 3, "json", DELTA_METADATA_Y).await;
    put(&store, 4, "json", DELTA).await;
    assert_pm(&engine, &url, 4, Proto::B, METADATA_Y).await;
}

/// v0.ckpt, v1, v1.crc, v2(Y), v3(B), v4 => protocol B, metadata Y from commits
#[tokio::test]
async fn test_metadata_then_protocol_after_crc() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA).await;
    put(&store, 1, "crc", CHECKSUM).await;
    put(&store, 2, "json", DELTA_METADATA_Y).await;
    put(&store, 3, "json", DELTA_PROTOCOL_B).await;
    put(&store, 4, "json", DELTA).await;
    assert_pm(&engine, &url, 4, Proto::B, METADATA_Y).await;
}

/// v10.ckpt, v11..v20, v12.crc, v14(B), v16(Y) => protocol B, metadata Y from commits
#[tokio::test]
async fn test_gap_split_pm_after_crc() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        match v {
            14 => put(&store, v, "json", DELTA_PROTOCOL_B).await,
            16 => put(&store, v, "json", DELTA_METADATA_Y).await,
            _ => put(&store, v, "json", DELTA).await,
        }
    }
    put(&store, 12, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::B, METADATA_Y).await;
}

/// v10.ckpt, v11..v20, v15.crc, v18(Y) => protocol C from CRC, metadata Y from commit
#[tokio::test]
async fn test_gap_metadata_after_crc() {
    let (store, engine, url) = setup();
    put(&store, 10, V2_CKPT, V2_CHECKPOINT).await;
    for v in 11..=20 {
        if v == 18 {
            put(&store, v, "json", DELTA_METADATA_Y).await;
        } else {
            put(&store, v, "json", DELTA).await;
        }
    }
    put(&store, 15, "crc", CHECKSUM).await;
    assert_pm(&engine, &url, 20, Proto::C, METADATA_Y).await;
}

/// v0.ckpt, v1(Y), v2, v2.crc(C,Y) => protocol C, metadata Y (both from CRC)
#[tokio::test]
async fn test_crc_captures_metadata_change() {
    let (store, engine, url) = setup();
    put(&store, 0, V2_CKPT, V2_CHECKPOINT).await;
    put(&store, 1, "json", DELTA_METADATA_Y).await;
    put(&store, 2, "json", DELTA).await;
    put(&store, 2, "crc", CHECKSUM_METADATA_Y).await;
    assert_pm(&engine, &url, 2, Proto::C, METADATA_Y).await;
}
