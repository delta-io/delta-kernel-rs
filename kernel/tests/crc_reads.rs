//! Tests for P&M replay with CRC files.
//!
//! Uses a `CrcTestCaseConfig` builder to set up in-memory Delta tables with specific
//! commit versions, checkpoint versions, and CRC files (valid or corrupt), then verifies
//! that Protocol & Metadata replay produces the correct results.

use std::sync::Arc;

use delta_kernel::arrow::array::StringArray;
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::parquet::arrow::ArrowWriter;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rstest::rstest;
use serde_json::json;
use url::Url;

use delta_kernel::actions::get_commit_schema;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::{Engine, EngineData, Snapshot};

use test_utils::{add_commit, delta_path_for_version};

const BASE_METADATA_ID: &str = "5fba94ed-9794-4965-ba6e-6ee3c0d22af9";
const ALT_METADATA_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

const SCHEMA_STRING: &str = r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"val","type":"string","nullable":true,"metadata":{}}]}"#;

/// Describes a Delta table layout for testing P&M replay with CRC.
///
/// The first version in `commit_versions` gets P+M (protocol + metadata). All subsequent
/// commits are commitInfo-only unless overridden via `protocol_at` or `metadata_at`. When a
/// checkpoint is present, it contains P+M and commits before it don't need to exist on disk.
struct CrcTestCaseConfig {
    /// Explicit commit versions to write (e.g. [0, 1, 2] or [11, 12, 13, ..., 20]).
    /// The first version gets P+M; the rest are commitInfo-only.
    commit_versions: Vec<u64>,
    /// Base protocol (min_reader_version, min_writer_version).
    base_protocol: (u32, u32),
    /// Optional: override protocol at a specific commit version.
    protocol_at: Option<(u64, u32, u32)>,
    /// Optional: override metadata at a specific commit version (uses ALT_METADATA_ID).
    metadata_at: Option<u64>,
    /// Optional: write a parquet checkpoint at this version.
    checkpoint: Option<u64>,
    /// Optional: write a CRC file at this version.
    crc: Option<u64>,
    /// CRC protocol values. When None, uses base_protocol.
    crc_protocol: Option<(u32, u32)>,
    /// If true, CRC file content is garbage (simulates corruption).
    corrupt_crc: bool,
}

impl CrcTestCaseConfig {
    /// Create a config with commits at versions 0..n.
    fn new(num_commits: u64) -> Self {
        Self {
            commit_versions: (0..num_commits).collect(),
            base_protocol: (1, 2),
            protocol_at: None,
            metadata_at: None,
            checkpoint: None,
            crc: None,
            crc_protocol: None,
            corrupt_crc: false,
        }
    }

    /// Create a config with explicit commit versions.
    fn with_commits(versions: impl IntoIterator<Item = u64>) -> Self {
        Self {
            commit_versions: versions.into_iter().collect(),
            base_protocol: (1, 2),
            protocol_at: None,
            metadata_at: None,
            checkpoint: None,
            crc: None,
            crc_protocol: None,
            corrupt_crc: false,
        }
    }

    fn crc(mut self, version: u64) -> Self {
        self.crc = Some(version);
        self
    }

    fn crc_protocol(mut self, reader: u32, writer: u32) -> Self {
        self.crc_protocol = Some((reader, writer));
        self
    }

    fn corrupt_crc(mut self) -> Self {
        self.corrupt_crc = true;
        self
    }

    fn protocol_at(mut self, version: u64, reader: u32, writer: u32) -> Self {
        self.protocol_at = Some((version, reader, writer));
        self
    }

    fn metadata_at(mut self, version: u64) -> Self {
        self.metadata_at = Some(version);
        self
    }

    fn checkpoint(mut self, version: u64) -> Self {
        self.checkpoint = Some(version);
        self
    }

    async fn build(self) -> (DefaultEngine<TokioBackgroundExecutor>, Url) {
        let store = Arc::new(InMemory::new());
        let url = Url::parse("memory:///").unwrap();
        let engine = DefaultEngineBuilder::new(store.clone()).build();

        let first_commit = self.commit_versions.first().copied();

        // Write commits
        for &version in &self.commit_versions {
            let is_first = Some(version) == first_commit;
            let commit_data = self.make_commit_json(version, is_first);
            add_commit(store.as_ref(), version, commit_data)
                .await
                .unwrap();
        }

        // Write checkpoint (contains P+M, so it stands alone without earlier commits)
        if let Some(cp_version) = self.checkpoint {
            self.write_checkpoint(&store, &engine, cp_version).await;
        }

        // Write CRC
        if let Some(crc_version) = self.crc {
            let crc_path = delta_path_for_version(crc_version, "crc");
            if self.corrupt_crc {
                store
                    .put(&crc_path, "CORRUPT_CRC_DATA".into())
                    .await
                    .unwrap();
            } else {
                let protocol = self.crc_protocol.unwrap_or(self.base_protocol);
                let metadata_id = self.effective_metadata_id(crc_version);
                let crc_json = make_crc_json(protocol, metadata_id);
                store.put(&crc_path, crc_json.into()).await.unwrap();
            }
        }

        (engine, url)
    }

    /// Build JSON for a single commit file.
    /// `include_pm`: if true, include protocol + metadata actions.
    fn make_commit_json(&self, version: u64, include_pm: bool) -> String {
        let mut actions = vec![json!({
            "commitInfo": {
                "timestamp": 1587968586154i64,
                "operation": "WRITE",
                "operationParameters": {"mode": "ErrorIfExists", "partitionBy": "[]"},
                "isBlindAppend": true
            }
        })];

        if include_pm {
            actions.push(make_protocol_json(self.base_protocol));
            actions.push(make_metadata_json(BASE_METADATA_ID));
        }

        // protocol_at override
        if let Some((at_version, reader, writer)) = self.protocol_at {
            if version == at_version {
                actions.push(make_protocol_json((reader, writer)));
            }
        }

        // metadata_at override
        if let Some(at_version) = self.metadata_at {
            if version == at_version {
                actions.push(make_metadata_json(ALT_METADATA_ID));
            }
        }

        actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn write_checkpoint(
        &self,
        store: &Arc<InMemory>,
        engine: &DefaultEngine<TokioBackgroundExecutor>,
        version: u64,
    ) {
        let actions = [
            make_protocol_json(self.effective_protocol_at(version)),
            make_metadata_json(self.effective_metadata_id(version)),
        ];

        let json_strings: StringArray = actions
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .into();

        let string_data = string_array_to_engine_data(json_strings);
        let parsed = engine
            .json_handler()
            .parse_json(string_data, get_commit_schema().clone())
            .unwrap();
        let arrow_data = ArrowEngineData::try_from_engine_data(parsed).unwrap();
        let batch: RecordBatch = (*arrow_data).into();

        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let path = delta_path_for_version(version, "checkpoint.parquet");
        store.put(&path, buffer.into()).await.unwrap();
    }

    fn effective_protocol_at(&self, version: u64) -> (u32, u32) {
        if let Some((at_version, reader, writer)) = self.protocol_at {
            if version >= at_version {
                return (reader, writer);
            }
        }
        self.base_protocol
    }

    fn effective_metadata_id(&self, version: u64) -> &str {
        if let Some(at_version) = self.metadata_at {
            if version >= at_version {
                return ALT_METADATA_ID;
            }
        }
        BASE_METADATA_ID
    }
}

fn make_protocol_json(protocol: (u32, u32)) -> serde_json::Value {
    json!({
        "protocol": {
            "minReaderVersion": protocol.0,
            "minWriterVersion": protocol.1
        }
    })
}

fn make_metadata_json(id: &str) -> serde_json::Value {
    json!({
        "metaData": {
            "id": id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": SCHEMA_STRING,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        }
    })
}

/// CRC JSON. Note: CRC uses "metadata" (not "metaData") and unwrapped protocol.
fn make_crc_json(protocol: (u32, u32), metadata_id: &str) -> String {
    json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "metadata": {
            "id": metadata_id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": SCHEMA_STRING,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        },
        "protocol": {
            "minReaderVersion": protocol.0,
            "minWriterVersion": protocol.1
        }
    })
    .to_string()
}

fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
    let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
    let schema = Arc::new(ArrowSchema::new(vec![string_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)]).unwrap();
    Box::new(ArrowEngineData::new(batch))
}

// Legend:
//   0.json(P+M)  = commit with protocol + metadata   0.json = commitInfo-only
//   0.crc(1,5)   = valid CRC with protocol (1,5)     0.crc(CORRUPT) = garbage CRC
//   0.checkpoint  = parquet checkpoint with P+M       0.json(P@2,6) = commit with protocol override
//   0.json(M')   = commit with alt metadata           0.crc(1,5,M') = CRC with alt metadata
//
// Each case: [files on disk] => expected protocol, expected metadata id
#[rstest]
//
// ---- Small table cases (commits from 0) ----
//
// 0.json(P+M), 1.json, 2.json => (1,2) from full log replay
#[case::commits_only(
    CrcTestCaseConfig::new(3),
    2,
    (1, 2),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 2.json, 2.crc(1,5) => (1,5) from CRC at target version
#[case::crc_at_target(
    CrcTestCaseConfig::new(3).crc(2).crc_protocol(1, 5),
    2,
    (1, 5),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 2.json, 2.crc(CORRUPT) => (1,2) from log replay fallback
#[case::corrupt_crc_at_target(
    CrcTestCaseConfig::new(3).crc(2).crc_protocol(1, 5).corrupt_crc(),
    2,
    (1, 2),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 1.crc(1,5), 2.json => (1,5) from CRC (no P+M change after v1)
#[case::crc_at_earlier_version(
    CrcTestCaseConfig::new(3).crc(1).crc_protocol(1, 5),
    2,
    (1, 5),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 1.crc(1,5), 2.json(P@2,6) => (2,6) from pruned replay (commit 2 wins over CRC)
#[case::pruned_replay_after_crc(
    CrcTestCaseConfig::new(3).crc(1).crc_protocol(1, 5).protocol_at(2, 2, 6),
    2,
    (2, 6),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 1.crc(CORRUPT), 2.json => (1,2) from full replay fallback
#[case::corrupt_crc_at_earlier(
    CrcTestCaseConfig::new(3).crc(1).corrupt_crc(),
    2,
    (1, 2),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 2.json, 2.checkpoint, 2.crc(1,5) => (1,5) from CRC (wins over checkpoint)
#[case::crc_over_checkpoint(
    CrcTestCaseConfig::new(3).checkpoint(2).crc(2).crc_protocol(1, 5),
    2,
    (1, 5),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 2.json, 2.checkpoint, 2.crc(CORRUPT) => (1,2) from checkpoint replay
#[case::checkpoint_on_corrupt_crc(
    CrcTestCaseConfig::new(3).checkpoint(2).crc(2).crc_protocol(1, 5).corrupt_crc(),
    2,
    (1, 2),
    BASE_METADATA_ID,
)]
// 0.json(P+M), 1.json, 2.json, 2.checkpoint => (1,2) from checkpoint replay
#[case::checkpoint_no_crc(
    CrcTestCaseConfig::new(3).checkpoint(2),
    2,
    (1, 2),
    BASE_METADATA_ID,
)]
//
// ---- Large gap cases (checkpoint at v10, commits 11..20, no commits 0..9) ----
//
// 10.checkpoint, 11.json, 12.json, ..., 20.json, 20.crc(1,5) => (1,5) from CRC at target
#[case::checkpoint_gap_crc_at_target(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(20).crc_protocol(1, 5),
    20,
    (1, 5),
    BASE_METADATA_ID,
)]
// 10.checkpoint, 11.json, ..., 15.crc(1,5), ..., 20.json => (1,5) from CRC (no P+M change after v15)
#[case::checkpoint_gap_crc_at_middle(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(15).crc_protocol(1, 5),
    20,
    (1, 5),
    BASE_METADATA_ID,
)]
// 10.checkpoint, 11.json, ..., 20.json, 20.crc(CORRUPT) => (1,2) from checkpoint + replay fallback
#[case::checkpoint_gap_corrupt_crc(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(20).crc_protocol(1, 5).corrupt_crc(),
    20,
    (1, 2),
    BASE_METADATA_ID,
)]
// 10.checkpoint, 11.json, 12.json, ..., 20.json => (1,2) from checkpoint + replay
#[case::checkpoint_gap_no_crc(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10),
    20,
    (1, 2),
    BASE_METADATA_ID,
)]
// 10.checkpoint, 11.json, ..., 15.crc(1,5), ..., 18.json(P@2,6), ..., 20.json => (2,6) from pruned replay
#[case::checkpoint_gap_crc_middle_protocol_after(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(15).crc_protocol(1, 5).protocol_at(18, 2, 6),
    20,
    (2, 6),
    BASE_METADATA_ID,
)]
//
// ---- Split P/M change cases (P and/or M change after CRC) ----
//
// 0.json(P+M), 1.json, 1.crc(1,5), 2.json(M'), 3.json => P(1,5) from CRC, M' from commit 2
#[case::metadata_change_after_crc(
    CrcTestCaseConfig::new(4).crc(1).crc_protocol(1, 5).metadata_at(2),
    3,
    (1, 5),
    ALT_METADATA_ID,
)]
// 0.json(P+M), 1.json, 1.crc(1,5), 2.json(P@2,6), 3.json(M'), 4.json => P(2,6), M' from commits
#[case::split_pm_protocol_then_metadata(
    CrcTestCaseConfig::new(5).crc(1).crc_protocol(1, 5).protocol_at(2, 2, 6).metadata_at(3),
    4,
    (2, 6),
    ALT_METADATA_ID,
)]
// 0.json(P+M), 1.json, 1.crc(1,5), 2.json(M'), 3.json(P@2,6), 4.json => P(2,6), M' from commits
#[case::split_pm_metadata_then_protocol(
    CrcTestCaseConfig::new(5).crc(1).crc_protocol(1, 5).metadata_at(2).protocol_at(3, 2, 6),
    4,
    (2, 6),
    ALT_METADATA_ID,
)]
// 10.checkpoint, 11-20.json, 12.crc(1,5), 14.json(P@2,6), 16.json(M') => P(2,6), M' from commits
#[case::checkpoint_gap_split_pm_after_crc(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(12).crc_protocol(1, 5).protocol_at(14, 2, 6).metadata_at(16),
    20,
    (2, 6),
    ALT_METADATA_ID,
)]
// 10.checkpoint, 11-20.json, 15.crc(1,5), 18.json(M') => P(1,5) from CRC, M' from commit 18
#[case::checkpoint_gap_metadata_after_crc(
    CrcTestCaseConfig::with_commits(11..=20).checkpoint(10).crc(15).crc_protocol(1, 5).metadata_at(18),
    20,
    (1, 5),
    ALT_METADATA_ID,
)]
// 0.json(P+M), 1.json(M'), 2.json, 2.crc(1,5,M') => P(1,5), M' (both from CRC at target)
#[case::crc_captures_earlier_metadata_change(
    CrcTestCaseConfig::new(3).metadata_at(1).crc(2).crc_protocol(1, 5),
    2,
    (1, 5),
    ALT_METADATA_ID,
)]
#[tokio::test]
async fn test_pm_replay(
    #[case] config: CrcTestCaseConfig,
    #[case] target_version: u64,
    #[case] expected_protocol: (u32, u32),
    #[case] expected_metadata_id: &str,
) {
    let (engine, url) = config.build().await;
    let snapshot = Snapshot::builder_for(url)
        .at_version(target_version)
        .build(&engine)
        .unwrap();
    let protocol = snapshot.table_configuration().protocol();
    assert_eq!(protocol.min_reader_version(), expected_protocol.0 as i32);
    assert_eq!(protocol.min_writer_version(), expected_protocol.1 as i32);
    let metadata = snapshot.table_configuration().metadata();
    assert_eq!(metadata.id(), expected_metadata_id);
}
