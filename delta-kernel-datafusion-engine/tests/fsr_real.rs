//! End-to-end FSR assertions with explicit expected rows.
//!
//! We assert:
//! 1) terminal `Snapshot::full_state` output rows (live add-file paths) against exact expected
//!    pretty-table rows; and
//! 2) intermediate commit-dedup action kinds contain protocol / metadata / domain metadata rows on
//!    a synthetic log fixture; and
//! 3) full `ToJson` payloads on real fixtures:
//!    - `table-with-dv-small`: protocol, metadata, full add (with DV), full remove.
//!    - `app-txn-no-checkpoint`: protocol, metadata, txn (two apps), multiple adds across commits.
//!
//!    No JSON commit fixture under `kernel/tests/data` contains a top-level `domainMetadata`
//!    action today (`crc-full` only lists `domainMetadata` in protocol writerFeatures); synthetic
//!    rows still cover domain metadata payloads.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::concat_or_clone;
use delta_kernel::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use delta_kernel::arrow::util::display::array_value_to_string;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::{Expression, UnaryExpressionOp};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::SinkType;
use delta_kernel::plans::ir::{Plan, RelationHandle};
use delta_kernel::plans::state_machines::scan::full_state::FSR_COMMIT_DEDUP;
use delta_kernel::plans::state_machines::scan::{
    build_fsr_plans, checkpoint_shape_from_last_checkpoint,
};
use delta_kernel::schema::DataType as KernelDataType;
use delta_kernel::{Engine as KernelEngine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use url::Url;

struct FixtureTable {
    _tmp: Option<TempDir>,
    url: Url,
}

fn commit_dedup_sink_handle(plans: &[Plan]) -> RelationHandle {
    plans
        .iter()
        .find_map(|p| match &p.sink {
            SinkType::Relation(h) if h.name == FSR_COMMIT_DEDUP => Some(h.clone()),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "FSR plans must include a Relation sink named {FSR_COMMIT_DEDUP:?}; got plan sinks: {:?}",
                plans
                    .iter()
                    .map(|p| &p.sink)
                    .collect::<Vec<_>>()
            )
        })
}

fn fixture_table(name: &str) -> FixtureTable {
    let data_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../kernel/tests/data");
    let direct = data_root.join(name);
    if direct.is_dir() {
        return FixtureTable {
            _tmp: None,
            url: Url::from_directory_path(direct.canonicalize().expect("fixture path"))
                .expect("table url"),
        };
    }

    let tmp = test_utils::load_test_data("../kernel/tests/data", name)
        .unwrap_or_else(|e| panic!("load archived fixture {name}: {e}"));
    let extracted = tmp.path().join(name);
    assert!(
        extracted.is_dir(),
        "archived fixture extraction missing directory: {}",
        extracted.display()
    );
    FixtureTable {
        _tmp: Some(tmp),
        url: Url::from_directory_path(extracted.canonicalize().expect("extracted fixture path"))
            .expect("table url"),
    }
}

fn default_engine() -> Arc<dyn KernelEngine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Build a kernel `(engine, snapshot)` pair for the table at `table_url`. Common scaffolding for
/// FSR tests that drive snapshots over either real fixtures or synthetic temp tables.
fn open_snapshot(table_url: Url) -> (Arc<dyn KernelEngine>, Arc<Snapshot>) {
    let engine = default_engine();
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot");
    (engine, snapshot)
}

/// `open_snapshot` keyed by a fixture name resolved via [`fixture_table`].
fn open_snapshot_for_fixture(table: &str) -> (Arc<dyn KernelEngine>, Arc<Snapshot>) {
    open_snapshot(fixture_table(table).url)
}

/// Canonicalize the path of an extracted/synthetic table directory into a `file://` URL.
fn synthetic_table_url(dir: &TempDir) -> Url {
    Url::from_directory_path(dir.path().canonicalize().expect("canon tmp table"))
        .expect("table url")
}

fn extract_sorted_non_null_paths_from_results(batches: &[RecordBatch]) -> Vec<String> {
    let merged = concat_or_clone(batches);
    let add_idx = merged.schema().index_of("add").expect("add idx");
    let add_col = merged.column(add_idx).as_struct_opt().expect("add struct");
    let path_col = add_col.column_by_name("path").expect("add.path");
    let mut out: Vec<String> = (0..path_col.len())
        .filter(|&i| add_col.is_valid(i) && path_col.is_valid(i))
        .map(|i| array_value_to_string(path_col.as_ref(), i).expect("stringify path"))
        .collect();
    out.sort();
    out
}

async fn run_full_state_results(table: &str) -> Vec<RecordBatch> {
    let (engine, snapshot) = open_snapshot_for_fixture(table);
    let ex = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine)).expect("executor");
    let sm = snapshot.full_state().expect("full_state SM");
    let rp = ex.drive_to_completion(sm).await.expect("drive full_state");
    ex.collect_result(rp)
        .await
        .expect("collect full_state result")
}

fn expected_live_file_count_from_scan_metadata(table: &str) -> usize {
    let (engine, snapshot) = open_snapshot_for_fixture(table);
    let scan = snapshot.scan_builder().build().expect("build scan");
    scan.scan_metadata(engine.as_ref())
        .expect("scan metadata")
        .map(|res| {
            let sm = res.expect("scan metadata batch");
            let sv = sm.scan_files.selection_vector();
            let row_count = sm.scan_files.data().len();
            if sv.len() < row_count {
                sv.iter().filter(|selected| **selected).count() + (row_count - sv.len())
            } else {
                sv.iter().filter(|selected| **selected).count()
            }
        })
        .sum()
}

fn write_json_commit(path: &Path) {
    let rows = [
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"metaData":{"id":"mid-1","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":null}}"#,
        r#"{"domainMetadata":{"domain":"d1","configuration":"c1","removed":false}}"#,
        r#"{"txn":{"appId":"app-1","version":7,"lastUpdated":123}}"#,
        r#"{"add":{"path":"x.parquet","partitionValues":{"p":"1"},"size":11,"modificationTime":22,"dataChange":true,"stats":"{\"numRecords\":1}","tags":{"t1":"v1","t2":null},"deletionVector":{"storageType":"u","pathOrInlineDv":"ab", "offset":3,"sizeInBytes":4,"cardinality":5},"baseRowId":6,"defaultRowCommitVersion":7,"clusteringProvider":"cp"}}"#,
        r#"{"remove":{"path":"y.parquet","deletionTimestamp":33,"dataChange":false,"extendedFileMetadata":true,"partitionValues":{"p":"2"},"size":44,"stats":"{\"numRecords\":1}","tags":{"rt":"rv"},"deletionVector":{"storageType":"u","pathOrInlineDv":"cd","offset":8,"sizeInBytes":9,"cardinality":10},"baseRowId":11,"defaultRowCommitVersion":12}}"#,
    ];
    fs::write(path, format!("{}\n", rows.join("\n"))).expect("write commit json");
}

fn build_synthetic_protocol_metadata_domain_table() -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir = dir.path().join("_delta_log");
    fs::create_dir_all(&log_dir).expect("mkdir _delta_log");
    write_json_commit(&log_dir.join("00000000000000000000.json"));
    dir
}

fn write_json_commit_lines(path: &Path, lines: &[&str]) {
    fs::write(path, format!("{}\n", lines.join("\n"))).expect("write commit json");
}

fn build_synthetic_metadata_replace_table() -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir = dir.path().join("_delta_log");
    fs::create_dir_all(&log_dir).expect("mkdir _delta_log");
    write_json_commit_lines(
        &log_dir.join("00000000000000000000.json"),
        &[
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"mid-old","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1}}"#,
        ],
    );
    write_json_commit_lines(
        &log_dir.join("00000000000000000001.json"),
        &[
            r#"{"metaData":{"id":"mid-new","name":null,"description":null,"format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":2}}"#,
        ],
    );
    dir
}

async fn commit_dedup_batches_for_snapshot(
    snapshot: Arc<Snapshot>,
    engine: Arc<dyn KernelEngine>,
) -> Vec<RecordBatch> {
    let shape = checkpoint_shape_from_last_checkpoint(&snapshot).expect("shape");
    let rp = build_fsr_plans(&snapshot, shape).expect("build fsr plans");
    let commit_dedup_handle = commit_dedup_sink_handle(&rp.plans);
    let ex = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    ex.execute_plans(&rp.plans).await.expect("run fsr plans");
    ex.collect_relation(&commit_dedup_handle)
        .await
        .expect("commit_dedup relation batches")
}

/// Action kind columns inspected to label commit-dedup rows by their non-null top-level field.
const DEDUP_ACTION_KINDS: &[&str] = &[
    "add",
    "remove",
    "protocol",
    "metaData",
    "domainMetadata",
    "txn",
];

async fn dedup_action_kinds_for_synthetic_table() -> Vec<String> {
    let batches = dedup_full_rows_for_synthetic_table().await;
    let mut kinds = Vec::new();
    for b in batches {
        let schema = b.schema();
        let columns: Vec<_> = DEDUP_ACTION_KINDS
            .iter()
            .map(|name| {
                let idx = schema
                    .index_of(name)
                    .unwrap_or_else(|_| panic!("{name} idx"));
                b.column(idx)
                    .as_struct_opt()
                    .unwrap_or_else(|| panic!("{name} struct"))
            })
            .collect();
        for i in 0..b.num_rows() {
            let kind = DEDUP_ACTION_KINDS
                .iter()
                .zip(&columns)
                .find_map(|(name, arr)| arr.is_valid(i).then_some(*name))
                .unwrap_or("none");
            kinds.push(kind.to_string());
        }
    }
    kinds.sort();
    kinds
}

async fn dedup_full_rows_for_synthetic_table() -> Vec<RecordBatch> {
    let dir = build_synthetic_protocol_metadata_domain_table();
    let (engine, snapshot) = open_snapshot(synthetic_table_url(&dir));
    commit_dedup_batches_for_snapshot(snapshot, engine).await
}

async fn commit_dedup_merged_for_real_fixture(table_name: &str) -> RecordBatch {
    let (engine, snapshot) = open_snapshot_for_fixture(table_name);
    let dedup_batches = commit_dedup_batches_for_snapshot(snapshot, engine).await;
    concat_or_clone(&dedup_batches)
}

async fn assert_fsr_matches_scan_paths(table_name: &str) {
    let fsr_batches = run_full_state_results(table_name).await;
    let fsr_paths = extract_sorted_non_null_paths_from_results(&fsr_batches);
    let expected_live_files = expected_live_file_count_from_scan_metadata(table_name);
    assert_eq!(
        fsr_paths.len(),
        expected_live_files,
        "FSR live-file rows must match scan_metadata live-file count for fixture {table_name}"
    );
}

fn payload_stable_sort_key(kind: &str, v: &JsonValue) -> (String, String) {
    let tie = match kind {
        "add" | "remove" => v
            .get("path")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        "metaData" => v
            .get("id")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        "domainMetadata" => format!(
            "{}|{}",
            v.get("domain").and_then(JsonValue::as_str).unwrap_or(""),
            v.get("configuration")
                .and_then(JsonValue::as_str)
                .unwrap_or("")
        ),
        "txn" => v
            .get("appId")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    };
    (kind.to_string(), tie)
}

fn evaluate_to_json_column(batch: &RecordBatch, col: &'static str) -> StringArray {
    let arr = evaluate_expression(
        &Expression::unary(UnaryExpressionOp::ToJson, Expression::column([col])),
        batch,
        Some(&KernelDataType::STRING),
    )
    .expect("evaluate to_json");
    arr.as_string::<i32>().clone()
}

fn full_action_payload_rows(batch: &RecordBatch) -> Vec<(String, JsonValue)> {
    let json_columns: Vec<_> = DEDUP_ACTION_KINDS
        .iter()
        .map(|name| (*name, evaluate_to_json_column(batch, name)))
        .collect();
    let mut rows = Vec::new();
    for i in 0..batch.num_rows() {
        if let Some((kind, arr)) = json_columns.iter().find(|(_, arr)| arr.is_valid(i)) {
            let payload = serde_json::from_str::<JsonValue>(arr.value(i)).expect("json payload");
            rows.push((kind.to_string(), payload));
        }
    }
    rows.sort_by(|(k1, v1), (k2, v2)| {
        payload_stable_sort_key(k1, v1).cmp(&payload_stable_sort_key(k2, v2))
    });
    rows
}

fn assert_singleton_protocol_and_metadata(rows: &[(String, JsonValue)]) {
    let protocol_count = rows.iter().filter(|(k, _)| k == "protocol").count();
    let metadata_count = rows.iter().filter(|(k, _)| k == "metaData").count();
    assert_eq!(
        protocol_count, 1,
        "FSR commit-dedup must contain exactly one protocol row"
    );
    assert_eq!(
        metadata_count, 1,
        "FSR commit-dedup must contain exactly one metadata row"
    );
}

#[tokio::test]
async fn fsr_results_no_checkpoint_expected_rows() {
    let batches = run_full_state_results("app-txn-no-checkpoint").await;
    let actual = extract_sorted_non_null_paths_from_results(&batches);
    let expected = vec![
        "modified=2021-02-01/part-00001-80996595-a345-43b7-b213-e247d6f091f7-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-01/part-00001-8ebcaf8b-0f48-4213-98c9-5c2156d20a7e-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-9a16b9f6-c12a-4609-a9c4-828eacb9526a-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-bfac5c74-426e-410f-ab74-21a64e518e9c-c000.snappy.parquet"
            .to_string(),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_results_v1_checkpoint_expected_rows() {
    let batches = run_full_state_results("app-txn-checkpoint").await;
    let actual = extract_sorted_non_null_paths_from_results(&batches);
    let expected = vec![
        "modified=2021-02-01/part-00001-3b6e7f26-8140-4067-8504-47540a363758-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-01/part-00001-7e32952f-35ad-423c-8926-dbd3d264b1ee-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-5113d412-8632-458b-a49a-f34db1069081-c000.snappy.parquet"
            .to_string(),
        "modified=2021-02-02/part-00001-f968feb7-7f54-40c5-8aea-6a3b7a406d9c-c000.snappy.parquet"
            .to_string(),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_includes_protocol_metadata_and_domain_metadata_rows() {
    let actual_kinds = dedup_action_kinds_for_synthetic_table().await;
    let expected_kinds = vec![
        "add".to_string(),
        "domainMetadata".to_string(),
        "metaData".to_string(),
        "protocol".to_string(),
        "remove".to_string(),
        "txn".to_string(),
    ];
    assert_eq!(actual_kinds, expected_kinds);
}

#[tokio::test]
async fn fsr_commit_dedup_keeps_latest_metadata_singleton() {
    let dir = build_synthetic_metadata_replace_table();
    let (engine, snapshot) = open_snapshot(synthetic_table_url(&dir));
    let dedup_batches = commit_dedup_batches_for_snapshot(snapshot, engine).await;
    let merged = concat_or_clone(&dedup_batches);
    let payloads = full_action_payload_rows(&merged);
    let metadata_rows: Vec<_> = payloads
        .iter()
        .filter_map(|(kind, payload)| (kind == "metaData").then_some(payload))
        .collect();
    assert_eq!(
        metadata_rows.len(),
        1,
        "FSR commit dedup must keep exactly one metadata row"
    );
    assert_eq!(
        metadata_rows[0].get("id").and_then(JsonValue::as_str),
        Some("mid-new"),
        "FSR commit dedup must keep latest metadata row by commit version"
    );
}

#[tokio::test]
async fn fsr_commit_dedup_full_rows_expected_table_includes_protocol_metadata_domain() {
    let dedup_batches = dedup_full_rows_for_synthetic_table().await;
    let merged = concat_or_clone(&dedup_batches);
    let actual = full_action_payload_rows(&merged);
    let expected = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"x.parquet",
                "partitionValues":{"p":"1"},
                "size":11,
                "modificationTime":22,
                "dataChange":true,
                "stats":"{\"numRecords\":1}",
                "tags":{"t1":"v1"},
                "deletionVector":{"storageType":"u","pathOrInlineDv":"ab","offset":3,"sizeInBytes":4,"cardinality":5},
                "baseRowId":6,
                "defaultRowCommitVersion":7,
                "clusteringProvider":"cp"
            }),
        ),
        (
            "domainMetadata".to_string(),
            serde_json::json!({"domain":"d1","configuration":"c1","removed":false}),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"mid-1",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":[],
                "configuration":{}
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({"minReaderVersion":1,"minWriterVersion":2}),
        ),
        (
            "remove".to_string(),
            serde_json::json!({
                "path":"y.parquet",
                "deletionTimestamp":33,
                "dataChange":false,
                "extendedFileMetadata":true,
                "partitionValues":{"p":"2"},
                "size":44,
                "stats":"{\"numRecords\":1}",
                "tags":{"rt":"rv"},
                "deletionVector":{"storageType":"u","pathOrInlineDv":"cd","offset":8,"sizeInBytes":9,"cardinality":10},
                "baseRowId":11,
                "defaultRowCommitVersion":12
            }),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"app-1","version":7,"lastUpdated":123}),
        ),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_real_table_with_dv_small_full_to_json_payloads() {
    let merged = commit_dedup_merged_for_real_fixture("table-with-dv-small").await;
    let actual = full_action_payload_rows(&merged);
    assert_singleton_protocol_and_metadata(&actual);

    let expected: Vec<(String, JsonValue)> = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
                "partitionValues":{},
                "size":635,
                "modificationTime":1677811178336_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":false}",
                "tags":{
                    "INSERTION_TIME":"1677811178336000",
                    "MIN_INSERTION_TIME":"1677811178336000",
                    "MAX_INSERTION_TIME":"1677811178336000",
                    "OPTIMIZE_TARGET_SIZE":"268435456"
                },
                "deletionVector":{
                    "storageType":"u",
                    "pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA",
                    "offset":1,
                    "sizeInBytes":36,
                    "cardinality":2
                }
            }),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"testId",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":[],
                "configuration":{
                    "delta.enableDeletionVectors":"true",
                    "delta.columnMapping.mode":"none"
                },
                "createdTime":1677811175819_i64
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({
                "minReaderVersion":3,
                "minWriterVersion":7,
                "readerFeatures":["deletionVectors"],
                "writerFeatures":["deletionVectors"]
            }),
        ),
        (
            "remove".to_string(),
            serde_json::json!({
                "path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
                "deletionTimestamp":1677811194426_i64,
                "dataChange":true,
                "extendedFileMetadata":true,
                "partitionValues":{},
                "size":635,
                "tags":{
                    "INSERTION_TIME":"1677811178336000",
                    "MIN_INSERTION_TIME":"1677811178336000",
                    "MAX_INSERTION_TIME":"1677811178336000",
                    "OPTIMIZE_TARGET_SIZE":"268435456"
                }
            }),
        ),
    ];

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_commit_dedup_real_app_txn_no_checkpoint_full_to_json_payloads() {
    let merged = commit_dedup_merged_for_real_fixture("app-txn-no-checkpoint").await;
    let actual = full_action_payload_rows(&merged);
    assert_singleton_protocol_and_metadata(&actual);

    let expected: Vec<(String, JsonValue)> = vec![
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-01/part-00001-80996595-a345-43b7-b213-e247d6f091f7-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-01"},
                "size":810,
                "modificationTime":1713400714557_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":8,\"minValues\":{\"value\":4,\"id\":\"A\"},\"maxValues\":{\"id\":\"B\",\"value\":11},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-01/part-00001-8ebcaf8b-0f48-4213-98c9-5c2156d20a7e-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-01"},
                "size":810,
                "modificationTime":1713400714564_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":8,\"minValues\":{\"value\":4,\"id\":\"A\"},\"maxValues\":{\"value\":11,\"id\":\"B\"},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-02/part-00001-9a16b9f6-c12a-4609-a9c4-828eacb9526a-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-02"},
                "size":789,
                "modificationTime":1713400714564_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":3,\"minValues\":{\"value\":1,\"id\":\"A\"},\"maxValues\":{\"id\":\"B\",\"value\":3},\"nullCount\":{\"id\":0,\"value\":0}}"
            }),
        ),
        (
            "add".to_string(),
            serde_json::json!({
                "path":"modified=2021-02-02/part-00001-bfac5c74-426e-410f-ab74-21a64e518e9c-c000.snappy.parquet",
                "partitionValues":{"modified":"2021-02-02"},
                "size":789,
                "modificationTime":1713400714557_i64,
                "dataChange":true,
                "stats":"{\"numRecords\":3,\"minValues\":{\"id\":\"A\",\"value\":1},\"maxValues\":{\"value\":3,\"id\":\"B\"},\"nullCount\":{\"value\":0,\"id\":0}}"
            }),
        ),
        (
            "metaData".to_string(),
            serde_json::json!({
                "id":"dc67687f-4462-4bc9-9070-b53a58e4780e",
                "format":{"provider":"parquet","options":{}},
                "schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"modified\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns":["modified"],
                "configuration":{},
                "createdTime":1713400714555_i64
            }),
        ),
        (
            "protocol".to_string(),
            serde_json::json!({"minReaderVersion":1,"minWriterVersion":2}),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"my-app","version":3}),
        ),
        (
            "txn".to_string(),
            serde_json::json!({"appId":"my-app2","version":2}),
        ),
    ];

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn fsr_matches_scan_for_all_v2_checkpoint_fixtures() {
    // These fixtures are checked in as directories and cover each distinct v2 shape:
    // - classic v2 parquet checkpoint
    // - v2 parquet sidecars
    // - v2 json sidecars
    for table in [
        "v2-classic-parquet-struct-stats-only",
        "v2-json-sidecars-struct-stats-only",
        "v2-parquet-sidecars-struct-stats-only",
    ] {
        assert_fsr_matches_scan_paths(table).await;
    }
}

#[tokio::test]
async fn fsr_matches_scan_for_dv_and_non_dv_fixtures() {
    for table in [
        "table-with-dv-small",
        "table-without-dv-small",
        "with-short-dv",
    ] {
        assert_fsr_matches_scan_paths(table).await;
    }
}

#[tokio::test]
async fn fsr_matches_scan_for_v1_and_checkpoint_discovery_shapes() {
    for table in ["app-txn-checkpoint", "with_checkpoint_no_last_checkpoint"] {
        assert_fsr_matches_scan_paths(table).await;
    }
}
