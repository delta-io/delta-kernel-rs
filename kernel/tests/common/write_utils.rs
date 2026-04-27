//! Shared helpers for integration tests that exercise Delta write paths.
//!
//! Each integration test file compiles as its own test binary and uses only a subset of these
//! helpers; the blanket `#![allow(dead_code)]` suppresses the per-binary dead-code warnings that
//! the unused subset would otherwise generate.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use delta_kernel::arrow::array::{Int32Array, StructArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::DynObjectStore;
use delta_kernel::parquet::file::reader::{FileReader, SerializedFileReader};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Engine, Snapshot, Version};
use serde_json::json;
use test_utils::{create_add_files_metadata, create_table, engine_store_setup};
use url::Url;
use uuid::Uuid;

/// Deterministic placeholder for test commit JSON comparisons.
pub const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// Single-column nullable `id: int` schema.
pub fn get_simple_schema() -> SchemaRef {
    Arc::new(StructType::try_new(vec![StructField::new("id", DataType::INTEGER, true)]).unwrap())
}

/// Builds a `RecordBatch` matching [`get_simple_schema`] from a vector of `id` values.
pub fn simple_id_batch(schema: &SchemaRef, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into_arrow().unwrap()),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap()
}

/// Returns the native parquet `field_id` for a field at the given physical path in a parquet file,
/// or `None` if the field has no `field_id` set.
///
/// Panics if the file cannot be read or the physical path doesn't exist in the parquet schema.
pub fn get_parquet_field_id(
    parquet_file: &std::path::Path,
    physical_path: &[String],
) -> Option<i32> {
    let file = std::fs::File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let root = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .clone();

    let mut current = &root;
    for name in physical_path {
        current = current
            .get_fields()
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("parquet schema missing field '{name}'"));
    }

    let info = current.get_basic_info();
    info.has_id().then(|| info.id())
}

/// Validate that `commitInfo["txnId"]` is a valid UUID.
pub fn validate_txn_id(commit_info: &serde_json::Value) {
    let txn_id = commit_info["txnId"]
        .as_str()
        .expect("txnId should be present in commitInfo");
    Uuid::parse_str(txn_id).expect("txnId should be valid UUID format");
}

/// Validate that `commitInfo["timestamp"]` is within the last two days.
pub fn validate_timestamp(commit_info: &serde_json::Value) {
    let timestamp = commit_info["timestamp"]
        .as_i64()
        .expect("timestamp should be present in commitInfo");
    let current_ts: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let two_days_ms = Duration::from_secs(2 * 24 * 60 * 60).as_millis() as i64;
    assert!(
        (timestamp <= current_ts && timestamp > current_ts - two_days_ms),
        "commit timestamp should be at most 2 days behind current system time: got {timestamp}, now {current_ts}"
    );
}

/// Check that the timestamps in commit_info and add actions are within 10s of SystemTime::now().
pub fn check_action_timestamps<'a>(
    parsed_commits: impl Iterator<Item = &'a serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()
        .unwrap();

    parsed_commits.for_each(|commit| {
        if let Some(commit_info_ts) = &commit.pointer("/commitInfo/timestamp") {
            assert!((now - commit_info_ts.as_i64().unwrap()).abs() < 10_000);
        }
        if let Some(add_ts) = &commit.pointer("/add/modificationTime") {
            assert!((now - add_ts.as_i64().unwrap()).abs() < 10_000);
        }
    });

    Ok(())
}

/// List all the files at `path`, assert that exactly two are parquet files with matching sizes,
/// and return that size.
pub async fn get_and_check_all_parquet_sizes(store: Arc<DynObjectStore>, path: &str) -> u64 {
    use futures::stream::StreamExt;
    let files: Vec<_> = store.list(Some(&Path::from(path))).collect().await;
    let parquet_files = files
        .into_iter()
        .filter(|f| match f {
            Ok(f) => f.location.extension() == Some("parquet"),
            Err(_) => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(parquet_files.len(), 2);
    let size = parquet_files.first().unwrap().as_ref().unwrap().size;
    assert!(parquet_files
        .iter()
        .all(|f| f.as_ref().unwrap().size == size));
    size
}

/// Write two small parquet files to the table and verify commit-level post-commit stats.
pub async fn write_data_and_check_result_and_stats(
    table_url: Url,
    schema: SchemaRef,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    expected_since_commit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let committer = Box::new(FileSystemCommitter::new());
    let mut txn = snapshot
        .transaction(committer, engine.as_ref())?
        .with_data_change(true);

    // create two new arrow record batches to append
    let append_data = [[1, 2, 3], [4, 5, 6]].map(|data| -> DeltaResult<_> {
        let data = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int32Array::from(data.to_vec()))],
        )?;
        Ok(Box::new(ArrowEngineData::new(data)))
    });

    // write data out by spawning async tasks to simulate executors
    let write_context = Arc::new(txn.unpartitioned_write_context().unwrap());
    let tasks = append_data.into_iter().map(|data| {
        // arc clones
        let engine = engine.clone();
        let write_context = write_context.clone();
        tokio::task::spawn(async move {
            engine
                .write_parquet(data.as_ref().unwrap(), write_context.as_ref())
                .await
        })
    });

    let add_files_metadata = futures::future::join_all(tasks).await.into_iter().flatten();
    for meta in add_files_metadata {
        txn.add_files(meta?);
    }

    // commit!
    match txn.commit(engine.as_ref())? {
        CommitResult::CommittedTransaction(committed) => {
            assert_eq!(committed.commit_version(), expected_since_commit as Version);
            assert_eq!(
                committed.post_commit_stats().commits_since_checkpoint,
                expected_since_commit
            );
            assert_eq!(
                committed.post_commit_stats().commits_since_log_compaction,
                expected_since_commit
            );
        }
        _ => panic!("Commit should have succeeded"),
    };

    Ok(())
}

/// A simple schema with a single nullable INTEGER column named `number`.
pub fn get_simple_int_schema() -> Arc<StructType> {
    Arc::new(StructType::try_new(vec![StructField::nullable("number", DataType::INTEGER)]).unwrap())
}

/// Write a metadata-update commit that sets a table property on the existing table.
/// Returns a fresh snapshot reflecting the new commit.
/// Used in tests as a hack to set table properties when create table doesn't support the property.
pub fn set_table_properties(
    table_path: &str,
    table_url: &Url,
    engine: &dyn Engine,
    current_version: Version,
    properties: &[(&str, &str)],
) -> Result<Arc<Snapshot>, Box<dyn std::error::Error>> {
    let v0_path = std::path::Path::new(table_path).join("_delta_log/00000000000000000000.json");
    let mut meta: serde_json::Value = std::fs::read_to_string(&v0_path)?
        .lines()
        .find_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .filter(|v| v.get("metaData").is_some())
        })
        .expect("version 0 should contain a metaData action");

    for &(key, value) in properties {
        meta["metaData"]["configuration"][key] = json!(value);
    }

    let new_commit = std::path::Path::new(table_path)
        .join(format!("_delta_log/{:020}.json", current_version + 1));
    std::fs::write(&new_commit, serde_json::to_string(&meta)?)?;
    Ok(Snapshot::builder_for(table_url.clone()).build(engine)?)
}

/// Assert that the snapshot's column mapping mode matches the given `cm_mode` string,
/// and return the resolved mode.
pub fn assert_column_mapping_mode(snapshot: &Snapshot, cm_mode: &str) -> ColumnMappingMode {
    let expected = match cm_mode {
        "none" => ColumnMappingMode::None,
        "name" => ColumnMappingMode::Name,
        "id" => ColumnMappingMode::Id,
        _ => panic!("unexpected cm_mode: {cm_mode}"),
    };
    let actual = snapshot
        .table_properties()
        .column_mapping_mode
        .expect("column mapping mode should be set");
    assert_eq!(actual, expected);
    actual
}

/// Resolve a nested column inside a [`StructArray`] by walking the given field-name path,
/// and downcast the leaf to the requested array type.
pub fn resolve_struct_field<'a, T: 'static>(root: &'a StructArray, path: &[String]) -> &'a T {
    assert!(!path.is_empty(), "path must be non-empty");
    let mut current: &StructArray = root;
    for (i, name) in path.iter().enumerate() {
        let col = current
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing field: {name}"));
        if i == path.len() - 1 {
            return col
                .as_any()
                .downcast_ref::<T>()
                .expect("leaf array type mismatch");
        }
        current = col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap_or_else(|| panic!("expected StructArray at field: {name}"));
    }
    unreachable!()
}

/// Navigate into a nested JSON value by following a sequence of object keys.
/// E.g. `resolve_json_path(stats, &["address", "street"])` returns `stats["address"]["street"]`.
pub fn resolve_json_path<'a>(
    root: &'a serde_json::Value,
    path: &[String],
) -> &'a serde_json::Value {
    path.iter().fold(root, |v, key| &v[key])
}

/// Assert that `stats["minValues"]` and `stats["maxValues"]` at the given physical path equal the
/// expected values.
pub fn assert_min_max_stats(
    stats: &serde_json::Value,
    physical_path: &[String],
    expected_min: impl Into<serde_json::Value>,
    expected_max: impl Into<serde_json::Value>,
) {
    assert_eq!(
        *resolve_json_path(&stats["minValues"], physical_path),
        expected_min.into()
    );
    assert_eq!(
        *resolve_json_path(&stats["maxValues"], physical_path),
        expected_max.into()
    );
}

/// Creates a table with deletion vector support and writes the specified files.
pub async fn create_dv_table_with_files(
    table_name: &str,
    schema: Arc<StructType>,
    file_paths: &[&str],
) -> Result<
    (
        Arc<DynObjectStore>,
        Arc<dyn delta_kernel::Engine>,
        Url,
        Vec<String>,
    ),
    Box<dyn std::error::Error>,
> {
    let (store, engine, table_url) = engine_store_setup(table_name, None);
    let engine = Arc::new(engine);

    // Create table with DV support (protocol 3/7 with deletionVectors feature)
    create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true, // use_37_protocol
        vec!["deletionVectors"],
        vec!["deletionVectors"],
    )
    .await?;

    // Write files
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let add_files_schema = txn.add_files_schema();

    // Build metadata for all files at once
    let files: Vec<(&str, i64, i64, i64)> = file_paths
        .iter()
        .enumerate()
        .map(|(i, &path)| {
            (
                path,
                1024 + i as i64 * 100, // size
                1000000 + i as i64,    // mod_time
                3,                     // num_records
            )
        })
        .collect();
    let metadata = create_add_files_metadata(add_files_schema, files)?;
    txn.add_files(metadata);

    let _ = txn.commit(engine.as_ref())?;

    let paths: Vec<String> = file_paths.iter().map(|&s| s.to_string()).collect();
    Ok((store, engine, table_url, paths))
}

/// Extracts scan files from a snapshot for use in deletion vector updates.
pub fn get_scan_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn delta_kernel::Engine,
) -> DeltaResult<Vec<FilteredEngineData>> {
    let scan = snapshot.scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>()?;

    Ok(all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect())
}
