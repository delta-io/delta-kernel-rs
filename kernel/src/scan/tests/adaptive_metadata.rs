//! Scans of adaptiveMetadata (AMT) tables, whose state at or below the latest `checkpoint` action
//! is read from that action's content tree.

use std::iter;
use std::sync::Arc;

use itertools::Itertools as _;
use rstest::rstest;
use serde_json::json;
use test_utils::add_commit;
use url::Url;

use super::get_files_for_scan;
use crate::arrow::array::{AsArray as _, StringArray};
use crate::arrow::datatypes::Int64Type;
use crate::content_tree::test_utils::{added_data_entry, data_manifest_entry, write_manifest};
use crate::content_tree::{ContentTreeNodeEntry, TrackingInfo, TrackingStatus};
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::expressions::{col, lit, Predicate as Pred, Scalar};
use crate::object_store::memory::InMemory;
use crate::scan::{Scan, ScanMetadata};
use crate::schema::{schema_ref, SchemaRef};
use crate::table_features::ColumnMappingMode;
use crate::transaction::BASE_ADD_FILES_SCHEMA;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    checkpoint_action_with_root, checkpoint_line, commit_file_changes, commit_files,
    commit_root_manifest, commit_root_manifest_file, create_adaptive_metadata_table,
    setup_adaptive_metadata_table,
};
use crate::unit_test_utils::{
    assert_result_error_with_message, string_array_to_engine_data,
    test_schema_flat_with_column_mapping,
};
use crate::{Engine, EngineData, FileMeta, KernelResult, Snapshot, Version};

const TABLE_ROOT: &str = "memory:///";

fn table_root() -> Url {
    Url::parse(TABLE_ROOT).unwrap()
}

fn deleted_entry(path: &str) -> ContentTreeNodeEntry {
    let mut entry = added_data_entry(path, 1, 1, 0, 0);
    entry.tracking.status = TrackingStatus::Deleted;
    entry
}

/// A table whose log is:
/// - v0: protocol and metadata;
/// - v1: an add of `stale.parquet`, which no tree lists;
/// - v2: a checkpoint action whose tree lists `a`, `b`, and a deleted `gone`;
/// - v3: an add of `g` and a remove of `a`;
/// - v4: a checkpoint action whose tree lists `b`, `g`, and `h`.
async fn two_checkpoint_table() -> SyncEngine {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    create_adaptive_metadata_table(&engine, &table_root(), &[]);
    commit_files(&engine, &table_root(), &["stale.parquet"], &[]);
    let tree1 = [
        added_data_entry("a.parquet", 1, 1, 0, 0),
        deleted_entry("gone.parquet"),
        added_data_entry("b.parquet", 1, 1, 1, 0),
    ];
    commit_root_manifest(&engine, &table_root(), "metadata/root-2.parquet", &tree1);
    commit_files(&engine, &table_root(), &["g.parquet"], &["a.parquet"]);

    // A root commit refuses commits pending since the last checkpoint action, so the second
    // checkpoint action, which folds in v3, is written directly.
    let tree2 = [
        added_data_entry("b.parquet", 1, 1, 1, 0),
        added_data_entry("g.parquet", 1, 1, 2, 2),
        added_data_entry("h.parquet", 1, 1, 3, 3),
    ];
    let root_path = "metadata/root-4.parquet";
    let root = write_manifest(&engine, table_root().join(root_path).unwrap(), &tree2);
    let checkpoint = checkpoint_action_with_root(root_path, root.size as i64, 4).unwrap();
    add_commit(TABLE_ROOT, store.as_ref(), 4, checkpoint_line(&checkpoint))
        .await
        .unwrap();
    engine
}

fn snapshot_at(engine: &SyncEngine, version: Version) -> Arc<Snapshot> {
    Snapshot::builder_for(TABLE_ROOT)
        .at_version(version)
        .build(engine)
        .unwrap()
}

fn scan_paths(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<String> {
    let scan = snapshot.scan_builder().build().unwrap();
    get_files_for_scan(scan, engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

#[rstest]
#[case::at_first_checkpoint(2, &["a.parquet", "b.parquet"])]
#[case::commits_after_first_checkpoint(3, &["b.parquet", "g.parquet"])]
#[case::at_second_checkpoint(4, &["b.parquet", "g.parquet", "h.parquet"])]
#[tokio::test]
async fn scan_reads_latest_tree_then_later_commits(
    #[case] version: Version,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table().await;
    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, version)),
        strings(expected)
    );
}

#[test]
fn scan_reads_leaf_manifests() {
    let (engine, _) = setup_adaptive_metadata_table();
    let leaf_path = "metadata/leaf.parquet";
    let leaf = write_manifest(
        &engine,
        table_root().join(leaf_path).unwrap(),
        &[
            added_data_entry("l1.parquet", 1, 1, 0, 0),
            added_data_entry("l2.parquet", 1, 1, 1, 0),
        ],
    );
    let mut leaf_entry = data_manifest_entry(leaf_path, leaf.size as i64, TrackingStatus::Added);
    leaf_entry.tracking = TrackingInfo {
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        first_row_id: Some(10),
        ..leaf_entry.tracking
    };
    let root = [added_data_entry("r.parquet", 1, 1, 0, 1), leaf_entry];
    let version = commit_root_manifest(&engine, &table_root(), "metadata/root.parquet", &root);

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, version)),
        strings(&["l1.parquet", "l2.parquet", "r.parquet"])
    );
}

#[test]
fn scan_of_partitioned_table_with_checkpoint_action_is_unsupported() {
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    create_adaptive_metadata_table(&engine, &table_root(), &["id"]);
    let version = commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root.parquet",
        &[added_data_entry("a.parquet", 1, 1, 0, 0)],
    );
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(
        scan.scan_metadata(&engine).map(|_| ()),
        r#"partition columns: ["id"]"#,
    );
}

/// A scan entry point that does not yet read the checkpoint action.
type EntryPoint = fn(&Scan, Arc<dyn Engine>) -> KernelResult<()>;

#[rstest]
#[case::parallel(
    "parallel_scan_metadata",
    (|scan, engine| scan.parallel_scan_metadata(engine).map(|_| ())) as EntryPoint
)]
#[cfg_attr(
    feature = "declarative-plans",
    case::declarative(
        "declarative_metadata_scan_plan",
        (|scan, engine| scan.declarative_metadata_scan_plan(engine.as_ref()).map(|_| ())) as EntryPoint
    )
)]
fn scan_entry_point_without_checkpoint_action_support_is_unsupported(
    #[case] name: &str,
    #[case] entry_point: EntryPoint,
) {
    let (engine, table_root) = setup_adaptive_metadata_table();
    let version = commit_root_manifest(
        &engine,
        &table_root,
        "metadata/root.parquet",
        &[added_data_entry("a.parquet", 1, 1, 0, 0)],
    );
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(
        entry_point(&scan, Arc::new(engine)),
        &format!(
            "{name} does not yet read the adaptiveMetadata checkpoint action at version {version}"
        ),
    );
}

#[test]
fn scan_with_missing_root_manifest_names_it() {
    let (engine, _) = setup_adaptive_metadata_table();
    let file = FileMeta {
        location: table_root().join("metadata/root.parquet").unwrap(),
        last_modified: 0,
        size: 1,
    };
    let version = commit_root_manifest_file(&engine, &table_root(), file);
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(get_files_for_scan(scan, &engine), "metadata/root.parquet");
}

/// Add-file write metadata for each `(path, min, max)`, with stats bounding the `id` column
/// (physical name `phys_id`) to `[min, max]`.
fn adds_with_id_ranges(engine: &SyncEngine, files: &[(&str, i64, i64)]) -> Box<dyn EngineData> {
    let schema = schema_ref! {
        ..(BASE_ADD_FILES_SCHEMA.fields().filter(|f| f.name() != "stats")),
        nullable "stats": {
            nullable "numRecords": LONG,
            nullable "nullCount": { nullable "phys_id": LONG },
            nullable "minValues": { nullable "phys_id": LONG },
            nullable "maxValues": { nullable "phys_id": LONG },
            nullable "tightBounds": BOOLEAN,
        },
    };
    let lines: StringArray = files
        .iter()
        .map(|(path, min, max)| {
            json!({
                "path": path,
                "partitionValues": {},
                "size": 1,
                "modificationTime": 1,
                "stats": {
                    "numRecords": 1,
                    "nullCount": { "phys_id": 0 },
                    "minValues": { "phys_id": min },
                    "maxValues": { "phys_id": max },
                    "tightBounds": true,
                },
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .into();
    engine
        .json_handler()
        .parse_json(string_array_to_engine_data(lines), schema)
        .unwrap()
}

#[test]
fn data_skipping_prunes_commit_files_but_keeps_tree_files_without_stats() {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root.parquet",
        &[added_data_entry("tree.parquet", 1, 1, 0, 0)],
    );
    let adds = adds_with_id_ranges(
        &engine,
        &[("low.parquet", 1, 5), ("high.parquet", 200, 300)],
    );
    let version = commit_file_changes(&engine, &table_root(), Some(adds), &[]);

    let predicate = Arc::new(Pred::gt(col!("id"), lit(100i64)));
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let paths: Vec<_> = get_files_for_scan(scan, &engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect();
    assert_eq!(paths, strings(&["high.parquet", "tree.parquet"]));
}

/// The scan metadata of `snapshot`, with selection vectors applied, as cached by a caller of
/// `scan_metadata_from`.
fn cached_scan_metadata(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<Box<dyn EngineData>> {
    let scan = snapshot.scan_builder().build().unwrap();
    scan.scan_metadata(engine)
        .unwrap()
        .map(|metadata| {
            let ScanMetadata { scan_files, .. } = metadata.unwrap();
            scan_files.apply_selection_vector().unwrap()
        })
        .collect()
}

#[rstest]
// From before the checkpoint action, the cached rows (here deliberately empty) are discarded and
// the table is fully rescanned: advancing commit by commit would miss `b`.
#[case::before_checkpoint_action(1, true, &["b.parquet", "g.parquet"])]
#[case::at_checkpoint_action(2, false, &["b.parquet", "g.parquet"])]
#[tokio::test]
async fn scan_metadata_from_hint_before_checkpoint_action_rescans(
    #[case] existing_version: Version,
    #[case] empty_cache: bool,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table().await;
    let existing_data = if empty_cache {
        vec![]
    } else {
        cached_scan_metadata(&engine, snapshot_at(&engine, existing_version))
    };
    let scan = snapshot_at(&engine, 3).scan_builder().build().unwrap();
    let mut paths = vec![];
    for metadata in scan
        .scan_metadata_from(&engine, existing_version, existing_data, None)
        .unwrap()
    {
        paths = metadata
            .unwrap()
            .visit_scan_files(paths, |paths: &mut Vec<String>, file| {
                paths.push(file.path.to_string())
            })
            .unwrap();
    }
    paths.sort();
    assert_eq!(paths, strings(expected));
}

/// Tree locations are decoded paths, so a data file literally named `c%20d.parquet` is found only
/// if `add.path` encodes its `%`: `Scan::execute` joins each scan file's path onto the table root.
#[test]
fn scan_execute_reads_files_whose_locations_need_encoding() {
    let (engine, table_root) = setup_adaptive_metadata_table();
    let physical_schema: SchemaRef = Arc::new(
        test_schema_flat_with_column_mapping()
            .make_physical(ColumnMappingMode::Id)
            .unwrap(),
    );
    let files = [
        ("a b.parquet", 1, "first"),
        ("dir/c%20d.parquet", 2, "second"),
    ];
    let entries: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(row_id, (location, id, name))| {
            let data = engine
                .evaluation_handler()
                .create_many(
                    physical_schema.clone(),
                    vec![vec![Scalar::from(*id as i64), Scalar::from(*name)]],
                )
                .unwrap();
            // Escape each segment independently of the code under test.
            let mut url = table_root.clone();
            url.path_segments_mut()
                .unwrap()
                .pop_if_empty()
                .extend(location.split('/'));
            let size = engine
                .parquet_handler()
                .write_parquet_file(url, Box::new(iter::once(Ok(data))))
                .unwrap();
            added_data_entry(location, size as i64, 1, row_id as i64, 0)
        })
        .collect();
    let version = commit_root_manifest(&engine, &table_root, "metadata/root.parquet", &entries);

    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    let rows: Vec<(i64, String)> = scan
        .execute(Arc::new(engine))
        .unwrap()
        .flat_map(|data| {
            let batch = data.unwrap().try_into_record_batch().unwrap();
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let names = batch.column_by_name("name").unwrap().as_string::<i32>();
            ids.values()
                .iter()
                .zip(names.iter().flatten())
                .map(|(id, name)| (*id, name.to_string()))
                .collect::<Vec<_>>()
        })
        .sorted()
        .collect();

    assert_eq!(
        rows,
        vec![(1, "first".to_string()), (2, "second".to_string())]
    );
}
