//! Scans of adaptiveMetadata (AMT) tables, whose state at or below the latest `checkpoint` action
//! is read from that action's content tree.

use std::iter;
use std::sync::Arc;

use itertools::Itertools as _;
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::get_files_for_scan;
use crate::actions::{CheckpointAction, DomainMetadata, Sidecar};
use crate::arrow::array::{AsArray as _, StringArray};
use crate::arrow::datatypes::Int64Type;
use crate::content_tree::test_utils::{
    CLUSTERING_FIELD_ID, added_data_entry, clustering_mapping_domain, clustering_only_stats_at,
    clustering_only_stats_value_at, data_manifest_entry, write_manifest, write_manifest_with_stats,
    x_full_stats, x_stats,
};
use crate::content_tree::{ContentTreeNodeEntry, TrackingInfo, TrackingStatus};
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::expressions::{Predicate as Pred, Scalar, col, lit};
use crate::scan::{Scan, ScanMetadata};
use crate::schema::{SchemaRef, StructType, schema_ref};
use crate::table_features::ColumnMappingMode;
use crate::transaction::BASE_ADD_FILES_SCHEMA;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    CLUSTERED_AMT_FEATURES, adaptive_metadata_protocol_and_metadata, checkpoint_action_with_root,
    clustering_provider_field_id, commit_file_changes, commit_files, commit_root_manifest,
    commit_root_manifest_file, commit_update, create_adaptive_metadata_table, domain,
    protocol_and_metadata_lines, setup_adaptive_metadata_table, setup_clustered_table,
    write_amt_last_checkpoint_hint, write_commit_after_manifest_commit,
    write_manifest_commit_keeping_pm,
};
use crate::unit_test_utils::{
    assert_result_error_with_message, parse_json_batch, string_array_to_engine_data,
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
fn two_checkpoint_table() -> SyncEngine {
    let (engine, _) = setup_adaptive_metadata_table();
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
    write_manifest_commit_keeping_pm(&engine, &table_root(), 4, checkpoint).unwrap();
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
fn scan_reads_latest_tree_then_later_commits(#[case] version: Version, #[case] expected: &[&str]) {
    let engine = two_checkpoint_table();
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
    let engine = SyncEngine::in_memory();
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

#[cfg(feature = "declarative-plans")]
#[test]
fn declarative_scan_without_checkpoint_action_does_not_require_checkpoint_info_support() {
    // Given: an adaptive-metadata snapshot without a checkpoint action or usable CRC.
    let (engine, table_root) = setup_adaptive_metadata_table();
    let version = commit_files(&engine, &table_root, &["a.parquet"], &[]);
    let loaded = snapshot_at(&engine, version);
    // Force commit replay even when the writer provided an allFiles CRC.
    let snapshot = Arc::new(
        Snapshot::new(
            loaded.log_segment().clone(),
            loaded.table_configuration().clone(),
        )
        .unwrap(),
    );
    assert!(snapshot.base_crc().is_none());
    assert!(
        snapshot
            .latest_checkpoint_action(&engine)
            .unwrap()
            .is_none()
    );
    let scan = snapshot.clone().scan_builder().build().unwrap();
    assert_result_error_with_message(
        snapshot.reader().get_checkpoint_info(&engine),
        "Checkpoint info does not support adaptive metadata",
    );

    // When: declarative scan planning resolves checkpoint state internally.
    let plan = scan.declarative_metadata_scan_plan(&engine).unwrap();

    // Then: planning succeeds while the complete-metadata getter remains unsupported.
    assert!(plan.is_some());
    assert_result_error_with_message(
        snapshot.reader().get_checkpoint_info(&engine),
        "Checkpoint info does not support adaptive metadata",
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

/// Tree files are pruned by their `content_stats`, which are matched to the table's columns by
/// field ID, so a schema change after the checkpoint action still prunes them.
#[rstest]
#[case::checkpoint_schema(false)]
#[case::schema_changed_after_checkpoint(true)]
fn data_skipping_prunes_tree_files_by_content_stats(#[case] change_schema: bool) {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = [
        added_data_entry("low.parquet", 1, 1, 0, 0),
        added_data_entry("high.parquet", 1, 1, 1, 0),
    ];
    // `id` (field ID 1) is in `[1, 9]` in `low` and `[200, 300]` in `high`, never null.
    let stats = vec![
        x_stats(&x_full_stats(), &[1, 9, 1, 0]),
        x_stats(&x_full_stats(), &[200, 300, 1, 0]),
    ];
    let location = table_root().join("metadata/root.parquet").unwrap();
    let root = write_manifest_with_stats(&engine, location, &entries, &x_full_stats(), stats);
    let mut version = commit_root_manifest_file(&engine, &table_root(), root);
    if change_schema {
        // Drops `name`, keeping `id` and its field ID.
        let id = test_schema_flat_with_column_mapping()
            .field("id")
            .unwrap()
            .clone();
        let lines = protocol_and_metadata_lines(Arc::new(StructType::new_unchecked([id])), &[]);
        let data = parse_json_batch(lines.lines().collect::<Vec<_>>().into());
        write_commit_after_manifest_commit(&engine, &table_root(), version + 1, version, data)
            .unwrap();
        version += 1;
    }

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
    assert_eq!(paths, strings(&["high.parquet"]));
}

/// What scanning the table of `data_skipping_skips_leaf_manifests_by_root_content_stats` yields.
#[derive(Debug)]
enum LeafScan {
    /// The missing leaf is skipped, and the scan returns these paths.
    Paths(&'static [&'static str]),
    /// The missing leaf is read, which fails the scan.
    ReadsMissingLeaf,
}

/// Leaf manifests are skipped by their root entries' `content_stats`. The leaf ruled out by the
/// predicate does not exist, so reading it fails the scan: without a predicate, no leaf is skipped.
#[rstest]
#[case::predicate(
    Some(Pred::gt(col!("id"), lit(100i64))),
    LeafScan::Paths(&["high.parquet"])
)]
#[case::no_predicate(None, LeafScan::ReadsMissingLeaf)]
fn data_skipping_skips_leaf_manifests_by_root_content_stats(
    #[case] predicate: Option<Pred>,
    #[case] expected: LeafScan,
) {
    let (engine, _) = setup_adaptive_metadata_table();
    let mut low = data_manifest_entry("metadata/missing.parquet", 1, TrackingStatus::Added);
    low.tracking.first_row_id = Some(0);
    let high_path = "metadata/high.parquet";
    let high_leaf = write_manifest(
        &engine,
        table_root().join(high_path).unwrap(),
        &[added_data_entry("high.parquet", 1, 1, 0, 0)],
    );
    let mut high = data_manifest_entry(high_path, high_leaf.size as i64, TrackingStatus::Added);
    high.tracking.first_row_id = Some(10);
    // `id` (field ID 1) is in `[1, 9]` under `low` and `[200, 300]` under `high`, never null.
    let stats = vec![
        x_stats(&x_full_stats(), &[1, 9, 1, 0]),
        x_stats(&x_full_stats(), &[200, 300, 1, 0]),
    ];
    let location = table_root().join("metadata/root.parquet").unwrap();
    let root = write_manifest_with_stats(&engine, location, &[low, high], &x_full_stats(), stats);
    let version = commit_root_manifest_file(&engine, &table_root(), root);

    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .with_predicate(predicate.map(Arc::new))
        .build()
        .unwrap();
    let paths: KernelResult<Vec<String>> =
        get_files_for_scan(scan, &engine).map(|paths| paths.into_iter().sorted().collect());
    match expected {
        LeafScan::Paths(expected) => assert_eq!(paths.unwrap(), strings(expected)),
        LeafScan::ReadsMissingLeaf => assert!(
            paths.is_err(),
            "reading the missing leaf should fail: {paths:?}"
        ),
    }
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
fn scan_metadata_from_hint_before_checkpoint_action_rescans(
    #[case] existing_version: Version,
    #[case] empty_cache: bool,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table();
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

/// The `clusteringProvider` value the tests record in tree entries.
const LIQUID: &str = "liquid";

/// Writes a root manifest at `root_path` holding `entries`, each with its `clusteringProvider`
/// (none for `None`) stored under `field_id`.
fn write_clustered_root(
    engine: &SyncEngine,
    root_path: &str,
    entries: Vec<(ContentTreeNodeEntry, Option<&str>)>,
    field_id: i32,
) -> FileMeta {
    let content_stats = clustering_only_stats_at(field_id);
    let stats = entries
        .iter()
        .map(|(_, clustering)| match clustering {
            Some(value) => clustering_only_stats_value_at(field_id, Scalar::from(*value)),
            None => Scalar::Null(content_stats.clone().into()),
        })
        .collect();
    let entries: Vec<_> = entries.into_iter().map(|(entry, _)| entry).collect();
    let location = table_root().join(root_path).unwrap();
    write_manifest_with_stats(engine, location, &entries, &content_stats, stats)
}

/// A checkpoint action at `version` whose root manifest holds `entries`, recording each entry's
/// `clusteringProvider` stats (none for `None`) and carrying `domains` inline.
fn clustered_checkpoint(
    engine: &SyncEngine,
    root_path: &str,
    entries: Vec<(ContentTreeNodeEntry, Option<&str>)>,
    version: Version,
    domains: Vec<DomainMetadata>,
) -> CheckpointAction {
    let root = write_clustered_root(engine, root_path, entries, CLUSTERING_FIELD_ID);
    let mut checkpoint = checkpoint_action_with_root(root_path, root.size as i64, version).unwrap();
    checkpoint.domain_metadata = domains;
    checkpoint
}

/// Commits a [`clustered_checkpoint`] at `version`.
fn commit_clustered_checkpoint(
    engine: &SyncEngine,
    root_path: &str,
    entries: Vec<(ContentTreeNodeEntry, Option<&str>)>,
    version: Version,
    domains: Vec<DomainMetadata>,
) {
    let checkpoint = clustered_checkpoint(engine, root_path, entries, version, domains);
    write_manifest_commit_keeping_pm(engine, &table_root(), version, checkpoint).unwrap();
}

/// Commits at `version` a schema that keeps `id` and drops `name`.
fn commit_schema_dropping_name(engine: &SyncEngine, version: Version) {
    let id = test_schema_flat_with_column_mapping()
        .field("id")
        .unwrap()
        .clone();
    let lines = protocol_and_metadata_lines(Arc::new(StructType::new_unchecked([id])), &[]);
    let data = parse_json_batch(lines.lines().collect::<Vec<_>>().into());
    write_commit_after_manifest_commit(engine, &table_root(), version, version - 1, data).unwrap();
}

/// Asserts the `(path, clusteringProvider)` of each file scanned at `version`, sorted by path.
fn assert_scan_clustering(
    engine: &SyncEngine,
    version: Version,
    expected: &[(&str, Option<&str>)],
) {
    let mut rows = vec![];
    for scan_files in cached_scan_metadata(engine, snapshot_at(engine, version)) {
        let batch = scan_files.try_into_record_batch().unwrap();
        let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
        let fields = batch
            .column_by_name("fileConstantValues")
            .unwrap()
            .as_struct();
        let providers = fields
            .column_by_name("clusteringProvider")
            .unwrap()
            .as_string::<i32>();
        rows.extend(
            paths
                .iter()
                .zip(providers.iter())
                .map(|(path, provider)| (path.unwrap().to_string(), provider.map(str::to_string))),
        );
    }
    rows.sort();
    let expected: Vec<_> = expected
        .iter()
        .map(|(path, provider)| (path.to_string(), provider.map(str::to_string)))
        .collect();
    assert_eq!(rows, expected);
}

#[test]
fn scan_reads_clustering_provider_stored_under_the_assigned_id() {
    let (engine, _) = setup_clustered_table(&CLUSTERED_AMT_FEATURES, 2, None);
    let snapshot = commit_update(&engine, &table_root(), |builder| builder).unwrap();
    let assigned = clustering_provider_field_id(&engine, &snapshot)
        .unwrap()
        .unwrap();

    let entries = vec![
        (added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID)),
        (added_data_entry("b.parquet", 1, 1, 1, 0), None),
    ];
    let root = write_clustered_root(&engine, "metadata/root-2.parquet", entries, assigned);
    let version = commit_root_manifest_file(&engine, &table_root(), root);

    assert_scan_clustering(
        &engine,
        version,
        &[("a.parquet", Some(LIQUID)), ("b.parquet", None)],
    );
}

#[rstest]
fn scan_restores_clustering_provider_from_tree_stats(
    #[values(true, false)] mapped: bool,
    #[values(true, false)] change_schema: bool,
) {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = vec![
        (added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID)),
        (added_data_entry("b.parquet", 1, 1, 1, 0), None),
    ];
    let domains = if mapped {
        vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)]
    } else {
        vec![]
    };
    commit_clustered_checkpoint(&engine, "metadata/root.parquet", entries, 1, domains);
    // Changes the schema after the checkpoint, so the scan reads the tree without Delta stats.
    let version = if change_schema {
        commit_schema_dropping_name(&engine, 2);
        2
    } else {
        1
    };

    let provider = mapped.then_some(LIQUID);
    assert_scan_clustering(
        &engine,
        version,
        &[("a.parquet", provider), ("b.parquet", None)],
    );
}

#[test]
fn scan_uses_the_mapping_of_the_checkpoint_at_each_version() {
    let (engine, _) = setup_adaptive_metadata_table();
    let entry = || (added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID));
    commit_clustered_checkpoint(&engine, "metadata/root-1.parquet", vec![entry()], 1, vec![]);
    let domains = vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)];
    commit_clustered_checkpoint(
        &engine,
        "metadata/root-2.parquet",
        vec![entry()],
        2,
        domains,
    );

    assert_scan_clustering(&engine, 1, &[("a.parquet", None)]);
    assert_scan_clustering(&engine, 2, &[("a.parquet", Some(LIQUID))]);
}

#[test]
fn scan_with_malformed_adaptive_metadata_domain_names_it() {
    let (engine, _) = setup_adaptive_metadata_table();
    let entry = (added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID));
    let domains = vec![domain("delta.adaptiveMetadata", "not json")];
    commit_clustered_checkpoint(&engine, "metadata/root.parquet", vec![entry], 1, domains);

    let scan = snapshot_at(&engine, 1).scan_builder().build().unwrap();
    assert_result_error_with_message(
        get_files_for_scan(scan, &engine),
        "Invalid Checkpoint: delta.adaptiveMetadata configuration is not valid",
    );
}

#[test]
fn scan_restores_clustering_provider_from_json_commit_after_checkpoint() {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = vec![
        (added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID)),
        (added_data_entry("b.parquet", 1, 1, 1, 0), None),
    ];
    let domains = vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)];
    commit_clustered_checkpoint(&engine, "metadata/root.parquet", entries, 1, domains);
    let add = json!({
        "add": {
            "path": "c.parquet",
            "partitionValues": {},
            "size": 1,
            "modificationTime": 1,
            "dataChange": true,
            "clusteringProvider": LIQUID,
        }
    });
    let data = parse_json_batch(vec![add.to_string()].into());
    write_commit_after_manifest_commit(&engine, &table_root(), 2, 1, data).unwrap();

    assert_scan_clustering(
        &engine,
        2,
        &[
            ("a.parquet", Some(LIQUID)),
            ("b.parquet", None),
            ("c.parquet", Some(LIQUID)),
        ],
    );
}

#[test]
fn scan_restores_clustering_provider_from_last_checkpoint_hint() {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = vec![(added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID))];
    let domains = vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)];
    let checkpoint = clustered_checkpoint(&engine, "metadata/root.parquet", entries, 1, domains);
    // The hint requires the commit at its version, so the commit is written as well.
    write_manifest_commit_keeping_pm(&engine, &table_root(), 1, checkpoint.clone()).unwrap();
    write_amt_last_checkpoint_hint(&engine, &table_root(), 1, checkpoint).unwrap();

    assert_scan_clustering(&engine, 1, &[("a.parquet", Some(LIQUID))]);
}

#[test]
fn scan_reads_file_metadata_mapping_without_opening_domain_sidecars() {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = vec![(added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID))];
    let domains = vec![clustering_mapping_domain(CLUSTERING_FIELD_ID)];
    let mut checkpoint =
        clustered_checkpoint(&engine, "metadata/root.parquet", entries, 1, domains);
    // The scan must not open the sidecar: the mapping is inline.
    checkpoint.domain_metadata_sidecars =
        vec![Sidecar::new("missing.parquet".to_string(), 1, 1, None)];
    write_manifest_commit_keeping_pm(&engine, &table_root(), 1, checkpoint).unwrap();

    assert_scan_clustering(&engine, 1, &[("a.parquet", Some(LIQUID))]);
}

/// Writes the CRC file at `version` for the table's protocol and metadata, with
/// `fileMetadataFieldIds` set to `field_ids` when given.
fn write_crc(engine: &SyncEngine, version: Version, field_ids: Option<Value>) {
    let (protocol, metadata) = adaptive_metadata_protocol_and_metadata();
    let mut crc = json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "protocol": protocol,
        "metadata": metadata,
        // AMT tables enable In-Commit Timestamps, so the CRC must carry one.
        "inCommitTimestampOpt": 1,
        "lastManifestCommit": { "version": version, "contentRootVersion": version },
    });
    if let Some(field_ids) = field_ids {
        crc["fileMetadataFieldIds"] = field_ids;
    }
    let path = table_root()
        .join(&format!("_delta_log/{version:020}.crc"))
        .unwrap();
    engine
        .storage_handler()
        .put(&path, crc.to_string().into(), false)
        .unwrap();
}

#[rstest]
#[case::crc_field_wins_over_checkpoint_mapping(
    true,
    Some(json!({"clusteringProvider": CLUSTERING_FIELD_ID})),
    CLUSTERING_FIELD_ID + 1,
    Some(LIQUID)
)]
#[case::crc_without_field_maps_nothing(true, None, CLUSTERING_FIELD_ID, None)]
#[case::malformed_crc_field_falls_back_to_checkpoint_mapping(
    true,
    Some(json!({"clusteringProvider": "x"})),
    CLUSTERING_FIELD_ID,
    Some(LIQUID)
)]
#[case::no_crc_uses_checkpoint_mapping(false, None, CLUSTERING_FIELD_ID, Some(LIQUID))]
fn scan_file_metadata_mapping_comes_from_crc_at_snapshot_version(
    #[case] write_crc_file: bool,
    #[case] crc_field_ids: Option<Value>,
    #[case] checkpoint_field_id: i32,
    #[case] expected: Option<&str>,
) {
    let (engine, _) = setup_adaptive_metadata_table();
    let entries = vec![(added_data_entry("a.parquet", 1, 1, 0, 0), Some(LIQUID))];
    let domains = vec![clustering_mapping_domain(checkpoint_field_id)];
    commit_clustered_checkpoint(&engine, "metadata/root.parquet", entries, 1, domains);
    if write_crc_file {
        write_crc(&engine, 1, crc_field_ids);
    }

    assert_scan_clustering(&engine, 1, &[("a.parquet", expected)]);
}
