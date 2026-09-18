use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use rstest::rstest;
use test_utils::LoggingTest;

use super::{
    replay_schema, table_changes_action_iter, table_changes_action_iter_with_mode,
    CommitBatchReader, LogReplayScanner, PreparePhaseVisitor, TableChangesScanMetadata,
};
use crate::actions::{Add, Cdc, CommitInfo, Metadata, Protocol, Remove};
use crate::arrow::array::{RecordBatch, StringArray};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::expressions::{column_expr, ArrayData, BinaryPredicateOp, ColumnName, Scalar};
use crate::log_segment::LogSegment;
use crate::metrics::{MeteredDeltaEngine, MetricEvent};
use crate::path::ParsedLogPath;
use crate::scan::state::DvInfo;
use crate::scan::PhysicalPredicate;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::table_changes::scan_file::scan_metadata_to_scan_file;
use crate::table_changes::test_utils::{
    row_tracking_metadata, row_tracking_table_config, test_deletion_vector,
};
use crate::table_changes::CdfMode;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ColumnMappingMode, TableFeature};
use crate::table_properties::{ENABLE_ROW_TRACKING, ROW_TRACKING_SUSPENDED};
use crate::utils::test_utils::{
    assert_result_error_with_message, install_thread_local_metrics_reporter, Action,
    CapturingReporter, LocalMockTable,
};
use crate::{
    DeltaResult, DeltaResultIterator, Engine, EngineData, Error, EvaluationHandler,
    FileDataReadResultIterator, FileMeta, FilteredEngineData, JsonHandler, ParquetHandler,
    Predicate, PredicateRef, RowVisitor, StorageHandler, Version,
};

fn get_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]))
}

fn get_default_table_config(table_root: &url::Url) -> TableConfiguration {
    let metadata = Metadata::try_new(
        None,
        None,
        get_schema(),
        vec![],
        0,
        HashMap::from([
            ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
            ("delta.columnMapping.mode".to_string(), "none".to_string()),
        ]),
    )
    .unwrap();
    // CDF requires min_writer_version = 4
    let protocol = Protocol::try_new_legacy(1, 4).unwrap();
    TableConfiguration::try_new(metadata, protocol, table_root.clone(), 0).unwrap()
}

/// Helper to create a Metadata action with the given schema and configuration
fn metadata_action(schema: SchemaRef, configuration: HashMap<String, String>) -> Action {
    Action::Metadata(
        Metadata::try_new(None, None, schema.clone(), vec![], 0, configuration).unwrap(),
    )
}

/// Helper to create a Metadata action with row tracking enabled
fn metadata_with_row_tracking(schema: SchemaRef) -> Action {
    Action::Metadata(row_tracking_metadata(schema))
}

/// Runs row-tracking log replay over all commits of `mock_table` against the given end schema.
fn execute_row_tracking(
    engine: Arc<dyn Engine>,
    mock_table: &LocalMockTable,
    end_schema: SchemaRef,
) -> DeltaResult<Vec<TableChangesScanMetadata>> {
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)?.into_iter();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = row_tracking_table_config(table_root_url, get_schema());
    table_changes_action_iter_with_mode(
        engine,
        &table_config,
        commits,
        end_schema,
        None,
        CdfMode::RowTracking,
    )?
    .try_collect()
}

/// Helper to create a Metadata action with CDF enabled
fn metadata_with_cdf(schema: SchemaRef) -> Action {
    metadata_action(
        schema,
        HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
    )
}

/// Helper to create a Protocol action
fn protocol_action(
    min_reader: i32,
    min_writer: i32,
    reader_features: Option<Vec<TableFeature>>,
    writer_features: Option<Vec<TableFeature>>,
) -> Action {
    Action::Protocol(
        Protocol::try_new(min_reader, min_writer, reader_features, writer_features).unwrap(),
    )
}

/// Helper to execute table_changes_action_iter for a specific version range
fn execute_table_changes(
    engine: Arc<dyn Engine>,
    mock_table: &LocalMockTable,
    start_version: Version,
    end_version: Option<Version>,
) -> DeltaResult<Vec<TableChangesScanMetadata>> {
    let commits = get_segment(
        engine.as_ref(),
        mock_table.table_root(),
        start_version,
        end_version,
    )?
    .into_iter();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    table_changes_action_iter(engine, &table_config, commits, get_schema(), None)?.try_collect()
}

/// Helper to assert midstream failure pattern:
/// - Reading v0 alone succeeds
/// - Reading v0-v1 fails with ChangeDataFeedUnsupported
/// - Reading v1 alone fails with ChangeDataFeedUnsupported
fn assert_midstream_failure(engine: Arc<dyn Engine>, mock_table: &LocalMockTable) {
    // Reading just the first commit (0 to 0) should succeed
    let res_v0 = execute_table_changes(engine.clone(), mock_table, 0, Some(0));
    assert!(res_v0.is_ok(), "Reading version 0 alone should succeed");

    // Reading commits 0-1 should fail
    let res_v0_v1 = execute_table_changes(engine.clone(), mock_table, 0, Some(1));
    assert!(
        matches!(res_v0_v1, Err(Error::ChangeDataFeedUnsupported(_))),
        "Reading versions 0-1 should fail"
    );

    // Reading just commit 1 should also fail
    let res_v1 = execute_table_changes(engine, mock_table, 1, Some(1));
    assert!(
        matches!(res_v1, Err(Error::ChangeDataFeedUnsupported(_))),
        "Reading version 1 alone should fail"
    );
}

fn get_segment(
    engine: &dyn Engine,
    path: &Path,
    start_version: Version,
    end_version: impl Into<Option<Version>>,
) -> DeltaResult<Vec<ParsedLogPath>> {
    let table_root = url::Url::from_directory_path(path).unwrap();
    let log_root = table_root.join("_delta_log/")?;
    let log_segment = LogSegment::for_table_changes(
        engine.storage_handler().as_ref(),
        log_root,
        start_version,
        end_version,
    )?;
    Ok(log_segment.listed.ascending_commit_files)
}

fn result_to_sv(iter: impl Iterator<Item = DeltaResult<TableChangesScanMetadata>>) -> Vec<bool> {
    iter.map_ok(|scan_metadata| scan_metadata.selection_vector.into_iter())
        .flatten_ok()
        .try_collect()
        .unwrap()
}

fn file_path_batch(path: &str, row_count: usize) -> Box<dyn EngineData> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "_file",
        ArrowDataType::Utf8,
        false,
    )]));
    let paths = Arc::new(StringArray::from(vec![path; row_count]));
    let batch = RecordBatch::try_new(schema, vec![paths]).unwrap();
    Box::new(ArrowEngineData::new(batch))
}

fn nullable_file_path_batch(path: Option<&str>) -> Box<dyn EngineData> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "_file",
        ArrowDataType::Utf8,
        true,
    )]));
    let paths = Arc::new(StringArray::from(vec![path]));
    let batch = RecordBatch::try_new(schema, vec![paths]).unwrap();
    Box::new(ArrowEngineData::new(batch))
}

fn action_batches_for_commit(
    engine: &dyn Engine,
    commit: &ParsedLogPath,
) -> FileDataReadResultIterator {
    engine
        .json_handler()
        .read_json_files(
            std::slice::from_ref(&commit.location),
            replay_schema(PreparePhaseVisitor::schema()).unwrap(),
            None,
        )
        .unwrap()
}

fn commit_batch_reader(
    commits: &[ParsedLogPath],
    batches: FileDataReadResultIterator,
) -> CommitBatchReader {
    let file_indices = commits
        .iter()
        .enumerate()
        .map(|(index, commit)| (commit.location.location.to_string(), index))
        .collect();
    CommitBatchReader::new(Arc::new(file_indices), batches)
}

fn test_commits(count: u64) -> Vec<ParsedLogPath> {
    let table_root = url::Url::parse("memory:///").unwrap();
    (0..count)
        .map(|version| ParsedLogPath::create_parsed_published_commit(&table_root, version))
        .collect()
}

struct BatchingEngine {
    inner: Arc<SyncEngine>,
    json_handler: Arc<BatchingJsonHandler>,
}

impl Engine for BatchingEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.inner.parquet_handler()
    }

    #[cfg(feature = "declarative-plans")]
    fn plan_executor(&self) -> Arc<dyn crate::plans::PlanExecutor> {
        self.inner.plan_executor()
    }
}

struct BatchingJsonHandler {
    inner: Arc<dyn JsonHandler>,
    batch_sizes: [usize; 2],
    requested_files: Mutex<Vec<Vec<String>>>,
    batches_read: [Arc<AtomicUsize>; 2],
    prepare_batches_live: Arc<AtomicUsize>,
    prepare_batches_peak: Arc<AtomicUsize>,
    readers_live: Arc<AtomicUsize>,
    error_at: Option<(usize, usize)>,
}

impl BatchingJsonHandler {
    fn engine(
        batch_sizes: [usize; 2],
        error_at: Option<(usize, usize)>,
    ) -> (Arc<dyn Engine>, Arc<Self>) {
        let inner = Arc::new(SyncEngine::new());
        let handler = Arc::new(Self {
            inner: inner.json_handler(),
            batch_sizes,
            requested_files: Mutex::default(),
            batches_read: std::array::from_fn(|_| Arc::default()),
            prepare_batches_live: Arc::default(),
            prepare_batches_peak: Arc::default(),
            readers_live: Arc::default(),
            error_at,
        });
        let engine = Arc::new(BatchingEngine {
            inner,
            json_handler: handler.clone(),
        });
        (engine, handler)
    }
}

impl JsonHandler for BatchingJsonHandler {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let pass = {
            let mut calls = self.requested_files.lock().unwrap();
            calls.push(files.iter().map(|file| file.location.to_string()).collect());
            calls.len() - 1
        };
        let batch_size = self.batch_sizes[pass];
        let batches = self.inner.read_json_files(files, schema, predicate)?;
        let batches = batches
            .map(move |batch| -> DeltaResult<_> {
                let batch: RecordBatch = ArrowEngineData::try_from_engine_data(batch?)?.into();
                Ok((0..batch.num_rows())
                    .step_by(batch_size)
                    .map(move |offset| {
                        batch.slice(offset, batch_size.min(batch.num_rows() - offset))
                    }))
            })
            .flatten_ok();
        let read = self.batches_read[pass].clone();
        let live = self.prepare_batches_live.clone();
        let peak = self.prepare_batches_peak.clone();
        let error_at = self.error_at;
        let batches = batches.map(move |batch| -> DeltaResult<Box<dyn EngineData>> {
            let index = read.fetch_add(1, Ordering::Relaxed);
            if error_at == Some((pass, index)) {
                return Err(Error::generic("injected JSON read failure"));
            }
            let data = Box::new(ArrowEngineData::new(batch?));
            if pass == 0 {
                let count = live.fetch_add(1, Ordering::Relaxed) + 1;
                peak.fetch_max(count, Ordering::Relaxed);
                Ok(Box::new(TrackedPrepareBatch {
                    data,
                    live: live.clone(),
                }))
            } else {
                Ok(data)
            }
        });
        self.readers_live.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(TrackedRead {
            batches: Box::new(batches),
            live: self.readers_live.clone(),
        }))
    }

    fn write_json_file(
        &self,
        path: &url::Url,
        data: DeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

struct TrackedRead {
    batches: FileDataReadResultIterator,
    live: Arc<AtomicUsize>,
}

impl Iterator for TrackedRead {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next()
    }
}

impl Drop for TrackedRead {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

struct TrackedPrepareBatch {
    data: Box<dyn EngineData>,
    live: Arc<AtomicUsize>,
}

impl Drop for TrackedPrepareBatch {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

impl EngineData for TrackedPrepareBatch {
    fn visit_rows(&self, columns: &[ColumnName], visitor: &mut dyn RowVisitor) -> DeltaResult<()> {
        self.data.visit_rows(columns, visitor)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn append_columns(
        &self,
        schema: SchemaRef,
        columns: Vec<ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.data.append_columns(schema, columns)
    }

    fn apply_selection_vector(self: Box<Self>, _: Vec<bool>) -> DeltaResult<Box<dyn EngineData>> {
        panic!("preparation must visit all actions without filtering")
    }

    fn has_field(&self, name: &ColumnName) -> bool {
        self.data.has_field(name)
    }
}

#[rstest]
#[case(None, "without a file path")]
#[case(Some("memory:///unexpected.json"), "unrequested commit file")]
fn rejects_batch_without_requested_file_path(
    #[case] batch_path: Option<&str>,
    #[case] expected_error: &str,
) {
    let commits = test_commits(1);
    let batches = vec![Ok::<_, Error>(nullable_file_path_batch(batch_path))];

    let mut reader = commit_batch_reader(&commits, Box::new(batches.into_iter()));
    let mut batches = reader.batches_for_commit(0);
    let result = batches.next().unwrap();

    assert_result_error_with_message(result, expected_error);
    assert!(batches.next().is_none());
}

#[test]
fn streams_batches_without_materializing_a_commit() {
    let commits = test_commits(3);
    let batches_read = Arc::new(AtomicUsize::new(0));
    let counter = batches_read.clone();
    let files = commits.clone();
    let batches = [0, 0, 0, 1, 2].into_iter().map(move |index| {
        counter.fetch_add(1, Ordering::Relaxed);
        Ok::<_, Error>(file_path_batch(files[index].location.location.as_str(), 1))
    });
    let mut reader = commit_batch_reader(&commits, Box::new(batches));

    assert!(reader.next_batch_for_commit(0).unwrap().is_some());
    assert_eq!(batches_read.load(Ordering::Relaxed), 1);
}

#[rstest]
#[case(vec![], vec![0, 0, 0, 0])]
#[case(vec![(1, 0), (1, 1), (1, 0), (1, 1)], vec![0, 2, 0, 0])]
#[case(vec![(0, 0), (0, 1), (2, 0), (2, 1)], vec![1, 0, 1, 0])]
#[case(vec![(3, 0), (3, 1)], vec![0, 0, 0, 1])]
#[case(vec![(0, 1), (0, 1), (1, 0), (2, 1)], vec![2, 0, 1, 0])]
fn preserves_empty_commits_in_each_position(
    #[case] batch_rows: Vec<(usize, usize)>,
    #[case] expected: Vec<usize>,
) {
    let commits = test_commits(4);
    let files = commits.clone();
    let batches = batch_rows.into_iter().map(move |(index, rows)| {
        Ok(file_path_batch(
            files[index].location.location.as_str(),
            rows,
        ))
    });
    let mut reader = commit_batch_reader(&commits, Box::new(batches));
    let counts = (0..commits.len())
        .map(|index| {
            reader
                .batches_for_commit(index)
                .try_fold(0, |count, batch| batch.map(|_| count + 1))
                .unwrap()
        })
        .collect_vec();
    assert_eq!(counts, expected);
}

#[test]
fn rejects_batches_returning_to_an_earlier_commit() {
    let commits = test_commits(2);
    let files = commits.clone();
    let batches = [0, 1, 0]
        .into_iter()
        .map(move |index| Ok(file_path_batch(files[index].location.location.as_str(), 1)));
    let mut reader = commit_batch_reader(&commits, Box::new(batches));
    assert!(reader.next_batch_for_commit(0).unwrap().is_some());
    assert!(reader.next_batch_for_commit(0).unwrap().is_none());
    assert!(reader.next_batch_for_commit(1).unwrap().is_some());
    assert_result_error_with_message(reader.next_batch_for_commit(1), "out-of-order");
}

#[rstest]
#[tokio::test]
async fn releases_prepare_batches_and_scans_lazily(#[values(2, 100)] action_count: usize) {
    let (engine, handler) = BatchingJsonHandler::engine([1, 1], None);
    let mut mock_table = LocalMockTable::new();
    for _ in 0..3 {
        mock_table
            .commit((0..action_count).map(|index| {
                Action::Add(Add {
                    path: format!("file_{index}.parquet"),
                    data_change: true,
                    ..Default::default()
                })
            }))
            .await;
    }
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None).unwrap();
    let expected_files = commits
        .iter()
        .map(|commit| commit.location.location.to_string())
        .collect_vec();
    let root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut replay = table_changes_action_iter(
        engine,
        &get_default_table_config(&root),
        commits,
        get_schema(),
        None,
    )
    .unwrap();
    assert_eq!(
        *handler.requested_files.lock().unwrap(),
        vec![expected_files.clone(), expected_files]
    );
    assert!(handler
        .batches_read
        .iter()
        .all(|count| count.load(Ordering::Relaxed) == 0));
    replay.next().unwrap().unwrap();
    // Preparation consumes the commit and one lookahead batch; scanning consumes just one batch.
    assert_eq!(
        handler.batches_read[0].load(Ordering::Relaxed),
        action_count + 1
    );
    assert_eq!(handler.batches_read[1].load(Ordering::Relaxed), 1);
    assert_eq!(handler.prepare_batches_live.load(Ordering::Relaxed), 1);
    assert_eq!(handler.prepare_batches_peak.load(Ordering::Relaxed), 1);
    drop(replay);
    assert_eq!(handler.prepare_batches_live.load(Ordering::Relaxed), 0);
    assert_eq!(handler.readers_live.load(Ordering::Relaxed), 0);
}

#[rstest]
#[tokio::test]
async fn read_failure_terminates_both_passes(
    #[values(0, 1)] pass: usize,
    #[values(0, 2)] batch_index: usize,
) {
    let (engine, handler) = BatchingJsonHandler::engine([1, 1], Some((pass, batch_index)));
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit((0..4).map(|index| {
            Action::Add(Add {
                path: format!("file_{index}.parquet"),
                data_change: true,
                ..Default::default()
            })
        }))
        .await;
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None).unwrap();
    let root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut replay = table_changes_action_iter(
        engine,
        &get_default_table_config(&root),
        commits,
        get_schema(),
        None,
    )
    .unwrap();
    if pass == 1 {
        for _ in 0..batch_index {
            replay.next().unwrap().unwrap();
        }
    }
    assert_result_error_with_message(replay.next().unwrap(), "injected JSON read failure");
    assert_eq!(handler.prepare_batches_live.load(Ordering::Relaxed), 0);
    assert_eq!(handler.readers_live.load(Ordering::Relaxed), 0);
    assert!(replay.next().is_none());
}

#[rstest]
#[tokio::test]
async fn independent_batch_boundaries_preserve_cdc_and_dv_resolution(
    #[values([1, 2], [2, 1])] batch_sizes: [usize; 2],
    #[values(false, true)] has_cdc: bool,
    #[values(CdfMode::ChangeDataFeed, CdfMode::RowTracking)] mode: CdfMode,
) {
    let (engine, handler) = BatchingJsonHandler::engine(batch_sizes, None);
    let mut mock_table = LocalMockTable::new();
    let old_dv = test_deletion_vector("old_dv", 1);
    let mut actions = vec![
        Action::Add(Add {
            path: "paired.parquet".into(),
            data_change: true,
            deletion_vector: Some(test_deletion_vector("new_dv", 2)),
            ..Default::default()
        }),
        Action::Add(Add {
            path: "unpaired.parquet".into(),
            data_change: true,
            ..Default::default()
        }),
        Action::Remove(Remove {
            path: "paired.parquet".into(),
            data_change: true,
            deletion_vector: Some(old_dv.clone()),
            ..Default::default()
        }),
    ];
    if has_cdc {
        actions.push(Action::Cdc(Cdc {
            path: "changes.parquet".into(),
            ..Default::default()
        }));
    }
    mock_table.commit(actions).await;
    mock_table.commit(std::iter::empty()).await;
    mock_table
        .commit([Action::Add(Add {
            path: "after_empty.parquet".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;
    mock_table.commit(std::iter::empty()).await;
    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None).unwrap();
    commits[0].location.last_modified = 101;
    commits[2].location.last_modified = 303;
    let root = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let config = match mode {
        CdfMode::ChangeDataFeed => get_default_table_config(&root),
        CdfMode::RowTracking => row_tracking_table_config(root, get_schema()),
    };
    let replay =
        table_changes_action_iter_with_mode(engine, &config, commits, get_schema(), None, mode)
            .unwrap();
    let scan_files: Vec<_> = scan_metadata_to_scan_file(replay).try_collect().unwrap();
    let observed = scan_files
        .iter()
        .map(|file| {
            (
                file.commit_version,
                file.commit_timestamp,
                file.path.as_str(),
            )
        })
        .collect_vec();
    let expected = if has_cdc && mode.uses_change_data_files() {
        vec![(0, 101, "changes.parquet"), (2, 303, "after_empty.parquet")]
    } else {
        assert_eq!(
            scan_files[0].remove_dv,
            Some(DvInfo {
                deletion_vector: Some(old_dv)
            })
        );
        vec![
            (0, 101, "paired.parquet"),
            (0, 101, "unpaired.parquet"),
            (2, 303, "after_empty.parquet"),
        ]
    };
    assert_eq!(observed, expected);
    assert_eq!(handler.prepare_batches_live.load(Ordering::Relaxed), 0);
    assert_eq!(handler.readers_live.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn reads_full_commit_range_in_two_passes() {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let engine: Arc<dyn Engine> = Arc::new(MeteredDeltaEngine::new(Arc::new(SyncEngine::new())));
    let mut mock_table = LocalMockTable::new();
    for version in 0..3 {
        mock_table
            .commit([Action::Add(Add {
                path: format!("file_{version}.parquet"),
                data_change: true,
                ..Default::default()
            })])
            .await;
    }

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None).unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);

    let scan_metadata =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None).unwrap();
    let scan_files: Vec<_> = scan_metadata_to_scan_file(scan_metadata)
        .map_ok(|scan_file| (scan_file.commit_version, scan_file.path))
        .try_collect()
        .unwrap();

    assert_eq!(
        scan_files,
        vec![
            (0, "file_0.parquet".to_string()),
            (1, "file_1.parquet".to_string()),
            (2, "file_2.parquet".to_string()),
        ]
    );
    let json_read_file_counts = reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::JsonReadCompleted(event) => Some(event.num_files),
            _ => None,
        })
        .collect_vec();
    assert_eq!(json_read_file_counts, vec![3, 3]);
}

#[tokio::test]
async fn empty_commit_does_not_shift_following_actions() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([Action::Add(Add {
            path: "file_0.parquet".to_string(),
            data_change: true,
            ..Default::default()
        })])
        .await;
    mock_table.commit(std::iter::empty()).await;
    mock_table
        .commit([Action::Add(Add {
            path: "file_2.parquet".to_string(),
            data_change: true,
            ..Default::default()
        })])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None).unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let scan_metadata =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None).unwrap();
    let scan_files: Vec<_> = scan_metadata_to_scan_file(scan_metadata)
        .map_ok(|scan_file| (scan_file.commit_version, scan_file.path))
        .try_collect()
        .unwrap();

    assert_eq!(
        scan_files,
        vec![
            (0, "file_0.parquet".to_string()),
            (2, "file_2.parquet".to_string()),
        ]
    );
}

#[tokio::test]
async fn metadata_protocol() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                        ("delta.columnMapping.mode".to_string(), "none".to_string()),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [TableFeature::DeletionVectors, TableFeature::ChangeDataFeed],
                )
                .unwrap(),
            ),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let scan_batches =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None).unwrap();
    let sv = result_to_sv(scan_batches);
    assert_eq!(sv, &[false, false]);
}
#[tokio::test]
async fn cdf_not_enabled() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    // Commit metadata without CDF property to test that CDF is rejected
    mock_table
        .commit([Action::Metadata(
            Metadata::try_new(None, None, get_schema(), vec![], 0, HashMap::new()).unwrap(),
        )])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))));
}

#[tokio::test]
async fn unsupported_reader_feature() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([Action::Protocol(
            Protocol::try_new_modern(
                [
                    TableFeature::DeletionVectors,
                    TableFeature::unknown("unsupportedReaderFeature"),
                ],
                [
                    TableFeature::DeletionVectors,
                    TableFeature::ChangeDataFeed,
                    TableFeature::unknown("unsupportedReaderFeature"),
                ],
            )
            .unwrap(),
        )])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    assert!(matches!(res, Err(Error::ChangeDataFeedUnsupported(_))));
}

#[tokio::test]
async fn column_mapping_should_succeed() {
    use crate::schema::{ColumnMetadataKey, MetadataValue};

    fn cm_field(name: &str, data_type: DataType, id: i64) -> StructField {
        StructField::nullable(name, data_type).with_metadata(HashMap::from([
            (
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                MetadataValue::Number(id),
            ),
            (
                ColumnMetadataKey::ColumnMappingPhysicalName
                    .as_ref()
                    .to_string(),
                MetadataValue::String(name.to_string()),
            ),
        ]))
    }

    let cm_schema = Arc::new(StructType::new_unchecked([
        cm_field("id", DataType::INTEGER, 1),
        cm_field("value", DataType::STRING, 2),
    ]));

    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors, TableFeature::ColumnMapping],
                    [
                        TableFeature::DeletionVectors,
                        TableFeature::ColumnMapping,
                        TableFeature::ChangeDataFeed,
                    ],
                )
                .unwrap(),
            ),
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    cm_schema.clone(),
                    vec![],
                    0,
                    HashMap::from([
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        ("delta.columnMapping.mode".to_string(), "id".to_string()),
                    ]),
                )
                .unwrap(),
            ),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, cm_schema, None)
            .unwrap()
            .try_collect();

    // Column mapping with CDF should now succeed
    assert!(res.is_ok(), "CDF should now support column mapping");
}

// Test that CDF fails when disabled mid-stream
#[tokio::test]
async fn cdf_disabled_midstream() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    // First commit: CDF enabled
    mock_table.commit([metadata_with_cdf(get_schema())]).await;

    // Second commit: CDF disabled
    mock_table
        .commit([metadata_action(
            get_schema(),
            HashMap::from([(
                "delta.enableChangeDataFeed".to_string(),
                "false".to_string(),
            )]),
        )])
        .await;

    assert_midstream_failure(engine, &mock_table);
}

#[rstest]
#[case::disabled(&[(ENABLE_ROW_TRACKING, "false")])]
#[case::suspended(&[
    (ENABLE_ROW_TRACKING, "true"),
    (ROW_TRACKING_SUSPENDED, "true"),
])]
#[tokio::test]
async fn row_tracking_unavailable_midstream_fails(#[case] properties: &[(&str, &str)]) {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([metadata_with_row_tracking(get_schema())])
        .await;
    mock_table
        .commit([metadata_action(
            get_schema(),
            properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )])
        .await;

    let res = execute_row_tracking(engine, &mock_table, get_schema()).map(|_| ());
    assert!(
        matches!(&res, Err(Error::RowTrackingChangeFeedUnsupported(1))),
        "expected row tracking to be unavailable at version 1, got {res:?}"
    );
}

fn nested_id_type(value_type: DataType) -> DataType {
    DataType::from(StructType::new_unchecked([StructField::nullable(
        "value", value_type,
    )]))
}

#[rstest]
#[case::additive(DataType::INTEGER, DataType::INTEGER, false, true, true)]
#[case::type_widening(DataType::INTEGER, DataType::LONG, false, false, false)]
#[case::nested_type_widening(
    nested_id_type(DataType::INTEGER),
    nested_id_type(DataType::LONG),
    false,
    false,
    false
)]
#[case::removed_column(DataType::INTEGER, DataType::INTEGER, true, false, false)]
#[tokio::test]
async fn row_tracking_schema_compatibility(
    #[case] commit_id_type: DataType,
    #[case] end_id_type: DataType,
    #[case] commit_has_year: bool,
    #[case] end_has_year: bool,
    #[case] expect_compatible: bool,
) {
    let schema = |id_type: DataType, has_year: bool| {
        let fields = [
            StructField::nullable("id", id_type),
            StructField::nullable("value", DataType::STRING),
        ]
        .into_iter()
        .chain(has_year.then(|| StructField::nullable("year", DataType::INTEGER)));
        Arc::new(StructType::new_unchecked(fields))
    };
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([metadata_with_row_tracking(schema(
            commit_id_type,
            commit_has_year,
        ))])
        .await;
    let res =
        execute_row_tracking(engine, &mock_table, schema(end_id_type, end_has_year)).map(|_| ());
    if expect_compatible {
        assert!(
            res.is_ok(),
            "expected compatible schema to succeed, got {res:?}"
        );
    } else {
        assert!(
            matches!(&res, Err(Error::ChangeDataFeedIncompatibleSchema(_, _))),
            "expected incompatible-schema error, got {res:?}"
        );
    }
}

#[rstest]
#[case::widen_nullability(false, true, true)]
#[case::tighten_nullability(true, false, false)]
fn row_tracking_schema_compatibility_checks_nullability(
    #[case] candidate_nullable: bool,
    #[case] read_nullable: bool,
    #[case] expected: bool,
) {
    let schema =
        |nullable| StructType::new_unchecked([StructField::new("id", DataType::INTEGER, nullable)]);
    assert_eq!(
        CdfMode::RowTracking
            .schemas_compatible(&schema(candidate_nullable), &schema(read_nullable),),
        expected
    );
}

#[rstest]
#[case::nullable(true, true)]
#[case::non_nullable(false, false)]
fn row_tracking_schema_compatibility_requires_new_columns_to_be_nullable(
    #[case] new_column_nullable: bool,
    #[case] expected: bool,
) {
    let candidate = StructType::new_unchecked([StructField::nullable("id", DataType::INTEGER)]);
    let read_schema = StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::new("new_column", DataType::STRING, new_column_nullable),
    ]);
    assert_eq!(
        CdfMode::RowTracking.schemas_compatible(&candidate, &read_schema),
        expected
    );
}

// Test that unsupported protocol features added mid-stream are rejected
#[tokio::test]
async fn unsupported_protocol_feature_midstream() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    // First commit: Basic protocol with CDF enabled
    mock_table
        .commit([
            protocol_action(2, 6, None, None),
            metadata_with_cdf(get_schema()),
        ])
        .await;

    // Second commit: Protocol update with unsupported feature
    mock_table
        .commit([protocol_action(
            3,
            7,
            Some(vec![TableFeature::unknown("unsupportedFeature")]),
            Some(vec![
                TableFeature::unknown("unsupportedFeature"),
                TableFeature::ChangeDataFeed,
            ]),
        )])
        .await;

    assert_midstream_failure(engine, &mock_table);
}

#[tokio::test]
async fn row_tracking_protocol_failure_preserves_the_underlying_error() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    let unknown = TableFeature::unknown("unsupportedFeature");
    mock_table
        .commit([Action::Protocol(
            Protocol::try_new_modern(
                [unknown.clone()],
                [
                    unknown,
                    TableFeature::RowTracking,
                    TableFeature::DomainMetadata,
                ],
            )
            .unwrap(),
        )])
        .await;

    let result = execute_row_tracking(engine, &mock_table, get_schema()).map(|_| ());
    assert!(
        matches!(&result, Err(Error::Unsupported(_))),
        "expected the protocol support error, got {result:?}"
    );
}

#[tokio::test]
async fn incompatible_schemas_fail() {
    async fn assert_incompatible_schema(commit_schema: SchemaRef, cdf_schema: SchemaRef) {
        let engine = Arc::new(SyncEngine::new());
        let mut mock_table = LocalMockTable::new();

        mock_table
            .commit([Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    commit_schema,
                    vec![],
                    0,
                    HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
                )
                .unwrap(),
            )])
            .await;

        let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
            .unwrap()
            .into_iter();

        let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
        let table_config = get_default_table_config(&table_root_url);
        let res: DeltaResult<Vec<_>> =
            table_changes_action_iter(engine, &table_config, commits, cdf_schema, None)
                .unwrap()
                .try_collect();

        assert!(matches!(
            res,
            Err(Error::ChangeDataFeedIncompatibleSchema(_, _))
        ));
    }

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields: `id: long`, `value: string` and `year: int` (nullable).
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
        StructField::nullable("year", DataType::INTEGER),
    ]));
    assert_incompatible_schema(schema, get_schema()).await;

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields: `id: long` and `value: string`.
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]));
    assert_incompatible_schema(schema, get_schema()).await;

    // NOTE: Once type widening is supported, this should not return an error.
    //
    // The CDF schema has fields: `id: long` and `value: string`.
    // This commit has schema with fields: `id: int` and `value: string`.
    let cdf_schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]));
    let commit_schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));
    assert_incompatible_schema(cdf_schema, commit_schema).await;

    // Note: Once schema evolution is supported, this should not return an error.
    //
    // The CDF schema has fields: nullable `id`  and nullable `value`.
    // This commit has schema with fields: non-nullable `id` and nullable `value`.
    let schema = Arc::new(StructType::new_unchecked([
        StructField::not_null("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]));
    assert_incompatible_schema(schema, get_schema()).await;

    // The CDF schema has fields: `id: int` and `value: string`.
    // This commit has schema with fields:`id: string` and `value: string`.
    let schema = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::STRING),
        StructField::nullable("value", DataType::STRING),
    ]));
    assert_incompatible_schema(schema, get_schema()).await;

    // Note: Once schema evolution is supported, this should not return an error.
    // The CDF schema has fields: `id` (nullable) and `value` (nullable).
    // This commit has schema with fields: `id` (nullable).
    let schema = Arc::new(get_schema().project_as_struct(&["id"]).unwrap());
    assert_incompatible_schema(schema, get_schema()).await;
}

// Helper function to test schema evolution scenarios.
// Returns an error if schema evolution fails (which is expected currently).
async fn test_schema_evolution(
    initial_schema: SchemaRef,
    evolved_schema: SchemaRef,
) -> DeltaResult<Vec<TableChangesScanMetadata>> {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    // Create initial commit with initial schema
    mock_table
        .commit([
            metadata_with_cdf(initial_schema.clone()),
            protocol_action(1, 1, None, None),
        ])
        .await;

    // Add some data with initial schema
    mock_table
        .commit([Action::Add(Add {
            path: "file1.parquet".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;

    // Evolve the schema
    mock_table
        .commit([metadata_with_cdf(evolved_schema.clone())])
        .await;

    // Add data with evolved schema
    mock_table
        .commit([Action::Add(Add {
            path: "file2.parquet".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)?.into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);

    // Try to read CDF using the evolved schema - this currently fails
    table_changes_action_iter(engine, &table_config, commits, evolved_schema, None)?.try_collect()
}

// This test demonstrates various schema evolution scenarios that currently fail
// but could be supported in the future. See: https://github.com/delta-io/delta-kernel-rs/issues/523
#[tokio::test]
async fn demonstration_schema_evolution_failures() {
    // Scenario 1: Adding a nullable column (safe evolution)
    // Initial: {id: int, value: string}
    // Evolved: {id: int, value: string, new_col: int?}
    let initial = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));
    let evolved = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
        StructField::nullable("new_col", DataType::INTEGER),
    ]));
    let res = test_schema_evolution(initial, evolved).await;
    assert!(
        matches!(res, Err(Error::ChangeDataFeedIncompatibleSchema(_, _))),
        "Expected ChangeDataFeedIncompatibleSchema error for adding nullable column"
    );

    // Scenario 2: Type widening (int -> long) - supported by type widening feature
    // Initial: {id: int, value: string}
    // Evolved: {id: long, value: string}
    let initial = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));
    let evolved = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::LONG),
        StructField::nullable("value", DataType::STRING),
    ]));
    let res = test_schema_evolution(initial, evolved).await;
    assert!(
        matches!(res, Err(Error::ChangeDataFeedIncompatibleSchema(_, _))),
        "Expected ChangeDataFeedIncompatibleSchema error for type widening"
    );

    // Scenario 3: Changing nullability from non-null to nullable (safe evolution)
    // Initial: {id: int!, value: string}
    // Evolved: {id: int?, value: string}
    let initial = Arc::new(StructType::new_unchecked([
        StructField::not_null("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));
    let evolved = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ]));
    let res = test_schema_evolution(initial, evolved).await;
    assert!(
        matches!(res, Err(Error::ChangeDataFeedIncompatibleSchema(_, _))),
        "Expected ChangeDataFeedIncompatibleSchema error for nullability change"
    );
}

#[tokio::test]
async fn add_remove() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[true, true]);
}

#[tokio::test]
async fn filter_data_change() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_3".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_4".into(),
                data_change: false,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_5".into(),
                data_change: false,
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[false; 5]);
}

#[tokio::test]
async fn cdc_selection() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Cdc(Cdc {
                path: "fake_path_3".into(),
                ..Default::default()
            }),
            Action::Cdc(Cdc {
                path: "fake_path_4".into(),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, HashMap::new().into());
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[true, false, true, true]);
}

#[tokio::test]
async fn dv() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    let deletion_vector1 = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
    let deletion_vector2 = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 3);
    // - fake_path_1 undergoes a restore. All rows are restored, so the deletion vector is removed.
    // - All remaining rows of fake_path_2 are deleted
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector1.clone()),
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector2.clone()),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let expected_remove_dvs = HashMap::from([(
        "fake_path_1".to_string(),
        DvInfo {
            deletion_vector: Some(deletion_vector1.clone()),
        },
    )])
    .into();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            assert_eq!(scan_metadata.remove_dvs, expected_remove_dvs);
            scan_metadata.selection_vector
        })
        .collect_vec();

    assert_eq!(sv, &[false, true, true]);
}

// Note: Data skipping does not work on Remove actions.
#[tokio::test]
async fn data_skipping_filter() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    let deletion_vector = Some(test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2));
    mock_table
        .commit([
            // Remove/Add pair with max value id = 6
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_1".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":6},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                deletion_vector: deletion_vector.clone(),
                ..Default::default()
            }),
            // Remove/Add pair with max value id = 4
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_2".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":4},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                deletion_vector,
                ..Default::default()
            }),
            // Add action with max value id = 5
            Action::Add(Add {
                path: "fake_path_3".into(),
                stats: Some("{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":3}}".into()),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    // Look for actions with id > 4
    let predicate = Predicate::binary(
        BinaryPredicateOp::GreaterThan,
        column_expr!("id"),
        Scalar::from(4),
    );
    let logical_schema = get_schema();
    let predicate =
        match PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::None) {
            Ok(PhysicalPredicate::Some(p, s)) => Some((p, s)),
            other => panic!("Unexpected result: {other:?}"),
        };
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, logical_schema, predicate)
        .unwrap()
        .flat_map(|scan_metadata| {
            let scan_metadata = scan_metadata.unwrap();
            scan_metadata.selection_vector
        })
        .collect_vec();

    // Note: since the first pair is a dv operation, remove action will always be filtered
    assert_eq!(sv, &[false, true, false, false, true]);
}

// The shared `for_raw_action_batch` filter prunes on partition values on the table_changes path
// too: partition values are parsed from `add.partitionValues` via `map_to_struct`, so `part = 'x'`
// drops the Add in partition `y`. The Remove in partition `y` must survive: the `OR(NOT is_add,
// ...)` guard shields non-Add rows from the predicate, so tombstones are never dropped from the
// change feed even when their partition does not match.
#[tokio::test]
async fn data_skipping_filter_prunes_partition_values_but_keeps_removes() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Add(Add {
                path: "in_x".into(),
                partition_values: HashMap::from([("part".to_string(), "x".to_string())]),
                data_change: true,
                ..Default::default()
            }),
            Action::Add(Add {
                path: "in_y".into(),
                partition_values: HashMap::from([("part".to_string(), "y".to_string())]),
                data_change: true,
                ..Default::default()
            }),
            // A tombstone in the pruned partition `y`. If the guard broke, `part = 'x'` would
            // silently drop it from the change feed.
            Action::Remove(Remove {
                path: "gone_y".into(),
                partition_values: Some(HashMap::from([("part".to_string(), "y".to_string())])),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    // Partitioned schema: `part` is the partition column, `id` a data column.
    let logical_schema: SchemaRef = Arc::new(StructType::new_unchecked([
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("part", DataType::STRING),
    ]));
    let metadata = Metadata::try_new(
        None,
        None,
        logical_schema.clone(),
        vec!["part".to_string()],
        0,
        HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
    )
    .unwrap();
    let protocol = Protocol::try_new_legacy(1, 4).unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = TableConfiguration::try_new(metadata, protocol, table_root_url, 0).unwrap();

    let predicate = Predicate::binary(
        BinaryPredicateOp::Equal,
        column_expr!("part"),
        Scalar::from("x"),
    );
    let predicate =
        match PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::None) {
            Ok(PhysicalPredicate::Some(p, s)) => Some((p, s)),
            other => panic!("Unexpected result: {other:?}"),
        };
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let sv = table_changes_action_iter(engine, &table_config, commits, logical_schema, predicate)
        .unwrap()
        .flat_map(|scan_metadata| scan_metadata.unwrap().selection_vector)
        .collect_vec();

    // Add in `x` survives, Add in `y` is pruned, and the Remove in `y` survives via the guard.
    assert_eq!(sv, &[true, false, true]);
}

// Stats-based pruning (as opposed to partition-value pruning) with a Remove present: `id > 4`
// drops the out-of-range Add via its `add.stats`, keeps the in-range Add, and the standalone
// Remove survives regardless of the predicate because non-Add rows bypass the stats filter.
#[tokio::test]
async fn data_skipping_filter_prunes_stats_but_keeps_removes() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            // Standalone Remove (no matching Add, no DV): survives as a tombstone.
            Action::Remove(Remove {
                path: "gone".into(),
                data_change: true,
                ..Default::default()
            }),
            // id in [0, 2]: provably excluded by `id > 4`.
            Action::Add(Add {
                path: "out_of_range".into(),
                stats: Some(
                    "{\"numRecords\":4,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":2},\"nullCount\":{\"id\":0}}".into(),
                ),
                data_change: true,
                ..Default::default()
            }),
            // id in [4, 6]: overlaps `id > 4`, so kept.
            Action::Add(Add {
                path: "in_range".into(),
                stats: Some(
                    "{\"numRecords\":4,\"minValues\":{\"id\":4},\"maxValues\":{\"id\":6},\"nullCount\":{\"id\":0}}".into(),
                ),
                data_change: true,
                ..Default::default()
            }),
        ])
        .await;

    let logical_schema = get_schema();
    let predicate = Predicate::binary(
        BinaryPredicateOp::GreaterThan,
        column_expr!("id"),
        Scalar::from(4),
    );
    let predicate =
        match PhysicalPredicate::try_new(&predicate, &logical_schema, ColumnMappingMode::None) {
            Ok(PhysicalPredicate::Some(p, s)) => Some((p, s)),
            other => panic!("Unexpected result: {other:?}"),
        };
    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let sv = table_changes_action_iter(engine, &table_config, commits, logical_schema, predicate)
        .unwrap()
        .flat_map(|scan_metadata| scan_metadata.unwrap().selection_vector)
        .collect_vec();

    // Remove survives, out-of-range Add is pruned, in-range Add survives.
    assert_eq!(sv, &[true, false, true]);
}

#[tokio::test]
async fn failing_protocol() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    let protocol = Protocol::try_new_modern(["fake_feature"], ["fake_feature"]).unwrap();

    mock_table
        .commit([
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Protocol(protocol),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let res: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    assert_result_error_with_message(
        res,
        "Change data feed is unsupported for the table at version 0",
    );
}

#[tokio::test]
async fn file_meta_timestamp() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([Action::Add(Add {
            path: "fake_path_1".into(),
            data_change: true,
            ..Default::default()
        })])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let file_meta_ts = commit.location.last_modified;
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut table_config = get_default_table_config(&table_root_url);
    let action_batches = action_batches_for_commit(engine.as_ref(), &commit);
    let scanner = LogReplayScanner::try_new(
        &mut table_config,
        commit,
        action_batches,
        &get_schema(),
        CdfMode::ChangeDataFeed,
    )
    .unwrap();
    assert_eq!(scanner.timestamp, file_meta_ts);
}

#[tokio::test]
async fn print_table_configuration() {
    let tracing_guard = LoggingTest::new();

    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    mock_table
        .commit([
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                        ("delta.columnMapping.mode".to_string(), "none".to_string()),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [TableFeature::DeletionVectors, TableFeature::ChangeDataFeed],
                )
                .unwrap(),
            ),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);

    let _scan_batches: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    let log_output = tracing_guard.logs();

    assert!(log_output.contains("Table configuration updated during CDF query"));
    assert!(log_output.contains("version=0"));
    assert!(log_output.contains("id="));
    assert!(log_output.contains("writerFeatures=[deletionVectors, changeDataFeed]"));
    assert!(log_output.contains("minReaderVersion=3"));
    assert!(log_output.contains("minWriterVersion=7"));
    assert!(log_output.contains("schemaString={\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"));
    assert!(log_output.contains("configuration="));
    assert!(log_output.contains("\"delta.enableChangeDataFeed\": \"true\""));
    assert!(log_output.contains("\"delta.columnMapping.mode\": \"none\""));
    assert!(log_output.contains("\"delta.enableDeletionVectors\": \"true\""));
}

#[tokio::test]
async fn print_table_info_post_phase1() {
    let tracing_guard = LoggingTest::new();

    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();
    // This specific commit (with these actions) isn't necessary to test the tracing for this test,
    // we just need to have one commit with any actions
    mock_table
        .commit([
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableDeletionVectors".to_string(),
                            "true".to_string(),
                        ),
                        ("delta.columnMapping.mode".to_string(), "none".to_string()),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [TableFeature::DeletionVectors, TableFeature::ChangeDataFeed],
                )
                .unwrap(),
            ),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);

    let _scan_batches: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    let log_output = tracing_guard.logs();

    assert!(log_output.contains("Phase 1 of CDF query processing completed"));
    assert!(log_output.contains("id="));
    assert!(log_output.contains("remove_dvs_size=0"));
    assert!(log_output.contains("has_cdc_action=false"));
    assert!(log_output.contains("file_path="));
    assert!(log_output.contains("version=0"));
    assert!(log_output.contains("timestamp="));
}

#[tokio::test]
async fn print_table_info_post_phase1_has_cdc() {
    let tracing_guard = LoggingTest::new();

    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Cdc(Cdc {
                path: "fake_path_2".into(),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);

    let _scan_batches: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    let log_output = tracing_guard.logs();

    assert!(log_output.contains("Phase 1 of CDF query processing completed"));
    assert!(log_output.contains("id="));
    assert!(log_output.contains("remove_dvs_size=0"));
    assert!(log_output.contains("has_cdc_action=true"));
    assert!(log_output.contains("file_path="));
    assert!(log_output.contains("version=0"));
    assert!(log_output.contains("timestamp="));
}

#[tokio::test]
async fn print_table_info_post_phase1_has_dv() {
    let tracing_guard = LoggingTest::new();

    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    let deletion_vector1 = test_deletion_vector("vBn[lx{q8@P<9BNH/isA", 2);
    let deletion_vector2 = test_deletion_vector("U5OWRz5k%CFT.Td}yCPW", 3);
    // - fake_path_1 undergoes a restore. All rows are restored, so the deletion vector is removed.
    // - All remaining rows of fake_path_2 are deleted
    mock_table
        .commit([
            Action::Remove(Remove {
                path: "fake_path_1".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector1.clone()),
                ..Default::default()
            }),
            Action::Add(Add {
                path: "fake_path_1".into(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "fake_path_2".into(),
                data_change: true,
                deletion_vector: Some(deletion_vector2.clone()),
                ..Default::default()
            }),
        ])
        .await;

    let commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let table_config = get_default_table_config(&table_root_url);
    let _scan_batches: DeltaResult<Vec<_>> =
        table_changes_action_iter(engine, &table_config, commits, get_schema(), None)
            .unwrap()
            .try_collect();

    let log_output = tracing_guard.logs();

    let expected_remove_dvs: Arc<HashMap<String, DvInfo>> = HashMap::from([(
        "fake_path_1".to_string(),
        DvInfo {
            deletion_vector: Some(deletion_vector1.clone()),
        },
    )])
    .into();

    assert!(log_output.contains("Phase 1 of CDF query processing completed"));
    assert!(log_output.contains("id="));
    assert!(log_output.contains(&format!("remove_dvs_size={}", expected_remove_dvs.len())));
    assert!(log_output.contains("has_cdc_action=false"));
    assert!(log_output.contains("file_path="));
    assert!(log_output.contains("version=0"));
    assert!(log_output.contains("timestamp="));
}

#[tokio::test]
async fn test_timestamp_with_ict_enabled() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([
            Action::CommitInfo(CommitInfo::new(1000, Some(2000), None, None, false)),
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableInCommitTimestamps".to_string(),
                            "true".to_string(),
                        ),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [
                        TableFeature::InCommitTimestamp,
                        TableFeature::ChangeDataFeed,
                        TableFeature::DeletionVectors,
                    ],
                )
                .unwrap(),
            ),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut table_config = get_default_table_config(&table_root_url);
    let action_batches = action_batches_for_commit(engine.as_ref(), &commit);
    let scanner = LogReplayScanner::try_new(
        &mut table_config,
        commit,
        action_batches,
        &get_schema(),
        CdfMode::ChangeDataFeed,
    )
    .unwrap();
    assert_eq!(scanner.timestamp, 2000);
}

#[tokio::test]
async fn test_timestamp_with_ict_disabled() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([
            Action::CommitInfo(CommitInfo::new(1000, Some(2000), None, None, false)),
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([("delta.enableChangeDataFeed".to_string(), "true".to_string())]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [
                        TableFeature::InCommitTimestamp,
                        TableFeature::ChangeDataFeed,
                        TableFeature::DeletionVectors,
                    ],
                )
                .unwrap(),
            ),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut table_config = get_default_table_config(&table_root_url);
    let action_batches = action_batches_for_commit(engine.as_ref(), &commit);
    let scanner = LogReplayScanner::try_new(
        &mut table_config,
        commit.clone(),
        action_batches,
        &get_schema(),
        CdfMode::ChangeDataFeed,
    )
    .unwrap();
    assert_ne!(scanner.timestamp, 2000);
    assert_eq!(scanner.timestamp, commit.location.last_modified);
}

#[tokio::test]
async fn test_timestamp_with_commit_info_not_first() {
    let engine = Arc::new(SyncEngine::new());
    let mut mock_table = LocalMockTable::new();

    mock_table
        .commit([
            Action::Metadata(
                Metadata::try_new(
                    None,
                    None,
                    get_schema(),
                    vec![],
                    0,
                    HashMap::from([
                        ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
                        (
                            "delta.enableInCommitTimestamps".to_string(),
                            "true".to_string(),
                        ),
                    ]),
                )
                .unwrap(),
            ),
            Action::Protocol(
                Protocol::try_new_modern(
                    [TableFeature::DeletionVectors],
                    [
                        TableFeature::InCommitTimestamp,
                        TableFeature::ChangeDataFeed,
                        TableFeature::DeletionVectors,
                    ],
                )
                .unwrap(),
            ),
            Action::CommitInfo(CommitInfo::new(1000, Some(2000), None, None, false)),
        ])
        .await;

    let mut commits = get_segment(engine.as_ref(), mock_table.table_root(), 0, None)
        .unwrap()
        .into_iter();

    let commit = commits.next().unwrap();
    let table_root_url = url::Url::from_directory_path(mock_table.table_root()).unwrap();
    let mut table_config = get_default_table_config(&table_root_url);
    let action_batches = action_batches_for_commit(engine.as_ref(), &commit);
    let result = LogReplayScanner::try_new(
        &mut table_config,
        commit,
        action_batches,
        &get_schema(),
        CdfMode::ChangeDataFeed,
    );

    // Should error because ICT is enabled but not found in the first action
    assert_result_error_with_message(
        result,
        "In-commit timestamp is enabled but not found in commit at version 0",
    );
}
