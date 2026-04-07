use std::sync::Arc;

use crate::action_reconciliation::{
    ActionReconciliationIterator, ActionReconciliationIteratorState,
};
use crate::actions::{Add, ADD_NAME};
use crate::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StructArray};
use crate::arrow::datatypes::{ArrowPrimitiveType, Int32Type, Int64Type};
use crate::checkpoint::sidecar::{SidecarSplitter, SingleSidecarDataIterator};
use crate::checkpoint::tests::{
    create_add_action, create_metadata_action, create_metadata_with_stats_config_and_partitions,
    create_remove_action, create_v2_checkpoint_protocol_action, new_in_memory_store,
    write_commit_to_store,
};
use crate::checkpoint::CheckpointWriter;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::engine::default::executor::tokio::TokioMultiThreadExecutor;
use crate::engine::default::DefaultEngineBuilder;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::object_store::ObjectStore;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::schema::{DataType, StructField, StructType};
use crate::utils::test_utils::Action;
use crate::{DeltaResult, Engine, EngineData, Snapshot};
use rstest::rstest;
use serde_json::json;
use url::Url;

struct CheckpointParts {
    sidecar_files: Vec<Url>,
    non_file_batches: Vec<Box<dyn EngineData>>,
    iter_state: Arc<ActionReconciliationIteratorState>,
}

/// Test helper: generates sidecar parquet files using the [`SidecarSplitter`] +
/// [`SingleSidecarDataIterator`] pipeline. Writes real parquet files to storage via
/// `engine.parquet_handler().write_parquet_file()` with names `sidecar_1.parquet`,
/// `sidecar_2.parquet`, etc. Does NOT write the main checkpoint or `_last_checkpoint`.
fn generate_checkpoint_parts(
    writer: &CheckpointWriter,
    engine: &dyn Engine,
    file_actions_per_sidecar_hint: usize,
) -> DeltaResult<CheckpointParts> {
    let data_iter = writer.checkpoint_data(engine)?;
    let iter_state = data_iter.state();
    let output_schema = writer
        .checkpoint_output_schema
        .get()
        .cloned()
        .expect("checkpoint_output_schema should be set by checkpoint_data");

    let splitter = SidecarSplitter::new(
        data_iter,
        engine.evaluation_handler().as_ref(),
        output_schema,
    )?;

    let sidecars_base = writer.snapshot.log_segment().log_root.join("_sidecars/")?;
    let mut sidecar_files = Vec::new();
    let mut sidecar_index = 1usize;
    loop {
        let mut single_sidecar_iter =
            SingleSidecarDataIterator::new(splitter.clone(), file_actions_per_sidecar_hint)?
                .peekable();
        if single_sidecar_iter.peek().is_some() {
            let sidecar_url = sidecars_base.join(&format!("sidecar_{sidecar_index}.parquet"))?;
            engine
                .parquet_handler()
                .write_parquet_file(sidecar_url.clone(), Box::new(single_sidecar_iter))?;
            sidecar_files.push(sidecar_url);
            sidecar_index += 1;
        }

        if splitter.lock().expect("splitter lock").is_exhausted() {
            break;
        }
    }

    let non_file_batches = splitter
        .lock()
        .expect("splitter lock")
        .take_non_file_batches();

    Ok(CheckpointParts {
        sidecar_files,
        non_file_batches,
        iter_state,
    })
}

/// Helper to build a DefaultEngine with multi-thread executor.
fn new_multi_thread_engine(store: Arc<InMemory>) -> impl Engine {
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    DefaultEngineBuilder::new(store)
        .with_task_executor(executor)
        .build()
}

struct NonFileActionSummary {
    protocol: usize,
    metadata: usize,
    checkpoint_metadata: usize,
}

struct ExpectedNonFileContent<'a> {
    reader_version: i32,
    writer_version: i32,
    table_name: &'a str,
    checkpoint_version: i64,
}

/// Reads sidecar parquet files from storage and returns all record batches.
async fn read_sidecar_batches(store: &Arc<InMemory>, sidecar_urls: &[Url]) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for url in sidecar_urls {
        let obj_path = Path::from(url.path().strip_prefix('/').unwrap_or(url.path()));
        let bytes = store
            .get(&obj_path)
            .await
            .unwrap_or_else(|_| panic!("sidecar should exist: {url}"))
            .bytes()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        for rb in reader {
            batches.push(rb.unwrap());
        }
    }
    batches
}

fn get_column_by_path<'a>(rb: &'a RecordBatch, path: &[&str]) -> Option<&'a ArrayRef> {
    let mut col = rb.column_by_name(path.first()?)?;
    for segment in &path[1..] {
        col = col.as_struct_opt()?.column_by_name(segment)?;
    }
    Some(col)
}

/// Collects non-null string values at the given column path across all batches, sorted.
/// Checks validity of the immediate parent struct and the leaf column.
fn collect_string_column(batches: &[RecordBatch], path: &[&str]) -> Vec<String> {
    assert!(!path.is_empty());
    let mut values = Vec::new();
    for rb in batches {
        let Some(parent) = get_column_by_path(rb, &path[..path.len() - 1]) else {
            continue;
        };
        let Some(leaf) = parent.as_struct().column_by_name(path.last().unwrap()) else {
            continue;
        };
        let string_col = leaf.as_string::<i32>();
        for row in 0..string_col.len() {
            if parent.is_valid(row) && string_col.is_valid(row) {
                values.push(string_col.value(row).to_string());
            }
        }
    }
    values.sort();
    values
}

/// Extracts sorted add/remove paths from sidecar record batches.
fn collect_file_action_paths(batches: &[RecordBatch]) -> (Vec<String>, Vec<String>) {
    (
        collect_string_column(batches, &["add", "path"]),
        collect_string_column(batches, &["remove", "path"]),
    )
}

/// Extract a typed value from a named field of a [`StructArray`] at the given row.
fn struct_field_value<T: ArrowPrimitiveType>(
    s: &StructArray,
    field: &str,
    row: usize,
) -> T::Native {
    s.column_by_name(field)
        .unwrap_or_else(|| panic!("missing field '{field}'"))
        .as_primitive::<T>()
        .value(row)
}

/// Extract a string value from a named field of a [`StructArray`] at the given row.
fn struct_field_string<'a>(s: &'a StructArray, field: &str, row: usize) -> &'a str {
    s.column_by_name(field)
        .unwrap_or_else(|| panic!("missing field '{field}'"))
        .as_string::<i32>()
        .value(row)
}

/// Inspects non-file-action batches and verifies:
/// - Counts of protocol, metadata, and checkpointMetadata rows
/// - Protocol has the expected minReaderVersion/minWriterVersion
/// - Metadata has the expected table name
/// - CheckpointMetadata has the expected version
/// - add/remove columns are null in every row
fn assert_non_file_batches(
    batches: &[Box<dyn EngineData>],
    expected: &ExpectedNonFileContent<'_>,
) -> NonFileActionSummary {
    assert!(!batches.is_empty(), "non-file batches should not be empty");
    let mut summary = NonFileActionSummary {
        protocol: 0,
        metadata: 0,
        checkpoint_metadata: 0,
    };
    for batch in batches {
        let arrow = batch
            .as_ref()
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("should be ArrowEngineData");
        let rb = arrow.record_batch();
        for row in 0..rb.num_rows() {
            let mut has_non_file_action = false;
            if let Some(col) = rb.column_by_name("protocol") {
                if col.is_valid(row) {
                    has_non_file_action = true;
                    summary.protocol += 1;
                    let parent_struct = col.as_struct();
                    let reader_v =
                        struct_field_value::<Int32Type>(parent_struct, "minReaderVersion", row);
                    let writer_v =
                        struct_field_value::<Int32Type>(parent_struct, "minWriterVersion", row);
                    assert_eq!(
                        reader_v, expected.reader_version,
                        "protocol minReaderVersion mismatch"
                    );
                    assert_eq!(
                        writer_v, expected.writer_version,
                        "protocol minWriterVersion mismatch"
                    );
                }
            }

            if let Some(col) = rb.column_by_name("metaData") {
                if col.is_valid(row) {
                    has_non_file_action = true;
                    summary.metadata += 1;
                    let ms = col.as_struct();
                    assert_eq!(
                        struct_field_string(ms, "name", row),
                        expected.table_name,
                        "metadata name mismatch"
                    );
                }
            }

            if let Some(col) = rb.column_by_name("checkpointMetadata") {
                if col.is_valid(row) {
                    has_non_file_action = true;
                    summary.checkpoint_metadata += 1;
                    let cs = col.as_struct();
                    let version = struct_field_value::<Int64Type>(cs, "version", row);
                    assert_eq!(
                        version, expected.checkpoint_version,
                        "checkpointMetadata version mismatch"
                    );
                }
            }

            if let Some(add_col) = rb.column_by_name("add") {
                assert!(
                    !add_col.is_valid(row),
                    "add should be null in non-file batch (row {row})"
                );
            }
            if let Some(remove_col) = rb.column_by_name("remove") {
                assert!(
                    !remove_col.is_valid(row),
                    "remove should be null in non-file batch (row {row})"
                );
            }

            assert!(
                has_non_file_action,
                "row {row} has all non-file-action columns null (should have been filtered)"
            );
        }
    }
    summary
}

/// V2 table with 3 adds + 1 remove, single sidecar.
/// Verifies: exactly 1 sidecar file, non-file batches buffered, iterator exhausted,
/// action counts correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_generate_sidecars_single_sidecar() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = new_multi_thread_engine(store.clone());

    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )
    .await?;

    write_commit_to_store(
        &store,
        vec![
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
            create_add_action("file3.parquet"),
        ],
        1,
    )
    .await?;

    write_commit_to_store(&store, vec![create_remove_action("file1.parquet")], 2).await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    let result = generate_checkpoint_parts(&writer, &engine, usize::MAX)?;

    assert_eq!(result.sidecar_files.len(), 1);

    let summary = assert_non_file_batches(
        &result.non_file_batches,
        &ExpectedNonFileContent {
            reader_version: 3,
            writer_version: 7,
            table_name: "test-table",
            checkpoint_version: 2,
        },
    );
    assert_eq!(summary.protocol, 1);
    assert_eq!(summary.metadata, 1);
    assert_eq!(summary.checkpoint_metadata, 1);

    assert!(result.iter_state.is_exhausted());

    // 1 protocol + 1 metadata + 2 adds (file1 removed) + 1 remove + 1 checkpointMetadata = 6
    assert_eq!(result.iter_state.actions_count(), 6);

    let batches = read_sidecar_batches(&store, &result.sidecar_files).await;
    let (adds, removes) = collect_file_action_paths(&batches);
    assert_eq!(adds, vec!["file2.parquet", "file3.parquet"]);
    assert_eq!(removes, vec!["file1.parquet"]);

    Ok(())
}

/// V2 table with adds and removes across multiple commits, hint=3.
/// Verifies: multiple sidecar files produced with adds/removes distributed across sidecars,
/// batches with both file and non-file actions correctly split.
/// And the row count of the file and non-file batches is correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_generate_sidecars_multiple_chunks() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = new_multi_thread_engine(store.clone());

    // Commit 0: v2 protocol + metadata + 3 adds (mixed batch of file and non-file actions)
    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
            create_add_action("file0a.parquet"),
            create_add_action("file0b.parquet"),
            create_add_action("file0c.parquet"),
        ],
        0,
    )
    .await?;

    // Spread adds and removes across commits
    // so the hint=3 causes chunking at batch boundaries, with removes landing in
    // multiple sidecars.
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
            create_remove_action("file0a.parquet"),
        ],
        1,
    )
    .await?;
    write_commit_to_store(
        &store,
        vec![
            create_add_action("file3.parquet"),
            create_add_action("file4.parquet"),
            create_remove_action("file0b.parquet"),
        ],
        2,
    )
    .await?;
    write_commit_to_store(&store, vec![create_add_action("file5.parquet")], 3).await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    // hint=3: with DefaultEngine, each commit is one batch. Reconciliation replays
    // in reverse: commit 3 (1 add), commit 2 (2 adds + 1 remove = 3 file rows),
    // commit 1 (2 adds + 1 remove = 3 file rows, file0a add reconciled out of commit 0),
    // commit 0 (1 add [file0a,file0b reconciled away] + protocol + metadata),
    // checkpointMetadata (1 row).
    //
    // Sidecar 1: commit 3 (1) + commit 2 (3) = 4 file action rows (hint exceeded)
    // Sidecar 2: commit 1 (3) = 3 file action rows (hint reached)
    // Sidecar 3: commit 0 file part (1) = 1 file action row
    let result = generate_checkpoint_parts(&writer, &engine, 3)?;

    assert!(result.iter_state.is_exhausted());

    let total_non_file_rows: usize = result.non_file_batches.iter().map(|b| b.len()).sum();
    assert_eq!(total_non_file_rows, 3); // 1 protocol + 1 metadata + 1 checkpointMetadata
    let summary = assert_non_file_batches(
        &result.non_file_batches,
        &ExpectedNonFileContent {
            reader_version: 3,
            writer_version: 7,
            table_name: "test-table",
            checkpoint_version: 3,
        },
    );
    assert_eq!(summary.protocol, 1);
    assert_eq!(summary.metadata, 1);
    assert_eq!(summary.checkpoint_metadata, 1);

    assert_eq!(
        result.sidecar_files.len(),
        3,
        "should produce exactly 3 sidecars"
    );

    // Sidecar 1: file5 (add) from commit 3, file3+file4 (add) + file0b (remove) from commit 2
    let s1 = read_sidecar_batches(&store, &result.sidecar_files[..1]).await;
    let s1_rows: usize = s1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(s1_rows, 4);
    let (s1_adds, s1_removes) = collect_file_action_paths(&s1);
    assert_eq!(
        s1_adds,
        vec!["file3.parquet", "file4.parquet", "file5.parquet"]
    );
    assert_eq!(s1_removes, vec!["file0b.parquet"]);

    // Sidecar 2: file1+file2 (add) + file0a (remove) from commit 1
    let s2 = read_sidecar_batches(&store, &result.sidecar_files[1..2]).await;
    let s2_rows: usize = s2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(s2_rows, 3);
    let (s2_adds, s2_removes) = collect_file_action_paths(&s2);
    assert_eq!(s2_adds, vec!["file1.parquet", "file2.parquet"]);
    assert_eq!(s2_removes, vec!["file0a.parquet"]);

    // Sidecar 3: file0c (add) from commit 0 (file0a and file0b reconciled away)
    let s3 = read_sidecar_batches(&store, &result.sidecar_files[2..]).await;
    let s3_rows: usize = s3.iter().map(|b| b.num_rows()).sum();
    assert_eq!(s3_rows, 1);
    let (s3_adds, s3_removes) = collect_file_action_paths(&s3);
    assert_eq!(s3_adds, vec!["file0c.parquet"]);
    assert!(s3_removes.is_empty());

    Ok(())
}

/// V2 table with adds across multiple commits, hint=1 (very small).
/// Verifies: produces multiple sidecar files, splitter is exhausted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_generate_sidecars_hint_one_per_batch() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = new_multi_thread_engine(store.clone());

    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )
    .await?;

    write_commit_to_store(
        &store,
        vec![
            create_add_action("file1.parquet"),
            create_add_action("file2.parquet"),
        ],
        1,
    )
    .await?;

    write_commit_to_store(
        &store,
        vec![
            create_add_action("file3.parquet"),
            create_add_action("file4.parquet"),
        ],
        2,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    // hint=1: each batch exceeds the hint, so each gets its own sidecar.
    let result = generate_checkpoint_parts(&writer, &engine, 1)?;

    assert!(result.iter_state.is_exhausted());
    assert_eq!(
        result.sidecar_files.len(),
        2,
        "hint=1 sidecar count mismatch"
    );

    // sidecar_1: commit 2 (reverse replay order)
    let s1 = read_sidecar_batches(&store, &result.sidecar_files[..1]).await;
    let (s1_adds, s1_removes) = collect_file_action_paths(&s1);
    assert_eq!(s1_adds, vec!["file3.parquet", "file4.parquet"]);
    assert!(s1_removes.is_empty());

    // sidecar_2: commit 1
    let s2 = read_sidecar_batches(&store, &result.sidecar_files[1..]).await;
    let (s2_adds, s2_removes) = collect_file_action_paths(&s2);
    assert_eq!(s2_adds, vec!["file1.parquet", "file2.parquet"]);
    assert!(s2_removes.is_empty());

    Ok(())
}

/// V2 partitioned table with writeStatsAsStruct=true.
/// Verifies the output schema of the sidecar files is correctly including stats_parsed and partitionValues_parsed in the add struct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_generate_sidecars_stats_and_partition_values() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = new_multi_thread_engine(store.clone());

    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_with_stats_config_and_partitions(true, true, vec!["category".into()]),
        ],
        0,
    )
    .await?;

    let mut add = Add {
        path: "category=books/file1.parquet".into(),
        data_change: true,
        stats: Some(
            json!({
                "numRecords": 42,
                "minValues": {"id": 1, "name": "alice"},
                "maxValues": {"id": 100, "name": "zoe"},
                "nullCount": {"id": 0, "name": 5}
            })
            .to_string(),
        ),
        ..Default::default()
    };
    add.partition_values
        .insert("category".into(), "books".into());
    write_commit_to_store(&store, vec![Action::Add(add)], 1).await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    let data_iter = writer.checkpoint_data(&engine)?;
    let mut all_batches = Vec::new();
    for batch_result in data_iter {
        let batch = batch_result?;
        let data = batch.apply_selection_vector()?;
        all_batches.push(data.try_into_record_batch()?);
    }

    // Validate the checkpoint data schema
    let schema = writer
        .checkpoint_output_schema
        .get()
        .expect("should be cached after checkpoint_data");
    let add_field = schema.field(ADD_NAME).expect("schema should have 'add'");
    if let DataType::Struct(ref add_struct) = add_field.data_type {
        assert!(
            add_struct.field("stats").is_some(),
            "add should have 'stats'"
        );
        assert!(
            add_struct.field("stats_parsed").is_some(),
            "add should have 'stats_parsed'"
        );
        let pv_field = add_struct
            .field("partitionValues_parsed")
            .expect("add should have 'partitionValues_parsed'");
        if let DataType::Struct(ref pv_struct) = pv_field.data_type {
            assert!(
                pv_struct.field("category").is_some(),
                "partitionValues_parsed should have 'category'"
            );
        } else {
            panic!("partitionValues_parsed should be a struct");
        }
    } else {
        panic!("add should be a struct");
    }

    // Validate the actually values.
    let mut found_add = false;
    for rb in &all_batches {
        let add_col = rb.column_by_name("add").unwrap();
        let add_struct = add_col.as_struct();
        for row in 0..rb.num_rows() {
            if !add_col.is_valid(row) {
                continue;
            }
            found_add = true;

            let pv = add_struct
                .column_by_name("partitionValues_parsed")
                .unwrap()
                .as_struct();
            assert_eq!(struct_field_string(pv, "category", row), "books");

            let stats = add_struct
                .column_by_name("stats_parsed")
                .unwrap()
                .as_struct();
            assert_eq!(
                struct_field_value::<Int64Type>(stats, "numRecords", row),
                42
            );
            let min_vals = stats.column_by_name("minValues").unwrap().as_struct();
            assert_eq!(struct_field_value::<Int64Type>(min_vals, "id", row), 1);
            assert_eq!(struct_field_string(min_vals, "name", row), "alice");
            let max_vals = stats.column_by_name("maxValues").unwrap().as_struct();
            assert_eq!(struct_field_value::<Int64Type>(max_vals, "id", row), 100);
            assert_eq!(struct_field_string(max_vals, "name", row), "zoe");
            let null_count = stats.column_by_name("nullCount").unwrap().as_struct();
            assert_eq!(struct_field_value::<Int64Type>(null_count, "id", row), 0);
            assert_eq!(struct_field_value::<Int64Type>(null_count, "name", row), 5);
        }
    }
    assert!(found_add, "should have found an add action");

    Ok(())
}

/// V2 table with only protocol + metadata (no adds/removes).
/// Verifies: SidecarSplitter yields no file-action rows and buffers all non-file actions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_splitter_no_file_actions() -> DeltaResult<()> {
    let (store, _) = new_in_memory_store();
    let engine = new_multi_thread_engine(store.clone());

    write_commit_to_store(
        &store,
        vec![
            create_v2_checkpoint_protocol_action(),
            create_metadata_action(),
        ],
        0,
    )
    .await?;

    let table_root = Url::parse("memory:///")?;
    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.create_checkpoint_writer()?;

    let data_iter = writer.checkpoint_data(&engine)?;
    let output_schema = writer
        .checkpoint_output_schema
        .get()
        .cloned()
        .expect("checkpoint_output_schema should be set by checkpoint_data");

    let splitter = SidecarSplitter::new(
        data_iter,
        engine.evaluation_handler().as_ref(),
        output_schema,
    )?;

    let iter = SingleSidecarDataIterator::new(splitter.clone(), usize::MAX)?;
    let total_file_rows: usize = iter.map(|batch| batch.unwrap().len()).sum();
    assert_eq!(total_file_rows, 0, "should have no file-action rows");

    let mut guard = splitter.lock().expect("splitter lock");
    assert!(guard.is_exhausted());

    let non_file_batches = guard.take_non_file_batches();
    let summary = assert_non_file_batches(
        &non_file_batches,
        &ExpectedNonFileContent {
            reader_version: 3,
            writer_version: 7,
            table_name: "test-table",
            checkpoint_version: 0,
        },
    );
    assert_eq!(summary.protocol, 1);
    assert_eq!(summary.metadata, 1);
    assert_eq!(summary.checkpoint_metadata, 1);

    Ok(())
}

/// Dummy struct type used as the data type for add/remove fields in schema validation tests.
fn dummy_struct() -> DataType {
    StructType::new_unchecked([]).into()
}

#[rstest]
#[case::missing_add(
    StructType::new_unchecked([StructField::nullable("remove", dummy_struct())]),
    "missing 'add'"
)]
#[case::missing_remove(
    StructType::new_unchecked([StructField::nullable("add", dummy_struct())]),
    "missing 'remove'"
)]
#[case::non_nullable_add(
    StructType::new_unchecked([
        StructField::not_null("add", dummy_struct()),
        StructField::nullable("remove", dummy_struct()),
    ]),
    "'add' field must be nullable"
)]
#[case::non_nullable_remove(
    StructType::new_unchecked([
        StructField::nullable("add", dummy_struct()),
        StructField::not_null("remove", dummy_struct()),
    ]),
    "'remove' field must be nullable"
)]
fn test_splitter_rejects_invalid_schema(#[case] schema: StructType, #[case] expected_msg: &str) {
    let iter = ActionReconciliationIterator::new(Box::new(std::iter::empty()));
    let result = SidecarSplitter::new(iter, &ArrowEvaluationHandler, Arc::new(schema));
    assert!(result.is_err(), "should reject invalid schema");
    let err = result.err().unwrap();
    assert!(
        err.to_string().contains(expected_msg),
        "error '{err}' should contain '{expected_msg}'"
    );
}

#[test]
fn test_single_sidecar_data_iterator_rejects_zero_max_file_actions_hint() {
    let schema: Arc<StructType> = StructType::new_unchecked([
        StructField::nullable("add", dummy_struct()),
        StructField::nullable("remove", dummy_struct()),
    ])
    .into();
    let iter = ActionReconciliationIterator::new(Box::new(std::iter::empty()));
    let splitter = SidecarSplitter::new(iter, &ArrowEvaluationHandler, schema).unwrap();
    let result = SingleSidecarDataIterator::new(splitter, 0);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        err.to_string()
            .contains("max_file_actions_hint must be greater than 0"),
        "error '{err}' should mention max_file_actions_hint"
    );
}
