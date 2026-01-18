//! End-to-end tests for checkpoint with parsed stats (stats_parsed).
//!
//! These tests verify the full workflow:
//! 1. Create a table with writeStatsAsStruct enabled
//! 2. Add data files with JSON stats
//! 3. Create a checkpoint (transforms JSON stats to stats_parsed)
//! 4. Add more data files after checkpoint
//! 5. Read and verify data skipping works with parsed stats

use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::checkpoint::CheckpointDataIterator;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::{column_expr, Expression, Predicate};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, FileMeta, Snapshot, SnapshotRef};

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::json;
use url::Url;

mod common;

/// Create an in-memory store for testing
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (Arc::new(InMemory::new()), Url::parse("memory:///").unwrap())
}

/// Write a commit file to the store
async fn write_commit(
    store: &Arc<InMemory>,
    actions: Vec<String>,
    version: u64,
) -> DeltaResult<()> {
    let content = actions.join("\n");
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    store.put(&path, content.into()).await?;
    Ok(())
}

/// Create protocol action with v2Checkpoint support
fn protocol_action() -> String {
    json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["v2Checkpoint"],
            "writerFeatures": ["v2Checkpoint"]
        }
    })
    .to_string()
}

/// Create metadata action with writeStatsAsStruct enabled
fn metadata_action_with_stats_struct() -> String {
    let schema = json!({
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": true, "metadata": {}},
            {"name": "name", "type": "string", "nullable": true, "metadata": {}}
        ]
    });

    json!({
        "metaData": {
            "id": "test-table-stats",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema.to_string(),
            "partitionColumns": [],
            "configuration": {
                "delta.checkpoint.writeStatsAsStruct": "true",
                "delta.checkpoint.writeStatsAsJson": "true"
            },
            "createdTime": 1700000000000i64
        }
    })
    .to_string()
}

/// Create an add action with JSON stats
fn add_action_with_stats(path: &str, num_records: i64, min_id: i64, max_id: i64) -> String {
    let stats = json!({
        "numRecords": num_records,
        "minValues": {"id": min_id, "name": "a"},
        "maxValues": {"id": max_id, "name": "z"},
        "nullCount": {"id": 0, "name": 0}
    });

    json!({
        "add": {
            "path": path,
            "partitionValues": {},
            "size": 1024,
            "modificationTime": 1700000000000i64,
            "dataChange": true,
            "stats": stats.to_string()
        }
    })
    .to_string()
}

/// Helper to check if FilteredEngineData has selected rows
fn has_selected_rows(selection_vector: &[bool], data_len: usize) -> bool {
    // Per FilteredEngineData contract: if selection vector is shorter than data,
    // all uncovered rows are selected
    if selection_vector.len() < data_len {
        return true;
    }
    selection_vector.contains(&true)
}

/// Write checkpoint data to parquet and return file metadata
async fn write_checkpoint_to_store(
    store: &Arc<InMemory>,
    snapshot: SnapshotRef,
    engine: &impl delta_kernel::Engine,
) -> DeltaResult<FileMeta> {
    use delta_kernel::parquet::arrow::ArrowWriter;
    use delta_kernel::parquet::file::properties::WriterProperties;

    let writer = snapshot.create_checkpoint_writer()?;
    let checkpoint_path = writer.checkpoint_path()?;
    let mut data_iter = writer.checkpoint_data(engine)?;

    // Collect all batches and write to parquet
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for batch_result in data_iter.by_ref() {
        let filtered_data = batch_result?;
        let sv = filtered_data.selection_vector();
        let data_len = filtered_data.data().len();
        if has_selected_rows(sv, data_len) {
            let data = filtered_data.apply_selection_vector()?;
            let batch = data.try_into_record_batch()?;
            all_batches.push(batch);
        }
    }

    // Write all batches to a single parquet file
    let mut buffer: Vec<u8> = Vec::new();
    if let Some(first_batch) = all_batches.first() {
        let props = WriterProperties::builder().build();
        let mut parquet_writer =
            ArrowWriter::try_new(&mut buffer, first_batch.schema(), Some(props))?;
        for batch in &all_batches {
            parquet_writer.write(batch)?;
        }
        parquet_writer.close()?;
    }

    // Write to store
    let path = Path::from_url_path(checkpoint_path.path())?;
    let size = buffer.len() as u64;
    store.put(&path, buffer.into()).await?;

    let file_meta = FileMeta {
        location: checkpoint_path,
        last_modified: 1700000000000,
        size,
    };

    // Finalize the checkpoint
    writer.finalize(engine, &file_meta, data_iter)?;

    Ok(file_meta)
}

/// Tests the full end-to-end workflow:
/// 1. Create table with writeStatsAsStruct=true
/// 2. Add files with JSON stats
/// 3. Create checkpoint (stats transformed to stats_parsed)
/// 4. Add more files after checkpoint
/// 5. Read with predicate to verify data skipping uses stats_parsed
#[tokio::test]
async fn test_checkpoint_with_parsed_stats_end_to_end() -> DeltaResult<()> {
    let (store, table_root) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // === Phase 1: Create table and add initial data ===

    // Version 0: Protocol + Metadata with writeStatsAsStruct=true
    write_commit(
        &store,
        vec![protocol_action(), metadata_action_with_stats_struct()],
        0,
    )
    .await?;

    // Version 1: Add first file (ids 1-100)
    write_commit(
        &store,
        vec![add_action_with_stats("file1.parquet", 100, 1, 100)],
        1,
    )
    .await?;

    // Version 2: Add second file (ids 101-200)
    write_commit(
        &store,
        vec![add_action_with_stats("file2.parquet", 100, 101, 200)],
        2,
    )
    .await?;

    // === Phase 2: Create checkpoint ===

    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(snapshot.version(), 2);

    // Verify checkpoint schema will include stats_parsed
    let writer = snapshot.clone().create_checkpoint_writer()?;
    let checkpoint_data = writer.checkpoint_data(&engine)?;
    let output_schema = checkpoint_data.output_schema();

    // Check that add action schema includes stats_parsed
    let add_field = output_schema
        .field("add")
        .expect("output schema should have 'add' field");
    if let DataType::Struct(add_struct) = add_field.data_type() {
        assert!(
            add_struct.field("stats_parsed").is_some(),
            "Add action should have stats_parsed field when writeStatsAsStruct=true"
        );
        assert!(
            add_struct.field("stats").is_some(),
            "Add action should also have stats field (writeStatsAsJson=true)"
        );
    } else {
        panic!("add field should be a struct");
    }

    // Consume checkpoint data to verify batches
    let mut batch_count = 0;
    for batch_result in checkpoint_data {
        let _batch = batch_result?;
        batch_count += 1;
    }
    assert!(batch_count > 0, "Should have checkpoint batches");

    // Create a fresh snapshot and write the checkpoint
    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    write_checkpoint_to_store(&store, snapshot.clone(), &engine).await?;

    // Verify _last_checkpoint file was created
    let last_checkpoint_path = Path::from("_delta_log/_last_checkpoint");
    let last_checkpoint_data = store.get(&last_checkpoint_path).await?;
    let last_checkpoint_bytes = last_checkpoint_data.bytes().await?;
    let last_checkpoint: serde_json::Value = serde_json::from_slice(&last_checkpoint_bytes)?;
    assert_eq!(last_checkpoint["version"], 2);

    // === Phase 3: Add more commits after checkpoint ===

    // Version 3: Add third file (ids 201-300)
    write_commit(
        &store,
        vec![add_action_with_stats("file3.parquet", 100, 201, 300)],
        3,
    )
    .await?;

    // Version 4: Add fourth file (ids 301-400)
    write_commit(
        &store,
        vec![add_action_with_stats("file4.parquet", 100, 301, 400)],
        4,
    )
    .await?;

    // === Phase 4: Read from table (will use checkpoint + new commits) ===

    let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(snapshot.version(), 4);

    // Build a scan with a predicate that should skip some files
    // Looking for id > 250 should skip file1 (1-100) and file2 (101-200)
    let predicate = Arc::new(Predicate::gt(
        column_expr!("id"),
        Expression::literal(250i64),
    ));

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    // Get scan metadata to verify data skipping
    let scan_metadata: Vec<_> = scan
        .scan_metadata(&engine)?
        .collect::<Result<Vec<_>, _>>()?;

    // Count the files that will be scanned
    let mut files_to_scan = 0;
    for metadata in &scan_metadata {
        // Count selected rows in scan_files
        let sv = metadata.scan_files.selection_vector();
        let data_len = metadata.scan_files.data().len();
        if sv.len() < data_len {
            // All uncovered rows are selected
            files_to_scan += data_len - sv.len();
        }
        for selected in sv {
            if *selected {
                files_to_scan += 1;
            }
        }
    }

    // With predicate id > 250:
    // - file1 (1-100): should be skipped
    // - file2 (101-200): should be skipped
    // - file3 (201-300): should NOT be skipped (300 > 250)
    // - file4 (301-400): should NOT be skipped (all > 250)
    // So we expect 2 files to be scanned
    assert_eq!(
        files_to_scan, 2,
        "Data skipping should have reduced files from 4 to 2"
    );

    // === Phase 5: Verify we can read without predicate too ===

    let scan_all = snapshot.clone().scan_builder().build()?;
    let all_metadata: Vec<_> = scan_all
        .scan_metadata(&engine)?
        .collect::<Result<Vec<_>, _>>()?;

    let mut total_files = 0;
    for metadata in &all_metadata {
        let sv = metadata.scan_files.selection_vector();
        let data_len = metadata.scan_files.data().len();
        if sv.len() < data_len {
            total_files += data_len - sv.len();
        }
        for selected in sv {
            if *selected {
                total_files += 1;
            }
        }
    }
    assert_eq!(total_files, 4, "Should have all 4 files without predicate");

    Ok(())
}

/// Tests that checkpoint correctly transforms JSON stats to stats_parsed
/// and that the transform is applied during iteration.
#[tokio::test]
async fn test_checkpoint_stats_transform() -> DeltaResult<()> {
    let (store, table_root) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // Create table with writeStatsAsStruct=true
    write_commit(
        &store,
        vec![protocol_action(), metadata_action_with_stats_struct()],
        0,
    )
    .await?;

    // Add file with JSON stats
    write_commit(
        &store,
        vec![add_action_with_stats("data.parquet", 42, 10, 99)],
        1,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    let writer = snapshot.clone().create_checkpoint_writer()?;

    // Get checkpoint data iterator
    let checkpoint_data = writer.checkpoint_data(&engine)?;
    let output_schema = checkpoint_data.output_schema();

    // Verify output schema structure
    let add_field = output_schema.field("add").expect("should have add field");
    if let DataType::Struct(add_struct) = add_field.data_type() {
        // stats_parsed should have the proper structure based on table schema
        let stats_parsed_field = add_struct
            .field("stats_parsed")
            .expect("should have stats_parsed");

        if let DataType::Struct(stats_struct) = stats_parsed_field.data_type() {
            assert!(
                stats_struct.field("numRecords").is_some(),
                "stats_parsed should have numRecords"
            );
            assert!(
                stats_struct.field("minValues").is_some(),
                "stats_parsed should have minValues"
            );
            assert!(
                stats_struct.field("maxValues").is_some(),
                "stats_parsed should have maxValues"
            );
            assert!(
                stats_struct.field("nullCount").is_some(),
                "stats_parsed should have nullCount"
            );
        } else {
            panic!("stats_parsed should be a struct");
        }
    } else {
        panic!("add should be a struct");
    }

    // Iterate through checkpoint data to verify transforms work
    let mut add_action_found = false;
    for batch_result in checkpoint_data {
        let filtered = batch_result?;
        let sv = filtered.selection_vector();
        let data_len = filtered.data().len();
        if has_selected_rows(sv, data_len) {
            let data = filtered.apply_selection_vector()?;
            let batch = data.try_into_record_batch()?;

            // Check if this batch has add actions
            if let Ok(add_col_idx) = batch.schema().index_of("add") {
                let add_col = batch.column(add_col_idx);
                if add_col.null_count() < add_col.len() {
                    add_action_found = true;
                }
            }
        }
    }

    assert!(
        add_action_found,
        "Should have found add action in checkpoint data"
    );

    Ok(())
}

/// Tests reading from a table after checkpoint with different stats configurations.
#[tokio::test]
async fn test_checkpoint_preserves_stats_across_versions() -> DeltaResult<()> {
    let (store, table_root) = new_in_memory_store();
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    // Create table with both JSON and struct stats enabled
    write_commit(
        &store,
        vec![protocol_action(), metadata_action_with_stats_struct()],
        0,
    )
    .await?;

    // Add initial file
    write_commit(
        &store,
        vec![add_action_with_stats("initial.parquet", 50, 1, 50)],
        1,
    )
    .await?;

    // Create checkpoint at version 1
    let snapshot_v1 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    write_checkpoint_to_store(&store, snapshot_v1, &engine).await?;

    // Add more files after checkpoint
    write_commit(
        &store,
        vec![add_action_with_stats(
            "post_checkpoint.parquet",
            50,
            51,
            100,
        )],
        2,
    )
    .await?;

    // Read at version 2 (will read checkpoint + commit 2)
    let snapshot_v2 = Snapshot::builder_for(table_root.clone()).build(&engine)?;
    assert_eq!(snapshot_v2.version(), 2);

    // Test data skipping with predicate that filters to second file only
    let predicate = Arc::new(Predicate::gt(
        column_expr!("id"),
        Expression::literal(50i64),
    ));
    let scan = snapshot_v2
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let scan_metadata: Vec<_> = scan
        .scan_metadata(&engine)?
        .collect::<Result<Vec<_>, _>>()?;

    let mut files_selected = 0;
    for metadata in &scan_metadata {
        let sv = metadata.scan_files.selection_vector();
        let data_len = metadata.scan_files.data().len();
        if sv.len() < data_len {
            files_selected += data_len - sv.len();
        }
        for selected in sv {
            if *selected {
                files_selected += 1;
            }
        }
    }

    // Only post_checkpoint.parquet (51-100) should be selected
    assert_eq!(files_selected, 1, "Should only select file with id > 50");

    Ok(())
}
