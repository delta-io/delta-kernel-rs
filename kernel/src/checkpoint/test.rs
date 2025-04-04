use std::sync::Arc;

use object_store::{memory::InMemory, path::Path, ObjectStore};
use url::Url;

use crate::{
    actions::{Add, Metadata, Protocol, Remove, }, checkpoint::CheckpointInfo, engine::{arrow_data::ArrowEngineData, default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine}}, scan::test_utils::{add_batch_simple, add_batch_with_remove, sidecar_batch_with_given_paths}, utils::test_utils::Action, DeltaResult, EngineData, Table
};

// Create an in-memory store and return the store and the URL for the store's _delta_log directory.
fn new_in_memory_store() -> (Arc<InMemory>, Url) {
    (
        Arc::new(InMemory::new()),
        Url::parse("memory:///")
            .unwrap()
            .join("_delta_log/")
            .unwrap(),
    )
}

/// Get an ObjectStore path for a delta file, based on the version
pub fn delta_path_for_version(version: u64, suffix: &str) -> Path {
    let path = format!("{version:020}.{suffix}");
    Path::from(path)
}

/// Writes all actions to a _delta_log json commit file in the store.
fn write_commit_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    version: u64,
) -> DeltaResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");

    let commit_path = format!("_delta_log/{}", delta_path_for_version(version, "json"));

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async { store.put(&Path::from(commit_path), content.into()).await })?;

    Ok(())
}

/// Writes all actions to a _delta_log parquet checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
fn add_checkpoint_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    let path = format!("_delta_log/{}", filename);
    write_parquet_to_store(store, path.clone(), data)?;
    Ok(())
}

/// Writes all actions to a _delta_log/_sidecars file in the store.
/// This function formats the provided filename into the _sidecars subdirectory.
fn add_sidecar_to_store(
    store: &Arc<InMemory>,
    data: Box<dyn crate::EngineData>,
    filename: &str,
) -> DeltaResult<()> {
    // Make sure the _sidecars directory exists
    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async { 
            store.put(
                &Path::from("_delta_log/_sidecars/"), 
                bytes::Bytes::new().into()
            ).await 
        })?;
    
    let path = format!("_delta_log/_sidecars/{}", filename);
    write_parquet_to_store(store, path, data)
}

// Writes a record batch obtained from engine data to the in-memory store at a given path.
fn write_parquet_to_store(
    store: &Arc<InMemory>,
    path: String,
    data: Box<dyn crate::EngineData>,
) -> DeltaResult<()> {
    let batch = ArrowEngineData::try_from_engine_data(data)?;
    let record_batch = batch.record_batch();

    let mut buffer = vec![];
    let mut writer = crate::parquet::arrow::ArrowWriter::try_new(&mut buffer, record_batch.schema(), None)?;
    writer.write(record_batch)?;
    writer.close()?;

    futures::executor::block_on(async { store.put(&Path::from(path), buffer.into()).await })?;

    Ok(())
}

/// Writes all actions to a _delta_log json checkpoint file in the store.
/// This function formats the provided filename into the _delta_log directory.
fn write_json_to_store(
    store: &Arc<InMemory>,
    actions: Vec<Action>,
    filename: &str,
) -> DeltaResult<()> {
    let json_lines: Vec<String> = actions
        .into_iter()
        .map(|action| serde_json::to_string(&action).expect("action to string"))
        .collect();
    let content = json_lines.join("\n");
    let checkpoint_path = format!("_delta_log/{}", filename);

    tokio::runtime::Runtime::new()
        .expect("create tokio runtime")
        .block_on(async {
            store
                .put(&Path::from(checkpoint_path), content.into())
                .await
        })?;

    Ok(())
}

/// Creates a test table with protocol supporting v2Checkpoints,
/// metadata, and several add/remove actions. Then adds a checkpoint
/// with sidecar references.
fn create_test_table_with_sidecars(store: &Arc<InMemory>) -> DeltaResult<Vec<String>> {
    // Version 0: Protocol with v2Checkpoint feature and metadata
    
    
    // Create sidecar files
    let sidecar_uuids = [
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
    ];
    
    let sidecar_paths: Vec<String> = sidecar_uuids
        .iter()
        .map(|uuid| format!("{}.parquet", uuid))
        .collect();
    
    // Create the sidecar files with actual content
    let read_schema = crate::actions::get_log_schema().project(&[
        crate::actions::ADD_NAME, 
        crate::actions::REMOVE_NAME
    ])?;
    
    // First sidecar gets add actions
    add_sidecar_to_store(
        store,
        add_batch_simple(read_schema.clone()),
        &sidecar_paths[0],
    )?;
    
    // Second sidecar gets remove actions
    add_sidecar_to_store(
        store,
        add_batch_with_remove(read_schema.clone()),
        &sidecar_paths[1],
    )?;
    
    // Third sidecar gets add actions again
    add_sidecar_to_store(
        store,
        add_batch_simple(read_schema.clone()),
        &sidecar_paths[2],
    )?;

    write_commit_to_store(
        store,
        vec![
            Action::Protocol(Protocol::try_new(
                3, 
                7, 
                vec!["v2Checkpoint"].into(), 
                vec!["v2Checkpoint"].into()
            )?),
            Action::Metadata(Metadata {
                id: "test-table".into(),
                schema_string: "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}".to_string(),
                ..Default::default()
            }),
        ],
        0,
    )?;

    // Version 1: Add a few files
    let add_actions = vec![
        Action::Add(Add {
            path: "file_1.parquet".into(),
            size: 1_000_000, 
            modification_time: 1_600_000_000_000,
            data_change: true,
            ..Default::default()
        }),
        Action::Add(Add {
            path: "file_2.parquet".into(),
            size: 2_000_000, 
            modification_time: 1_600_000_000_001,
            data_change: true,
            ..Default::default()
        }),
        Action::Add(Add {
            path: "file_3.parquet".into(),
            size: 3_000_000, 
            modification_time: 1_600_000_000_002,
            data_change: true,
            ..Default::default()
        }),
    ];
    
    write_commit_to_store(store, add_actions, 1)?;

    // Version 2: Add more files and remove one
    let add_remove_actions = vec![
        Action::Add(Add {
            path: "file_4.parquet".into(),
            size: 4_000_000,
            modification_time: 1_600_000_100_000,
            data_change: true,
            ..Default::default()
        }),
        Action::Add(Add {
            path: "file_5.parquet".into(),
            size: 5_000_000,
            modification_time: 1_600_000_100_001,
            data_change: true,
            ..Default::default()
        }),
        Action::Remove(Remove {
            path: "file_2.parquet".into(),
            data_change: true,
            ..Default::default()
        }),
    ];
    
    write_commit_to_store(store, add_remove_actions, 2)?;
    
    // Create Version 3: Checkpoint with sidecar references
    let checkpoint_uuid = uuid::Uuid::new_v4().to_string();
    let checkpoint_filename = format!("00000000000000000000.checkpoint.{}.parquet", checkpoint_uuid);
    
    // Create a batch with sidecar references
    let sidecars_with_paths = sidecar_paths.iter()
        .map(|path| format!("_sidecars/{}", path))
        .collect::<Vec<_>>();
    
    let sidecar_batch = sidecar_batch_with_given_paths(
        sidecars_with_paths.iter().map(|s| s.as_str()).collect(),
        crate::actions::get_log_schema().clone(),
    );

    // Add the checkpoint with sidecar references
    add_checkpoint_to_store(store, sidecar_batch, &checkpoint_filename)?;
    
    Ok(sidecars_with_paths)
}

/// Test for multi-file V2 checkpoint with sidecars
#[test]
fn test_multi_file_v2_checkpoint_with_sidecars() -> DeltaResult<()> {
    // Setup in-memory store and engine
    let (store, _) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create a table with V2Checkpoint support that already has sidecars
    let sidecar_paths = create_test_table_with_sidecars(&store)?;
    
    println!("Created test table with sidecars at paths:");
    for path in &sidecar_paths {
        println!(" - {}", path);
    }

    // Now try to load this table and read the checkpoint with sidecars
    println!("Loading table from existing multi-file checkpoint...");
    let table = Table::new(Url::parse("memory:///")?);
    let snapshot = table.snapshot(&engine, None)?;
    
    println!("Successfully loaded snapshot with version {}", snapshot.version());
    println!("Checkpoint files in snapshot: {}", snapshot.log_segment().checkpoint_parts.len());
    
    // Now try to build a new checkpoint using this snapshot
    println!("Building a new checkpoint from the existing table...");
    let builder = table.checkpoint(&engine, None)?
        .with_sidecar_support(3);
        
    // Build the manifest writer
    let mut writer = builder.build(&engine)?;
    
    // Get sidecar tasks - this should properly process the existing sidecars
    let sidecar_tasks = writer.get_sidecar_tasks(&engine)?;
    
    println!("Generated {} sidecar tasks from existing checkpoint", sidecar_tasks.len());
    assert!(sidecar_tasks.len() > 0, "Expected at least one sidecar task");
    
    // Verify the tasks by looking at their data
    for (idx, mut task) in sidecar_tasks.into_iter().enumerate() {
        println!("Examining sidecar task {}", idx);
        
        // Get the info for this task
        let info = task.get_checkpoint_info(&engine)?;
        println!("  Target path: {}", info.path);
        
        // Look at the actions in the task
        let mut action_count = 0;
        for action_result in info.data {
            let action = action_result?;
            let selected_count = action.selection_vector.iter().filter(|&v| *v).count();
            println!("  Found {} selected actions in batch", selected_count);
            action_count += selected_count;
        }
        
        println!("  Total actions for task {}: {}", idx, action_count);
    }
    
    Ok(())
}