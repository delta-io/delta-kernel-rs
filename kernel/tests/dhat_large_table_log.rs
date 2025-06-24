//! This tests our memory usage for reading tables with large/many log files.

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::local::LocalFileSystem;

use delta_kernel::Table;
use serde_json::json;
use tempfile::tempdir;
use tracing::info;
use url::Url;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const NUM_COMMITS: u64 = 1_000; // number of commit files to generate
const TOTAL_ACTIONS: u64 = 1_000_000; // total add/remove actions across all commits

/// Generate a delta log with NUM_COMMITS commit files and TOTAL_ACTIONS add/remove action pairs
fn generate_delta_log(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let delta_log_path = path.join("_delta_log");
    create_dir_all(&delta_log_path)?;

    let actions_per_commit = TOTAL_ACTIONS / NUM_COMMITS;
    let mut current_file_id = 0u64;

    for commit_id in 0..NUM_COMMITS {
        let commit_filename = format!("{:020}.json", commit_id);
        let commit_path = delta_log_path.join(&commit_filename);
        let mut file = File::create(&commit_path)?;

        let mut actions = Vec::new();

        // Add metadata and protocol only for the first commit
        if commit_id == 0 {
            actions.push(json!({
                "metaData": {
                    "id": format!("{:020}", commit_id),
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "configuration": {}
                }
            }));
            actions.push(json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }));
        }

        // Generate add/remove action pairs for this commit
        for _ in 0..actions_per_commit / 2 {
            // Add action
            actions.push(json!({
                "add": {
                    "path": format!("{}.parquet", current_file_id),
                    "partitionValues": {},
                    "size": 1000000,
                    "modificationTime": commit_id * 1000,
                    "dataChange": true
                }
            }));

            // Remove action (remove a previous file if we have any)
            if current_file_id > 0 {
                actions.push(json!({
                    "remove": {
                        "path": format!("{}.parquet", current_file_id - 1),
                        "deletionTimestamp": commit_id * 1000 + 500,
                        "dataChange": true
                    }
                }));
            }

            current_file_id += 1;
        }

        // Write actions to file
        for action in actions {
            writeln!(file, "{}", action)?;
        }
    }

    info!(
        "Generated {} commit files with {} total actions",
        NUM_COMMITS, TOTAL_ACTIONS
    );
    Ok(())
}

#[test]
fn test_dhat_large_table_log() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let table_path = dir.path();

    info!(
        "Generating delta log with {} commits and {} total actions",
        NUM_COMMITS, TOTAL_ACTIONS
    );
    generate_delta_log(table_path)?;

    let _profiler = dhat::Profiler::builder().testing().build();
    let store = Arc::new(LocalFileSystem::new());
    let url = "file:///".to_owned() + table_path.to_str().unwrap();
    let url = Url::parse(&url).unwrap();
    let engine = DefaultEngine::new(store, Arc::new(TokioBackgroundExecutor::new()));

    let table = Table::try_from_uri(&url).expect("Failed to create Delta Table");
    let snapshot = table
        .snapshot(&engine, None)
        .expect("Failed to get latest snapshot");

    let stats = dhat::HeapStats::get();
    println!("Heap stats after PM replay: {:?}", stats);

    let scan = snapshot
        .into_scan_builder()
        .build()
        .expect("Failed to build scan");
    let scan_metadata = scan
        .scan_metadata(&engine)
        .expect("Failed to get scan metadata");
    for res in scan_metadata {
        let _scan_metadata = res.expect("Failed to read scan metadata");
        // scan_metadata.visit_scan_files((), |_, file, _, _, _, _, _| {
        //     // do something
        // })?;
    }

    let stats = dhat::HeapStats::get();
    println!("Heap stats after Scan replay: {:?}", stats);

    Ok(())
}
