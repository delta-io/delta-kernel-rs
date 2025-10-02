use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::engine::to_json_bytes;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::{create_table, engine_store_setup};

use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

/// Convert a URL to an object_store::Path
fn url_to_object_store_path(url: &Url) -> Result<Path, Box<dyn std::error::Error>> {
    let path_segments = url
        .path_segments()
        .ok_or_else(|| format!("URL has no path segments: {}", url))?;

    let path_string = path_segments.skip(1).collect::<Vec<_>>().join("/");

    Ok(Path::from(path_string))
}

#[tokio::test]
async fn action_reconciliation_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    // Create a simple table schema: one int column named 'id'
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    // Setup engine and storage - this creates a proper temporary table
    let (store, engine, table_location) = engine_store_setup("test_compaction_table", None);

    // Create table (this will be commit 0)
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    // Commit 1: Add two files
    let commit1_content = r#"{"commitInfo":{"timestamp":1587968586000,"operation":"WRITE","operationParameters":{"mode":"Append"},"isBlindAppend":true}}
{"add":{"path":"part-00000-file1.parquet","partitionValues":{},"size":1024,"modificationTime":1587968586000,"dataChange":true, "stats":"{\"numRecords\":10,\"nullCount\":{\"id\":0},\"minValues\":{\"id\": 1},\"maxValues\":{\"id\":10}}"}}
{"add":{"path":"part-00001-file2.parquet","partitionValues":{},"size":2048,"modificationTime":1587968586000,"dataChange":true, "stats":"{\"numRecords\":20,\"nullCount\":{\"id\":0},\"minValues\":{\"id\": 11},\"maxValues\":{\"id\":30}}"}}
"#;
    store
        .put(
            &Path::from("test_compaction_table/_delta_log/00000000000000000001.json"),
            commit1_content.as_bytes().into(),
        )
        .await?;

    // Commit 2: Remove only the first file with a recent deletionTimestamp, keep the second file
    let current_timestamp_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let commit2_content = format!(
        r#"{{"commitInfo":{{"timestamp":{},"operation":"DELETE","operationParameters":{{"predicate":"id <= 10"}},"isBlindAppend":false}}}}
{{"remove":{{"path":"part-00000-file1.parquet","partitionValues":{{}},"size":1024,"modificationTime":1587968586000,"dataChange":true,"deletionTimestamp":{}}}}}
"#,
        current_timestamp_millis, current_timestamp_millis
    );
    store
        .put(
            &Path::from("test_compaction_table/_delta_log/00000000000000000002.json"),
            commit2_content.clone().into_bytes().into(),
        )
        .await?;

    // Create snapshot and log compaction writer
    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut writer = snapshot.log_compaction_writer(0, 2)?;

    // Get compaction data iterator
    let mut compaction_data = writer.compaction_data(&engine)?;
    let compaction_path = writer.compaction_path().clone();

    // Verify the compaction file name
    let expected_filename = "00000000000000000000.00000000000000000002.compacted.json";
    assert!(compaction_path.to_string().ends_with(expected_filename));

    // Process compaction data batches and collect the actual compacted data
    let mut batch_count = 0;
    let mut compacted_data_batches = Vec::new();

    // Log compaction should produce reconciled actions from the version range:
    // - Protocol + metadata from table creation
    // - Add action for file1 (first/newest add for this file path)
    // - Add action for file2 (first/newest add for this file path)
    // - Remove action for file1 (first/newest remove for this file path, non-expired tombstone)
    // - CommitInfo actions should be excluded from compaction
    //
    // Note: Actions are processed in reverse chronological order (newest to oldest).
    // The reconciliation keeps the first (newest) occurrence of each action type
    // for each unique file path, so both add and remove actions for file1 are kept.
    for batch_result in compaction_data.by_ref() {
        let batch = batch_result?;
        compacted_data_batches.push(batch);
        batch_count += 1;
    }

    assert!(
        batch_count > 0,
        "Should have processed at least one compaction batch"
    );

    // Convert the end-to-end flow of writing the JSON. We are going beyond the public
    // log compaction APIs since the test is writing the compacted JSON and verifying it
    // bu this is intentional, as most engines would be implementing something similar
    let compaction_data_iter = compacted_data_batches.into_iter().map(Ok);
    let json_bytes = to_json_bytes(compaction_data_iter)?;
    let final_content = String::from_utf8(json_bytes)?;

    let compaction_file_path = url_to_object_store_path(&compaction_path)?;

    store
        .put(&compaction_file_path, final_content.clone().into())
        .await?;

    // Verify the compacted file content that we just wrote
    let compacted_content = store.get(&compaction_file_path).await?;
    let compacted_bytes = compacted_content.bytes().await?;
    let compacted_str = std::str::from_utf8(&compacted_bytes)?;

    // Parse and verify the actions
    let compacted_lines: Vec<&str> = compacted_str.trim().lines().collect();
    assert!(
        !compacted_lines.is_empty(),
        "Compacted file should not be empty"
    );

    // Check for expected actions
    let has_protocol = compacted_lines.iter().any(|line| line.contains("protocol"));
    let has_metadata = compacted_lines.iter().any(|line| line.contains("metaData"));
    let has_remove = compacted_lines.iter().any(|line| line.contains("remove"));
    let has_add_file1 = compacted_lines
        .iter()
        .any(|line| line.contains("part-00000-file1.parquet") && line.contains("add"));
    let has_add_file2 = compacted_lines
        .iter()
        .any(|line| line.contains("part-00001-file2.parquet") && line.contains("add"));
    let has_commit_info = compacted_lines
        .iter()
        .any(|line| line.contains("commitInfo"));

    assert!(
        has_protocol,
        "Compacted file should contain protocol action"
    );
    assert!(
        has_metadata,
        "Compacted file should contain metadata action"
    );
    assert!(
        has_remove,
        "Compacted file should contain remove action (non-expired tombstone)"
    );
    assert!(
        !has_add_file1,
        "Compacted file should not contain add action for removed file file1"
    );
    assert!(
        has_add_file2,
        "Compacted file should contain add action for file2 (it was not removed)"
    );
    assert!(
        !has_commit_info,
        "Compacted file should NOT contain commitInfo actions (they should be excluded)"
    );

    // Verify the remove action has the current timestamp
    let remove_line = compacted_lines
        .iter()
        .find(|line| line.contains("remove"))
        .ok_or("Remove action should be present in compacted content")?;
    let parsed_remove: serde_json::Value = serde_json::from_str(remove_line)?;

    let actual_deletion_timestamp = parsed_remove["remove"]["deletionTimestamp"]
        .as_i64()
        .ok_or_else(|| {
            format!(
                "deletionTimestamp should be present in remove action: {}",
                remove_line
            )
        })?;
    assert_eq!(actual_deletion_timestamp, current_timestamp_millis);

    Ok(())
}

/// Test log compaction behavior with expired tombstones.
#[tokio::test]
async fn expired_tombstone_exclusion() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    let (store, engine, table_location) = engine_store_setup("test_expired_tombstone_table", None);

    // Create table (this will be commit 0)
    let table_url = create_table(
        store.clone(),
        table_location,
        schema.clone(),
        &[],
        false,
        vec![],
        vec![],
    )
    .await?;

    // Commit 1: Add three files
    let commit1_content = r#"{"commitInfo":{"timestamp":1587968586000,"operation":"WRITE","operationParameters":{"mode":"Append"},"isBlindAppend":true}}
{"add":{"path":"part-00000-expired-file.parquet","partitionValues":{},"size":1024,"modificationTime":1587968586000,"dataChange":true, "stats":"{\"numRecords\":10,\"nullCount\":{\"id\":0},\"minValues\":{\"id\": 1},\"maxValues\":{\"id\":10}}"}}
{"add":{"path":"part-00001-recent-file.parquet","partitionValues":{},"size":2048,"modificationTime":1587968586000,"dataChange":true, "stats":"{\"numRecords\":20,\"nullCount\":{\"id\":0},\"minValues\":{\"id\": 11},\"maxValues\":{\"id\":30}}"}}
{"add":{"path":"part-00002-keep-file.parquet","partitionValues":{},"size":3072,"modificationTime":1587968586000,"dataChange":true, "stats":"{\"numRecords\":30,\"nullCount\":{\"id\":0},\"minValues\":{\"id\": 31},\"maxValues\":{\"id\":60}}"}}
"#;
    store
        .put(
            &Path::from("test_expired_tombstone_table/_delta_log/00000000000000000001.json"),
            commit1_content.as_bytes().into(),
        )
        .await?;

    // Commit 2: Remove the first file with an expired (old) deletionTimestamp
    // Use a timestamp from 30 days ago (older than default 7-day retention)
    let expired_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - (30 * 24 * 60 * 60 * 1000); // 30 days ago

    let commit2_content = format!(
        r#"{{"commitInfo":{{"timestamp":{},"operation":"DELETE","operationParameters":{{"predicate":"id <= 10"}},"isBlindAppend":false}}}}
{{"remove":{{"path":"part-00000-expired-file.parquet","partitionValues":{{}},"size":1024,"modificationTime":1587968586000,"dataChange":true,"deletionTimestamp":{}}}}}
"#,
        expired_timestamp + 1000,
        expired_timestamp
    );
    store
        .put(
            &Path::from("test_expired_tombstone_table/_delta_log/00000000000000000002.json"),
            commit2_content.into_bytes().into(),
        )
        .await?;

    // Commit 3: Remove the second file with a recent (non-expired) deletionTimestamp
    let recent_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - (24 * 60 * 60 * 1000); // 1 day ago

    let commit3_content = format!(
        r#"{{"commitInfo":{{"timestamp":{},"operation":"DELETE","operationParameters":{{"predicate":"id BETWEEN 11 AND 30"}},"isBlindAppend":false}}}}
{{"remove":{{"path":"part-00001-recent-file.parquet","partitionValues":{{}},"size":2048,"modificationTime":1587968586000,"dataChange":true,"deletionTimestamp":{}}}}}
"#,
        recent_timestamp + 1000,
        recent_timestamp
    );
    store
        .put(
            &Path::from("test_expired_tombstone_table/_delta_log/00000000000000000003.json"),
            commit3_content.into_bytes().into(),
        )
        .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut writer = snapshot.log_compaction_writer(0, 3)?;

    let mut compaction_data = writer.compaction_data(&engine)?;
    let compaction_path = writer.compaction_path().clone();

    // Verify the compaction file name
    let expected_filename = "00000000000000000000.00000000000000000003.compacted.json";
    assert!(compaction_path.to_string().ends_with(expected_filename));

    let mut batch_count = 0;
    let mut compacted_data_batches = Vec::new();

    for batch_result in compaction_data.by_ref() {
        let batch = batch_result?;
        compacted_data_batches.push(batch);
        batch_count += 1;
    }

    assert!(
        batch_count > 0,
        "Should have processed at least one compaction batch"
    );

    // Convert to JSON and write to storage for verification
    let compaction_data_iter = compacted_data_batches.into_iter().map(Ok);
    let json_bytes = to_json_bytes(compaction_data_iter)?;
    let final_content = String::from_utf8(json_bytes)?;

    let compaction_file_path = url_to_object_store_path(&compaction_path)?;

    store
        .put(&compaction_file_path, final_content.clone().into())
        .await?;

    // Verify the compacted file content
    let compacted_content = store.get(&compaction_file_path).await?;
    let compacted_bytes = compacted_content.bytes().await?;
    let compacted_str = std::str::from_utf8(&compacted_bytes)?;

    // Parse and verify the actions
    let compacted_lines: Vec<&str> = compacted_str.trim().lines().collect();
    assert!(
        !compacted_lines.is_empty(),
        "Compacted file should not be empty"
    );

    // Check for expected actions
    let has_protocol = compacted_lines.iter().any(|line| line.contains("protocol"));
    let has_metadata = compacted_lines.iter().any(|line| line.contains("metaData"));
    let has_add_expired_file = compacted_lines
        .iter()
        .any(|line| line.contains("part-00000-expired-file.parquet") && line.contains("add"));
    let has_add_recent_file = compacted_lines
        .iter()
        .any(|line| line.contains("part-00001-recent-file.parquet") && line.contains("add"));
    let has_add_keep_file = compacted_lines
        .iter()
        .any(|line| line.contains("part-00002-keep-file.parquet") && line.contains("add"));
    let has_remove_expired_file = compacted_lines
        .iter()
        .any(|line| line.contains("part-00000-expired-file.parquet") && line.contains("remove"));
    let has_remove_recent_file = compacted_lines
        .iter()
        .any(|line| line.contains("part-00001-recent-file.parquet") && line.contains("remove"));
    let has_commit_info = compacted_lines
        .iter()
        .any(|line| line.contains("commitInfo"));

    assert!(
        has_protocol,
        "Compacted file should contain protocol action"
    );
    assert!(
        has_metadata,
        "Compacted file should contain metadata action"
    );
    assert!(
        !has_add_expired_file,
        "Compacted file should not contain add action for file that was removed by expired remove action"
    );
    assert!(
        !has_add_recent_file,
        "Compacted file should not contain add action for recent file that was removed"
    );
    assert!(
        has_add_keep_file,
        "Compacted file should contain add action for kept file (file that was never removed)"
    );
    assert!(
        !has_commit_info,
        "Compacted file should NOT contain commitInfo actions (they should be excluded)"
    );
    assert!(
        !has_remove_expired_file,
        "Compacted file should NOT contain remove action for expired file"
    );
    assert!(
        has_remove_recent_file,
        "Compacted file should contain remove action for recent file"
    );

    // Verify the recent remove action has the expected timestamp
    let recent_remove_line = compacted_lines
        .iter()
        .find(|line| line.contains("part-00001-recent-file.parquet") && line.contains("remove"))
        .ok_or("Recent remove action should be present in compacted content")?;
    let parsed_remove: serde_json::Value = serde_json::from_str(recent_remove_line)?;

    let actual_deletion_timestamp = parsed_remove["remove"]["deletionTimestamp"]
        .as_i64()
        .ok_or_else(|| {
            format!(
                "deletionTimestamp should be present in recent remove action: {}",
                recent_remove_line
            )
        })?;
    assert_eq!(actual_deletion_timestamp, recent_timestamp);

    let total_actions = compacted_lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .count();
    assert!(
        total_actions >= 4,
        "Should have at least 4 actions: protocol, metadata, 1 add, 1 remove (recent). Found {}",
        total_actions
    );
    Ok(())
}
