//! Integration tests for remove-file and deletion-vector-update write paths.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::{column_expr, Scalar};
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::CommitResult;
use delta_kernel::{Expression as Expr, Predicate as Pred, Snapshot};
use itertools::Itertools;
use serde_json::Deserializer;
use tempfile::tempdir;
use test_utils::{
    begin_transaction, copy_directory, create_default_engine, create_default_engine_mt_executor,
    load_and_begin_transaction, read_actions_from_commit, setup_test_tables,
};
use url::Url;

use crate::common::write_utils::{
    create_dv_table_with_files, get_scan_files, get_simple_int_schema, set_table_properties,
    write_data_and_check_result_and_stats,
};

#[tokio::test]
async fn test_remove_files_adds_expected_entries() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that Remove actions generated from scan metadata contain all expected
    // fields from the Remove struct (defined in kernel/src/actions/mod.rs).
    //
    // This test uses the table-with-dv-small dataset which contains files with tags and deletion
    // vectors.
    //
    // Not populated in the dataset are (covered by row_tracking tests):
    // baseRowId (optional i64)
    // defaultRowCommitVersion (optional i64)
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_data_change(true);

    let scan = snapshot.scan_builder().build()?;
    let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;

    let (data, selection_vector) = scan_metadata.scan_files.into_parts();
    let remove_metadata = FilteredEngineData::try_new(data, selection_vector)?;

    txn.remove_files(remove_metadata);

    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly to verify remove actions
            let commit_path = tmp_table_path.join(format!("_delta_log/{commit_version:020}.json"));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Verify we have at least commitInfo and remove actions
            assert!(
                parsed_commits.len() >= 2,
                "Expected at least 2 actions (commitInfo + remove)"
            );

            // Extract the commitInfo timestamp to validate against deletionTimestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert!(
                !remove_actions.is_empty(),
                "Expected at least one remove action"
            );

            assert_eq!(remove_actions.len(), 1);
            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            // path (required)
            assert!(remove.get("path").is_some(), "Missing path field");
            let path = remove["path"].as_str().expect("path should be a string");
            assert_eq!(
                path,
                "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
            );

            // dataChange (required)
            assert_eq!(remove["dataChange"].as_bool(), Some(true));

            // deletionTimestamp (optional) - should match commit timestamp
            let deletion_timestamp = remove["deletionTimestamp"]
                .as_i64()
                .expect("Missing deletionTimestamp");
            assert_eq!(
                deletion_timestamp, commit_timestamp,
                "deletionTimestamp should match commit timestamp"
            );

            // extendedFileMetadata (optional)
            assert_eq!(remove["extendedFileMetadata"].as_bool(), Some(true));

            // partitionValues (optional)
            let partition_vals = remove["partitionValues"]
                .as_object()
                .expect("Missing partitionValues");
            assert_eq!(partition_vals.len(), 0);

            // size (optional)
            let size = remove["size"].as_i64().expect("Missing size");
            assert_eq!(size, 635);

            // stats (optional)
            let stats = remove["stats"].as_str().expect("Missing stats");
            let stats_json: serde_json::Value = serde_json::from_str(stats)?;
            assert_eq!(stats_json["numRecords"], 10);

            // tags (optional)
            let tags = remove["tags"].as_object().expect("Missing tags");
            assert_eq!(
                tags.get("INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MIN_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("MAX_INSERTION_TIME").and_then(|v| v.as_str()),
                Some("1677811178336000")
            );
            assert_eq!(
                tags.get("OPTIMIZE_TARGET_SIZE").and_then(|v| v.as_str()),
                Some("268435456")
            );

            // deletionVector (optional)
            let dv = remove["deletionVector"]
                .as_object()
                .expect("Missing deletionVector");
            assert_eq!(dv.get("storageType").and_then(|v| v.as_str()), Some("u"));
            assert_eq!(
                dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA")
            );
            assert_eq!(dv.get("offset").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(dv.get("sizeInBytes").and_then(|v| v.as_i64()), Some(36));
            assert_eq!(dv.get("cardinality").and_then(|v| v.as_i64()), Some(2));

            // Row tracking fields should be absent as the feature is was not enabled on writing
            // row_tracking tests cover having these populated.
            assert!(remove.get("baseRowId").is_none());
            assert!(remove.get("defaultRowCommitVersion").is_none());
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_update_deletion_vectors_adds_expected_entries(
) -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that deletion vector updates write proper Remove and Add actions
    // to the transaction log.
    //
    // NOTE: Additional unit tests for update_deletion_vectors exist in
    // kernel/src/transaction/mod.rs
    //
    // The test validates:
    // 1. Transaction setup for DV updates
    // 2. Scanning and extracting scan files with DV data
    // 3. Creating new DV descriptors for the files
    // 4. Calling update_deletion_vectors to update the DVs
    // 5. Committing and verifying the generated actions
    //
    // Expected commit log structure:
    // - commitInfo: Contains metadata about the transaction
    // - remove: Contains OLD deletion vector data and original file metadata
    // - add: Contains NEW deletion vector data and updated file metadata
    //
    // The test ensures:
    // - Remove action has the OLD DV descriptor with all 5 fields
    // - Add action has the NEW DV descriptor with all 5 fields
    // - All file metadata is preserved (size, stats, tags, partitionValues)
    // - dataChange is properly set to true
    // - deletionTimestamp matches commit timestamp
    use std::path::PathBuf;

    let _ = tracing_subscriber::fmt::try_init();

    let tmp_dir = tempdir()?;
    let tmp_table_path = tmp_dir.path().join("table-with-dv-small");
    let source_path = std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/"))?;
    copy_directory(&source_path, &tmp_table_path)?;

    let table_url = url::Url::from_directory_path(&tmp_table_path).unwrap();
    let engine = create_default_engine(&table_url)?;

    let snapshot = Snapshot::builder_for(table_url.clone())
        .at_version(1)
        .build(engine.as_ref())?;

    // Create transaction with DV update mode enabled
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    // Build scan and collect all scan metadata
    let scan = snapshot.clone().scan_builder().build()?;
    let all_scan_metadata: Vec<_> = scan
        .scan_metadata(engine.as_ref())?
        .collect::<Result<Vec<_>, _>>()?;

    // Extract scan files for DV update
    let scan_files: Vec<_> = all_scan_metadata
        .into_iter()
        .map(|sm| sm.scan_files)
        .collect();

    // Create new DV descriptors for the files
    let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";
    let mut dv_map = HashMap::new();

    // Create a NEW deletion vector descriptor (different from the original)
    let new_dv = DeletionVectorDescriptor {
        storage_type: DeletionVectorStorageType::PersistedRelative,
        path_or_inline_dv: "cd^-aqEH.-t@S}K{vb[*k^".to_string(),
        offset: Some(10),
        size_in_bytes: 40,
        cardinality: 3,
    };
    dv_map.insert(file_path.to_string(), new_dv);

    // Call update_deletion_vectors to exercise the API
    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the original version 1 log to get original file metadata
            let original_log_path = tmp_table_path.join("_delta_log/00000000000000000001.json");
            let original_log_content = std::fs::read_to_string(original_log_path)?;
            let original_commits: Vec<_> = Deserializer::from_str(&original_log_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            let file_path = "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet";

            // Extract original file metadata from version 1
            let original_add = original_commits
                .iter()
                .find(|action| {
                    action
                        .get("add")
                        .and_then(|add| add.get("path").and_then(|p| p.as_str()))
                        == Some(file_path)
                })
                .expect("Missing original add action in version 1")
                .get("add")
                .expect("Should have add field");

            let original_size = original_add["size"]
                .as_i64()
                .expect("Original add action should have size");
            let original_partition_values = original_add["partitionValues"]
                .as_object()
                .expect("Original add action should have partitionValues");
            let original_tags = original_add.get("tags");
            let original_stats = original_add.get("stats");

            // Read the commit log directly
            let commit_path = tmp_table_path.join(format!("_delta_log/{commit_version:020}.json"));
            let commit_content = std::fs::read_to_string(commit_path)?;

            let parsed_commits: Vec<_> = Deserializer::from_str(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Should have commitInfo, remove, and add actions
            assert!(
                parsed_commits.len() >= 3,
                "Expected at least 3 actions (commitInfo + remove + add), got {}",
                parsed_commits.len()
            );

            // Extract commitInfo timestamp
            let commit_info_action = parsed_commits
                .iter()
                .find(|action| action.get("commitInfo").is_some())
                .expect("Missing commitInfo action");
            let commit_info = &commit_info_action["commitInfo"];
            let commit_timestamp = commit_info["timestamp"]
                .as_i64()
                .expect("Missing timestamp in commitInfo");

            // Verify remove action contains OLD DV information
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            assert_eq!(
                remove_actions.len(),
                1,
                "Expected exactly one remove action"
            );

            let remove_action = remove_actions[0];
            let remove = &remove_action["remove"];

            assert_eq!(
                remove["path"].as_str(),
                Some(file_path),
                "Remove path should match"
            );
            assert_eq!(remove["dataChange"].as_bool(), Some(true));
            assert_eq!(
                remove["deletionTimestamp"].as_i64(),
                Some(commit_timestamp),
                "deletionTimestamp should match commit timestamp"
            );

            // Verify OLD deletion vector in remove action
            let old_dv = remove["deletionVector"]
                .as_object()
                .expect("Remove action should have deletionVector");
            assert_eq!(
                old_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "Old DV storage type should be 'u'"
            );
            assert_eq!(
                old_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("vBn[lx{q8@P<9BNH/isA"),
                "Old DV path should match original"
            );
            assert_eq!(
                old_dv.get("offset").and_then(|v| v.as_i64()),
                Some(1),
                "Old DV offset should be 1"
            );
            assert_eq!(
                old_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(36),
                "Old DV size should be 36"
            );
            assert_eq!(
                old_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(2),
                "Old DV cardinality should be 2"
            );

            // Verify file metadata is preserved in remove action
            let remove_size = remove["size"]
                .as_i64()
                .expect("Remove action should have size");
            let remove_partition_values = remove["partitionValues"]
                .as_object()
                .expect("Remove action should have partitionValues");
            let remove_tags = remove.get("tags");
            let remove_stats = remove.get("stats");

            // Verify add action contains NEW DV information
            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            assert_eq!(add_actions.len(), 1, "Expected exactly one add action");

            let add_action = add_actions[0];
            let add = &add_action["add"];

            assert_eq!(
                add["path"].as_str(),
                Some(file_path),
                "Add path should match"
            );
            assert_eq!(add["dataChange"].as_bool(), Some(true));

            // Verify NEW deletion vector in add action
            let new_dv = add["deletionVector"]
                .as_object()
                .expect("Add action should have deletionVector");
            assert_eq!(
                new_dv.get("storageType").and_then(|v| v.as_str()),
                Some("u"),
                "New DV storage type should be 'u'"
            );
            assert_eq!(
                new_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                Some("cd^-aqEH.-t@S}K{vb[*k^"),
                "New DV path should match updated value"
            );
            assert_eq!(
                new_dv.get("offset").and_then(|v| v.as_i64()),
                Some(10),
                "New DV offset should be 10"
            );
            assert_eq!(
                new_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                Some(40),
                "New DV size should be 40"
            );
            assert_eq!(
                new_dv.get("cardinality").and_then(|v| v.as_i64()),
                Some(3),
                "New DV cardinality should be 3"
            );

            // Verify file metadata is preserved in add action
            let add_size = add["size"].as_i64().expect("Add action should have size");
            let add_partition_values = add["partitionValues"]
                .as_object()
                .expect("Add action should have partitionValues");
            let add_tags = add.get("tags");
            let add_stats = add.get("stats");

            // Ensure metadata is consistent between remove and add actions
            assert_eq!(
                remove_size, add_size,
                "File size should be preserved between remove and add"
            );
            assert_eq!(
                remove_partition_values, add_partition_values,
                "Partition values should be preserved between remove and add"
            );
            assert_eq!(
                remove_tags, add_tags,
                "Tags should be preserved between remove and add"
            );
            assert_eq!(
                remove_stats, add_stats,
                "Stats should be preserved between remove and add"
            );

            // Ensure metadata matches the original file metadata from version 1
            assert_eq!(
                remove_size, original_size,
                "Remove action size should match original file size"
            );
            assert_eq!(
                add_size, original_size,
                "Add action size should match original file size"
            );
            assert_eq!(
                remove_partition_values, original_partition_values,
                "Remove action partition values should match original"
            );
            assert_eq!(
                add_partition_values, original_partition_values,
                "Add action partition values should match original"
            );
            assert_eq!(
                remove_tags, original_tags,
                "Remove action tags should match original"
            );
            assert_eq!(
                add_tags, original_tags,
                "Add action tags should match original"
            );
            assert_eq!(
                remove_stats, original_stats,
                "Remove action stats should match original"
            );
            assert_eq!(
                add_stats, original_stats,
                "Add action stats should match original"
            );
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_update_deletion_vectors_multiple_files() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies that update_deletion_vectors can update multiple files
    // in a single call, creating proper Remove and Add actions for each file.
    let _ = tracing_subscriber::fmt::try_init();

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?);

    // Setup: Create table with 3 files
    let file_names = &["file0.parquet", "file1.parquet", "file2.parquet"];
    let (store, engine, table_url, file_paths) =
        create_dv_table_with_files("test_table", schema, file_names).await?;

    // Create DV update transaction
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_engine_info("test engine")
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    let mut scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;

    // Update deletion vectors for all 3 files in a single call
    let mut dv_map = HashMap::new();
    for (idx, file_path) in file_paths.iter().enumerate() {
        let descriptor = DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedRelative,
            path_or_inline_dv: format!("dv_file_{idx}.bin"),
            offset: Some(idx as i32 * 10),
            size_in_bytes: 40 + idx as i32,
            cardinality: idx as i64 + 1,
        };
        dv_map.insert(file_path.to_string(), descriptor);
    }

    txn.update_deletion_vectors(dv_map, scan_files.drain(..).map(Ok))?;

    // Commit the transaction
    let result = txn.commit(engine.as_ref())?;

    match result {
        CommitResult::CommittedTransaction(committed) => {
            let commit_version = committed.commit_version();

            // Read the commit log directly from object store
            let final_commit_path =
                table_url.join(&format!("_delta_log/{commit_version:020}.json"))?;
            let commit_content = store
                .get(&Path::from_url_path(final_commit_path.path())?)
                .await?
                .bytes()
                .await?;

            let parsed_commits: Vec<_> = Deserializer::from_slice(&commit_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            // Extract all remove and add actions
            let remove_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("remove").is_some())
                .collect();

            let add_actions: Vec<_> = parsed_commits
                .iter()
                .filter(|action| action.get("add").is_some())
                .collect();

            // Should have 3 remove and 3 add actions
            assert_eq!(
                remove_actions.len(),
                3,
                "Expected 3 remove actions for 3 files"
            );
            assert_eq!(add_actions.len(), 3, "Expected 3 add actions for 3 files");

            // Verify each file has a DV in both remove and add
            for (idx, file_path) in file_paths.iter().enumerate() {
                // Find the remove action for this file
                let remove_action = remove_actions
                    .iter()
                    .find(|action| action["remove"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find remove action for {file_path}"));

                // Find the add action for this file
                let add_action = add_actions
                    .iter()
                    .find(|action| action["add"]["path"].as_str() == Some(file_path.as_str()))
                    .unwrap_or_else(|| panic!("Should find add action for {file_path}"));

                // Verify remove action does NOT have a DV (since these were newly written files)
                assert!(
                    remove_action["remove"]["deletionVector"].is_null(),
                    "Remove action for newly written file should not have a DV"
                );

                // Verify add action has the NEW DV
                let add_dv = add_action["add"]["deletionVector"]
                    .as_object()
                    .expect("Add action should have deletionVector");

                let expected_path = format!("dv_file_{idx}.bin");
                assert_eq!(
                    add_dv.get("pathOrInlineDv").and_then(|v| v.as_str()),
                    Some(expected_path.as_str()),
                    "DV path should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("offset").and_then(|v| v.as_i64()),
                    Some(idx as i64 * 10),
                    "DV offset should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("sizeInBytes").and_then(|v| v.as_i64()),
                    Some(40 + idx as i64),
                    "DV size should match for file {file_path}"
                );
                assert_eq!(
                    add_dv.get("cardinality").and_then(|v| v.as_i64()),
                    Some(idx as i64 + 1),
                    "DV cardinality should match for file {file_path}"
                );
            }
        }
        _ => panic!("Transaction should be committed"),
    }

    Ok(())
}

#[tokio::test]
async fn test_remove_files_verify_files_excluded_from_scan(
) -> Result<(), Box<dyn std::error::Error>> {
    // Adds and then removes files and then verifies they don't appear in the scan.

    // setup tracing
    let _ = tracing_subscriber::fmt::try_init();

    // create a simple table: one int column named 'number'
    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        // First, add some files to the table
        let engine = Arc::new(engine);
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;
        let scan_metadata = scan.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (_, selection_vector) = scan_metadata.scan_files.into_parts();
        let initial_file_count = selection_vector.iter().filter(|&x| *x).count();

        assert!(initial_file_count > 0);

        // Now create a transaction to remove files
        let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?;

        // Create a new scan to get file metadata for removal
        let scan2 = snapshot.scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;

        // Create FilteredEngineData for removal (select all rows for removal)
        let file_remove_count = (scan_metadata2.scan_files.data().len()
            - scan_metadata2.scan_files.selection_vector().len())
            + scan_metadata2
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        assert!(file_remove_count > 0);

        // Add remove files to transaction
        txn.remove_files(scan_metadata2.scan_files);

        // Commit the transaction
        let result = txn.commit(engine.as_ref());

        match result? {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 2);

                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(2)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    new_file_count += new_metadata?.scan_files.data().len();
                }

                // All files were removed, so new_file_count should be zero
                assert_eq!(new_file_count, 0);
            }
            _ => panic!("Transaction did not succeeed."),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_remove_files_with_modified_selection_vector() -> Result<(), Box<dyn std::error::Error>>
{
    // This test verifies that we can selectively remove files by:
    // 1. Calling remove_files multiple times with different subsets
    // 2. Modifying the selection vector to choose which files to remove

    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], None, "test_table").await?
    {
        let engine = Arc::new(engine);

        // Write data multiple times to create multiple files
        for i in 1..=5 {
            write_data_and_check_result_and_stats(
                table_url.clone(),
                schema.clone(),
                engine.clone(),
                i,
            )
            .await?;
        }

        // Get initial file count
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let scan = snapshot.clone().scan_builder().build()?;

        let mut initial_file_count = 0;
        for metadata in scan.scan_metadata(engine.as_ref())? {
            let metadata = metadata?;
            initial_file_count += metadata
                .scan_files
                .selection_vector()
                .iter()
                .filter(|&x| *x)
                .count();
        }

        assert!(
            initial_file_count >= 3,
            "Need at least 3 files for this test, got {initial_file_count}"
        );

        // Create a transaction to remove files in two batches
        let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
            .with_engine_info("selective remove test")
            .with_operation("DELETE".to_string())
            .with_data_change(true);

        // First batch: Remove only the first file
        let scan2 = snapshot.clone().scan_builder().build()?;
        let scan_metadata2 = scan2.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data, mut selection_vector) = scan_metadata2.scan_files.into_parts();

        // Select only the first file for removal
        let mut first_batch_removed = 0;
        for selected in selection_vector.iter_mut() {
            if *selected && first_batch_removed < 1 {
                // Keep selected for removal
                first_batch_removed += 1;
            } else {
                // Don't remove
                *selected = false;
            }
        }

        assert_eq!(
            first_batch_removed, 1,
            "Should remove exactly 1 file in first batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data, selection_vector)?);

        // Second batch: Remove only the last file
        let scan3 = snapshot.clone().scan_builder().build()?;
        let scan_metadata3 = scan3.scan_metadata(engine.as_ref())?.next().unwrap()?;
        let (data2, mut selection_vector2) = scan_metadata3.scan_files.into_parts();

        // Find the last selected file and keep only that one selected
        let mut last_selected_idx = None;
        for (i, &selected) in selection_vector2.iter().enumerate() {
            if selected {
                last_selected_idx = Some(i);
            }
        }

        // Deselect all except the last one
        for (i, selected) in selection_vector2.iter_mut().enumerate() {
            if Some(i) != last_selected_idx {
                *selected = false;
            }
        }

        let second_batch_removed = selection_vector2.iter().filter(|&x| *x).count();
        assert_eq!(
            second_batch_removed, 1,
            "Should remove exactly 1 file in second batch"
        );
        txn.remove_files(FilteredEngineData::try_new(data2, selection_vector2)?);

        // Commit the transaction
        let result = txn.commit(engine.as_ref())?;

        match result {
            CommitResult::CommittedTransaction(committed) => {
                assert_eq!(committed.commit_version(), 6);

                // Verify that exactly 2 files were removed (1 from each batch)
                let new_snapshot = Snapshot::builder_for(table_url.clone())
                    .at_version(6)
                    .build(engine.as_ref())?;

                let new_scan = new_snapshot.scan_builder().build()?;
                let mut new_file_count = 0;
                for new_metadata in new_scan.scan_metadata(engine.as_ref())? {
                    let metadata = new_metadata?;
                    new_file_count += metadata
                        .scan_files
                        .selection_vector()
                        .iter()
                        .filter(|&x| *x)
                        .count();
                }

                // Verify we removed exactly 2 files (1 + 1)
                let total_removed = first_batch_removed + second_batch_removed;
                assert_eq!(total_removed, 2);
                assert_eq!(new_file_count, initial_file_count - total_removed);
                assert!(new_file_count > 0, "At least one file should remain");
            }
            _ => panic!("Transaction did not succeed"),
        }
    }
    Ok(())
}

/// Regression test for https://github.com/delta-io/delta-kernel-rs/issues/2040
///
/// When `scan_metadata()` is called with a predicate, the scan row schema includes a
/// `stats_parsed` column (7th column). Passing that scan metadata to `remove_files()` then
/// `commit()` previously failed with "Too few fields in output schema" because the transform
/// evaluator exhausted the output schema when it encountered the extra column.
///
/// Both predicate and non-predicate scans are tested because `remove_files` should behave
/// identically regardless. Cases also vary the checkpoint format:
/// - `use_struct_stats_checkpoint=false`: `stats` is non-null (raw JSON from the Add action). The
///   remove action's `stats` comes from passthrough.
/// - `use_struct_stats_checkpoint=true`: a checkpoint is written with `writeStatsAsJson=false,
///   writeStatsAsStruct=true`, so the checkpoint stores `stats_parsed` but omits the raw `stats`
///   JSON string. Scan rows from that checkpoint have `stats=null` but `stats_parsed` non-null. The
///   remove action's `stats` is produced via `coalesce(null, to_json(stats_parsed))`, which
///   exercises the coalesce path in the fix.
#[rstest::rstest]
#[case(false, false)]
#[case(false, true)]
#[case(true, false)]
#[case(true, true)]
// Multi-thread runtime required because the struct-stats checkpoint case calls
// `Snapshot::checkpoint`, which makes nested `block_on` calls that deadlock
// a single-threaded executor. `TokioMultiThreadExecutor` uses `block_in_place`
// which avoids the deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_remove_files_after_predicate_scan_includes_stats_parsed(
    #[case] use_struct_stats_checkpoint: bool,
    #[case] use_predicate: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let schema = get_simple_int_schema();

    // Use a local directory so we can inspect the commit log for stats content.
    let tmp_dir = tempdir()?;
    let tmp_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    for (table_url, engine, _store, _table_name) in
        setup_test_tables(schema.clone(), &[], Some(&tmp_url), "test_table").await?
    {
        let engine = Arc::new(engine);

        // Write data (two parquet files with numbers [1,2,3] and [4,5,6]).
        write_data_and_check_result_and_stats(table_url.clone(), schema.clone(), engine.clone(), 1)
            .await?;

        // When use_struct_stats_checkpoint=true, update table properties so the checkpoint
        // omits the stats JSON string (writeStatsAsJson=false) but stores stats as a struct
        // (writeStatsAsStruct=true). After checkpointing, scan rows from the checkpoint have
        // stats=null and stats_parsed=non-null, exercising the coalesce path in the fix.
        let snapshot = if use_struct_stats_checkpoint {
            let table_path = table_url.to_file_path().unwrap();
            let snapshot_v2 = set_table_properties(
                table_path.to_str().unwrap(),
                &table_url,
                engine.as_ref(),
                1,
                &[
                    ("delta.checkpoint.writeStatsAsJson", "false"),
                    ("delta.checkpoint.writeStatsAsStruct", "true"),
                ],
            )?;
            // `Snapshot::checkpoint` makes nested `block_on` calls internally (it reads the
            // log segment lazily while writing). This requires `TokioMultiThreadExecutor`,
            // which uses `block_in_place` to avoid deadlocking a single-thread runtime.
            let mt_engine = create_default_engine_mt_executor(&table_url)?;
            snapshot_v2.checkpoint(mt_engine.as_ref(), None)?;
            Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?
        } else {
            Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?
        };

        // commit_version = 2 (no checkpoint) or 3 (properties commit + checkpoint bump)
        let expected_commit_version = if use_struct_stats_checkpoint { 3 } else { 2 };

        // Always request all stats columns so stats_parsed is present in scan metadata
        // regardless of whether a predicate is used. This ensures remove_files can always
        // reconstruct stats (including the coalesce path when writeStatsAsJson=false).
        let mut scan_builder = snapshot.clone().scan_builder().include_all_stats_columns();
        if use_predicate {
            scan_builder = scan_builder.with_predicate(Arc::new(Pred::gt(
                column_expr!("number"),
                Expr::literal(0_i32),
            )));
        }
        let scan = scan_builder.build()?;

        let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);

        // Pass scan metadata (which contains stats_parsed) directly to remove_files.
        // This previously failed with "Too few fields in output schema".
        for scan_metadata in scan.scan_metadata(engine.as_ref())? {
            txn.remove_files(scan_metadata?.scan_files);
        }

        let committed = txn.commit(engine.as_ref())?.unwrap_committed();
        assert_eq!(committed.commit_version(), expected_commit_version);

        let remove_actions =
            read_actions_from_commit(&table_url, expected_commit_version, "remove")?;
        assert!(
            !remove_actions.is_empty(),
            "expected remove actions in commit"
        );

        // stats must be populated in every remove action: stats_parsed is always present
        // (via include_all_stats_columns), so the coalesce path handles even checkpoints
        // that omit the raw JSON stats string (writeStatsAsJson=false).
        for remove in &remove_actions {
            let stats_str = remove["stats"]
                .as_str()
                .expect("stats field should be a non-null JSON string");
            let stats: serde_json::Value = serde_json::from_str(stats_str)?;
            assert!(
                stats["numRecords"].as_i64().unwrap_or(0) > 0,
                "stats.numRecords should be populated, got: {stats}"
            );
        }
    }
    Ok(())
}

/// Remove files via scan metadata on a partitioned table. Covers three predicate
/// shapes against the same table so the remove-transform correctly handles the
/// parsed scan columns in every combination:
/// - no predicate: no `partitionValues_parsed`.
/// - data-column predicate: no `partitionValues_parsed` (negative case; the fix must not affect
///   scans whose predicate misses the partition columns).
/// - partition predicate: `partitionValues_parsed` present.
///
/// Every case calls `.include_all_stats_columns()`, which forces `stats_parsed`
/// into the scan output regardless of the predicate shape, so the partition-
/// predicate case exercises both parsed-column drop paths together while the
/// other two exercise only the `stats_parsed` drop path. The coalesce
/// *reconstruction* of `stats` from `stats_parsed` is not exercised here
/// because `stats` is non-null; the sibling
/// `test_remove_files_after_predicate_scan_includes_stats_parsed` covers that.
///
/// `expected_partitions` is the multiset of `country` values expected across
/// the generated Remove actions. Its length gives the expected Remove count,
/// and its contents pin the correct partition was chosen (catches regressions
/// where the wrong partition is removed).
#[rstest::rstest]
#[case::no_predicate(None, &["usa", "japan"])]
#[case::data_predicate(
    Some(Pred::gt(column_expr!("id"), Expr::literal(0_i32))),
    &["usa", "japan"]
)]
#[case::partition_predicate(
    Some(Pred::eq(column_expr!("country"), Expr::literal("usa".to_string()))),
    &["usa"]
)]
#[tokio::test]
async fn test_remove_files_partitioned_with_parsed_columns(
    #[case] predicate: Option<Pred>,
    #[case] expected_partitions: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let partition_col = "country";
    let table_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("country", DataType::STRING),
    ])?);
    let data_schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);

    // Local directory backing: `read_actions_from_commit` reads commit JSON off disk
    // and does not support the default in-memory store's `memory://` URL.
    let tmp_dir = tempdir()?;
    let tmp_url = Url::from_directory_path(tmp_dir.path()).unwrap();

    for (table_url, engine, _store, _table_name) in setup_test_tables(
        table_schema.clone(),
        &[partition_col],
        Some(&tmp_url),
        "test_table",
    )
    .await?
    {
        let engine = Arc::new(engine);

        // Write two partitions: country="usa" and country="japan".
        let mut txn =
            load_and_begin_transaction(table_url.clone(), engine.as_ref())?.with_data_change(true);
        let append_data = [[1, 2, 3], [10, 20, 30]].map(|data| -> delta_kernel::DeltaResult<_> {
            let data = RecordBatch::try_new(
                Arc::new(data_schema.as_ref().try_into_arrow()?),
                vec![Arc::new(Int32Array::from(data.to_vec()))],
            )?;
            Ok(Box::new(ArrowEngineData::new(data)))
        });
        for (data, partition_val) in append_data.into_iter().zip(["usa", "japan"]) {
            let ctx = Arc::new(txn.partitioned_write_context(HashMap::from([(
                partition_col.to_string(),
                Scalar::String(partition_val.into()),
            )]))?);
            let add_meta = engine.write_parquet(data?.as_ref(), ctx.as_ref()).await?;
            txn.add_files(add_meta);
        }
        txn.commit(engine.as_ref())?.unwrap_committed();

        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let mut scan_builder = snapshot.clone().scan_builder().include_all_stats_columns();
        if let Some(pred) = predicate.clone() {
            scan_builder = scan_builder.with_predicate(Arc::new(pred));
        }
        let scan = scan_builder.build()?;

        let mut txn = begin_transaction(snapshot, engine.as_ref())?.with_data_change(true);
        for scan_metadata in scan.scan_metadata(engine.as_ref())? {
            txn.remove_files(scan_metadata?.scan_files);
        }
        let committed = txn.commit(engine.as_ref())?.unwrap_committed();
        assert_eq!(committed.commit_version(), 2);

        let remove_actions = read_actions_from_commit(&table_url, 2, "remove")?;
        assert_eq!(
            remove_actions.len(),
            expected_partitions.len(),
            "unexpected remove count; got {}: {remove_actions:?}",
            remove_actions.len()
        );

        let mut actual_partitions: Vec<String> = remove_actions
            .iter()
            .filter_map(|r| {
                r["partitionValues"][partition_col]
                    .as_str()
                    .map(String::from)
            })
            .collect();
        actual_partitions.sort();
        let mut expected_sorted: Vec<String> =
            expected_partitions.iter().map(|s| s.to_string()).collect();
        expected_sorted.sort();
        assert_eq!(
            actual_partitions, expected_sorted,
            "partitionValues mismatch across removes; got: {remove_actions:?}"
        );

        // stats_parsed is present on every scan row, so the stats-with-parsed
        // evaluator is selected for every case; it must still yield a populated
        // stats JSON on every remove action.
        for remove in &remove_actions {
            let stats_str = remove["stats"]
                .as_str()
                .expect("stats field should be a non-null JSON string");
            let stats: serde_json::Value = serde_json::from_str(stats_str)?;
            assert!(
                stats["numRecords"].as_i64().unwrap_or(0) > 0,
                "stats.numRecords should be populated, got: {stats}"
            );
        }
    }
    Ok(())
}
