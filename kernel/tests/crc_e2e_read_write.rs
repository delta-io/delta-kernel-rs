//! End-to-end tests for CRC (version checksum) file writing and reading.
//!
//! Tests the full lifecycle: create table -> write data -> write CRC -> read CRC back -> verify.
//! Covers clustering, user domain metadata, ICT, multi-version CRC chains, and edge cases.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::DeltaResult;
use test_utils::{create_add_files_metadata, test_table_setup};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Read the CRC file at the given version as raw JSON.
fn read_crc_json(table_path: &str, version: u64) -> serde_json::Value {
    let crc_path = format!("{}/_delta_log/{:020}.crc", table_path, version);
    let content = std::fs::read_to_string(&crc_path)
        .unwrap_or_else(|e| panic!("Failed to read CRC file at {crc_path}: {e}"));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse CRC JSON at {crc_path}: {e}"))
}

/// Check that a CRC file exists at the given version.
fn crc_file_exists(table_path: &str, version: u64) -> bool {
    let crc_path = format!("{}/_delta_log/{:020}.crc", table_path, version);
    std::path::Path::new(&crc_path).exists()
}

/// Helper to commit a transaction and assert it was committed, returning the CommittedTransaction.
fn assert_committed(result: CommitResult) -> delta_kernel::transaction::CommittedTransaction {
    match result {
        CommitResult::CommittedTransaction(committed) => committed,
        other => panic!("Expected CommittedTransaction, got: {other:?}"),
    }
}

/// Seed a CRC JSON value from the commit file at the given version.
///
/// Extracts protocol and metadata from the commit JSON and constructs a minimal CRC.
/// Used for tables created with the low-level test helper (which doesn't produce CRC files).
fn seed_crc_from_commit(table_path: &str, version: u64) -> serde_json::Value {
    let commit_path = format!("{}/_delta_log/{:020}.json", table_path, version);
    let content = std::fs::read_to_string(&commit_path)
        .unwrap_or_else(|e| panic!("Failed to read commit file at {commit_path}: {e}"));

    let mut protocol = serde_json::Value::Null;
    let mut metadata = serde_json::Value::Null;
    let mut ict = serde_json::Value::Null;

    for line in content.lines() {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(p) = val.get("protocol") {
                protocol = p.clone();
            }
            if let Some(m) = val.get("metaData") {
                metadata = m.clone();
            }
            if let Some(ci) = val.get("commitInfo") {
                if let Some(ts) = ci.get("inCommitTimestamp") {
                    ict = ts.clone();
                }
            }
        }
    }

    serde_json::json!({
        "tableSizeBytes": 0,
        "numFiles": 0,
        "numMetadata": 1,
        "numProtocol": 1,
        "inCommitTimestampOpt": ict,
        "metadata": metadata,
        "protocol": protocol,
    })
}

/// Helper to create a simple two-column schema.
fn simple_schema() -> DeltaResult<Arc<StructType>> {
    Ok(Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("value", DataType::STRING, true),
    ])?))
}

// -- Test 1: Create a clustered table with user domain metadata, write CRC at v0 -----------

#[tokio::test]
async fn test_crc_create_table_with_clustering_and_domain_metadata() -> DeltaResult<()> {
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("name", DataType::STRING, true),
    ])?);

    // Create clustered table with user domain metadata
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let user_domain = "app.settings";
    let user_config = r#"{"version":1,"enabled":true}"#;

    let result = txn
        .with_domain_metadata(user_domain.to_string(), user_config.to_string())
        .commit(engine.as_ref())?;

    let committed = assert_committed(result);
    assert_eq!(committed.commit_version(), 0);

    // Write CRC at version 0 (create-table case, no previous CRC needed)
    let crc_written = committed.write_crc(engine.as_ref())?;
    assert!(
        crc_written,
        "CRC should be writable for create-table commits"
    );

    // Verify CRC file exists
    assert!(crc_file_exists(&table_path, 0));

    // Read and verify CRC contents
    let crc = read_crc_json(&table_path, 0);
    assert_eq!(crc["tableSizeBytes"], 0, "No files added yet");
    assert_eq!(crc["numFiles"], 0, "No files added yet");
    assert_eq!(crc["numMetadata"], 1);
    assert_eq!(crc["numProtocol"], 1);

    // Verify domain metadata: should have clustering domain + user domain
    let domains = crc["domainMetadata"]
        .as_array()
        .expect("domainMetadata should be an array");
    assert!(
        domains.len() >= 2,
        "Should have at least clustering + user domain, got {}",
        domains.len()
    );

    // Find the user domain
    let user_dm = domains
        .iter()
        .find(|d| d["domain"].as_str() == Some(user_domain))
        .expect("User domain metadata should be in CRC");
    assert_eq!(user_dm["configuration"].as_str(), Some(user_config));
    assert_eq!(user_dm["removed"], false);

    // Find the clustering domain
    let clustering_dm = domains
        .iter()
        .find(|d| {
            d["domain"]
                .as_str()
                .is_some_and(|s| s.starts_with("delta.clustering"))
        })
        .expect("Clustering domain metadata should be in CRC");
    assert_eq!(clustering_dm["removed"], false);

    // Verify protocol has clustering + domainMetadata features
    let writer_features = crc["protocol"]["writerFeatures"]
        .as_array()
        .expect("writerFeatures should be an array");
    let writer_feature_strs: Vec<&str> =
        writer_features.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        writer_feature_strs.contains(&"clustering"),
        "Writer features should include clustering"
    );
    assert!(
        writer_feature_strs.contains(&"domainMetadata"),
        "Writer features should include domainMetadata"
    );

    // Verify metadata schema string includes our columns
    let schema_string = crc["metadata"]["schemaString"]
        .as_str()
        .expect("schemaString should be present");
    assert!(schema_string.contains("\"name\":\"id\""));
    assert!(schema_string.contains("\"name\":\"name\""));

    // Verify clustering is accessible via snapshot after creating from the CRC-backed table
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let clustering_cols = snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_cols, Some(vec![ColumnName::new(["id"])]));

    Ok(())
}

// -- Test 2: Multi-version CRC chain (create -> write -> write -> write) ----

#[tokio::test]
async fn test_crc_multi_version_chain() -> TestResult {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // v0: Create table, write CRC
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed_v0 = assert_committed(txn.commit(engine.as_ref())?);
    assert_eq!(committed_v0.commit_version(), 0);
    assert!(committed_v0.write_crc(engine.as_ref())?);

    let crc_v0 = read_crc_json(&table_path, 0);
    assert_eq!(crc_v0["tableSizeBytes"], 0);
    assert_eq!(crc_v0["numFiles"], 0);

    // v1: Add 2 files (sizes 1000 and 2000), write CRC
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files_v1 = vec![
        ("file1.parquet", 1000_i64, 1000000_i64, 100_i64),
        ("file2.parquet", 2000, 1000001, 200),
    ];
    let metadata_v1 = create_add_files_metadata(txn_v1.add_files_schema(), files_v1)?;
    txn_v1.add_files(metadata_v1);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);
    assert_eq!(committed_v1.commit_version(), 1);
    assert!(committed_v1.write_crc(engine.as_ref())?);

    let crc_v1 = read_crc_json(&table_path, 1);
    assert_eq!(crc_v1["tableSizeBytes"], 3000);
    assert_eq!(crc_v1["numFiles"], 2);

    // v2: Add 1 more file (size 500), write CRC
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v2 = snapshot_v1
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files_v2 = vec![("file3.parquet", 500_i64, 1000002_i64, 50_i64)];
    let metadata_v2 = create_add_files_metadata(txn_v2.add_files_schema(), files_v2)?;
    txn_v2.add_files(metadata_v2);
    let committed_v2 = assert_committed(txn_v2.commit(engine.as_ref())?);
    assert_eq!(committed_v2.commit_version(), 2);
    assert!(committed_v2.write_crc(engine.as_ref())?);

    let crc_v2 = read_crc_json(&table_path, 2);
    assert_eq!(crc_v2["tableSizeBytes"], 3500);
    assert_eq!(crc_v2["numFiles"], 3);

    // v3: Add 3 files (sizes 100, 200, 300), write CRC
    let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v3 = snapshot_v2
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files_v3 = vec![
        ("file4.parquet", 100_i64, 1000003_i64, 10_i64),
        ("file5.parquet", 200, 1000004, 20),
        ("file6.parquet", 300, 1000005, 30),
    ];
    let metadata_v3 = create_add_files_metadata(txn_v3.add_files_schema(), files_v3)?;
    txn_v3.add_files(metadata_v3);
    let committed_v3 = assert_committed(txn_v3.commit(engine.as_ref())?);
    assert_eq!(committed_v3.commit_version(), 3);
    assert!(committed_v3.write_crc(engine.as_ref())?);

    let crc_v3 = read_crc_json(&table_path, 3);
    assert_eq!(crc_v3["tableSizeBytes"], 4100);
    assert_eq!(crc_v3["numFiles"], 6);

    // All four CRC files should exist
    for v in 0..=3 {
        assert!(crc_file_exists(&table_path, v), "CRC should exist at v{v}");
    }

    Ok(())
}

// -- Test 3: CRC with user domain metadata additions and removals across versions -----------

#[tokio::test]
async fn test_crc_domain_metadata_lifecycle() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // v0: Create table with domainMetadata feature + initial domain
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let committed_v0 = assert_committed(
        txn.with_domain_metadata("domain.a".to_string(), r#"{"key":"val_a"}"#.to_string())
            .commit(engine.as_ref())?,
    );
    assert!(committed_v0.write_crc(engine.as_ref())?);

    let crc_v0 = read_crc_json(&table_path, 0);
    let domains_v0: Vec<_> = crc_v0["domainMetadata"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["domain"].as_str().unwrap().to_string())
        .collect();
    assert!(domains_v0.contains(&"domain.a".to_string()));

    // v1: Add another domain
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let committed_v1 = assert_committed(
        txn_v1
            .with_domain_metadata("domain.b".to_string(), r#"{"key":"val_b"}"#.to_string())
            .commit(engine.as_ref())?,
    );
    assert!(committed_v1.write_crc(engine.as_ref())?);

    let crc_v1 = read_crc_json(&table_path, 1);
    let domains_v1: Vec<_> = crc_v1["domainMetadata"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["domain"].as_str().unwrap().to_string())
        .collect();
    assert!(
        domains_v1.contains(&"domain.a".to_string()),
        "domain.a should persist"
    );
    assert!(
        domains_v1.contains(&"domain.b".to_string()),
        "domain.b should be added"
    );

    // v2: Remove domain.a
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let txn_v2 = snapshot_v1
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let committed_v2 = assert_committed(
        txn_v2
            .with_domain_metadata_removed("domain.a".to_string())
            .commit(engine.as_ref())?,
    );
    assert!(committed_v2.write_crc(engine.as_ref())?);

    let crc_v2 = read_crc_json(&table_path, 2);
    let domains_v2: Vec<_> = crc_v2["domainMetadata"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["domain"].as_str().unwrap().to_string())
        .collect();
    assert!(
        !domains_v2.contains(&"domain.a".to_string()),
        "domain.a should be removed"
    );
    assert!(
        domains_v2.contains(&"domain.b".to_string()),
        "domain.b should persist"
    );

    // v3: Update domain.b with new config
    let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let txn_v3 = snapshot_v2
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let new_config = r#"{"key":"val_b_updated"}"#;
    let committed_v3 = assert_committed(
        txn_v3
            .with_domain_metadata("domain.b".to_string(), new_config.to_string())
            .commit(engine.as_ref())?,
    );
    assert!(committed_v3.write_crc(engine.as_ref())?);

    let crc_v3 = read_crc_json(&table_path, 3);
    let domain_b = crc_v3["domainMetadata"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["domain"].as_str() == Some("domain.b"))
        .expect("domain.b should still be in CRC");
    assert_eq!(
        domain_b["configuration"].as_str(),
        Some(new_config),
        "domain.b config should be updated"
    );

    // Also verify via the snapshot read path that domain metadata is consistent
    let snapshot_v3 = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let dm_b = snapshot_v3.get_domain_metadata("domain.b", engine.as_ref())?;
    assert_eq!(dm_b, Some(new_config.to_string()));
    let dm_a = snapshot_v3.get_domain_metadata("domain.a", engine.as_ref())?;
    assert!(dm_a.is_none(), "domain.a should have been removed");

    Ok(())
}

// -- Test 4: CRC protocol and metadata consistency across versions --------------------------

#[tokio::test]
async fn test_crc_protocol_metadata_consistency() -> TestResult {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table and write CRC
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed_v0 = assert_committed(txn.commit(engine.as_ref())?);
    assert!(committed_v0.write_crc(engine.as_ref())?);

    // Verify CRC P&M matches snapshot P&M at v0
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let crc_v0 = read_crc_json(&table_path, 0);

    assert_eq!(
        crc_v0["protocol"]["minReaderVersion"],
        snapshot_v0
            .table_configuration()
            .protocol()
            .min_reader_version() as u64
    );
    assert_eq!(
        crc_v0["protocol"]["minWriterVersion"],
        snapshot_v0
            .table_configuration()
            .protocol()
            .min_writer_version() as u64
    );
    assert_eq!(
        crc_v0["metadata"]["id"].as_str().unwrap(),
        snapshot_v0.table_configuration().metadata().id()
    );

    // Write another version, verify P&M is carried forward unchanged
    let mut txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files = vec![("file.parquet", 1024_i64, 1000000_i64, 10_i64)];
    let metadata = create_add_files_metadata(txn_v1.add_files_schema(), files)?;
    txn_v1.add_files(metadata);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);
    assert!(committed_v1.write_crc(engine.as_ref())?);

    let crc_v1 = read_crc_json(&table_path, 1);

    // P&M should be identical between v0 and v1 (no schema/protocol changes)
    assert_eq!(crc_v0["protocol"], crc_v1["protocol"]);
    assert_eq!(crc_v0["metadata"]["id"], crc_v1["metadata"]["id"]);
    assert_eq!(
        crc_v0["metadata"]["schemaString"],
        crc_v1["metadata"]["schemaString"]
    );

    Ok(())
}

// -- Test 5: CRC without previous CRC (SIMPLE path unavailable) ----------------------------

#[tokio::test]
async fn test_crc_simple_path_unavailable_without_previous_crc() -> TestResult {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // v0: Create table WITHOUT writing CRC
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let _committed_v0 = assert_committed(txn.commit(engine.as_ref())?);
    // Deliberately skip write_crc

    // v1: Append files
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files = vec![("file1.parquet", 1024_i64, 1000000_i64, 10_i64)];
    let metadata = create_add_files_metadata(txn_v1.add_files_schema(), files)?;
    txn_v1.add_files(metadata);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);

    // write_crc should return false: no previous CRC at v0 -> SIMPLE path unavailable
    let crc_written = committed_v1.write_crc(engine.as_ref())?;
    assert!(
        !crc_written,
        "CRC should not be writable without previous CRC"
    );
    assert!(!crc_file_exists(&table_path, 1));

    Ok(())
}

// -- Test 6: Idempotent CRC write ----------------------------------------------------------

#[tokio::test]
async fn test_crc_write_idempotent() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed = assert_committed(txn.commit(engine.as_ref())?);

    // Write CRC twice - should not error
    assert!(committed.write_crc(engine.as_ref())?);
    assert!(committed.write_crc(engine.as_ref())?);

    // Verify file is still valid
    let crc = read_crc_json(&table_path, 0);
    assert_eq!(crc["numFiles"], 0);
    assert_eq!(crc["tableSizeBytes"], 0);

    Ok(())
}

// -- Test 7: CRC with ICT (In-Commit Timestamps) ------------------------------------------

#[tokio::test]
async fn test_crc_with_in_commit_timestamps() -> TestResult {
    use tempfile::tempdir;
    use test_utils::engine_store_setup;
    use url::Url;

    let schema = simple_schema()?;

    // Create table with ICT enabled using the low-level test helper (the high-level
    // create_table API doesn't support setting delta.enableInCommitTimestamps directly).
    // Use local filesystem so we can read the CRC file back as JSON.
    let tmp_dir = tempdir()?;
    let tmp_dir_url = Url::from_directory_path(tmp_dir.path()).unwrap();
    let (_store, engine, table_url) = engine_store_setup("test_crc_ict", Some(&tmp_dir_url));
    let engine = Arc::new(engine);
    let table_url = test_utils::create_table(
        _store.clone(),
        table_url,
        schema.clone(),
        &[],
        true,                      // use protocol 3.7
        vec![],                    // no reader features
        vec!["inCommitTimestamp"], // enable ICT
    )
    .await?;

    // Derive the filesystem path from the table URL for CRC file reading
    let table_path = table_url
        .to_file_path()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // The low-level create_table helper doesn't produce a CRC file. Seed a CRC at v0
    // so the SIMPLE path is available for subsequent commits. Extract P&M from the v0
    // commit JSON and write a minimal CRC file.
    let v0_crc = seed_crc_from_commit(&table_path, 0);
    let crc_path = format!("{}/_delta_log/{:020}.crc", table_path, 0);
    std::fs::write(&crc_path, serde_json::to_string(&v0_crc)?)?;

    // v1: Add files to the ICT-enabled table, write CRC via SIMPLE path (uses CRC at v0)
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(snapshot_v0.version(), 0);

    let mut txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files = vec![("file1.parquet", 512_i64, 1000000_i64, 10_i64)];
    let metadata = create_add_files_metadata(txn_v1.add_files_schema(), files)?;
    txn_v1.add_files(metadata);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);
    assert_eq!(committed_v1.commit_version(), 1);
    assert!(committed_v1.write_crc(engine.as_ref())?);

    // Verify CRC has ICT
    let crc_v1 = read_crc_json(&table_path, 1);
    let ict_v1 = crc_v1["inCommitTimestampOpt"]
        .as_i64()
        .expect("CRC v1 should have inCommitTimestampOpt when ICT is enabled");
    assert!(ict_v1 > 0, "ICT should be a positive timestamp");
    assert_eq!(crc_v1["tableSizeBytes"], 512);
    assert_eq!(crc_v1["numFiles"], 1);

    // v2: Add more files, verify CRC chain works with ICT
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v2 = snapshot_v1
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files_v2 = vec![("file2.parquet", 1024_i64, 1000001_i64, 20_i64)];
    let metadata_v2 = create_add_files_metadata(txn_v2.add_files_schema(), files_v2)?;
    txn_v2.add_files(metadata_v2);
    let committed_v2 = assert_committed(txn_v2.commit(engine.as_ref())?);
    assert_eq!(committed_v2.commit_version(), 2);
    assert!(committed_v2.write_crc(engine.as_ref())?);

    let crc_v2 = read_crc_json(&table_path, 2);
    let ict_v2 = crc_v2["inCommitTimestampOpt"]
        .as_i64()
        .expect("CRC v2 should have inCommitTimestampOpt");
    assert!(
        ict_v2 >= ict_v1,
        "ICT at v2 ({ict_v2}) should be >= ICT at v1 ({ict_v1})"
    );
    assert_eq!(crc_v2["tableSizeBytes"], 512 + 1024);
    assert_eq!(crc_v2["numFiles"], 2);

    Ok(())
}

// -- Test 8: CRC chain broken and resumed --------------------------------------------------

#[tokio::test]
async fn test_crc_chain_broken_and_resumed() -> TestResult {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    // v0: Create table, write CRC
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed_v0 = assert_committed(txn.commit(engine.as_ref())?);
    assert!(committed_v0.write_crc(engine.as_ref())?);

    let crc_v0 = read_crc_json(&table_path, 0);
    assert_eq!(crc_v0["numFiles"], 0);

    // v1: Add files, write CRC
    let snapshot_v0 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v1 = snapshot_v0
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());
    let files_v1 = vec![("a.parquet", 1000_i64, 1000000_i64, 10_i64)];
    let metadata_v1 = create_add_files_metadata(txn_v1.add_files_schema(), files_v1)?;
    txn_v1.add_files(metadata_v1);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);
    assert!(committed_v1.write_crc(engine.as_ref())?);

    // v2: Add files, deliberately skip CRC (break the chain)
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v2 = snapshot_v1
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());
    let files_v2 = vec![("b.parquet", 2000_i64, 1000001_i64, 20_i64)];
    let metadata_v2 = create_add_files_metadata(txn_v2.add_files_schema(), files_v2)?;
    txn_v2.add_files(metadata_v2);
    let _committed_v2 = assert_committed(txn_v2.commit(engine.as_ref())?);
    // Skip write_crc for v2

    // v3: Try to write CRC - should fail because no CRC at v2
    let snapshot_v2 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn_v3 = snapshot_v2
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());
    let files_v3 = vec![("c.parquet", 3000_i64, 1000002_i64, 30_i64)];
    let metadata_v3 = create_add_files_metadata(txn_v3.add_files_schema(), files_v3)?;
    txn_v3.add_files(metadata_v3);
    let committed_v3 = assert_committed(txn_v3.commit(engine.as_ref())?);

    let crc_written_v3 = committed_v3.write_crc(engine.as_ref())?;
    assert!(
        !crc_written_v3,
        "CRC at v3 should not be writable because CRC chain was broken at v2"
    );
    assert!(!crc_file_exists(&table_path, 2));
    assert!(!crc_file_exists(&table_path, 3));

    // The CRC at v0 and v1 should still exist
    assert!(crc_file_exists(&table_path, 0));
    assert!(crc_file_exists(&table_path, 1));

    Ok(())
}

// -- Test 9: CRC with clustering domain metadata preserved through appends -----------------

#[tokio::test]
async fn test_crc_clustering_domain_preserved_through_appends() -> TestResult {
    use delta_kernel::expressions::ColumnName;
    use delta_kernel::transaction::data_layout::DataLayout;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("value", DataType::STRING, true),
    ])?);

    // v0: Create clustered table, write CRC
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed_v0 = assert_committed(txn.commit(engine.as_ref())?);
    assert!(committed_v0.write_crc(engine.as_ref())?);

    // Get initial clustering domain count
    let crc_v0 = read_crc_json(&table_path, 0);
    let domains_v0 = crc_v0["domainMetadata"].as_array().unwrap();
    let clustering_domains_v0: Vec<_> = domains_v0
        .iter()
        .filter(|d| {
            d["domain"]
                .as_str()
                .is_some_and(|s| s.starts_with("delta.clustering"))
        })
        .collect();
    assert!(
        !clustering_domains_v0.is_empty(),
        "Should have clustering domain at v0"
    );

    // v1, v2, v3: Append files, write CRC at each
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    for version in 1..=3_i64 {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let mut txn = snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
            .with_operation("WRITE".to_string());

        let file = format!("file_{version}.parquet");
        let files = vec![(file.as_str(), version * 100, 1000000 + version, 10_i64)];
        let metadata = create_add_files_metadata(txn.add_files_schema(), files)?;
        txn.add_files(metadata);
        let committed = assert_committed(txn.commit(engine.as_ref())?);
        assert!(committed.write_crc(engine.as_ref())?);
    }

    // Verify clustering domain is preserved in the last CRC
    let crc_v3 = read_crc_json(&table_path, 3);
    let domains_v3 = crc_v3["domainMetadata"].as_array().unwrap();
    let clustering_domains_v3: Vec<_> = domains_v3
        .iter()
        .filter(|d| {
            d["domain"]
                .as_str()
                .is_some_and(|s| s.starts_with("delta.clustering"))
        })
        .collect();
    assert_eq!(
        clustering_domains_v0.len(),
        clustering_domains_v3.len(),
        "Clustering domain count should be preserved across appends"
    );

    // Verify that the file stats accumulated correctly: 100 + 200 + 300 = 600
    assert_eq!(crc_v3["numFiles"], 3);
    assert_eq!(crc_v3["tableSizeBytes"], 600);

    // Verify clustering is still accessible through the snapshot read path
    let final_snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let clustering_cols = final_snapshot.get_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_cols, Some(vec![ColumnName::new(["id"])]));

    Ok(())
}

// -- Test 10: Post-commit snapshot has correct properties ----------------------------------

#[tokio::test]
async fn test_post_commit_snapshot_properties() -> TestResult {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Create table
    let txn = create_table(&table_path, schema, "CRC-E2E-Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let committed_v0 = assert_committed(txn.commit(engine.as_ref())?);

    // Post-commit snapshot should have post_commit_stats
    let snapshot_v0 = committed_v0
        .post_commit_snapshot()
        .expect("Should have post-commit snapshot");
    assert_eq!(snapshot_v0.version(), 0);
    assert!(snapshot_v0.has_post_commit_stats());

    // Write CRC and verify
    assert!(committed_v0.write_crc(engine.as_ref())?);

    // v1: Append and verify post-commit snapshot
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn_v1 = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?
        .with_operation("WRITE".to_string());

    let files = vec![("file.parquet", 4096_i64, 1000000_i64, 100_i64)];
    let metadata = create_add_files_metadata(txn_v1.add_files_schema(), files)?;
    txn_v1.add_files(metadata);
    let committed_v1 = assert_committed(txn_v1.commit(engine.as_ref())?);

    let snapshot_v1 = committed_v1
        .post_commit_snapshot()
        .expect("Should have post-commit snapshot");
    assert_eq!(snapshot_v1.version(), 1);
    assert!(snapshot_v1.has_post_commit_stats());

    // Write CRC via both APIs
    assert!(committed_v1.write_crc(engine.as_ref())?);
    assert!(snapshot_v1.write_crc(engine.as_ref())?); // idempotent

    let crc = read_crc_json(&table_path, 1);
    assert_eq!(crc["numFiles"], 1);
    assert_eq!(crc["tableSizeBytes"], 4096);

    Ok(())
}
