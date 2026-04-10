//! Row tracking integration tests for the CreateTable API.
//!
//! Tests that creating a table with `delta.enableRowTracking=true` automatically adds the
//! `rowTracking` and `domainMetadata` features to the protocol and writes the initial row
//! tracking domain metadata with `rowIdHighWaterMark = -1`.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, StringArray};
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::DeltaResult;
use itertools::Itertools;
use rstest::rstest;
use serde_json::Deserializer;
use test_utils::test_table_setup;

/// Reads the commit JSON at the given version from the local filesystem and returns parsed
/// JSON actions.
fn read_commit_actions(table_path: &str, version: u64) -> Vec<serde_json::Value> {
    let commit_path = format!("{}/_delta_log/{:020}.json", table_path, version);
    let data = std::fs::read(&commit_path).unwrap();
    Deserializer::from_slice(&data)
        .into_iter::<serde_json::Value>()
        .try_collect()
        .unwrap()
}

/// Extracts domain metadata actions from parsed commit JSON.
fn find_domain_metadata_actions(actions: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    actions
        .iter()
        .filter_map(|a| a.get("domainMetadata"))
        .collect()
}

/// Asserts protocol features are correct for row tracking on a snapshot.
fn assert_row_tracking_protocol(snapshot: &Snapshot) {
    let protocol = snapshot.table_configuration().protocol();

    assert!(protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION);
    assert!(protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION);

    // RowTracking is writer-only
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::RowTracking)),
        "rowTracking should be in writer features"
    );
    assert!(
        !protocol
            .reader_features()
            .is_some_and(|f| f.contains(&TableFeature::RowTracking)),
        "rowTracking should NOT be in reader features"
    );

    // DomainMetadata dependency is writer-only
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::DomainMetadata)),
        "domainMetadata should be in writer features"
    );
}

/// Verifies row tracking across both activation paths (enablement property and feature signal)
/// and both table shapes (empty CREATE TABLE and CTAS with a single file of 5 rows).
///
/// Key behavioral differences by case:
/// - Activation path: `delta.enableRowTracking=true` sets materialized column name properties;
///   feature-signal-only does not.
/// - Table shape: empty tables get `rowIdHighWaterMark = -1` with no add actions; CTAS
///   assigns `baseRowId = 0` and `defaultRowCommitVersion = 0` and sets `rowIdHighWaterMark = 4`.
#[rstest]
#[tokio::test]
async fn test_create_table_with_row_tracking(
    #[values(
        ("delta.enableRowTracking", "true"),
        ("delta.feature.rowTracking", "supported")
    )]
    activation: (&str, &str),
    #[values(false, true)] with_data: bool,
) -> DeltaResult<()> {
    let (key, value) = activation;
    let is_property_enabled = key == "delta.enableRowTracking";

    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = super::simple_schema()?;

    let mut txn = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([(key, value)])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    if with_data {
        // Write one parquet file with 5 rows
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

        let write_context = Arc::new(txn.get_write_context());
        let add_files = engine
            .write_parquet(
                &ArrowEngineData::new(batch),
                write_context.as_ref(),
                HashMap::new(),
            )
            .await?;
        txn.add_files(add_files);
    }

    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    let snapshot = committed
        .post_commit_snapshot()
        .expect("should have snapshot");
    assert_row_tracking_protocol(snapshot);

    // Verify domain metadata persists to disk for empty tables. CTAS is not checked here
    // because the high water mark is written via add-file processing, not the initial
    // domain metadata path.
    if !with_data {
        let disk_snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
        assert_row_tracking_protocol(&disk_snapshot);
    }

    let actions = read_commit_actions(&table_path, 0);

    // Verify row tracking domain metadata
    let dm_actions = find_domain_metadata_actions(&actions);
    let row_tracking_dm: Vec<_> = dm_actions
        .iter()
        .filter(|dm| dm["domain"] == "delta.rowTracking")
        .collect();
    assert_eq!(
        row_tracking_dm.len(),
        1,
        "Expected exactly one row tracking domain metadata"
    );
    let dm_config: serde_json::Value =
        serde_json::from_str(row_tracking_dm[0]["configuration"].as_str().unwrap()).unwrap();
    let expected_high_water_mark: i64 = if with_data { 4 } else { -1 };
    assert_eq!(dm_config["rowIdHighWaterMark"], expected_high_water_mark);

    // Verify add actions
    let add_actions: Vec<_> = actions.iter().filter_map(|a| a.get("add")).collect();
    if with_data {
        assert_eq!(add_actions.len(), 1, "Expected one add action");
        assert_eq!(
            add_actions[0]["baseRowId"].as_i64(),
            Some(0),
            "baseRowId should be 0"
        );
        assert_eq!(
            add_actions[0]["defaultRowCommitVersion"].as_i64(),
            Some(0),
            "defaultRowCommitVersion should be 0"
        );
    } else {
        assert!(
            add_actions.is_empty(),
            "Expected no add actions for empty create"
        );
    }

    // Verify materialized column name properties.
    // Only set when delta.enableRowTracking=true; feature-signal-only tables do not get them.
    let metadata_actions: Vec<_> = actions.iter().filter_map(|a| a.get("metaData")).collect();
    assert_eq!(metadata_actions.len(), 1);
    let meta_config = metadata_actions[0].get("configuration").unwrap();

    if is_property_enabled {
        let row_id_col = meta_config
            .get("delta.rowTracking.materializedRowIdColumnName")
            .and_then(|v| v.as_str())
            .expect("materializedRowIdColumnName should be set");
        assert!(row_id_col.starts_with("_row-id-col-"), "got {row_id_col}");

        let commit_version_col = meta_config
            .get("delta.rowTracking.materializedRowCommitVersionColumnName")
            .and_then(|v| v.as_str())
            .expect("materializedRowCommitVersionColumnName should be set");
        assert!(
            commit_version_col.starts_with("_row-commit-version-col-"),
            "got {commit_version_col}"
        );
    } else {
        assert!(
            meta_config
                .get("delta.rowTracking.materializedRowIdColumnName")
                .is_none(),
            "materializedRowIdColumnName should NOT be set for feature-signal-only tables"
        );
        assert!(
            meta_config
                .get("delta.rowTracking.materializedRowCommitVersionColumnName")
                .is_none(),
            "materializedRowCommitVersionColumnName should NOT be set for feature-signal-only tables"
        );
        assert!(
            meta_config.get("delta.enableRowTracking").is_none(),
            "delta.enableRowTracking should NOT be set for feature-signal-only tables"
        );
    }

    Ok(())
}

/// Verifies that CTAS with multiple files assigns non-overlapping baseRowId ranges and
/// computes the correct cumulative high water mark.
#[tokio::test]
async fn test_create_table_with_multiple_files_and_row_tracking() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = super::simple_schema()?;
    let mut txn = create_table(&table_path, schema.clone(), "Test/1.0")
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let arrow_schema: Arc<delta_kernel::arrow::datatypes::Schema> =
        Arc::new(schema.as_ref().try_into_arrow()?);

    // Write two separate parquet files: 3 rows and 5 rows
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    let batch2 = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec!["d", "e", "f", "g", "h"])),
        ],
    )
    .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;

    let write_context = Arc::new(txn.get_write_context());
    let adds1 = engine
        .write_parquet(
            &ArrowEngineData::new(batch1),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;
    let adds2 = engine
        .write_parquet(
            &ArrowEngineData::new(batch2),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;

    txn.add_files(adds1);
    txn.add_files(adds2);

    let committed = txn.commit(engine.as_ref())?.unwrap_committed();
    assert_eq!(committed.commit_version(), 0);

    // Verify commit file contents
    let actions = read_commit_actions(&table_path, 0);
    let mut add_actions: Vec<_> = actions.iter().filter_map(|a| a.get("add")).collect();
    assert_eq!(add_actions.len(), 2, "Expected two add actions");

    // Sort by baseRowId to get deterministic order
    add_actions.sort_by_key(|a| a["baseRowId"].as_i64().unwrap());

    // First file (3 rows): baseRowId = 0
    assert_eq!(add_actions[0]["baseRowId"].as_i64(), Some(0));
    assert_eq!(add_actions[0]["defaultRowCommitVersion"].as_i64(), Some(0));

    // Second file (5 rows): baseRowId = 3 (first file had 3 rows)
    assert_eq!(add_actions[1]["baseRowId"].as_i64(), Some(3));
    assert_eq!(add_actions[1]["defaultRowCommitVersion"].as_i64(), Some(0));

    // HWM should be 7 (IDs 0-2 from file 1, IDs 3-7 from file 2)
    let dm_actions = find_domain_metadata_actions(&actions);
    let row_tracking_dm: Vec<_> = dm_actions
        .iter()
        .filter(|dm| dm["domain"] == "delta.rowTracking")
        .collect();
    assert_eq!(row_tracking_dm.len(), 1);
    let config: serde_json::Value =
        serde_json::from_str(row_tracking_dm[0]["configuration"].as_str().unwrap()).unwrap();
    assert_eq!(
        config["rowIdHighWaterMark"], 7,
        "HWM should be 7 for 8 total rows (3 + 5) starting from -1"
    );

    Ok(())
}

/// Verifies that row tracking and clustering can be enabled together. Both features require
/// DomainMetadata, which should appear exactly once in the protocol. Both domain metadata
/// entries (delta.rowTracking and delta.clustering) should be present in the commit.
#[test]
fn test_create_table_with_row_tracking_and_clustering() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let committed = create_table(&table_path, super::simple_schema()?, "Test/1.0")
        .with_table_properties([("delta.enableRowTracking", "true")])
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let snapshot = committed
        .post_commit_snapshot()
        .expect("should have snapshot");
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("should have writer features");

    assert!(writer_features.contains(&TableFeature::RowTracking));
    assert!(writer_features.contains(&TableFeature::ClusteredTable));
    // DomainMetadata should appear exactly once despite two features requiring it
    let dm_count = writer_features
        .iter()
        .filter(|f| **f == TableFeature::DomainMetadata)
        .count();
    assert_eq!(dm_count, 1, "DomainMetadata should not be duplicated");

    // Verify both domain metadata entries are present in the commit
    let actions = read_commit_actions(&table_path, 0);
    let dm_actions = find_domain_metadata_actions(&actions);
    assert!(
        dm_actions
            .iter()
            .any(|dm| dm["domain"] == "delta.rowTracking"),
        "row tracking domain metadata should be present"
    );
    assert!(
        dm_actions
            .iter()
            .any(|dm| dm["domain"] == "delta.clustering"),
        "clustering domain metadata should be present"
    );

    Ok(())
}
