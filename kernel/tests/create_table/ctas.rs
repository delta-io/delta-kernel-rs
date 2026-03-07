//! CTAS (Create Table As Select) integration tests.
//!
//! These tests exercise a CTAS-style flow: create a source table with certain
//! features, write seed data, scan it, create a target table with (possibly
//! different) features, write the scanned data, then verify the target.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int64Array, StringArray, StructArray};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::ColumnName;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    get_any_level_column_physical_name, ColumnMappingMode, TableFeature,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{Engine, FileMeta};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use test_utils::{
    assert_schema_has_field, nested_batches, nested_schema, read_add_infos, test_table_setup,
    write_batch_to_table,
};

const VERIFIED_PATHS: &[&[&str]] = &[&["row_number"], &["address", "street"]];

// ---------------------------------------------------------------------------
// Unified column naming verification
// ---------------------------------------------------------------------------

/// Validates that column names are consistent (logical or physical) across all
/// table metadata surfaces: schema annotations, stats, clustering domain
/// metadata, and Parquet file footers.
async fn verify_column_names_in_metadata(
    snapshot: &Snapshot,
    engine: &impl Engine,
    store: &dyn ObjectStore,
    table_url: &Url,
    cm_mode: ColumnMappingMode,
    clustered: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    super::column_mapping::assert_column_mapping_config(snapshot, cm_mode);
    verify_column_names_in_stats(snapshot, engine, cm_mode)?;
    if clustered {
        verify_column_names_in_clustering_metadata(snapshot, engine, cm_mode)?;
    }
    verify_column_names_in_parquet_footer(snapshot, engine, store, table_url, cm_mode).await?;
    Ok(())
}

/// Asserts that minValues keys in add-action stats use the expected column
/// names (physical when column mapping is enabled, logical otherwise).
fn verify_column_names_in_stats(
    snapshot: &Snapshot,
    engine: &impl Engine,
    cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = snapshot.schema();
    let add_actions = read_add_infos(snapshot, engine)?;
    let stats = add_actions
        .iter()
        .filter_map(|a| a.stats.as_ref())
        .find(|s| s.get("minValues").is_some());

    if let Some(stats) = stats {
        let min_values = &stats["minValues"];
        for logical_path in VERIFIED_PATHS {
            let col = ColumnName::new(logical_path.iter().copied());
            let expected =
                get_any_level_column_physical_name(schema.as_ref(), &col, cm_mode)?.into_inner();
            let mut current = min_values;
            for (i, field) in expected.iter().enumerate() {
                assert!(
                    current.get(field).is_some(),
                    "stats minValues missing key '{field}' for {logical_path:?}"
                );
                if i < expected.len() - 1 {
                    current = &current[field];
                }
            }
        }
    }

    Ok(())
}

/// Asserts that column paths stored in clustering domain metadata use the
/// expected names (physical when column mapping is enabled, logical otherwise).
fn verify_column_names_in_clustering_metadata(
    snapshot: &Snapshot,
    engine: &impl Engine,
    cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = snapshot.schema();
    let clustering_columns = snapshot
        .get_clustering_columns_physical(engine)?
        .expect("Clustering columns should be present");

    assert_eq!(
        clustering_columns.len(),
        1,
        "Expected exactly one clustering column"
    );
    let stored_path = clustering_columns[0].path();
    let col = ColumnName::new(["row_number"]);
    let expected = get_any_level_column_physical_name(schema.as_ref(), &col, cm_mode)?.into_inner();

    assert_eq!(
        stored_path, &expected,
        "Clustering column naming mismatch: stored={stored_path:?}, expected={expected:?}"
    );

    if cm_mode != ColumnMappingMode::None {
        for field in stored_path {
            assert!(
                field.as_str().starts_with("col-"),
                "Clustering path field '{field}' should be a physical name"
            );
        }
    } else {
        assert_eq!(
            stored_path,
            &["row_number"],
            "Without column mapping, clustering path should use logical name"
        );
    }

    Ok(())
}

/// Asserts that Parquet file footer field names match the expected column
/// names (physical when column mapping is enabled, logical otherwise).
async fn verify_column_names_in_parquet_footer(
    snapshot: &Snapshot,
    engine: &impl Engine,
    store: &dyn ObjectStore,
    table_url: &Url,
    cm_mode: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = snapshot.schema();
    let add_actions = read_add_infos(snapshot, engine)?;
    let first_add = add_actions
        .first()
        .expect("should have at least one add file");

    let parquet_url = table_url.join(&first_add.path)?;
    let obj_meta = store
        .head(&Path::from_url_path(parquet_url.path())?)
        .await?;
    let file_meta = FileMeta::new(parquet_url, 0, obj_meta.size as u64);
    let footer = engine.parquet_handler().read_parquet_footer(&file_meta)?;

    for logical_path in VERIFIED_PATHS {
        let col = ColumnName::new(logical_path.iter().copied());
        let expected =
            get_any_level_column_physical_name(schema.as_ref(), &col, cm_mode)?.into_inner();
        assert_schema_has_field(&footer.schema, &expected);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Core CTAS test flow
// ---------------------------------------------------------------------------

/// Returns the table property value for the given column mapping mode, or
/// `None` for `ColumnMappingMode::None` (no property needed).
fn cm_mode_property(mode: ColumnMappingMode) -> Option<&'static str> {
    match mode {
        ColumnMappingMode::None => None,
        ColumnMappingMode::Name => Some("name"),
        ColumnMappingMode::Id => Some("id"),
    }
}

/// Core CTAS test logic:
/// 1. Set up engine and source table with the requested features
/// 2. Write seed data to the source table
/// 3. Scan all data from the source table
/// 4. Create target table and write scanned data in a single CTAS transaction
/// 5. Verify target version, feature flags, and column naming consistency
/// 6. Verify data integrity: scan target and check row count, row_number
///    values, and nested address.street values all match the source
async fn run_ctas_test(
    src_cm: ColumnMappingMode,
    src_clustered: bool,
    tgt_cm: ColumnMappingMode,
    tgt_clustered: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Set up engine and source table with the requested features
    let schema = nested_schema()?;

    let (_src_tmp, src_table_path, _) = test_table_setup()?;
    let src_url = Url::from_directory_path(&src_table_path).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
    let engine = Arc::new(
        DefaultEngineBuilder::new(store.clone())
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    );

    let mut src_snapshot = {
        let mut builder = create_table(&src_table_path, schema.clone(), "ctas-test");
        if let Some(mode_str) = cm_mode_property(src_cm) {
            builder = builder.with_table_properties([("delta.columnMapping.mode", mode_str)]);
        }
        if src_clustered {
            builder = builder.with_data_layout(DataLayout::clustered(["row_number"]));
        }
        let result = builder
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
            .commit(engine.as_ref())?;
        match result {
            CommitResult::CommittedTransaction(c) => c
                .post_commit_snapshot()
                .expect("should have post_commit_snapshot")
                .clone(),
            _ => panic!("Source create should succeed"),
        }
    };

    // 2. Write seed data to the source table
    for batch in nested_batches()? {
        src_snapshot =
            write_batch_to_table(&src_snapshot, engine.as_ref(), batch, HashMap::new()).await?;
    }

    // 3. Scan all data from the source table
    let src_snapshot_for_scan = Snapshot::builder_for(src_url.clone()).build(engine.as_ref())?;
    let src_scan = src_snapshot_for_scan.scan_builder().build()?;
    let src_batches = test_utils::read_scan(&src_scan, engine.clone())?;
    let src_arrow_schema = src_batches[0].schema();
    let source_data =
        delta_kernel::arrow::compute::concat_batches(&src_arrow_schema, &src_batches)?;
    let source_row_count = source_data.num_rows();
    assert_eq!(source_row_count, 6, "Source should have 6 rows");

    // 4. Create target table and write scanned data in a single CTAS transaction
    let (_tgt_tmp, tgt_table_path, _) = test_table_setup()?;
    let tgt_url = Url::from_directory_path(&tgt_table_path).unwrap();

    let mut tgt_builder = create_table(&tgt_table_path, schema.clone(), "ctas-test");
    if let Some(mode_str) = cm_mode_property(tgt_cm) {
        tgt_builder = tgt_builder.with_table_properties([("delta.columnMapping.mode", mode_str)]);
    }
    if tgt_clustered {
        tgt_builder = tgt_builder.with_data_layout(DataLayout::clustered(["row_number"]));
    }
    let mut tgt_txn = tgt_builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let write_context = Arc::new(tgt_txn.get_write_context());
    let add_meta = engine
        .write_parquet(
            &ArrowEngineData::new(source_data),
            write_context.as_ref(),
            HashMap::new(),
        )
        .await?;
    tgt_txn.add_files(add_meta);

    let commit_result = tgt_txn.commit(engine.as_ref())?;
    let tgt_snapshot = match commit_result {
        CommitResult::CommittedTransaction(c) => c
            .post_commit_snapshot()
            .expect("should have post_commit_snapshot")
            .clone(),
        _ => panic!("CTAS commit should succeed"),
    };

    // 5. Verify target version, feature flags, and column naming consistency
    assert_eq!(tgt_snapshot.version(), 0, "CTAS should produce version-0");

    if tgt_clustered {
        let tc = tgt_snapshot.table_configuration();
        assert!(
            tc.is_feature_supported(&TableFeature::ClusteredTable),
            "Clustered table feature should be supported"
        );
        assert!(
            tc.is_feature_supported(&TableFeature::DomainMetadata),
            "Domain metadata feature should be supported for clustered tables"
        );
    }

    verify_column_names_in_metadata(
        &tgt_snapshot,
        engine.as_ref(),
        store.as_ref(),
        &tgt_url,
        tgt_cm,
        tgt_clustered,
    )
    .await?;

    // 6. Verify data integrity: scan target and check row count, row_number
    //    values, and nested address.street values all match the source
    let tgt_snapshot_for_scan = Snapshot::builder_for(tgt_url.clone()).build(engine.as_ref())?;
    let tgt_scan = tgt_snapshot_for_scan.scan_builder().build()?;
    let tgt_batches = test_utils::read_scan(&tgt_scan, engine.clone())?;
    let tgt_arrow_schema = tgt_batches[0].schema();
    let tgt_combined =
        delta_kernel::arrow::compute::concat_batches(&tgt_arrow_schema, &tgt_batches)?;
    assert_eq!(
        tgt_combined.num_rows(),
        source_row_count,
        "Target row count should match source"
    );

    let row_numbers = tgt_combined
        .column_by_name("row_number")
        .expect("should have 'row_number'")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("row_number should be Int64");
    // Scan order is non-deterministic, so sort before comparing
    let vals = {
        let mut v: Vec<i64> = (0..row_numbers.len())
            .map(|i| row_numbers.value(i))
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        vals,
        (1..=source_row_count as i64).collect::<Vec<_>>(),
        "row_number values should be 1..={source_row_count}"
    );

    let address = tgt_combined
        .column_by_name("address")
        .expect("should have 'address'")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("address should be a struct");
    let streets = address
        .column_by_name("street")
        .expect("address should have 'street'")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("street should be String");
    let street_vals = {
        let mut v: Vec<&str> = (0..streets.len()).map(|i| streets.value(i)).collect();
        v.sort();
        v
    };
    let expected: Vec<String> = (1..=source_row_count).map(|i| format!("st{i}")).collect();
    let expected_refs: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_eq!(
        street_vals, expected_refs,
        "address.street values should be st1..st{source_row_count}"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Test functions
// ---------------------------------------------------------------------------

/// Verifies CTAS data roundtrip for all 9 source/target column-mapping mode
/// combinations (None/Name/Id x None/Name/Id, no clustering). Ensures column
/// naming is consistent across metadata and Parquet files regardless of mode.
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_ctas_column_mapping_combinations(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    src_cm: ColumnMappingMode,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    tgt_cm: ColumnMappingMode,
) -> Result<(), Box<dyn std::error::Error>> {
    run_ctas_test(src_cm, false, tgt_cm, false).await
}

/// Verifies CTAS data roundtrip for all 27 non-trivial combinations of
/// source/target column-mapping mode (None/Name/Id) and clustering. Skips
/// the 9 cases where neither table is clustered (covered by
/// `test_ctas_column_mapping_combinations`).
#[rstest::rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_ctas_clustering_and_column_mapping_combinations(
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    src_cm: ColumnMappingMode,
    #[values(false, true)] src_clustered: bool,
    #[values(
        ColumnMappingMode::None,
        ColumnMappingMode::Name,
        ColumnMappingMode::Id
    )]
    tgt_cm: ColumnMappingMode,
    #[values(false, true)] tgt_clustered: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !src_clustered && !tgt_clustered {
        return Ok(());
    }
    run_ctas_test(src_cm, src_clustered, tgt_cm, tgt_clustered).await
}
