//! Clustering integration tests for the CreateTable API.

use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{ColumnName, column_name};
use delta_kernel::schema::{DataType, StructField, StructType, schema_ref};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;
use serde_json::{Value, json};
use test_utils::{
    assert_result_error_with_message, insert_data, read_actions_from_commit, read_scan,
    test_table_setup,
};

use super::simple_schema;
use crate::common::write_utils::{get_simple_schema, simple_id_batch};

/// Builds a schema that supports clustering at depths 1, 2, and 5:
///   { id: int, name: string, address: { city: string, zip: string },
///     l1: { l2: { l3: { l4: { value: double } } } } }
fn clustering_test_schema() -> KernelResult<Arc<StructType>> {
    Ok(schema_ref! {
        nullable "id": INTEGER,
        nullable "name": STRING,
        nullable "address": {
            nullable "city": STRING,
            nullable "zip": STRING,
        },
        nullable "l1": {
            nullable "l2": {
                nullable "l3": {
                    nullable "l4": {
                        nullable "value": DOUBLE,
                    },
                },
            },
        },
    })
}

#[rstest]
#[case::empty(vec![])]
#[case::top_level(vec![vec!["id"]])]
#[case::nested_2(vec![vec!["address", "city"]])]
#[case::mismatched_casing(vec![vec!["ID"]])]
#[case::nested_mismatched_casing(vec![vec!["ADDRESS", "CITY"]])]
#[case::mixed(vec![vec!["id"], vec!["name"], vec!["address", "city"], vec!["address", "zip"], vec!["l1", "l2", "l3", "l4", "value"]])]
#[tokio::test]
async fn test_create_clustered_table(
    #[case] col_paths: Vec<Vec<&str>>,
) -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = clustering_test_schema()?;
    let input_cols: Vec<ColumnName> = col_paths
        .iter()
        .map(|p| ColumnName::new(p.iter().copied()))
        .collect();
    // Schema fields are all lowercase; normalization converts user casing to schema casing
    let expected_cols: Vec<ColumnName> = col_paths
        .iter()
        .map(|p| ColumnName::new(p.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>()))
        .collect();

    let txn = create_table(&table_path, schema, "Test/1.0", true)
        .with_data_layout(DataLayout::Clustered {
            columns: input_cols,
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    let stats_cols = txn.stats_columns();
    for col in &expected_cols {
        assert!(
            stats_cols.contains(col),
            "Clustering column '{col}' should be in stats columns"
        );
    }

    let _ = txn.commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let clustering_columns = snapshot
        .reader()
        .get_physical_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(expected_cols));

    let table_configuration = snapshot.table_configuration();
    assert!(
        table_configuration.is_feature_supported(&TableFeature::DomainMetadata),
        "Protocol should support domainMetadata feature"
    );
    assert!(
        table_configuration.is_feature_supported(&TableFeature::ClusteredTable),
        "Protocol should support clustering feature"
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_clustering_metadata_extension_survives_create_and_append(
    #[values("none", "name", "id")] cm_mode: &str,
    #[values(false, true)] with_columns: bool,
    #[values(false, true)] with_initial_data: bool,
) -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = get_simple_schema();
    let columns = if with_columns { vec!["id"] } else { vec![] };
    let mut txn = create_table(&table_path, schema.clone(), "Test/1.0", true)
        .with_table_properties([("delta.columnMapping.mode", cm_mode)])
        .with_data_layout(DataLayout::clustered(columns))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    for version in [1, 2] {
        txn.transform_system_domain_metadata("delta.clustering", |configuration| {
            let mut configuration: Value = serde_json::from_str(configuration)?;
            configuration["connectorField"] = json!({"version": version});
            Ok(serde_json::to_string(&configuration)?)
        })?;
    }

    if with_initial_data {
        let batch = ArrowEngineData::new(simple_id_batch(&schema, vec![0]));
        let context = txn.write_state()?.write_context_builder().build()?;
        txn.add_files(engine.write_parquet(&batch, &context).await?);
    }
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let expected_columns = snapshot
        .reader()
        .get_physical_clustering_columns(engine.as_ref())?;
    assert_eq!(
        expected_columns.as_ref().unwrap().len(),
        usize::from(with_columns)
    );
    let domains = read_actions_from_commit(snapshot.table_root(), 0, "domainMetadata")?;
    assert_eq!(domains.len(), 1);
    assert_eq!(domains[0]["domain"], "delta.clustering");
    let configuration: Value = serde_json::from_str(domains[0]["configuration"].as_str().unwrap())
        .map_err(KernelError::from)?;
    assert_eq!(configuration["connectorField"], json!({"version": 2}));

    insert_data(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_committed();

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);
    assert_eq!(
        snapshot
            .reader()
            .get_physical_clustering_columns(engine.as_ref())?,
        expected_columns
    );
    let persisted = snapshot
        .reader()
        .get_domain_metadata_internal("delta.clustering", engine.as_ref())?
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&persisted).map_err(KernelError::from)?,
        configuration
    );
    for feature in [TableFeature::ClusteredTable, TableFeature::DomainMetadata] {
        assert!(
            snapshot
                .table_configuration()
                .is_feature_supported(&feature)
        );
    }
    let rows = if with_initial_data {
        vec![0, 1, 2, 3]
    } else {
        vec![1, 2, 3]
    };
    let expected = pretty_format_batches(&[simple_id_batch(&snapshot.schema(), rows)])
        .map_err(KernelError::from)?
        .to_string();
    let expected: Vec<_> = expected.lines().collect();
    let batches = read_scan(&snapshot.scan_builder().build()?, engine)?;
    assert_batches_sorted_eq!(expected, &batches);
    Ok(())
}

#[rstest]
#[case::unknown_domain("delta.unknown", Some("{}"), "not staged by Kernel")]
#[case::user_domain("app.config", Some("{}"), "not staged by Kernel")]
#[case::changed_columns(
    "delta.clustering",
    Some(r#"{"clusteringColumns":[["id"]]}"#),
    "cannot change clustering columns"
)]
#[case::missing_columns("delta.clustering", Some("{}"), "missing field")]
#[case::invalid_json("delta.clustering", Some("invalid"), "expected value")]
#[case::callback_error("delta.clustering", None, "extension failed")]
#[test]
fn test_system_domain_metadata_transform_failure_preserves_configuration(
    #[case] domain: &str,
    #[case] configuration: Option<&str>,
    #[case] expected_error: &str,
) -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let mut txn = create_table(&table_path, get_simple_schema(), "Test/1.0", true)
        .with_data_layout(DataLayout::clustered::<_, &str>([]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let result = txn.transform_system_domain_metadata(domain, |_| {
        configuration
            .map(str::to_owned)
            .ok_or_else(|| KernelError::generic("extension failed"))
    });
    assert_result_error_with_message(result, expected_error);

    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let configuration = snapshot
        .reader()
        .get_domain_metadata_internal("delta.clustering", engine.as_ref())?
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&configuration).map_err(KernelError::from)?,
        json!({"clusteringColumns": []})
    );
    Ok(())
}

/// Test that combining explicit feature signals with auto-enabled features doesn't create
/// duplicates.
///
/// This tests the edge case where a user provides `delta.feature.domainMetadata=supported`
/// AND uses `DataLayout::Clustered`. Both would try to add DomainMetadata, but we should
/// only have it once in the feature lists.
#[tokio::test]
async fn test_clustering_with_explicit_feature_signal_no_duplicates() -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = simple_schema()?;

    // Combine BOTH: explicit feature signal AND clustering (which auto-adds domainMetadata)
    let _ = create_table(&table_path, schema, "Test/1.0", true)
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .with_data_layout(DataLayout::clustered(["id"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Read back using kernel APIs and verify no duplicate features
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("Writer features should exist");

    // Count occurrences of DomainMetadata - should be exactly 1, not 2
    let domain_metadata_count = writer_features
        .iter()
        .filter(|f| **f == TableFeature::DomainMetadata)
        .count();

    assert_eq!(
        domain_metadata_count, 1,
        "domainMetadata should appear exactly once, not {domain_metadata_count} times (duplicate detected!)"
    );

    // Verify clustering columns via snapshot read path
    let clustering_columns = snapshot
        .reader()
        .get_physical_clustering_columns(engine.as_ref())?;
    assert_eq!(clustering_columns, Some(vec![column_name!("id")]));

    Ok(())
}

#[tokio::test]
async fn test_clustering_stats_columns_within_limit() -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 10 columns (cluster on column 5, within default 32 limit)
    let fields: Vec<StructField> = (0..10)
        .map(|i| StructField::nullable(format!("col{i}"), DataType::INTEGER))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col5
    let txn = create_table(&table_path, schema, "Test/1.0", true)
        .with_data_layout(DataLayout::clustered(["col5"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "col5"),
        "Clustering column col5 should be in stats columns"
    );

    Ok(())
}

#[tokio::test]
async fn test_clustering_stats_columns_beyond_limit() -> delta_kernel::Result<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // Build schema with 40 columns (cluster on column 35, beyond default 32 limit)
    let fields: Vec<StructField> = (0..40)
        .map(|i| StructField::nullable(format!("col{i}"), DataType::INTEGER))
        .collect();
    let schema = Arc::new(StructType::try_new(fields)?);

    // Create clustered table on col35 (position > 32)
    let txn = create_table(&table_path, schema, "Test/1.0", true)
        .with_data_layout(DataLayout::clustered(["col35"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;

    // Verify stats_columns includes the clustering column even beyond limit
    let stats_cols = txn.stats_columns();
    assert!(
        stats_cols.iter().any(|c| c.to_string() == "col35"),
        "Clustering column col35 should be in stats columns even beyond DEFAULT_NUM_INDEXED_COLS"
    );

    // Verify we have exactly 33 stats columns: first 32 + col35
    // (col35 is added in Pass 2 of collect_columns)
    assert_eq!(
        stats_cols.len(),
        33,
        "Should have 32 indexed cols + 1 clustering col"
    );

    Ok(())
}

#[rstest]
#[case::not_in_schema(vec!["nonexistent"], "not found in schema")]
#[case::nested_not_found(vec!["l1", "l2", "l3", "l4", "missing"], "not found in schema")]
#[case::struct_as_leaf(vec!["address"], "unsupported type")]
#[tokio::test]
async fn test_clustering_column_error(
    #[case] col_path: Vec<&str>,
    #[case] expected_error: &str,
) -> KernelResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let schema = clustering_test_schema()?;

    let result = create_table(&table_path, schema, "Test/1.0", true)
        .with_data_layout(DataLayout::Clustered {
            columns: vec![ColumnName::new(col_path.iter().copied())],
        })
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(result, expected_error);

    Ok(())
}
