use std::sync::Arc;

use delta_kernel::expressions::{col, lit, Predicate};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::{schema_ref, DataType, UserDefinedType};
use delta_kernel::Snapshot;
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, test_table_setup_mt};
use url::Url;

use super::{surviving_paths_with_stats, AllNullSource};

#[rstest]
#[case::comparison(Predicate::eq(col!("value"), lit(42i64)), vec!["mixed.parquet", "non_null.parquet"])]
#[case::is_null(Predicate::is_null(col!("value")), vec!["all_null.parquet", "mixed.parquet"])]
#[case::is_not_null(Predicate::is_not_null(col!("value")), vec!["mixed.parquet", "non_null.parquet"])]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udt_skipping_ignores_min_max_and_reads_leaf_null_count(
    #[case] predicate: Predicate,
    #[case] expected: Vec<&str>,
    #[values(
        AllNullSource::CommitOnly,
        AllNullSource::CheckpointJsonStats,
        AllNullSource::CheckpointStructStats
    )]
    source: AllNullSource,
    #[values(false, true)] parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, table_path, engine) = test_table_setup_mt()?;
    let table_root = Url::from_directory_path(&table_path).unwrap().to_string();
    let schema = schema_ref! {
        nullable "value": (UserDefinedType {
            sql_type: Box::new(DataType::LONG),
            annotation: [("class".into(), Some("example.Value".into()))].into(),
        }),
    };
    let struct_only = matches!(source, AllNullSource::CheckpointStructStats);
    let configuration = if struct_only {
        json!({"delta.checkpoint.writeStatsAsStruct":"true",
            "delta.checkpoint.writeStatsAsJson":"false"})
    } else {
        json!({})
    };
    let mut actions = vec![
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        json!({"metaData":{
            "id":"udt-stats", "format":{"provider":"parquet","options":{}},
            "schemaString":serde_json::to_string(&schema)?, "partitionColumns":[],
            "configuration":configuration,
            "createdTime":0,
        }}),
    ];
    for (path, null_count) in [
        ("non_null.parquet", 0),
        ("mixed.parquet", 1),
        ("all_null.parquet", 2),
    ] {
        // A reader must ignore UDT min/max even when a log contains them.
        let stats = json!({"numRecords":2, "nullCount":{"value":null_count},
            "minValues":{"value":100}, "maxValues":{"value":100}, "tightBounds":true});
        actions.push(json!({"add":{
            "path":path, "size":100, "partitionValues":{}, "modificationTime":0,
            "dataChange":true, "stats":stats.to_string(),
        }}));
    }
    add_commit(
        &table_root,
        &LocalFileSystem::new(),
        0,
        actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .await?;
    if !matches!(source, AllNullSource::CommitOnly) {
        let snapshot = Snapshot::builder_for(&table_root).build(engine.as_ref())?;
        snapshot.checkpoint(engine.as_ref(), None)?;
    }
    // Parallel scans only read JSON checkpoint stats; struct-only stats keep all files (#2832).
    let expected = if parallel && struct_only {
        vec!["all_null.parquet", "mixed.parquet", "non_null.parquet"]
    } else {
        expected
    };
    assert_eq!(
        surviving_paths_with_stats(
            &table_path,
            engine,
            Arc::new(predicate),
            StatsOptions::default(),
            parallel,
        )?,
        expected
    );
    Ok(())
}
