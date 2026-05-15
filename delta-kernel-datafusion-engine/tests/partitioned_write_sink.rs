//! [`SinkType::PartitionedWrite`] routing and output smoke tests for the DataFusion engine.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use delta_kernel::arrow::array::AsArray;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::PartitionedWriteSink;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use url::Url;

fn region_value_schema() -> delta_kernel::schema::SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null("region", DataType::STRING),
        StructField::not_null("v", DataType::LONG),
    ]))
}

fn file_url(dir: &TempDir) -> Url {
    Url::from_directory_path(dir.path()).expect("temp dir URL")
}

fn hive_parquet_path(base: &Path, segments: &[&str]) -> PathBuf {
    let mut p = base.to_path_buf();
    for s in segments {
        p.push(s);
    }
    p.push("part-000.parquet");
    p
}

fn read_parquet_path(path: &Path) -> Vec<delta_kernel::arrow::array::RecordBatch> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
    let reader = builder.build().expect("build reader");
    let mut out = Vec::new();
    for b in reader {
        out.push(b.expect("batch"));
    }
    out
}

#[tokio::test]
async fn partitioned_write_parquet_hive_paths_and_row_counts() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = region_value_schema();
    let rows = vec![
        vec![Scalar::String("eu".into()), Scalar::Long(1)],
        vec![Scalar::String("us".into()), Scalar::Long(2)],
        vec![Scalar::String("eu".into()), Scalar::Long(3)],
    ];
    let sink = PartitionedWriteSink::parquet(dest, vec!["region".into()]);
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    let collected = executor.execute_plan_collect(plan).await.expect("run");
    assert!(
        collected.is_empty(),
        "partitioned write drains without yielding batches"
    );

    let eu_batches = read_parquet_path(&hive_parquet_path(dir.path(), &["region=eu"]));
    assert_eq!(eu_batches.len(), 1);
    assert_eq!(eu_batches[0].num_rows(), 2);

    let us_batches = read_parquet_path(&hive_parquet_path(dir.path(), &["region=us"]));
    assert_eq!(us_batches.len(), 1);
    assert_eq!(us_batches[0].num_rows(), 1);

    let v_eu = eu_batches[0]
        .column(1)
        .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
    let mut vals: Vec<i64> = (0..eu_batches[0].num_rows())
        .map(|i| v_eu.value(i))
        .collect();
    vals.sort_unstable();
    assert_eq!(vals, vec![1, 3]);
}

#[tokio::test]
async fn partitioned_write_json_lines_roundtrip_values() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = region_value_schema();
    let rows = vec![
        vec![Scalar::String("eu".into()), Scalar::Long(10)],
        vec![Scalar::String("us".into()), Scalar::Long(20)],
    ];
    let sink = PartitionedWriteSink::json_lines(dest, vec!["region".into()]);
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    let stream = executor.execute_plan_to_stream(plan).await.expect("stream");
    let n: usize = stream
        .try_fold(0, |acc, _| async move { Ok(acc + 1) })
        .await
        .expect("drain");
    assert_eq!(n, 0);

    let eu_path = dir.path().join("region=eu").join("part-000.jsonl");
    let us_path = dir.path().join("region=us").join("part-000.jsonl");
    let eu_raw = std::fs::read_to_string(&eu_path).expect("eu jsonl");
    let us_raw = std::fs::read_to_string(&us_path).expect("us jsonl");

    let eu: serde_json::Value = serde_json::from_str(eu_raw.lines().next().expect("line")).unwrap();
    let us: serde_json::Value = serde_json::from_str(us_raw.lines().next().expect("line")).unwrap();
    assert_eq!(eu["region"], "eu");
    assert_eq!(eu["v"], 10);
    assert_eq!(us["region"], "us");
    assert_eq!(us["v"], 20);
}

#[tokio::test]
async fn partitioned_write_two_level_hive_layout() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = Arc::new(StructType::new_unchecked([
        StructField::not_null("dt", DataType::STRING),
        StructField::not_null("region", DataType::STRING),
        StructField::not_null("v", DataType::LONG),
    ]));
    let rows = vec![
        vec![
            Scalar::String("2024-01-01".into()),
            Scalar::String("eu".into()),
            Scalar::Long(1),
        ],
        vec![
            Scalar::String("2024-01-01".into()),
            Scalar::String("us".into()),
            Scalar::Long(2),
        ],
        vec![
            Scalar::String("2024-01-02".into()),
            Scalar::String("eu".into()),
            Scalar::Long(3),
        ],
    ];
    let sink = PartitionedWriteSink::parquet(dest, vec!["dt".into(), "region".into()]);
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    executor.execute_plan_collect(plan).await.expect("run");

    let p1 = read_parquet_path(&hive_parquet_path(
        dir.path(),
        &["dt=2024-01-01", "region=eu"],
    ));
    let p2 = read_parquet_path(&hive_parquet_path(
        dir.path(),
        &["dt=2024-01-01", "region=us"],
    ));
    let p3 = read_parquet_path(&hive_parquet_path(
        dir.path(),
        &["dt=2024-01-02", "region=eu"],
    ));
    assert_eq!(p1[0].num_rows(), 1);
    assert_eq!(p2[0].num_rows(), 1);
    assert_eq!(p3[0].num_rows(), 1);
}

#[tokio::test]
async fn partitioned_write_union_batches_merge_same_directory() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = region_value_schema();
    let a = DeclarativePlanNode::values(
        schema.clone(),
        vec![vec![Scalar::String("eu".into()), Scalar::Long(1)]],
    )
    .expect("lit a");
    let b = DeclarativePlanNode::values(
        schema,
        vec![vec![Scalar::String("eu".into()), Scalar::Long(2)]],
    )
    .expect("lit b");
    let unioned = DeclarativePlanNode::union([a, b]).expect("union");
    let sink = PartitionedWriteSink::parquet(dest, vec!["region".into()]);
    let plan = unioned.into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    executor.execute_plan_collect(plan).await.expect("run");

    let batches = read_parquet_path(&hive_parquet_path(dir.path(), &["region=eu"]));
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn partitioned_write_escapes_special_characters_in_segment() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = region_value_schema();
    let rows = vec![vec![Scalar::String("a:b".into()), Scalar::Long(7)]];
    let sink = PartitionedWriteSink::parquet(dest, vec!["region".into()]);
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    executor.execute_plan_collect(plan).await.expect("run");

    let segment_dir = dir.path().join("region=a%3Ab");
    assert!(segment_dir.is_dir(), "expected {:?} to exist", segment_dir);
    let batches = read_parquet_path(&hive_parquet_path(dir.path(), &["region=a%3Ab"]));
    assert_eq!(batches[0].num_rows(), 1);
}

#[test]
fn partitioned_write_compile_errors_on_unknown_partition_column() {
    let dir = TempDir::new().expect("tempdir");
    let dest = file_url(&dir);
    let schema = region_value_schema();
    let rows = vec![vec![Scalar::String("eu".into()), Scalar::Long(1)]];
    let sink = PartitionedWriteSink::parquet(dest, vec!["no_such_col".into()]);
    let plan = DeclarativePlanNode::values(schema, rows)
        .expect("literal")
        .into_partitioned_write(sink);

    let executor = DataFusionExecutor::try_new().expect("executor");
    let err = executor
        .compile_plan(&plan)
        .expect_err("unknown partition column");
    let detail = format!("{err}");
    assert!(
        detail.contains("no_such_col"),
        "unexpected error message: {detail}"
    );
}
