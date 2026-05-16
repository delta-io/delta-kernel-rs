//! Integration tests for [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load),
//! DV masking via [`LoadSink::dv_ref`], and parquet footer reads (SchemaQuery-shaped API).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::expressions::{ColumnName, Scalar, StructData};
use delta_kernel::plans::ir::nodes::{
    DvRef, FileType, LoadSink, RelationHandle, ScanFileColumns, ValuesNode,
};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType, ToSchema};
use delta_kernel::FileMeta;
use delta_kernel_datafusion_engine::DataFusionExecutor;
use parquet::arrow::ArrowWriter;
use url::Url;

fn col(name: &str) -> ColumnName {
    ColumnName::from_naive_str_split(name)
}

fn write_i64_parquet(path: &std::path::Path, field: &str, values: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        field,
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(values.iter().copied())) as ArrayRef],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn load_sink_broadcasts_passthrough_columns() {
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("data.parquet");
    write_i64_parquet(&parquet_path, "x", &[1_i64, 2_i64]);

    let rel_path = parquet_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("tag", DataType::STRING),
        ])
        .unwrap(),
    );

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());

    let merged_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("x", DataType::LONG),
            StructField::not_null("tag", DataType::STRING),
        ])
        .unwrap(),
    );

    let handle = RelationHandle::fresh("loaded", merged_schema.clone());

    let sink = LoadSink {
        output_relation: handle.clone(),
        file_schema,
        base_url: Some(base_url),
        file_meta: ScanFileColumns {
            path: col("path"),
            size: None,
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: vec![col("tag")],
        file_type: FileType::Parquet,
    };

    let lit = DeclarativePlanNode::Values(ValuesNode {
        schema: upstream_schema,
        rows: vec![
            vec![
                Scalar::String(rel_path.clone()),
                Scalar::String("alpha".into()),
            ],
            vec![Scalar::String(rel_path), Scalar::String("beta".into())],
        ],
    });

    let producer_plan = lit.into_load(sink);

    let executor = DataFusionExecutor::try_new().unwrap();
    executor.execute_plans(&[producer_plan]).await.unwrap();
    let producer_rows: usize = executor
        .collect_relation(&handle)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(producer_rows, 4);

    let batches = executor.collect_relation(&handle).await.unwrap();

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4);

    let idx_x = batches[0].schema().column_with_name("x").unwrap().0;
    let idx_tag = batches[0].schema().column_with_name("tag").unwrap().0;

    let mut pairs = Vec::new();
    for b in &batches {
        let xa = b
            .column(idx_x)
            .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>();
        let ta = b.column(idx_tag).as_string::<i32>();
        for r in 0..b.num_rows() {
            pairs.push((xa.value(r), ta.value(r).to_string()));
        }
    }
    pairs.sort_by_key(|p| (p.0, p.1.clone()));
    assert_eq!(
        pairs,
        vec![
            (1, "alpha".into()),
            (1, "beta".into()),
            (2, "alpha".into()),
            (2, "beta".into()),
        ]
    );
}

#[tokio::test]
async fn load_sink_without_dv_reads_full_parquet_row_group() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::fs::canonicalize(manifest_dir.join("../kernel/tests/data/table-with-dv-small"))
        .unwrap();
    let base_url = Url::from_directory_path(&root).unwrap();
    let path_str =
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet".to_string();

    let upstream_schema =
        Arc::new(StructType::try_new([StructField::not_null("path", DataType::STRING)]).unwrap());

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("value", DataType::INTEGER)]).unwrap());

    let handle = RelationHandle::fresh("dv_loaded", Arc::clone(&file_schema));

    let sink = LoadSink {
        output_relation: handle.clone(),
        file_schema: Arc::clone(&file_schema),
        base_url: Some(base_url),
        file_meta: ScanFileColumns {
            path: col("path"),
            size: None,
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: Vec::new(),
        file_type: FileType::Parquet,
    };

    let lit = DeclarativePlanNode::Values(ValuesNode {
        schema: upstream_schema,
        rows: vec![vec![Scalar::String(path_str)]],
    });

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_plans(&[lit.into_load(sink)])
        .await
        .unwrap();
    let producer_rows: usize = executor
        .collect_relation(&handle)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(producer_rows, 10);

    let batches = executor.collect_relation(&handle).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 10);
}

fn dv_example_scalar() -> Scalar {
    let fields: Vec<_> = DeletionVectorDescriptor::to_schema()
        .fields()
        .cloned()
        .collect();
    Scalar::Struct(
        StructData::try_new(
            fields,
            vec![
                Scalar::String("u".into()),
                Scalar::String("vBn[lx{q8@P<9BNH/isA".into()),
                Scalar::Integer(1),
                Scalar::Integer(36),
                Scalar::Long(2),
            ],
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn load_sink_applies_dv_ref_masking_from_descriptor_column() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::fs::canonicalize(manifest_dir.join("../kernel/tests/data/table-with-dv-small"))
        .unwrap();
    let base_url = Url::from_directory_path(&root).unwrap();
    let path_str =
        "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet".to_string();

    let dv_cn = col("dv");

    let upstream_schema = Arc::new(
        StructType::try_new([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null(
                dv_cn.to_string(),
                DataType::Struct(Box::new(DeletionVectorDescriptor::to_schema())),
            ),
        ])
        .unwrap(),
    );

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("value", DataType::INTEGER)]).unwrap());

    let handle = RelationHandle::fresh("dv_masked", Arc::clone(&file_schema));

    let sink = LoadSink {
        output_relation: handle.clone(),
        file_schema: Arc::clone(&file_schema),
        base_url: Some(base_url),
        file_meta: ScanFileColumns {
            path: col("path"),
            size: None,
            record_count: None,
        },
        dv_ref: Some(DvRef::skip(dv_cn)),
        passthrough_columns: Vec::new(),
        file_type: FileType::Parquet,
    };

    let lit = DeclarativePlanNode::Values(ValuesNode {
        schema: upstream_schema,
        rows: vec![vec![Scalar::String(path_str), dv_example_scalar()]],
    });

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_plans(&[lit.into_load(sink)])
        .await
        .unwrap();
    let producer_rows: usize = executor
        .collect_relation(&handle)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(producer_rows, 8);

    let batches = executor.collect_relation(&handle).await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 8);
}

#[tokio::test]
async fn load_sink_reads_ndjson_with_matching_schema() {
    let dir = tempfile::tempdir().unwrap();
    let jpath = dir.path().join("rows.ndjson");
    std::fs::write(&jpath, "{\"y\":10}\n{\"y\":20}\n").unwrap();

    let rel = jpath.file_name().unwrap().to_str().unwrap().to_string();
    let base_url = Url::from_directory_path(dir.path()).unwrap();

    let upstream_schema =
        Arc::new(StructType::try_new([StructField::not_null("path", DataType::STRING)]).unwrap());

    let file_schema =
        Arc::new(StructType::try_new([StructField::not_null("y", DataType::LONG)]).unwrap());

    let handle = RelationHandle::fresh("json_loaded", Arc::clone(&file_schema));

    let sink = LoadSink {
        output_relation: handle.clone(),
        file_schema: Arc::clone(&file_schema),
        base_url: Some(base_url),
        file_meta: ScanFileColumns {
            path: col("path"),
            size: None,
            record_count: None,
        },
        dv_ref: None,
        passthrough_columns: Vec::new(),
        file_type: FileType::Json,
    };

    let lit = DeclarativePlanNode::Values(ValuesNode {
        schema: upstream_schema,
        rows: vec![vec![Scalar::String(rel)]],
    });

    let executor = DataFusionExecutor::try_new().unwrap();
    executor
        .execute_plans(&[lit.into_load(sink)])
        .await
        .unwrap();
    let producer_rows: usize = executor
        .collect_relation(&handle)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(producer_rows, 2);

    let batches = executor.collect_relation(&handle).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    let idx_y = batches[0].schema().column_with_name("y").unwrap().0;
    let mut ys = Vec::new();
    for b in &batches {
        ys.extend(
            b.column(idx_y)
                .as_primitive::<delta_kernel::arrow::datatypes::Int64Type>()
                .values()
                .iter()
                .copied(),
        );
    }
    ys.sort_unstable();
    assert_eq!(ys, vec![10_i64, 20_i64]);
}

#[tokio::test]
async fn parquet_footer_schema_query_matches_file_footer() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("one.parquet");
    write_i64_parquet(&p, "metric", &[42_i64]);

    let url = Url::from_file_path(&p).unwrap();
    let len = std::fs::metadata(&p).unwrap().len();
    let meta = FileMeta::new(url, 0, len);

    let executor = DataFusionExecutor::try_new().unwrap();
    let footer_schema = executor
        .engine()
        .parquet_handler()
        .read_parquet_footer(&meta)
        .unwrap()
        .schema;

    assert!(
        footer_schema.fields().any(|f| f.name() == "metric"),
        "{footer_schema:?}"
    );
}
