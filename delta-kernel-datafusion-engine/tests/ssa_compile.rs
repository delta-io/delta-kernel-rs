//! Round-trip integration tests for the SSA `compile_ssa` lowering. Each test builds an
//! [`ssa::Plan`] directly via [`Plan::push`], wraps it in an [`ssa::ResultPlan`], and runs
//! it through [`DataFusionExecutor::ssa_result_to_dataframe`] -- exercising the per-Node
//! lowerings without requiring a state machine.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::SumRowsConsumer;
use delta_kernel::arrow::array::{AsArray, RecordBatch};
use delta_kernel::arrow::compute::concat_batches;
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
use delta_kernel::plans::ir::nodes::{ConsumeSink, FileType, ScanFileColumns};
use delta_kernel::plans::ir::ssa::{JoinKind, Node, Plan as SsaPlan, ResultPlan as SsaResultPlan};
use delta_kernel::plans::operations::framework::step::Step;
use delta_kernel::plans::operations::framework::step_payload::StepPayload;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel_datafusion_engine::{testing, DataFusionExecutor};

fn run_to_one_batch(rp: SsaResultPlan) -> RecordBatch {
    let exec = DataFusionExecutor::try_new().expect("executor");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let batches = runtime
        .block_on(testing::collect_ssa_result(&exec, rp))
        .expect("collect");
    assert!(!batches.is_empty(), "expected at least one batch");
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("concat")
}

fn long_field(name: &str) -> StructField {
    StructField::new(name, DataType::LONG, true)
}

fn long_schema(fields: &[&str]) -> SchemaRef {
    Arc::new(StructType::try_new(fields.iter().map(|n| long_field(n))).expect("schema"))
}

fn long_col(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("column {name} not found in {:?}", batch.schema()));
    batch
        .column(idx)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

/// `Values` rows lower to a `LogicalPlan::Values` whose batches preserve row order.
#[test]
fn values_round_trip_preserves_rows() {
    let mut plan = SsaPlan::new();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(2), Scalar::Long(20)],
        vec![Scalar::Long(3), Scalar::Long(30)],
    ];
    let result = plan.push(
        Node::Values {
            schema: long_schema(&["a", "b"]),
            rows,
        },
        vec![],
    );
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "a"), vec![1, 2, 3]);
    assert_eq!(long_col(&batch, "b"), vec![10, 20, 30]);
}

/// `Filter` keeps rows where the predicate evaluates true.
#[test]
fn filter_drops_rows_where_predicate_is_false() {
    let mut plan = SsaPlan::new();
    let src = plan.push(
        Node::Values {
            schema: long_schema(&["x"]),
            rows: vec![
                vec![Scalar::Long(1)],
                vec![Scalar::Long(2)],
                vec![Scalar::Long(3)],
                vec![Scalar::Long(4)],
            ],
        },
        vec![],
    );
    let predicate = Arc::new(Predicate::gt(
        Expression::column(["x"]),
        Expression::literal(Scalar::Long(2)),
    ));
    let result = plan.push(Node::Filter { predicate }, vec![src]);
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    let kept = long_col(&batch, "x");
    let kept_set: HashSet<i64> = kept.iter().copied().collect();
    assert_eq!(kept_set, HashSet::from([3, 4]));
}

/// `Project` renames + reorders columns; the output schema honors the named expression list.
#[test]
fn project_renames_columns() {
    let mut plan = SsaPlan::new();
    let src = plan.push(
        Node::Values {
            schema: long_schema(&["a", "b"]),
            rows: vec![vec![Scalar::Long(11), Scalar::Long(22)]],
        },
        vec![],
    );
    let result = plan.push(
        Node::Project {
            named_exprs: vec![
                ("y".to_string(), Arc::new(Expression::column(["b"]))),
                ("x".to_string(), Arc::new(Expression::column(["a"]))),
            ],
            output_schema: long_schema(&["y", "x"]),
        },
        vec![src],
    );
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "y"), vec![22]);
    assert_eq!(long_col(&batch, "x"), vec![11]);
}

/// `Union { ordered: true }` concatenates inputs in order. We tag each side with a known
/// constant so the order is observable post-collect.
#[test]
fn ordered_union_preserves_input_order() {
    let mut plan = SsaPlan::new();
    let left = plan.push(
        Node::Values {
            schema: long_schema(&["v"]),
            rows: vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]],
        },
        vec![],
    );
    let right = plan.push(
        Node::Values {
            schema: long_schema(&["v"]),
            rows: vec![vec![Scalar::Long(3)], vec![Scalar::Long(4)]],
        },
        vec![],
    );
    let result = plan.push(Node::Union { ordered: true }, vec![left, right]);
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    assert_eq!(long_col(&batch, "v"), vec![1, 2, 3, 4]);
}

/// `EquiJoin { kind: Inner }` emits matching `(left, right)` rows.
#[test]
fn inner_equi_join_emits_matching_rows() {
    let mut plan = SsaPlan::new();
    let left = plan.push(
        Node::Values {
            schema: long_schema(&["k", "v_left"]),
            rows: vec![
                vec![Scalar::Long(1), Scalar::Long(10)],
                vec![Scalar::Long(2), Scalar::Long(20)],
                vec![Scalar::Long(3), Scalar::Long(30)],
            ],
        },
        vec![],
    );
    let right = plan.push(
        Node::Values {
            schema: long_schema(&["rk", "v_right"]),
            rows: vec![
                vec![Scalar::Long(2), Scalar::Long(200)],
                vec![Scalar::Long(3), Scalar::Long(300)],
                vec![Scalar::Long(4), Scalar::Long(400)],
            ],
        },
        vec![],
    );
    let result = plan.push(
        Node::EquiJoin {
            kind: JoinKind::Inner,
            key_pairs: vec![(
                Arc::new(Expression::column(["k"])),
                Arc::new(Expression::column(["rk"])),
            )],
        },
        vec![left, right],
    );
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    let mut tuples: Vec<(i64, i64, i64, i64)> = (0..batch.num_rows())
        .map(|i| {
            (
                long_col(&batch, "k")[i],
                long_col(&batch, "v_left")[i],
                long_col(&batch, "rk")[i],
                long_col(&batch, "v_right")[i],
            )
        })
        .collect();
    tuples.sort();
    assert_eq!(tuples, vec![(2, 20, 2, 200), (3, 30, 3, 300)]);
}

/// `EquiJoin { kind: LeftAnti }` emits each left row whose key matches no right row.
#[test]
fn left_anti_join_drops_matched_left_rows() {
    let mut plan = SsaPlan::new();
    let left = plan.push(
        Node::Values {
            schema: long_schema(&["k"]),
            rows: vec![
                vec![Scalar::Long(1)],
                vec![Scalar::Long(2)],
                vec![Scalar::Long(3)],
            ],
        },
        vec![],
    );
    let right = plan.push(
        Node::Values {
            schema: long_schema(&["k"]),
            rows: vec![vec![Scalar::Long(2)]],
        },
        vec![],
    );
    let result = plan.push(
        Node::EquiJoin {
            kind: JoinKind::LeftAnti,
            key_pairs: vec![(
                Arc::new(Expression::column(["k"])),
                Arc::new(Expression::column(["k"])),
            )],
        },
        vec![left, right],
    );
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    let kept: HashSet<i64> = long_col(&batch, "k").into_iter().collect();
    assert_eq!(kept, HashSet::from([1, 3]));
}

/// `Step::Consume` drains an SSA dataflow into a [`KernelConsumer`]
/// (`KernelConsumer::finish` -> `usize` row count) and the executor returns the finalized
/// handle as `StepPayload::Consumer`, keyed by the sink's token.
#[tokio::test]
async fn step_consume_drains_ssa_into_consumer_handle() {
    let mut plan = SsaPlan::new();
    let src = plan.push(
        Node::Values {
            schema: long_schema(&["v"]),
            rows: vec![
                vec![Scalar::Long(1)],
                vec![Scalar::Long(2)],
                vec![Scalar::Long(3)],
                vec![Scalar::Long(4)],
            ],
        },
        vec![],
    );
    let predicate = Arc::new(Predicate::gt(
        Expression::column(["v"]),
        Expression::literal(Scalar::Long(2)),
    ));
    let terminal = plan.push(Node::Filter { predicate }, vec![src]);

    let sink = ConsumeSink::new_consumer(SumRowsConsumer::new("ssa.consume_test"));
    let token = sink.token.clone();
    let stmts = plan.stmts;

    let executor = DataFusionExecutor::try_new().unwrap();
    let payload = executor
        .execute_step(Step::Consume {
            stmts,
            terminal,
            sink,
        })
        .await
        .expect("Step::Consume execution");

    let handle = match payload {
        StepPayload::Consumer(h) => h,
        other => panic!("expected StepPayload::Consumer, got {other:?}"),
    };
    assert_eq!(
        handle.token, token,
        "finished handle carries the sink token"
    );
    let total = *handle
        .erased
        .downcast::<usize>()
        .expect("SumRowsConsumer finishes with usize");
    assert_eq!(total, 2, "filter keeps rows with v > 2 (i.e., 3 and 4)");
}

/// `Node::Load` reads each upstream row's path-column file in `file_type`, broadcasts the
/// `passthrough_columns` onto every emitted file row, and lifts the `file_schema` columns
/// alongside. Verifies the engine-side ephemeral-`RelationHandle` plumbing (synthesized
/// inside `compile_ssa` so we don't have to refactor `LoadTableProvider`).
#[tokio::test]
async fn load_node_reads_files_and_broadcasts_passthrough() {
    use test_utils::parquet::write_i64_parquet;
    use url::Url;

    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("data.parquet");
    write_i64_parquet(&parquet_path, "x", &[10_i64, 20_i64]);
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

    let mut plan = SsaPlan::new();
    let upstream = plan.push(
        Node::Values {
            schema: upstream_schema,
            rows: vec![
                vec![
                    Scalar::String(rel_path.clone()),
                    Scalar::String("alpha".into()),
                ],
                vec![Scalar::String(rel_path), Scalar::String("beta".into())],
            ],
        },
        vec![],
    );
    let result = plan.push(
        Node::Load {
            file_schema,
            file_type: FileType::Parquet,
            base_url: Some(base_url),
            passthrough_columns: vec![ColumnName::new(["tag"])],
            file_meta: ScanFileColumns {
                path: ColumnName::new(["path"]),
                size: None,
                record_count: None,
            },
            dv_ref: None,
        },
        vec![upstream],
    );
    let rp = SsaResultPlan { plan, result };

    let exec = DataFusionExecutor::try_new().unwrap();
    let batches = testing::collect_ssa_result(&exec, rp).await.unwrap();
    assert!(!batches.is_empty(), "expected at least one batch");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // Two upstream rows, each broadcasting onto two file rows -> 4 emitted rows.
    assert_eq!(total_rows, 4);

    // Every emitted row carries the upstream `tag` value broadcast onto each file row.
    let schema = batches[0].schema();
    assert!(schema.field_with_name("x").is_ok());
    assert!(schema.field_with_name("tag").is_ok());
}

/// `MaxByVersion` keeps the row with the largest `version` per group key, narrowed to the
/// declared `value_columns` (group_by exprs are aggregation-internal and do NOT appear in
/// the output).
#[test]
fn max_by_version_keeps_top_row_per_group_and_narrows_to_value_columns() {
    let mut plan = SsaPlan::new();
    let src = plan.push(
        Node::Values {
            schema: long_schema(&["k", "version", "payload"]),
            rows: vec![
                vec![Scalar::Long(1), Scalar::Long(1), Scalar::Long(100)],
                vec![Scalar::Long(1), Scalar::Long(3), Scalar::Long(300)],
                vec![Scalar::Long(1), Scalar::Long(2), Scalar::Long(200)],
                vec![Scalar::Long(2), Scalar::Long(5), Scalar::Long(500)],
                vec![Scalar::Long(2), Scalar::Long(7), Scalar::Long(700)],
            ],
        },
        vec![],
    );
    let result = plan.push(
        Node::MaxByVersion {
            group_by: vec![Arc::new(Expression::column(["k"]))],
            version_column: Arc::new(Expression::column(["version"])),
            value_columns: vec!["payload".to_string()],
        },
        vec![src],
    );
    let rp = SsaResultPlan { plan, result };

    let batch = run_to_one_batch(rp);
    // Output is `value_columns` only.
    assert_eq!(batch.schema().fields().len(), 1);
    assert_eq!(batch.schema().field(0).name(), "payload");
    let payloads: HashSet<i64> = long_col(&batch, "payload").into_iter().collect();
    assert_eq!(payloads, HashSet::from([300, 700]));
}
