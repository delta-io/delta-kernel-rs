//! DataFusion execution scaffold for Delta Kernel declarative [`Plan`] trees.
//!
//! Supported sinks:
//! - [`SinkType::Relation`](delta_kernel::plans::ir::nodes::SinkType::Relation) -- registers a
//!   lazy [`ViewTable`](datafusion::datasource::ViewTable) wrapping the upstream `LogicalPlan`.
//!   DataFusion's `InlineTableScan` analyzer rule inlines the wrapped plan into the consumer's
//!   tree when the relation is read, so predicate / projection pushdown and CSE flow across
//!   plan boundaries.
//! - [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) -- registers a lazy
//!   [`LoadTableProvider`](exec::LoadTableProvider) capturing the upstream `LogicalPlan` + load
//!   config + kernel [`Engine`](delta_kernel::Engine). Its `scan()` streams per-row file
//!   batches via [`LoadExec`](exec::LoadExec); no I/O happens until the consumer reads.
//! - [`SinkType::Consume`](delta_kernel::plans::ir::nodes::SinkType::Consume) -- the only sink
//!   with eager side effects: drains the physical plan into a
//!   [`delta_kernel::plans::kdf::ConsumerKdf`] handle at execute-plan time.
//!
//! Read-style state machines return a [`delta_kernel::plans::ir::ResultPlan`] naming the
//! relation the caller reads after executing the result plan's plans. Because `Relation` /
//! `Load` provider registration is lazy, the producing-plan ordering still matters (later plans
//! can see earlier plans' relation handles in their compile context) but the upstream
//! pipelines are not run until the result is collected.
//!
//! Sinks are annotations on the kernel [`Plan`](delta_kernel::plans::ir::Plan), not envelopes
//! on the DataFusion physical plan. Unsupported constructs surface a
//! [`datafusion_common::error::DataFusionError::NotImplemented`] via [`error::unsupported`];
//! the engine -> kernel boundary methods on [`DataFusionExecutor`] translate that into a
//! [`delta_kernel::plans::errors::DeltaError`].

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;

pub use executor::DataFusionExecutor;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::{Expression, Scalar};
    use delta_kernel::plans::ir::nodes::SinkType;
    use delta_kernel::plans::ir::{PlanBuilder, RelationRegistry};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use uuid::Uuid;

    use crate::DataFusionExecutor;

    fn two_bool_schema() -> delta_kernel::schema::SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::BOOLEAN),
            StructField::not_null("b", DataType::BOOLEAN),
        ]))
    }

    #[tokio::test]
    async fn logical_relation_sink_materializes_into_session_ctx() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let schema = two_bool_schema();
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let plan = PlanBuilder::values(
            Arc::clone(&schema),
            vec![vec![Scalar::Boolean(true), Scalar::Boolean(false)]],
        )
        .unwrap()
        .project(
            vec![
                Arc::new(Expression::column(["a"])),
                Arc::new(Expression::column(["b"])),
            ],
            Arc::clone(&schema),
        )
        .into_relation("logical_relation_test", &mut registry)
        .expect("relation sink");
        let SinkType::Relation(handle) = plan.sink.clone() else {
            unreachable!("into_relation always produces SinkType::Relation");
        };

        ex.execute_plans(&[plan]).await.unwrap();
        let relation_batches = ex
            .collect_relation(&handle)
            .await
            .expect("relation sink should materialize output batches");
        assert_eq!(relation_batches.len(), 1);
        assert_eq!(relation_batches[0].num_rows(), 1);
        assert_eq!(relation_batches[0].num_columns(), 2);
    }

    /// `Relation` sinks must register a *lazy* `ViewTable` -- not an eagerly-materialized
    /// `MemTable`. Downcasting the provider returned by `relation_provider` proves both that the
    /// upstream `LogicalPlan` round-trips into the registry and that no eager `collect` path
    /// remains.
    #[tokio::test]
    async fn relation_sink_registers_view_table_not_memtable() {
        use datafusion::datasource::{MemTable, ViewTable};

        let ex = DataFusionExecutor::try_new().unwrap();
        let schema = two_bool_schema();
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let plan = PlanBuilder::values(
            Arc::clone(&schema),
            vec![vec![Scalar::Boolean(true), Scalar::Boolean(false)]],
        )
        .unwrap()
        .into_relation("lazy_view_test", &mut registry)
        .expect("relation sink");
        let SinkType::Relation(handle) = plan.sink.clone() else {
            unreachable!("into_relation always produces SinkType::Relation");
        };

        ex.execute_plans(&[plan]).await.unwrap();

        let provider = ex
            .relation_provider(handle.id.as_str())
            .expect("relation sink should register a provider under its handle id");
        // Critical: lazy registration must yield a `ViewTable`. A `MemTable` here would prove
        // we still eagerly drain the upstream pipeline at register time.
        assert!(
            provider.as_any().downcast_ref::<ViewTable>().is_some(),
            "relation provider must be a ViewTable (got {:?})",
            provider,
        );
        assert!(
            provider.as_any().downcast_ref::<MemTable>().is_none(),
            "relation provider must NOT be a MemTable; lazy registration regressed",
        );

        // And reading the relation lazily must still produce the upstream's row.
        let batches = ex.collect_relation(&handle).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    /// `Load` sinks must register a [`LoadTableProvider`](crate::exec::LoadTableProvider). The
    /// provider holds the upstream `LogicalPlan` + sink + engine and lowers + streams on first
    /// consumer read; nothing materializes at registration time.
    #[tokio::test]
    async fn load_sink_registers_load_table_provider_not_memtable() {
        use datafusion::catalog::TableProvider;
        use datafusion::datasource::MemTable;
        use delta_kernel::plans::ir::nodes::{FileType, ScanFileColumns};

        // Minimal multi-row parquet so the registration path has a real file to wrap. We don't
        // read it; the test asserts the provider type only.
        let dir = tempfile::tempdir().unwrap();
        let parquet_path = dir.path().join("data.parquet");
        test_utils::parquet::write_i64_parquet(&parquet_path, "x", &[1_i64, 2_i64]);

        let rel = parquet_path.file_name().unwrap().to_str().unwrap().to_string();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();

        let upstream_schema = Arc::new(
            StructType::new_unchecked([StructField::not_null("path", DataType::STRING)]),
        );
        let file_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "x",
            DataType::LONG,
        )]));

        let lit = PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(rel)]]).unwrap();
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let plan = lit
            .load(
                "lazy_load_test",
                file_schema,
                FileType::Parquet,
                Some(base_url),
                Vec::new(),
                ScanFileColumns {
                    path: delta_kernel::expressions::ColumnName::from_naive_str_split("path"),
                    size: None,
                    record_count: None,
                },
                None,
                &mut registry,
            )
            .expect("load sink");
        let SinkType::Load(load_sink) = &plan.sink else {
            unreachable!("PlanBuilder::load always produces SinkType::Load");
        };
        let handle_id = load_sink.output_relation.id.clone();

        let ex = DataFusionExecutor::try_new().unwrap();
        ex.execute_plans(&[plan]).await.unwrap();

        let provider = ex
            .relation_provider(&handle_id)
            .expect("load sink should register a provider under its handle id");
        // Sanity: it's our LoadTableProvider, not a leftover MemTable from the old eager path.
        assert!(
            (provider.as_ref() as &dyn TableProvider)
                .as_any()
                .downcast_ref::<crate::exec::LoadTableProvider>()
                .is_some(),
            "load provider must be a LoadTableProvider (got {:?})",
            provider,
        );
        assert!(
            provider.as_any().downcast_ref::<MemTable>().is_none(),
            "load provider must NOT be a MemTable; lazy registration regressed",
        );
    }

    /// Projection pushdown into a [`LoadTableProvider`] must narrow BOTH (a) the kernel
    /// parquet/json handler's request schema and (b) the broadcast passthrough set, all the way
    /// through. This test drives `scan(state, Some(projection), &[], None)` directly so it
    /// asserts the actual `LoadExec` state -- not what DataFusion's optimizer happens to ask
    /// for from a SQL query.
    #[tokio::test]
    async fn load_provider_projection_narrows_read_and_passthrough() {
        use datafusion::catalog::TableProvider;
        use delta_kernel::plans::ir::nodes::{FileType, ScanFileColumns};

        use crate::exec::{LoadExec, LoadTableProvider};

        // Parquet with two columns; only one will be projected.
        let dir = tempfile::tempdir().unwrap();
        let parquet_path = dir.path().join("data.parquet");
        {
            use std::fs::File;

            use delta_kernel::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch};
            use delta_kernel::arrow::datatypes::{
                DataType as ArrowDataType, Field, Schema as ArrowSchema,
            };
            use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;

            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("a", ArrowDataType::Int64, false),
                Field::new("b", ArrowDataType::Int64, false),
            ]));
            let batch = ArrowRecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([1_i64, 2_i64])),
                    Arc::new(Int64Array::from_iter_values([10_i64, 20_i64])),
                ],
            )
            .unwrap();
            let file = File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let rel = parquet_path.file_name().unwrap().to_str().unwrap().to_string();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();

        // Upstream relation carries `path` + two passthrough columns (`p1`, `p2`). The Load
        // sink reads `[a, b]` from each file and broadcasts both passthrough columns.
        let upstream_schema = Arc::new(
            StructType::new_unchecked([
                StructField::not_null("path", DataType::STRING),
                StructField::nullable("p1", DataType::LONG),
                StructField::nullable("p2", DataType::LONG),
            ]),
        );
        let file_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ]));

        let lit = PlanBuilder::values(
            upstream_schema,
            vec![vec![Scalar::String(rel), Scalar::Long(7), Scalar::Long(70)]],
        )
        .unwrap();
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let plan = lit
            .load(
                "proj_pushdown_test",
                file_schema,
                FileType::Parquet,
                Some(base_url),
                vec![
                    delta_kernel::expressions::ColumnName::from_naive_str_split("p1"),
                    delta_kernel::expressions::ColumnName::from_naive_str_split("p2"),
                ],
                ScanFileColumns {
                    path: delta_kernel::expressions::ColumnName::from_naive_str_split("path"),
                    size: None,
                    record_count: None,
                },
                None,
                &mut registry,
            )
            .expect("load sink");
        let SinkType::Load(load_sink) = &plan.sink else {
            unreachable!("PlanBuilder::load always produces SinkType::Load");
        };
        let handle_id = load_sink.output_relation.id.clone();

        let ex = DataFusionExecutor::try_new().unwrap();
        ex.execute_plans(&[plan]).await.unwrap();

        let provider = ex.relation_provider(&handle_id).expect("registered");
        let provider = (provider.as_ref() as &dyn TableProvider)
            .as_any()
            .downcast_ref::<LoadTableProvider>()
            .expect("LoadTableProvider");

        // Output schema = [a, b, p1, p2]. Project [b, p1] -> kernel handler should be asked
        // for just `b`, and only `p1` should be broadcast.
        let state = ex.session_state();
        let projected_plan = provider
            .scan(&state, Some(&vec![1usize, 2usize]), &[], None)
            .await
            .expect("scan");
        let load_exec = projected_plan
            .as_any()
            .downcast_ref::<LoadExec>()
            .expect("scan must produce a LoadExec");

        // Projection pushdown narrowed both the parquet read schema and the partition-value
        // (passthrough) broadcast set. Both are reachable through `LoadExec`'s test-only
        // accessors, which read directly off the projected `FileSource::table_schema()` /
        // `projection()` -- the canonical post-pushdown state.
        let narrow_file = load_exec.projected_file_fields();
        assert_eq!(
            narrow_file,
            vec!["b".to_string()],
            "file source read schema must be narrowed to projected file columns",
        );

        let narrow_pt = load_exec.projected_passthrough_fields();
        assert_eq!(
            narrow_pt,
            vec!["p1".to_string()],
            "broadcast set must be narrowed to projected passthrough columns",
        );

        // And the data shape matches: 2 cols, ordered [b, p1], 2 rows.
        let stream = datafusion::physical_plan::execute_stream(
            projected_plan,
            Arc::new(datafusion::execution::TaskContext::default()),
        )
        .unwrap();
        let batches: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(stream)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["b", "p1"]);
    }

    /// Limit pushdown into a [`LoadTableProvider`] must (a) cap the total emitted row count
    /// across files / row groups and (b) stop opening further row groups / files once the
    /// budget is exhausted. We drive a single file containing 64 rows across 4 row groups and
    /// ask for `limit = 20`; the new streaming `LoadExec` slices the second row group and
    /// declines to open the third.
    #[tokio::test]
    async fn load_provider_limit_caps_total_streamed_rows() {
        use std::fs::File;

        use datafusion::catalog::TableProvider;
        use delta_kernel::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch};
        use delta_kernel::arrow::datatypes::{
            DataType as ArrowDataType, Field, Schema as ArrowSchema,
        };
        use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
        use delta_kernel::parquet::file::properties::WriterProperties;
        use delta_kernel::plans::ir::nodes::{FileType, ScanFileColumns};

        use crate::exec::LoadTableProvider;

        let dir = tempfile::tempdir().unwrap();
        let parquet_path = dir.path().join("rg.parquet");
        {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "x",
                ArrowDataType::Int64,
                false,
            )]));
            let batch = ArrowRecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(0..64_i64))],
            )
            .unwrap();
            let file = File::create(&parquet_path).unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(16))
                .build();
            let mut writer =
                ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let rel = parquet_path.file_name().unwrap().to_str().unwrap().to_string();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let upstream_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
        ]));
        let file_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "x",
            DataType::LONG,
        )]));

        let lit =
            PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(rel)]]).unwrap();
        let mut registry = RelationRegistry::new(Uuid::new_v4());
        let plan = lit
            .load(
                "limit_pushdown_test",
                file_schema,
                FileType::Parquet,
                Some(base_url),
                Vec::new(),
                ScanFileColumns {
                    path: delta_kernel::expressions::ColumnName::from_naive_str_split("path"),
                    size: None,
                    record_count: None,
                },
                None,
                &mut registry,
            )
            .expect("load sink");
        let SinkType::Load(load_sink) = &plan.sink else {
            unreachable!();
        };
        let handle_id = load_sink.output_relation.id.clone();

        let ex = DataFusionExecutor::try_new().unwrap();
        ex.execute_plans(&[plan]).await.unwrap();

        let provider = ex.relation_provider(&handle_id).expect("registered");
        let provider = (provider.as_ref() as &dyn TableProvider)
            .as_any()
            .downcast_ref::<LoadTableProvider>()
            .expect("LoadTableProvider");

        let state = ex.session_state();
        let limited = provider
            .scan(&state, None, &[], Some(20))
            .await
            .expect("scan with limit");

        let stream = datafusion::physical_plan::execute_stream(
            limited,
            Arc::new(datafusion::execution::TaskContext::default()),
        )
        .unwrap();
        let batches: Vec<_> = futures::TryStreamExt::try_collect::<Vec<_>>(stream)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 20,
            "limit must cap total emitted rows to exactly the budget; got {total_rows}",
        );

        // And every batch must be non-empty (no degenerate zero-row slices appended after the
        // budget hit).
        for b in &batches {
            assert!(
                b.num_rows() > 0,
                "LoadExec must not emit zero-row batches after limit slicing",
            );
        }
    }
}
