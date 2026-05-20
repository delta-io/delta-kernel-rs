//! DataFusion execution scaffold for Delta Kernel declarative [`delta_kernel::plans::ir::Plan`]
//! trees.
//!
//! Supported sinks (registration is lazy; no I/O until the consumer reads):
//! - `Relation` -- registers a [`ViewTable`](datafusion::datasource::ViewTable) over the upstream
//!   `LogicalPlan`; DataFusion's `InlineTableScan` rule inlines it so pushdown and CSE cross plan
//!   boundaries.
//! - `Load` -- registers a [`LoadTableProvider`](exec::LoadTableProvider) whose `scan()` yields a
//!   [`LoadExec`](exec::LoadExec) streaming per-row file batches.
//! - `Consume` -- the only sink with eager side effects: drains the physical plan into a
//!   [`delta_kernel::plans::kdf::ConsumerKdf`] handle at execute-plan time.
//!
//! Unsupported constructs surface via [`error::unsupported`]; the engine -> kernel boundary
//! methods on [`DataFusionExecutor`] translate that into a
//! [`delta_kernel::plans::errors::DeltaError`].

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;
#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

pub use executor::DataFusionExecutor;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::Scalar;
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

    /// `Relation` sinks must register a *lazy* `ViewTable` -- not an eagerly-materialized
    /// `MemTable`. Downcasting the provider returned by `relation_provider` proves both that the
    /// upstream `LogicalPlan` round-trips into the registry and that no eager `collect` path
    /// remains.
    #[tokio::test]
    async fn relation_sink_registers_view_table_not_memtable() {
        use datafusion::datasource::{MemTable, ViewTable};

        let ex = DataFusionExecutor::try_new().unwrap();
        let schema = two_bool_schema();
        let mut registry = RelationRegistry::new(Uuid::new_v4(), "");
        let handle = PlanBuilder::values(
            Arc::clone(&schema),
            vec![vec![Scalar::Boolean(true), Scalar::Boolean(false)]],
        )
        .unwrap()
        .into_relation("lazy_view_test", &mut registry)
        .expect("relation sink");

        ex.execute_plans(&registry.take_plans()).await.unwrap();

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
        let batches = crate::testing::collect_relation(&ex, &handle)
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
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
        let rel = parquet_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();

        // Upstream relation carries `path` + two passthrough columns (`p1`, `p2`). The Load
        // sink reads `[a, b]` from each file and broadcasts both passthrough columns.
        let upstream_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::nullable("p1", DataType::LONG),
            StructField::nullable("p2", DataType::LONG),
        ]));
        let file_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::LONG),
            StructField::not_null("b", DataType::LONG),
        ]));

        let lit = PlanBuilder::values(
            upstream_schema,
            vec![vec![Scalar::String(rel), Scalar::Long(7), Scalar::Long(70)]],
        )
        .unwrap();
        let mut registry = RelationRegistry::new(Uuid::new_v4(), "");
        let handle = lit
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

        let ex = DataFusionExecutor::try_new().unwrap();
        ex.execute_plans(&registry.take_plans()).await.unwrap();

        let provider = ex
            .relation_provider(handle.id.as_str())
            .expect("registered");
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
            let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let rel = parquet_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let base_url = url::Url::from_directory_path(dir.path()).unwrap();
        let upstream_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "path",
            DataType::STRING,
        )]));
        let file_schema = Arc::new(StructType::new_unchecked([StructField::not_null(
            "x",
            DataType::LONG,
        )]));

        let lit = PlanBuilder::values(upstream_schema, vec![vec![Scalar::String(rel)]]).unwrap();
        let mut registry = RelationRegistry::new(Uuid::new_v4(), "");
        let handle = lit
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

        let ex = DataFusionExecutor::try_new().unwrap();
        ex.execute_plans(&registry.take_plans()).await.unwrap();

        let provider = ex
            .relation_provider(handle.id.as_str())
            .expect("registered");
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
