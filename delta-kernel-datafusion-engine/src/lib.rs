//! DataFusion execution scaffold for kernel [`delta_kernel::plans::ir::Plan`] trees. See
//! [`DataFusionExecutor`] and [`exec`] / [`compile`] modules.

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

    /// `Relation` sinks must register a lazy `ViewTable` (not an eager `MemTable`).
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
            .expect("relation sink should register a provider");
        assert!(
            provider.as_ref().downcast_ref::<ViewTable>().is_some(),
            "expected ViewTable, got {provider:?}",
        );
        assert!(provider.as_ref().downcast_ref::<MemTable>().is_none());

        let batches = crate::testing::collect_relation(&ex, &handle)
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    /// Projection pushdown through a Load provider narrows both the file read schema and the
    /// passthrough broadcast set; both eager and streaming dispatch must produce the same
    /// projected output.
    #[tokio::test]
    async fn load_provider_projection_narrows_read_and_passthrough() {
        use datafusion::catalog::TableProvider;
        use delta_kernel::plans::ir::nodes::{FileType, ScanFileColumns};

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

        // Output schema is `[a, b, p1, p2]`; projecting `[b, p1]` should yield 2 cols, 2 rows.
        let provider = ex
            .relation_provider(handle.id.as_str())
            .expect("registered");
        let state = ex.session_state();
        let projected_plan = (provider.as_ref() as &dyn TableProvider)
            .scan(&state, Some(&vec![1usize, 2usize]), &[], None)
            .await
            .expect("scan");

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

    /// Limit pushdown caps the total emitted row count and stops opening further row groups
    /// once the budget is exhausted. 64 rows across 4 row groups, `limit = 20`.
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
        let state = ex.session_state();
        let limited = (provider.as_ref() as &dyn TableProvider)
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
            "limit must cap rows exactly; got {total_rows}"
        );
        for b in &batches {
            assert!(b.num_rows() > 0, "no zero-row slices after limit");
        }
    }
}
