//! DataFusion execution scaffold for Delta Kernel declarative [`Plan`] trees.
//!
//! Supported sinks include [`delta_kernel::plans::ir::nodes::SinkType::Results`] (stream batches to
//! the caller), [`SinkType::Relation`](delta_kernel::plans::ir::nodes::SinkType::Relation) /
//! [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) (materialize batches as a
//! [`MemTable`](datafusion::datasource::MemTable) registered in the executor's
//! [`SessionContext`](datafusion::execution::context::SessionContext) so downstream
//! [`RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaves can scan
//! it), and
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) (observe
//! batches via a [`delta_kernel::plans::kdf::ConsumerKdf`]). Unsupported constructs still
//! return [`delta_kernel::plans::errors::DeltaError`] via [`error::unsupported`].

pub mod compile;
pub mod error;
pub mod exec;
pub mod executor;

pub use error::{datafusion_err_to_delta, LiftDeltaErr};
pub use executor::DataFusionExecutor;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::expressions::{Expression, Scalar};
    use delta_kernel::plans::ir::nodes::RelationHandle;
    use delta_kernel::plans::ir::DeclarativePlanNode;
    use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
    use delta_kernel::schema::{DataType, StructField, StructType};

    use crate::DataFusionExecutor;

    fn two_bool_schema() -> delta_kernel::schema::SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::BOOLEAN),
            StructField::not_null("b", DataType::BOOLEAN),
        ]))
    }

    #[tokio::test]
    async fn logical_results_sink_streams_batches() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let plan = DeclarativePlanNode::values(
            two_bool_schema(),
            vec![vec![Scalar::Boolean(true), Scalar::Boolean(false)]],
        )
        .unwrap()
        .project(
            vec![
                Arc::new(Expression::column(["a"])),
                Arc::new(Expression::column(["b"])),
            ],
            two_bool_schema(),
        )
        .into_results();
        let batches = ex.execute_plan_collect(plan).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn logical_relation_sink_materializes_into_session_ctx() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let schema = two_bool_schema();
        let handle = RelationHandle::fresh("logical_relation_test", Arc::clone(&schema));
        let plan = DeclarativePlanNode::values(
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
        .into_relation(handle.clone());

        ex.execute_phase_operation(PhaseOperation::Plans(vec![plan]))
            .await
            .unwrap();
        let relation_batches = ex
            .collect_relation(&handle)
            .await
            .expect("relation sink should materialize output batches");
        assert_eq!(relation_batches.len(), 1);
        assert_eq!(relation_batches[0].num_rows(), 1);
        assert_eq!(relation_batches[0].num_columns(), 2);
    }
}
