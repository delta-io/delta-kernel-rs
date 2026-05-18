//! DataFusion execution scaffold for Delta Kernel declarative [`Plan`] trees.
//!
//! Supported sinks: [`SinkType::Relation`](delta_kernel::plans::ir::nodes::SinkType::Relation)
//! / [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) (materialize batches
//! as a [`MemTable`](datafusion::datasource::MemTable) registered in the executor's
//! [`SessionContext`](datafusion::execution::context::SessionContext) so downstream
//! [`RelationRef`](delta_kernel::plans::ir::DeclarativePlanNode::RelationRef) leaves can scan
//! it), and
//! [`SinkType::Consume`](delta_kernel::plans::ir::nodes::SinkType::Consume) (observe
//! batches via a [`delta_kernel::plans::kdf::ConsumerKdf`]). Read-style state machines return
//! a [`delta_kernel::plans::ir::ResultPlan`] naming the relation the caller reads after
//! executing the result plan's plans.
//!
//! Sinks are annotations on the kernel [`Plan`](delta_kernel::plans::ir::Plan), not envelopes
//! on the DataFusion physical plan; the executor handles each sink type as a post-drain side
//! effect. Unsupported constructs surface a
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
}
