//! DataFusion execution scaffold for Delta Kernel declarative [`Plan`] trees.
//!
//! Supported sinks include [`delta_kernel::plans::ir::nodes::SinkType::Results`] (stream batches to
//! the caller), [`SinkType::Relation`](delta_kernel::plans::ir::nodes::SinkType::Relation)
//! (materialize into an in-memory registry after draining),
//! [`SinkType::ConsumeByKdf`](delta_kernel::plans::ir::nodes::SinkType::ConsumeByKdf) (observe
//! batches via a [`delta_kernel::plans::kdf::ConsumerKdf`]),
//! [`SinkType::Write`](delta_kernel::plans::ir::nodes::SinkType::Write) (single-target Parquet /
//! newline-delimited JSON via DataFusion file sinks when the runtime provides object-store access
//! for the destination URL),
//! and [`SinkType::PartitionedWrite`](delta_kernel::plans::ir::nodes::SinkType::PartitionedWrite)
//! (Hive-style directories under a destination prefix),
//! and [`SinkType::Load`](delta_kernel::plans::ir::nodes::SinkType::Load) (per-row parquet or JSON
//! reads via kernel parquet/json handlers into the relation registry).
//! Unsupported constructs still return
//! [`delta_kernel::plans::errors::DeltaError`] via [`error::unsupported`].

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
    use delta_kernel::plans::errors::DeltaErrorCode;
    use delta_kernel::plans::ir::nodes::{AssertCheck, RelationHandle};
    use delta_kernel::plans::ir::DeclarativePlanNode;
    use delta_kernel::plans::state_machines::framework::phase_operation::PhaseOperation;
    use delta_kernel::schema::{DataType, StructField, StructType};
    use futures::TryStreamExt;

    use crate::error::LiftDeltaErr;
    use crate::DataFusionExecutor;

    fn bool_schema() -> delta_kernel::schema::SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "ok",
            DataType::BOOLEAN,
        )]))
    }

    fn two_bool_schema() -> delta_kernel::schema::SchemaRef {
        Arc::new(StructType::new_unchecked([
            StructField::not_null("a", DataType::BOOLEAN),
            StructField::not_null("b", DataType::BOOLEAN),
        ]))
    }

    #[tokio::test]
    async fn assert_passes_when_predicate_true() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let plan = DeclarativePlanNode::values_row(bool_schema(), vec![Scalar::Boolean(true)])
            .unwrap()
            .assert(vec![AssertCheck {
                predicate: Arc::new(Expression::literal(true)),
                error_code: "SHOULD_NOT_FIRE".into(),
                error_message: "unexpected".into(),
            }])
            .into_results();

        let batches = ex.execute_plan_collect(plan).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn assert_accepts_empty_checks() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let plan = DeclarativePlanNode::values_row(bool_schema(), vec![Scalar::Boolean(false)])
            .unwrap()
            .assert(vec![])
            .into_results();

        let batches = ex.execute_plan_collect(plan).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn assert_fails_when_predicate_false() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let plan = DeclarativePlanNode::values_row(bool_schema(), vec![Scalar::Boolean(true)])
            .unwrap()
            .assert(vec![AssertCheck {
                predicate: Arc::new(Expression::literal(false)),
                error_code: "MY_CODE_FALSE".into(),
                error_message: "human readable false".into(),
            }])
            .into_results();

        let stream = ex.execute_plan_to_stream(plan).await.unwrap();
        let err = stream.try_collect::<Vec<_>>().await.lift().unwrap_err();
        assert_eq!(err.code, DeltaErrorCode::DeltaCommandInvariantViolation);
        let rendered = err.rendered_message();
        assert!(
            rendered.contains("human readable false"),
            "expected IR message verbatim in rendered output: {rendered}"
        );
        assert!(
            rendered.contains("MY_CODE_FALSE"),
            "expected stable code in rendered output: {rendered}"
        );
        assert!(
            rendered.contains("predicate was false"),
            "expected null/false hint: {rendered}"
        );
    }

    #[tokio::test]
    async fn assert_fails_when_predicate_null() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            "flag",
            DataType::BOOLEAN,
        )]));
        let plan = DeclarativePlanNode::values_row(schema, vec![Scalar::Null(DataType::BOOLEAN)])
            .unwrap()
            .assert(vec![AssertCheck {
                predicate: Arc::new(Expression::column(["flag"])),
                error_code: "NULL_PRED".into(),
                error_message: "flag must be known".into(),
            }])
            .into_results();

        let stream = ex.execute_plan_to_stream(plan).await.unwrap();
        let err = stream.try_collect::<Vec<_>>().await.lift().unwrap_err();
        let rendered = err.rendered_message();
        assert!(
            rendered.contains("predicate was NULL"),
            "NULL predicate must fail per IR: {rendered}"
        );
        assert!(rendered.contains("NULL_PRED"));
        assert!(rendered.contains("flag must be known"));
    }

    #[tokio::test]
    async fn assert_first_failing_check_wins_per_row() {
        let ex = DataFusionExecutor::try_new().unwrap();
        let plan = DeclarativePlanNode::values(
            two_bool_schema(),
            vec![vec![Scalar::Boolean(false), Scalar::Boolean(false)]],
        )
        .unwrap()
        .assert(vec![
            AssertCheck {
                predicate: Arc::new(Expression::column(["a"])),
                error_code: "FIRST".into(),
                error_message: "first check".into(),
            },
            AssertCheck {
                predicate: Arc::new(Expression::column(["b"])),
                error_code: "SECOND".into(),
                error_message: "second check".into(),
            },
        ])
        .into_results();

        let stream = ex.execute_plan_to_stream(plan).await.unwrap();
        let err = stream.try_collect::<Vec<_>>().await.lift().unwrap_err();
        let rendered = err.rendered_message();
        assert!(
            rendered.contains("FIRST"),
            "expected earlier declared check to fail first: {rendered}"
        );
        assert!(!rendered.contains("SECOND"));
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
    async fn logical_relation_sink_materializes_into_registry() {
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
            .relation_batch_registry()
            .get_cloned(handle.id)
            .expect("relation sink should materialize output batches");
        assert_eq!(relation_batches.len(), 1);
        assert_eq!(relation_batches[0].num_rows(), 1);
        assert_eq!(relation_batches[0].num_columns(), 2);
    }
}
