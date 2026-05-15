//! Row-level [`AssertNode`] enforcement: predicates must be true per row; NULL fails.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{Array, AsArray, BooleanArray, RecordBatch};
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::plans::ir::nodes::AssertCheck;
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{EvaluationHandler, ExpressionEvaluator};
use futures::{Stream, StreamExt};

#[derive(Clone)]
struct CompiledAssertCheck {
    evaluator: Arc<dyn ExpressionEvaluator>,
    error_code: String,
    error_message: String,
}

pub struct KernelAssertExec {
    child: Arc<dyn ExecutionPlan>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    checks: Vec<CompiledAssertCheck>,
    properties: Arc<PlanProperties>,
}

impl KernelAssertExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        checks: &[AssertCheck],
    ) -> Result<Self, delta_kernel::plans::errors::DeltaError> {
        let mut compiled = Vec::with_capacity(checks.len());
        for c in checks {
            let evaluator = ArrowEvaluationHandler
                .new_expression_evaluator(
                    input_schema.clone(),
                    c.predicate.clone(),
                    DataType::BOOLEAN,
                )
                .map_err(|e| crate::error::internal_error(format!("assert evaluator init: {e}")))?;
            compiled.push(CompiledAssertCheck {
                evaluator,
                error_code: c.error_code.clone(),
                error_message: c.error_message.clone(),
            });
        }

        let schema = child.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            child,
            schema,
            checks: compiled,
            properties,
        })
    }
}

impl fmt::Debug for KernelAssertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KernelAssertExec")
            .field("checks", &self.checks.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for KernelAssertExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KernelAssertExec({} checks)", self.checks.len())
    }
}

impl ExecutionPlan for KernelAssertExec {
    fn name(&self) -> &str {
        "KernelAssertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let child = super::expect_single_child(children, "KernelAssertExec")?;
        Ok(Arc::new(Self {
            schema: self.schema.clone(),
            checks: self.checks.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                child.properties().output_partitioning().clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            child,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        Ok(Box::pin(KernelAssertStream {
            input,
            schema: self.schema.clone(),
            checks: self.checks.clone(),
        }))
    }
}

struct KernelAssertStream {
    input: SendableRecordBatchStream,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    checks: Vec<CompiledAssertCheck>,
}

impl Stream for KernelAssertStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() == 0 || self.checks.is_empty() {
                    return Poll::Ready(Some(Ok(batch)));
                }

                let batch_for_eval = ArrowEngineData::new(batch.clone());

                let mut predicate_cols: Vec<BooleanArray> = Vec::with_capacity(self.checks.len());
                for check in &self.checks {
                    let pred_data = match check.evaluator.evaluate(&batch_for_eval) {
                        Ok(v) => v,
                        Err(e) => {
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(e))));
                        }
                    };
                    let pred_batch = match pred_data.try_into_record_batch() {
                        Ok(rb) => rb,
                        Err(e) => {
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(e))));
                        }
                    };
                    predicate_cols.push(pred_batch.column(0).as_boolean().clone());
                }

                let num_rows = batch.num_rows();
                for row in 0..num_rows {
                    for (check_idx, predicate) in predicate_cols.iter().enumerate() {
                        let failed_null = predicate.is_null(row);
                        let failed_false = !failed_null && !predicate.value(row);
                        if failed_null || failed_false {
                            let check = &self.checks[check_idx];
                            let delta_err = crate::error::assert_violation(
                                &check.error_code,
                                &check.error_message,
                                row,
                                failed_null,
                            );
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(delta_err))));
                        }
                    }
                }

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for KernelAssertStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
