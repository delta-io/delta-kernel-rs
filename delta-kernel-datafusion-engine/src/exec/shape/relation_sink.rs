//! Materialize all batches from the child plan into a [`RelationBatchRegistry`] under the sink
//! handle id.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use delta_kernel::arrow::array::RecordBatch;
use futures::{Stream, StreamExt};

use crate::exec::RelationBatchRegistry;

#[derive(Debug)]
pub struct RelationSinkExec {
    child: Arc<dyn ExecutionPlan>,
    handle_id: u64,
    registry: Arc<RelationBatchRegistry>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RelationSinkExec {
    pub fn new(
        child: Arc<dyn ExecutionPlan>,
        handle_id: u64,
        registry: Arc<RelationBatchRegistry>,
    ) -> Self {
        let schema = child.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            // This sink is executed from partition 0 only; it drains every child partition
            // internally and materializes to the relation registry partition-by-partition.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child,
            handle_id,
            registry,
            schema,
            properties,
        }
    }
}

impl DisplayAs for RelationSinkExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelationSinkExec(id={})", self.handle_id)
    }
}

impl ExecutionPlan for RelationSinkExec {
    fn name(&self) -> &str {
        "RelationSinkExec"
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
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "RelationSinkExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(RelationSinkExec::new(
            Arc::clone(&children[0]),
            self.handle_id,
            Arc::clone(&self.registry),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "RelationSinkExec supports partition 0 only, got {partition}",
            )));
        }
        let mut inners = Vec::new();
        for child_partition in 0..self.child.output_partitioning().partition_count() {
            inners.push(self.child.execute(child_partition, Arc::clone(&context))?);
        }
        let partitioned_batches = (0..inners.len()).map(|_| Vec::new()).collect();
        Ok(Box::pin(RelationSinkStream {
            inners,
            current_inner: 0,
            partitioned_batches,
            handle_id: self.handle_id,
            registry: Arc::clone(&self.registry),
            schema: self.schema.clone(),
            registered: false,
        }))
    }
}

struct RelationSinkStream {
    inners: Vec<SendableRecordBatchStream>,
    current_inner: usize,
    partitioned_batches: Vec<Vec<RecordBatch>>,
    handle_id: u64,
    registry: Arc<RelationBatchRegistry>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    registered: bool,
}

impl Stream for RelationSinkStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.registered {
            return Poll::Ready(None);
        }
        loop {
            if self.current_inner >= self.inners.len() {
                let partitions = std::mem::take(&mut self.partitioned_batches);
                self.registry
                    .register_partitions(self.handle_id, partitions);
                self.registered = true;
                return Poll::Ready(None);
            }
            let idx = self.current_inner;
            match self.inners[idx].poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.partitioned_batches[idx].push(batch);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.current_inner += 1;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for RelationSinkStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
