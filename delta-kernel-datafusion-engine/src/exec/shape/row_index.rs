//! Fallback row-index injection for formats without native decoder support (JSON today).
//!
//! [`RowIndexExec`] assigns contiguous `0..` indices within **each execution partition stream**
//! emitted by its child. Multi-file scans that need per-file resets therefore compile as one scan
//! arm per file under [`crate::exec::OrderedUnionExec`] whenever `ScanNode::row_index_column` is
//! set (even when `ScanNode::ordered` is `false`; see [`crate::compile::scan::compile_scan`]).
//!
//! **Limitation:** A single physical file scanned through multiple concurrent partitions could
//! yield duplicate indices across partitions; Parquet scans prefer native [`RowNumber`] columns
//! instead.
//!
//! [`RowNumber`]: parquet::arrow::RowNumber

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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use futures::{Stream, StreamExt};

/// Appends a synthetic `INT64` row-index column by counting rows along each output partition
/// stream.
#[derive(Debug)]
pub struct RowIndexExec {
    child: Arc<dyn ExecutionPlan>,
    row_index_col: String,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RowIndexExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, row_index_col: String) -> Self {
        let mut fields: Vec<Arc<Field>> = child.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(&row_index_col, DataType::Int64, false)));
        let schema = Arc::new(ArrowSchema::new(fields));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child,
            row_index_col,
            schema,
            properties,
        }
    }
}

impl DisplayAs for RowIndexExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowIndexExec(col={})", self.row_index_col)
    }
}

impl ExecutionPlan for RowIndexExec {
    fn name(&self) -> &str {
        "RowIndexExec"
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
        let child = super::expect_single_child(children, "RowIndexExec")?;
        Ok(Arc::new(Self::new(child, self.row_index_col.clone())))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.child.execute(partition, context)?;
        Ok(Box::pin(RowIndexStream {
            schema: self.schema.clone(),
            input,
            current_index: 0,
        }))
    }
}

struct RowIndexStream {
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    input: SendableRecordBatchStream,
    current_index: i64,
}

impl Stream for RowIndexStream {
    type Item = DfResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let n = batch.num_rows() as i64;
                let vals: Vec<i64> = (0..n).map(|i| self.current_index + i).collect();
                self.current_index += n;
                let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
                cols.push(Arc::new(Int64Array::from(vals)));
                let out = RecordBatch::try_new(self.schema.clone(), cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                Poll::Ready(Some(out))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for RowIndexStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
