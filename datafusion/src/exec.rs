use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_common::HashMap;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, Statistics};
use delta_kernel::arrow::array::{AsArray, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::SchemaRef;
use futures::stream::{Stream, StreamExt};

pub(crate) const FILE_ID_COLUMN: &str = "file_id";

#[derive(Clone, Debug)]
pub struct DeltaScanExec {
    /// Output schema for processed data.
    schema: SchemaRef,
    /// Execution plan yielding the raw data read from data files.
    input: Arc<dyn ExecutionPlan>,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, PhysicalExprRef>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for DeltaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // TODO: actually implement formatting according to the type
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "KernelTransformExec: ")
            }
        }
    }
}

impl DeltaScanExec {
    pub(crate) fn new(
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        transforms: Arc<HashMap<String, PhysicalExprRef>>,
    ) -> Self {
        Self {
            schema,
            input,
            transforms,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for DeltaScanExec {
    fn name(&self) -> &'static str {
        "DeltaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        // TODO: check individual properties and see if it is correct
        // to just forward them
        &self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // fn maintains_input_order(&self) -> Vec<bool> {
    //     // Tell optimizer this operator doesn't reorder its input
    //     vec![true]
    // }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    // fn benefits_from_input_partitioning(&self) -> Vec<bool> {
    //     let all_simple_exprs = self
    //         .expr
    //         .iter()
    //         .all(|(e, _)| e.as_any().is::<Column>() || e.as_any().is::<Literal>());
    //     // If expressions are all either column_expr or Literal, then all computations in this projection are reorder or rename,
    //     // and projection would not benefit from the repartition, benefits_from_input_partitioning will return false.
    //     vec![!all_simple_exprs]
    // }

    /// Redistribute files across partitions within the underlying
    /// [`ParquetExec`] according to their size.
    ///
    /// [ParquetExec]: datafusion::datasource::physical_plan::ParquetExec
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(new_pq) = self.input.repartitioned(target_partitions, config)? {
            let mut new_plan = self.clone();
            new_plan.input = new_pq;
            Ok(Some(Arc::new(new_plan)))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(DeltaScanStream {
            schema: Arc::clone(&self.schema),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            transforms: Arc::clone(&self.transforms),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(new_input) = self.input.with_fetch(limit) {
            let mut new_plan = self.clone();
            new_plan.input = new_input;
            Some(Arc::new(new_plan))
        } else {
            None
        }
    }
}

struct DeltaScanStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    /// Transforms to be applied to data eminating from individual files
    transforms: Arc<HashMap<String, PhysicalExprRef>>,
}

impl DeltaScanStream {
    fn batch_project(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let file_id_idx = batch
            .schema_ref()
            .fields()
            .iter()
            .position(|f| f.name() == FILE_ID_COLUMN)
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Expected column 'file_id' to be present in the input".to_string(),
                )
            })?;

        let file_id = batch
            .column(file_id_idx)
            .as_string::<i32>()
            .value(0)
            .to_string();
        batch.remove_column(file_id_idx);

        let Some(transform) = self.transforms.get(&file_id) else {
            return Ok(batch);
        };

        let ColumnarValue::Array(logical) = transform.evaluate(&batch)? else {
            return Err(DataFusionError::Internal(
                "Expected transformation result to be an array".to_string(),
            ));
        };

        Ok(logical
            .as_struct_opt()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Expected transformation result to be a struct".to_string(),
                )
            })?
            .into())
    }
}

impl Stream for DeltaScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(batch)),
            other => other,
        });
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for DeltaScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
