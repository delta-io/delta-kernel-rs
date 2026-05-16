//! Drain the child stream through a [`ConsumeByKdfSink`] [`ConsumerKdf`] and record the finalized
//! handle.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
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
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::plans::ir::nodes::ConsumeByKdfSink;
use delta_kernel::plans::kdf::{ConsumerKdf, FinishedHandle, Handle, KdfControl, TraceContext};
use delta_kernel::plans::state_machines::framework::phase_state::PhaseState;
use futures::{Stream, StreamExt};

use crate::error::unsupported;

pub struct KernelConsumeByKdfExec {
    child: Arc<dyn ExecutionPlan>,
    sink: Arc<Mutex<ConsumeByKdfSink>>,
    harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    phase_state: Option<PhaseState>,
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

impl KernelConsumeByKdfExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        sink: ConsumeByKdfSink,
        harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
        phase_state: Option<PhaseState>,
    ) -> Result<Self, DataFusionError> {
        if sink.requires_ordering.is_some() {
            return Err(unsupported(
                "ConsumeByKdf with requires_ordering is not implemented for the DataFusion engine",
            ));
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
            sink: Arc::new(Mutex::new(sink)),
            harvest_slot,
            phase_state,
            schema,
            properties,
        })
    }
}

impl fmt::Debug for KernelConsumeByKdfExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kdf_id = self
            .sink
            .lock()
            .map(|g| g.initial_state.kdf_id())
            .unwrap_or_default();
        f.debug_struct("KernelConsumeByKdfExec")
            .field("kdf_id", &kdf_id)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for KernelConsumeByKdfExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kdf_id = self
            .sink
            .lock()
            .map(|g| g.initial_state.kdf_id())
            .unwrap_or_default();
        write!(f, "KernelConsumeByKdfExec(kdf_id={})", kdf_id)
    }
}

impl ExecutionPlan for KernelConsumeByKdfExec {
    fn name(&self) -> &str {
        "KernelConsumeByKdfExec"
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
        let child = super::expect_single_child(children, "KernelConsumeByKdfExec")?;
        Ok(Arc::new(KernelConsumeByKdfExec::try_new(
            child,
            self.sink
                .lock()
                .map_err(|_| {
                    DataFusionError::Internal(
                        "KernelConsumeByKdfExec: mutex poisoned rebuilding children".into(),
                    )
                })?
                .clone(),
            Arc::clone(&self.harvest_slot),
            self.phase_state.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "KernelConsumeByKdfExec supports partition 0 only, got {partition}",
            )));
        }

        let handle = self
            .sink
            .lock()
            .map_err(|_| {
                DataFusionError::Internal(
                    "KernelConsumeByKdfExec: mutex poisoned at execute".into(),
                )
            })?
            .new_handle(
                TraceContext::new("datafusion-engine", "execute"),
                partition as u32,
            );

        let inner = self.child.execute(partition, context)?;
        Ok(Box::pin(ConsumeKdfStream {
            schema: self.schema.clone(),
            inner: Some(inner),
            handle: Some(handle),
            harvest_slot: Arc::clone(&self.harvest_slot),
            phase_state: self.phase_state.clone(),
            finished: false,
        }))
    }
}

struct ConsumeKdfStream {
    schema: delta_kernel::arrow::datatypes::SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    handle: Option<Handle<dyn ConsumerKdf>>,
    harvest_slot: Arc<Mutex<Option<FinishedHandle>>>,
    phase_state: Option<PhaseState>,
    finished: bool,
}

impl ConsumeKdfStream {
    fn finalize(&mut self) {
        if self.finished {
            return;
        }
        self.inner.take();
        if let Some(h) = self.handle.take() {
            let done = h.finish();
            if let Some(state) = self.phase_state.as_ref() {
                state.submit_kdf_handle(done);
            } else {
                let mut guard = self.harvest_slot.lock().unwrap_or_else(|e| e.into_inner());
                *guard = Some(done);
            }
        }
        self.finished = true;
    }
}

impl Drop for ConsumeKdfStream {
    fn drop(&mut self) {
        self.finalize();
    }
}

impl Stream for ConsumeKdfStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            let Some(inner) = self.inner.as_mut() else {
                self.finalize();
                return Poll::Ready(None);
            };

            match inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let arrow = ArrowEngineData::new(batch);
                    let Some(handle_mut) = self.handle.as_mut() else {
                        self.finalize();
                        return Poll::Ready(None);
                    };
                    match handle_mut.apply_consumer(&arrow) {
                        Ok(KdfControl::Continue) => {}
                        Ok(KdfControl::Break) => {
                            self.finalize();
                            return Poll::Ready(None);
                        }
                        Err(e) => {
                            self.finalize();
                            return Poll::Ready(Some(Err(crate::error::wrap_delta_err(e))));
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    self.finalize();
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.finalize();
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for ConsumeKdfStream {
    fn schema(&self) -> delta_kernel::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}
