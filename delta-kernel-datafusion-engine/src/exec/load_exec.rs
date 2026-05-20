//! Streaming physical plan behind [`super::LoadTableProvider`].
//!
//! For each upstream metadata row, [`build_load_stream`] runs `buffer_unordered` over open
//! futures: each resolves the optional DV via [`resolve_dv_async`], builds a per-file plan
//! via [`build_per_file_plan`] (`DataSourceExec` plus an optional `FilterExec(not_in_dv)`
//! → `ProjectionExec` stack when DV is present), and drains it. Output ordering across files
//! is unspecified; intra-file order is preserved. JSON+DV is rejected at construction.

use std::fmt;
use std::sync::Arc;

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::Engine;
use futures::stream::{Stream, StreamExt, TryStreamExt};

use crate::exec::load_helpers::{
    build_file_source, build_per_file_plan, extract_row_inputs, resolve_dv_async, sink_base_url,
    RowInputs,
};

/// Per-partition file-open concurrency when `target_partitions` is zero.
const DEFAULT_LOAD_CONCURRENCY: usize = 8;

/// Caps tokio task pressure for very-wide scans.
const MAX_LOAD_CONCURRENCY: usize = 64;

pub struct LoadExec {
    sink: Arc<LoadSink>,
    /// Used only for deletion-vector resolution; file decoding goes through DataFusion's
    /// parquet/json sources, not this engine.
    engine: Arc<dyn Engine>,
    upstream: Arc<dyn ExecutionPlan>,
    /// Pre-projection schema (= file_schema fields ++ passthrough fields). Kept so
    /// `with_new_children` can rebuild against the same shape.
    full_schema: ArrowSchemaRef,
    projection: Option<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    limit: Option<usize>,
    /// Indices into `sink.passthrough_columns` to materialize, in projected order. `Arc` so
    /// per-row open futures can clone cheaply.
    projected_passthrough: Arc<Vec<usize>>,
    /// File source without `_row_number` for rows whose DV column is null (or whole sink
    /// has no DV).
    file_source_no_dv: Arc<dyn datafusion_datasource::file::FileSource>,
    /// File source with `_row_number` virtual column appended for DV rows. `None` iff
    /// `sink.dv_ref.is_none()`.
    file_source_with_dv: Option<Arc<dyn datafusion_datasource::file::FileSource>>,
    properties: Arc<PlanProperties>,
}

impl LoadExec {
    pub fn new(
        upstream: Arc<dyn ExecutionPlan>,
        sink: Arc<LoadSink>,
        engine: Arc<dyn Engine>,
        full_schema: ArrowSchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> DfResult<Self> {
        // JSON has no `_row_number` virtual column; delta DVs only apply to parquet anyway.
        if matches!(sink.file_type, FileType::Json) && sink.dv_ref.is_some() {
            return Err(crate::error::plan_compilation(
                "LoadSink with FileType::Json and dv_ref set is not supported (no \
                 _row_number virtual column for JSON)",
            ));
        }

        let file_count = sink.file_schema.fields().len();
        let passthrough_count = sink.passthrough_columns.len();
        debug_assert_eq!(full_schema.fields().len(), file_count + passthrough_count);

        // Always build the no-DV variant: even when `sink.dv_ref` is set, individual rows may
        // have a NULL DV (no DV for that file).
        let file_source_no_dv = build_file_source(
            sink.file_type,
            &full_schema,
            file_count,
            projection.as_deref(),
            false,
        )?;
        let file_source_with_dv = sink
            .dv_ref
            .is_some()
            .then(|| {
                build_file_source(
                    sink.file_type,
                    &full_schema,
                    file_count,
                    projection.as_deref(),
                    true,
                )
            })
            .transpose()?;

        let output_schema = match projection.as_ref() {
            Some(proj) => Arc::new(full_schema.project(proj)?),
            None => Arc::clone(&full_schema),
        };

        // Filter projection indices to passthrough range (>= file_count) and translate to
        // passthrough-local indices.
        let projected_passthrough: Vec<usize> = match projection.as_ref() {
            Some(proj) => proj
                .iter()
                .copied()
                .filter(|&i| i >= file_count)
                .map(|i| i - file_count)
                .collect(),
            None => (0..passthrough_count).collect(),
        };

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            // Single partition: the merger interleaves files within one stream.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            upstream.properties().boundedness,
        ));
        Ok(Self {
            sink,
            engine,
            upstream,
            full_schema,
            projection,
            output_schema,
            limit,
            projected_passthrough: Arc::new(projected_passthrough),
            file_source_no_dv,
            file_source_with_dv,
            properties,
        })
    }
}

impl fmt::Debug for LoadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadExec")
            .field("output_relation", &self.sink.output_relation.name)
            .field("file_type", &self.sink.file_type)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .field("output_fields", &self.output_schema.fields().len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LoadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LoadExec(output_relation={}, file_type={:?}, projection={:?}, limit={:?}, \
             output_fields={})",
            self.sink.output_relation.name,
            self.sink.file_type,
            self.projection,
            self.limit,
            self.output_schema.fields().len(),
        )
    }
}

impl ExecutionPlan for LoadExec {
    fn name(&self) -> &str {
        "LoadExec"
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.upstream]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr,
        ) -> DfResult<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> DfResult<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [upstream] = children.try_into().map_err(|c: Vec<_>| {
            DataFusionError::Plan(format!(
                "LoadExec requires exactly one child, got {}",
                c.len()
            ))
        })?;
        Ok(Arc::new(LoadExec::new(
            upstream,
            Arc::clone(&self.sink),
            Arc::clone(&self.engine),
            Arc::clone(&self.full_schema),
            self.projection.clone(),
            self.limit,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "LoadExec only supports partition 0, got {partition}"
            )));
        }
        // Coalesce the upstream's partitions into one stream so the row expander sees one
        // batch at a time.
        let upstream = datafusion::physical_plan::execute_stream(
            Arc::clone(&self.upstream),
            Arc::clone(&context),
        )?;
        let concurrency = context
            .session_config()
            .target_partitions()
            .clamp(1, MAX_LOAD_CONCURRENCY);
        let concurrency = if concurrency == 0 {
            DEFAULT_LOAD_CONCURRENCY
        } else {
            concurrency
        };
        let stream = build_load_stream(
            upstream,
            Arc::clone(&self.sink),
            Arc::clone(&self.engine),
            Arc::clone(&self.file_source_no_dv),
            self.file_source_with_dv.as_ref().map(Arc::clone),
            Arc::clone(&self.projected_passthrough),
            Arc::clone(&self.output_schema),
            context,
            self.limit,
            concurrency,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }
}

/// Up to `concurrency` per-row open futures run at once via `buffer_unordered`; the outer
/// `try_flatten` interleaves files freely while preserving intra-file batch order. `limit`
/// is enforced by slicing the final batch + early-terminating.
#[allow(clippy::too_many_arguments)]
fn build_load_stream(
    upstream: SendableRecordBatchStream,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    file_source_no_dv: Arc<dyn datafusion_datasource::file::FileSource>,
    file_source_with_dv: Option<Arc<dyn datafusion_datasource::file::FileSource>>,
    projected_passthrough: Arc<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    task_context: Arc<TaskContext>,
    limit: Option<usize>,
    concurrency: usize,
) -> impl Stream<Item = DfResult<RecordBatch>> + Send + 'static {
    // Explode upstream batches into one item per row.
    let row_stream = upstream
        .map_ok(|batch| {
            let n = batch.num_rows();
            let batch = Arc::new(batch);
            futures::stream::iter((0..n).map(move |row| DfResult::Ok((Arc::clone(&batch), row))))
        })
        .try_flatten();

    // Per row, an open future producing the per-file `RecordBatch` stream.
    let per_file_streams = row_stream.map(move |row_result: DfResult<_>| {
        let sink = Arc::clone(&sink);
        let engine = Arc::clone(&engine);
        let task_ctx = Arc::clone(&task_context);
        let pt = Arc::clone(&projected_passthrough);
        let file_source_no_dv = Arc::clone(&file_source_no_dv);
        let file_source_with_dv = file_source_with_dv.as_ref().map(Arc::clone);
        let output_schema = Arc::clone(&output_schema);

        async move {
            let (batch, row) = row_result?;
            let inputs: RowInputs = extract_row_inputs(&batch, row, &sink, &pt)?;

            let dv = match inputs.dv_descriptor.clone() {
                Some(desc) => {
                    let base = sink_base_url(&sink)?.clone();
                    Some(resolve_dv_async(desc, base, Arc::clone(&engine)).await?)
                }
                None => None,
            };

            // `file_source_with_dv` is `Some` iff `sink.dv_ref.is_some()`, which is the only
            // way `dv` can be `Some` here.
            let file_source = match (dv.is_some(), file_source_with_dv) {
                (true, Some(src)) => src,
                _ => file_source_no_dv,
            };

            let plan = build_per_file_plan(
                inputs,
                dv,
                file_source,
                sink.file_type,
                &output_schema,
                task_ctx.as_ref(),
            )
            .await?;
            let stream = plan.execute(0, task_ctx)?;
            Ok::<_, DataFusionError>(stream)
        }
    });

    // Concurrent flatten + limit slicing.
    let flattened = per_file_streams.buffer_unordered(concurrency).try_flatten();
    async_stream::try_stream! {
        let mut remaining = limit;
        let mut s = std::pin::pin!(flattened);
        while let Some(batch) = s.try_next().await? {
            let mut out = batch;
            if let Some(rem) = remaining.as_mut() {
                if out.num_rows() > *rem {
                    out = out.slice(0, *rem);
                }
                *rem -= out.num_rows();
            }
            if out.num_rows() > 0 {
                yield out;
            }
            if matches!(remaining, Some(0)) {
                return;
            }
        }
    }
}
