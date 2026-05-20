//! [`LoadExec`] -- the streaming physical plan that powers
//! [`super::LoadTableProvider`].
//!
//! For each upstream metadata batch, [`build_load_stream`] runs an **unordered concurrent
//! merger** over per-row open futures via [`futures::stream::buffer_unordered`]. Each open
//! future async-resolves the optional deletion vector via
//! [`crate::exec::load_helpers::resolve_dv_async`] (a [`tokio::task::spawn_blocking`] hop
//! around kernel's sync `DeletionVectorDescriptor::read`), builds a per-file
//! [`datafusion_physical_plan::ExecutionPlan`] stack via
//! [`crate::exec::load_helpers::build_per_file_plan`], and runs `plan.execute(0, ctx)` to
//! get a `BoxStream<RecordBatch>`.
//!
//! The outer flatten interleaves files freely (no upstream-row ordering across files);
//! within each file, batches arrive in file-internal order from the parquet/json decoder.
//!
//! Column-mapping-aware decode reshape is handled by
//! [`super::FieldIdPhysicalExprAdapterFactory`] wired into the per-file
//! [`datafusion_datasource::file_scan_config::FileScanConfig::expr_adapter_factory`], so
//! opener batches already match the kernel-declared logical file schema.
//!
//! Deletion vectors apply via a `not_in_dv(_row_number)` [`datafusion_expr::ScalarUDF`]
//! plugged into a [`datafusion_physical_plan::filter::FilterExec`] above the per-file
//! [`datafusion_datasource::source::DataSourceExec`]. The parquet decoder injects
//! `_row_number` (Int64 + `RowNumber` extension type) only when the file has a DV; the
//! UDF closes over the kernel-resolved `Arc<roaring::RoaringTreemap>` of deleted row
//! IDs. JSON+DV is rejected at construction.
//!
//! Passthrough columns ride through as `partition_values`. `projection` is pushed into the
//! [`FileSource`]; `limit` caps emitted rows by slicing the final batch. Filter pushdown is
//! left to upstream operators (see
//! [`super::LoadTableProvider::supports_filters_pushdown`]).

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

/// Default per-partition file-open concurrency when `target_partitions` is zero.
const DEFAULT_LOAD_CONCURRENCY: usize = 8;

/// Upper bound on per-partition file-open concurrency. Caps tokio task pressure for very-wide
/// scans; tunable later via a dedicated `load_concurrency` knob.
const MAX_LOAD_CONCURRENCY: usize = 64;

/// Custom physical plan for [`super::LoadTableProvider`] sinks: streams upstream metadata rows
/// through per-file [`ExecutionPlan`] stacks under a [`futures::stream::buffer_unordered`]
/// merger. See module-level docs.
pub struct LoadExec {
    sink: Arc<LoadSink>,
    /// Kernel engine kept only for deletion-vector resolution
    /// ([`delta_kernel::actions::deletion_vector::DeletionVectorDescriptor::read`]).
    /// File decoding goes through DataFusion's parquet/json sources; this engine is *not*
    /// consulted for parquet/json reads.
    engine: Arc<dyn Engine>,
    upstream: Arc<dyn ExecutionPlan>,
    /// Full pre-projection output schema (= file_schema fields ++ passthrough fields). Kept on
    /// the struct only so that [`ExecutionPlan::with_new_children`] can rebuild the file source
    /// against the same shape.
    full_schema: ArrowSchemaRef,
    /// Original projection (kept verbatim for replay through `with_new_children`).
    projection: Option<Vec<usize>>,
    /// Final output schema (projection applied to `full_schema`). What downstream operators
    /// see via [`ExecutionPlan::schema`].
    output_schema: ArrowSchemaRef,
    /// Optional row-count cap from `TableProvider::scan(limit)`. `None` = read to upstream end.
    limit: Option<usize>,
    /// Pre-computed global indices into `sink.passthrough_columns`, in projected order
    /// (Phase 4). Iterated once per upstream row in `extract_row_inputs` to materialize
    /// only the kept passthrough columns. Wrapped in `Arc` so cheap clones travel into the
    /// per-row open future without an extra allocation per row.
    projected_passthrough: Arc<Vec<usize>>,
    /// File source built without `_row_number` -- used for files with no DV. Cloned per-execute
    /// into the per-file [`datafusion_datasource::file_scan_config::FileScanConfig`].
    file_source_no_dv: Arc<dyn datafusion_datasource::file::FileSource>,
    /// File source built **with** `_row_number` virtual column appended -- used for files
    /// with a DV so the parquet decoder injects it for the not_in_dv UDF predicate. None when
    /// the sink has no `dv_ref` (so we never construct the row-number variant).
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
        // Reject JSON + DV at construction (Phase 2). JSON has no row-number virtual column;
        // delta DVs only apply to parquet data files in practice.
        if matches!(sink.file_type, FileType::Json) && sink.dv_ref.is_some() {
            return Err(DataFusionError::Plan(
                "LoadSink with FileType::Json and dv_ref set is not supported: deletion \
                 vectors only apply to parquet data files (no _row_number virtual column \
                 for JSON)."
                    .to_string(),
            ));
        }

        let file_count = sink.file_schema.fields().len();
        let passthrough_count = sink.passthrough_columns.len();
        debug_assert_eq!(full_schema.fields().len(), file_count + passthrough_count);

        // Build the no-DV file source (always needed: even when sink.dv_ref is set, individual
        // rows may have a NULL DV column meaning "no DV for this file").
        let file_source_no_dv = build_file_source(
            sink.file_type,
            &full_schema,
            file_count,
            projection.as_deref(),
            /* include_row_number */ false,
        )?;
        let file_source_with_dv = if sink.dv_ref.is_some() {
            Some(build_file_source(
                sink.file_type,
                &full_schema,
                file_count,
                projection.as_deref(),
                /* include_row_number */ true,
            )?)
        } else {
            None
        };

        // Output schema: projection narrows full_schema (Phase 4 contract: file fields come
        // first, then projected passthrough fields in order).
        let output_schema = match projection.as_ref() {
            Some(proj) => Arc::new(full_schema.project(proj)?),
            None => Arc::clone(&full_schema),
        };

        // Pre-compute kept passthrough indices once (Phase 4). When projection is None, all
        // passthrough columns are kept in their original order. When projection is set,
        // filter to indices >= file_count and translate to passthrough-local indices.
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
            // Single output partition: the merger interleaves files within one stream.
            // Multi-partition fan-out is a follow-up.
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
        // Coalesce the upstream's possibly-many partitions into a single stream so the row
        // expander sees one batch at a time. `execute_stream` does exactly this.
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

/// Stream upstream metadata rows through the unordered concurrent merger. Each row produces
/// a per-file `BoxStream<RecordBatch>` (built lazily inside an open future); up to
/// `concurrency` open futures run at once via `buffer_unordered`. The outer `try_flatten`
/// interleaves files freely; intra-file batch order is preserved by sequential drain.
///
/// `limit` is enforced at the flattened output by slicing + early-terminating.
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
    // Step 1: explode upstream batches into a flat `Stream<DfResult<(Arc<RecordBatch>, usize)>>`,
    // one item per row.
    let row_stream = upstream.map_ok(|batch| {
        let n = batch.num_rows();
        let batch = Arc::new(batch);
        futures::stream::iter((0..n).map(move |row| Ok((Arc::clone(&batch), row))))
    });
    let row_stream = row_stream
        .try_flatten()
        .map_err(|e: DataFusionError| e);

    // Step 2: per-row, build an open future returning the per-file RecordBatch stream.
    let sink_for_map = Arc::clone(&sink);
    let engine_for_map = Arc::clone(&engine);
    let task_ctx_for_map = Arc::clone(&task_context);
    let pt_for_map = Arc::clone(&projected_passthrough);
    let no_dv_for_map = Arc::clone(&file_source_no_dv);
    let with_dv_for_map = file_source_with_dv;
    let output_schema_for_map = Arc::clone(&output_schema);

    let per_file_streams = row_stream.map(move |row_result| {
        let sink = Arc::clone(&sink_for_map);
        let engine = Arc::clone(&engine_for_map);
        let task_ctx = Arc::clone(&task_ctx_for_map);
        let pt = Arc::clone(&pt_for_map);
        let file_source_no_dv = Arc::clone(&no_dv_for_map);
        let file_source_with_dv = with_dv_for_map.as_ref().map(Arc::clone);
        let output_schema = Arc::clone(&output_schema_for_map);

        async move {
            let (batch, row) = row_result?;
            let inputs: RowInputs = extract_row_inputs(&batch, row, &sink, &pt)?;

            // Async-resolve DV when present.
            let dv = match inputs.dv_descriptor.clone() {
                Some(desc) => {
                    let base = sink_base_url(&sink)?.clone();
                    Some(resolve_dv_async(desc, base, Arc::clone(&engine)).await?)
                }
                None => None,
            };

            // Pick the appropriate file_source: with `_row_number` when this file has a DV,
            // without otherwise. We require a configured with-DV source if any row has a DV
            // (`sink.dv_ref` is set) AND that row's DV column is non-null.
            let file_source = match (dv.is_some(), file_source_with_dv) {
                (true, Some(src)) => src,
                (true, None) => {
                    return Err(crate::error::internal_error(
                        "LoadExec: row has DV but file_source_with_dv was not constructed \
                         (sink.dv_ref must be set at construction time for any DV-bearing \
                         upstream)",
                    ));
                }
                (false, _) => file_source_no_dv,
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

    // Step 3: run up to `concurrency` opens at once; flatten per-file streams into one outer
    // stream. Order across files is arbitrary; intra-file order is preserved.
    let flattened = per_file_streams
        .buffer_unordered(concurrency)
        .try_flatten();

    // Step 4: limit by slicing + early termination.
    apply_limit(flattened, limit)
}

/// Apply an optional row-count cap to a stream of [`RecordBatch`]es. Slices the final batch
/// when it would overshoot, and stops pulling once the budget is exhausted.
fn apply_limit<S>(stream: S, limit: Option<usize>) -> impl Stream<Item = DfResult<RecordBatch>>
where
    S: Stream<Item = DfResult<RecordBatch>> + Send + 'static,
{
    async_stream::try_stream! {
        let mut remaining = limit;
        let mut s = std::pin::pin!(stream);
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
