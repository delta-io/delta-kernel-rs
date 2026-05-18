//! [`LoadExec`] -- the streaming physical plan that powers
//! [`super::LoadTableProvider`].
//!
//! For each upstream metadata batch, [`LoadExecStream`] walks rows one at a time. For each row
//! it resolves a file URL + optional DV mask + (narrowed) per-row passthrough column values,
//! then opens the file via DataFusion's [`FileSource::create_file_opener`] +
//! [`FileOpener::open`] -- the *same* parquet/json opener `ListingTable` uses. Each opener-
//! produced batch is yielded downstream as soon as it's ready; there is no per-batch or
//! per-file accumulation.
//!
//! # Why a DataFusion opener and not kernel's parquet handler
//!
//! Column-mapping-aware decode reshape (logical name + nested rename via
//! `PARQUET:field_id` / `delta.columnMapping.physicalName`) is handled by
//! [`super::FieldIdPhysicalExprAdapterFactory`] wired into the opener's
//! [`FileScanConfig::expr_adapter_factory`]. That makes the opener-produced batches match the
//! kernel-declared logical file schema end-to-end -- no post-scan structural realignment is
//! needed.
//!
//! # Pushdown
//!
//! * `projection: Option<Vec<usize>>` is split into (a) which file-schema columns to ask the
//!   opener for (via a narrowed [`KernelSchemaRef`] used as the [`ParquetSource`] /
//!   [`JsonSource`] table schema) and (b) which passthrough columns to broadcast. Both
//!   narrowings flow all the way through; the output batch is assembled in projection order.
//!   See [`ProjectionPlan`].
//! * `limit: Option<usize>` caps total emitted rows. Once a batch would overshoot the budget it
//!   is sliced; the next `poll_next` returns end-of-stream without opening any further files.
//!
//! Filter pushdown is intentionally left to upstream operators (see
//! [`super::LoadTableProvider::supports_filters_pushdown`]).

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::{ListingTableUrl, PartitionedFile};
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{ArrayRef, RecordBatch};
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::schema::SchemaRef as KernelSchemaRef;
use delta_kernel::Engine;
use futures::stream::{BoxStream, Stream};
use url::Url;

use crate::exec::field_id_adapter::FieldIdPhysicalExprAdapterFactory;
use crate::exec::load_helpers::{
    apply_optional_dv, dv_table_root, extract_column_array, optional_i64,
    optional_selection_vector_for_row, replicate_row, resolve_file_location, utf8_value_at,
};

/// Default opener batch size. DataFusion's parquet/json openers require a non-`None` batch size
/// before `create_file_opener` is called. The session config's `batch_size` would be nicer but
/// it's only available at `execute()` time and the source is built at construction time -- we
/// pre-populate this default and let the per-execute path override if desired.
const DEFAULT_OPENER_BATCH_SIZE: usize = 8192;

/// One slot in [`ProjectionPlan::assembly`]: where to source an output column from.
#[derive(Debug, Clone, Copy)]
enum AssemblyCol {
    /// Output column = column at `narrow_file_pos` in the opener's batch.
    File { narrow_file_pos: usize },
    /// Output column = broadcast of the upstream row's value of the passthrough column at
    /// `narrow_passthrough_pos`.
    Passthrough { narrow_passthrough_pos: usize },
}

/// Pre-computed projection state for [`LoadExec`].
///
/// Splits an `Option<Vec<usize>>` projection into the narrow file-schema, the narrow passthrough
/// column list, and an `assembly` plan that maps each output column to its source. Built once
/// at [`LoadExec::new`]; cloned cheaply (`Arc`) into each [`LoadExecStream`].
struct ProjectionPlan {
    /// File-schema subset handed to the [`ParquetSource`] / [`JsonSource`] as its table schema.
    /// The [`FieldIdPhysicalExprAdapterFactory`] in the opener config sees this as the
    /// "logical" side and matches it against the physical parquet schema by field id /
    /// column-mapping physical name.
    narrow_file_schema: KernelSchemaRef,
    /// Number of fields in `narrow_file_schema`. Cached because we need it during assembly
    /// validation and the kernel SchemaRef accessor allocates an iterator each call.
    narrow_file_field_count: usize,
    /// Subset of [`LoadSink::passthrough_columns`] referenced by projection, in original order.
    narrow_passthrough_columns: Vec<delta_kernel::expressions::ColumnName>,
    /// Per-output-column source descriptor in projection order.
    assembly: Vec<AssemblyCol>,
    /// Final output Arrow schema (projection applied).
    output_schema: ArrowSchemaRef,
}

impl ProjectionPlan {
    fn new(
        sink: &LoadSink,
        full_schema: &ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        let file_count = sink.file_schema.fields().len();
        let passthrough_count = sink.passthrough_columns.len();
        debug_assert_eq!(full_schema.fields().len(), file_count + passthrough_count);

        // No projection: identity passthrough.
        let Some(proj) = projection else {
            let assembly = (0..file_count)
                .map(|i| AssemblyCol::File { narrow_file_pos: i })
                .chain((0..passthrough_count).map(|p| AssemblyCol::Passthrough {
                    narrow_passthrough_pos: p,
                }))
                .collect();
            return Ok(Self {
                narrow_file_schema: sink.file_schema.clone(),
                narrow_file_field_count: file_count,
                narrow_passthrough_columns: sink.passthrough_columns.clone(),
                assembly,
                output_schema: Arc::clone(full_schema),
            });
        };

        // Walk projection once, recording each referenced source column the first time we see
        // it. `*_pos` maps original-source-index -> position in the narrowed list (which is
        // also the index used by `AssemblyCol::{File,Passthrough}`).
        let mut file_indices: Vec<usize> = Vec::with_capacity(proj.len().min(file_count));
        let mut file_pos: HashMap<usize, usize> = HashMap::with_capacity(file_count);
        let mut pt_indices: Vec<usize> = Vec::with_capacity(proj.len().min(passthrough_count));
        let mut pt_pos: HashMap<usize, usize> = HashMap::with_capacity(passthrough_count);
        for &full_idx in proj {
            if full_idx < file_count {
                if let std::collections::hash_map::Entry::Vacant(entry) = file_pos.entry(full_idx)
                {
                    entry.insert(file_indices.len());
                    file_indices.push(full_idx);
                }
            } else {
                let p = full_idx - file_count;
                if p >= passthrough_count {
                    return Err(crate::error::plan_compilation(format!(
                        "LoadExec projection index {full_idx} out of range for output schema of \
                         {file_count} file fields + {passthrough_count} passthrough fields",
                    )));
                }
                if let std::collections::hash_map::Entry::Vacant(entry) = pt_pos.entry(p) {
                    entry.insert(pt_indices.len());
                    pt_indices.push(p);
                }
            }
        }

        // Build narrow_file_schema. We use `project_as_struct` (kernel) which preserves the
        // requested field order *and* per-field metadata (column-mapping IDs, parquet field
        // IDs). When the projection touches zero file fields we still need a non-empty read
        // schema so the opener reports per-file row counts -- fall back to the full
        // file_schema in that pathological case. DataFusion's projection pushdown rarely
        // generates a passthrough-only projection in practice but we guard against it for
        // correctness.
        let narrow_file_schema: KernelSchemaRef = if file_indices.is_empty() {
            sink.file_schema.clone()
        } else {
            let field_names: Vec<&str> = file_indices
                .iter()
                .map(|&i| {
                    sink.file_schema
                        .fields()
                        .nth(i)
                        .expect("file_indices bounded by file_count")
                        .name()
                        .as_str()
                })
                .collect();
            sink.file_schema.project(&field_names).map_err(|e| {
                crate::error::internal_error(format!(
                    "LoadExec narrow file_schema projection failed: {e}"
                ))
            })?
        };
        let narrow_file_field_count = narrow_file_schema.fields().len();

        let narrow_passthrough_columns: Vec<_> = pt_indices
            .iter()
            .map(|&p| sink.passthrough_columns[p].clone())
            .collect();

        let assembly = proj
            .iter()
            .map(|&full_idx| {
                if full_idx < file_count {
                    if file_indices.is_empty() {
                        // Pathological projection-only-passthrough fallback: we read the entire
                        // file_schema above, so the assembly position equals the original index.
                        AssemblyCol::File {
                            narrow_file_pos: full_idx,
                        }
                    } else {
                        AssemblyCol::File {
                            narrow_file_pos: *file_pos
                                .get(&full_idx)
                                .expect("file_pos populated for every projected file index"),
                        }
                    }
                } else {
                    let p = full_idx - file_count;
                    AssemblyCol::Passthrough {
                        narrow_passthrough_pos: *pt_pos
                            .get(&p)
                            .expect("pt_pos populated for every projected passthrough index"),
                    }
                }
            })
            .collect();

        let output_schema = Arc::new(full_schema.project(proj)?);
        Ok(Self {
            narrow_file_schema,
            narrow_file_field_count,
            narrow_passthrough_columns,
            assembly,
            output_schema,
        })
    }
}

/// Custom physical plan for [`super::LoadTableProvider`] sinks: stream upstream metadata rows
/// through the configured DataFusion [`FileOpener`] one file at a time, yielding each
/// opener-produced batch as soon as it's ready (no per-batch / per-file buffering). See the
/// module-level docs for opener selection + pushdown semantics.
pub struct LoadExec {
    sink: Arc<LoadSink>,
    /// Kernel engine kept only for deletion-vector resolution (kernel
    /// [`selection_vector`](delta_kernel::scan::selection_vector)). File decoding goes through
    /// the DataFusion opener; this engine is *not* consulted for parquet/json reads.
    engine: Arc<dyn Engine>,
    upstream: Arc<dyn ExecutionPlan>,
    /// Full pre-projection output schema (= file_schema fields ++ passthrough fields). Kept on
    /// the struct only so that [`ExecutionPlan::with_new_children`] can recompute the same
    /// projection plan with the same input shape.
    full_schema: ArrowSchemaRef,
    /// Original projection (kept verbatim for replay through `with_new_children`).
    projection: Option<Vec<usize>>,
    /// Pre-computed projection split. Shared by reference into each [`LoadExecStream`].
    projection_plan: Arc<ProjectionPlan>,
    /// Optional row-count cap from `TableProvider::scan(limit)`. `None` = read to upstream end.
    limit: Option<usize>,
    /// Pre-built file source (Parquet/Json) configured with the narrow file schema (Arrow) +
    /// batch size. Cloned per-execute into the per-file `FileScanConfig`.
    file_source: Arc<dyn datafusion_datasource::file::FileSource>,
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
        let projection_plan = Arc::new(ProjectionPlan::new(
            sink.as_ref(),
            &full_schema,
            projection.as_ref(),
        )?);
        let file_source = build_file_source(sink.as_ref(), &projection_plan.narrow_file_schema)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projection_plan.output_schema)),
            // File openers are single-stream; emit on a single partition to match pre-refactor
            // `drain_load` semantics.
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
            projection_plan,
            limit,
            file_source,
            properties,
        })
    }
}

/// Build a [`FileSource`] for the load's file type, configured with the narrow kernel file
/// schema (converted to Arrow with `PARQUET:field_id` / `delta.columnMapping.physicalName`
/// metadata preserved by [`TryIntoArrow`]) and a default batch size. The
/// [`FieldIdPhysicalExprAdapterFactory`] wired into the per-execute [`FileScanConfig`] uses
/// that metadata to match logical fields against the physical parquet schema.
fn build_file_source(
    sink: &LoadSink,
    narrow_file_schema: &KernelSchemaRef,
) -> DfResult<Arc<dyn datafusion_datasource::file::FileSource>> {
    let arrow_schema: ArrowSchemaRef = Arc::new(
        narrow_file_schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| {
                crate::error::internal_error(format!(
                    "LoadExec narrow file_schema arrow conversion failed: {e}"
                ))
            })?,
    );
    let source: Arc<dyn datafusion_datasource::file::FileSource> = match sink.file_type {
        FileType::Parquet => Arc::new(ParquetSource::new(arrow_schema)),
        FileType::Json => {
            // NDJSON; matches the kernel default for delta JSON sidecars/logs.
            Arc::new(JsonSource::new(arrow_schema))
        }
    };
    Ok(source.with_batch_size(DEFAULT_OPENER_BATCH_SIZE))
}

impl fmt::Debug for LoadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadExec")
            .field("output_relation", &self.sink.output_relation.name)
            .field("file_type", &self.sink.file_type)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .field(
                "narrow_file_fields",
                &self.projection_plan.narrow_file_field_count,
            )
            .field(
                "narrow_passthrough_fields",
                &self.projection_plan.narrow_passthrough_columns.len(),
            )
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LoadExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LoadExec(output_relation={}, file_type={:?}, projection={:?}, limit={:?}, \
             narrow_file_fields={}, narrow_passthrough_fields={})",
            self.sink.output_relation.name,
            self.sink.file_type,
            self.projection,
            self.limit,
            self.projection_plan.narrow_file_field_count,
            self.projection_plan.narrow_passthrough_columns.len(),
        )
    }
}

impl ExecutionPlan for LoadExec {
    fn name(&self) -> &str {
        "LoadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.projection_plan.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.upstream]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "LoadExec requires exactly one child".into(),
            ));
        }
        let upstream = children.into_iter().next().unwrap();
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
        // walker sees one batch at a time. `execute_stream` does exactly this.
        let upstream = datafusion::physical_plan::execute_stream(
            Arc::clone(&self.upstream),
            Arc::clone(&context),
        )?;
        Ok(Box::pin(LoadExecStream::new(
            upstream,
            Arc::clone(&self.sink),
            Arc::clone(&self.engine),
            Arc::clone(&self.file_source),
            Arc::clone(&self.projection_plan),
            self.limit,
            context,
        )))
    }
}

#[cfg(test)]
impl LoadExec {
    /// Narrow file-schema actually handed to the [`ParquetSource`] / [`JsonSource`] after
    /// projection pushdown. Exposed for tests that assert pushdown shrunk the read request.
    pub(crate) fn narrow_file_schema(&self) -> &KernelSchemaRef {
        &self.projection_plan.narrow_file_schema
    }

    /// Narrow passthrough columns actually broadcast after projection pushdown.
    pub(crate) fn narrow_passthrough_columns(&self) -> &[delta_kernel::expressions::ColumnName] {
        &self.projection_plan.narrow_passthrough_columns
    }
}

/// Stream-time state for the row currently being processed.
enum RowState {
    /// No active file. Need to advance to the next upstream row.
    Idle,
    /// Awaiting the [`FileOpenFuture`] for a row's file.
    Opening {
        upstream_row: usize,
        passthrough_raw: Vec<ArrayRef>,
        mask: Option<Vec<bool>>,
        future: FileOpenFuture,
    },
    /// Streaming opener-produced batches for a row's file.
    Scanning {
        upstream_row: usize,
        passthrough_raw: Vec<ArrayRef>,
        mask: Option<Vec<bool>>,
        stream: BoxStream<'static, DfResult<RecordBatch>>,
        /// File-local row cursor, advanced by each opener batch we apply DV to.
        file_row_cursor: usize,
    },
}

/// `Stream<Item = Result<RecordBatch>>` + [`RecordBatchStream`] driver for [`LoadExec`].
struct LoadExecStream {
    upstream: SendableRecordBatchStream,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    file_source: Arc<dyn datafusion_datasource::file::FileSource>,
    projection_plan: Arc<ProjectionPlan>,
    task_context: Arc<TaskContext>,
    /// Remaining row budget. `None` = unbounded. When `Some(0)` the stream is finished.
    remaining: Option<usize>,
    /// Current upstream batch we're walking. `None` until we pull the first; reset on
    /// exhaustion.
    current_batch: Option<RecordBatch>,
    /// Next row to start within `current_batch`.
    next_row: usize,
    /// Per-row state machine: Idle -> Opening -> Scanning -> Idle (next row).
    row_state: RowState,
}

impl LoadExecStream {
    #[allow(clippy::too_many_arguments)]
    fn new(
        upstream: SendableRecordBatchStream,
        sink: Arc<LoadSink>,
        engine: Arc<dyn Engine>,
        file_source: Arc<dyn datafusion_datasource::file::FileSource>,
        projection_plan: Arc<ProjectionPlan>,
        limit: Option<usize>,
        task_context: Arc<TaskContext>,
    ) -> Self {
        Self {
            upstream,
            sink,
            engine,
            file_source,
            projection_plan,
            task_context,
            remaining: limit,
            current_batch: None,
            next_row: 0,
            row_state: RowState::Idle,
        }
    }

    /// Resolve the next upstream row into an `Opening` row state: parses file URL + size,
    /// resolves an optional DV mask via kernel, pre-extracts narrow passthrough arrays, then
    /// constructs the DataFusion file opener (with [`FieldIdPhysicalExprAdapterFactory`] for
    /// parquet) and starts the file-open future.
    fn start_open_row(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<RowState, DataFusionError> {
        let path_cn = &self.sink.file_meta.path;
        let path_arr = extract_column_array(batch, path_cn)?;
        let path_raw = utf8_value_at(&path_arr, row)?.ok_or_else(|| {
            crate::error::plan_compilation(format!(
                "Load sink path column `{}` was NULL at upstream row {row}",
                path_cn
            ))
        })?;
        let url = resolve_file_location(&self.sink.base_url, &path_raw)?;
        let table_root = dv_table_root(self.sink.as_ref(), &url);

        let mask = optional_selection_vector_for_row(
            batch,
            self.sink.as_ref(),
            row,
            self.engine.as_ref(),
            &table_root,
        )?;
        let size = file_size_for_row(batch, self.sink.as_ref(), row, &url)?;

        // Pre-extract only the NARROW passthrough columns (projection pushdown).
        let mut passthrough_raw =
            Vec::with_capacity(self.projection_plan.narrow_passthrough_columns.len());
        for cn in &self.projection_plan.narrow_passthrough_columns {
            passthrough_raw.push(extract_column_array(batch, cn)?);
        }

        let (object_store_url, partitioned_file) = into_partitioned_file(&url, size)?;
        let object_store = self
            .task_context
            .runtime_env()
            .object_store(&object_store_url)?;

        // Per-row FileScanConfig: identical template (file_source + adapter factory) with the
        // per-file object_store_url stamped in. `create_file_opener` only consults
        // `object_store`, `expr_adapter_factory`, `limit`, `preserve_order` -- not the file
        // groups -- so leaving them empty is fine; we feed the actual file directly into
        // `opener.open(...)`.
        let base_config = FileScanConfigBuilder::new(object_store_url, Arc::clone(&self.file_source))
            .with_expr_adapter(adapter_factory_for(self.sink.file_type))
            .build();
        let opener = self
            .file_source
            .create_file_opener(object_store, &base_config, 0)?;
        let future = opener.open(partitioned_file)?;

        Ok(RowState::Opening {
            upstream_row: row,
            passthrough_raw,
            mask,
            future,
        })
    }

    /// Take one opener batch and assemble the projected output batch (DV-filtered + passthrough
    /// broadcast in projection order). The opener-produced batch is already in the kernel
    /// logical schema thanks to [`FieldIdPhysicalExprAdapterFactory`], so no further
    /// realignment is needed -- output columns flow straight into [`RecordBatch::try_new`]
    /// against the strict declared output schema.
    fn assemble_output(
        &self,
        rb: RecordBatch,
        upstream_row: usize,
        passthrough_raw: &[ArrayRef],
        mask: Option<&[bool]>,
        file_row_cursor: &mut usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let pre_dv_rows = rb.num_rows();
        let cursor = *file_row_cursor;
        *file_row_cursor += pre_dv_rows;
        let masked = apply_optional_dv(rb, mask, cursor)?;
        let target_len = masked.num_rows();

        let mut out_cols: Vec<ArrayRef> =
            Vec::with_capacity(self.projection_plan.assembly.len());
        for slot in &self.projection_plan.assembly {
            match *slot {
                AssemblyCol::File { narrow_file_pos } => {
                    out_cols.push(masked.column(narrow_file_pos).clone());
                }
                AssemblyCol::Passthrough {
                    narrow_passthrough_pos,
                } => {
                    out_cols.push(replicate_row(
                        &passthrough_raw[narrow_passthrough_pos],
                        upstream_row,
                        target_len,
                    )?);
                }
            }
        }

        // Build the intermediate batch under a schema derived from the actual arrays (which is
        // shape-correct but lacks the kernel's Delta-protocol per-field metadata), then re-stamp
        // it to the kernel-declared output schema in one zero-copy `cast`. See
        // `metadata_stamper::stamp_batch_metadata`.
        use datafusion_common::arrow::datatypes::{Field, Schema};
        let intermediate_schema = Arc::new(Schema::new(
            out_cols
                .iter()
                .zip(self.projection_plan.output_schema.fields().iter())
                .map(|(arr, target)| {
                    Field::new(target.name(), arr.data_type().clone(), target.is_nullable())
                })
                .collect::<Vec<_>>(),
        ));
        let intermediate = RecordBatch::try_new(intermediate_schema, out_cols).map_err(|e| {
            crate::error::internal_error(format!("LoadExec assembled batch failed: {e}"))
        })?;
        crate::exec::stamp_batch_metadata(&intermediate, &self.projection_plan.output_schema)
    }
}

impl Stream for LoadExecStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // 0) Limit exhausted? End-of-stream without doing any more I/O.
            if matches!(self.remaining, Some(0)) {
                self.row_state = RowState::Idle;
                self.current_batch = None;
                return Poll::Ready(None);
            }

            // 1) Drive whichever per-row state is active.
            match std::mem::replace(&mut self.row_state, RowState::Idle) {
                RowState::Scanning {
                    upstream_row,
                    passthrough_raw,
                    mask,
                    mut stream,
                    mut file_row_cursor,
                } => match stream.as_mut().poll_next(cx) {
                    Poll::Pending => {
                        self.row_state = RowState::Scanning {
                            upstream_row,
                            passthrough_raw,
                            mask,
                            stream,
                            file_row_cursor,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(Some(Ok(rb))) => {
                        let assembled = match self.assemble_output(
                            rb,
                            upstream_row,
                            &passthrough_raw,
                            mask.as_deref(),
                            &mut file_row_cursor,
                        ) {
                            Ok(batch) => batch,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };
                        let mut out = assembled;
                        if let Some(rem) = self.remaining.as_mut() {
                            if out.num_rows() > *rem {
                                out = out.slice(0, *rem);
                            }
                            *rem -= out.num_rows();
                        }
                        // Reinstate the scanning state so the next poll continues from this
                        // same file iterator (subject to the budget check at step 0).
                        self.row_state = RowState::Scanning {
                            upstream_row,
                            passthrough_raw,
                            mask,
                            stream,
                            file_row_cursor,
                        };
                        return Poll::Ready(Some(Ok(out)));
                    }
                    Poll::Ready(None) => {
                        // File exhausted; fall through to advance to next upstream row.
                    }
                },
                RowState::Opening {
                    upstream_row,
                    passthrough_raw,
                    mask,
                    mut future,
                } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.row_state = RowState::Opening {
                            upstream_row,
                            passthrough_raw,
                            mask,
                            future,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(Ok(stream)) => {
                        self.row_state = RowState::Scanning {
                            upstream_row,
                            passthrough_raw,
                            mask,
                            stream,
                            file_row_cursor: 0,
                        };
                        continue;
                    }
                },
                RowState::Idle => {}
            }

            // 2) No active state. Try to advance to the next upstream row.
            if let Some(batch) = self.current_batch.clone() {
                if self.next_row < batch.num_rows() {
                    let row = self.next_row;
                    self.next_row += 1;
                    self.row_state = match self.start_open_row(&batch, row) {
                        Ok(state) => state,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    continue;
                }
                // Current upstream batch exhausted; drop it and pull the next.
                self.current_batch = None;
                self.next_row = 0;
            }
            // 3) Pull the next upstream batch.
            match Pin::new(&mut self.upstream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(upstream_batch))) => {
                    self.current_batch = Some(upstream_batch);
                    self.next_row = 0;
                }
            }
        }
    }
}

impl RecordBatchStream for LoadExecStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.projection_plan.output_schema)
    }
}

/// Select the [`PhysicalExprAdapterFactory`](datafusion_physical_expr_adapter::PhysicalExprAdapterFactory)
/// for a given file type. Parquet decoding goes through field-id/column-mapping aware
/// adaptation; JSON keeps DataFusion's default name-based adapter (we don't ship column-mapped
/// JSON loads today).
fn adapter_factory_for(
    file_type: FileType,
) -> Option<Arc<dyn datafusion_physical_expr_adapter::PhysicalExprAdapterFactory>> {
    match file_type {
        FileType::Parquet => Some(Arc::new(FieldIdPhysicalExprAdapterFactory)),
        FileType::Json => None,
    }
}

/// Translate `url` + `size` into a DataFusion [`ObjectStoreUrl`] + [`PartitionedFile`] pair.
/// The ObjectStoreUrl carries the scheme + authority (e.g. `s3://bucket/` or `file://`); the
/// PartitionedFile's `object_meta.location` is the path within that store (matches what
/// `ListingTableUrl::parse(url).prefix()` produces).
fn into_partitioned_file(
    url: &Url,
    size: i64,
) -> Result<(ObjectStoreUrl, PartitionedFile), DataFusionError> {
    let listing = ListingTableUrl::parse(url.as_str())?;
    let object_store_url = listing.object_store();
    let store_path = listing.prefix().clone();
    let mut pf = PartitionedFile::new(store_path.as_ref().to_owned(), u64::try_from(size.max(0)).unwrap_or(0));
    // `PartitionedFile::new` reparses the path as `object_store::path::Path` from a string. To
    // preserve the exact `Path` instance ListingTableUrl produced (which already handled URL
    // decoding), stamp it directly.
    pf.object_meta.location = store_path;
    Ok((object_store_url, pf))
}

fn file_size_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    url: &Url,
) -> Result<i64, DataFusionError> {
    if let Some(sz_cn) = sink.file_meta.size.as_ref() {
        return Ok(optional_i64(batch, sz_cn, row)?.unwrap_or(0));
    }
    if url.scheme() == "file" {
        let p = url.to_file_path().map_err(|()| {
            crate::error::plan_compilation(format!("file URL could not be converted to path: {url}"))
        })?;
        return Ok(std::fs::metadata(&p).map(|m| m.len() as i64).unwrap_or(0));
    }
    Ok(0)
}
