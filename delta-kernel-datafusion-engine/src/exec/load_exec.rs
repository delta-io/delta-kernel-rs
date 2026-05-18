//! [`LoadExec`] -- the streaming physical plan that powers
//! [`super::LoadTableProvider`].
//!
//! For each upstream metadata batch, [`LoadExecStream`] walks rows one at a time. For each row
//! it resolves a file URL + optional DV mask + per-row passthrough column values, then opens
//! the file via DataFusion's [`FileSource::create_file_opener`] + [`FileOpener::open`] -- the
//! *same* parquet/json opener `ListingTable` uses. Each opener-produced batch is yielded
//! downstream as soon as it's ready; there is no per-batch / per-file accumulation.
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
//! * Passthrough columns (kernel metadata-side values like `_metadata`, `path`) are handed to
//!   the opener as [`TableSchema::with_table_partition_cols`]. Per upstream row we extract the
//!   corresponding [`ScalarValue`]s and stamp them onto [`PartitionedFile::partition_values`];
//!   the [`ProjectionOpener`] (parquet) / opener wrapper (json) substitutes them as literal
//!   columns when materializing the file's batches. No bespoke per-batch broadcast.
//! * `projection: Option<Vec<usize>>` is pushed straight into the [`FileSource`] via
//!   [`FileScanConfigBuilder::with_projection_indices`]; the source then narrows both the
//!   parquet read schema and which partition columns it materializes.
//! * `limit: Option<usize>` caps total emitted rows. Once a batch would overshoot the budget it
//!   is sliced; the next `poll_next` returns end-of-stream without opening any further files.
//!
//! Filter pushdown is intentionally left to upstream operators (see
//! [`super::LoadTableProvider::supports_filters_pushdown`]).

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::TimeZone;
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_common::ScalarValue;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::{ListingTableUrl, PartitionedFile, TableSchema};
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
use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::arrow::datatypes::{FieldRef, SchemaRef as ArrowSchemaRef};
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::Engine;
use futures::stream::{BoxStream, Stream};
use url::Url;

use crate::exec::field_id_adapter::FieldIdPhysicalExprAdapterFactory;
use crate::exec::load_helpers::{
    apply_optional_dv, extract_column_array, optional_i64, optional_selection_vector_for_row,
    resolve_file_location, utf8_value_at,
};

/// Default opener batch size. DataFusion's parquet/json openers require a non-`None` batch size
/// before `create_file_opener` is called. The session config's `batch_size` would be nicer but
/// it's only available at `execute()` time and the source is built at construction time -- we
/// pre-populate this default and let the per-execute path override if desired.
const DEFAULT_OPENER_BATCH_SIZE: usize = 8192;

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
    /// the struct only so that [`ExecutionPlan::with_new_children`] can rebuild the file source
    /// against the same shape.
    full_schema: ArrowSchemaRef,
    /// Original projection (kept verbatim for replay through `with_new_children`).
    projection: Option<Vec<usize>>,
    /// Final output schema (projection applied to `full_schema`). What downstream operators see
    /// via [`ExecutionPlan::schema`].
    output_schema: ArrowSchemaRef,
    /// Optional row-count cap from `TableProvider::scan(limit)`. `None` = read to upstream end.
    limit: Option<usize>,
    /// File source with [`TableSchema`] = file fields ++ passthrough fields, projection applied
    /// once via [`FileScanConfigBuilder::with_projection_indices`]. Cloned per-execute into the
    /// per-file [`FileScanConfig`].
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
        let file_count = sink.file_schema.fields().len();
        let passthrough_count = sink.passthrough_columns.len();
        debug_assert_eq!(full_schema.fields().len(), file_count + passthrough_count);
        let file_source = build_file_source(
            sink.file_type,
            &full_schema,
            file_count,
            projection.as_deref(),
        )?;
        // The output schema is derived from the projected file source's table_schema, which
        // carries the kernel-stamped per-field metadata end-to-end (set on the TableSchema in
        // build_file_source, narrowed by `with_projection_indices`).
        let output_schema = match projection.as_ref() {
            Some(proj) => Arc::new(full_schema.project(proj)?),
            None => Arc::clone(&full_schema),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
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
            output_schema,
            limit,
            file_source,
            properties,
        })
    }
}

/// Build a [`FileSource`] for the load's file type whose [`TableSchema`] = file fields ++
/// passthrough fields. Passthrough fields are registered as DataFusion *partition columns*
/// so the opener broadcasts their per-file values from `PartitionedFile.partition_values`
/// (parquet via `replace_columns_with_literals`, json via `ProjectionOpener`). Projection,
/// when present, is pushed straight into the source via [`with_projection_indices`] -- which
/// narrows both the parquet read schema and the partition column materialization.
fn build_file_source(
    file_type: FileType,
    full_schema: &ArrowSchemaRef,
    file_field_count: usize,
    projection: Option<&[usize]>,
) -> DfResult<Arc<dyn datafusion_datasource::file::FileSource>> {
    let (file_fields, passthrough_fields) = full_schema.fields().split_at(file_field_count);
    let file_arrow_schema: ArrowSchemaRef = Arc::new(
        datafusion_common::arrow::datatypes::Schema::new(file_fields.to_vec())
            .with_metadata(full_schema.metadata().clone()),
    );
    // Strip per-field metadata from passthrough fields before handing them to `TableSchema`.
    // DataFusion's partition-col broadcast (`replace_columns_with_literals` / `ProjectionOpener`)
    // synthesizes the output array's `data_type` from the upstream-extracted `ScalarValue`, which
    // never carries the kernel's `delta.columnMapping.*` / `PARQUET:field_id` per-field metadata.
    // Declaring the partition cols with metadata would make the opener's projection output
    // schema disagree with the broadcast array (which DataFusion compares structurally,
    // including nested field metadata), and the opener would fail before producing a batch.
    // The kernel-declared metadata is reapplied per-batch in `assemble_output` via
    // [`stamp_batch_metadata`].
    let stripped_passthrough_fields: Vec<FieldRef> = passthrough_fields
        .iter()
        .map(|f| Arc::new(strip_field_metadata_recursive(f.as_ref())))
        .collect();
    let table_schema = TableSchema::new(file_arrow_schema, stripped_passthrough_fields);
    let source: Arc<dyn datafusion_datasource::file::FileSource> = match file_type {
        FileType::Parquet => Arc::new(ParquetSource::new(table_schema)),
        FileType::Json => {
            // NDJSON; matches the kernel default for delta JSON sidecars/logs.
            Arc::new(JsonSource::new(table_schema))
        }
    };
    let source = source.with_batch_size(DEFAULT_OPENER_BATCH_SIZE);
    // Push projection into the source. `with_projection_indices` calls
    // `FileSource::try_pushdown_projection` which (for both parquet and json) narrows the
    // read + adjusts the partition-column materialization to match.
    let Some(proj) = projection else {
        return Ok(source);
    };
    let projected_config = FileScanConfigBuilder::new(
        // Placeholder URL; `with_projection_indices` only mutates the file_source, not the
        // object_store_url -- we'll set the real URL per file in `start_open_row`.
        ObjectStoreUrl::local_filesystem(),
        source,
    )
    .with_projection_indices(Some(proj.to_vec()))?
    .build();
    Ok(Arc::clone(projected_config.file_source()))
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

    fn as_any(&self) -> &dyn Any {
        self
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
        Ok(Box::pin(LoadExecStream {
            upstream,
            sink: Arc::clone(&self.sink),
            engine: Arc::clone(&self.engine),
            file_source: Arc::clone(&self.file_source),
            output_schema: Arc::clone(&self.output_schema),
            task_context: context,
            remaining: self.limit,
            current_batch: None,
            next_row: 0,
            row_state: RowState::Idle,
        }))
    }
}

#[cfg(test)]
impl LoadExec {
    /// File-schema fields actually retained after projection pushdown -- the subset of
    /// `sink.file_schema` referenced by `self.projection`, in original order. Exposed for
    /// tests asserting pushdown shrunk the read request.
    pub(crate) fn projected_file_fields(&self) -> Vec<String> {
        let file_count = self.sink.file_schema.fields().len();
        let file_names: Vec<String> = self
            .sink
            .file_schema
            .fields()
            .map(|f| f.name().clone())
            .collect();
        match &self.projection {
            Some(proj) => proj
                .iter()
                .copied()
                .filter(|&i| i < file_count)
                .map(|i| file_names[i].clone())
                .collect(),
            None => file_names,
        }
    }

    /// Passthrough fields actually broadcast after projection pushdown -- the subset of
    /// `sink.passthrough_columns` referenced by `self.projection`, in original order. Exposed
    /// for tests.
    pub(crate) fn projected_passthrough_fields(&self) -> Vec<String> {
        let file_count = self.sink.file_schema.fields().len();
        let pt_names: Vec<String> = self
            .sink
            .passthrough_columns
            .iter()
            .map(|c| c.to_string())
            .collect();
        match &self.projection {
            Some(proj) => proj
                .iter()
                .copied()
                .filter(|&i| i >= file_count)
                .map(|i| pt_names[i - file_count].clone())
                .collect(),
            None => pt_names,
        }
    }
}

/// Stream-time state for the row currently being processed.
enum RowState {
    /// No active file. Need to advance to the next upstream row.
    Idle,
    /// Awaiting the [`FileOpenFuture`] for a row's file.
    Opening {
        mask: Option<Vec<bool>>,
        future: FileOpenFuture,
    },
    /// Streaming opener-produced batches for a row's file.
    Scanning {
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
    output_schema: ArrowSchemaRef,
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
    /// Resolve the next upstream row into an `Opening` row state: parse file URL + size,
    /// resolve an optional DV mask via kernel, extract per-row [`ScalarValue`]s for every
    /// passthrough column (stamped onto [`PartitionedFile::partition_values`] for the opener
    /// to broadcast), then construct the DataFusion file opener (with
    /// [`FieldIdPhysicalExprAdapterFactory`] for parquet) and start the file-open future.
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
        let url = resolve_file_location(self.sink.as_ref(), &path_raw)?;

        let mask = optional_selection_vector_for_row(
            batch,
            self.sink.as_ref(),
            row,
            self.engine.as_ref(),
        )?;
        let size = file_size_for_row(batch, self.sink.as_ref(), row, &url)?;

        // Per-row partition_values: one `ScalarValue` per passthrough field declared on
        // `file_source.table_schema().table_partition_cols()`. The opener's
        // `replace_columns_with_literals` (parquet) / `ProjectionOpener` (json) sees these as
        // file-constant literal columns and broadcasts them to every emitted batch. No bespoke
        // per-batch `take` broadcast on our side.
        let partition_values = self.extract_partition_values(batch, row)?;

        let (object_store_url, mut partitioned_file) = into_partitioned_file(&url, size)?;
        partitioned_file.partition_values = partition_values;
        let object_store = self
            .task_context
            .runtime_env()
            .object_store(&object_store_url)?;

        // `create_file_opener` only consults `object_store`, `expr_adapter_factory`, `limit`,
        // `preserve_order` -- not the file groups -- so leaving them empty is fine; we feed
        // the actual file directly into `opener.open(...)`.
        let base_config = FileScanConfigBuilder::new(object_store_url, Arc::clone(&self.file_source))
            .with_expr_adapter(adapter_factory_for(self.sink.file_type))
            .build();
        let opener = self
            .file_source
            .create_file_opener(object_store, &base_config, 0)?;
        let future = opener.open(partitioned_file)?;

        Ok(RowState::Opening { mask, future })
    }

    /// Pre-extract one [`ScalarValue`] per passthrough column declared on the file source's
    /// `table_partition_cols`. Indices line up positionally with the source's partition col
    /// declarations because both came from `sink.passthrough_columns` in original order.
    fn extract_partition_values(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Vec<ScalarValue>, DataFusionError> {
        let pt_fields: &[FieldRef] = self.file_source.table_schema().table_partition_cols();
        let mut out = Vec::with_capacity(pt_fields.len());
        for (cn, _field) in self.sink.passthrough_columns.iter().zip(pt_fields.iter()) {
            let arr = extract_column_array(batch, cn)?;
            out.push(ScalarValue::try_from_array(arr.as_ref(), row)?);
        }
        Ok(out)
    }

    /// Take one opener batch, apply DV mask if present, then re-stamp the kernel-declared
    /// schema metadata onto the result. The opener already broadcast partition values into the
    /// batch and ran logical-name reshape via [`FieldIdPhysicalExprAdapterFactory`], so
    /// nothing else needs to be assembled here.
    fn assemble_output(
        &self,
        rb: RecordBatch,
        mask: Option<&[bool]>,
        file_row_cursor: &mut usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let pre_dv_rows = rb.num_rows();
        let cursor = *file_row_cursor;
        *file_row_cursor += pre_dv_rows;
        let masked = apply_optional_dv(rb, mask, cursor)?;
        crate::exec::stamp_batch_metadata(&masked, &self.output_schema)
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
                    mask,
                    mut stream,
                    mut file_row_cursor,
                } => match stream.as_mut().poll_next(cx) {
                    Poll::Pending => {
                        self.row_state = RowState::Scanning {
                            mask,
                            stream,
                            file_row_cursor,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(Some(Ok(rb))) => {
                        let assembled =
                            match self.assemble_output(rb, mask.as_deref(), &mut file_row_cursor) {
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
                RowState::Opening { mask, mut future } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.row_state = RowState::Opening { mask, future };
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(Ok(stream)) => {
                        self.row_state = RowState::Scanning {
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
        Arc::clone(&self.output_schema)
    }
}

/// Recursively strip per-field metadata from `field` so the resulting field's `data_type`
/// matches structurally what DataFusion's partition-col broadcast (literal-substituted via
/// `ScalarValue`) produces at runtime. Used only for declaring `TableSchema.table_partition_cols`;
/// the kernel's metadata is reapplied per-batch in `assemble_output` via [`stamp_batch_metadata`].
fn strip_field_metadata_recursive(
    field: &delta_kernel::arrow::datatypes::Field,
) -> delta_kernel::arrow::datatypes::Field {
    use delta_kernel::arrow::datatypes::{DataType, Field, Fields};
    let stripped_dt = match field.data_type() {
        DataType::Struct(fs) => DataType::Struct(Fields::from(
            fs.iter()
                .map(|f| strip_field_metadata_recursive(f.as_ref()))
                .collect::<Vec<_>>(),
        )),
        DataType::List(inner) => DataType::List(Arc::new(strip_field_metadata_recursive(inner))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(strip_field_metadata_recursive(inner)))
        }
        DataType::FixedSizeList(inner, n) => {
            DataType::FixedSizeList(Arc::new(strip_field_metadata_recursive(inner)), *n)
        }
        DataType::Map(entry, sorted) => {
            DataType::Map(Arc::new(strip_field_metadata_recursive(entry)), *sorted)
        }
        other => other.clone(),
    };
    Field::new(field.name(), stripped_dt, field.is_nullable())
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
    let object_meta = delta_kernel::object_store::ObjectMeta {
        location: store_path,
        last_modified: chrono::Utc.timestamp_nanos(0),
        size: u64::try_from(size.max(0)).unwrap_or(0),
        e_tag: None,
        version: None,
    };
    Ok((object_store_url, PartitionedFile::new_from_meta(object_meta)))
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
