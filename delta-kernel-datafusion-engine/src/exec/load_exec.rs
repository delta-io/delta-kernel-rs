//! [`LoadExec`] -- the streaming physical plan that powers
//! [`super::LoadTableProvider`].
//!
//! For each upstream metadata batch, [`build_load_stream`] walks rows one at a time. For each
//! row it resolves a file URL + optional DV mask + per-row passthrough column values, then
//! opens the file via DataFusion's [`FileSource::create_file_opener`] + [`FileOpener::open`]
//! -- the *same* parquet/json opener `ListingTable` uses. Each opener-produced batch is
//! yielded downstream as soon as it's ready; there is no per-batch / per-file accumulation.
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
//! * Passthrough columns (kernel metadata-side values like `_metadata`, `path`) are handed to the
//!   opener as [`TableSchema::with_table_partition_cols`]. Per upstream row we extract the
//!   corresponding [`ScalarValue`]s and stamp them onto [`PartitionedFile::partition_values`]; the
//!   [`ProjectionOpener`] (parquet) / opener wrapper (json) substitutes them as literal columns
//!   when materializing the file's batches. No bespoke per-batch broadcast.
//! * `projection: Option<Vec<usize>>` is pushed straight into the [`FileSource`] via
//!   [`FileScanConfigBuilder::with_projection_indices`]; the source then narrows both the parquet
//!   read schema and which partition columns it materializes.
//! * `limit: Option<usize>` caps total emitted rows. Once a batch would overshoot the budget it is
//!   sliced; the next stream iteration returns end-of-stream without opening any further files.
//!
//! Filter pushdown is intentionally left to upstream operators (see
//! [`super::LoadTableProvider::supports_filters_pushdown`]).

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use chrono::TimeZone;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{ListingTableUrl, PartitionedFile, TableSchema};
use datafusion_datasource_json::source::JsonSource;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::types::Int64Type;
use delta_kernel::arrow::array::{Array, AsArray, RecordBatch};
use delta_kernel::arrow::datatypes::{FieldRef, SchemaRef as ArrowSchemaRef};
use delta_kernel::plans::ir::nodes::{FileType, LoadSink};
use delta_kernel::Engine;
use futures::stream::{self, BoxStream, Stream};
use futures::StreamExt;
use url::Url;

use crate::exec::field_id_adapter::FieldIdPhysicalExprAdapterFactory;
use crate::exec::load_helpers::{
    apply_optional_dv, extract_column_array, optional_selection_vector_for_row,
    resolve_file_location,
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
        let stream = build_load_stream(
            upstream,
            Arc::clone(&self.sink),
            Arc::clone(&self.engine),
            Arc::clone(&self.file_source),
            Arc::clone(&self.output_schema),
            context,
            self.limit,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            stream,
        )))
    }
}

#[cfg(test)]
impl LoadExec {
    /// File-schema fields actually retained after projection pushdown -- the subset of
    /// `sink.file_schema` referenced by `self.projection`, in original order. Exposed for
    /// tests asserting pushdown shrunk the read request.
    pub(crate) fn projected_file_fields(&self) -> Vec<String> {
        let file_count = self.sink.file_schema.fields().len();
        let names: Vec<String> = self
            .sink
            .file_schema
            .fields()
            .map(|f| f.name().clone())
            .collect();
        self.project_names(&names, |i| i < file_count, |i| i)
    }

    /// Passthrough fields actually broadcast after projection pushdown -- the subset of
    /// `sink.passthrough_columns` referenced by `self.projection`, in original order. Exposed
    /// for tests.
    pub(crate) fn projected_passthrough_fields(&self) -> Vec<String> {
        let file_count = self.sink.file_schema.fields().len();
        let names: Vec<String> = self
            .sink
            .passthrough_columns
            .iter()
            .map(|c| c.to_string())
            .collect();
        self.project_names(&names, |i| i >= file_count, |i| i - file_count)
    }

    /// Shared logic for the two `projected_*_fields` helpers: when `self.projection` is set,
    /// keep indices passing `keep` and map them through `to_local` before indexing `names`;
    /// when absent, return `names` verbatim.
    fn project_names(
        &self,
        names: &[String],
        keep: impl Fn(usize) -> bool,
        to_local: impl Fn(usize) -> usize,
    ) -> Vec<String> {
        match &self.projection {
            Some(proj) => proj
                .iter()
                .copied()
                .filter(|&i| keep(i))
                .map(|i| names[to_local(i)].clone())
                .collect(),
            None => names.to_vec(),
        }
    }
}

/// An opened file's running batch stream + DV mask + file-local row cursor. Built by
/// [`open_file_for_row`] and held in [`LoadStreamState::active`] while we drain it.
struct ActiveFile {
    mask: Option<Vec<bool>>,
    stream: BoxStream<'static, DfResult<RecordBatch>>,
    /// File-local row cursor, advanced by each opener batch we apply DV to.
    file_row_cursor: usize,
}

/// State carried through [`stream::unfold`] in [`build_load_stream`]: upstream + per-execute
/// kernel/source handles + the limit budget + the currently-open file (if any) + the
/// upstream-batch row walker. Each iteration yields zero or one [`RecordBatch`] and rethreads
/// the state.
struct LoadStreamState {
    upstream: SendableRecordBatchStream,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    file_source: Arc<dyn datafusion_datasource::file::FileSource>,
    output_schema: ArrowSchemaRef,
    task_context: Arc<TaskContext>,
    /// Remaining row budget. `None` = unbounded. When `Some(0)` the stream is finished.
    remaining: Option<usize>,
    /// Currently-open file we're streaming batches from, if any.
    active: Option<ActiveFile>,
    /// Current upstream batch + next row index within it. `None` between batches.
    cursor: Option<(RecordBatch, usize)>,
}

/// Build the [`LoadExec`] output stream: walk upstream rows, open one file per row through the
/// configured [`FileSource`] opener, then yield each opener-produced batch (DV-masked +
/// metadata-restamped) downstream. Returns an `impl Stream` so callers wrap it in a
/// [`RecordBatchStreamAdapter`] to attach the output schema.
fn build_load_stream(
    upstream: SendableRecordBatchStream,
    sink: Arc<LoadSink>,
    engine: Arc<dyn Engine>,
    file_source: Arc<dyn datafusion_datasource::file::FileSource>,
    output_schema: ArrowSchemaRef,
    task_context: Arc<TaskContext>,
    limit: Option<usize>,
) -> impl Stream<Item = DfResult<RecordBatch>> {
    let state = LoadStreamState {
        upstream,
        sink,
        engine,
        file_source,
        output_schema,
        task_context,
        remaining: limit,
        active: None,
        cursor: None,
    };
    stream::unfold(state, |mut s| async move {
        loop {
            // 0) Limit exhausted? End-of-stream without doing any more I/O.
            if matches!(s.remaining, Some(0)) {
                return None;
            }
            // 1) If a file is currently open, pull its next batch.
            if let Some(active) = s.active.as_mut() {
                match active.stream.next().await {
                    Some(Ok(rb)) => {
                        let assembled = match assemble_output(
                            rb,
                            active.mask.as_deref(),
                            &mut active.file_row_cursor,
                            &s.output_schema,
                        ) {
                            Ok(b) => b,
                            Err(e) => return Some((Err(e), s)),
                        };
                        let mut out = assembled;
                        if let Some(rem) = s.remaining.as_mut() {
                            if out.num_rows() > *rem {
                                out = out.slice(0, *rem);
                            }
                            *rem -= out.num_rows();
                        }
                        return Some((Ok(out), s));
                    }
                    Some(Err(e)) => return Some((Err(e), s)),
                    None => s.active = None, // file exhausted; advance to next row
                }
            }
            // 2) Open the next upstream row's file, pulling more upstream batches as needed. Loop
            //    body either opens a file (breaks the inner loop and the outer continues so the new
            //    file gets drained on the next iteration), or hits upstream EOF (returns None,
            //    ending the unfold).
            let row_to_open: Option<(RecordBatch, usize)> = loop {
                if let Some((batch, row)) = s.cursor.as_mut() {
                    if *row < batch.num_rows() {
                        let r = *row;
                        *row += 1;
                        // `RecordBatch` clone is shallow (Arc-shared columns) so this doesn't
                        // copy any data.
                        break Some((batch.clone(), r));
                    }
                    s.cursor = None;
                }
                match s.upstream.next().await {
                    Some(Ok(b)) => s.cursor = Some((b, 0)),
                    Some(Err(e)) => return Some((Err(e), s)),
                    None => break None,
                }
            };
            let (batch, row) = row_to_open?;
            match open_file_for_row(
                &batch,
                row,
                s.sink.as_ref(),
                s.engine.as_ref(),
                &s.file_source,
                &s.task_context,
            )
            .await
            {
                Ok(active) => s.active = Some(active),
                Err(e) => return Some((Err(e), s)),
            }
            // continue outer loop to start draining the file we just opened
        }
    })
}

/// Resolve one upstream row into an [`ActiveFile`]: parse file URL + size, resolve an optional
/// DV mask via kernel, extract per-row [`ScalarValue`]s for every passthrough column (stamped
/// onto [`PartitionedFile::partition_values`] for the opener to broadcast), then construct the
/// DataFusion file opener (with [`FieldIdPhysicalExprAdapterFactory`] for parquet) and await
/// its file-open future to get back the running batch stream.
async fn open_file_for_row(
    batch: &RecordBatch,
    row: usize,
    sink: &LoadSink,
    engine: &dyn Engine,
    file_source: &Arc<dyn datafusion_datasource::file::FileSource>,
    task_context: &Arc<TaskContext>,
) -> DfResult<ActiveFile> {
    let path_cn = &sink.file_meta.path;
    let path_arr = extract_column_array(batch, path_cn)?;
    if path_arr.is_null(row) {
        return Err(crate::error::plan_compilation(format!(
            "Load sink path column `{path_cn}` was NULL at upstream row {row}"
        )));
    }
    // Path columns are always Utf8 per kernel's scan_live_actions_schema (STRING).
    let path_raw = path_arr.as_string::<i32>().value(row);
    let url = resolve_file_location(sink, path_raw)?;

    let mask = optional_selection_vector_for_row(batch, sink, row, engine)?;
    let size = file_size_for_row(batch, sink, row, &url)?;

    // Per-row partition_values: one `ScalarValue` per passthrough field declared on
    // `file_source.table_schema().table_partition_cols()`. The opener's
    // `replace_columns_with_literals` (parquet) / `ProjectionOpener` (json) sees these as
    // file-constant literal columns and broadcasts them to every emitted batch. No bespoke
    // per-batch `take` broadcast on our side.
    let partition_values = extract_partition_values(batch, row, sink)?;

    let (object_store_url, mut partitioned_file) = into_partitioned_file(&url, size)?;
    partitioned_file.partition_values = partition_values;
    let object_store = task_context.runtime_env().object_store(&object_store_url)?;

    // `create_file_opener` only consults `object_store`, `expr_adapter_factory`, `limit`,
    // `preserve_order` -- not the file groups -- so leaving them empty is fine; we feed the
    // actual file directly into `opener.open(...)`.
    let base_config = FileScanConfigBuilder::new(object_store_url, Arc::clone(file_source))
        .with_expr_adapter(adapter_factory_for(sink.file_type))
        .build();
    let opener = file_source.create_file_opener(object_store, &base_config, 0)?;
    let stream = opener.open(partitioned_file)?.await?;
    Ok(ActiveFile {
        mask,
        stream,
        file_row_cursor: 0,
    })
}

/// Pre-extract one [`ScalarValue`] per passthrough column declared on the file source's
/// `table_partition_cols`. Indices line up positionally with the source's partition col
/// declarations because both came from `sink.passthrough_columns` in original order.
fn extract_partition_values(
    batch: &RecordBatch,
    row: usize,
    sink: &LoadSink,
) -> DfResult<Vec<ScalarValue>> {
    sink.passthrough_columns
        .iter()
        .map(|cn| {
            let arr = extract_column_array(batch, cn)?;
            ScalarValue::try_from_array(arr.as_ref(), row)
        })
        .collect()
}

/// Take one opener batch, apply DV mask if present, then re-stamp the kernel-declared schema
/// metadata onto the result. The opener already broadcast partition values into the batch and
/// ran logical-name reshape via [`FieldIdPhysicalExprAdapterFactory`], so nothing else needs to
/// be assembled here.
fn assemble_output(
    rb: RecordBatch,
    mask: Option<&[bool]>,
    file_row_cursor: &mut usize,
    output_schema: &ArrowSchemaRef,
) -> DfResult<RecordBatch> {
    let pre_dv_rows = rb.num_rows();
    let cursor = *file_row_cursor;
    *file_row_cursor += pre_dv_rows;
    let masked = apply_optional_dv(rb, mask, cursor)?;
    crate::exec::stamp_batch_metadata(&masked, output_schema)
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

/// Select the
/// [`PhysicalExprAdapterFactory`](datafusion_physical_expr_adapter::PhysicalExprAdapterFactory) for
/// a given file type. Parquet decoding goes through field-id/column-mapping aware adaptation; JSON
/// keeps DataFusion's default name-based adapter (we don't ship column-mapped JSON loads today).
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
    Ok((
        object_store_url,
        PartitionedFile::new_from_meta(object_meta),
    ))
}

fn file_size_for_row(
    batch: &RecordBatch,
    sink: &LoadSink,
    row: usize,
    url: &Url,
) -> Result<i64, DataFusionError> {
    if let Some(sz_cn) = sink.file_meta.size.as_ref() {
        // Size columns are always Int64 per kernel's scan_live_actions_schema (LONG).
        let arr = extract_column_array(batch, sz_cn)?;
        return Ok(if arr.is_null(row) {
            0
        } else {
            arr.as_primitive::<Int64Type>().value(row)
        });
    }
    if url.scheme() == "file" {
        let p = url.to_file_path().map_err(|()| {
            crate::error::plan_compilation(format!(
                "file URL could not be converted to path: {url}"
            ))
        })?;
        return Ok(std::fs::metadata(&p).map(|m| m.len() as i64).unwrap_or(0));
    }
    Ok(0)
}
