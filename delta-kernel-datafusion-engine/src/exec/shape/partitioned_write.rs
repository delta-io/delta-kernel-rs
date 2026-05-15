//! Hive-style partitioned file sink: route upstream batches into nested directories under a base
//! [`url::Url`] and emit Parquet or newline-delimited JSON.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_json::LineDelimitedWriter;
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use delta_kernel::arrow::array::{ArrayRef, AsArray, RecordBatch, UInt32Array};
use delta_kernel::arrow::compute::{cast, take};
use delta_kernel::arrow::datatypes::{DataType, SchemaRef};
use delta_kernel::plans::errors::DeltaError;
use delta_kernel::plans::ir::nodes::{PartitionedWriteSink, WriteFileFormat};
use futures::{ready, Stream, StreamExt};
use parquet::arrow::ArrowWriter;
use url::Url;

/// Writes partitioned Parquet / JSON-lines under [`PartitionedWriteSink::destination`].
#[derive(Debug)]
pub struct KernelPartitionedWriteExec {
    child: Arc<dyn ExecutionPlan>,
    sink: PartitionedWriteSink,
    partition_indices: Vec<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl KernelPartitionedWriteExec {
    pub fn try_new(
        child: Arc<dyn ExecutionPlan>,
        sink: PartitionedWriteSink,
    ) -> Result<Self, DeltaError> {
        let schema = child.schema();
        let mut seen = HashSet::new();
        let mut partition_indices = Vec::with_capacity(sink.partition_columns.len());
        for name in &sink.partition_columns {
            if !seen.insert(name.as_str()) {
                return Err(crate::error::plan_compilation(format!(
                    "duplicate partition column name `{name}`"
                )));
            }
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name().as_str() == name.as_str())
                .ok_or_else(|| {
                    crate::error::plan_compilation(format!(
                        "partition column `{name}` not found in output schema"
                    ))
                })?;
            partition_indices.push(idx);
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            child.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            child,
            sink,
            partition_indices,
            schema,
            properties,
        })
    }
}

impl DisplayAs for KernelPartitionedWriteExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KernelPartitionedWriteExec(format={:?}, parts={:?})",
            self.sink.format, self.sink.partition_columns
        )
    }
}

impl ExecutionPlan for KernelPartitionedWriteExec {
    fn name(&self) -> &str {
        "KernelPartitionedWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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
        let child = super::expect_single_child(children, "KernelPartitionedWriteExec")?;
        Ok(Arc::new(
            KernelPartitionedWriteExec::try_new(child, self.sink.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "KernelPartitionedWriteExec supports partition 0 only, got {partition}",
            )));
        }
        let inner = self.child.execute(partition, context)?;
        let base_dir = destination_base_dir(&self.sink.destination)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::pin(PartitionedWriteStream {
            inner,
            sink: self.sink.clone(),
            partition_indices: self.partition_indices.clone(),
            schema: self.schema.clone(),
            base_dir,
            writers: HashMap::new(),
            finished: false,
        }))
    }
}

fn destination_base_dir(url: &Url) -> Result<PathBuf, DeltaError> {
    match url.scheme() {
        "file" => url
            .to_file_path()
            .map_err(|_| crate::error::unsupported(format!(
                "PartitionedWrite destination is not a usable file path: {url}"
            ))),
        other => Err(crate::error::unsupported(format!(
            "PartitionedWrite for `{other}` URLs is not implemented in the DataFusion engine (only file://)",
        ))),
    }
}

/// Minimal escaping for Hive-style path segments (covers `/`, `\`, control chars, `=`, `%`, space).
fn encode_hive_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '/' | '\\' | '\n' | '\r' | '\t' => push_pct(&mut out, ch),
            '=' => out.push_str("%3D"),
            '%' => out.push_str("%25"),
            ' ' => out.push_str("%20"),
            ':' => out.push_str("%3A"),
            _ => out.push(ch),
        }
    }
    out
}

fn push_pct(out: &mut String, ch: char) {
    let mut buf = [0u8; 4];
    let encoded = ch.encode_utf8(&mut buf);
    for byte in encoded.bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "%{byte:02X}");
    }
}

fn utf8_arrays_for_partition_cols(
    batch: &RecordBatch,
    indices: &[usize],
) -> Result<Vec<ArrayRef>, DeltaError> {
    indices
        .iter()
        .map(|&col_idx| {
            cast(batch.column(col_idx).as_ref(), &DataType::Utf8).map_err(|e| {
                crate::error::plan_compilation(format!(
                    "could not cast partition column `{}` to Utf8: {e}",
                    batch.schema().field(col_idx).name()
                ))
            })
        })
        .collect()
}

fn partition_key_row(arrays: &[ArrayRef], row: usize) -> Result<Vec<String>, DeltaError> {
    let mut parts = Vec::with_capacity(arrays.len());
    for arr in arrays {
        if arr.is_null(row) {
            return Err(crate::error::unsupported(
                "null partition key values are not supported for PartitionedWrite in the DataFusion engine",
            ));
        }
        let s = arr.as_string::<i32>();
        parts.push(s.value(row).to_string());
    }
    Ok(parts)
}

fn hive_dir_relative(partition_cols: &[String], values: &[String]) -> PathBuf {
    debug_assert_eq!(partition_cols.len(), values.len());
    let mut p = PathBuf::new();
    for (col, val) in partition_cols.iter().zip(values) {
        p.push(format!("{}={}", col, encode_hive_segment(val)));
    }
    p
}

fn split_batch_by_partition(
    batch: &RecordBatch,
    sink: &PartitionedWriteSink,
    partition_indices: &[usize],
) -> Result<Vec<(PathBuf, RecordBatch)>, DeltaError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let utf8_parts = utf8_arrays_for_partition_cols(batch, partition_indices)?;

    let mut groups: HashMap<Vec<String>, Vec<u32>> = HashMap::new();
    for row in 0..batch.num_rows() {
        let key = partition_key_row(&utf8_parts, row)?;
        groups.entry(key).or_default().push(row as u32);
    }

    let mut out = Vec::with_capacity(groups.len());
    for (key_vals, idxs) in groups {
        let rel = hive_dir_relative(&sink.partition_columns, &key_vals);
        let indices = UInt32Array::from(idxs);
        let cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| {
                take(c.as_ref(), &indices, None).map_err(|e| {
                    crate::error::internal_error(format!("take rows for partitioned write: {e}"))
                })
            })
            .collect::<Result<_, _>>()?;
        let slice = RecordBatch::try_new(batch.schema(), cols)
            .map_err(|e| crate::error::internal_error(format!("partition slice batch: {e}")))?;
        out.push((rel, slice));
    }
    Ok(out)
}

enum OpenWriter {
    Parquet(ArrowWriter<File>),
    JsonLines(LineDelimitedWriter<BufWriter<File>>),
}

impl OpenWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DeltaError> {
        match self {
            OpenWriter::Parquet(w) => w
                .write(batch)
                .map_err(|e| crate::error::internal_error(format!("parquet write: {e}"))),
            OpenWriter::JsonLines(w) => w
                .write(batch)
                .map_err(|e| crate::error::internal_error(format!("json lines write: {e}"))),
        }
    }

    fn finish(self) -> Result<(), DeltaError> {
        match self {
            OpenWriter::Parquet(w) => w
                .close()
                .map(|_| ())
                .map_err(|e| crate::error::internal_error(format!("parquet close: {e}"))),
            OpenWriter::JsonLines(mut w) => w
                .finish()
                .map_err(|e| crate::error::internal_error(format!("json lines finish: {e}"))),
        }
    }
}

fn open_writer_for_partition(
    base: &Path,
    rel: &Path,
    format: WriteFileFormat,
    schema: SchemaRef,
) -> Result<OpenWriter, DeltaError> {
    let dir = base.join(rel);
    fs::create_dir_all(&dir).map_err(|e| {
        crate::error::internal_error(format!("create partition directory {}: {e}", dir.display()))
    })?;

    match format {
        WriteFileFormat::Parquet => {
            let path = dir.join("part-000.parquet");
            let file = File::create(&path).map_err(|e| {
                crate::error::internal_error(format!("create {}: {e}", path.display()))
            })?;
            let writer = ArrowWriter::try_new(file, schema, None)
                .map_err(|e| crate::error::internal_error(format!("ArrowWriter::try_new: {e}")))?;
            Ok(OpenWriter::Parquet(writer))
        }
        WriteFileFormat::JsonLines => {
            let path = dir.join("part-000.jsonl");
            let file = File::create(&path).map_err(|e| {
                crate::error::internal_error(format!("create {}: {e}", path.display()))
            })?;
            let w = LineDelimitedWriter::new(BufWriter::new(file));
            Ok(OpenWriter::JsonLines(w))
        }
    }
}

struct PartitionedWriteStream {
    inner: SendableRecordBatchStream,
    sink: PartitionedWriteSink,
    partition_indices: Vec<usize>,
    schema: SchemaRef,
    base_dir: PathBuf,
    writers: HashMap<PathBuf, OpenWriter>,
    finished: bool,
}

impl Stream for PartitionedWriteStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let splits =
                        match split_batch_by_partition(&batch, &self.sink, &self.partition_indices)
                        {
                            Ok(s) => s,
                            Err(e) => {
                                return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                                    e,
                                )))))
                            }
                        };
                    for (rel, slice) in splits {
                        if let Some(w) = self.writers.get_mut(&rel) {
                            if let Err(err) = w.write_batch(&slice) {
                                return Poll::Ready(Some(Err(DataFusionError::External(
                                    Box::new(err),
                                ))));
                            }
                        } else {
                            let mut w = match open_writer_for_partition(
                                &self.base_dir,
                                &rel,
                                self.sink.format,
                                Arc::clone(&self.schema),
                            ) {
                                Ok(w) => w,
                                Err(err) => {
                                    return Poll::Ready(Some(Err(DataFusionError::External(
                                        Box::new(err),
                                    ))));
                                }
                            };
                            if let Err(err) = w.write_batch(&slice) {
                                return Poll::Ready(Some(Err(DataFusionError::External(
                                    Box::new(err),
                                ))));
                            }
                            self.writers.insert(rel, w);
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    let writers = std::mem::take(&mut self.writers);
                    for (_, w) in writers {
                        if let Err(err) = w.finish() {
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(
                                err,
                            )))));
                        }
                    }
                    self.finished = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for PartitionedWriteStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
