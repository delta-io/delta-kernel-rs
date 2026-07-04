//! A synchronous, test-only [`PlanExecutor`] backed by [`SyncEngine`] handlers.
//!
//! Wired into [`SyncEngine::plan_executor`]. The query path evaluates a [`Plan`] eagerly,
//! materializing every node's output in full before its consumers run. Streaming is not needed
//! because this executor only serves tests, which never approach memory limits.
//!
//! [`SyncEngine`]: super::SyncEngine
//! [`SyncEngine::plan_executor`]: super::SyncEngine
//
// TODO: The `IoOperation` paths will eventually be used to replace SyncEngine with an
// PlanBasedEngine (backed by this PlanExecutor)

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;

use super::json::try_create_from_json;
use super::parquet::{parquet_footer, try_create_from_parquet};
use super::read_files_arrow;
use super::storage::SyncStorageHandler;
use crate::arrow::array::{new_null_array, Array, ArrayRef, Int64Array, ListArray, RecordBatch};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use crate::engine::arrow_conversion::{TryFromKernel as _, TryIntoArrow as _};
use crate::engine::arrow_data::ArrowEngineData;
use crate::expressions::{ArrayData, ColumnName, Scalar};
use crate::object_store::DynObjectStore;
use crate::plans::ir::nodes::{
    Agg, Aggregate, FileType, Operator, ScanFile, ScanJson, ScanParquet, Values,
};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use crate::schema::{ArrayType, SchemaRef, StructType};
use crate::{DeltaResult, Error, FileMeta, StorageHandler as _};

/// A synchronous, test-only [`PlanExecutor`].
///
/// Scans read files directly through the sync module's Arrow read core ([`read_files_arrow`],
/// [`parquet_footer`]) that backs the [`JsonHandler`] / [`ParquetHandler`] traits, so those
/// handlers can eventually be retired in favor of declarative plans. [`IoOperation`]s still
/// delegate to [`SyncStorageHandler`].
///
/// All I/O is performed synchronously via [`futures::executor::block_on`]; cloud-backed stores are
/// not supported (see [`super`] module docs).
///
/// [`JsonHandler`]: crate::JsonHandler
/// [`ParquetHandler`]: crate::ParquetHandler
pub(crate) struct SyncPlanExecutor {
    storage: SyncStorageHandler,
}

impl SyncPlanExecutor {
    /// Create a `SyncPlanExecutor` over `store`, or over a per-URL [`LocalFileSystem`] when `None`.
    ///
    /// [`LocalFileSystem`]: crate::object_store::local::LocalFileSystem
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        let storage = SyncStorageHandler::new(store);
        Self { storage }
    }
}

// Convenience constructor for tests that don't customize the object store.
impl Default for SyncPlanExecutor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl PlanExecutor for SyncPlanExecutor {
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        match op {
            Operation::IoOperation(io_op) => self.execute_io(io_op),
            Operation::QueryPlan(query) => self.execute_query(query),
        }
    }
}

impl SyncPlanExecutor {
    fn execute_io(&self, op: IoOperation) -> DeltaResult<PlanResult> {
        match op {
            IoOperation::FileListing { url } => {
                // `StorageHandler::list_from` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let metas: Vec<DeltaResult<FileMeta>> = self.storage.list_from(&url)?.collect();
                Ok(PlanResult::FileMeta(Box::new(metas.into_iter())))
            }
            IoOperation::ReadBytes { files } => {
                // `StorageHandler::read_files` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let bytes: Vec<DeltaResult<Bytes>> = self.storage.read_files(files)?.collect();
                Ok(PlanResult::Bytes(Box::new(bytes.into_iter())))
            }
            IoOperation::WriteBytes {
                url,
                data,
                overwrite,
            } => {
                self.storage.put(&url, data, overwrite)?;
                Ok(PlanResult::Unit)
            }
            IoOperation::HeadFile { url } => {
                let meta = self.storage.head(&url)?;
                Ok(PlanResult::FileMeta(Box::new(std::iter::once(Ok(meta)))))
            }
            IoOperation::AtomicCopy {
                source,
                destination,
            } => {
                self.storage.copy_atomic(&source, &destination)?;
                Ok(PlanResult::Unit)
            }
            IoOperation::ParquetFooter { file } => {
                let footer = parquet_footer(self.storage.store(), &file)?;
                Ok(PlanResult::ParquetFooter(footer))
            }
        }
    }

    /// Evaluates `query` by materializing each node's output in slice (topological) order, then
    /// streams the terminal (last) node's batches to the caller.
    fn execute_query(&self, query: Plan) -> DeltaResult<PlanResult> {
        let mut outputs: Vec<Vec<RecordBatch>> = Vec::with_capacity(query.nodes.len());
        for node in query.nodes {
            let output = self.eval_node(node, &outputs)?;
            outputs.push(output);
        }
        let terminal = outputs
            .pop()
            .ok_or_else(|| Error::generic("plan has no nodes"))?;
        let batches = terminal
            .into_iter()
            .map(|batch| Ok(Box::new(ArrowEngineData::new(batch)) as _));
        Ok(PlanResult::Data(Box::new(batches)))
    }

    /// Evaluates a single plan node. `node.inputs` are indices into `outputs`, the
    /// already-materialized results of every prior node, in the node's declared input order.
    fn eval_node(
        &self,
        node: PlanNode,
        outputs: &[Vec<RecordBatch>],
    ) -> DeltaResult<Vec<RecordBatch>> {
        let PlanNode { op, inputs } = node;
        match op {
            Operator::ScanJson(ScanJson {
                files,
                file_constant_columns,
                schema,
            }) => self.eval_scan(FileType::Json, files, file_constant_columns, schema),
            Operator::ScanParquet(ScanParquet {
                files,
                file_constant_columns,
                schema,
            }) => self.eval_scan(FileType::Parquet, files, file_constant_columns, schema),
            Operator::Values(values) => Ok(vec![values_to_record_batch(values)?]),
            Operator::UnionAll(_) => Ok(inputs
                .iter()
                .flat_map(|&i| outputs[i].iter().cloned())
                .collect()),
            Operator::Aggregate(aggregate) => eval_aggregate(&aggregate, &outputs[inputs[0]]),
            other => Err(Error::generic(format!(
                "SyncPlanExecutor does not support Operator::{other}"
            ))),
        }
    }

    /// Reads `files` as `file_type`, broadcasting each file's [`ScanFile::file_constants`] into the
    /// output columns named by `file_constant_columns` (see [`ScanParquet`]). Columns not sourced
    /// from a file constant are read from the file itself.
    ///
    /// Files are read one at a time so each batch stays associated with the file whose constants
    /// must be broadcast onto it.
    fn eval_scan(
        &self,
        file_type: FileType,
        files: Vec<ScanFile>,
        file_constant_columns: Vec<String>,
        schema: SchemaRef,
    ) -> DeltaResult<Vec<RecordBatch>> {
        // The engine reads only the non-constant columns; constants are spliced in afterwards.
        let read_fields = schema
            .fields()
            .filter(|f| !file_constant_columns.contains(f.name()))
            .cloned();
        let read_schema = Arc::new(StructType::try_new(read_fields)?);
        let output_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);

        let store = self.storage.store();
        let mut batches = Vec::new();
        for file in files {
            let metas = [file.meta.clone()];
            let read_schema = read_schema.clone();
            // The two constructors have distinct `impl Iterator` types, so box to unify the arms.
            let data: Box<dyn Iterator<Item = DeltaResult<ArrowEngineData>>> = match file_type {
                FileType::Json => Box::new(read_files_arrow(
                    store,
                    &metas,
                    read_schema,
                    None,
                    try_create_from_json,
                )),
                FileType::Parquet => Box::new(read_files_arrow(
                    store,
                    &metas,
                    read_schema,
                    None,
                    try_create_from_parquet,
                )),
            };
            for batch in data {
                let batch: RecordBatch = batch?.into();
                let columns = splice_file_constants(
                    batch,
                    &schema,
                    &file_constant_columns,
                    &file.file_constants,
                )?;
                batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }
        Ok(batches)
    }
}

/// Builds output columns in `schema` order: each column named in `file_constant_columns` is
/// broadcast from `constants` (at the matching slot), and every other column is drained in turn
/// from `batch`, which holds exactly the non-constant columns in schema order.
fn splice_file_constants(
    batch: RecordBatch,
    schema: &SchemaRef,
    file_constant_columns: &[String],
    constants: &[Scalar],
) -> DeltaResult<Vec<ArrayRef>> {
    let (_, read_columns, rows) = batch.into_parts();
    let mut read_columns = read_columns.into_iter();
    schema
        .fields()
        .map(
            |field| match file_constant_columns.iter().position(|c| c == field.name()) {
                Some(slot) => constants[slot].to_array(rows),
                None => read_columns
                    .next()
                    .ok_or_else(|| Error::generic("scan output has fewer columns than schema")),
            },
        )
        .collect()
}

/// Evaluates an [`Aggregate`], producing exactly one output row.
/// Currently supports only ungrouped MaxNonNullBy aggregates comparing LONG-typed keys.
fn eval_aggregate(aggregate: &Aggregate, input: &[RecordBatch]) -> DeltaResult<Vec<RecordBatch>> {
    if !aggregate.group_by.is_empty() {
        return Err(Error::generic(
            "SyncPlanExecutor does not support grouped Aggregate",
        ));
    }
    let mut fields = Vec::with_capacity(aggregate.aggs.len());
    let mut columns = Vec::with_capacity(aggregate.aggs.len());
    // Output schema lists aggregate columns in order (no group keys), so zip aligns each agg with
    // its output field (which already carries any alias).
    for (agg, field) in aggregate.aggs.iter().zip(aggregate.schema.fields()) {
        let Agg::MaxNonNullBy { value, key } = agg else {
            return Err(Error::generic(
                "SyncPlanExecutor only supports the max_non_null_by aggregate",
            ));
        };
        let data_type = ArrowDataType::try_from_kernel(field.data_type())?;
        columns.push(max_non_null_by(input, value, key, &data_type)?);
        fields.push(ArrowField::new(field.name(), data_type, true));
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)?;
    Ok(vec![batch])
}

/// The `value` from the input row with the greatest `key`, considering only rows where both
/// `value` and `key` are non-null (see [`Agg::max_non_null_by`]). Returns a one-row array: the
/// winning value, or NULL (typed by `output_type`) when no row qualifies.
///
/// Extraction is deferred: the winning `(column, row)` is tracked as batches stream by, then sliced
/// once at the end -- avoiding a per-candidate copy of the (possibly struct-typed) value. Grouped
/// aggregation would generalize this to one `arrow::compute::take` over the winning indices.
///
/// [`Agg::max_non_null_by`]: crate::plans::ir::nodes::Agg::max_non_null_by
fn max_non_null_by(
    input: &[RecordBatch],
    value: &ColumnName,
    key: &ColumnName,
    output_type: &ArrowDataType,
) -> DeltaResult<ArrayRef> {
    let value = single_column(value)?;
    let key = single_column(key)?;
    let mut best: Option<(ArrayRef, usize, i64)> = None;
    for batch in input {
        let values = column(batch, value)?;
        let keys = column(batch, key)?;
        let keys = keys
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::generic("max_non_null_by: key column must be a LONG"))?;
        for row in 0..batch.num_rows() {
            if values.is_null(row) || keys.is_null(row) {
                continue;
            }
            let candidate = keys.value(row);
            if matches!(best, Some((_, _, best_key)) if candidate <= best_key) {
                continue;
            }
            best = Some((values.clone(), row, candidate));
        }
    }
    match best {
        Some((values, row, _)) => Ok(values.slice(row, 1)),
        None => Ok(new_null_array(output_type, 1)),
    }
}

/// The single path segment of a top-level column, erroring on nested columns (unsupported here).
fn single_column(name: &ColumnName) -> DeltaResult<&str> {
    match name.path() {
        [segment] => Ok(segment.as_str()),
        _ => Err(Error::generic(format!(
            "SyncPlanExecutor does not support nested column `{name}`"
        ))),
    }
}

/// Looks up `name` in `batch`, erroring if absent.
fn column<'a>(batch: &'a RecordBatch, name: &str) -> DeltaResult<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::generic(format!("column `{name}` not found")))
}

/// Materialize a [`Values`] node's literal rows into a [`RecordBatch`]. An empty relation (the
/// [`PlanBuilder::build`] output for an absent input) yields zero-row data.
///
/// [`PlanBuilder::build`]: crate::plans::PlanBuilder::build
fn values_to_record_batch(values: Values) -> DeltaResult<RecordBatch> {
    let Values { schema, rows } = values;
    let columns: Vec<ArrayRef> = schema
        .fields()
        .enumerate()
        .map(|(col, field)| -> DeltaResult<ArrayRef> {
            let element_type = ArrayType::new(field.data_type().clone(), true);
            let column = ArrayData::try_new(element_type, rows.iter().map(|row| row[col].clone()))?;
            // This produces a single array row. The array contains n elements, one for each
            // attribute of the column.
            let list = Scalar::Array(column).to_array(1)?;
            let list = list.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                Error::generic("Values: Scalar::Array did not lower to a ListArray")
            })?;
            let (_field, _offsets, values, _nulls) = list.clone().into_parts();
            Ok(values)
        })
        .try_collect()?;
    let schema = Arc::new(schema.as_ref().try_into_arrow()?);
    Ok(RecordBatch::try_new(schema, columns)?)
}
