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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use itertools::Itertools;
use url::Url;

use super::aggs::eval_aggregate;
use super::json::try_create_from_json;
use super::parquet::{parquet_footer, try_create_from_parquet};
use super::storage::SyncStorageHandler;
use super::{put_bytes, read_files_arrow};
use crate::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray,
};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::Schema as ArrowSchema;
use crate::arrow::row::{OwnedRow, RowConverter, SortField};
use crate::engine::arrow_conversion::{TryFromArrow as _, TryIntoArrow as _};
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::arrow_expression::evaluate_expression::extract_column_ref;
use crate::engine::arrow_expression::{extract_column, ArrowEvaluationHandler};
use crate::engine::arrow_utils::{coerce_columns_to_schema, to_json_bytes};
use crate::engine::writer_options;
use crate::expressions::{ArrayData, ColumnName, PredicateRef, Scalar};
use crate::object_store::DynObjectStore;
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::plans::ir::nodes::{
    DynamicScan, FileType, Operator, Project, RelationId, RelationRef, ScanFile, ScanJson,
    ScanParquet, SemiJoin, Values, WriteJson, WriteParquet, FILE_META_SCHEMA,
};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult, ScopedPlanExecutor};
use crate::schema::{ArrayType, DataType, SchemaRef, StructType};
use crate::{
    EvaluationHandler as _, FileMeta, FilteredEngineData, KernelError, KernelResult,
    KernelResultIteratorStatic, Result, StorageHandler as _,
};

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
#[derive(Clone)]
pub(crate) struct SyncPlanExecutor {
    storage: Arc<SyncStorageHandler>,
}

// Distinct IDs prevent a ref from another scope from resolving to a local relation.
static NEXT_RELATION_ID: AtomicU64 = AtomicU64::new(0);

/// A synchronous plan executor that retains relations for its own lifetime.
pub(crate) struct ScopedSyncPlanExecutor {
    executor: SyncPlanExecutor,
    relation_store: RelationStore,
}

type RelationStore = Mutex<HashMap<RelationId, RetainedRelation>>;

struct RetainedRelation {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl SyncPlanExecutor {
    /// Create a `SyncPlanExecutor` over `store`, or over a per-URL [`LocalFileSystem`] when `None`.
    ///
    /// [`LocalFileSystem`]: crate::object_store::local::LocalFileSystem
    pub(crate) fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self {
            storage: Arc::new(SyncStorageHandler::new(store)),
        }
    }

    /// Returns an executor that retains relations until the scope is dropped.
    pub(crate) fn get_scoped(&self) -> ScopedSyncPlanExecutor {
        ScopedSyncPlanExecutor {
            executor: self.clone(),
            relation_store: Mutex::new(HashMap::new()),
        }
    }
}

impl ScopedSyncPlanExecutor {
    /// Retains an Arrow batch and returns a ref readable until this scope is dropped.
    /// `name` is prepended to the relation ID as a debug hint.
    ///
    /// Returns an error if `schema` differs from the batch schema or retention fails.
    pub(crate) fn create_ref(
        &self,
        name: &str,
        schema: SchemaRef,
        record_batch: RecordBatch,
    ) -> KernelResult<RelationRef> {
        retain_relation(&self.relation_store, name, schema, vec![record_batch])
    }
}

// Convenience constructor for tests that don't customize the object store.
impl Default for SyncPlanExecutor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl PlanExecutor for SyncPlanExecutor {
    fn execute_op(&self, op: Operation) -> Result<PlanResult> {
        self.execute_op_with_relation_store(op, None)
    }
}

impl PlanExecutor for ScopedSyncPlanExecutor {
    fn execute_op(&self, op: Operation) -> Result<PlanResult> {
        self.executor
            .execute_op_with_relation_store(op, Some(&self.relation_store))
    }
}

impl ScopedPlanExecutor for ScopedSyncPlanExecutor {
    fn execute_and_retain(&self, name: &str, plan: Plan) -> Result<RelationRef> {
        let schema = Arc::clone(&plan.schema);
        let batches = self.executor.eval_plan(plan, Some(&self.relation_store))?;
        retain_relation(&self.relation_store, name, schema, batches)
    }
}

impl SyncPlanExecutor {
    fn execute_op_with_relation_store(
        &self,
        op: Operation,
        relation_store: Option<&RelationStore>,
    ) -> KernelResult<PlanResult> {
        match op {
            Operation::IoOperation(io_op) => self.execute_io(io_op),
            Operation::QueryPlan(query) => self.execute_query(query, relation_store),
        }
    }

    fn execute_io(&self, op: IoOperation) -> KernelResult<PlanResult> {
        match op {
            IoOperation::FileListing { url } => {
                // `StorageHandler::list_from` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let metas: Vec<KernelResult<FileMeta>> = self.storage.list_from(&url)?.collect();
                Ok(PlanResult::FileMeta(Box::new(metas.into_iter())))
            }
            IoOperation::ReadBytes { files } => {
                // `StorageHandler::read_files` returns a non-`Send` iterator, so we collect into
                // a `Vec` first to convert into a `Send` iterator.
                // TODO(#2619): Evaluate whether StorageHandler should just return `Send` iterators
                let bytes: Vec<KernelResult<Bytes>> = self.storage.read_files(files)?.collect();
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
    fn execute_query(
        &self,
        query: Plan,
        relation_store: Option<&RelationStore>,
    ) -> KernelResult<PlanResult> {
        let terminal = self.eval_plan(query, relation_store)?;
        let batches = terminal
            .into_iter()
            .map(|batch| Ok(Box::new(ArrowEngineData::new(batch)) as _));
        Ok(PlanResult::Data(Box::new(batches)))
    }

    fn eval_plan(
        &self,
        plan: Plan,
        relation_store: Option<&RelationStore>,
    ) -> KernelResult<Vec<RecordBatch>> {
        let mut outputs: Vec<Vec<RecordBatch>> = Vec::with_capacity(plan.nodes.len());
        for node in plan.nodes {
            let output = self.eval_node(node, &outputs, relation_store)?;
            outputs.push(output);
        }
        outputs
            .pop()
            .ok_or_else(|| KernelError::generic("plan has no nodes"))
    }

    /// Evaluates a single plan node. `node.inputs` are indices into `outputs`, the
    /// already-materialized results of every prior node, in the node's declared input order.
    fn eval_node(
        &self,
        node: PlanNode,
        results: &[Vec<RecordBatch>],
        relation_store: Option<&RelationStore>,
    ) -> KernelResult<Vec<RecordBatch>> {
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
            Operator::RelationSource(relation_ref) => {
                self.eval_relation_source(&relation_ref, relation_store)
            }
            Operator::UnionAll(_) => Ok(Vec::from_iter(
                inputs.iter().flat_map(|&i| results[i].iter().cloned()),
            )),
            Operator::Project(project) => eval_project(project, &results[inputs[0]]),
            Operator::Filter(filter) => eval_filter(filter.predicate, &results[inputs[0]]),
            Operator::DynamicScan(dynamic_scan) => {
                self.eval_dynamic_scan(dynamic_scan, &results[inputs[0]])
            }
            Operator::Aggregate(aggregate) => eval_aggregate(&aggregate, &results[inputs[0]]),
            Operator::WriteJson(WriteJson {
                schema,
                file_path,
                overwrite,
            }) => self.eval_write(
                FileType::Json,
                &schema,
                &file_path,
                overwrite,
                &results[inputs[0]],
            ),
            Operator::WriteParquet(WriteParquet {
                schema,
                file_path,
                overwrite,
            }) => self.eval_write(
                FileType::Parquet,
                &schema,
                &file_path,
                overwrite,
                &results[inputs[0]],
            ),
            Operator::SemiJoin(join) => {
                eval_semi_join(join, &results[inputs[0]], &results[inputs[1]])
            }
        }
    }

    fn eval_write(
        &self,
        file_type: FileType,
        schema: &SchemaRef,
        file_path: &Url,
        overwrite: bool,
        input: &[RecordBatch],
    ) -> KernelResult<Vec<RecordBatch>> {
        let arrow_schema: ArrowSchema = schema
            .as_ref()
            .try_into_arrow()
            .map_err(|err| KernelError::schema(format!("Unsupported write schema: {err}")))?;
        for batch in input {
            if batch.schema().as_ref() != &arrow_schema {
                return Err(KernelError::schema(
                    "Write input batch schema does not match the declared schema",
                ));
            }
        }
        if input.iter().all(|batch| batch.num_rows() == 0) {
            return Err(KernelError::generic(
                "Write requires at least one input row",
            ));
        }
        // Use a closure in order to map internal errors to kernel's expected variants
        let write = || -> KernelResult<Vec<RecordBatch>> {
            let bytes = match file_type {
                FileType::Json => to_json_bytes(input.iter().map(|batch| {
                    Ok(FilteredEngineData::with_all_rows_selected(Box::new(
                        ArrowEngineData::new(batch.clone()),
                    )))
                }))?,
                FileType::Parquet => {
                    let mut writer = ArrowWriter::try_new_with_options(
                        Vec::new(),
                        Arc::new(arrow_schema),
                        writer_options(),
                    )?;
                    for batch in input {
                        writer.write(batch)?;
                    }
                    writer.into_inner()?
                }
            };
            put_bytes(self.storage.store(), file_path, bytes.into(), overwrite)?;
            let meta = self.storage.head(file_path)?;
            let size = i64::try_from(meta.size)
                .map_err(|_| KernelError::generic("Written file size exceeds LONG range"))?;
            Ok(vec![values_to_record_batch(Values::new(
                FILE_META_SCHEMA.clone(),
                vec![vec![
                    meta.location.to_string().into(),
                    size.into(),
                    meta.last_modified.into(),
                ]],
            ))?])
        };
        write().map_err(|err| match err {
            KernelError::FileAlreadyExists(_) => err,
            other => KernelError::generic(format!("Failed to write {file_path}: {other}")),
        })
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
    ) -> KernelResult<Vec<RecordBatch>> {
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
            let data: KernelResultIteratorStatic<ArrowEngineData> = match file_type {
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
                // Reconcile writer-chosen map/list field names to `output_schema`'s before
                // `try_new` asserts the schema. See `coerce_columns_to_schema`.
                let columns = coerce_columns_to_schema(columns, &output_schema)?;
                batches.push(RecordBatch::try_new(output_schema.clone(), columns)?);
            }
        }
        Ok(batches)
    }

    /// Reads files named by `input` rows.
    fn eval_dynamic_scan(
        &self,
        dynamic_scan: DynamicScan,
        input: &[RecordBatch],
    ) -> KernelResult<Vec<RecordBatch>> {
        let files = dynamic_scan_files(&dynamic_scan, input)?;
        self.eval_scan(
            dynamic_scan.file_type,
            files,
            dynamic_scan.file_constant_columns,
            dynamic_scan.schema,
        )
    }

    fn eval_relation_source(
        &self,
        relation_ref: &RelationRef,
        relation_store: Option<&RelationStore>,
    ) -> KernelResult<Vec<RecordBatch>> {
        let relation_store = relation_store.ok_or_else(|| {
            KernelError::unsupported("RelationSource requires an executor that can store relations")
        })?;
        let retained_relations = relation_store
            .lock()
            .map_err(|_| KernelError::internal_error("retained relations mutex poisoned"))?;
        let retained = retained_relations.get(relation_ref.id()).ok_or_else(|| {
            KernelError::generic(format!(
                "RelationSource references unknown relation id {}",
                relation_ref.id()
            ))
        })?;
        // TODO(#3485): schema equality currently does not consider field order.
        if &retained.schema != relation_ref.schema() {
            return Err(KernelError::generic(format!(
                "RelationSource schema does not match retained relation id {}",
                relation_ref.id()
            )));
        }
        // RecordBatch clones share their Arrow arrays and is relatively cheap.
        Ok(retained.batches.clone())
    }
}

fn retain_relation(
    relation_store: &RelationStore,
    name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> KernelResult<RelationRef> {
    validate_relation_batches(&schema, &batches)?;
    // `try_update` is unavailable at the crate's MSRV.
    #[allow(deprecated)]
    let counter = NEXT_RELATION_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| KernelError::internal_error("relation id space exhausted"))?;
    let id = format!("{name}-{counter}");
    relation_store
        .lock()
        .map_err(|_| KernelError::internal_error("retained relations mutex poisoned"))?
        .insert(
            id.clone(),
            RetainedRelation {
                schema: schema.clone(),
                batches,
            },
        );
    Ok(RelationRef::new(id, schema))
}

fn validate_relation_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> KernelResult<()> {
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
    for batch in batches {
        if batch.schema().as_ref() != &arrow_schema {
            return Err(KernelError::generic(
                "record batch schema does not match relation schema",
            ));
        }
    }
    Ok(())
}

fn dynamic_scan_files(
    dynamic_scan: &DynamicScan,
    input: &[RecordBatch],
) -> KernelResult<Vec<ScanFile>> {
    let mut files = Vec::new();
    for batch in input {
        let path = extract_column(batch, dynamic_scan.path_column.path())?;
        let path = path.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            KernelError::generic(format!(
                "Expected STRING Load path, got {:?}",
                path.data_type()
            ))
        })?;
        let size = extract_column(batch, dynamic_scan.file_size_column.path())?;
        let size = size.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            KernelError::generic(format!(
                "Expected LONG Load file size, got {:?}",
                size.data_type()
            ))
        })?;
        let last_modified = extract_column_ref(batch, dynamic_scan.last_modified_column.path())?;
        let last_modified = last_modified
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                KernelError::generic(format!(
                    "Expected LONG Load last modified, got {:?}",
                    last_modified.data_type()
                ))
            })?;
        let dv = match &dynamic_scan.dv_column {
            Some(dv_column) => {
                let dv_path = dv_column.path();
                let dv = extract_column(batch, dv_path)?;
                let dv_ancestors: Vec<_> = (1..dv_path.len())
                    .map(|len| extract_column(batch, &dv_path[..len]))
                    .try_collect()?;
                Some((dv, dv_ancestors))
            }
            None => None,
        };

        for row in 0..batch.num_rows() {
            if path.is_null(row) {
                return Err(KernelError::generic("DynamicScan path must not be null"));
            }
            if let Some((dv, dv_ancestors)) = &dv {
                if dv.is_valid(row) && dv_ancestors.iter().all(|ancestor| ancestor.is_valid(row)) {
                    return Err(KernelError::unsupported(
                        "SyncPlanExecutor DynamicScan with deletion vectors",
                    ));
                }
            }
            let path = path.value(row);
            let location = dynamic_scan.base_url.join(path)?;
            if size.is_null(row) {
                return Err(KernelError::generic(
                    "DynamicScan file size must not be null",
                ));
            }
            let size = size.value(row);
            if size <= 0 {
                return Err(KernelError::generic(
                    "DynamicScan file size must be positive",
                ));
            }
            let size = u64::try_from(size)
                .map_err(|_| KernelError::generic("DynamicScan file size must fit in a u64"))?;
            if last_modified.is_null(row) {
                return Err(KernelError::generic(
                    "DynamicScan last-modified time must not be null",
                ));
            }
            let last_modified = last_modified.value(row);
            let file_constants = dynamic_scan
                .file_constant_columns
                .iter()
                .map(|name| scalar_value(extract_column(batch, &[name])?.as_ref(), row))
                .try_collect()?;
            files.push(ScanFile {
                meta: FileMeta {
                    location,
                    last_modified,
                    size,
                },
                file_constants,
            });
        }
    }
    Ok(files)
}

fn eval_project(project: Project, input: &[RecordBatch]) -> KernelResult<Vec<RecordBatch>> {
    let Some(first_batch) = input.first() else {
        return Ok(vec![]);
    };
    let input_schema = Arc::new(StructType::try_from_arrow(first_batch.schema().as_ref())?);
    let evaluator = ArrowEvaluationHandler.new_expression_evaluator(
        input_schema,
        project.expr,
        project.schema.as_ref().clone().into(),
    )?;
    input
        .iter()
        .map(|batch| {
            evaluator
                .evaluate(&ArrowEngineData::new(batch.clone()))?
                .try_into_record_batch()
        })
        .collect()
}

fn eval_filter(predicate: PredicateRef, input: &[RecordBatch]) -> KernelResult<Vec<RecordBatch>> {
    let Some(first_batch) = input.first() else {
        return Ok(vec![]);
    };
    let input_schema = Arc::new(StructType::try_from_arrow(first_batch.schema().as_ref())?);
    let evaluator = ArrowEvaluationHandler.new_predicate_evaluator(input_schema, predicate)?;
    input
        .iter()
        .map(|batch| {
            let mask = evaluator
                .evaluate(&ArrowEngineData::new(batch.clone()))?
                .try_into_record_batch()?;
            let mask = mask
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    KernelError::generic("Filter predicate did not produce a boolean array")
                })?;
            Ok(filter_record_batch(batch, mask)?)
        })
        .collect()
}

fn eval_semi_join(
    join: SemiJoin,
    probe: &[RecordBatch],
    build: &[RecordBatch],
) -> KernelResult<Vec<RecordBatch>> {
    let mut build_keys = HashSet::new();
    for batch in build {
        build_keys.extend(encode_keys_as_rows(batch, &join.build_keys)?);
    }

    probe
        .iter()
        .map(|batch| {
            let keep = encode_keys_as_rows(batch, &join.probe_keys)?
                .into_iter()
                .map(|key| join.inverted != build_keys.contains(&key));
            Ok(filter_record_batch(batch, &BooleanArray::from_iter(keep))?)
        })
        .collect()
}

/// Builds output columns in `schema` order: each column named in `file_constant_columns` is
/// broadcast from `constants` (at the matching slot), and every other column is drained in turn
/// from `batch`, which holds exactly the non-constant columns in schema order.
fn splice_file_constants(
    batch: RecordBatch,
    schema: &SchemaRef,
    file_constant_columns: &[String],
    constants: &[Scalar],
) -> KernelResult<Vec<ArrayRef>> {
    let (_, read_columns, rows) = batch.into_parts();
    let mut read_columns = read_columns.into_iter();
    schema
        .fields()
        .map(
            |field| match file_constant_columns.iter().position(|c| c == field.name()) {
                Some(slot) => constants[slot].to_array(rows),
                None => read_columns.next().ok_or_else(|| {
                    KernelError::generic("scan output has fewer columns than schema")
                }),
            },
        )
        .collect()
}

/// Encodes `columns` of `batch` as one comparable/hashable [`OwnedRow`] key per input row.
///
/// Empty `columns` (e.g. ungrouped aggregation) yields a vec of empty keys that self-compare equal
pub(super) fn encode_keys_as_rows(
    batch: &RecordBatch,
    columns: &[ColumnName],
) -> KernelResult<Vec<OwnedRow>> {
    if columns.is_empty() {
        let key = RowConverter::new(vec![])?.parser().parse(&[]).owned();
        return Ok(vec![key; batch.num_rows()]);
    }
    let arrays: Vec<_> = columns
        .iter()
        .map(|name| extract_column(batch, name))
        .try_collect()?;
    // Constructing RowConverter requires a `SortField`. We initialize default, unsorted field for
    // each column.
    let sort_fields = arrays
        .iter()
        .map(|array| SortField::new(array.data_type().clone()))
        .collect();
    let converter = RowConverter::new(sort_fields)?;
    let rows = converter.convert_columns(&arrays)?;
    Ok(rows.iter().map(|row| row.owned()).collect())
}

fn scalar_value(array: &dyn Array, row: usize) -> KernelResult<Scalar> {
    if array.is_null(row) {
        return Ok(Scalar::Null(DataType::try_from_arrow(array.data_type())?));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Scalar::String(strings.value(row).to_string()));
    }
    if let Some(longs) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Scalar::Long(longs.value(row)));
    }
    Err(KernelError::unsupported(format!(
        "Scalar conversion from array type {:?}",
        array.data_type()
    )))
}

/// Materialize a [`Values`] node's literal rows into a [`RecordBatch`]. An empty relation (the
/// [`PlanBuilder::build`] output for an absent input) yields zero-row data.
///
/// [`PlanBuilder::build`]: crate::plans::PlanBuilder::build
fn values_to_record_batch(values: Values) -> KernelResult<RecordBatch> {
    let Values { schema, rows } = values;
    let columns: Vec<ArrayRef> = schema
        .fields()
        .enumerate()
        .map(|(col, field)| -> KernelResult<ArrayRef> {
            let element_type = ArrayType::new(field.data_type().clone(), true);
            let column = ArrayData::try_new(element_type, rows.iter().map(|row| row[col].clone()))?;
            // This produces a single array row. The array contains n elements, one for each
            // attribute of the column.
            let list = Scalar::Array(column).to_array(1)?;
            let list = list.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                KernelError::generic("Values: Scalar::Array did not lower to a ListArray")
            })?;
            let (_field, _offsets, values, _nulls) = list.clone().into_parts();
            Ok(values)
        })
        .try_collect()?;
    let schema = Arc::new(schema.as_ref().try_into_arrow()?);
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_batches_sorted_eq;
    use url::Url;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::StructArray;
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::engine::sync::get_bytes;
    use crate::expressions::{col, column_name, lit, Predicate, StructData};
    use crate::object_store::memory::InMemory;
    use crate::plans::PlanBuilder;
    use crate::schema::{schema, schema_ref, MetadataValue, StructField, ToSchema as _};

    fn write_plan(
        input: PlanBuilder,
        file_type: FileType,
        path: &Url,
        overwrite: bool,
    ) -> Result<Plan> {
        match file_type {
            FileType::Json => input.write_json(path.clone(), overwrite),
            FileType::Parquet => input.write_parquet(path.clone(), overwrite),
        }?
        .build()
    }

    fn write_and_check_output(
        executor: &SyncPlanExecutor,
        plan: Plan,
        file_type: FileType,
        path: &Url,
        schema: SchemaRef,
        expected_rows: &[&str],
    ) -> Result<()> {
        assert_eq!(
            plan.schema,
            schema_ref! {
                not_null "location": STRING,
                not_null "size": LONG,
                not_null "last_modified": LONG,
            }
        );
        let batches = executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<Result<Vec<_>>>()?;
        let meta = executor.storage.head(path)?;
        let expected = values_to_record_batch(Values::new(
            FILE_META_SCHEMA.clone(),
            vec![vec![
                path.to_string().into(),
                (meta.size as i64).into(),
                meta.last_modified.into(),
            ]],
        ))?;
        assert_eq!(batches, vec![expected]);
        assert_eq!(
            meta.size,
            get_bytes(executor.storage.store(), path)?.len() as u64
        );
        let scan = match file_type {
            FileType::Json => PlanBuilder::scan_json([meta], &[], schema)?,
            FileType::Parquet => PlanBuilder::scan_parquet([meta], &[], schema)?,
        }
        .build()?;
        let batches = executor.eval_plan(scan, None)?;
        assert_batches_sorted_eq!(expected_rows, &batches);
        Ok(())
    }

    #[rstest]
    fn write_nodes_write_all_batches_and_honor_overwrite(
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
        #[values(false, true)] in_memory: bool,
        #[values(false, true)] overwrite: bool,
    ) -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = if in_memory {
            Url::parse("memory:///nested/output")?
        } else {
            Url::from_file_path(dir.path().join("nested/output")).unwrap()
        };
        let executor = SyncPlanExecutor::new(in_memory.then(|| Arc::new(InMemory::new()) as _));
        let schema = schema_ref! { not_null "id": LONG };
        let input = PlanBuilder::union_all([
            PlanBuilder::values(schema.clone(), vec![vec![0i64.into()]])?,
            PlanBuilder::values(schema.clone(), vec![vec![1i64.into()], vec![2i64.into()]])?,
            PlanBuilder::values(schema.clone(), vec![vec![3i64.into()], vec![4i64.into()]])?,
        ])?
        .filter(Predicate::gt(col!("id"), lit(1i64)))?;
        write_and_check_output(
            &executor,
            write_plan(input, file_type, &path, overwrite)?,
            file_type,
            &path,
            schema.clone(),
            &[
                "+----+", "| id |", "+----+", "| 2  |", "| 3  |", "| 4  |", "+----+",
            ],
        )?;

        let original_bytes = get_bytes(executor.storage.store(), &path)?;
        let replacement = PlanBuilder::values(schema.clone(), vec![vec![9i64.into()]])?;
        let replacement = write_plan(replacement, file_type, &path, overwrite)?;
        if overwrite {
            write_and_check_output(
                &executor,
                replacement,
                file_type,
                &path,
                schema,
                &["+----+", "| id |", "+----+", "| 9  |", "+----+"],
            )?;
        } else {
            let result = executor.execute_op(Operation::QueryPlan(replacement));
            assert!(matches!(result, Err(KernelError::FileAlreadyExists(_))));
            assert_eq!(get_bytes(executor.storage.store(), &path)?, original_bytes);
        }

        Ok(())
    }

    #[rstest]
    #[case::filter("filter")]
    #[case::scan("scan")]
    #[case::aggregate("aggregate")]
    #[case::retained("retained")]
    fn write_nodes_reject_empty_input(
        #[case] source: &str,
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
        #[values(false, true)] destination_exists: bool,
        #[values(false, true)] overwrite: bool,
    ) -> Result<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let scoped = executor.get_scoped();
        let schema = schema_ref! { not_null "id": LONG };
        let filtered = PlanBuilder::values(schema.clone(), vec![vec![1i64.into()]])?
            .filter(Predicate::FALSE)?;
        let empty_path = Url::parse("memory:///empty.json")?;
        executor.storage.put(&empty_path, Bytes::new(), false)?;
        let scan =
            PlanBuilder::scan_json([executor.storage.head(&empty_path)?], &[], schema.clone())?;
        let input = match source {
            "filter" => filtered,
            "scan" => scan,
            "aggregate" => filtered.aggregate_by([column_name!("id")], |aggs| aggs)?,
            "retained" => {
                PlanBuilder::relation_source(scoped.execute_and_retain("empty", scan.build()?)?)
            }
            _ => unreachable!(),
        };
        let path = Url::parse("memory:///output")?;
        let original = Bytes::from_static(b"original contents");
        if destination_exists {
            executor.storage.put(&path, original.clone(), false)?;
        }
        let plan = write_plan(input, file_type, &path, overwrite)?;
        assert!(matches!(
            scoped.execute_op(Operation::QueryPlan(plan)),
            Err(KernelError::Generic(message)) if message.contains("at least one input row")
        ));
        if destination_exists {
            assert_eq!(get_bytes(executor.storage.store(), &path)?, original);
        } else {
            assert!(matches!(
                executor.storage.head(&path),
                Err(KernelError::FileNotFound(_))
            ));
        }
        Ok(())
    }

    #[rstest]
    #[case::name(schema_ref! { not_null "other": LONG, not_null "value": LONG })]
    #[case::order(schema_ref! { not_null "value": LONG, not_null "id": LONG })]
    #[case::nullability(schema_ref! { nullable "id": LONG, not_null "value": LONG })]
    #[case::type_mismatch(schema_ref! { not_null "id": INTEGER, not_null "value": LONG })]
    #[case::metadata(schema_ref! {
        (StructField::not_null("id", DataType::LONG)
            .add_metadata([("parquet.field.id", MetadataValue::Number(1))])),
        not_null "value": LONG,
    })]
    fn write_nodes_reject_mismatched_batch_without_writing(
        #[case] mismatched_schema: SchemaRef,
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
        #[values(false, true)] empty_batch: bool,
        #[values(false, true)] overwrite: bool,
    ) -> Result<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let path = Url::parse("memory:///output")?;
        let original = Bytes::from_static(b"original contents");
        executor.storage.put(&path, original.clone(), false)?;
        let schema = schema_ref! { not_null "id": LONG, not_null "value": LONG };
        let input = PlanBuilder::values(schema.clone(), vec![vec![1i64.into(), 2i64.into()]])?;
        let plan = write_plan(input, file_type, &path, overwrite)?;
        let valid =
            values_to_record_batch(Values::new(schema, vec![vec![1i64.into(), 2i64.into()]]))?;
        let first_value =
            if mismatched_schema.fields().next().unwrap().data_type() == &DataType::INTEGER {
                1i32.into()
            } else {
                1i64.into()
            };
        let mismatched = values_to_record_batch(Values::new(
            mismatched_schema,
            if empty_batch {
                vec![]
            } else {
                vec![vec![first_value, 2i64.into()]]
            },
        ))?;
        // A valid first batch must not hide a later schema mismatch, even in an empty batch.
        let result = executor.eval_node(
            plan.nodes.into_iter().last().unwrap(),
            &[vec![valid, mismatched]],
            None,
        );
        assert!(matches!(result, Err(KernelError::Schema(_))));
        assert_eq!(get_bytes(executor.storage.store(), &path)?, original);
        Ok(())
    }

    #[rstest]
    fn write_nodes_report_storage_errors(
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
    ) -> Result<()> {
        let dir = tempfile::tempdir()?;
        let parent_file = dir.path().join("file");
        std::fs::write(&parent_file, b"original contents")?;
        let path = Url::from_file_path(parent_file.join("nested/output")).unwrap();
        let input =
            PlanBuilder::values(schema_ref! { not_null "id": LONG }, vec![vec![1i64.into()]])?;
        let plan = write_plan(input, file_type, &path, true)?;
        // Creating the destination's parent directory fails because an ancestor is a file.
        assert!(matches!(
            SyncPlanExecutor::default().execute_op(Operation::QueryPlan(plan)),
            Err(KernelError::Generic(_))
        ));
        assert_eq!(std::fs::read(parent_file)?, b"original contents");
        Ok(())
    }

    #[rstest]
    fn write_nodes_preserve_destination_on_input_error(
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
        #[values(false, true)] overwrite: bool,
    ) -> Result<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let path = Url::parse("memory:///output")?;
        let original = Bytes::from_static(b"original contents");
        executor.storage.put(&path, original.clone(), false)?;
        let input = PlanBuilder::scan_json(
            [FileMeta {
                location: Url::parse("memory:///missing.json")?,
                size: 0,
                last_modified: 0,
            }],
            &[],
            schema_ref! { not_null "id": LONG },
        )?;
        let plan = write_plan(input, file_type, &path, overwrite)?;
        // The source file is deliberately absent, so the scan must fail before any write occurs.
        assert!(matches!(
            executor.execute_op(Operation::QueryPlan(plan)),
            Err(KernelError::FileNotFound(missing)) if missing.ends_with("missing.json")
        ));
        assert_eq!(get_bytes(executor.storage.store(), &path)?, original);
        Ok(())
    }

    #[test]
    fn scoped_executor_creates_ref_from_arrow_batch() -> Result<()> {
        let executor = SyncPlanExecutor::default();
        let scoped = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )?;

        let wrong_schema = schema_ref! { nullable "other": LONG };
        let error = scoped
            .create_ref("arrow-batch", wrong_schema, batch.clone())
            .expect_err("a mismatched schema must be rejected");
        assert!(error
            .to_string()
            .contains("record batch schema does not match"));

        let relation_ref = scoped.create_ref("arrow-batch", schema, batch.clone())?;
        assert!(relation_ref.id().starts_with("arrow-batch-"));
        let query = PlanBuilder::relation_source(relation_ref.clone()).build()?;
        let scoped_batches: Vec<RecordBatch> = scoped
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<Result<_>>()?;
        assert_eq!(scoped_batches, vec![batch]);

        let query = PlanBuilder::relation_source(relation_ref).build()?;
        let error = executor
            .execute_op(Operation::QueryPlan(query))
            .err()
            .expect("the unscoped executor must reject a scope's relation");
        assert!(error
            .to_string()
            .contains("requires an executor that can store relations"));
        Ok(())
    }

    #[test]
    fn scoped_executor_rejects_batches_that_differ_from_plan_schema() -> Result<()> {
        let scoped = SyncPlanExecutor::default().get_scoped();
        let batch_schema = schema_ref! { nullable "id": LONG };
        let mut plan = PlanBuilder::values(batch_schema, vec![vec![1i64.into()]])?.build()?;
        plan.schema = schema_ref! { nullable "other": LONG };

        let error = scoped
            .execute_and_retain("wrong-schema", plan)
            .expect_err("a mismatched plan schema must be rejected");
        assert!(error
            .to_string()
            .contains("record batch schema does not match"));
        assert!(scoped.relation_store.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn scoped_executor_retains_and_reuses_filtered_relations() -> Result<()> {
        let executor = SyncPlanExecutor::default();
        let scoped = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let source = PlanBuilder::values(
            schema.clone(),
            vec![vec![1i64.into()], vec![2i64.into()], vec![3i64.into()]],
        )?
        .filter(Predicate::gt(col!("id"), lit(1i64)))?
        .build()?;
        let first = scoped.execute_and_retain("filtered", source)?;
        assert_eq!(first.schema(), &schema);
        assert!(first.id().starts_with("filtered-"));

        let second_source = PlanBuilder::relation_source(first.clone())
            .filter(Predicate::lt(col!("id"), lit(3i64)))?
            .build()?;
        let second = scoped.execute_and_retain("refiltered", second_source)?;
        assert_ne!(first.id(), second.id());
        assert_eq!(second.schema(), &schema);

        let query = PlanBuilder::relation_source(second).build()?;
        let batches: Vec<RecordBatch> = scoped
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<Result<_>>()?;
        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.value(0), 2);
        Ok(())
    }

    #[test]
    fn scoped_executors_isolate_relations_and_release_batches_on_drop() -> Result<()> {
        let executor = SyncPlanExecutor::default();
        let first_scope = executor.get_scoped();
        let second_scope = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let first = first_scope.execute_and_retain(
            "shared",
            PlanBuilder::values(schema.clone(), vec![vec![1i64.into()]])?.build()?,
        )?;
        let second = second_scope.execute_and_retain(
            "shared",
            PlanBuilder::values(schema.clone(), vec![vec![2i64.into()]])?.build()?,
        )?;
        assert_ne!(first.id(), second.id());

        let foreign = PlanBuilder::relation_source(first.clone()).build()?;
        let error = second_scope
            .execute_op(Operation::QueryPlan(foreign))
            .err()
            .expect("a scope must reject another scope's relation");
        assert!(error.to_string().contains("unknown relation id"));

        let unscoped = PlanBuilder::relation_source(first.clone()).build()?;
        let error = executor
            .execute_op(Operation::QueryPlan(unscoped))
            .err()
            .expect("the unscoped executor must reject a scope's relation");
        assert!(error
            .to_string()
            .contains("requires an executor that can store relations"));

        let weak_column = {
            let retained_relations = first_scope.relation_store.lock().unwrap();
            let batch = &retained_relations.get(first.id()).unwrap().batches[0];
            Arc::downgrade(batch.column(0))
        };
        drop(first_scope);
        assert!(weak_column.upgrade().is_none());

        let query = PlanBuilder::relation_source(second).build()?;
        let batches: Vec<RecordBatch> = second_scope
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<Result<_>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        Ok(())
    }

    #[test]
    fn scoped_executor_rejects_relation_ref_with_wrong_schema() -> Result<()> {
        let executor = SyncPlanExecutor::default();
        let scoped = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let retained = scoped.execute_and_retain(
            "wrong-schema",
            PlanBuilder::values(schema, vec![vec![1i64.into()]])?.build()?,
        )?;
        let wrong_schema = schema_ref! { nullable "other": LONG };
        let query =
            PlanBuilder::relation_source(RelationRef::new(retained.id(), wrong_schema)).build()?;
        let error = scoped
            .execute_op(Operation::QueryPlan(query))
            .err()
            .expect("a relation ref with the wrong schema must fail");
        assert!(error.to_string().contains("schema does not match"));
        Ok(())
    }

    #[test]
    fn scoped_executor_retains_empty_relation_with_schema() -> Result<()> {
        let executor = SyncPlanExecutor::default();
        let scoped = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let empty = PlanBuilder::values(schema.clone(), vec![])?.build()?;
        let retained = scoped.execute_and_retain("empty", empty)?;
        assert_eq!(retained.schema(), &schema);

        let query = PlanBuilder::relation_source(retained).build()?;
        let batches: Vec<RecordBatch> = scoped
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<Result<_>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        Ok(())
    }

    #[test]
    fn encode_keys_as_rows_synthesizes_empty_keys_when_ungrouped() -> Result<()> {
        let batch = RecordBatch::try_from_iter([(
            "x",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])?;
        let keys = encode_keys_as_rows(&batch, &[])?;
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|key| key == &keys[0]));
        Ok(())
    }

    fn null_dv() -> Scalar {
        Scalar::null(DeletionVectorDescriptor::to_schema())
    }

    fn present_dv() -> Scalar {
        Scalar::Struct(
            StructData::try_new(
                DeletionVectorDescriptor::to_schema()
                    .fields()
                    .cloned()
                    .collect(),
                vec![
                    Scalar::from("i"),
                    Scalar::from("inline"),
                    Scalar::Null(DataType::INTEGER),
                    Scalar::from(1_i32),
                    Scalar::from(1_i64),
                ],
            )
            .unwrap(),
        )
    }

    fn dynamic_scan_files_result(
        path: Scalar,
        size: Scalar,
        last_modified: Scalar,
        dv: Scalar,
    ) -> Result<Vec<ScanFile>> {
        let input_schema = schema_ref! {
            nullable "path": STRING,
            nullable "size": LONG,
            nullable "filemod": LONG,
            nullable "dv": (DeletionVectorDescriptor::to_schema()),
        };
        let input = values_to_record_batch(Values::new(
            input_schema,
            vec![vec![path, size, last_modified, dv]],
        ))
        .unwrap();
        let dynamic_scan = DynamicScan {
            schema: schema_ref! {},
            file_type: FileType::Parquet,
            base_url: Url::parse("memory:///").unwrap(),
            file_constant_columns: vec![],
            path_column: column_name!("path"),
            file_size_column: column_name!("size"),
            last_modified_column: column_name!("filemod"),
            dv_column: Some(column_name!("dv")),
        };

        dynamic_scan_files(&dynamic_scan, &[input])
    }

    #[test]
    fn dynamic_scan_executor_rejects_non_null_deletion_vector() {
        let err = dynamic_scan_files_result(
            "file.parquet".into(),
            1_i64.into(),
            0_i64.into(),
            present_dv(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("with deletion vectors"), "{err}");
    }

    #[rstest]
    #[case::path(
        Scalar::Null(DataType::STRING),
        1_i64.into(),
        0_i64.into(),
        "path must not be null"
    )]
    #[case::size(
        "file.parquet".into(),
        Scalar::Null(DataType::LONG),
        0_i64.into(),
        "file size must not be null"
    )]
    #[case::last_modified(
        "file.parquet".into(),
        1_i64.into(),
        Scalar::Null(DataType::LONG),
        "last-modified time must not be null"
    )]
    fn dynamic_scan_executor_rejects_null_required_field(
        #[case] path: Scalar,
        #[case] size: Scalar,
        #[case] last_modified: Scalar,
        #[case] needle: &str,
    ) {
        let err = dynamic_scan_files_result(path, size, last_modified, null_dv()).unwrap_err();
        assert!(err.to_string().contains(needle), "{err}");
    }

    #[rstest]
    #[case::zero(0_i64.into(), "file size must be positive")]
    #[case::negative((-1_i64).into(), "file size must be positive")]
    fn dynamic_scan_executor_rejects_invalid_size(#[case] size: Scalar, #[case] needle: &str) {
        let err = dynamic_scan_files_result("file.parquet".into(), size, 0_i64.into(), null_dv())
            .unwrap_err();
        assert!(err.to_string().contains(needle), "{err}");
    }

    #[test]
    fn dynamic_scan_executor_accepts_i64_max_file_size() {
        let err = dynamic_scan_files_result(
            "file.parquet".into(),
            i64::MAX.into(),
            Scalar::Null(DataType::LONG),
            null_dv(),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("last-modified time must not be null"),
            "got a size validation error before last-modified validation: {err}"
        );
    }

    #[test]
    fn dynamic_scan_threads_last_modified_into_file_meta() {
        let files = dynamic_scan_files_result(
            "file.parquet".into(),
            1_i64.into(),
            1_700_000_000_123_i64.into(),
            null_dv(),
        )
        .unwrap();

        assert_eq!(files[0].meta.last_modified, 1_700_000_000_123);
    }

    #[test]
    fn dynamic_scan_executor_accepts_no_dv_column() {
        let input_schema = schema_ref! {
            nullable "path": STRING,
            nullable "size": LONG,
            nullable "filemod": LONG,
        };
        let input = values_to_record_batch(Values::new(
            input_schema,
            vec![vec!["file.parquet".into(), 1_i64.into(), 42_i64.into()]],
        ))
        .unwrap();
        let dynamic_scan = DynamicScan {
            schema: schema_ref! {},
            file_type: FileType::Parquet,
            base_url: Url::parse("memory:///").unwrap(),
            file_constant_columns: vec![],
            path_column: column_name!("path"),
            file_size_column: column_name!("size"),
            last_modified_column: column_name!("filemod"),
            dv_column: None,
        };
        let files = dynamic_scan_files(&dynamic_scan, &[input]).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].meta.last_modified, 42);
    }

    #[test]
    fn dynamic_scan_treats_dv_under_null_ancestor_as_null() {
        let metadata_type = schema! {
            nullable "dv": (DeletionVectorDescriptor::to_schema()),
        };
        let input_schema = schema_ref! {
            not_null "path": STRING,
            not_null "size": LONG,
            not_null "filemod": LONG,
            nullable "metadata": (metadata_type.clone()),
        };
        let metadata_schema: ArrowSchema = (&metadata_type).try_into_arrow().unwrap();
        let metadata = StructArray::new(
            metadata_schema.fields().clone(),
            vec![present_dv().to_array(1).unwrap()],
            Some(NullBuffer::new(BooleanBuffer::from(vec![false]))),
        );
        let arrow_schema = Arc::new(input_schema.as_ref().try_into_arrow().unwrap());
        let input = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["file.parquet"])),
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(metadata),
            ],
        )
        .unwrap();
        let dynamic_scan = DynamicScan {
            schema: schema_ref! {},
            file_type: FileType::Parquet,
            base_url: Url::parse("memory:///").unwrap(),
            file_constant_columns: vec![],
            path_column: column_name!("path"),
            file_size_column: column_name!("size"),
            last_modified_column: column_name!("filemod"),
            dv_column: Some(column_name!("metadata.dv")),
        };

        assert_eq!(
            dynamic_scan_files(&dynamic_scan, &[input]).unwrap().len(),
            1
        );
    }
}
