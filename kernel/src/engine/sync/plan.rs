//! A synchronous [`PlanExecutor`] for tests, backed by the sync storage helpers.
//!
//! The query path evaluates a [`Plan`] eagerly, materializing every node's output in full before
//! its consumers run. Streaming is not needed because this executor only serves tests, which never
//! approach memory limits.
//
// TODO: The `IoOperation` paths will eventually be used to replace SyncEngine with an
// PlanBasedEngine (backed by this PlanExecutor)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use itertools::Itertools;
use uuid::Uuid;

use super::aggs::eval_aggregate;
use super::json::try_create_from_json;
use super::parquet::{parquet_footer, try_create_from_parquet};
use super::prefix_sum::eval_prefix_sum;
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
use crate::engine::arrow_expression::{ArrowEvaluationHandler, extract_column};
use crate::engine::arrow_utils::{
    coerce_columns_to_schema, fix_record_batch_nested_null_masks, to_json_bytes,
};
use crate::engine::data_file_writer_options;
use crate::expressions::{ArrayData, ColumnName, PredicateRef, Scalar};
use crate::object_store::DynObjectStore;
use crate::parquet::arrow::arrow_writer::ArrowWriter;
use crate::plans::ir::nodes::{
    DynamicScan, FILE_META_SCHEMA, FILES_META_SCHEMA, FileType, Operator, Project, RelationId,
    RelationRef, ScanFile, ScanJson, ScanParquet, SemiJoin, Values, WriteJson, WriteParquet,
    WriteParquetFiles,
};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult, ScopedPlanExecutor};
use crate::schema::compare::data_types_equal_ordered;
use crate::schema::{ArrayType, DataType, SchemaRef, StructField, StructType};
use crate::{
    EvaluationHandler as _, FileMeta, FilteredEngineData, KernelError, KernelResult,
    KernelResultIterator, StorageHandler as _,
};

/// A synchronous, test-only [`PlanExecutor`].
///
/// Scans read files through the sync module's Arrow read core, which also backs the
/// [`JsonHandler`] and [`ParquetHandler`] traits. [`IoOperation`]s use the sync storage handler.
///
/// All I/O is performed synchronously via [`futures::executor::block_on`]; cloud-backed stores are
/// not supported (see [`super`] module docs).
///
/// [`JsonHandler`]: crate::JsonHandler
/// [`ParquetHandler`]: crate::ParquetHandler
#[derive(Clone)]
pub struct SyncPlanExecutor {
    storage: Arc<SyncStorageHandler>,
}

// Distinct IDs prevent a ref from another scope from resolving to a local relation.
static NEXT_RELATION_ID: AtomicU64 = AtomicU64::new(0);

/// A synchronous plan executor that retains relations for its own lifetime.
pub struct ScopedSyncPlanExecutor {
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
    pub fn new(store: Option<Arc<DynObjectStore>>) -> Self {
        Self {
            storage: Arc::new(SyncStorageHandler::new(store)),
        }
    }

    /// Returns an executor that retains relations until the scope is dropped.
    pub fn get_scoped(&self) -> ScopedSyncPlanExecutor {
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
    pub fn create_ref(
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
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        self.execute_op_with_relation_store(op, None)
    }
}

impl PlanExecutor for ScopedSyncPlanExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        self.executor
            .execute_op_with_relation_store(op, Some(&self.relation_store))
    }
}

impl ScopedPlanExecutor for ScopedSyncPlanExecutor {
    fn execute_and_retain(&self, name: &str, plan: Plan) -> KernelResult<RelationRef> {
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
            // Every consumer, including the caller of the final node, sees nested children with
            // their ancestors' null masks applied. Project can introduce new nullable structs.
            outputs.push(
                output
                    .into_iter()
                    .map(fix_record_batch_nested_null_masks)
                    .try_collect()?,
            );
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
            Operator::PrefixSum(prefix_sum) => eval_prefix_sum(&prefix_sum, &results[inputs[0]]),
            Operator::WriteJson(write) => {
                self.eval_write_json(&write, inputs.iter().flat_map(|&i| results[i].iter()))
            }
            Operator::WriteParquet(write) => {
                self.eval_write_parquet(&write, inputs.iter().flat_map(|&i| results[i].iter()))
            }
            Operator::WriteParquetFiles(write) => self
                .eval_write_parquet_files(&write, inputs.iter().flat_map(|&i| results[i].iter())),
            Operator::SemiJoin(join) => {
                eval_semi_join(join, &results[inputs[0]], &results[inputs[1]])
            }
        }
    }

    fn eval_write_json<'a>(
        &self,
        write: &WriteJson,
        input: impl Iterator<Item = &'a RecordBatch> + Clone + Send,
    ) -> KernelResult<Vec<RecordBatch>> {
        let WriteJson {
            schema,
            file_path,
            overwrite,
            // This executor preserves input order even when `ordered` is false.
            ordered: _ordered,
        } = write;
        let arrow_schema: ArrowSchema = schema
            .as_ref()
            .try_into_arrow()
            .map_err(|err| KernelError::schema(format!("Unsupported write schema: {err}")))?;
        for batch in input.clone() {
            if batch.schema().as_ref() != &arrow_schema {
                return Err(KernelError::schema(
                    "Write input batch schema does not match the declared schema",
                ));
            }
        }
        if input.clone().all(|batch| batch.num_rows() == 0) {
            return Err(KernelError::generic(
                "Write requires at least one input row",
            ));
        }
        let bytes = to_json_bytes(input.map(|batch| {
            Ok(FilteredEngineData::with_all_rows_selected(Box::new(
                ArrowEngineData::new(batch.clone()),
            )))
        }))?;
        Ok(vec![self.write_file_result(
            file_path,
            bytes.into(),
            *overwrite,
        )?])
    }

    fn eval_write_parquet<'a>(
        &self,
        write: &WriteParquet,
        input: impl Iterator<Item = &'a RecordBatch>,
    ) -> KernelResult<Vec<RecordBatch>> {
        let schema = Arc::new(write.write_schema.as_ref().try_into_arrow()?);
        let bytes = Self::parquet_bytes(&write.write_schema, schema, input)?
            .ok_or_else(|| KernelError::generic("Write requires at least one input row"))?;
        Ok(vec![self.write_file_result(
            &write.file_path,
            bytes,
            write.overwrite,
        )?])
    }

    fn parquet_bytes<'a>(
        write_schema: &StructType,
        schema: Arc<ArrowSchema>,
        input: impl Iterator<Item = &'a RecordBatch>,
    ) -> KernelResult<Option<Bytes>> {
        let mut bytes = Vec::new();
        let options = data_file_writer_options(&schema, write_schema)?;
        let mut writer = ArrowWriter::try_new_with_options(&mut bytes, schema.clone(), options)?;
        let mut has_rows = false;
        // Input order is preserved even when `ordered` is false.
        for batch in input {
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    let (index, source) = batch
                        .schema_ref()
                        .column_with_name(field.name())
                        .ok_or_else(|| {
                            KernelError::schema(format!("Missing write field {}", field.name()))
                        })?;
                    if source.is_nullable() != field.is_nullable()
                        || (source.data_type() != field.data_type()
                            && !data_types_equal_ordered(
                                StructField::try_from_arrow(source)?.data_type(),
                                StructField::try_from_arrow(field.as_ref())?.data_type(),
                            ))
                    {
                        return Err(KernelError::schema(format!(
                            "Write field {} does not match its input type and nullability",
                            field.name()
                        )));
                    }
                    Ok(batch.column(index).clone())
                })
                .collect::<KernelResult<Vec<_>>>()?;
            // Rebind metadata on list/map child fields without changing their value types.
            let columns = coerce_columns_to_schema(columns, &schema)?;
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            has_rows |= batch.num_rows() != 0;
            writer.write(&batch)?;
        }
        if !has_rows {
            return Ok(None);
        }
        writer.close()?;
        Ok(Some(bytes.into()))
    }

    fn eval_write_parquet_files<'a>(
        &self,
        write: &WriteParquetFiles,
        input: impl Iterator<Item = &'a RecordBatch>,
    ) -> KernelResult<Vec<RecordBatch>> {
        write.validate_destination()?;
        let schema: Arc<ArrowSchema> = Arc::new(write.write_schema.as_ref().try_into_arrow()?);
        // Serialize every batch before publishing any file, including validating empty batches.
        let files = input
            .map(|batch| {
                Self::parquet_bytes(&write.write_schema, schema.clone(), std::iter::once(batch))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let output_schema: Arc<ArrowSchema> =
            Arc::new(FILES_META_SCHEMA.as_ref().try_into_arrow()?);
        let mut output = Vec::new();
        for bytes in files.into_iter().flatten() {
            let file_name = write
                .file_name_template
                .replace("{uuid}", &Uuid::new_v4().to_string());
            let path = write.base_url.join(&file_name)?;
            let meta = self.write_file_result(&path, bytes, false)?;
            let columns = std::iter::once(Arc::new(StringArray::from(vec![file_name])) as ArrayRef)
                .chain(meta.columns().iter().cloned())
                .collect();
            output.push(RecordBatch::try_new(output_schema.clone(), columns)?);
        }
        Ok(output)
    }

    fn write_file_result(
        &self,
        file_path: &url::Url,
        bytes: Bytes,
        overwrite: bool,
    ) -> KernelResult<RecordBatch> {
        put_bytes(self.storage.store(), file_path, bytes, overwrite)?;
        let meta = self.storage.head(file_path)?;
        let size = i64::try_from(meta.size)
            .map_err(|_| KernelError::generic("Written file size exceeds LONG range"))?;
        values_to_record_batch(Values::new(
            FILE_META_SCHEMA.clone(),
            vec![vec![
                meta.location.to_string().into(),
                size.into(),
                meta.last_modified.into(),
            ]],
        ))
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
            let data: Box<dyn KernelResultIterator<ArrowEngineData>> = match file_type {
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
    let counter = NEXT_RELATION_ID
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
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
            if let Some((dv, dv_ancestors)) = &dv
                && dv.is_valid(row)
                && dv_ancestors.iter().all(|ancestor| ancestor.is_valid(row))
            {
                return Err(KernelError::unsupported(
                    "SyncPlanExecutor DynamicScan with deletion vectors",
                ));
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
    use test_utils::{assert_batches_sorted_eq, sort_lines};
    use url::Url;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::{AsArray, StructArray};
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Int64Type};
    use crate::arrow::util::pretty::pretty_format_batches;
    use crate::committer::file_meta_from_batches;
    #[cfg(feature = "arrow-60")]
    use crate::engine::file_groups;
    use crate::engine::sync::get_bytes;
    use crate::expressions::{
        Expression, MapData, Predicate, StructData, col, column_name, expr, lit,
    };
    use crate::object_store::memory::InMemory;
    use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::plans::PlanBuilder;
    use crate::plans::ir::nodes::Agg;
    use crate::schema::{MapType, MetadataValue, ToSchema as _, schema, schema_ref};

    fn write_plan(
        inputs: impl IntoIterator<Item = PlanBuilder>,
        path: &Url,
        overwrite: bool,
        ordered: bool,
        file_type: FileType,
    ) -> KernelResult<Plan> {
        let inputs = Vec::from_iter(inputs);
        match file_type {
            FileType::Json => PlanBuilder::write_json(inputs, path.clone(), overwrite, ordered),
            FileType::Parquet => {
                let schema = inputs[0].schema().clone();
                PlanBuilder::write_parquet(inputs, schema, path.clone(), overwrite, ordered)
            }
        }?
        .build()
    }

    fn write_and_check_output(
        executor: &SyncPlanExecutor,
        plan: Plan,
        path: &Url,
        schema: SchemaRef,
        expected_rows: &[&str],
        file_type: FileType,
    ) -> KernelResult<()> {
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
            .collect::<KernelResult<Vec<_>>>()?;
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
            FileType::Json => PlanBuilder::scan_json([meta], &[], schema),
            FileType::Parquet => PlanBuilder::scan_parquet([meta], &[], schema),
        }?
        .build()?;
        let batches = executor.eval_plan(scan, None)?;
        assert_batches_sorted_eq!(expected_rows, &batches);
        Ok(())
    }

    #[rstest]
    fn write_writes_all_inputs_and_honors_options(
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
        #[values(false, true)] ordered: bool,
        #[values(false, true)] in_memory: bool,
        #[values(false, true)] overwrite: bool,
    ) -> KernelResult<()> {
        let dir = tempfile::tempdir()?;
        let path = if in_memory {
            Url::parse("memory:///nested/output")?
        } else {
            Url::from_file_path(dir.path().join("nested/output")).unwrap()
        };
        let executor = SyncPlanExecutor::new(in_memory.then(|| Arc::new(InMemory::new()) as _));
        let schema = schema_ref! { not_null "id": LONG };
        let first = PlanBuilder::union_all([
            PlanBuilder::values(schema.clone(), vec![vec![0i64.into()]])?,
            PlanBuilder::values(schema.clone(), vec![vec![7i64.into()], vec![6i64.into()]])?,
        ])?
        .filter(Predicate::from_expr(expr!(col("id") > 1i64)))?;
        let second = PlanBuilder::union_all([
            PlanBuilder::values(schema.clone(), vec![vec![3i64.into()], vec![4i64.into()]])?,
            PlanBuilder::values(schema.clone(), vec![vec![2i64.into()]])?,
        ])?;
        let empty = PlanBuilder::values(schema.clone(), vec![vec![0i64.into()]])?
            .filter(Predicate::FALSE)?;
        #[rustfmt::skip]
        let expected_rows = &[
            "+----+",
            "| id |",
            "+----+",
            "| 2  |",
            "| 3  |",
            "| 4  |",
            "| 6  |",
            "| 6  |",
            "| 7  |",
            "| 7  |",
            "+----+",
        ];
        write_and_check_output(
            &executor,
            write_plan(
                [
                    empty.clone(),
                    first.clone(),
                    empty.clone(),
                    second,
                    first,
                    empty,
                ],
                &path,
                overwrite,
                ordered,
                file_type,
            )?,
            &path,
            schema.clone(),
            expected_rows,
            file_type,
        )?;

        let original_bytes = get_bytes(executor.storage.store(), &path)?;
        if ordered {
            let meta = executor.storage.head(&path)?;
            let scan = match file_type {
                FileType::Json => PlanBuilder::scan_json([meta], &[], schema.clone()),
                FileType::Parquet => PlanBuilder::scan_parquet([meta], &[], schema.clone()),
            }?
            .build()?;
            let batches = executor.eval_plan(scan, None)?;
            let mut ids = batches
                .iter()
                .flat_map(|batch| {
                    batch["id"]
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            // Each input's rows must stay together; their order within the group is unrestricted.
            assert_eq!(ids.len(), 7);
            ids[..2].sort_unstable();
            ids[2..5].sort_unstable();
            ids[5..].sort_unstable();
            assert_eq!(ids, [6, 7, 2, 3, 4, 6, 7]);
        }
        let replacement = PlanBuilder::values(schema.clone(), vec![vec![9i64.into()]])?;
        let replacement = write_plan([replacement], &path, overwrite, ordered, file_type)?;
        if overwrite {
            #[rustfmt::skip]
            let expected_rows = &[
                "+----+",
                "| id |",
                "+----+",
                "| 9  |",
                "+----+",
            ];
            write_and_check_output(
                &executor,
                replacement,
                &path,
                schema,
                expected_rows,
                file_type,
            )?;
        } else {
            let result = executor.execute_op(Operation::QueryPlan(replacement));
            assert!(matches!(result, Err(KernelError::FileAlreadyExists(_))));
            assert_eq!(get_bytes(executor.storage.store(), &path)?, original_bytes);
        }

        Ok(())
    }

    #[rstest]
    fn write_rejects_empty_input(
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let input =
            PlanBuilder::values(schema_ref! { not_null "id": LONG }, vec![vec![1i64.into()]])?
                .filter(Predicate::FALSE)?;
        let path = Url::parse("memory:///output")?;
        let original = Bytes::from_static(b"original contents");
        executor.storage.put(&path, original.clone(), false)?;
        let plan = write_plan([input], &path, true, false, file_type)?;
        assert!(matches!(
            executor.execute_op(Operation::QueryPlan(plan)),
            Err(KernelError::Generic(message)) if message.contains("at least one input row")
        ));
        assert_eq!(get_bytes(executor.storage.store(), &path)?, original);
        Ok(())
    }

    #[rstest]
    #[case::wrong_type(schema_ref! { not_null "id": STRING })]
    #[case::missing_field(schema_ref! { not_null "other": LONG })]
    #[case::wrong_nullability(schema_ref! { nullable "id": LONG })]
    fn write_rejects_mismatched_batch_without_writing(
        #[case] schema: SchemaRef,
        #[values(FileType::Json, FileType::Parquet)] file_type: FileType,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let path = Url::parse("memory:///output")?;
        let original = Bytes::from_static(b"original contents");
        executor.storage.put(&path, original.clone(), false)?;
        let input =
            PlanBuilder::values(schema_ref! { not_null "id": LONG }, vec![vec![1i64.into()]])?;
        let mut plan = write_plan([input], &path, true, false, file_type)?;
        match &mut plan.nodes.last_mut().unwrap().op {
            Operator::WriteJson(write) => write.schema = schema,
            Operator::WriteParquet(write) => write.write_schema = schema,
            _ => panic!("expected a write"),
        }
        assert!(matches!(
            executor.execute_op(Operation::QueryPlan(plan)),
            Err(KernelError::Schema(_))
        ));
        assert_eq!(get_bytes(executor.storage.store(), &path)?, original);
        Ok(())
    }

    #[rstest]
    #[case::absent(&[])]
    #[case::empty(&[None, None])]
    #[case::populated(&[Some(1), Some(2)])]
    #[case::mixed(&[None, Some(1), None, Some(2), None])]
    fn write_parquet_files_returns_stored_files_once_for_shared_consumers(
        #[case] ids: &[Option<i64>],
        #[values(false, true)] in_memory: bool,
    ) -> KernelResult<()> {
        let dir = tempfile::tempdir()?;
        let base = if in_memory {
            Url::parse("memory:///files/")?
        } else {
            Url::from_directory_path(dir.path()).unwrap()
        };
        let executor = SyncPlanExecutor::new(in_memory.then(|| Arc::new(InMemory::new()) as _));
        let schema = schema_ref! { not_null "id": LONG };
        let inputs = ids
            .iter()
            .map(|id| {
                PlanBuilder::values(schema.clone(), vec![vec![id.unwrap_or_default().into()]])?
                    .filter(Predicate::literal(id.is_some()))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        let files = PlanBuilder::write_parquet_files(
            inputs,
            schema.clone(),
            base.clone(),
            "part-{uuid}.parquet".into(),
        )?;
        assert_eq!(files.schema(), &*FILES_META_SCHEMA);
        let plan = PlanBuilder::union_all([files.clone(), files])?.build()?;
        let actual = executor.eval_plan(plan, None)?;
        let expected_files = ids.iter().flatten().count();
        assert_eq!(
            actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_files * 2
        );
        let stored = executor
            .storage
            .list_from(&base)?
            .collect::<KernelResult<Vec<_>>>()?;
        assert_eq!(stored.len(), expected_files);
        let mut rows = Vec::new();
        for meta in &stored {
            let name = meta.location.path_segments().unwrap().next_back().unwrap();
            Uuid::parse_str(
                name.strip_prefix("part-")
                    .unwrap()
                    .strip_suffix(".parquet")
                    .unwrap(),
            )
            .unwrap();
            let row = vec![
                name.into(),
                meta.location.to_string().into(),
                (meta.size as i64).into(),
                meta.last_modified.into(),
            ];
            rows.extend([row.clone(), row]);
        }
        if expected_files != 0 {
            let expected = values_to_record_batch(Values::new(FILES_META_SCHEMA.clone(), rows))?;
            let formatted = pretty_format_batches(&[expected])?.to_string();
            let mut expected: Vec<_> = formatted.trim().lines().collect();
            sort_lines!(expected);
            assert_batches_sorted_eq!(expected, &actual);
            let read = PlanBuilder::scan_parquet(stored, &[], schema)?.build()?;
            assert_batches_sorted_eq!(
                ["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"].as_slice(),
                &executor.eval_plan(read, None)?
            );
        }
        Ok(())
    }

    #[rstest]
    fn write_parquet_files_rejects_bad_later_batch_before_creating_files(
        #[values(false, true)] empty_bad_batch: bool,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let schema = schema_ref! { not_null "id": LONG };
        let first = PlanBuilder::values(schema.clone(), vec![vec![1i64.into()]])?;
        let second = PlanBuilder::values(schema.clone(), vec![vec![2i64.into()]])?;
        let base = Url::parse("memory:///files/")?;
        let mut plan = PlanBuilder::write_parquet_files(
            [first, second],
            schema,
            base.clone(),
            "{uuid}.parquet".into(),
        )?
        .build()?;
        let Operator::Values(second) = &mut plan.nodes[1].op else {
            panic!("expected Values")
        };
        second.schema = schema_ref! { not_null "id": STRING };
        second.rows = if empty_bad_batch {
            vec![]
        } else {
            vec![vec!["bad".into()]]
        };
        assert!(matches!(
            executor.eval_plan(plan, None),
            Err(KernelError::Schema(_))
        ));
        assert_eq!(executor.storage.list_from(&base)?.count(), 0);
        Ok(())
    }

    #[rstest]
    fn write_parquet_projects_fields_and_preserves_struct_stats(
        #[values(false, true)] multiple_files: bool,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let path = Url::parse("memory:///output.parquet")?;
        let stats = schema! {
            (StructField::nullable("numRecords", DataType::LONG)
                .with_metadata([("parquet.field.id", 2i64)])),
        };
        let input_schema = schema_ref! {
            nullable "stats": (stats.clone()),
            nullable "amount": LONG,
            nullable "region": STRING,
        };
        let write_schema = schema_ref! {
            (StructField::nullable("amount", DataType::LONG)
                .with_metadata([("parquet.field.id", 1i64)])),
            (StructField::nullable("stats", stats.clone())
                .with_metadata([("parquet.field.id", 3i64)])),
        };
        let rows = PlanBuilder::values(
            input_schema,
            vec![
                vec![
                    StructData::try_new(stats.fields().cloned().collect(), vec![2i64.into()])?
                        .into(),
                    10i64.into(),
                    "US".into(),
                ],
                vec![
                    Scalar::Null(stats.into()),
                    Scalar::Null(DataType::LONG),
                    "FR".into(),
                ],
            ],
        )?;
        let plan = if multiple_files {
            PlanBuilder::write_parquet_files(
                [rows],
                write_schema.clone(),
                Url::parse("memory:///")?,
                "{uuid}.parquet".into(),
            )?
            .project_patch(|patch| patch.drop("file_name"))?
        } else {
            PlanBuilder::write_parquet([rows], write_schema.clone(), path.clone(), false, false)?
        }
        .build()?;
        let result = executor.execute_op(Operation::QueryPlan(plan))?;
        let file = file_meta_from_batches(result.into_data()?)?;
        let path = file.location.clone();
        assert_eq!(file, executor.storage.head(&path)?);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(get_bytes(executor.storage.store(), &path)?)?;
        let fields = reader.parquet_schema().root_schema().get_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].get_basic_info().id(), 1);
        assert_eq!(fields[1].get_basic_info().id(), 3);
        assert_eq!(fields[1].get_fields()[0].get_basic_info().id(), 2);
        let plan = PlanBuilder::scan_parquet([file], &[], write_schema)?.build()?;
        let actual = executor.eval_plan(plan, None)?;
        #[rustfmt::skip]
        let expected: &[&str] = &[
            "+--------+-----------------+",
            "| amount | stats           |",
            "+--------+-----------------+",
            "|        |                 |",
            "| 10     | {numRecords: 2} |",
            "+--------+-----------------+",
        ];
        assert_batches_sorted_eq!(expected, &actual);
        Ok(())
    }

    #[rstest]
    fn write_parquet_preserves_file_annotation(
        #[values(false, true)] multiple_files: bool,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let base = Url::parse("memory:///")?;
        let schema = schema_ref! {
            (StructField::nullable("file", DataType::file_type())
                .with_metadata([("parquet.field.id", 10i64)])),
        };
        let input = PlanBuilder::values(
            schema.clone(),
            vec![vec![Scalar::Null(DataType::file_type())]],
        )?;
        let write = if multiple_files {
            PlanBuilder::write_parquet_files(
                [input],
                schema,
                base.clone(),
                "{uuid}.parquet".into(),
            )?
            .project_patch(|patch| patch.drop("file_name"))?
        } else {
            PlanBuilder::write_parquet([input], schema, base.join("output.parquet")?, false, false)?
        }
        .build()?;
        let result = executor.execute_op(Operation::QueryPlan(write));
        #[cfg(not(feature = "arrow-60"))]
        {
            assert!(matches!(result, Err(error)
                if error.to_string().contains("requires the `arrow-60` feature")));
            assert_eq!(executor.storage.list_from(&base)?.count(), 0);
        }
        #[cfg(feature = "arrow-60")]
        {
            let file = file_meta_from_batches(result?.into_data()?)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(get_bytes(
                executor.storage.store(),
                &file.location,
            )?)?;
            let groups = file_groups(reader.parquet_schema().root_schema());
            assert_eq!(groups.len(), 1);
            assert_eq!(groups[0].path, "file");
            assert_eq!(groups[0].id, Some(10));
            assert_eq!(
                groups[0].sub_field_ids,
                (11..17).map(Some).collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[rstest]
    #[case::struct_field(|schema: StructType| schema.into())]
    #[case::array_element(|schema: StructType| ArrayType::new(schema, true).into())]
    #[case::map_key(|schema: StructType| MapType::new(schema, DataType::LONG, true).into())]
    #[case::map_value(|schema: StructType| MapType::new(DataType::STRING, schema, true).into())]
    fn write_parquet_rejects_reordered_nested_fields(
        #[case] wrap: fn(StructType) -> DataType,
        #[values(false, true)] multiple_files: bool,
    ) -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let base = Url::parse("memory:///")?;
        let source_type = wrap(schema! { nullable "a": LONG, nullable "b": LONG });
        let source = schema_ref! { nullable "value": (source_type.clone()) };
        let target = schema_ref! {
            nullable "value": (wrap(schema! { nullable "b": LONG, nullable "a": LONG })),
        };
        let input = PlanBuilder::values(source.clone(), vec![vec![Scalar::Null(source_type)]])?;
        let write = |schema| {
            if multiple_files {
                PlanBuilder::write_parquet_files(
                    [input.clone()],
                    schema,
                    base.clone(),
                    "{uuid}.parquet".into(),
                )
            } else {
                PlanBuilder::write_parquet(
                    [input.clone()],
                    schema,
                    base.join("output.parquet")?,
                    false,
                    false,
                )
            }
        };
        assert!(matches!(write(target.clone()), Err(KernelError::Schema(_))));
        // Plans received by an executor need the same validation as locally built plans.
        let mut plan = write(source)?.build()?;
        match &mut plan.nodes.last_mut().unwrap().op {
            Operator::WriteParquet(write) => write.write_schema = target,
            Operator::WriteParquetFiles(write) => write.write_schema = target,
            _ => unreachable!(),
        }
        assert!(matches!(
            executor.eval_plan(plan, None),
            Err(KernelError::Schema(_))
        ));
        assert_eq!(executor.storage.list_from(&base)?.count(), 0);
        Ok(())
    }

    #[test]
    fn write_parquet_adds_nested_ids() -> KernelResult<()> {
        let executor = SyncPlanExecutor::new(Some(Arc::new(InMemory::new())));
        let path = Url::parse("memory:///nested.parquet")?;
        let element = schema! {
            (StructField::not_null("number", DataType::LONG).with_metadata([("numeric", 7i64)])),
        };
        let array_type = ArrayType::new(element.clone(), true);
        let map_type = MapType::new(DataType::STRING, array_type.clone(), true);
        let field = StructField::nullable("items", map_type.clone());
        let write_field = field.clone().with_metadata([(
            "delta.columnMapping.nested.ids",
            MetadataValue::Other(serde_json::json!({
                "items.key": 1, "items.value": 2, "items.value.element": 3,
            })),
        )]);
        let input_schema = schema_ref! { (field), };
        let write_schema = schema_ref! { (write_field), };
        let value = StructData::try_new(element.fields().cloned().collect(), vec![1i64.into()])?;
        let array = ArrayData::try_new(array_type, [value.into(), Scalar::Null(element.into())])?;
        let rows = vec![vec![
            MapData::try_new(map_type, [("x", Scalar::from(array))])?.into(),
        ]];
        let input = PlanBuilder::values(input_schema.clone(), rows.clone())?;
        let plan =
            PlanBuilder::write_parquet([input], write_schema.clone(), path.clone(), false, false)?
                .build()?;
        let result = executor.execute_op(Operation::QueryPlan(plan))?;
        let file = file_meta_from_batches(result.into_data()?)?;
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(get_bytes(executor.storage.store(), &path)?)?;
        let fields =
            reader.parquet_schema().root_schema().get_fields()[0].get_fields()[0].get_fields();
        assert_eq!(fields[0].get_basic_info().id(), 1);
        assert_eq!(fields[1].get_basic_info().id(), 2);
        assert_eq!(
            fields[1].get_fields()[0].get_fields()[0]
                .get_basic_info()
                .id(),
            3
        );
        let actual = PlanBuilder::scan_parquet([file], &[], input_schema.clone())?.build()?;
        let expected = PlanBuilder::values(input_schema, rows)?.build()?;
        assert_eq!(
            executor.eval_plan(actual, None)?,
            executor.eval_plan(expected, None)?
        );
        Ok(())
    }

    #[test]
    fn scoped_executor_creates_ref_from_arrow_batch() -> KernelResult<()> {
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
        assert!(
            error
                .to_string()
                .contains("record batch schema does not match")
        );

        let relation_ref = scoped.create_ref("arrow-batch", schema, batch.clone())?;
        assert!(relation_ref.id().starts_with("arrow-batch-"));
        let query = PlanBuilder::relation_source(relation_ref.clone()).build()?;
        let scoped_batches: Vec<RecordBatch> = scoped
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<KernelResult<_>>()?;
        assert_eq!(scoped_batches, vec![batch]);

        let query = PlanBuilder::relation_source(relation_ref).build()?;
        let error = executor
            .execute_op(Operation::QueryPlan(query))
            .err()
            .expect("the unscoped executor must reject a scope's relation");
        assert!(
            error
                .to_string()
                .contains("requires an executor that can store relations")
        );
        Ok(())
    }

    #[test]
    fn aggregate_respects_nested_parent_nulls_from_relation() -> KernelResult<()> {
        let schema = schema_ref! { nullable "nested": { nullable "value": LONG } };
        let nested = StructArray::new(
            vec![Arc::new(Field::new("value", ArrowDataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![99, 7]))],
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(nested)],
        )?;
        let scoped = SyncPlanExecutor::default().get_scoped();
        let source = scoped.create_ref("nested", schema, batch)?;

        let path = column_name!("nested.value");
        let aggregate = PlanBuilder::relation_source(source)
            .aggregate_ungrouped(|aggs| {
                aggs.aggregate_as(Agg::count(path.clone()), "count")
                    .aggregate_as(Agg::first_non_null(path.clone()), "first")
                    .aggregate_as(Agg::min(path.clone()), "min")
                    .aggregate_as(Agg::sum(path.clone()), "sum")
                    .aggregate_as(
                        Agg::min_non_null_by(path.clone(), path.clone(), path),
                        "selected",
                    )
            })?
            .build()?;
        let batches = scoped
            .executor
            .eval_plan(aggregate, Some(&scoped.relation_store))?;
        let values: Vec<_> = batches[0]
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap()
            })
            .collect();
        assert_eq!(values, [Some(1), Some(7), Some(7), Some(7), Some(7)]);
        Ok(())
    }

    #[test]
    fn aggregate_respects_nested_parent_nulls_from_project() -> KernelResult<()> {
        let nested = Expression::struct_with_nullability_from(
            [col!("value")],
            Expression::from(Predicate::eq(col!("value"), lit(7i64))),
        );
        let plan = PlanBuilder::values(
            schema_ref! { not_null "value": LONG },
            vec![vec![99i64.into()], vec![7i64.into()]],
        )?
        .project(
            Expression::struct_from([nested]),
            schema_ref! { nullable "nested": { nullable "value": LONG } },
        )?
        .aggregate_ungrouped(|aggs| {
            aggs.aggregate_as(Agg::count(column_name!("nested.value")), "count")
                .aggregate_as(Agg::first_non_null(column_name!("nested.value")), "first")
        })?
        .build()?;
        let batches = SyncPlanExecutor::default().eval_plan(plan, None)?;
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let first = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.iter().collect::<Vec<_>>(), [Some(1)]);
        assert_eq!(first.iter().collect::<Vec<_>>(), [Some(7)]);
        Ok(())
    }

    #[test]
    fn filter_respects_nested_parent_nulls_from_relation() -> KernelResult<()> {
        let schema = schema_ref! { nullable "nested": { nullable "value": LONG } };
        let nested = StructArray::new(
            vec![Arc::new(Field::new("value", ArrowDataType::Int64, true))].into(),
            vec![Arc::new(Int64Array::from(vec![99, 7]))],
            Some(NullBuffer::from(vec![false, true])),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![Arc::new(nested)],
        )?;
        let scoped = SyncPlanExecutor::default().get_scoped();
        let source = scoped.create_ref("nested", schema, batch)?;
        let plan = PlanBuilder::relation_source(source)
            .filter(Predicate::is_not_null(col!("nested.value")))?
            .project(
                Expression::struct_from([col!("nested.value")]),
                schema_ref! { nullable "value": LONG },
            )?
            .build()?;
        let output = scoped
            .executor
            .eval_plan(plan, Some(&scoped.relation_store))?;
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(7)]);
        Ok(())
    }

    #[test]
    fn project_returns_logical_values_from_nullable_struct() -> KernelResult<()> {
        let nested = Expression::struct_with_nullability_from(
            [col!("value")],
            Expression::from(Predicate::eq(col!("value"), lit(7i64))),
        );
        let plan = PlanBuilder::values(
            schema_ref! { not_null "value": LONG },
            vec![vec![99i64.into()], vec![7i64.into()]],
        )?
        .project(
            Expression::struct_from([nested]),
            schema_ref! { nullable "nested": { nullable "value": LONG } },
        )?
        .project(
            Expression::struct_from([col!("nested.value")]),
            schema_ref! { nullable "value": LONG },
        )?
        .build()?;
        let output = SyncPlanExecutor::default().eval_plan(plan, None)?;
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [None, Some(7)]);
        Ok(())
    }

    #[test]
    fn scoped_executor_rejects_batches_that_differ_from_plan_schema() -> KernelResult<()> {
        let scoped = SyncPlanExecutor::default().get_scoped();
        let batch_schema = schema_ref! { nullable "id": LONG };
        let mut plan = PlanBuilder::values(batch_schema, vec![vec![1i64.into()]])?.build()?;
        plan.schema = schema_ref! { nullable "other": LONG };

        let error = scoped
            .execute_and_retain("wrong-schema", plan)
            .expect_err("a mismatched plan schema must be rejected");
        assert!(
            error
                .to_string()
                .contains("record batch schema does not match")
        );
        assert!(scoped.relation_store.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn scoped_executor_retains_and_reuses_filtered_relations() -> KernelResult<()> {
        let executor = SyncPlanExecutor::default();
        let scoped = executor.get_scoped();
        let schema = schema_ref! { nullable "id": LONG };
        let source = PlanBuilder::values(
            schema.clone(),
            vec![vec![1i64.into()], vec![2i64.into()], vec![3i64.into()]],
        )?
        .filter(Predicate::from_expr(expr!(col("id") > 1i64)))?
        .build()?;
        let first = scoped.execute_and_retain("filtered", source)?;
        assert_eq!(first.schema(), &schema);
        assert!(first.id().starts_with("filtered-"));

        let second_source = PlanBuilder::relation_source(first.clone())
            .filter(Predicate::from_expr(expr!(col("id") < 3i64)))?
            .build()?;
        let second = scoped.execute_and_retain("refiltered", second_source)?;
        assert_ne!(first.id(), second.id());
        assert_eq!(second.schema(), &schema);

        let query = PlanBuilder::relation_source(second).build()?;
        let batches: Vec<RecordBatch> = scoped
            .execute_op(Operation::QueryPlan(query))?
            .into_data()?
            .map(|data| data.try_into_record_batch())
            .collect::<KernelResult<_>>()?;
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
    fn scoped_executors_isolate_relations_and_release_batches_on_drop() -> KernelResult<()> {
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
        assert!(
            error
                .to_string()
                .contains("requires an executor that can store relations")
        );

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
            .collect::<KernelResult<_>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        Ok(())
    }

    #[test]
    fn scoped_executor_rejects_relation_ref_with_wrong_schema() -> KernelResult<()> {
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
    fn scoped_executor_retains_empty_relation_with_schema() -> KernelResult<()> {
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
            .collect::<KernelResult<_>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        Ok(())
    }

    #[test]
    fn encode_keys_as_rows_synthesizes_empty_keys_when_ungrouped() -> KernelResult<()> {
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
    ) -> KernelResult<Vec<ScanFile>> {
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
