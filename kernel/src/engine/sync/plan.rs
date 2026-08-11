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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;

use super::aggs::eval_aggregate;
use super::json::try_create_from_json;
use super::parquet::{parquet_footer, try_create_from_parquet};
use super::read_files_arrow;
use super::storage::SyncStorageHandler;
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
use crate::engine::arrow_utils::coerce_columns_to_schema;
use crate::expressions::{ArrayData, ColumnName, PredicateRef, Scalar};
use crate::object_store::DynObjectStore;
use crate::plans::ir::nodes::{
    DynamicScan, FileType, Operator, Project, ScanFile, ScanJson, ScanParquet, SemiJoin, Values,
};
use crate::plans::ir::plan::{Plan, PlanNode};
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use crate::schema::{ArrayType, DataType, SchemaRef, StructType};
use crate::{DeltaResult, Error, EvaluationHandler as _, FileMeta, StorageHandler as _};

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
        results: &[Vec<RecordBatch>],
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
            Operator::UnionAll(_) => Ok(Vec::from_iter(
                inputs.iter().flat_map(|&i| results[i].iter().cloned()),
            )),
            Operator::Project(project) => eval_project(project, &results[inputs[0]]),
            Operator::Filter(filter) => eval_filter(filter.predicate, &results[inputs[0]]),
            Operator::DynamicScan(dynamic_scan) => {
                self.eval_dynamic_scan(dynamic_scan, &results[inputs[0]])
            }
            Operator::Aggregate(aggregate) => eval_aggregate(&aggregate, &results[inputs[0]]),
            Operator::SemiJoin(join) => {
                eval_semi_join(join, &results[inputs[0]], &results[inputs[1]])
            }
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
    ) -> DeltaResult<Vec<RecordBatch>> {
        let files = dynamic_scan_files(&dynamic_scan, input)?;
        self.eval_scan(
            dynamic_scan.file_type,
            files,
            dynamic_scan.file_constant_columns,
            dynamic_scan.schema,
        )
    }
}

fn dynamic_scan_files(
    dynamic_scan: &DynamicScan,
    input: &[RecordBatch],
) -> DeltaResult<Vec<ScanFile>> {
    let mut files = Vec::new();
    for batch in input {
        let path = extract_column(batch, dynamic_scan.path_column.path())?;
        let path = path.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            Error::generic(format!(
                "Expected STRING Load path, got {:?}",
                path.data_type()
            ))
        })?;
        let size = extract_column(batch, dynamic_scan.file_size_column.path())?;
        let size = size.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            Error::generic(format!(
                "Expected LONG Load file size, got {:?}",
                size.data_type()
            ))
        })?;
        let last_modified = extract_column_ref(batch, dynamic_scan.last_modified_column.path())?;
        let last_modified = last_modified
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::generic(format!(
                    "Expected LONG Load last modified, got {:?}",
                    last_modified.data_type()
                ))
            })?;
        let dv_path = dynamic_scan.dv_column.path();
        let dv = extract_column(batch, dv_path)?;
        let dv_ancestors: Vec<_> = (1..dv_path.len())
            .map(|len| extract_column(batch, &dv_path[..len]))
            .try_collect()?;

        for row in 0..batch.num_rows() {
            if path.is_null(row) {
                return Err(Error::generic("DynamicScan path must not be null"));
            }
            if dv.is_valid(row) && dv_ancestors.iter().all(|ancestor| ancestor.is_valid(row)) {
                return Err(Error::unsupported(
                    "SyncPlanExecutor DynamicScan with deletion vectors",
                ));
            }
            let path = path.value(row);
            let location = dynamic_scan.base_url.join(path)?;
            if size.is_null(row) {
                return Err(Error::generic("DynamicScan file size must not be null"));
            }
            let size = size.value(row);
            if size <= 0 {
                return Err(Error::generic("DynamicScan file size must be positive"));
            }
            let size = u64::try_from(size)
                .map_err(|_| Error::generic("DynamicScan file size must fit in a u64"))?;
            if last_modified.is_null(row) {
                return Err(Error::generic(
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

fn eval_project(project: Project, input: &[RecordBatch]) -> DeltaResult<Vec<RecordBatch>> {
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

fn eval_filter(predicate: PredicateRef, input: &[RecordBatch]) -> DeltaResult<Vec<RecordBatch>> {
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
                    Error::generic("Filter predicate did not produce a boolean array")
                })?;
            Ok(filter_record_batch(batch, mask)?)
        })
        .collect()
}

fn eval_semi_join(
    join: SemiJoin,
    probe: &[RecordBatch],
    build: &[RecordBatch],
) -> DeltaResult<Vec<RecordBatch>> {
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

/// Encodes `columns` of `batch` as one comparable/hashable [`OwnedRow`] key per input row.
///
/// Empty `columns` (e.g. ungrouped aggregation) yields a vec of empty keys that self-compare equal
pub(super) fn encode_keys_as_rows(
    batch: &RecordBatch,
    columns: &[ColumnName],
) -> DeltaResult<Vec<OwnedRow>> {
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

fn scalar_value(array: &dyn Array, row: usize) -> DeltaResult<Scalar> {
    if array.is_null(row) {
        return Ok(Scalar::Null(DataType::try_from_arrow(array.data_type())?));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Scalar::String(strings.value(row).to_string()));
    }
    if let Some(longs) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Scalar::Long(longs.value(row)));
    }
    Err(Error::unsupported(format!(
        "Scalar conversion from array type {:?}",
        array.data_type()
    )))
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::deletion_vector::DeletionVectorDescriptor;
    use crate::arrow::array::StructArray;
    use crate::arrow::buffer::{BooleanBuffer, NullBuffer};
    use crate::expressions::StructData;
    use crate::schema::{StructField, ToSchema as _};

    #[test]
    fn encode_keys_as_rows_synthesizes_empty_keys_when_ungrouped() -> DeltaResult<()> {
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
        Scalar::Null(DataType::from(DeletionVectorDescriptor::to_schema()))
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
    ) -> DeltaResult<Vec<ScanFile>> {
        let input_schema = Arc::new(StructType::new_unchecked([
            StructField::nullable("path", DataType::STRING),
            StructField::nullable("size", DataType::LONG),
            StructField::nullable("filemod", DataType::LONG),
            StructField::nullable("dv", DeletionVectorDescriptor::to_schema()),
        ]));
        let input = values_to_record_batch(Values::new(
            input_schema,
            vec![vec![path, size, last_modified, dv]],
        ))
        .unwrap();
        let dynamic_scan = DynamicScan {
            schema: Arc::new(StructType::new_unchecked(Vec::<StructField>::new())),
            file_type: FileType::Parquet,
            base_url: Url::parse("memory:///").unwrap(),
            file_constant_columns: vec![],
            path_column: ColumnName::new(["path"]),
            file_size_column: ColumnName::new(["size"]),
            last_modified_column: ColumnName::new(["filemod"]),
            dv_column: ColumnName::new(["dv"]),
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
    fn dynamic_scan_treats_dv_under_null_ancestor_as_null() {
        let dv_field = StructField::nullable("dv", DeletionVectorDescriptor::to_schema());
        let metadata_type = StructType::new_unchecked([dv_field]);
        let input_schema = Arc::new(StructType::new_unchecked([
            StructField::not_null("path", DataType::STRING),
            StructField::not_null("size", DataType::LONG),
            StructField::not_null("filemod", DataType::LONG),
            StructField::nullable("metadata", metadata_type.clone()),
        ]));
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
            schema: Arc::new(StructType::new_unchecked(Vec::<StructField>::new())),
            file_type: FileType::Parquet,
            base_url: Url::parse("memory:///").unwrap(),
            file_constant_columns: vec![],
            path_column: ColumnName::new(["path"]),
            file_size_column: ColumnName::new(["size"]),
            last_modified_column: ColumnName::new(["filemod"]),
            dv_column: ColumnName::new(["metadata", "dv"]),
        };

        assert_eq!(
            dynamic_scan_files(&dynamic_scan, &[input]).unwrap().len(),
            1
        );
    }
}
