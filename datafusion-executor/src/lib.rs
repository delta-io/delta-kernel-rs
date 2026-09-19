//! A DataFusion-based [`PlanExecutor`] for delta_kernel declarative plans.
//!
//! Kernel emits executor-independent logical [`Plan`]s. This crate lowers them to DataFusion
//! logical plans, plans and executes them, and adapts their Arrow output back to [`EngineData`].

use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use datafusion::physical_plan::collect;
use delta_kernel::engine::arrow_conversion::TryIntoKernel as _;
use delta_kernel::engine::arrow_data::{fix_nested_null_masks, ArrowEngineData};
use delta_kernel::plans::ir::plan::Plan;
use delta_kernel::{
    DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileMeta, IoOperation, Operation,
    ParquetFooter, PlanExecutor, PlanResult, StorageHandler,
};
use tokio::runtime::Handle;

mod dynamic_scan;
mod expression;
mod operator;
mod parquet_expr_adapter;
mod plan;
mod predicate;
mod scalar;
mod scan;
mod utils;

pub use expression::to_df_expr;
use plan::to_df_plan;
pub use predicate::to_df_predicate_expr;
pub use scalar::to_df_scalar;

/// Executes kernel declarative plans on DataFusion.
///
/// The caller supplies three resources because each belongs to the connector rather than the
/// executor:
///
/// - `session_context` is DataFusion's long-lived [`SessionContext`]. It holds optimizer rules,
///   functions, catalogs, and the runtime environment whose object stores and resource limits are
///   used by parquet and JSON scans. The caller supplies it so kernel plans use the connector's
///   DataFusion configuration. The executor snapshots it per query to create both the physical plan
///   and its matching task context.
/// - `storage_handler` implements kernel's [`StorageHandler`] contract for explicit
///   [`IoOperation`]s such as listing files, reading or writing bytes, and reading parquet footers.
///   These operations do not run through DataFusion's object stores. The caller therefore supplies
///   its underlying handler, not a `PlanBasedStorageHandler` backed by this executor, which would
///   recurse into itself.
/// - `runtime_handle` identifies the caller-owned Tokio [`Handle`]. DataFusion planning and
///   execution are asynchronous, while [`PlanExecutor`] is synchronous, so the executor uses this
///   handle to schedule the query and waits for its collected result. A Tokio runtime is separate
///   from DataFusion's runtime environment, and the executor does not create one because its
///   lifecycle and enabled drivers belong to the connector.
///
/// [`PlanExecutor::execute_op`] is synchronous and does not return until query results are
/// collected. Callers using the executor from async code must run it on a blocking thread such as
/// one provided by `tokio::task::spawn_blocking`. The returned data iterator is entirely in memory
/// and can be consumed directly. Blocking a current-thread runtime while this executor submits work
/// to that runtime will deadlock.
pub struct DataFusionExecutor {
    session_context: SessionContext,
    storage_handler: Arc<dyn StorageHandler>,
    runtime_handle: Handle,
}

impl DataFusionExecutor {
    /// Creates an executor from caller-owned DataFusion, storage, and Tokio resources.
    ///
    /// The runtime referenced by `runtime_handle` must remain alive while a query operation is
    /// executing. Returned iterators do not depend on it.
    ///
    /// Returns an executor that uses only these caller-provided resources.
    pub fn new(
        session_context: SessionContext,
        storage_handler: Arc<dyn StorageHandler>,
        runtime_handle: Handle,
    ) -> Self {
        Self {
            session_context,
            storage_handler,
            runtime_handle,
        }
    }

    fn execute_query(&self, plan: Plan) -> DeltaResult<PlanResult> {
        let logical_plan = to_df_plan(&plan).map_err(Error::generic_err)?;
        let session_state = self.session_context.state();
        let task_ctx = session_state.task_ctx();
        let batches = futures::executor::block_on(self.runtime_handle.spawn(async move {
            let physical_plan = session_state.create_physical_plan(&logical_plan).await?;
            collect(physical_plan, task_ctx).await
        }))
        .map_err(Error::join_failure)?
        .map_err(Error::generic_err)?;

        let data: DeltaResultIteratorStatic<Box<dyn EngineData>> =
            Box::new(batches.into_iter().map(|batch| {
                let batch: RecordBatch = fix_nested_null_masks(batch.into()).into();
                Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
            }));
        Ok(PlanResult::Data(data))
    }

    fn execute_io(&self, op: IoOperation) -> DeltaResult<PlanResult> {
        match op {
            IoOperation::FileListing { url } => {
                // TODO(#2619): Remove this collection when StorageHandler returns Send iterators.
                let files: Vec<_> = self.storage_handler.list_from(&url)?.collect();
                Ok(PlanResult::FileMeta(Box::new(files.into_iter())))
            }
            IoOperation::ReadBytes { files } => {
                // TODO(#2619): Remove this collection when StorageHandler returns Send iterators.
                let bytes: Vec<_> = self.storage_handler.read_files(files)?.collect();
                Ok(PlanResult::Bytes(Box::new(bytes.into_iter())))
            }
            IoOperation::WriteBytes {
                url,
                data,
                overwrite,
            } => {
                self.storage_handler.put(&url, data, overwrite)?;
                Ok(PlanResult::Unit)
            }
            IoOperation::HeadFile { url } => {
                let file = self.storage_handler.head(&url)?;
                Ok(PlanResult::FileMeta(Box::new(std::iter::once(Ok(file)))))
            }
            IoOperation::AtomicCopy {
                source,
                destination,
            } => {
                self.storage_handler.copy_atomic(&source, &destination)?;
                Ok(PlanResult::Unit)
            }
            IoOperation::ParquetFooter { file } => {
                let footer = self.read_footer(&file)?;
                Ok(PlanResult::ParquetFooter(footer))
            }
        }
    }

    fn read_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        const FOOTER_SIZE: u64 = 8;

        let footer_start = file
            .size
            .checked_sub(FOOTER_SIZE)
            .ok_or_else(|| Error::generic("Parquet file is smaller than its eight-byte footer"))?;
        let footer_bytes = self.read_range(file, footer_start..file.size)?;
        let footer_bytes: [u8; FOOTER_SIZE as usize] = footer_bytes
            .as_ref()
            .try_into()
            .map_err(Error::generic_err)?;
        let footer = FooterTail::try_new(&footer_bytes)?;
        if footer.is_encrypted_footer() {
            return Err(Error::unsupported(
                "encrypted Parquet footers require decryption properties",
            ));
        }

        let metadata_length: u64 = footer
            .metadata_length()
            .try_into()
            .map_err(Error::generic_err)?;
        let metadata_start = footer_start.checked_sub(metadata_length).ok_or_else(|| {
            Error::generic("Parquet footer metadata length exceeds the file size")
        })?;
        let metadata_bytes = self.read_range(file, metadata_start..footer_start)?;
        let metadata = ParquetMetaDataReader::decode_metadata(&metadata_bytes)?;
        let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let metadata = ArrowReaderMetadata::try_new(Arc::new(metadata), options)?;
        let schema = Arc::new(metadata.schema().as_ref().try_into_kernel()?);
        Ok(ParquetFooter { schema })
    }

    fn read_range(&self, file: &FileMeta, range: Range<u64>) -> DeltaResult<bytes::Bytes> {
        let expected_length = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| Error::internal_error("Parquet footer read range is inverted"))?;
        let mut results = self
            .storage_handler
            .read_files(vec![(file.location.clone(), Some(range.clone()))])?;
        let bytes = results
            .next()
            .ok_or_else(|| Error::generic("StorageHandler returned no bytes for a range read"))??;
        if results.next().is_some() {
            return Err(Error::generic(
                "StorageHandler returned multiple results for one range read",
            ));
        }

        let actual_length: u64 = bytes.len().try_into().map_err(Error::generic_err)?;
        if actual_length == expected_length {
            return Ok(bytes);
        }
        if actual_length != file.size {
            let message = format!(
                "StorageHandler returned {actual_length} bytes for a range of {expected_length} bytes"
            );
            return Err(Error::generic(message));
        }

        // A handler that cannot issue range requests may return the full file; slice it locally.
        let start: usize = range.start.try_into().map_err(Error::generic_err)?;
        let end: usize = range.end.try_into().map_err(Error::generic_err)?;
        Ok(bytes.slice(start..end))
    }
}

impl PlanExecutor for DataFusionExecutor {
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        match op {
            Operation::QueryPlan(plan) => self.execute_query(plan),
            Operation::IoOperation(op) => self.execute_io(op),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StructArray};
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
    use datafusion::logical_expr::{
        col as df_col, LogicalPlan as DFLogicalPlan, LogicalPlanBuilder,
    };
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path as ObjectPath;
    use datafusion::object_store::ObjectStoreExt as _;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
    use delta_kernel::expressions::{column_name, Scalar, StructData};
    use delta_kernel::plans::ir::nodes::{DynamicScan, FileType, ScanFile, ScanParquet, Values};
    use delta_kernel::plans::ir::plan::{Plan as KernelPlan, PlanNode};
    use delta_kernel::schema::{
        schema_ref, ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
        ToSchema as _,
    };
    use delta_kernel::{FileSlice, StorageHandler};
    use rstest::rstest;
    use url::Url;

    use super::*;

    #[derive(Default)]
    struct TestStorage {
        data: Bytes,
        return_full_file: bool,
    }

    impl StorageHandler for TestStorage {
        fn list_from(
            &self,
            _path: &Url,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<FileMeta>>>> {
            unimplemented!()
        }

        fn read_files(
            &self,
            files: Vec<FileSlice>,
        ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
            let mut output = Vec::with_capacity(files.len());
            for (_, range) in files {
                let bytes = match (self.return_full_file, range) {
                    (false, Some(range)) => self.data.slice(
                        usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap(),
                    ),
                    _ => self.data.clone(),
                };
                output.push(Ok(bytes));
            }
            Ok(Box::new(output.into_iter()))
        }

        fn copy_atomic(&self, _src: &Url, _dest: &Url) -> DeltaResult<()> {
            unimplemented!()
        }

        fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> DeltaResult<()> {
            unimplemented!()
        }

        fn head(&self, _path: &Url) -> DeltaResult<FileMeta> {
            unimplemented!()
        }

        fn delete(&self, _path: &Url) -> DeltaResult<()> {
            unimplemented!()
        }
    }

    fn test_executor(runtime_handle: Handle) -> DataFusionExecutor {
        DataFusionExecutor::new(
            SessionContext::new(),
            Arc::new(TestStorage::default()),
            runtime_handle,
        )
    }

    #[test]
    fn constructor_does_not_modify_shared_session_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let session = SessionContext::new();
        let session_id = session.session_id();
        let original_state = session.state();
        let original_planner = Arc::clone(original_state.query_planner());
        let original_create_default_catalog = original_state
            .config_options()
            .catalog
            .create_default_catalog_and_schema;

        let executor = DataFusionExecutor::new(
            session.clone(),
            Arc::new(TestStorage::default()),
            runtime.handle().clone(),
        );

        let session_state = session.state();
        let executor_state = executor.session_context.state();
        assert_eq!(session_state.session_id(), session_id);
        assert_eq!(executor_state.session_id(), session_id);
        assert!(Arc::ptr_eq(
            session_state.query_planner(),
            &original_planner
        ));
        assert!(Arc::ptr_eq(
            session_state.query_planner(),
            executor_state.query_planner()
        ));
        assert_eq!(
            session_state
                .config_options()
                .catalog
                .create_default_catalog_and_schema,
            original_create_default_catalog
        );
    }

    fn test_file(data: &Bytes) -> FileMeta {
        FileMeta::new(
            Url::parse("memory:///test.parquet").unwrap(),
            0,
            data.len().try_into().unwrap(),
        )
    }

    fn values_plan() -> KernelPlan {
        let schema = StructType::try_new([StructField::nullable("a", DataType::LONG)]).unwrap();
        let values = Values::new(schema, vec![vec![1i64.into()], vec![2i64.into()]]);
        KernelPlan {
            nodes: vec![PlanNode::new(values, vec![])],
        }
    }

    fn parquet_scan_plan(files: impl IntoIterator<Item = FileMeta>) -> KernelPlan {
        let schema =
            Arc::new(StructType::try_new([StructField::nullable("id", DataType::LONG)]).unwrap());
        let scan = ScanParquet {
            files: files.into_iter().map(ScanFile::new).collect(),
            file_constant_columns: Vec::new(),
            schema,
        };
        KernelPlan {
            nodes: vec![PlanNode::new(scan, vec![])],
        }
    }

    fn dynamic_scan_plan(
        files: impl IntoIterator<Item = (&'static str, u64, i64)>,
        file_type: FileType,
        deletion_vector: Scalar,
    ) -> KernelPlan {
        let output_schema = schema_ref! {
            nullable "version": LONG,
            nullable "id": LONG,
        };
        dynamic_scan_plan_with_schema(files, file_type, deletion_vector, output_schema)
    }

    fn dynamic_scan_plan_with_schema(
        files: impl IntoIterator<Item = (&'static str, u64, i64)>,
        file_type: FileType,
        deletion_vector: Scalar,
        output_schema: delta_kernel::schema::SchemaRef,
    ) -> KernelPlan {
        let input_schema = schema_ref! {
            not_null "path": STRING,
            not_null "size": LONG,
            not_null "filemod": LONG,
            nullable "dv": (DeletionVectorDescriptor::to_schema()),
            nullable "version": LONG,
        };
        let rows = files
            .into_iter()
            .map(|(path, size, version)| {
                vec![
                    path.into(),
                    i64::try_from(size).unwrap().into(),
                    0_i64.into(),
                    deletion_vector.clone(),
                    version.into(),
                ]
            })
            .collect();
        let values = Values::new(Arc::clone(&input_schema), rows);
        let dynamic_scan = DynamicScan::try_new(
            &input_schema,
            output_schema,
            file_type,
            Url::parse("memory:///sidecars/").unwrap(),
            ["version"],
            column_name!("path"),
            column_name!("size"),
            column_name!("filemod"),
            column_name!("dv"),
        )
        .unwrap();
        KernelPlan {
            nodes: vec![
                PlanNode::new(values, vec![]),
                PlanNode::new(dynamic_scan, vec![0]),
            ],
        }
    }

    fn present_deletion_vector() -> Scalar {
        Scalar::Struct(
            StructData::try_new(
                DeletionVectorDescriptor::to_schema()
                    .fields()
                    .cloned()
                    .collect(),
                vec![
                    "i".into(),
                    "inline".into(),
                    Scalar::null(DataType::INTEGER),
                    1_i32.into(),
                    1_i64.into(),
                ],
            )
            .unwrap(),
        )
    }

    fn parquet_bytes() -> Bytes {
        let field = Field::new("id", ArrowDataType::Int64, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "7".to_string(),
        )]));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let mut output = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut output, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(output)
    }

    fn nested_nullable_parquet_bytes() -> Bytes {
        let fields = Fields::from(vec![Arc::new(Field::new(
            "required",
            ArrowDataType::Int64,
            true,
        ))]);
        let action = StructArray::new(
            fields.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1)]))],
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "action",
            ArrowDataType::Struct(fields),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(action)]).unwrap();
        let mut output = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut output, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(output)
    }

    async fn assert_query_execution_from_spawn_blocking() {
        let executor = test_executor(Handle::current());
        let data = tokio::task::spawn_blocking(move || -> DeltaResult<_> {
            executor
                .execute_op(Operation::QueryPlan(values_plan()))?
                .into_data()
        })
        .await
        .unwrap()
        .unwrap();
        let batches: Vec<_> = data
            .map(|batch| batch?.try_into_record_batch())
            .collect::<DeltaResult<_>>()
            .unwrap();

        assert_batches_eq!(
            &["+---+", "| a |", "+---+", "| 1 |", "| 2 |", "+---+"],
            &batches
        );
    }

    #[tokio::test]
    async fn query_execution_uses_a_current_thread_runtime_from_spawn_blocking() {
        assert_query_execution_from_spawn_blocking().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_execution_uses_a_multi_thread_runtime_from_spawn_blocking() {
        assert_query_execution_from_spawn_blocking().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_execution_observes_later_session_context_registration() {
        let parquet = parquet_bytes();
        let store = Arc::new(InMemory::new());
        let locations = ["first.parquet", "second.parquet"];
        for location in locations {
            store
                .put(&ObjectPath::from(location), parquet.clone().into())
                .await
                .unwrap();
        }

        let session = SessionContext::new();
        let executor = DataFusionExecutor::new(
            session.clone(),
            Arc::new(TestStorage::default()),
            Handle::current(),
        );
        session.register_object_store(&Url::parse("memory:///").unwrap(), store);

        let size = parquet.len().try_into().unwrap();
        let files = locations.map(|location| {
            FileMeta::new(
                Url::parse(&format!("memory:///{location}")).unwrap(),
                0,
                size,
            )
        });
        let batches = tokio::task::spawn_blocking(move || -> DeltaResult<Vec<RecordBatch>> {
            executor
                .execute_op(Operation::QueryPlan(parquet_scan_plan(files)))?
                .into_data()?
                .map(|batch| batch?.try_into_record_batch())
                .collect()
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_scan_reads_runtime_parquet_files_and_broadcasts_constants() {
        let parquet = parquet_bytes();
        let store = Arc::new(InMemory::new());
        for location in ["sidecars/first.parquet", "sidecars/second.parquet"] {
            store
                .put(&ObjectPath::from(location), parquet.clone().into())
                .await
                .unwrap();
        }

        let session = SessionContext::new();
        let executor = DataFusionExecutor::new(
            session.clone(),
            Arc::new(TestStorage::default()),
            Handle::current(),
        );
        session.register_object_store(&Url::parse("memory:///").unwrap(), store);
        let size = parquet.len().try_into().unwrap();
        let plan = dynamic_scan_plan(
            [("first.parquet", size, 10), ("second.parquet", size, 20)],
            FileType::Parquet,
            Scalar::null(DeletionVectorDescriptor::to_schema()),
        );

        let batches = tokio::task::spawn_blocking(move || -> DeltaResult<Vec<RecordBatch>> {
            executor
                .execute_op(Operation::QueryPlan(plan))?
                .into_data()?
                .map(|batch| batch?.try_into_record_batch())
                .collect()
        })
        .await
        .unwrap()
        .unwrap();

        assert_batches_sorted_eq!(
            &[
                "+---------+----+",
                "| version | id |",
                "+---------+----+",
                "| 10      |    |",
                "| 10      | 1  |",
                "| 20      |    |",
                "| 20      | 1  |",
                "+---------+----+",
            ],
            &batches
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_scan_reconciles_nested_parquet_nullability() {
        let parquet = nested_nullable_parquet_bytes();
        let store = Arc::new(InMemory::new());
        store
            .put(
                &ObjectPath::from("sidecars/file.parquet"),
                parquet.clone().into(),
            )
            .await
            .unwrap();

        let session = SessionContext::new();
        let executor = DataFusionExecutor::new(
            session.clone(),
            Arc::new(TestStorage::default()),
            Handle::current(),
        );
        session.register_object_store(&Url::parse("memory:///").unwrap(), store);
        let output_schema = schema_ref! {
            nullable "version": LONG,
            nullable "action": {
                not_null "required": LONG,
            },
        };
        let expected_schema: Schema = output_schema.as_ref().try_into_arrow().unwrap();
        let size = parquet.len().try_into().unwrap();
        let plan = dynamic_scan_plan_with_schema(
            [("file.parquet", size, 10)],
            FileType::Parquet,
            Scalar::null(DeletionVectorDescriptor::to_schema()),
            output_schema,
        );

        let batches = tokio::task::spawn_blocking(move || -> DeltaResult<Vec<RecordBatch>> {
            executor
                .execute_op(Operation::QueryPlan(plan))?
                .into_data()?
                .map(|batch| batch?.try_into_record_batch())
                .collect()
        })
        .await
        .unwrap()
        .unwrap();

        let [batch] = batches.as_slice() else {
            panic!("expected one batch, got {}", batches.len());
        };
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        assert_eq!(batch.num_rows(), 1);
        let action = batch
            .column_by_name("action")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let required = action
            .column_by_name("required")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(required.value(0), 1);
    }

    #[tokio::test]
    async fn dynamic_scan_table_provider_uses_default_planner_and_pushes_projection() {
        let parquet = parquet_bytes();
        let parquet_size = parquet.len().try_into().unwrap();
        let store = Arc::new(InMemory::new());
        store
            .put(&ObjectPath::from("sidecars/file.parquet"), parquet.into())
            .await
            .unwrap();

        let session = SessionContext::new();
        session.register_object_store(&Url::parse("memory:///").unwrap(), store);
        let plan = dynamic_scan_plan(
            [("file.parquet", parquet_size, 10)],
            FileType::Parquet,
            Scalar::null(DeletionVectorDescriptor::to_schema()),
        );
        let logical_plan = to_df_plan(&plan).unwrap();
        assert!(matches!(&logical_plan, DFLogicalPlan::TableScan(_)));
        let projected_plan = LogicalPlanBuilder::from(logical_plan)
            .project([df_col("id")])
            .unwrap()
            .build()
            .unwrap();

        let dataframe = session.execute_logical_plan(projected_plan).await.unwrap();
        let batches = dataframe.collect().await.unwrap();

        assert_batches_eq!(
            &["+----+", "| id |", "+----+", "| 1  |", "|    |", "+----+"],
            &batches
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_scan_rejects_non_null_deletion_vectors() {
        let executor = test_executor(Handle::current());
        let plan = dynamic_scan_plan(
            [("file.parquet", 1, 10)],
            FileType::Parquet,
            present_deletion_vector(),
        );

        let error = tokio::task::spawn_blocking(move || {
            let mut data = executor
                .execute_op(Operation::QueryPlan(plan))?
                .into_data()?;
            data.next().transpose()
        })
        .await
        .unwrap()
        .err()
        .unwrap();

        assert!(error.to_string().contains("deletion vectors"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_scan_rejects_json_before_execution() {
        let executor = test_executor(Handle::current());
        let plan = dynamic_scan_plan(
            std::iter::empty(),
            FileType::Json,
            Scalar::null(DeletionVectorDescriptor::to_schema()),
        );

        let error =
            tokio::task::spawn_blocking(move || executor.execute_op(Operation::QueryPlan(plan)))
                .await
                .unwrap()
                .err()
                .unwrap();

        assert!(
            error.to_string().contains("only supports Parquet"),
            "{error}"
        );
    }

    #[test]
    fn stopped_runtime_returns_an_error() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let executor = test_executor(runtime.handle().clone());
        drop(runtime);

        let error = executor
            .execute_op(Operation::QueryPlan(values_plan()))
            .err()
            .unwrap();

        assert!(matches!(error, Error::JoinFailure(_)), "{error}");
    }

    #[rstest]
    #[case::range_responses(false)]
    #[case::whole_file_responses(true)]
    fn parquet_footer_uses_storage_ranges_and_preserves_field_ids(#[case] return_full_file: bool) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let data = parquet_bytes();
        let file = test_file(&data);
        let executor = DataFusionExecutor::new(
            SessionContext::new(),
            Arc::new(TestStorage {
                data,
                return_full_file,
            }),
            runtime.handle().clone(),
        );
        let footer = executor.read_footer(&file).unwrap();

        let field = footer.schema.field("id").unwrap();
        assert_eq!(
            field.get_config_value(&ColumnMetadataKey::ParquetFieldId),
            Some(&MetadataValue::Number(7))
        );
    }

    #[test]
    fn parquet_footer_rejects_files_smaller_than_the_tail() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let data = Bytes::from_static(b"PAR1");
        let file = test_file(&data);
        let executor = test_executor(runtime.handle().clone());

        let error = executor.read_footer(&file).unwrap_err();

        assert!(error.to_string().contains("smaller"), "{error}");
    }
}
