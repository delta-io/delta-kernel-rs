use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::KernelDeletionVector;
use delta_kernel::arrow::array::{ArrayRef, Int64Array};
use delta_kernel::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::PredicateRef;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectStore, ObjectStoreExt as _};
use delta_kernel::schema::{schema_ref, MetadataColumnSpec, SchemaRef};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{
    DeltaResult, DeltaResultIteratorStatic, Engine, EngineData, EvaluationHandler,
    FileDataReadResultIterator, FileMeta, JsonHandler, ParquetFooter, ParquetHandler,
    StorageHandler,
};
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    begin_transaction, create_add_files_metadata, generate_batch, read_scan, record_batch_to_bytes,
    IntoArray,
};
use url::Url;

use crate::common::write_utils::{
    create_dv_update_transaction, get_scan_files, write_deletion_vector_to_store,
};

#[rstest::rstest]
#[case::no_dv(vec![], 2)]
#[case::one_deleted_row(vec![0], 1)]
#[case::all_rows_deleted(vec![0, 1], 0)]
#[tokio::test]
async fn execute_preserves_empty_projection_row_count_with_custom_reader(
    #[case] deleted: Vec<u64>,
    #[case] expected_rows: usize,
    #[values(1, 2)] batch_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
    let snapshot = create_table(
        "memory:///",
        schema_ref! { nullable "id": LONG },
        "empty projection test",
    )
    .with_table_properties([("delta.enableDeletionVectors", "true")])
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
    .commit(engine.as_ref())?
    .unwrap_post_commit_snapshot();
    let batch = generate_batch(vec![("id", vec![0i64, 1].into_arrow_array())])?;
    let bytes = record_batch_to_bytes(&batch);
    let size = bytes.len() as i64;
    store.put(&Path::from("data.parquet"), bytes.into()).await?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    txn.add_files(create_add_files_metadata(
        txn.add_files_schema(),
        vec![("data.parquet", size, 0, Some(2))],
    )?);
    let mut snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    if !deleted.is_empty() {
        let mut txn = create_dv_update_transaction(snapshot.table_root(), engine.as_ref())?;
        let context = txn.write_state()?.write_context_builder().build()?;
        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(deleted);
        let descriptor = write_deletion_vector_to_store(&store, &context, dv, "").await?;
        txn.update_deletion_vectors(
            HashMap::from([("data.parquet".to_string(), descriptor)]),
            get_scan_files(snapshot, engine.as_ref())?
                .into_iter()
                .map(Ok),
        )?;
        snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    }

    let scan = snapshot
        .scan_builder()
        .with_schema(schema_ref! {})
        .build()?;
    let engine = Arc::new(EmptyProjectionEngine { engine, batch_size });
    let batches = read_scan(&scan, engine)?;
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected_rows
    );
    Ok(())
}

// Keep the default evaluator but supply a reader that supports zero-column batches.
struct EmptyProjectionEngine {
    engine: Arc<dyn Engine>,
    batch_size: usize,
}

impl Engine for EmptyProjectionEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.engine.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.engine.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.engine.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        Arc::new(EmptyProjectionParquetHandler {
            batch_size: self.batch_size,
        })
    }
}

struct EmptyProjectionParquetHandler {
    batch_size: usize,
}

impl ParquetHandler for EmptyProjectionParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].location.path(), "/data.parquet");
        assert!(predicate.is_none());
        let columns: Vec<ArrayRef> = schema
            .fields()
            .map(|field| {
                assert_eq!(
                    field.get_metadata_column_spec(),
                    Some(MetadataColumnSpec::RowIndex)
                );
                Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.as_ref().try_into_arrow()?),
            columns,
            &RecordBatchOptions::default().with_row_count(Some(2)),
        )?;
        let batch_size = self.batch_size;
        Ok(Box::new((0..2).step_by(batch_size).map(move |offset| {
            let batch = batch.slice(offset, batch_size.min(2 - offset));
            Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
        })))
    }

    fn read_parquet_footer(&self, _file: &FileMeta) -> DeltaResult<ParquetFooter> {
        unimplemented!()
    }

    fn write_parquet_file(
        &self,
        _location: Url,
        _data: DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        unimplemented!()
    }
}
