//! This module holds functionality for managing transactions.
mod transaction_id;
mod write_context;

use std::sync::Arc;

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{unwrap_and_parse_path_as_url, TryFromStringSlice};
use crate::{DeltaResult, ExternEngine, Snapshot, Url};
use crate::{ExclusiveEngineData, SharedExternEngine};
use crate::{KernelStringSlice, SharedSchema, SharedSnapshot};
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::transaction::create_table::{
    CreateTableTransaction, CreateTableTransactionBuilder,
};
use delta_kernel::transaction::{CommitResult, Transaction};
use delta_kernel_ffi_macros::handle_descriptor;

/// A handle for an existing-table transaction (`Transaction<ExistingTable>`).
///
/// Returned by [`transaction`] and [`transaction_with_committer`]. Supports all transaction
/// operations including existing-table-only operations like blind append and file removal.
#[handle_descriptor(target=Transaction, mutable=true, sized=true)]
pub struct ExclusiveTransaction;

/// A handle for a create-table transaction (`Transaction<CreateTable>`).
///
/// Returned by [`create_table_builder_build`]. Only supports operations valid during table
/// creation: adding files, setting data change, engine info, and committing. Operations like
/// file removal, blind append, and deletion vector updates are not available.
#[handle_descriptor(target=CreateTableTransaction, mutable=true, sized=true)]
pub struct ExclusiveCreateTransaction;

/// Handle for a mutable boxed committer that can be passed across FFI
#[handle_descriptor(target = dyn Committer, mutable = true, sized = false)]
pub struct MutableCommitter;

/// Start a transaction on the latest snapshot of the table.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn transaction(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveTransaction>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    transaction_impl(url, engine).into_extern_result(&engine)
}

fn transaction_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Handle<ExclusiveTransaction>> {
    let engine = extern_engine.engine();
    let snapshot = Snapshot::builder_for(url?).build(engine.as_ref())?;
    let committer = Box::new(FileSystemCommitter::new());
    let transaction = snapshot.transaction(committer, engine.as_ref());
    Ok(Box::new(transaction?).into())
}

/// Start a transaction with a custom committer
/// NOTE: This consumes the committer handle
///
/// # Safety
///
/// Caller is responsible for passing valid handles
#[no_mangle]
pub unsafe extern "C" fn transaction_with_committer(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    committer: Handle<MutableCommitter>,
) -> ExternResult<Handle<ExclusiveTransaction>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    let engine = unsafe { engine.as_ref() };
    let committer = unsafe { committer.into_inner() };
    transaction_with_committer_impl(snapshot, engine, committer).into_extern_result(&engine)
}

fn transaction_with_committer_impl(
    snapshot: Arc<Snapshot>,
    extern_engine: &dyn ExternEngine,
    committer: Box<dyn Committer>,
) -> DeltaResult<Handle<ExclusiveTransaction>> {
    let engine = extern_engine.engine();
    let transaction = snapshot.transaction(committer, engine.as_ref());
    Ok(Box::new(transaction?).into())
}

/// Convert a [`CommitResult`] into a committed version number, or an error if the commit was not
/// successful.
///
/// TODO: expose the full `CommitResult` enum through FFI for conflict resolution.
fn commit_result_to_version<S>(result: DeltaResult<CommitResult<S>>) -> DeltaResult<u64> {
    match result? {
        CommitResult::CommittedTransaction(committed) => Ok(committed.commit_version()),
        CommitResult::RetryableTransaction(_) => Err(delta_kernel::Error::unsupported(
            "commit failed: retryable transaction not supported in FFI (yet)",
        )),
        CommitResult::ConflictedTransaction(conflicted) => {
            Err(delta_kernel::Error::Generic(format!(
                "commit conflict at version {}",
                conflicted.conflict_version()
            )))
        }
    }
}

// ============================================================================
// Existing-table transaction FFI functions
// ============================================================================

/// Free an existing-table transaction handle without committing.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_transaction(txn: Handle<ExclusiveTransaction>) {
    txn.drop_handle();
}

/// Attaches engine info to an existing-table transaction.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. CONSUMES the transaction handle.
#[no_mangle]
pub unsafe extern "C" fn with_engine_info(
    txn: Handle<ExclusiveTransaction>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    with_engine_info_impl(*txn, engine_info).into_extern_result(&engine)
}

fn with_engine_info_impl(
    txn: Transaction,
    engine_info: KernelStringSlice,
) -> DeltaResult<Handle<ExclusiveTransaction>> {
    let info: &str = unsafe { TryFromStringSlice::try_from_slice(&engine_info) }?;
    Ok(Box::new(txn.with_engine_info(info)).into())
}

/// Add file metadata to the transaction for files that have been written. The metadata contains
/// information about files written during the transaction that will be added to the Delta log
/// during commit.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. Consumes write_metadata.
#[no_mangle]
pub unsafe extern "C" fn add_files(
    mut txn: Handle<ExclusiveTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
) {
    let txn = unsafe { txn.as_mut() };
    let write_metadata = unsafe { write_metadata.into_inner() };
    txn.add_files(write_metadata);
}

/// Mark the transaction as having data changes or not (these are recorded at the file level).
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn set_data_change(mut txn: Handle<ExclusiveTransaction>, data_change: bool) {
    let underlying_txn = unsafe { txn.as_mut() };
    underlying_txn.set_data_change(data_change);
}

/// Attempt to commit a transaction to the table. Returns version number if successful.
/// Returns error if the commit fails.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. And MUST NOT USE transaction after this
/// method is called.
#[no_mangle]
pub unsafe extern "C" fn commit(
    txn: Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<u64> {
    let txn = unsafe { txn.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let engine = extern_engine.engine();
    commit_result_to_version(txn.commit(engine.as_ref())).into_extern_result(&extern_engine)
}

// ============================================================================
// Create-table transaction FFI functions
// ============================================================================

/// Free a create-table transaction handle without committing.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn create_table_free_transaction(txn: Handle<ExclusiveCreateTransaction>) {
    txn.drop_handle();
}

/// Attaches engine info to a create-table transaction.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. CONSUMES the transaction handle.
#[no_mangle]
pub unsafe extern "C" fn create_table_with_engine_info(
    txn: Handle<ExclusiveCreateTransaction>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    create_table_with_engine_info_impl(*txn, engine_info).into_extern_result(&engine)
}

fn create_table_with_engine_info_impl(
    txn: CreateTableTransaction,
    engine_info: KernelStringSlice,
) -> DeltaResult<Handle<ExclusiveCreateTransaction>> {
    let info: &str = unsafe { TryFromStringSlice::try_from_slice(&engine_info) }?;
    Ok(Box::new(txn.with_engine_info(info)).into())
}

/// Add file metadata to a create-table transaction for files that have been written. The metadata
/// contains information about files written during the transaction that will be added to the
/// Delta log during commit.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. Consumes write_metadata.
#[no_mangle]
pub unsafe extern "C" fn create_table_add_files(
    mut txn: Handle<ExclusiveCreateTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
) {
    let txn = unsafe { txn.as_mut() };
    let write_metadata = unsafe { write_metadata.into_inner() };
    txn.add_files(write_metadata);
}

/// Mark the create-table transaction as having data changes or not (these are recorded at the
/// file level).
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn create_table_set_data_change(
    mut txn: Handle<ExclusiveCreateTransaction>,
    data_change: bool,
) {
    let underlying_txn = unsafe { txn.as_mut() };
    underlying_txn.set_data_change(data_change);
}

/// Attempt to commit a create-table transaction. Returns version number if successful.
/// Returns error if the commit fails.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. And MUST NOT USE transaction after this
/// method is called.
#[no_mangle]
pub unsafe extern "C" fn create_table_commit(
    txn: Handle<ExclusiveCreateTransaction>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<u64> {
    let txn = unsafe { txn.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let engine = extern_engine.engine();
    commit_result_to_version(txn.commit(engine.as_ref())).into_extern_result(&extern_engine)
}

// ============================================================================
// Create Table DDL
// ============================================================================

/// A handle representing an exclusive [`CreateTableTransactionBuilder`].
///
/// The caller must eventually either call [`create_table_builder_build`] (which consumes the
/// handle and returns a transaction) or [`free_create_table_builder`] (which drops it without
/// creating anything).
#[handle_descriptor(target=CreateTableTransactionBuilder, mutable=true, sized=true)]
pub struct ExclusiveCreateTableBuilder;

// TODO: Add `create_table_builder_with_data_layout` FFI function to support partitioned and
// clustered table creation. The kernel's
// `CreateTableTransactionBuilder::with_data_layout(DataLayout)` supports this but is not yet
// exposed through FFI.

/// Create a new [`CreateTableTransactionBuilder`] for creating a Delta table at the given path.
///
/// The returned builder can be configured with [`create_table_builder_with_table_property`]
/// before building with [`create_table_builder_build`]. The engine is only used for error
/// reporting at this stage.
///
/// # Safety
///
/// Caller is responsible for passing a valid `path`, `schema`, `engine_info`, and `engine`.
/// Does NOT consume the `schema` handle -- the caller is still responsible for freeing it.
#[no_mangle]
pub unsafe extern "C" fn get_create_table_builder(
    path: KernelStringSlice,
    schema: Handle<SharedSchema>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let path = unsafe { TryFromStringSlice::try_from_slice(&path) };
    let info = unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    let schema = unsafe { schema.clone_as_arc() };
    get_create_table_builder_impl(path, schema, info).into_extern_result(&engine)
}

fn get_create_table_builder_impl(
    path: DeltaResult<&str>,
    schema: Arc<delta_kernel::schema::Schema>,
    engine_info: DeltaResult<&str>,
) -> DeltaResult<Handle<ExclusiveCreateTableBuilder>> {
    let builder = delta_kernel::transaction::create_table::create_table(
        path?,
        schema,
        engine_info?.to_string(),
    );
    Ok(Box::new(builder).into())
}

/// Add a single table property to a [`CreateTableTransactionBuilder`].
///
/// This consumes the builder handle and returns a new one. The caller MUST replace their handle
/// pointer with the returned handle. On error, the old builder handle is consumed and gone --
/// do not free or reuse it. There is no new handle to free either.
///
/// # Safety
///
/// Caller is responsible for passing a valid builder handle, `key`, `value`, and `engine`.
/// CONSUMES the builder handle unconditionally (even on error).
#[no_mangle]
pub unsafe extern "C" fn create_table_builder_with_table_property(
    builder: Handle<ExclusiveCreateTableBuilder>,
    key: KernelStringSlice,
    value: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { *builder.into_inner() };
    let key = unsafe { TryFromStringSlice::try_from_slice(&key) };
    let value = unsafe { TryFromStringSlice::try_from_slice(&value) };
    create_table_builder_with_table_property_impl(builder, key, value).into_extern_result(&engine)
}

fn create_table_builder_with_table_property_impl(
    builder: CreateTableTransactionBuilder,
    key: DeltaResult<String>,
    value: DeltaResult<String>,
) -> DeltaResult<Handle<ExclusiveCreateTableBuilder>> {
    let builder = builder.with_table_properties([(key?, value?)]);
    Ok(Box::new(builder).into())
}

/// Build a create-table transaction using the default [`FileSystemCommitter`]. Returns a
/// create-table transaction handle that can be used with [`create_table_add_files`],
/// [`create_table_set_data_change`], [`create_table_with_engine_info`], and
/// [`create_table_commit`] to optionally stage initial data before committing.
///
/// # Safety
///
/// Caller is responsible for passing valid builder and engine handles.
/// CONSUMES the builder handle -- caller must not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn create_table_builder_build(
    builder: Handle<ExclusiveCreateTableBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    let committer = Box::new(FileSystemCommitter::new());
    create_table_builder_build_impl(builder, committer, extern_engine)
        .into_extern_result(&extern_engine)
}

/// Build a create-table transaction with a custom committer. Same as
/// [`create_table_builder_build`] but uses the provided committer instead of the default.
///
/// # Safety
///
/// Caller is responsible for passing valid handles.
/// CONSUMES both the builder and committer handles -- caller must not use them after this call.
#[no_mangle]
pub unsafe extern "C" fn create_table_builder_build_with_committer(
    builder: Handle<ExclusiveCreateTableBuilder>,
    committer: Handle<MutableCommitter>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let committer = unsafe { committer.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    create_table_builder_build_impl(builder, committer, extern_engine)
        .into_extern_result(&extern_engine)
}

fn create_table_builder_build_impl(
    builder: CreateTableTransactionBuilder,
    committer: Box<dyn Committer>,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Handle<ExclusiveCreateTransaction>> {
    let engine = extern_engine.engine();
    let create_txn = builder.build(engine.as_ref(), committer)?;
    Ok(Box::new(create_txn).into())
}

/// Free a [`CreateTableTransactionBuilder`] without building.
///
/// Use this on failure paths when the builder will not be built.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_create_table_builder(builder: Handle<ExclusiveCreateTableBuilder>) {
    builder.drop_handle();
}

// ============================================================================
// Remove Files DML
// ============================================================================

/// Remove files from a transaction using engine data and a selection vector.
///
/// The `data` handle is consumed. The selection vector indicates which rows in `data` represent
/// files to remove: nonzero means the row is selected for removal, `0` means it is skipped.
/// If `selection_vector` is null or `selection_vector_len` is 0, all rows are selected. When
/// `selection_vector_len` is 0, the `selection_vector` pointer is not accessed and may be null
/// or any arbitrary value.
///
/// The `data` schema must match the scan row schema returned by
/// [`scan_metadata_next`](crate::scan::scan_metadata_next).
///
/// Note: Unlike [`add_files`], this function takes an `engine` handle and returns
/// [`ExternResult`] because the selection vector validation can fail. Returns `true` on
/// success (the value itself is not meaningful).
///
/// # Safety
///
/// Caller is responsible for passing valid handles. The `selection_vector` pointer must be valid
/// for `selection_vector_len` bytes, or null. Consumes the `data` handle. Does NOT consume
/// the `txn` handle.
#[no_mangle]
pub unsafe extern "C" fn remove_files(
    mut txn: Handle<ExclusiveTransaction>,
    data: Handle<ExclusiveEngineData>,
    selection_vector: *const u8,
    selection_vector_len: usize,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let data = unsafe { data.into_inner() };
    let txn = unsafe { txn.as_mut() };
    // empty sv = all rows selected (per FilteredEngineData contract)
    let sv = if selection_vector.is_null() || selection_vector_len == 0 {
        vec![]
    } else {
        let raw = unsafe { std::slice::from_raw_parts(selection_vector, selection_vector_len) };
        raw.iter().map(|&b| b != 0).collect()
    };
    let result: DeltaResult<bool> = (|| {
        let filtered = FilteredEngineData::try_new(data, sv)?;
        txn.remove_files(filtered);
        Ok(true)
    })();
    result.into_extern_result(&engine)
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::table_features::TableFeature;

    use delta_kernel::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray};
    use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
    use delta_kernel::arrow::ffi::to_ffi;
    use delta_kernel::arrow::json::reader::ReaderBuilder;
    use delta_kernel::arrow::record_batch::RecordBatch;

    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStore;
    use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
    use delta_kernel::parquet::file::properties::WriterProperties;

    use delta_kernel_ffi::engine_data::get_engine_data;
    use delta_kernel_ffi::engine_data::ArrowFFIData;

    use delta_kernel_ffi::ffi_test_utils::{allocate_str, ok_or_panic, recover_string};
    use delta_kernel_ffi::tests::get_default_engine;

    use crate::{free_engine, free_schema, free_snapshot, kernel_string_slice};
    use crate::{logical_schema, snapshot, version};
    use write_context::{free_write_context, get_write_context, get_write_path, get_write_schema};

    use test_utils::{set_json_value, setup_test_tables, test_read};

    use itertools::Itertools;
    use serde_json::json;
    use serde_json::Deserializer;

    use std::sync::Arc;

    const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

    use super::*;

    use tempfile::tempdir;

    fn check_txn_id_exists(commit_info: &serde_json::Value) {
        commit_info["txnId"]
            .as_str()
            .expect("txnId should be present in commitInfo");
    }

    fn create_arrow_ffi_from_json(
        schema: ArrowSchema,
        json_string: &str,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        let cursor = std::io::Cursor::new(json_string.as_bytes());
        let mut reader = ReaderBuilder::new(schema.into()).build(cursor).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let batch_struct_array: StructArray = batch.into();
        let array_data = batch_struct_array.into_data();
        let (out_array, out_schema) = to_ffi(&array_data)?;

        Ok(ArrowFFIData {
            array: out_array,
            schema: out_schema,
        })
    }

    fn create_file_metadata(
        path: &str,
        file_size_bytes: u64,
        num_rows: i64,
        metadata_schema: ArrowSchema,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        let current_time: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let file_metadata = format!(
            r#"{{"path":"{path}", "partitionValues": {{}}, "size": {file_size_bytes}, "modificationTime": {current_time}, "stats": {{"numRecords": {num_rows}}}}}"#,
        );

        create_arrow_ffi_from_json(metadata_schema, file_metadata.as_str())
    }

    fn write_parquet_file(
        delta_path: &str,
        file_path: &str,
        batch: &RecordBatch,
        metadata_schema: ArrowSchema,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder().build();

        let full_path = format!("{delta_path}{file_path}");
        let file = std::fs::File::create(&full_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(batch).expect("Writing batch");

        // writer must be closed to write footer
        let res = writer.close().unwrap();

        let file_size_bytes = std::fs::metadata(&full_path)?.len();

        #[cfg(any(not(feature = "arrow-56"), feature = "arrow-57"))]
        let num_rows = res.file_metadata().num_rows();
        #[cfg(all(feature = "arrow-56", not(feature = "arrow-57")))]
        let num_rows = res.num_rows;

        create_file_metadata(file_path, file_size_bytes, num_rows, metadata_schema)
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // FIXME: re-enable miri (can't call foreign function `linkat` on OS `linux`)
    async fn test_basic_append() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::try_new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ])?);

        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // TODO: test with partitions
        let partition_columns = vec![];

        for (table_url, _engine, store, _table_name) in setup_test_tables(
            schema,
            &partition_columns,
            Some(&tmp_dir_local_url),
            "test_table",
        )
        .await?
        {
            let table_path = table_url.to_file_path().unwrap();
            let table_path_str = table_path.to_str().unwrap();
            let engine = get_default_engine(table_path_str);

            // Start the transaction
            let txn = ok_or_panic(unsafe {
                transaction(kernel_string_slice!(table_path_str), engine.shallow_copy())
            });
            unsafe { set_data_change(txn.shallow_copy(), false) };

            // Add engine info
            let engine_info = "default_engine";
            let engine_info_kernel_string = kernel_string_slice!(engine_info);
            let txn_with_engine_info = unsafe {
                ok_or_panic(with_engine_info(
                    txn,
                    engine_info_kernel_string,
                    engine.shallow_copy(),
                ))
            };

            let write_context = unsafe { get_write_context(txn_with_engine_info.shallow_copy()) };

            // Ensure we get the correct schema
            let write_schema = unsafe { get_write_schema(write_context.shallow_copy()) };
            let write_schema_ref = unsafe { write_schema.as_ref() };
            assert_eq!(write_schema_ref.num_fields(), 2);
            assert_eq!(write_schema_ref.field_at_index(0).unwrap().name, "number");
            assert_eq!(
                write_schema_ref.field_at_index(0).unwrap().data_type,
                DataType::INTEGER
            );
            assert_eq!(write_schema_ref.field_at_index(1).unwrap().name, "string");
            assert_eq!(
                write_schema_ref.field_at_index(1).unwrap().data_type,
                DataType::STRING
            );

            // Ensure the ffi returns the correct table path
            let write_path = unsafe { get_write_path(write_context.shallow_copy(), allocate_str) };
            assert!(write_path.is_some());
            let unwrapped_url = recover_string(write_path.unwrap());
            let parsed_unwrapped_url = Url::parse(&unwrapped_url)?;
            assert_eq!(
                std::fs::canonicalize(parsed_unwrapped_url.to_file_path().unwrap())?,
                std::fs::canonicalize(table_url.to_file_path().unwrap())?
            );

            // Create some test data
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "number",
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                ),
                (
                    "string",
                    Arc::new(StringArray::from(vec![
                        "value-1", "value-2", "value-3", "value-4", "value-5",
                    ])) as ArrayRef,
                ),
            ])
            .unwrap();
            let parquet_schema = unsafe {
                txn_with_engine_info
                    .shallow_copy()
                    .as_ref()
                    .add_files_schema()
            };
            let file_info = write_parquet_file(
                table_path_str,
                "my_file.parquet",
                &batch,
                parquet_schema.as_ref().try_into_arrow()?,
            )?;

            let file_info_engine_data = ok_or_panic(unsafe {
                get_engine_data(
                    file_info.array,
                    &file_info.schema,
                    crate::ffi_test_utils::allocate_err,
                )
            });

            unsafe { add_files(txn_with_engine_info.shallow_copy(), file_info_engine_data) };

            ok_or_panic(unsafe { commit(txn_with_engine_info, engine.shallow_copy()) });

            // Confirm that our commit is what we expect
            let commit1_url = table_url
                .join("_delta_log/00000000000000000001.json")
                .unwrap();
            let commit1 = store
                .get(&Path::from_url_path(commit1_url.path()).unwrap())
                .await?;
            let mut parsed_commits: Vec<_> = Deserializer::from_slice(&commit1.bytes().await?)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            check_txn_id_exists(&parsed_commits[0]["commitInfo"]);

            // set timestamps to 0, paths and txn_id to known string values for comparison
            // (otherwise timestamps are non-deterministic, paths and txn_id are random UUIDs)
            set_json_value(&mut parsed_commits[0], "commitInfo.timestamp", json!(0))?;
            set_json_value(&mut parsed_commits[0], "commitInfo.txnId", json!(ZERO_UUID))?;
            set_json_value(&mut parsed_commits[1], "add.modificationTime", json!(0))?;
            set_json_value(&mut parsed_commits[1], "add.size", json!(0))?;

            let expected_commit = vec![
                json!({
                    "commitInfo": {
                        "timestamp": 0,
                        "engineInfo": "default_engine",
                        "operation": "UNKNOWN",
                        "kernelVersion": format!("v{}", env!("CARGO_PKG_VERSION")),
                        "operationParameters": {},
                        "txnId": ZERO_UUID
                    }
                }),
                json!({
                    "add": {
                        "path": "my_file.parquet",
                        "partitionValues": {},
                        "size": 0,
                        "modificationTime": 0,
                        "dataChange": false,
                        "stats": "{\"numRecords\":5}"
                    }
                }),
            ];

            assert_eq!(parsed_commits, expected_commit);

            // Confirm that the data matches what we appended
            let test_batch = ArrowEngineData::from(batch);
            test_read(&test_batch, &table_url, unsafe { engine.as_ref().engine() })?;

            unsafe { free_schema(write_schema) };
            unsafe { free_write_context(write_context) };
            unsafe { free_engine(engine) };
        }

        Ok(())
    }

    #[cfg(feature = "delta-kernel-unity-catalog")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(miri, ignore)]
    async fn test_transaction_with_uc_committer() -> Result<(), Box<dyn std::error::Error>> {
        use crate::delta_kernel_unity_catalog::{
            free_uc_commit_client, get_uc_commit_client, get_uc_committer,
            tests::{cast_test_context, get_test_context, recover_test_context},
            CommitRequest,
        };
        use crate::{Handle, NullableCvoid, OptionalValue};

        #[no_mangle]
        extern "C" fn test_uc_commit(
            context: NullableCvoid,
            request: CommitRequest,
        ) -> OptionalValue<Handle<crate::ExclusiveRustString>> {
            let context = cast_test_context(context).unwrap();
            context.commit_called = true;

            let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
            let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

            context.last_commit_request = Some((table_id.clone(), table_uri.clone()));

            // Capture the staged commit file name if present
            if let OptionalValue::Some(commit_info) = request.commit_info {
                let file_name = unsafe { String::try_from_slice(&commit_info.file_name).unwrap() };
                context.last_staged_filename = Some(file_name);
            }

            OptionalValue::None
        }

        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ]));

        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // Create a catalog-managed table so UCCommitter (a catalog committer) is allowed.
        let (store, _test_engine, table_location) =
            test_utils::engine_store_setup("test_uc_table", Some(&tmp_dir_local_url));
        let table_url = test_utils::create_table(
            store.clone(),
            table_location,
            schema,
            &[],
            true, // use v3/v7 protocol
            vec!["catalogManaged", "vacuumProtocolCheck"],
            vec!["inCommitTimestamp", "catalogManaged", "vacuumProtocolCheck"],
        )
        .await?;

        {
            let table_path = table_url.to_file_path().unwrap();
            let table_path_str = table_path.to_str().unwrap();
            let engine = get_default_engine(table_path_str);

            let snapshot = unsafe {
                ok_or_panic(crate::snapshot(
                    kernel_string_slice!(table_path_str),
                    engine.shallow_copy(),
                ))
            };

            let context = get_test_context(false);

            let uc_client = unsafe { get_uc_commit_client(context, test_uc_commit) };
            let table_id = "test_id";
            let uc_committer = unsafe {
                ok_or_panic(get_uc_committer(
                    uc_client.shallow_copy(),
                    kernel_string_slice!(table_id),
                    crate::ffi_test_utils::allocate_err,
                ))
            };

            let txn = ok_or_panic(unsafe {
                transaction_with_committer(snapshot, engine.shallow_copy(), uc_committer)
            });
            unsafe { set_data_change(txn.shallow_copy(), false) };

            let engine_info = "uc_test_engine";
            let engine_info_kernel_string = kernel_string_slice!(engine_info);
            let txn_with_engine_info = unsafe {
                ok_or_panic(with_engine_info(
                    txn,
                    engine_info_kernel_string,
                    engine.shallow_copy(),
                ))
            };

            let write_context = unsafe { get_write_context(txn_with_engine_info.shallow_copy()) };

            let batch = RecordBatch::try_from_iter(vec![
                (
                    "number",
                    Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
                ),
                (
                    "string",
                    Arc::new(StringArray::from(vec!["uc-1", "uc-2", "uc-3"])) as ArrayRef,
                ),
            ])
            .unwrap();

            let parquet_schema = unsafe {
                txn_with_engine_info
                    .shallow_copy()
                    .as_ref()
                    .add_files_schema()
            };
            let file_info = write_parquet_file(
                table_path_str,
                "uc_test_file.parquet",
                &batch,
                parquet_schema.as_ref().try_into_arrow()?,
            )?;

            let file_info_engine_data = ok_or_panic(unsafe {
                get_engine_data(
                    file_info.array,
                    &file_info.schema,
                    crate::ffi_test_utils::allocate_err,
                )
            });

            unsafe { add_files(txn_with_engine_info.shallow_copy(), file_info_engine_data) };

            let commit_result = unsafe { commit(txn_with_engine_info, engine.shallow_copy()) };

            // UC committer returns success from our mock callback
            assert!(commit_result.is_ok(), "Commit should succeed");

            let context = recover_test_context(context).unwrap();

            assert!(
                context.commit_called,
                "Commit callback should be called after commit"
            );

            {
                // scope so we don't hold mutex across the await lower down
                let (last_table_id, _) = context.last_commit_request.unwrap();
                assert_eq!(
                    last_table_id, "test_id",
                    "Table ID should match the one passed to UCCommitter"
                );
            }

            let staged_file_name = {
                // scope so we don't hold mutex across await
                assert!(
                    context.last_staged_filename.is_some(),
                    "Staged commit file name should be captured"
                );

                context.last_staged_filename.clone().unwrap()
            };

            // Read the staged commit
            let staged_commit_url = table_url
                .join(&format!("_delta_log/_staged_commits/{staged_file_name}"))
                .unwrap();
            let staged_commit = store
                .get(&Path::from_url_path(staged_commit_url.path()).unwrap())
                .await?;
            let staged_content = staged_commit.bytes().await?;
            let staged_actions: Vec<_> = Deserializer::from_slice(&staged_content)
                .into_iter::<serde_json::Value>()
                .try_collect()?;

            assert!(
                !staged_actions.is_empty(),
                "Staged commit should have actions"
            );

            // Should have commitInfo and add action
            let has_commit_info = staged_actions.iter().any(|a| a.get("commitInfo").is_some());
            let has_add = staged_actions.iter().any(|a| a.get("add").is_some());

            assert!(has_commit_info, "Staged commit should contain commitInfo");
            assert!(has_add, "Staged commit should contain add action");

            let add_action = staged_actions
                .iter()
                .find(|a| a.get("add").is_some())
                .unwrap();
            assert_eq!(
                add_action["add"]["path"].as_str().unwrap(),
                "uc_test_file.parquet",
                "Add action should reference the correct file"
            );

            let commit_info = staged_actions
                .iter()
                .find(|a| a.get("commitInfo").is_some())
                .unwrap();
            assert_eq!(
                commit_info["commitInfo"]["engineInfo"].as_str().unwrap(),
                "uc_test_engine",
                "CommitInfo should contain the engine info"
            );

            unsafe { free_write_context(write_context) };
            unsafe { free_engine(engine) };
            unsafe { free_uc_commit_client(uc_client) };
        }

        Ok(())
    }

    /// Create a [`CreateTableTransactionBuilder`] handle via the FFI, using the given schema
    /// fields. Returns `(table_path, engine_handle, builder_handle)`. The caller is responsible
    /// for freeing/consuming the engine and builder handles.
    fn create_table_builder(
        tmp_dir: &tempfile::TempDir,
        fields: Vec<StructField>,
    ) -> (
        String,
        Handle<SharedExternEngine>,
        Handle<ExclusiveCreateTableBuilder>,
    ) {
        let table_path = tmp_dir.path().to_str().unwrap().to_string();
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let engine = get_default_engine(&table_path);
        let schema_handle: Handle<SharedSchema> = schema.into();
        let engine_info = "test-engine/1.0";
        let builder = ok_or_panic(unsafe {
            get_create_table_builder(
                kernel_string_slice!(table_path),
                schema_handle.shallow_copy(),
                kernel_string_slice!(engine_info),
                engine.shallow_copy(),
            )
        });
        // get_create_table_builder does NOT consume the schema handle -- free it
        unsafe { free_schema(schema_handle) };
        (table_path, engine, builder)
    }

    /// Build and commit a create-table builder with default (null) committer, asserting that
    /// the committed version is 0.
    fn build_and_commit(
        builder: Handle<ExclusiveCreateTableBuilder>,
        engine: &Handle<SharedExternEngine>,
    ) -> u64 {
        let txn =
            ok_or_panic(unsafe { create_table_builder_build(builder, engine.shallow_copy()) });
        let version = ok_or_panic(unsafe { create_table_commit(txn, engine.shallow_copy()) });
        assert_eq!(version, 0);
        version
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_create_table_basic() -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        let (table_path, engine, builder) = create_table_builder(
            &tmp_dir,
            vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("name", DataType::STRING),
            ],
        );
        let table_path_str = table_path.as_str();

        build_and_commit(builder, &engine);

        // Verify by opening a snapshot of the created table
        let snap = ok_or_panic(unsafe {
            snapshot(kernel_string_slice!(table_path_str), engine.shallow_copy())
        });
        let snap_version = unsafe { version(snap.shallow_copy()) };
        assert_eq!(snap_version, 0);

        // Verify schema
        let snap_schema = unsafe { logical_schema(snap.shallow_copy()) };
        let snap_schema_ref = unsafe { snap_schema.as_ref() };
        assert_eq!(snap_schema_ref.num_fields(), 2);
        assert_eq!(snap_schema_ref.field_at_index(0).unwrap().name, "id");
        assert_eq!(snap_schema_ref.field_at_index(1).unwrap().name, "name");

        unsafe { free_schema(snap_schema) };
        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_create_table_with_properties() {
        let tmp_dir = tempdir().unwrap();
        let (table_path, engine, builder) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );

        // Set properties
        let prop_key1 = "delta.appendOnly";
        let prop_val1 = "true";
        let builder = ok_or_panic(unsafe {
            create_table_builder_with_table_property(
                builder,
                kernel_string_slice!(prop_key1),
                kernel_string_slice!(prop_val1),
                engine.shallow_copy(),
            )
        });
        let prop_key2 = "custom.key";
        let prop_val2 = "custom_value";
        let builder = ok_or_panic(unsafe {
            create_table_builder_with_table_property(
                builder,
                kernel_string_slice!(prop_key2),
                kernel_string_slice!(prop_val2),
                engine.shallow_copy(),
            )
        });

        build_and_commit(builder, &engine);

        // Verify properties via snapshot
        let table_path_str = table_path.as_str();
        let snap = ok_or_panic(unsafe {
            snapshot(kernel_string_slice!(table_path_str), engine.shallow_copy())
        });
        let snap_ref = unsafe { snap.as_ref() };

        // Verify parsed table properties
        let props = snap_ref.table_properties();
        assert_eq!(props.append_only, Some(true));
        assert_eq!(
            props
                .unknown_properties
                .get("custom.key")
                .map(|s| s.as_str()),
            Some("custom_value")
        );

        // Verify feature is enabled in protocol (property set + protocol supports it)
        let config = snap_ref.table_configuration();
        assert!(config.is_feature_enabled(&TableFeature::AppendOnly));

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_create_table_already_exists() -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;

        // Create the table first time -- should succeed
        let (_table_path, engine, builder) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );
        build_and_commit(builder, &engine);

        // Try to create the same table again -- build should error (table already exists)
        let (_, engine2, builder2) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );
        let result = unsafe { create_table_builder_build(builder2, engine2.shallow_copy()) };
        match result {
            ExternResult::Err(e) => {
                // Clean up the error to prevent leaks
                let error = unsafe { crate::ffi_test_utils::recover_error(e) };
                assert!(
                    error.message.contains("already exists"),
                    "Expected 'already exists' error, got: {}",
                    error.message
                );
            }
            ExternResult::Ok(txn) => {
                unsafe { create_table_free_transaction(txn) };
                panic!("Expected error for table that already exists");
            }
        }

        unsafe { free_engine(engine2) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[test]
    fn test_free_create_table_builder() {
        let schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)]).unwrap(),
        );
        let builder =
            delta_kernel::transaction::create_table::create_table("memory:///test", schema, "test");
        let handle: Handle<ExclusiveCreateTableBuilder> = Box::new(builder).into();
        // Should not panic or leak
        unsafe { free_create_table_builder(handle) };
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_create_table_build_with_empty_schema_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        // An empty schema is always invalid for table creation
        let (_table_path, engine, builder) = create_table_builder(&tmp_dir, vec![]);

        let result = unsafe { create_table_builder_build(builder, engine.shallow_copy()) };
        match result {
            ExternResult::Err(e) => {
                let error = unsafe { crate::ffi_test_utils::recover_error(e) };
                assert!(
                    error.message.contains("schema")
                        || error.message.contains("field")
                        || error.message.contains("empty"),
                    "Expected schema-related error, got: {}",
                    error.message
                );
            }
            ExternResult::Ok(txn) => {
                unsafe { create_table_free_transaction(txn) };
                panic!("Expected error for empty schema");
            }
        }

        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_create_table_with_custom_committer() -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        let (table_path, engine, builder) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );
        let table_path_str = table_path.as_str();

        // Create a FileSystemCommitter handle and pass it to build_with_committer
        let committer: Box<dyn delta_kernel::committer::Committer> =
            Box::new(FileSystemCommitter::new());
        let committer_handle: Handle<MutableCommitter> = committer.into();

        let txn = ok_or_panic(unsafe {
            create_table_builder_build_with_committer(
                builder,
                committer_handle,
                engine.shallow_copy(),
            )
        });
        let committed_version =
            ok_or_panic(unsafe { create_table_commit(txn, engine.shallow_copy()) });
        assert_eq!(committed_version, 0);

        // Verify the table was created
        let snap = ok_or_panic(unsafe {
            snapshot(kernel_string_slice!(table_path_str), engine.shallow_copy())
        });
        assert_eq!(unsafe { version(snap.shallow_copy()) }, 0);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    /// Helper: create a table, write one parquet file, and return (table_path, engine_handle).
    /// The caller is responsible for freeing the engine handle.
    fn create_table_with_one_file(
        tmp_dir: &tempfile::TempDir,
    ) -> Result<(String, Handle<SharedExternEngine>), Box<dyn std::error::Error>> {
        let table_path = tmp_dir.path().to_str().unwrap();
        let schema = Arc::new(StructType::try_new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("value", DataType::STRING),
        ])?);

        let engine = get_default_engine(table_path);
        let schema_handle: Handle<SharedSchema> = schema.into();

        // Create the table
        let engine_info = "test-engine/1.0";
        let builder = ok_or_panic(unsafe {
            get_create_table_builder(
                kernel_string_slice!(table_path),
                schema_handle.shallow_copy(),
                kernel_string_slice!(engine_info),
                engine.shallow_copy(),
            )
        });
        // get_create_table_builder does NOT consume the schema handle -- free it
        unsafe { free_schema(schema_handle) };
        build_and_commit(builder, &engine);

        // Write a parquet file and commit it
        let batch = RecordBatch::try_from_iter(vec![
            (
                "number",
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ])?;

        let txn = ok_or_panic(unsafe {
            transaction(kernel_string_slice!(table_path), engine.shallow_copy())
        });
        let engine_info = "test-engine/1.0";
        let txn = ok_or_panic(unsafe {
            with_engine_info(
                txn,
                kernel_string_slice!(engine_info),
                engine.shallow_copy(),
            )
        });

        let parquet_schema = unsafe { txn.shallow_copy().as_ref().add_files_schema() };
        let file_info = write_parquet_file(
            table_path,
            "file1.parquet",
            &batch,
            parquet_schema.as_ref().try_into_arrow()?,
        )?;
        let file_info_engine_data = ok_or_panic(unsafe {
            get_engine_data(
                file_info.array,
                &file_info.schema,
                crate::ffi_test_utils::allocate_err,
            )
        });
        unsafe { add_files(txn.shallow_copy(), file_info_engine_data) };
        ok_or_panic(unsafe { commit(txn, engine.shallow_copy()) });

        Ok((table_path.to_string(), engine))
    }

    fn assert_no_active_files(
        kernel_engine: &Arc<dyn delta_kernel::Engine>,
        table_path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot =
            delta_kernel::snapshot::Snapshot::builder_for(delta_kernel::try_parse_uri(table_path)?)
                .build(kernel_engine.as_ref())?;
        let scan = snapshot.scan_builder().build()?;
        let scan_meta: Vec<_> = scan
            .scan_metadata(kernel_engine.as_ref())?
            .collect::<Result<_, _>>()?;
        let total_selected: usize = scan_meta
            .iter()
            .map(|m| {
                let sv = m.scan_files.selection_vector();
                let data_len = m.scan_files.data().len();
                if sv.is_empty() {
                    data_len
                } else {
                    sv.iter().filter(|&&b| b).count()
                }
            })
            .sum();
        assert_eq!(total_selected, 0, "Expected 0 files after removal");
        Ok(())
    }

    /// Helper: create a table with one file, build a snapshot, extract scan metadata, and
    /// return the components needed for remove_files tests. Caller must free the engine handle
    /// (and txn if not committed).
    #[allow(clippy::type_complexity)]
    fn setup_remove_files_test(
        tmp_dir: &tempfile::TempDir,
    ) -> Result<
        (
            Box<dyn delta_kernel::EngineData>,
            Vec<bool>,
            Handle<ExclusiveTransaction>,
            Handle<SharedExternEngine>,
            Arc<dyn delta_kernel::Engine>,
            String,
        ),
        Box<dyn std::error::Error>,
    > {
        let (table_path, engine) = create_table_with_one_file(tmp_dir)?;
        let table_path_str = table_path.as_str();

        let kernel_engine = unsafe { engine.as_ref() }.engine();
        let snapshot = delta_kernel::snapshot::Snapshot::builder_for(delta_kernel::try_parse_uri(
            table_path_str,
        )?)
        .build(kernel_engine.as_ref())?;

        let scan = snapshot.scan_builder().build()?;
        let scan_meta_items: Vec<_> = scan
            .scan_metadata(kernel_engine.as_ref())?
            .collect::<Result<_, _>>()?;
        assert_eq!(scan_meta_items.len(), 1);

        let scan_meta = scan_meta_items.into_iter().next().unwrap();
        let (data, sv) = scan_meta.scan_files.into_parts();

        let txn = ok_or_panic(unsafe {
            transaction(kernel_string_slice!(table_path_str), engine.shallow_copy())
        });
        let engine_info = "test-engine/1.0";
        let txn = ok_or_panic(unsafe {
            with_engine_info(
                txn,
                kernel_string_slice!(engine_info),
                engine.shallow_copy(),
            )
        });

        Ok((data, sv, txn, engine, kernel_engine, table_path))
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_remove_files_with_null_sv_commits_and_removes_all(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        let (data, sv, txn, engine, kernel_engine, table_path) = setup_remove_files_test(&tmp_dir)?;
        let data_handle: Handle<ExclusiveEngineData> = data.into();

        // Pass the original SV as u8 values
        let sv_u8: Vec<u8> = sv.iter().map(|&b| b as u8).collect();
        let sv_ptr = if sv_u8.is_empty() {
            std::ptr::null()
        } else {
            sv_u8.as_ptr()
        };
        ok_or_panic(unsafe {
            remove_files(
                txn.shallow_copy(),
                data_handle,
                sv_ptr,
                sv_u8.len(),
                engine.shallow_copy(),
            )
        });

        ok_or_panic(unsafe { commit(txn, engine.shallow_copy()) });
        assert_no_active_files(&kernel_engine, table_path.as_str())?;

        unsafe { free_engine(engine) };
        Ok(())
    }

    #[test]
    fn test_empty_sv_creates_filtered_data_selecting_all_rows() {
        // Verifies the FilteredEngineData contract that an empty selection vector means
        // "all rows selected". The remove_files FFI wrapper normalizes null/zero-length
        // pointers to an empty vec before constructing FilteredEngineData.
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let data: Box<dyn delta_kernel::EngineData> = Box::new(ArrowEngineData::from(batch));

        let filtered = FilteredEngineData::try_new(data, vec![]).unwrap();
        assert_eq!(filtered.selection_vector().len(), 0);
        assert_eq!(filtered.data().len(), 3);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_remove_files_with_non_empty_sv_exercises_from_raw_parts(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Exercises the from_raw_parts code path in the remove_files FFI wrapper by passing
        // a non-null selection vector pointer with non-zero length. The null-SV test always
        // passes a null pointer because scan_metadata for a single-file table returns an
        // empty selection vector.
        let tmp_dir = tempdir()?;
        let (data, _sv, txn, engine, _kernel_engine, _table_path) =
            setup_remove_files_test(&tmp_dir)?;
        let num_rows = data.len();
        assert!(num_rows > 0);
        let data_handle: Handle<ExclusiveEngineData> = data.into();

        // Construct a non-empty all-true SV selecting every row. Uses u8 values to match
        // the FFI signature (nonzero = selected).
        let sv: Vec<u8> = vec![1; num_rows];
        ok_or_panic(unsafe {
            remove_files(
                txn.shallow_copy(),
                data_handle,
                sv.as_ptr(),
                sv.len(),
                engine.shallow_copy(),
            )
        });

        // Don't commit -- the from_raw_parts path has been exercised. Committing with
        // a non-empty SV triggers a different code path in generate_remove_actions that
        // requires additional scan row fields not present in this test setup.
        unsafe { free_transaction(txn) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
