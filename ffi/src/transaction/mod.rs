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
use delta_kernel::transaction::create_table::CreateTableTransactionBuilder;
use delta_kernel::transaction::{CommitResult, Transaction};
use delta_kernel_ffi_macros::handle_descriptor;

/// A handle representing an exclusive transaction on a Delta table. (Similar to a Box<_>)
///
/// This struct provides a safe wrapper around the underlying `Transaction` type,
/// ensuring exclusive access to transaction operations. The transaction can be used
/// to stage changes and commit them atomically to the Delta table.
#[handle_descriptor(target=Transaction, mutable=true, sized=true)]
pub struct ExclusiveTransaction;

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

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_transaction(txn: Handle<ExclusiveTransaction>) {
    txn.drop_handle();
}

/// Attaches commit information to a transaction. The commit info contains metadata about the
/// transaction that will be written to the log during commit.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. CONSUMES TRANSACTION and commit info
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
    let info_string: DeltaResult<&str> =
        unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    Ok(Box::new(txn.with_engine_info(info_string?)).into())
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

///
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

/// Convert a [`CommitResult`] into a committed version number, or an error if the commit was not
/// successful. Used by both [`commit`] and [`create_table_commit`].
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
// Create Table DDL
// ============================================================================

/// A handle representing an exclusive [`CreateTableTransactionBuilder`].
///
/// The caller must eventually either call [`create_table_build`] (which consumes the handle and
/// returns a transaction) or [`free_create_table_builder`] (which drops it without creating
/// anything).
#[handle_descriptor(target=CreateTableTransactionBuilder, mutable=true, sized=true)]
pub struct ExclusiveCreateTableBuilder;

// TODO: Add `create_table_set_data_layout` FFI function to support partitioned and clustered
// table creation. The kernel's `CreateTableTransactionBuilder::with_data_layout(DataLayout)`
// supports this but is not yet exposed through FFI.

/// Create a new [`CreateTableTransactionBuilder`] for creating a Delta table at the given path.
///
/// The returned builder can be configured with [`create_table_set_property`] before building
/// with [`create_table_build`]. The engine is only used for error reporting at this stage.
///
/// # Safety
///
/// Caller is responsible for passing a valid `path`, `schema`, `engine_info`, and `engine`.
/// Does NOT consume the `schema` handle -- the caller is still responsible for freeing it.
#[no_mangle]
pub unsafe extern "C" fn create_table(
    path: KernelStringSlice,
    schema: Handle<SharedSchema>,
    engine_info: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let path: DeltaResult<&str> = unsafe { TryFromStringSlice::try_from_slice(&path) };
    let info: DeltaResult<&str> = unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    let schema = unsafe { schema.clone_as_arc() };
    create_table_impl(path, schema, info).into_extern_result(&engine)
}

fn create_table_impl(
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

/// Set a single table property on a [`CreateTableTransactionBuilder`].
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
pub unsafe extern "C" fn create_table_set_property(
    builder: Handle<ExclusiveCreateTableBuilder>,
    key: KernelStringSlice,
    value: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCreateTableBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { *builder.into_inner() };
    let key: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&key) };
    let value: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&value) };
    create_table_set_property_impl(builder, key, value).into_extern_result(&engine)
}

fn create_table_set_property_impl(
    builder: CreateTableTransactionBuilder,
    key: DeltaResult<String>,
    value: DeltaResult<String>,
) -> DeltaResult<Handle<ExclusiveCreateTableBuilder>> {
    let builder = builder.with_table_properties([(key?, value?)]);
    Ok(Box::new(builder).into())
}

/// Build a create-table transaction from the configured builder. Returns a transaction handle
/// that can be used with the standard transaction functions ([`add_files`], [`set_data_change`],
/// [`commit`], etc.) to optionally stage initial data before committing.
///
/// Uses [`FileSystemCommitter`] by default.
///
/// # Safety
///
/// Caller is responsible for passing a valid builder and engine handle.
/// CONSUMES the builder handle -- caller must not use it after this call.
#[no_mangle]
pub unsafe extern "C" fn create_table_build(
    builder: Handle<ExclusiveCreateTableBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveTransaction>> {
    let builder = unsafe { builder.into_inner() };
    let extern_engine = unsafe { engine.as_ref() };
    create_table_build_impl(*builder, extern_engine).into_extern_result(&extern_engine)
}

fn create_table_build_impl(
    builder: CreateTableTransactionBuilder,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Handle<ExclusiveTransaction>> {
    let engine = extern_engine.engine();
    let committer = Box::new(FileSystemCommitter::new());
    let create_txn = builder.build(engine.as_ref(), committer)?;
    // SAFETY: `Transaction<CreateTable>` and `Transaction<ExistingTable>` have identical memory
    // layouts -- the only difference is `PhantomData<S>` which is a zero-sized type. The FFI
    // boundary erases the type parameter anyway. Runtime behavior (protocol/metadata action
    // generation during commit) is controlled by the snapshot version (`PRE_COMMIT_VERSION`),
    // not the type parameter.
    let transaction: Transaction = unsafe { std::mem::transmute(create_txn) };
    Ok(Box::new(transaction).into())
}

/// Free a [`CreateTableTransactionBuilder`] without committing.
///
/// Use this on failure paths when the builder will not be committed.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_create_table_builder(builder: Handle<ExclusiveCreateTableBuilder>) {
    builder.drop_handle();
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::{DataType, StructField, StructType};

    use delta_kernel::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray};
    use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
    use delta_kernel::arrow::ffi::to_ffi;
    use delta_kernel::arrow::json::reader::ReaderBuilder;
    use delta_kernel::arrow::record_batch::RecordBatch;

    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::object_store::local::LocalFileSystem;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStore;
    use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
    use delta_kernel::parquet::file::properties::WriterProperties;

    use delta_kernel_ffi::engine_data::get_engine_data;
    use delta_kernel_ffi::engine_data::ArrowFFIData;

    use delta_kernel_ffi::ffi_test_utils::{allocate_str, ok_or_panic, recover_string};
    use delta_kernel_ffi::tests::get_default_engine;

    use crate::{free_engine, free_schema, kernel_string_slice};
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
        let partition_columns = vec![];

        for (table_url, _engine, store, _table_name) in setup_test_tables(
            schema,
            &partition_columns,
            Some(&tmp_dir_local_url),
            "test_uc_table",
        )
        .await?
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
            let table_id = "foo";
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
                    last_table_id, "foo",
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
        Handle<crate::SharedExternEngine>,
        Handle<ExclusiveCreateTableBuilder>,
    ) {
        let table_path = tmp_dir.path().to_str().unwrap().to_string();
        let schema = Arc::new(StructType::try_new(fields).unwrap());
        let engine = get_default_engine(&table_path);
        let schema_handle: Handle<crate::SharedSchema> = schema.into();
        let engine_info = "test-engine/1.0";
        let builder = ok_or_panic(unsafe {
            create_table(
                kernel_string_slice!(table_path),
                schema_handle.shallow_copy(),
                kernel_string_slice!(engine_info),
                engine.shallow_copy(),
            )
        });
        // create_table does NOT consume the schema handle -- free it
        unsafe { free_schema(schema_handle) };
        (table_path, engine, builder)
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

        // Build transaction, then commit -- should return version 0
        let txn = ok_or_panic(unsafe { create_table_build(builder, engine.shallow_copy()) });
        let version = ok_or_panic(unsafe { commit(txn, engine.shallow_copy()) });
        assert_eq!(version, 0);

        // Verify by opening a snapshot of the created table
        let snap = ok_or_panic(unsafe {
            crate::snapshot(kernel_string_slice!(table_path_str), engine.shallow_copy())
        });
        let snap_version = unsafe { crate::version(snap.shallow_copy()) };
        assert_eq!(snap_version, 0);

        // Verify schema
        let snap_schema = unsafe { crate::logical_schema(snap.shallow_copy()) };
        let snap_schema_ref = unsafe { snap_schema.as_ref() };
        assert_eq!(snap_schema_ref.num_fields(), 2);
        assert_eq!(snap_schema_ref.field_at_index(0).unwrap().name, "id");
        assert_eq!(snap_schema_ref.field_at_index(1).unwrap().name, "name");

        unsafe { free_schema(snap_schema) };
        unsafe { crate::free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_create_table_with_properties() -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        let (_table_path, engine, builder) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );

        // Set properties
        let prop_key1 = "delta.appendOnly";
        let prop_val1 = "true";
        let builder = ok_or_panic(unsafe {
            create_table_set_property(
                builder,
                kernel_string_slice!(prop_key1),
                kernel_string_slice!(prop_val1),
                engine.shallow_copy(),
            )
        });
        let prop_key2 = "custom.key";
        let prop_val2 = "custom_value";
        let builder = ok_or_panic(unsafe {
            create_table_set_property(
                builder,
                kernel_string_slice!(prop_key2),
                kernel_string_slice!(prop_val2),
                engine.shallow_copy(),
            )
        });

        let txn = ok_or_panic(unsafe { create_table_build(builder, engine.shallow_copy()) });
        let version = ok_or_panic(unsafe { commit(txn, engine.shallow_copy()) });
        assert_eq!(version, 0);

        // Verify properties by reading the commit log directly
        let commit_path = Path::from("_delta_log/00000000000000000000.json");
        let storage = LocalFileSystem::new_with_prefix(tmp_dir.path()).unwrap();
        let commit_bytes = storage
            .get(&commit_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let commit_str = std::str::from_utf8(&commit_bytes).unwrap();
        // Parse each line as a JSON action and find the metaData action
        let metadata_action: serde_json::Value = commit_str
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|v| v.get("metaData").is_some())
            .expect("commit should contain a metaData action");
        let config = &metadata_action["metaData"]["configuration"];
        assert_eq!(config["delta.appendOnly"], "true");
        assert_eq!(config["custom.key"], "custom_value");

        unsafe { free_engine(engine) };
        Ok(())
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
        let txn = ok_or_panic(unsafe { create_table_build(builder, engine.shallow_copy()) });
        let version = ok_or_panic(unsafe { commit(txn, engine.shallow_copy()) });
        assert_eq!(version, 0);

        // Try to create the same table again -- build should error (table already exists)
        let (_, _, builder2) = create_table_builder(
            &tmp_dir,
            vec![StructField::nullable("id", DataType::INTEGER)],
        );
        let result = unsafe { create_table_build(builder2, engine.shallow_copy()) };
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
                unsafe { free_transaction(txn) };
                panic!("Expected error for table that already exists");
            }
        }

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

        let result = unsafe { create_table_build(builder, engine.shallow_copy()) };
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
                unsafe { free_transaction(txn) };
                panic!("Expected error for empty schema");
            }
        }

        unsafe { free_engine(engine) };
        Ok(())
    }
}
