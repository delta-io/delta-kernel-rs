//! This module holds functionality for managing transactions.
mod write_context;

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::KernelStringSlice;
use crate::{unwrap_and_parse_path_as_url, TryFromStringSlice};
use crate::{DeltaResult, ExternEngine, Snapshot, Url};
use crate::{ExclusiveEngineData, SharedExternEngine};
use delta_kernel::transaction::{CommitResult, Transaction};
use delta_kernel_ffi_macros::handle_descriptor;

use std::sync::Arc;

/// A handle representing an exclusive transaction on a Delta table. (Similar to a Box<_>)
///
/// This struct provides a safe wrapper around the underlying `Transaction` type,
/// ensuring exclusive access to transaction operations. The transaction can be used
/// to stage changes and commit them atomically to the Delta table.
#[handle_descriptor(target=Transaction, mutable=true, sized=true)]
pub struct ExclusiveTransaction;

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
    let snapshot =
        Arc::new(Snapshot::try_from_uri(url?, extern_engine.engine().as_ref(), None).unwrap());
    let transaction = snapshot.transaction();
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
) -> Handle<ExclusiveTransaction> {
    let txn = unsafe { txn.into_inner() };
    let info_string: DeltaResult<&str> =
        unsafe { TryFromStringSlice::try_from_slice(&engine_info) };
    Box::new(txn.with_engine_info(info_string.unwrap())).into()
}

/// Add file metadata to the transaction for files that have been written. The metadata contains
/// information about files written during the transaction that will be added to the Delta log
/// during commit.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle. Assumes no concurrent txn usage. Consumes
/// write_metadata.
#[no_mangle]
pub unsafe extern "C" fn add_files(
    mut txn: Handle<ExclusiveTransaction>,
    write_metadata: Handle<ExclusiveEngineData>,
) {
    let txn = unsafe { txn.as_mut() };
    let write_metadata = unsafe { write_metadata.into_inner() };
    txn.add_files(write_metadata);
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
    // TODO: for now this removes the enum, which prevents doing any conflict resolution. We should fix
    //       this by making the commit function return the enum somehow.
    match txn.commit(engine.as_ref()) {
        Ok(CommitResult::Committed {
            version: v,
            post_commit_stats: _,
        }) => Ok(v),
        Ok(CommitResult::Conflict(_, v)) => Err(delta_kernel::Error::Generic(format!(
            "commit conflict at version {v}"
        ))),
        Err(e) => Err(e),
    }
    .into_extern_result(&extern_engine)
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::{DataType, StructField, StructType};

    use delta_kernel::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use delta_kernel::arrow::ffi::to_ffi;
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::parquet::arrow::arrow_writer::ArrowWriter;
    use delta_kernel::parquet::file::properties::WriterProperties;

    use delta_kernel_ffi::engine_data::get_engine_data;
    use delta_kernel_ffi::engine_data::ArrowFFIData;

    use delta_kernel_ffi::tests::{get_default_engine, ok_or_panic};

    use crate::{free_engine, free_schema, kernel_string_slice};
    use write_context::{free_write_context, get_write_context, get_write_path, get_write_schema};

    use test_utils::{setup_test_tables, test_read};

    use std::sync::Arc;

    use super::*;

    use tempfile::tempdir;

    fn create_arrow_ffi_from_json(
        schema: ArrowSchema,
        json_string: &str,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        let cursor = std::io::Cursor::new(json_string.as_bytes());
        let mut reader = delta_kernel::arrow::json::reader::ReaderBuilder::new(schema.into())
            .build(cursor)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let commit_info_struct_array: StructArray = batch.into();
        let array_data = commit_info_struct_array.into_data();
        let (out_array, out_schema) = to_ffi(&array_data)?;

        Ok(ArrowFFIData {
            array: out_array,
            schema: out_schema,
        })
    }

    fn create_file_metadata(
        path: &str,
        num_rows: i64,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        let schema = ArrowSchema::new(vec![
            Field::new("path", ArrowDataType::Utf8, false),
            Field::new(
                "partitionValues",
                ArrowDataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        ArrowDataType::Struct(
                            vec![
                                Field::new("key", ArrowDataType::Utf8, false),
                                Field::new("value", ArrowDataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ),
            Field::new("size", ArrowDataType::Int64, false),
            Field::new("modificationTime", ArrowDataType::Int64, false),
            Field::new("dataChange", ArrowDataType::Boolean, false),
        ]);

        let current_time: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let file_metadata = format!(
            r#"{{"path":"{path}", "partitionValues": {{}}, "size": {num_rows}, "modificationTime": {current_time}, "dataChange": true}}"#,
        );

        create_arrow_ffi_from_json(schema, file_metadata.as_str())
    }

    fn write_parquet_file(
        delta_path: &str,
        file_path: &str,
        batch: &RecordBatch,
    ) -> Result<ArrowFFIData, Box<dyn std::error::Error>> {
        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder().build();

        let full_path = format!("{delta_path}{file_path}");
        let file = std::fs::File::create(&full_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(batch).expect("Writing batch");

        // writer must be closed to write footer
        let res = writer.close().unwrap();

        create_file_metadata(file_path, res.num_rows)
    }

    #[tokio::test]
    async fn test_basic_append() -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(StructType::new(vec![
            StructField::nullable("number", DataType::INTEGER),
            StructField::nullable("string", DataType::STRING),
        ]));

        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // TODO: test with partitions
        let partition_columns = vec![];

        for (table_url, _engine, _store, _table_name) in setup_test_tables(
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

            // Add engine info
            let engine_info = "default_engine";
            let engine_info_kernel_string = kernel_string_slice!(engine_info);
            let txn_with_engine_info = unsafe { with_engine_info(txn, engine_info_kernel_string) };

            let write_context = unsafe { get_write_context(txn_with_engine_info.shallow_copy()) };

            // TODO: write separate test for properly testing the write schema
            let write_schema = unsafe { get_write_schema(write_context.shallow_copy()) };

            // Ensure the ffi returns the correct table path TODO: can the write path be different from the table path?
            let write_path =
                unsafe { get_write_path(write_context.shallow_copy(), crate::tests::allocate_str) };
            assert!(write_path.is_some());
            let unwrapped_url = crate::tests::recover_string(write_path.unwrap());
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

            let file_info = write_parquet_file(table_path_str, "my_file.parquet", &batch)?;

            let file_info_engine_data = ok_or_panic(unsafe {
                get_engine_data(file_info.array, &file_info.schema, engine.shallow_copy())
            });

            unsafe { add_files(txn_with_engine_info.shallow_copy(), file_info_engine_data) };

            ok_or_panic(unsafe { commit(txn_with_engine_info, engine.shallow_copy()) });

            // Confirm that the data matches what we appended
            let test_batch = ArrowEngineData::from(batch);
            test_read(&test_batch, &table_url, unsafe { engine.as_ref().engine() })?;

            unsafe { free_schema(write_schema) };
            unsafe { free_write_context(write_context) };
            unsafe { free_engine(engine) };
        }

        Ok(())
    }
}
