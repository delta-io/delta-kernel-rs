//! TableChanges related ffi code

use std::sync::Arc;
use std::sync::Mutex;

use delta_kernel::arrow::array::{Array, ArrayData, RecordBatch, StructArray};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::ffi::to_ffi;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::ScanResult;
use delta_kernel::table_changes::scan::TableChangesScan;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::Error;
use delta_kernel::{DeltaResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;

use super::handle::Handle;
use url::Url;

use crate::engine_data::ArrowFFIData;
use crate::expressions::kernel_visitor::{unwrap_kernel_predicate, KernelExpressionVisitorState};
use crate::scan::EnginePredicate;
use crate::{
    kernel_string_slice, unwrap_and_parse_path_as_url, AllocateStringFn, ExternEngine,
    ExternResult, IntoExternResult, KernelStringSlice, NullableCvoid, SharedExternEngine,
    SharedSchema,
};

#[handle_descriptor(target=TableChanges, mutable=true, sized=true)]
pub struct ExclusiveTableChanges;

/// Get the table changes from the specified table at a specific version
///
/// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
/// - `engine`: Implementation of `Engine` apis.
/// - `start_version`: The start version of the change data feed
///   End version will be the newest table version.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn table_changes_from_version(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    start_version: Version,
) -> ExternResult<Handle<ExclusiveTableChanges>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    table_changes_impl(url, engine, start_version, None).into_extern_result(&engine)
}

/// Get the table changes from the specified table between two versions
///
/// - `table_root`: url pointing at the table root (where `_delta_log` folder is located)
/// - `engine`: Implementation of `Engine` apis.
/// - `start_version`: The start version of the change data feed
/// - `end_version`: The end version (inclusive) of the change data feed.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn table_changes_between_versions(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    start_version: Version,
    end_version: Version,
) -> ExternResult<Handle<ExclusiveTableChanges>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    table_changes_impl(url, engine, start_version, end_version.into()).into_extern_result(&engine)
}

fn table_changes_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
    start_version: Version,
    end_version: Option<Version>,
) -> DeltaResult<Handle<ExclusiveTableChanges>> {
    let table_changes = TableChanges::try_new(
        url?,
        extern_engine.engine().as_ref(),
        start_version,
        end_version,
    );
    Ok(Box::new(table_changes?).into())
}

/// Drops table changes.
///
/// # Safety
/// Caller is responsible for passing a valid table changes handle.
#[no_mangle]
pub unsafe extern "C" fn free_table_changes(table_changes: Handle<ExclusiveTableChanges>) {
    table_changes.drop_handle();
}

/// Get schema from the specified TableChanges.
///
/// # Safety
///
/// Caller is responsible for passing a valid table changes handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_schema(
    table_changes: Handle<ExclusiveTableChanges>,
) -> Handle<SharedSchema> {
    let table_changes = unsafe { table_changes.as_ref() };
    Arc::new(table_changes.schema().clone()).into()
}

/// Get table root from the specified TableChanges.
///
/// # Safety
///
/// Caller is responsible for passing a valid table changes handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_table_root(
    table_changes: Handle<ExclusiveTableChanges>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let table_changes = unsafe { table_changes.as_ref() };
    let table_root = table_changes.table_root().to_string();
    allocate_fn(kernel_string_slice!(table_root))
}

/// Get start version from the specified TableChanges.
///
/// # Safety
///
/// Caller is responsible for passing a valid table changes handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_start_version(
    table_changes: Handle<ExclusiveTableChanges>,
) -> u64 {
    let table_changes = unsafe { table_changes.as_ref() };
    table_changes.start_version()
}

/// Get end version from the specified TableChanges.
///
/// # Safety
///
/// Caller is responsible for passing a valid table changes handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_end_version(
    table_changes: Handle<ExclusiveTableChanges>,
) -> u64 {
    let table_changes = unsafe { table_changes.as_ref() };
    table_changes.end_version()
}

#[handle_descriptor(target=TableChangesScan, mutable=false, sized=true)]
pub struct SharedTableChangesScan;

/// Get a [`TableChangesScan`] over the table specified by the passed table changes.
/// It is the responsibility of the _engine_ to free this scan when complete by calling [`free_table_changes_scan`].
/// Consumes TableChanges.
///
/// # Safety
///
/// Caller is responsible for passing a valid table changes pointer, and engine pointer
#[no_mangle]
pub unsafe extern "C" fn table_changes_scan(
    table_changes: Handle<ExclusiveTableChanges>,
    engine: Handle<SharedExternEngine>,
    predicate: Option<&mut EnginePredicate>,
) -> ExternResult<Handle<SharedTableChangesScan>> {
    let table_changes = unsafe { table_changes.into_inner() };
    table_changes_scan_impl(*table_changes, predicate).into_extern_result(&engine.as_ref())
}

fn table_changes_scan_impl(
    table_changes: TableChanges,
    predicate: Option<&mut EnginePredicate>,
) -> DeltaResult<Handle<SharedTableChangesScan>> {
    let mut scan_builder = table_changes.into_scan_builder();
    if let Some(predicate) = predicate {
        let mut visitor_state = KernelExpressionVisitorState::default();
        let pred_id = (predicate.visitor)(predicate.predicate, &mut visitor_state);
        let predicate = unwrap_kernel_predicate(&mut visitor_state, pred_id);
        debug!("Table changes got predicate: {:#?}", predicate);
        scan_builder = scan_builder.with_predicate(predicate.map(Arc::new));
    }
    Ok(Arc::new(scan_builder.build()?).into())
}

/// Drops a table changes scan.
///
/// # Safety
/// Caller is responsible for passing a valid scan handle.
#[no_mangle]
pub unsafe extern "C" fn free_table_changes_scan(
    table_changes_scan: Handle<SharedTableChangesScan>,
) {
    table_changes_scan.drop_handle();
}

/// Get the table root of a table changes scan.
///
/// # Safety
/// Engine is responsible for providing a valid scan pointer and allocate_fn (for allocating the
/// string)
#[no_mangle]
pub unsafe extern "C" fn table_changes_scan_table_root(
    table_changes_scan: Handle<SharedTableChangesScan>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let table_changes_scan = unsafe { table_changes_scan.as_ref() };
    let table_root = table_changes_scan.table_root().to_string();
    allocate_fn(kernel_string_slice!(table_root))
}

/// Get the logical schema of the specified table changes scan.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_scan_logical_schema(
    table_changes_scan: Handle<SharedTableChangesScan>,
) -> Handle<SharedSchema> {
    let table_changes_scan = unsafe { table_changes_scan.as_ref() };
    table_changes_scan.logical_schema().clone().into()
}

/// Get the physical schema of the specified table changes scan.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn table_changes_scan_physical_schema(
    table_changes_scan: Handle<SharedTableChangesScan>,
) -> Handle<SharedSchema> {
    let table_changes_scan = unsafe { table_changes_scan.as_ref() };
    table_changes_scan.physical_schema().clone().into()
}

pub struct ScanTableChangesIterator {
    data: Mutex<Box<dyn Iterator<Item = DeltaResult<ScanResult>> + Send>>,
    engine: Arc<dyn ExternEngine>,
}

#[handle_descriptor(target=ScanTableChangesIterator, mutable=false, sized=true)]
pub struct SharedScanTableChangesIterator;

impl Drop for ScanTableChangesIterator {
    fn drop(&mut self) {
        debug!("dropping ScanTableChangesIterator");
    }
}

/// Get an iterator over the data needed to perform a table changes scan. This will return a
/// [`ScanTableChangesIterator`] which can be passed to [`scan_table_changes_next`] to get the
/// actual data in the iterator.
///
/// # Safety
///
/// Engine is responsible for passing a valid [`SharedExternEngine`] and [`SharedTableChangesScan`]
#[no_mangle]
pub unsafe extern "C" fn table_changes_scan_execute(
    table_changes_scan: Handle<SharedTableChangesScan>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedScanTableChangesIterator>> {
    let table_changes_scan = unsafe { table_changes_scan.as_ref() };
    let engine = unsafe { engine.clone_as_arc() };
    table_changes_scan_execute_impl(table_changes_scan, engine.clone())
        .into_extern_result(&engine.as_ref())
}

fn table_changes_scan_execute_impl(
    table_changes_scan: &TableChangesScan,
    engine: Arc<dyn ExternEngine>,
) -> DeltaResult<Handle<SharedScanTableChangesIterator>> {
    let table_changes_iter = table_changes_scan.execute(engine.engine().clone())?;
    let data = ScanTableChangesIterator {
        data: Mutex::new(Box::new(table_changes_iter)),
        engine: engine.clone(),
    };
    Ok(Arc::new(data).into())
}

/// # Safety
///
/// Drops table changes iterator.
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`table_changes_scan_execute`].
#[no_mangle]
pub unsafe extern "C" fn free_scan_table_changes_iter(
    data: Handle<SharedScanTableChangesIterator>,
) {
    data.drop_handle();
}

/// Get next batch of data from the table changes iterator.
///
/// # Safety
///
/// The iterator must be valid (returned by [table_changes_scan_execute]) and not yet freed by
/// [`free_scan_table_changes_iter`].
#[no_mangle]
pub unsafe extern "C" fn scan_table_changes_next(
    data: Handle<SharedScanTableChangesIterator>,
) -> ExternResult<ArrowFFIData> {
    let data = unsafe { data.as_ref() };
    scan_table_changes_next_impl(data).into_extern_result(&data.engine.as_ref())
}

fn scan_table_changes_next_impl(data: &ScanTableChangesIterator) -> DeltaResult<ArrowFFIData> {
    let mut data = data
        .data
        .lock()
        .map_err(|_| Error::generic("poisoned scan table changes iterator mutex"))?;

    let Some(scan_result) = data.next().transpose()? else {
        return Ok(ArrowFFIData::empty());
    };

    let mask = scan_result.full_mask();
    let data = scan_result.raw_data?;
    let mut record_batch: RecordBatch = data
        .into_any()
        .downcast::<ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into();

    if let Some(mask) = mask {
        record_batch = filter_record_batch(&record_batch, &mask.into())?;
    }

    let batch_struct_array: StructArray = record_batch.into();
    let array_data: ArrayData = batch_struct_array.into_data();
    let (out_array, out_schema) = to_ffi(&array_data)?;
    Ok(ArrowFFIData {
        array: out_array,
        schema: out_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_test_utils::{allocate_err, allocate_str, ok_or_panic, recover_string};
    use crate::{engine_to_handle, free_engine, free_schema, kernel_string_slice};

    use delta_kernel::arrow::array::{ArrayRef, Int32Array, StringArray};
    use delta_kernel::arrow::datatypes::{Field, Schema};
    use delta_kernel::arrow::error::ArrowError;
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::arrow::util::pretty::pretty_format_batches;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Engine;
    use delta_kernel_ffi::engine_data::get_engine_data;
    use itertools::Itertools;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use std::sync::Arc;
    use test_utils::{
        actions_to_string_with_metadata, add_commit, generate_batch, record_batch_to_bytes,
        to_arrow, IntoArray as _, TestAction,
    };

    const PARQUET_FILE1: &str =
        "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet";
    const PARQUET_FILE2: &str =
        "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet";

    pub const METADATA: &str = r#"
    {"commitInfo": {
        "timestamp": 1587968586154,
        "operation": "WRITE",
        "operationParameters": {
        "mode": "ErrorIfExists",
        "partitionBy": "[]"
        },
        "isBlindAppend": true
    }}
    {"protocol": {
        "minReaderVersion": 1,
        "minWriterVersion": 2
    }}
    {"metaData": {
        "id": "5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
        "format": {
        "provider": "parquet",
        "options": {}
        },
        "schemaString": "{
        \"type\": \"struct\",
        \"fields\": [
            {
            \"name\": \"id\",
            \"type\": \"integer\",
            \"nullable\": true,
            \"metadata\": {}
            },
            {
            \"name\": \"val\",
            \"type\": \"string\",
            \"nullable\": true,
            \"metadata\": {}
            }
        ]
        }",
        "partitionColumns": [],
        "configuration": {
        "delta.enableChangeDataFeed": "true"
        },
        "createdTime": 1587968585495
    }}
    "#;

    async fn commit_add_file(
        storage: &dyn ObjectStore,
        version: u64,
        file: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        add_commit(
            storage,
            version,
            actions_to_string_with_metadata(
                vec![TestAction::Metadata, TestAction::Add(file)],
                METADATA,
            ),
        )
        .await
    }

    async fn commit_remove_file(
        storage: &dyn ObjectStore,
        version: u64,
        file: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        add_commit(
            storage,
            version,
            actions_to_string_with_metadata(
                vec![TestAction::Metadata, TestAction::Remove(file)],
                METADATA,
            ),
        )
        .await
    }

    async fn put_file(
        storage: &dyn ObjectStore,
        file: String,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error>> {
        storage
            .put(&Path::from(file), record_batch_to_bytes(batch).into())
            .await?;
        Ok(())
    }

    pub fn generate_batch_with_id(start_i: i32) -> Result<RecordBatch, ArrowError> {
        generate_batch(vec![
            ("id", vec![start_i, start_i + 1, start_i + 2].into_array()),
            ("val", vec!["a", "b", "c"].into_array()),
        ])
    }

    pub fn get_batch_schema() -> Arc<StructType> {
        Arc::new(
            StructType::try_new(vec![
                StructField::nullable("id", DataType::INTEGER),
                StructField::nullable("val", DataType::STRING),
                StructField::nullable("_change_type", DataType::STRING),
                StructField::nullable("_commit_version", DataType::INTEGER),
            ])
            .unwrap(),
        )
    }

    fn check_columns_in_schema(fields: &[&str], schema: &StructType) -> bool {
        fields.iter().all(|f| schema.contains(f))
    }

    fn read_scan(
        scan: &TableChangesScan,
        engine: Arc<dyn Engine>,
    ) -> DeltaResult<Vec<RecordBatch>> {
        let scan_results = scan.execute(engine)?;
        scan_results
            .map(|scan_result| -> DeltaResult<_> {
                let scan_result = scan_result?;
                let mask = scan_result.full_mask();
                let data = scan_result.raw_data?;
                let record_batch = to_arrow(data)?;
                if let Some(mask) = mask {
                    Ok(filter_record_batch(&record_batch, &mask.into())?)
                } else {
                    Ok(record_batch)
                }
            })
            .try_collect()
    }

    fn filter_batches(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
        batches
            .into_iter()
            .map(|batch| {
                let schema = batch.schema();
                let keep_indices: Vec<usize> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, field)| {
                        if field.name() != "_commit_timestamp" {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect();

                let columns: Vec<ArrayRef> = keep_indices
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect();

                let fields: Vec<Arc<Field>> = keep_indices
                    .iter()
                    .map(|&i| Arc::new(schema.field(i).clone()))
                    .collect();

                let filtered_schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(filtered_schema, columns).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_table_changes_getters() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        commit_add_file(storage.as_ref(), 0, PARQUET_FILE1.to_string()).await?;
        commit_add_file(storage.as_ref(), 1, PARQUET_FILE2.to_string()).await?;

        let batch = generate_batch_with_id(1)?;
        put_file(storage.as_ref(), PARQUET_FILE1.to_string(), &batch).await?;
        let batch = generate_batch_with_id(4)?;
        put_file(storage.as_ref(), PARQUET_FILE2.to_string(), &batch).await?;

        let path = "memory:///";
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let table_changes = ok_or_panic(unsafe {
            table_changes_from_version(kernel_string_slice!(path), engine.shallow_copy(), 0)
        });

        assert_eq!(
            unsafe { table_changes_start_version(table_changes.shallow_copy()) },
            0
        );
        assert_eq!(
            unsafe { table_changes_end_version(table_changes.shallow_copy()) },
            1
        );

        let table_root =
            unsafe { table_changes_table_root(table_changes.shallow_copy(), allocate_str) };
        assert_eq!(recover_string(table_root.unwrap()), path);

        let schema = unsafe { table_changes_schema(table_changes.shallow_copy()).shallow_copy() };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().len(), 5);
        check_columns_in_schema(
            &[
                "id",
                "val",
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
            ],
            schema_ref,
        );

        let table_changes_scan =
            ok_or_panic(unsafe { table_changes_scan(table_changes, engine.shallow_copy(), None) });

        let table_root = unsafe {
            table_changes_scan_table_root(table_changes_scan.shallow_copy(), allocate_str)
        };
        assert_eq!(recover_string(table_root.unwrap()), path);

        let logical_schema = unsafe {
            table_changes_scan_logical_schema(table_changes_scan.shallow_copy()).shallow_copy()
        };
        let logical_schema_ref = unsafe { logical_schema.as_ref() };
        assert_eq!(logical_schema_ref.fields().len(), 5);
        check_columns_in_schema(
            &[
                "id",
                "val",
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
            ],
            logical_schema_ref,
        );

        let physical_schema = unsafe {
            table_changes_scan_physical_schema(table_changes_scan.shallow_copy()).shallow_copy()
        };
        let physical_schema_ref = unsafe { physical_schema.as_ref() };
        assert_eq!(physical_schema_ref.fields().len(), 2);
        check_columns_in_schema(&["id", "val"], physical_schema_ref);

        unsafe {
            free_table_changes_scan(table_changes_scan);
            free_engine(engine);
            free_schema(schema);
            free_schema(logical_schema);
            free_schema(physical_schema);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_table_changes_scan() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        commit_add_file(storage.as_ref(), 0, PARQUET_FILE1.to_string()).await?;
        commit_add_file(storage.as_ref(), 1, PARQUET_FILE2.to_string()).await?;

        let batch = generate_batch_with_id(1)?;
        put_file(storage.as_ref(), PARQUET_FILE1.to_string(), &batch).await?;
        let batch = generate_batch_with_id(4)?;
        put_file(storage.as_ref(), PARQUET_FILE2.to_string(), &batch).await?;

        let path = "memory:///";
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let table_changes = ok_or_panic(unsafe {
            table_changes_from_version(kernel_string_slice!(path), engine.shallow_copy(), 0)
        });
        let table_changes_scan =
            ok_or_panic(unsafe { table_changes_scan(table_changes, engine.shallow_copy(), None) });
        let batches = unsafe {
            read_scan(
                &table_changes_scan.into_inner(),
                engine.into_inner().engine(),
            )
        };
        let batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
        let filtered_batches: Vec<RecordBatch> = filter_batches(batches);

        let table_schema = get_batch_schema();
        let expected = &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(table_schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b", "c"])),
                Arc::new(StringArray::from(vec![
                    "insert", "insert", "insert", "insert", "insert", "insert",
                ])),
                Arc::new(Int32Array::from(vec![0, 0, 0, 1, 1, 1])),
            ],
        )?);

        let formatted = pretty_format_batches(&filtered_batches)
            .unwrap()
            .to_string();
        let expected = pretty_format_batches(&[expected.record_batch().clone()])
            .unwrap()
            .to_string();

        println!("actual:\n{formatted}");
        println!("expected:\n{expected}");
        assert_eq!(formatted, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_changes_scan_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        commit_add_file(storage.as_ref(), 0, PARQUET_FILE1.to_string()).await?;
        commit_add_file(storage.as_ref(), 1, PARQUET_FILE2.to_string()).await?;

        let batch = generate_batch_with_id(1)?;
        put_file(storage.as_ref(), PARQUET_FILE1.to_string(), &batch).await?;
        let batch = generate_batch_with_id(4)?;
        put_file(storage.as_ref(), PARQUET_FILE2.to_string(), &batch).await?;

        let path = "memory:///";
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let table_changes = ok_or_panic(unsafe {
            table_changes_from_version(kernel_string_slice!(path), engine.shallow_copy(), 0)
        });

        let table_changes_scan =
            ok_or_panic(unsafe { table_changes_scan(table_changes, engine.shallow_copy(), None) });

        let table_changes_scan_iter_result = ok_or_panic(unsafe {
            table_changes_scan_execute(table_changes_scan.shallow_copy(), engine.shallow_copy())
        });

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut i: i32 = 0;
        loop {
            i += 1;
            let data = ok_or_panic(unsafe {
                scan_table_changes_next(table_changes_scan_iter_result.shallow_copy())
            });
            if data.array.is_empty() {
                break;
            }
            let engine_data =
                ok_or_panic(unsafe { get_engine_data(data.array, &data.schema, allocate_err) });
            let record_batch = unsafe { to_arrow(engine_data.into_inner()) }?;

            println!("Batch ({i}) num rows {:?}", record_batch.num_rows());
            batches.push(record_batch);
        }

        let filtered_batches: Vec<RecordBatch> = filter_batches(batches);
        let formatted = pretty_format_batches(&filtered_batches)
            .unwrap()
            .to_string();

        let table_schema = get_batch_schema();
        let expected = &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(table_schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b", "c"])),
                Arc::new(StringArray::from(vec![
                    "insert", "insert", "insert", "insert", "insert", "insert",
                ])),
                Arc::new(Int32Array::from(vec![0, 0, 0, 1, 1, 1])),
            ],
        )?);

        let expected = pretty_format_batches(&[expected.record_batch().clone()])
            .unwrap()
            .to_string();

        println!("actual:\n{formatted}");
        println!("expected:\n{expected}");
        assert_eq!(formatted, expected);

        unsafe {
            free_table_changes_scan(table_changes_scan);
            free_scan_table_changes_iter(table_changes_scan_iter_result);
            free_engine(engine);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_table_changes_between_commits() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        commit_add_file(storage.as_ref(), 0, PARQUET_FILE1.to_string()).await?;
        commit_add_file(storage.as_ref(), 1, PARQUET_FILE2.to_string()).await?;
        commit_remove_file(storage.as_ref(), 2, PARQUET_FILE1.to_string()).await?;
        commit_remove_file(storage.as_ref(), 3, PARQUET_FILE2.to_string()).await?;

        let batch = generate_batch_with_id(1)?;
        put_file(storage.as_ref(), PARQUET_FILE1.to_string(), &batch).await?;
        let batch = generate_batch_with_id(4)?;
        put_file(storage.as_ref(), PARQUET_FILE2.to_string(), &batch).await?;

        let path = "memory:///";
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let table_changes = ok_or_panic(unsafe {
            table_changes_between_versions(kernel_string_slice!(path), engine.shallow_copy(), 1, 2)
        });
        let table_changes_scan =
            ok_or_panic(unsafe { table_changes_scan(table_changes, engine.shallow_copy(), None) });
        let batches = unsafe {
            read_scan(
                &table_changes_scan.into_inner(),
                engine.into_inner().engine(),
            )
        };
        let batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
        let filtered_batches: Vec<RecordBatch> = filter_batches(batches);

        let table_schema = Arc::new(StructType::try_new(vec![
            StructField::nullable("id", DataType::INTEGER),
            StructField::nullable("val", DataType::STRING),
            StructField::nullable("_change_type", DataType::STRING),
            StructField::nullable("_commit_version", DataType::INTEGER),
        ])?);
        let expected = &ArrowEngineData::new(RecordBatch::try_new(
            Arc::new(table_schema.as_ref().try_into_arrow()?),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b", "c"])),
                Arc::new(StringArray::from(vec![
                    "insert", "insert", "insert", "delete", "delete", "delete",
                ])),
                Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2, 2])),
            ],
        )?);

        let formatted = pretty_format_batches(&filtered_batches)
            .unwrap()
            .to_string();
        let expected = pretty_format_batches(&[expected.record_batch().clone()])
            .unwrap()
            .to_string();

        println!("actual:\n{formatted}");
        println!("expected:\n{expected}");
        assert_eq!(formatted, expected);

        Ok(())
    }
}
