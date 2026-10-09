//! Prepare connector-requested deletion vectors without implementing their storage format.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::scan::state::ScanFile;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{Operation, TableFeature};
use delta_kernel::transaction::BoundWriteContext;
use delta_kernel::{Engine, KernelError, Result};

use super::deletion_vector::{DvDescriptorMap, ExclusiveDvDescriptorMap};
use super::write_context::SharedWriteContext;
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{FfiSlice, KernelStringSlice, SharedExternEngine, SharedSnapshot, TryFromStringSlice};

/// Deleted physical row indexes grouped under one active data-file path.
#[repr(C)]
pub struct FfiDeletionVectorUpdate {
    /// URI-encoded path exactly as recorded in the snapshot's Add action.
    pub data_file_path: KernelStringSlice,
    /// Zero-based row ordinals across the entire Parquet file, not within a row group.
    /// Duplicate indexes are accepted; an empty slice is rejected.
    pub row_indexes: FfiSlice<u64>,
}

/// Write on-disk deletion vectors for a bounded batch of active files.
///
/// `snapshot` supplies active-file metadata and existing DVs. `write_context` generates DV paths
/// under that same table root. `updates` supplies one entry per file. `engine` performs the I/O
/// and allocates errors. The operation reads snapshot metadata once and merges each file's existing
/// deleted indexes with the requested indexes. It does not read or rewrite business Parquet data.
///
/// Returns an owned descriptor map. Pass it to `transaction_update_deletion_vectors` with a
/// metadata iterator from the same snapshot, or release it with `free_dv_descriptor_map`.
/// No actions are staged or committed. An empty batch returns an empty map.
///
/// # Errors
///
/// Returns an error for unsupported/disabled DVs, mismatched table roots, duplicate or inactive
/// paths, missing physical row counts, invalid row indexes, malformed input, or storage failures.
/// Input paths and requested indexes are validated before uploading any DV. A later existing-DV
/// read or upload failure can leave uncommitted DV files; the connector must retain them until
/// safe orphan cleanup. No partially prepared map is returned on error.
///
/// # Safety
///
/// All handles are borrowed and must be valid. The write context and the transaction receiving the
/// returned map must come from `snapshot`; a matching table root alone does not establish snapshot
/// consistency. The update slice, its UTF-8 strings, and its row-index slices must be aligned,
/// initialized, readable, and remain valid for this call. Null slice pointers are valid only for
/// zero lengths. Kernel does not retain any input memory after returning.
#[no_mangle]
pub unsafe extern "C" fn write_deletion_vectors(
    snapshot: Handle<SharedSnapshot>,
    write_context: Handle<SharedWriteContext>,
    updates: FfiSlice<FfiDeletionVectorUpdate>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDvDescriptorMap>> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let updates = unsafe { updates.try_as_slice() }?;
        let updates = unsafe { decode_updates(updates) }?;
        let snapshot = unsafe { snapshot.clone_as_arc() };
        let context = unsafe { write_context.as_ref() };
        write_deletion_vectors_impl(snapshot, context, updates, engine.engine().as_ref())
            .map(|map| Box::new(map).into())
    })();
    result.into_extern_result(&engine)
}

struct RequestedFiles<'a> {
    indexes: HashMap<&'a str, &'a [u64]>,
    files: HashMap<String, ScanFile>,
    duplicate: Option<String>,
}

unsafe fn decode_updates(updates: &[FfiDeletionVectorUpdate]) -> Result<HashMap<&str, &[u64]>> {
    let mut requests = HashMap::with_capacity(updates.len());
    for update in updates {
        let path: &str = unsafe { TryFromStringSlice::try_from_slice(&update.data_file_path) }?;
        let indexes = unsafe { update.row_indexes.try_as_slice() }?;
        if path.is_empty() || indexes.is_empty() {
            return Err(KernelError::generic(
                "DV update paths and row-index slices must be nonempty",
            ));
        }
        if requests.insert(path, indexes).is_some() {
            return Err(KernelError::generic(format!(
                "duplicate DV update path: {path}"
            )));
        }
    }
    Ok(requests)
}

fn write_deletion_vectors_impl(
    snapshot: Arc<Snapshot>,
    context: &BoundWriteContext,
    indexes: HashMap<&str, &[u64]>,
    engine: &dyn Engine,
) -> Result<DvDescriptorMap> {
    if snapshot.table_root() != context.table_root_dir() {
        return Err(KernelError::generic(
            "DV snapshot and write context have different table roots",
        ));
    }
    let configuration = snapshot.table_configuration();
    configuration.ensure_operation_supported(Operation::Write)?;
    if !configuration.is_feature_enabled(&TableFeature::DeletionVectors) {
        return Err(KernelError::generic(
            "DV writes require deletionVectors features and delta.enableDeletionVectors=true",
        ));
    }

    let mut map = DvDescriptorMap {
        inner: HashMap::with_capacity(indexes.len()),
    };
    if indexes.is_empty() {
        return Ok(map);
    }
    let scan = snapshot.clone().scan_builder().build()?;
    let mut requested = RequestedFiles {
        indexes,
        files: HashMap::new(),
        duplicate: None,
    };
    for metadata in scan.scan_metadata(engine)? {
        requested = metadata?.visit_scan_files(requested, select_file)?;
    }
    if let Some(path) = requested.duplicate {
        return Err(KernelError::generic(format!(
            "multiple active DV file instances: {path}"
        )));
    }

    for (path, indexes) in &requested.indexes {
        let file = requested.files.get(*path).ok_or_else(|| {
            KernelError::generic(format!(
                "DV data file is not active in the snapshot: {path}"
            ))
        })?;
        validate_rows(path, indexes, physical_row_count(file)?)?;
    }

    for (path, indexes) in requested.indexes {
        let file = requested
            .files
            .get(path)
            .ok_or_else(|| KernelError::internal_error("validated DV file metadata is missing"))?;
        let mut dv = KernelDeletionVector::new();
        if let Some(previous) = file
            .dv_info
            .get_row_indexes(engine, snapshot.table_root())?
        {
            if file.dv_info.cardinality()? != Some(previous.len() as u64) {
                return Err(KernelError::deletion_vector(format!(
                    "existing DV cardinality does not match its decoded indexes for {path}",
                )));
            }
            validate_rows(path, &previous, physical_row_count(file)?)?;
            dv.add_deleted_row_indexes(previous);
        }
        dv.add_deleted_row_indexes(indexes);

        let dv_path = context.new_deletion_vector_path(String::new());
        let mut bytes = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
        let result = writer.write_deletion_vector(dv)?;
        writer.finalize()?;
        engine
            .storage_handler()
            .put(&dv_path.absolute_path()?, bytes.into(), false)?;
        map.inner
            .insert(path.to_owned(), result.to_descriptor(&dv_path));
    }
    Ok(map)
}

fn select_file(requested: &mut RequestedFiles<'_>, file: ScanFile) {
    if requested.indexes.contains_key(file.path.as_str()) {
        let path = file.path.clone();
        if requested.files.insert(path.clone(), file).is_some() {
            requested.duplicate = Some(path);
        }
    }
}

fn physical_row_count(file: &ScanFile) -> Result<u64> {
    file.stats
        .as_ref()
        .map(|stats| stats.num_records)
        .ok_or_else(|| {
            KernelError::generic(format!(
                "DV data file has no numRecords statistic: {}",
                file.path
            ))
        })
}

fn validate_rows(path: &str, rows: &[u64], num_records: u64) -> Result<()> {
    if let Some(index) = rows.iter().find(|index| **index >= num_records) {
        return Err(KernelError::generic(format!(
            "DV row index {index} is outside physical row count {num_records} for {path}",
        )));
    }
    Ok(())
}

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
    use std::ptr;

    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::{DynObjectStore, ObjectStoreExt};
    use delta_kernel::parquet::file::properties::WriterProperties;
    use delta_kernel::schema::schema_ref;
    use delta_kernel::transaction::create_table::create_table;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use test_utils::{
        create_add_files_metadata, engine_store_setup, into_record_batch, record_batch_to_bytes,
        record_batch_to_bytes_with_props,
    };

    use super::*;
    use crate::ffi_test_utils::{engine_handle_for_store, ok_or_panic, recover_error};
    use crate::scan::{free_scan, scan, scan_metadata_iter_init};
    use crate::transaction::write_context::free_write_context;
    use crate::transaction::{
        commit, committed_transaction_version, free_committed_transaction, free_transaction,
        transaction_from_snapshot, transaction_update_deletion_vectors,
    };
    use crate::{free_engine, free_snapshot, kernel_string_slice};

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    struct Fixture {
        store: Arc<DynObjectStore>,
        kernel_engine: Arc<dyn Engine>,
        engine: Handle<SharedExternEngine>,
        snapshot: Handle<SharedSnapshot>,
        context: Handle<SharedWriteContext>,
        table_url: url::Url,
    }

    impl Fixture {
        async fn new(enabled: bool) -> TestResult<Self> {
            Self::for_table(enabled, "write_dv_updates").await
        }

        async fn for_table(enabled: bool, name: &str) -> TestResult<Self> {
            let (store, _, table_url) = engine_store_setup(name, None);
            let kernel_engine: Arc<dyn Engine> =
                Arc::new(DefaultEngineBuilder::new(store.clone()).build());
            let schema = schema_ref! { nullable "id": INTEGER };
            create_table(&table_url, schema, "ffi-dv-writer-test")
                .with_table_properties([(
                    "delta.enableDeletionVectors",
                    if enabled { "true" } else { "false" },
                )])
                .build(kernel_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
                .commit(kernel_engine.as_ref())?
                .unwrap_committed();
            let snapshot = Snapshot::builder_for(&table_url).build(kernel_engine.as_ref())?;
            let mut transaction = snapshot
                .transaction(Box::new(FileSystemCommitter::new()), kernel_engine.as_ref())?;
            let mut metadata = Vec::new();
            for (path, ids) in [
                ("first.parquet", vec![10, 20, 30, 40]),
                ("second.parquet", vec![50, 60]),
            ] {
                let batch = RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int32Array::from(ids)) as delta_kernel::arrow::array::ArrayRef,
                )])?;
                let bytes = record_batch_to_bytes_with_props(
                    &batch,
                    WriterProperties::builder()
                        .set_max_row_group_row_count(Some(2))
                        .build(),
                );
                let size = bytes.len() as i64;
                store
                    .put(
                        &Path::from_url_path(table_url.join(path)?.path())?,
                        bytes.into(),
                    )
                    .await?;
                metadata.push((path, size, 1_000_000, Some(batch.num_rows() as i64)));
            }
            transaction.add_files(create_add_files_metadata(
                transaction.add_files_schema(),
                metadata,
            )?);
            let snapshot = transaction
                .commit(kernel_engine.as_ref())?
                .unwrap_post_commit_snapshot();
            let context = snapshot
                .clone()
                .transaction(Box::new(FileSystemCommitter::new()), kernel_engine.as_ref())?
                .write_state()?
                .write_context_builder()
                .build()?;
            Ok(Self {
                store: store.clone(),
                kernel_engine,
                engine: engine_handle_for_store(store),
                snapshot: snapshot.into(),
                context: Arc::new(context).into(),
                table_url,
            })
        }

        fn prepare(
            &self,
            entries: &[FfiDeletionVectorUpdate],
        ) -> ExternResult<Handle<ExclusiveDvDescriptorMap>> {
            unsafe {
                write_deletion_vectors(
                    self.snapshot.shallow_copy(),
                    self.context.shallow_copy(),
                    FfiSlice::new_unsafe(entries),
                    self.engine.shallow_copy(),
                )
            }
        }

        fn dv_count(&self) -> usize {
            self.kernel_engine
                .storage_handler()
                .list_from(&self.table_url)
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .iter()
                .filter(|file| file.location.as_str().contains("deletion_vector_"))
                .count()
        }

        async fn original_bytes(&self) -> Vec<bytes::Bytes> {
            let mut files = Vec::new();
            for path in ["first.parquet", "second.parquet"] {
                files.push(
                    self.store
                        .get(
                            &Path::from_url_path(self.table_url.join(path).unwrap().path())
                                .unwrap(),
                        )
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                );
            }
            files
        }

        fn refresh(&mut self) -> TestResult<()> {
            let snapshot =
                Snapshot::builder_for(&self.table_url).build(self.kernel_engine.as_ref())?;
            let context = snapshot
                .clone()
                .transaction(
                    Box::new(FileSystemCommitter::new()),
                    self.kernel_engine.as_ref(),
                )?
                .write_state()?
                .write_context_builder()
                .build()?;
            unsafe {
                free_write_context(self.context.shallow_copy());
                free_snapshot(self.snapshot.shallow_copy());
            }
            self.snapshot = snapshot.into();
            self.context = Arc::new(context).into();
            Ok(())
        }

        async fn commit(
            &self,
            map: Handle<ExclusiveDvDescriptorMap>,
            new_rows: Option<(&str, Vec<i32>)>,
        ) -> TestResult<()> {
            let snapshot_version = unsafe { self.snapshot.as_ref() }.version();
            let mut transaction_handle = ok_or_panic(unsafe {
                transaction_from_snapshot(self.snapshot.shallow_copy(), self.engine.shallow_copy())
            });
            if let Some((path, ids)) = new_rows {
                let batch = RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int32Array::from(ids)) as delta_kernel::arrow::array::ArrayRef,
                )])?;
                let bytes = record_batch_to_bytes(&batch);
                let size = bytes.len() as i64;
                self.store
                    .put(
                        &Path::from_url_path(self.table_url.join(path)?.path())?,
                        bytes.into(),
                    )
                    .await?;
                let metadata = create_add_files_metadata(
                    unsafe { transaction_handle.as_ref() }.add_files_schema(),
                    vec![(path, size, 1_000_000, Some(batch.num_rows() as i64))],
                )?;
                unsafe { transaction_handle.as_mut() }.add_files(metadata);
            }
            let scan = ok_or_panic(unsafe {
                scan(
                    self.snapshot.shallow_copy(),
                    self.engine.shallow_copy(),
                    None,
                    None,
                )
            });
            let metadata = ok_or_panic(unsafe {
                scan_metadata_iter_init(self.engine.shallow_copy(), scan.shallow_copy())
            });
            let result = unsafe {
                transaction_update_deletion_vectors(
                    transaction_handle.shallow_copy(),
                    map,
                    metadata,
                    self.engine.shallow_copy(),
                )
            };
            unsafe { free_scan(scan) };
            ok_or_panic(result);
            let committed = match unsafe { commit(transaction_handle, self.engine.shallow_copy()) }
            {
                ExternResult::Ok(committed) => committed,
                ExternResult::Err(error) => {
                    return Err(unsafe { recover_error(error) }.message.into());
                }
            };
            assert_eq!(
                unsafe { committed_transaction_version(&committed) },
                snapshot_version + 1
            );
            unsafe { free_committed_transaction(committed) };
            Ok(())
        }

        fn visible_ids(&self) -> TestResult<Vec<i32>> {
            let snapshot =
                Snapshot::builder_for(&self.table_url).build(self.kernel_engine.as_ref())?;
            let mut ids = Vec::new();
            for batch in snapshot
                .scan_builder()
                .build()?
                .execute(self.kernel_engine.clone())?
            {
                let batch = into_record_batch(batch?);
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ids.extend(column.values().iter().copied());
            }
            ids.sort_unstable();
            Ok(ids)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            unsafe {
                free_write_context(self.context.shallow_copy());
                free_snapshot(self.snapshot.shallow_copy());
                free_engine(self.engine.shallow_copy());
            }
        }
    }

    unsafe fn entry(path: &str, rows: &[u64]) -> FfiDeletionVectorUpdate {
        FfiDeletionVectorUpdate {
            data_file_path: kernel_string_slice!(path),
            row_indexes: FfiSlice::new_unsafe(rows),
        }
    }

    #[tokio::test]
    async fn writes_multiple_files_and_preserves_existing_dvs_without_rewriting_parquet(
    ) -> TestResult<()> {
        let mut fixture = Fixture::new(true).await?;
        let before = fixture.original_bytes().await;
        let entries = unsafe {
            [
                entry("first.parquet", &[1, 2, 2]),
                entry("second.parquet", &[0]),
            ]
        };
        let map = ok_or_panic(fixture.prepare(&entries));
        assert_eq!(unsafe { map.as_ref() }.inner.len(), 2);
        assert_eq!(fixture.dv_count(), 2);
        assert_eq!(fixture.visible_ids()?, vec![10, 20, 30, 40, 50, 60]);
        fixture
            .commit(map, Some(("new.parquet", vec![20, 30, 70])))
            .await?;
        assert_eq!(fixture.visible_ids()?, vec![10, 20, 30, 40, 60, 70]);
        fixture.refresh()?;

        let entries = unsafe { [entry("first.parquet", &[0, 1])] };
        let map = ok_or_panic(fixture.prepare(&entries));
        assert_eq!(
            unsafe { map.as_ref() }.inner["first.parquet"].cardinality,
            3
        );
        fixture.commit(map, None).await?;
        assert_eq!(fixture.visible_ids()?, vec![20, 30, 40, 60, 70]);
        assert_eq!(fixture.original_bytes().await, before);
        Ok(())
    }

    #[rstest]
    #[case::missing_file("missing.parquet", &[0], "not active")]
    #[case::past_end("second.parquet", &[2], "physical row count")]
    #[case::max_index("second.parquet", &[u64::MAX], "physical row count")]
    #[case::empty_path("", &[0], "nonempty")]
    #[case::empty_indexes("second.parquet", &[], "nonempty")]
    #[tokio::test]
    async fn invalid_batch_does_not_upload_any_dv(
        #[case] path: &str,
        #[case] rows: &[u64],
        #[case] expected: &str,
    ) -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let entries = unsafe { [entry("first.parquet", &[1]), entry(path, rows)] };
        match fixture.prepare(&entries) {
            ExternResult::Err(error) => {
                let error = unsafe { recover_error(error) };
                assert!(error.message.contains(expected), "{}", error.message);
            }
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("invalid batch succeeded");
            }
        }
        assert_eq!(fixture.dv_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_file_paths_are_rejected_before_io() -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let entries = unsafe { [entry("first.parquet", &[1]), entry("first.parquet", &[2])] };
        let error = match fixture.prepare(&entries) {
            ExternResult::Err(error) => unsafe { recover_error(error) },
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("duplicate paths succeeded");
            }
        };
        assert!(error.message.contains("duplicate"));
        assert_eq!(fixture.dv_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_context_for_another_table_is_rejected_before_upload() -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let other = Fixture::for_table(true, "other_table").await?;
        let entries = unsafe { [entry("first.parquet", &[1])] };
        let result = unsafe {
            write_deletion_vectors(
                fixture.snapshot.shallow_copy(),
                other.context.shallow_copy(),
                FfiSlice::new_unsafe(&entries),
                fixture.engine.shallow_copy(),
            )
        };
        match result {
            ExternResult::Err(error) => {
                assert!(unsafe { recover_error(error) }
                    .message
                    .contains("different table roots"));
            }
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("mismatched table roots succeeded");
            }
        }
        assert_eq!(fixture.dv_count(), 0);
        assert_eq!(other.dv_count(), 0);
        Ok(())
    }

    #[rstest]
    #[case::invalid_utf8(true)]
    #[case::null_row_slice(false)]
    #[tokio::test]
    async fn malformed_nested_slices_fail_without_uploads(
        #[case] invalid_utf8: bool,
    ) -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let bytes = [0xffu8];
        let mut update = unsafe { entry("first.parquet", &[1]) };
        let expected = if invalid_utf8 {
            update.data_file_path = KernelStringSlice {
                ptr: bytes.as_ptr().cast(),
                len: bytes.len(),
            };
            "utf"
        } else {
            update.row_indexes = FfiSlice {
                ptr: ptr::null(),
                len: 1,
            };
            "null"
        };
        match fixture.prepare(&[update]) {
            ExternResult::Err(error) => {
                let error = unsafe { recover_error(error) };
                assert!(
                    error.message.to_lowercase().contains(expected),
                    "{}",
                    error.message
                );
            }
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("malformed input succeeded");
            }
        }
        assert_eq!(fixture.dv_count(), 0);
        // Errors borrow the input handles; the same fixture remains usable.
        let entries = unsafe { [entry("first.parquet", &[1])] };
        let map = ok_or_panic(fixture.prepare(&entries));
        unsafe { super::super::free_dv_descriptor_map(map) };
        Ok(())
    }

    #[rstest]
    #[case::unreadable(false)]
    #[case::wrong_cardinality(true)]
    #[tokio::test]
    async fn an_invalid_existing_dv_is_not_replaced_by_only_new_indexes(
        #[case] wrong_cardinality: bool,
    ) -> TestResult<()> {
        let mut fixture = Fixture::new(true).await?;
        let entries = unsafe { [entry("first.parquet", &[1])] };
        let mut map = ok_or_panic(fixture.prepare(&entries));
        if wrong_cardinality {
            unsafe { map.as_mut() }
                .inner
                .get_mut("first.parquet")
                .unwrap()
                .cardinality += 1;
        }
        let descriptor = unsafe { map.as_ref() }.inner["first.parquet"].clone();
        fixture.commit(map, None).await?;
        fixture.refresh()?;
        if !wrong_cardinality {
            let path = descriptor.absolute_path(&fixture.table_url)?.unwrap();
            fixture.kernel_engine.storage_handler().delete(&path)?;
        }
        let count = fixture.dv_count();

        let entries = unsafe { [entry("first.parquet", &[2])] };
        match fixture.prepare(&entries) {
            ExternResult::Err(error) => {
                let error = unsafe { recover_error(error) };
                if wrong_cardinality {
                    assert!(error.message.contains("cardinality"), "{}", error.message);
                }
            }
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("invalid previous DV was ignored");
            }
        }
        assert_eq!(fixture.dv_count(), count);
        Ok(())
    }

    #[tokio::test]
    async fn stale_preparation_conflicts_and_fresh_preparation_preserves_both_deletes(
    ) -> TestResult<()> {
        let mut fixture = Fixture::new(true).await?;
        let entries = unsafe { [entry("first.parquet", &[1])] };
        let stale = ok_or_panic(fixture.prepare(&entries));
        let entries = unsafe { [entry("first.parquet", &[2])] };
        let concurrent = ok_or_panic(fixture.prepare(&entries));
        fixture.commit(concurrent, None).await?;

        let error = fixture.commit(stale, None).await.unwrap_err();
        assert!(error.to_string().contains("conflict"), "{error}");
        assert_eq!(fixture.visible_ids()?, vec![10, 20, 40, 50, 60]);
        fixture.refresh()?;
        let entries = unsafe { [entry("first.parquet", &[1])] };
        let fresh = ok_or_panic(fixture.prepare(&entries));
        fixture.commit(fresh, None).await?;
        assert_eq!(fixture.visible_ids()?, vec![10, 40, 50, 60]);
        Ok(())
    }

    #[tokio::test]
    async fn filesystem_transaction_retains_its_snapshot_after_input_handles_are_released(
    ) -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let engine = engine_handle_for_store(fixture.store.clone());
        let snapshot: Handle<SharedSnapshot> = unsafe { fixture.snapshot.clone_as_arc() }.into();
        let table_root = fixture.table_url.clone();
        let transaction = ok_or_panic(unsafe {
            transaction_from_snapshot(snapshot.shallow_copy(), engine.shallow_copy())
        });
        unsafe { free_snapshot(snapshot) };
        drop(fixture);

        let state = unsafe { transaction.as_ref() }.write_state()?;
        let context = state.write_context_builder().build()?;
        assert_eq!(context.table_root_dir(), &table_root);
        assert_eq!(context.logical_data_schema().num_fields(), 1);
        unsafe {
            free_transaction(transaction);
            free_engine(engine);
        }
        Ok(())
    }

    #[test]
    fn missing_physical_row_counts_and_duplicate_active_paths_are_rejected() {
        let file = ScanFile {
            path: "first.parquet".to_string(),
            size: 1,
            modification_time: 0,
            stats: None,
            dv_info: Default::default(),
            transform: None,
            partition_values: HashMap::new(),
        };
        assert!(physical_row_count(&file)
            .unwrap_err()
            .to_string()
            .contains("numRecords"));
        let indexes: &[u64] = &[0];
        let mut requested = RequestedFiles {
            indexes: HashMap::from([("first.parquet", indexes)]),
            files: HashMap::new(),
            duplicate: None,
        };
        select_file(&mut requested, file.clone());
        select_file(&mut requested, file);
        assert_eq!(requested.duplicate.as_deref(), Some("first.parquet"));
    }

    #[tokio::test]
    async fn disabled_deletion_vectors_are_rejected() -> TestResult<()> {
        let fixture = Fixture::new(false).await?;
        let entries = unsafe { [entry("first.parquet", &[1])] };
        match fixture.prepare(&entries) {
            ExternResult::Err(error) => {
                assert!(unsafe { recover_error(error) }
                    .message
                    .contains("deletionVectors"));
            }
            ExternResult::Ok(map) => {
                unsafe { super::super::free_dv_descriptor_map(map) };
                panic!("disabled DV write succeeded");
            }
        }
        assert_eq!(fixture.dv_count(), 0);
        Ok(())
    }

    #[rstest]
    #[case::empty(false)]
    #[case::nonempty(true)]
    #[tokio::test]
    async fn null_update_pointer_is_valid_only_for_empty_batches(
        #[case] nonempty: bool,
    ) -> TestResult<()> {
        let fixture = Fixture::new(true).await?;
        let result = unsafe {
            write_deletion_vectors(
                fixture.snapshot.shallow_copy(),
                fixture.context.shallow_copy(),
                FfiSlice {
                    ptr: ptr::null(),
                    len: usize::from(nonempty),
                },
                fixture.engine.shallow_copy(),
            )
        };
        match result {
            ExternResult::Ok(map) => {
                assert!(!nonempty);
                assert!(unsafe { map.as_ref() }.inner.is_empty());
                unsafe { super::super::free_dv_descriptor_map(map) };
            }
            ExternResult::Err(error) => {
                assert!(nonempty);
                assert!(unsafe { recover_error(error) }.message.contains("null"));
            }
        }
        assert_eq!(fixture.dv_count(), 0);
        Ok(())
    }
}
