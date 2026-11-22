use std::sync::Arc;
use std::thread;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::LimitedBytes;
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::schema::{DataType, MetadataValue, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use rstest::rstest;
use test_utils::{TestAction, actions_to_string};

use super::*;
use crate::error::{ExternResult, FFIKernelError};
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, allocate_bytes, assert_extern_result_error_with_message, ok_or_panic,
    recover_bytes,
};
use crate::scan::{
    SharedScanMetadata, free_scan, free_scan_metadata, free_scan_metadata_iter, scan_builder,
    scan_builder_build, scan_metadata_iter_init, scan_metadata_next,
};
use crate::snapshot::snapshot_builder_with_memory_budget;
use crate::transaction::write_context::{
    free_write_state, update_table_txn_write_state, write_state_decode_with_memory_budget,
    write_state_encode,
};
use crate::transaction::{
    KernelUpdateTableOperation, committed_transaction_post_commit_snapshot,
    free_committed_transaction, new_update_table_txn_builder, update_table_txn_builder_build,
    update_table_txn_commit,
};
use crate::{
    SharedExternEngine, SharedSnapshot, engine_to_handle, free_engine, free_snapshot,
    get_snapshot_builder, get_snapshot_builder_from, kernel_bytes_slice, kernel_string_slice,
    snapshot_builder_build,
};

const ROOT: &str = "memory:///";
const ENGINE_INFO: &str = "test";

fn usage(budget: &Handle<SharedMemoryBudget>) -> FfiMemoryUsage {
    let mut usage = FfiMemoryUsage::default();
    assert!(unsafe { memory_budget_usage(budget.shallow_copy(), &mut usage) });
    usage
}

#[test]
fn budget_handles_and_allocations_outlive_the_creating_thread() {
    let budget = kernel_memory_budget_new(1024);
    assert!(!unsafe { memory_budget_usage(budget.shallow_copy(), std::ptr::null_mut()) });
    let observer = unsafe { clone_memory_budget(budget.shallow_copy()) };
    let mut bytes = LimitedBytes::new_in(unsafe { budget.as_ref() }.allocator());
    bytes.try_extend_from_slice(&[1; 256]).unwrap();
    unsafe { free_memory_budget(budget) };
    thread::spawn(move || {
        assert!(bytes.try_extend_from_slice(&[2; 2048]).is_err());
        assert_eq!(bytes.len(), 256);
        drop(bytes);
    })
    .join()
    .unwrap();
    let usage = usage(&observer);
    assert_eq!(usage.reserved, 0);
    assert!(usage.peak <= usage.limit);
    assert!(usage.rejected > 0);
    unsafe { free_memory_budget(observer) };
}

#[cfg(feature = "declarative-plans")]
#[test]
fn ffi_byte_destructor_releases_the_original_allocator_on_another_thread() {
    let budget = MemoryBudget::new(4096);
    let bytes = bytes::Bytes::from_owner(LimitedBytes::try_copy(&budget, &[7; 1024]).unwrap());
    let retained = bytes.clone();
    let ffi_bytes = crate::KernelOwnedBytes::from(bytes);
    thread::spawn(move || unsafe { crate::free_kernel_bytes(ffi_bytes) })
        .join()
        .unwrap();
    assert_eq!(budget.usage().reserved, 1024);
    drop(retained);
    assert_eq!(budget.usage().reserved, 0);

    let bytes = bytes::Bytes::from_owner(LimitedBytes::try_copy(&budget, &[8; 512]).unwrap());
    let ffi_bytes = crate::KernelOwnedBytes::from(bytes);
    assert_eq!(budget.usage().reserved, 512);
    thread::spawn(move || unsafe { crate::free_kernel_bytes(ffi_bytes) })
        .join()
        .unwrap();
    assert_eq!(budget.usage().reserved, 0);
}

async fn fixture() -> (Handle<SharedExternEngine>, Handle<SharedSnapshot>) {
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let schema = Arc::new(
        StructType::try_new([StructField::nullable("id", DataType::INTEGER)
            .with_metadata([("comment", MetadataValue::String("retained comment".into()))])])
        .unwrap(),
    );
    create_table("memory:///", schema, "test", true)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap()
        .unwrap_committed();
    test_utils::add_commit(
        "memory:///",
        store.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("a.parquet".into())]),
    )
    .await
    .unwrap();
    let engine = engine_to_handle(engine, TEST_ERROR_ALLOCATORS);
    let snapshot = unsafe {
        ok_or_panic(snapshot_builder_build(ok_or_panic(get_snapshot_builder(
            kernel_string_slice!(ROOT),
            engine.shallow_copy(),
        ))))
    };
    (engine, snapshot)
}

extern "C" fn release_batch(_: crate::NullableCvoid, batch: Handle<SharedScanMetadata>) {
    unsafe { free_scan_metadata(batch) };
}

#[rstest]
#[tokio::test]
async fn ffi_snapshot_and_deferred_scan_keep_the_budget_across_calls_and_threads(
    #[values(false, true)] scan_rejects: bool,
) {
    let (engine, original) = fixture().await;
    let budget = kernel_memory_budget_new(1024 * 1024);
    let observer = unsafe { clone_memory_budget(budget.shallow_copy()) };
    let builder = unsafe {
        ok_or_panic(get_snapshot_builder(
            kernel_string_slice!(ROOT),
            engine.shallow_copy(),
        ))
    };
    let builder = unsafe { snapshot_builder_with_memory_budget(builder, budget.shallow_copy()) };
    unsafe { free_memory_budget(budget) };
    let worker_engine = unsafe { engine.clone_handle() };
    let snapshot = thread::spawn(move || unsafe {
        let snapshot = ok_or_panic(snapshot_builder_build(builder));
        free_engine(worker_engine);
        snapshot
    })
    .join()
    .unwrap();
    assert!(usage(&observer).reserved > 0);
    // A no-op refresh must preserve ownership too.
    let builder = unsafe {
        ok_or_panic(get_snapshot_builder_from(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    let refreshed = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let scan_budget = kernel_memory_budget_new(0);
    let builder = unsafe { scan_builder(refreshed.shallow_copy()) };
    let builder = if scan_rejects {
        unsafe { scan_builder_with_memory_budget(builder, scan_budget.shallow_copy()) }
    } else {
        builder
    };
    let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
    let iter = unsafe {
        ok_or_panic(scan_metadata_iter_init(
            engine.shallow_copy(),
            scan.shallow_copy(),
        ))
    };
    unsafe {
        free_scan(scan);
        free_snapshot(snapshot);
        free_snapshot(refreshed);
        free_snapshot(original);
        free_engine(engine);
    }
    let count = thread::spawn(move || {
        let mut count = 0;
        loop {
            match unsafe { scan_metadata_next(iter.shallow_copy(), None, release_batch) } {
                ExternResult::Ok(true) => count += 1,
                ExternResult::Ok(false) => break,
                failure => {
                    assert!(scan_rejects);
                    assert_extern_result_error_with_message(
                        failure,
                        FFIKernelError::MemoryBudgetError,
                        None,
                    );
                    break;
                }
            }
        }
        unsafe { free_scan_metadata_iter(iter) };
        count
    })
    .join()
    .unwrap();
    assert_eq!(count == 0, scan_rejects);
    assert_eq!(usage(&observer).reserved, 0);
    assert!(usage(&observer).peak <= usage(&observer).limit);
    assert_eq!(usage(&scan_budget).rejected > 0, scan_rejects);
    unsafe {
        free_memory_budget(observer);
        free_memory_budget(scan_budget);
    }
}

#[tokio::test]
async fn ffi_snapshot_exhaustion_has_a_distinct_error_and_releases_reservations() {
    let (engine, snapshot) = fixture().await;
    let budget = kernel_memory_budget_new(0);
    let builder = unsafe {
        ok_or_panic(get_snapshot_builder(
            kernel_string_slice!(ROOT),
            engine.shallow_copy(),
        ))
    };
    let builder = unsafe { snapshot_builder_with_memory_budget(builder, budget.shallow_copy()) };
    assert_extern_result_error_with_message(
        unsafe { snapshot_builder_build(builder) },
        FFIKernelError::MemoryBudgetError,
        None,
    );
    assert_eq!(usage(&budget).reserved, 0);
    assert!(usage(&budget).rejected > 0);
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
        free_memory_budget(budget);
    }
}

#[tokio::test]
async fn ffi_transaction_state_encode_decode_and_commit_retain_explicit_owners() {
    let (engine, snapshot) = fixture().await;
    let budget = kernel_memory_budget_new(1024 * 1024);
    let builder = unsafe {
        ok_or_panic(new_update_table_txn_builder(
            snapshot.shallow_copy(),
            KernelUpdateTableOperation::Write,
            kernel_string_slice!(ENGINE_INFO),
            true,
            engine.shallow_copy(),
        ))
    };
    let builder =
        unsafe { update_table_txn_builder_with_memory_budget(builder, budget.shallow_copy()) };
    let txn = unsafe {
        ok_or_panic(update_table_txn_builder_build(
            builder,
            engine.shallow_copy(),
        ))
    };
    let state = unsafe {
        ok_or_panic(update_table_txn_write_state(
            txn.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    let bytes = unsafe {
        ok_or_panic(write_state_encode(
            state.shallow_copy(),
            allocate_bytes,
            engine.shallow_copy(),
        ))
    }
    .unwrap();
    let bytes = recover_bytes(bytes);
    let zero = kernel_memory_budget_new(0);
    assert_extern_result_error_with_message(
        unsafe {
            write_state_decode_with_memory_budget(
                kernel_bytes_slice!(bytes),
                zero.shallow_copy(),
                engine.shallow_copy(),
            )
        },
        FFIKernelError::MemoryBudgetError,
        None,
    );
    let decoded = unsafe {
        ok_or_panic(write_state_decode_with_memory_budget(
            kernel_bytes_slice!(bytes),
            budget.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    let result = unsafe { ok_or_panic(update_table_txn_commit(txn, engine.shallow_copy())) };
    let post = unsafe { committed_transaction_post_commit_snapshot(&result) };
    unsafe {
        free_committed_transaction(result);
        free_write_state(state);
        free_write_state(decoded);
    }
    if let crate::OptionalValue::Some(post) = post {
        assert!(unsafe { post.as_ref() }.memory_allocator().is_some());
        unsafe { free_snapshot(post) };
    } else {
        panic!("missing post-commit snapshot");
    }
    // InMemory retains the committed Bytes owner until its backing store is dropped.
    assert!(usage(&budget).reserved > 0);
    assert!(usage(&budget).peak > 0);
    assert!(usage(&budget).peak <= usage(&budget).limit);
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
    assert_eq!(usage(&budget).reserved, 0);
    unsafe {
        free_memory_budget(zero);
        free_memory_budget(budget);
    }
}
