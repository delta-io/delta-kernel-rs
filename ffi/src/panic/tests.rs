use std::panic::panic_any;
use std::sync::Arc;

use delta_kernel::{KernelError, KernelResult};
use delta_kernel_ffi_macros::ffi_export;
use rstest::rstest;

use super::catch_panic;
use crate::error::{ExternResult, FFIKernelError, IntoExternResult};
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error,
    error_only_engine_handle, ok_or_panic, recover_error,
};
use crate::{free_engine, get_snapshot_builder, kernel_string_slice, snapshot_builder_build};

struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic_any(Self);
    }
}

#[rstest]
#[case::literal(|| panic!("literal panic"), "literal panic")]
#[case::owned(|| panic_any("owned panic".to_owned()), "owned panic")]
#[case::opaque(|| panic_any(42_u32), "non-string panic payload")]
#[case::capacity(|| drop(Vec::<u8>::with_capacity(usize::MAX)), "capacity overflow")]
#[case::panicking_drop(|| panic_any(PanicOnDrop), "non-string panic payload")]
fn contains_panics(#[case] panic: fn(), #[case] expected: &str) {
    let allocation = Arc::new(());
    let weak = Arc::downgrade(&allocation);
    let line = line!() + 1;
    let result = catch_panic::<(), KernelError>(|| {
        let _allocation = allocation;
        panic();
        Ok(())
    });
    assert!(weak.upgrade().is_none());
    let result = unsafe { result.into_extern_result(&TEST_ERROR_ALLOCATORS) };
    let ExternResult::Err(error) = result else {
        panic!("expected panic error");
    };
    let error = unsafe { recover_error(error) };
    assert_eq!(error.etype, FFIKernelError::InternalError);
    assert!(error.message.contains(&format!("{}:{line}:", file!())));
    assert!(error.message.contains(expected));
    assert_eq!(catch_panic::<_, KernelError>(|| Ok(42)).unwrap(), 42);
}

#[rstest]
#[case::success(Ok(42))]
#[case::ordinary_error(Err(KernelError::MissingVersion(7)))]
fn preserves_results(#[case] input: KernelResult<usize>) {
    let expected = input.as_ref().copied().map_err(ToString::to_string);
    let result = catch_panic(|| input);
    let result = unsafe { result.into_extern_result(&TEST_ERROR_ALLOCATORS) };
    match expected {
        Ok(value) => assert_eq!(ok_or_panic(result), value),
        Err(message) => assert_error(result, FFIKernelError::MissingVersionError, &message),
    }
}

const EXPORT_LINE: u32 = line!() + 3;

#[ffi_export(error_allocator = &TEST_ERROR_ALLOCATORS)]
unsafe extern "C" fn macro_result(outcome: u8) -> ExternResult<usize> {
    match outcome {
        0 => Ok(42),
        1 => Err(KernelError::MissingVersion(7).into()),
        2 => Err(delta_kernel::Error::from(
            delta_kernel::DeltaError::for_test(
                delta_kernel::DeltaErrorCondition::DeltaVersionsNotContiguous,
                &["10,12", "10", "12", "12"],
                None,
            )
            .unwrap(),
        )),
        _ => panic!("macro panic"),
    }
}

#[rstest]
#[case::success(0)]
#[case::kernel_error(1)]
#[case::delta_error(2)]
#[case::panic(3)]
fn macro_preserves_error_channels_and_location(#[case] outcome: u8) {
    let result = unsafe { macro_result(outcome) };
    if outcome == 0 {
        assert_eq!(ok_or_panic(result), 42);
        return;
    }
    let ExternResult::Err(error) = result else {
        panic!("expected error");
    };
    let error = unsafe { recover_error(error) };
    match outcome {
        1 => assert_eq!(error.etype, FFIKernelError::MissingVersionError),
        2 => assert_eq!(
            error.condition.as_deref(),
            Some("DELTA_VERSIONS_NOT_CONTIGUOUS")
        ),
        _ => {
            assert_eq!(error.etype, FFIKernelError::InternalError);
            assert!(
                error
                    .message
                    .contains(&format!("{}:{EXPORT_LINE}:", file!())),
                "{}",
                error.message
            );
            assert!(error.message.contains("macro panic"));
        }
    }
}

#[test]
fn snapshot_builder_keeps_allocator_alive_during_unwind() {
    let root = "memory:///";
    let engine = error_only_engine_handle();
    let weak = Arc::downgrade(&unsafe { engine.clone_as_arc() });
    let builder = ok_or_panic(unsafe {
        get_snapshot_builder(kernel_string_slice!(root), engine.shallow_copy())
    });
    unsafe { free_engine(engine) };
    assert_eq!(weak.strong_count(), 1);
    let result = unsafe { snapshot_builder_build(builder) };
    assert!(weak.upgrade().is_none());
    assert_error(
        result,
        FFIKernelError::InternalError,
        "error-only test engine",
    );
}

#[cfg(feature = "declarative-plans")]
mod plan_based {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use delta_kernel::Engine;
    use delta_kernel::engine::plans::PlanBasedEngine;
    use delta_kernel::engine::sync::plan::SyncPlanExecutor;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::plans::proto::operation as proto;
    use prost::Message;
    use rstest::rstest;
    use test_utils::{TestAction, actions_to_string, add_commit};

    use crate::error::{AllocateError, FFIKernelError};
    use crate::ffi_test_utils::{
        TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error, ok_or_panic,
    };
    use crate::handle::Handle;
    use crate::scan::{
        free_scan, scan_builder, scan_builder_build, scan_declarative_metadata_plan,
    };
    use crate::transaction::{
        KernelUpdateTableOperation, free_update_table_txn, new_update_table_txn_builder,
        update_table_txn_builder_build, update_table_txn_commit,
    };
    use crate::{
        ExternEngine, OptionalValue, SharedExternEngine, free_engine, free_snapshot,
        get_snapshot_builder, kernel_string_slice, snapshot_builder_build,
    };

    struct PanicOnceEngine {
        inner: Arc<dyn Engine>,
        fail_next: AtomicBool,
    }

    impl ExternEngine for PanicOnceEngine {
        fn engine(&self) -> Arc<dyn Engine> {
            assert!(!self.fail_next.swap(false, Ordering::SeqCst));
            self.inner.clone()
        }

        fn error_allocator(&self) -> &dyn AllocateError {
            &TEST_ERROR_ALLOCATORS
        }
    }

    #[rstest]
    #[case::snapshot(0, "builder.rs:")]
    #[case::scan_plan(1, "scan.rs:")]
    #[case::transaction_build(2, "update_table.rs:")]
    #[case::transaction_commit(3, "update_table.rs:")]
    #[tokio::test]
    async fn planning_succeeds_after_panic(#[case] panic_stage: u8, #[case] expected: &str) {
        let store = Arc::new(InMemory::new());
        let root = "memory:///";
        let actions = vec![
            TestAction::Metadata,
            TestAction::Add("part-1.parquet".into()),
        ];
        let commit = actions_to_string(actions);
        add_commit(root, store.as_ref(), 0, commit).await.unwrap();
        let external = Arc::new(PanicOnceEngine {
            inner: Arc::new(PlanBasedEngine::new(
                None,
                Arc::new(SyncPlanExecutor::new(Some(store))),
            )),
            fail_next: AtomicBool::new(panic_stage == 0),
        });
        let engine: Handle<SharedExternEngine> = (external.clone() as Arc<dyn ExternEngine>).into();
        let baseline = Arc::strong_count(&external);
        let build = || unsafe {
            snapshot_builder_build(ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(root),
                engine.shallow_copy(),
            )))
        };
        if panic_stage == 0 {
            assert_error(build(), FFIKernelError::InternalError, expected);
        }
        let snapshot = ok_or_panic(build());
        assert_eq!(Arc::strong_count(&external), baseline);
        assert_eq!(unsafe { snapshot.as_ref() }.version(), 0);
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let scan = ok_or_panic(unsafe { scan_builder_build(builder, engine.shallow_copy()) });
        let plan = || unsafe {
            scan_declarative_metadata_plan(scan.shallow_copy(), engine.shallow_copy())
        };
        if panic_stage == 1 {
            external.fail_next.store(true, Ordering::SeqCst);
            assert_error(plan(), FFIKernelError::InternalError, expected);
        }
        let OptionalValue::Some(bytes) = ok_or_panic(plan()) else {
            panic!("expected plan");
        };
        let bytes = unsafe { bytes.into_vec() };
        let op = proto::Operation::decode(bytes.as_slice()).unwrap();
        assert!(matches!(op.op, Some(proto::operation::Op::QueryPlan(_))));
        if panic_stage >= 2 {
            let engine_info = "panic-test/1.0";
            let transaction = || unsafe {
                let builder = ok_or_panic(new_update_table_txn_builder(
                    snapshot.shallow_copy(),
                    KernelUpdateTableOperation::Write,
                    kernel_string_slice!(engine_info),
                    true,
                    engine.shallow_copy(),
                ));
                update_table_txn_builder_build(builder, engine.shallow_copy())
            };
            if panic_stage == 2 {
                external.fail_next.store(true, Ordering::SeqCst);
                assert_error(transaction(), FFIKernelError::InternalError, expected);
            } else {
                let txn = ok_or_panic(transaction());
                external.fail_next.store(true, Ordering::SeqCst);
                assert_error(
                    unsafe { update_table_txn_commit(txn, engine.shallow_copy()) },
                    FFIKernelError::InternalError,
                    expected,
                );
            }
            unsafe { free_update_table_txn(ok_or_panic(transaction())) };
        }
        unsafe {
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
        assert_eq!(Arc::strong_count(&external), 1);
    }
}
