use std::panic::panic_any;
use std::sync::Arc;

use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;

use super::{catch, catch_panic_into_result};
use crate::error::FFIKernelError;
use crate::ffi_test_utils::{
    TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains as assert_error, ok_or_panic,
};

#[rstest]
#[case::literal(|| panic!("literal panic"), "literal panic")]
#[case::owned(|| panic_any("owned panic".to_owned()), "owned panic")]
#[case::opaque(|| panic_any(42_u32), "non-string panic payload")]
#[case::capacity(|| drop(Vec::<u8>::with_capacity(usize::MAX)), "capacity overflow")]
fn contains_panics(#[case] panic: fn(), #[case] expected: &str) {
    let allocation = Arc::new(());
    let weak = Arc::downgrade(&allocation);
    let error = catch::<(), KernelError>("probe", || {
        let _allocation = allocation;
        panic();
        Ok(())
    })
    .unwrap_err();
    assert!(weak.upgrade().is_none());
    let message = error.to_string();
    assert!(matches!(error, KernelError::InternalError(_)));
    assert!(message.contains("Rust panic in probe: ") && message.contains(expected));
    assert_eq!(catch::<_, KernelError>("next call", || Ok(42)).unwrap(), 42);
}

#[rstest]
#[case::success(Ok(42))]
#[case::ordinary_error(Err(KernelError::MissingVersion(7)))]
fn preserves_results(#[case] input: KernelResult<usize>) {
    let expected = input.as_ref().copied().map_err(ToString::to_string);
    let result = unsafe { catch_panic_into_result(&TEST_ERROR_ALLOCATORS, || input) };
    match expected {
        Ok(value) => assert_eq!(ok_or_panic(result), value),
        Err(message) => assert_error(result, FFIKernelError::MissingVersionError, &message),
    }
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

        unsafe {
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
        assert_eq!(Arc::strong_count(&external), 1);
    }
}
