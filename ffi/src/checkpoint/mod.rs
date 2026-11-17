//! FFI checkpoint layout and metadata resolved through the engine's declarative executor.

use delta_kernel::KernelResult;
use delta_kernel::checkpoint::{CheckpointKind, CheckpointShape};

use crate::delta_types::{CheckpointBacking, FfiLastCheckpoint};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    KernelStringSlice, NullableCvoid, SharedExternEngine, SharedSnapshot, kernel_string_slice,
};

/// Complete borrowed checkpoint summary passed to [`CheckpointShapeVisitor`].
/// All nested storage expires when that visitor returns.
#[repr(C)]
pub struct FfiCheckpointShape {
    /// Schema JSON for the files containing Add and Remove actions. For manifest checkpoints this
    /// is the sidecar schema; JSON leaves use the canonical action read schema.
    pub leaf_schema: KernelStringSlice,
    /// Sidecar count for a manifest, otherwise the number of checkpoint parts.
    pub leaf_file_count: usize,
    /// Total bytes of top-level checkpoint parts and referenced sidecars.
    pub size_in_bytes: u64,
    /// Metadata for the selected checkpoint. Unknown action count is -1; optional fields and
    /// bounded hint lists may be absent.
    pub last_checkpoint: FfiLastCheckpoint,
    /// Whether file actions are inline or stored in sidecars, independently of the file name.
    pub kind: FfiCheckpointKind,
}

/// Where a checkpoint stores its Add and Remove actions, independently of its naming scheme.
///
/// cbindgen:prefix-with-name=true
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum FfiCheckpointKind {
    /// File actions are stored in the checkpoint parts themselves.
    Inline = 0,
    /// File actions are stored in sidecars referenced by the checkpoint.
    Manifest = 1,
}

/// Receives one summary, or null if the snapshot has no selected checkpoint.
/// Copy every retained value before returning.
pub type CheckpointShapeVisitor =
    extern "C" fn(context: NullableCvoid, checkpoint: *const FfiCheckpointShape);

impl From<CheckpointKind> for FfiCheckpointKind {
    fn from(kind: CheckpointKind) -> Self {
        match kind {
            CheckpointKind::Inline => Self::Inline,
            CheckpointKind::Manifest => Self::Manifest,
        }
    }
}

/// Resolves the selected checkpoint and invokes `visitor` once with its complete summary.
///
/// The first access may read checkpoint metadata and a Parquet footer. Successful resolution is
/// cached by the snapshot and reused by scan planning. Failures can be retried and do not invoke
/// the callback. Returns true after the callback, including when no checkpoint is selected.
/// `context` is passed unchanged and may be null.
///
/// # Safety
/// The snapshot and engine handles are borrowed and must remain valid for this call. The callback
/// must not consume either handle, retain borrowed pointers, or unwind across the FFI boundary.
/// The summary and all nested storage remain valid only until the callback returns.
/// The callback may re-enter with the same snapshot and engine handles: initialization has finished
/// and its lock is released before the callback runs.
/// During initialization, the plan executor must not call this function recursively on the same
/// snapshot: the recursive call would wait for the initialization already in progress.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_checkpoint_info(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: CheckpointShapeVisitor,
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = snapshot
        .reader()
        .get_checkpoint_info(engine.engine().as_ref())
        .and_then(|info| visit(info.as_deref(), context, visitor))
        .map(|()| true);
    unsafe { result.into_extern_result(&engine) }
}

fn visit(
    info: Option<&CheckpointShape>,
    context: NullableCvoid,
    visitor: CheckpointShapeVisitor,
) -> KernelResult<()> {
    let Some(info) = info else {
        visitor(context, std::ptr::null());
        return Ok(());
    };
    let schema = serde_json::to_string(info.leaf_schema())?;
    let checkpoint = CheckpointBacking::try_new(info.last_checkpoint())?;
    let value = FfiCheckpointShape {
        leaf_schema: kernel_string_slice!(schema),
        leaf_file_count: info.leaf_file_count(),
        size_in_bytes: info.size_in_bytes(),
        last_checkpoint: checkpoint.as_ffi(),
        kind: info.kind().into(),
    };
    visitor(context, &value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::Arc;

    use delta_kernel::engine::{DelegatingEngine, PanicEngine};
    use delta_kernel::last_checkpoint_hint::LastCheckpointHint;
    use delta_kernel::plans::{Operation, PlanExecutor, PlanResult};
    use delta_kernel::snapshot::{
        PublicationWatermark, Snapshot, SnapshotHint, SnapshotHintFreshness,
    };
    use delta_kernel::{FileMeta, KernelResult, LogPath};
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::ffi_test_utils::{TEST_ERROR_ALLOCATORS, ok_or_panic};
    use crate::{TryFromStringSlice, engine_to_handle, free_engine, free_snapshot};

    #[derive(Default)]
    struct Captured {
        visits: usize,
        info: Option<(String, usize, u64, LastCheckpointHint, FfiCheckpointKind)>,
    }

    extern "C" fn capture(context: NullableCvoid, info: *const FfiCheckpointShape) {
        let captured = unsafe { &mut *context.unwrap().as_ptr().cast::<Captured>() };
        captured.visits += 1;
        captured.info = unsafe { info.as_ref() }.map(|info| {
            (
                unsafe { String::try_from_slice(&info.leaf_schema) }.unwrap(),
                info.leaf_file_count,
                info.size_in_bytes,
                unsafe { info.last_checkpoint.try_to_kernel() }.unwrap(),
                info.kind,
            )
        });
    }

    #[rstest]
    #[case::no_checkpoint(false, false, false)]
    #[case::manifest(true, true, false)]
    #[case::leaf(true, false, false)]
    #[case::failed_resolution(true, true, true)]
    fn callback_copies_metadata_and_borrows_handles(
        #[case] checkpoint: bool,
        #[case] manifest: bool,
        #[case] invalid_size: bool,
    ) {
        // Given: borrowed handles for an absent, inline, manifest, or invalid checkpoint.
        let (snapshot, engine) = checkpoint_handles(checkpoint, manifest, invalid_size);
        let mut captured = Captured::default();
        for attempt in 1..=2 {
            // When: the same handles are used for repeated exports.
            let result = unsafe {
                snapshot_checkpoint_info(
                    snapshot.shallow_copy(),
                    engine.shallow_copy(),
                    Some(NonNull::from(&mut captured).cast()),
                    capture,
                )
            };

            // Then: success delivers one callback; a failure delivers none and can retry.
            if invalid_size {
                crate::ffi_test_utils::assert_extern_result_error_contains(
                    result,
                    crate::error::FFIKernelError::GenericError,
                    "size",
                );
                assert_eq!(captured.visits, 0);
            } else {
                assert!(ok_or_panic(result));
                assert_eq!(captured.visits, attempt);
            }
            assert_eq!(unsafe { snapshot.as_ref() }.version(), 0);
        }
        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }

        // Then: copied schema and nested hint values remain usable after both handles are freed.
        if checkpoint && !invalid_size {
            let (copied_schema, count, bytes, hint, kind) = captured.info.unwrap();
            assert_eq!(
                kind,
                if manifest {
                    FfiCheckpointKind::Manifest
                } else {
                    FfiCheckpointKind::Inline
                }
            );
            if manifest {
                assert_eq!(copied_schema, r#"{"type":"struct","fields":[]}"#);
            } else {
                let schema: delta_kernel::schema::StructType =
                    serde_json::from_str(&copied_schema).unwrap();
                assert!(schema.contains("add"));
            }
            assert_eq!((count, bytes), (1, if manifest { 30 } else { 17 }));
            assert_eq!(hint.size(), 7);
            let sidecars = hint.v2_checkpoint().unwrap().sidecar_files().unwrap();
            if manifest {
                assert_eq!(sidecars[0].path, "leaf.parquet");
            } else {
                assert!(sidecars.is_empty());
            }
        } else {
            assert!(captured.info.is_none());
        }
    }

    struct ReentrantContext {
        snapshot: Handle<SharedSnapshot>,
        engine: Handle<SharedExternEngine>,
        visits: usize,
    }

    extern "C" fn capture_null_context(context: NullableCvoid, info: *const FfiCheckpointShape) {
        assert!(context.is_none());
        assert!(!info.is_null());
        assert_eq!(unsafe { (*info).size_in_bytes }, 17);
    }

    extern "C" fn reenter(context: NullableCvoid, info: *const FfiCheckpointShape) {
        let state = unsafe { &mut *context.unwrap().as_ptr().cast::<ReentrantContext>() };
        state.visits += 1;
        assert!(!info.is_null());
        let result = unsafe {
            snapshot_checkpoint_info(
                state.snapshot.shallow_copy(),
                state.engine.shallow_copy(),
                None,
                capture_null_context,
            )
        };
        assert!(ok_or_panic(result));
        // Nested export must not invalidate the outer callback's borrowed summary.
        assert_eq!(unsafe { (*info).size_in_bytes }, 17);
    }

    #[test]
    fn callback_can_reenter_same_snapshot_with_null_context() {
        // Given: a callback that recursively exports from the same borrowed handles.
        let (snapshot, engine) = checkpoint_handles(true, false, false);
        let mut state = ReentrantContext {
            snapshot,
            engine,
            visits: 0,
        };

        // When: the outer export invokes it.
        let result = unsafe {
            snapshot_checkpoint_info(
                state.snapshot.shallow_copy(),
                state.engine.shallow_copy(),
                Some(NonNull::from(&mut state).cast()),
                reenter,
            )
        };

        // Then: nested export accepts null context and leaves the outer summary and handles valid.
        assert!(ok_or_panic(result));
        assert_eq!(state.visits, 1);
        assert_eq!(unsafe { state.snapshot.as_ref() }.version(), 0);
        unsafe {
            free_snapshot(state.snapshot);
            free_engine(state.engine);
        }
    }

    struct NoIoExecutor;

    impl PlanExecutor for NoIoExecutor {
        fn execute_op(&self, _op: Operation) -> KernelResult<PlanResult> {
            panic!("unexpected declarative execution")
        }
    }

    fn checkpoint_handles(
        checkpoint: bool,
        manifest: bool,
        invalid_size: bool,
    ) -> (Handle<SharedSnapshot>, Handle<SharedExternEngine>) {
        let engine = Arc::new(
            DelegatingEngine::new(Arc::new(PanicEngine)).with_plan_executor(Arc::new(NoIoExecutor)),
        );
        let schema = r#"{"type":"struct","fields":[]}"#;
        let filename = "00000000000000000000.checkpoint.11111111-1111-1111-1111-111111111111.json";
        let last_checkpoint = checkpoint.then(|| serde_json::from_value::<LastCheckpointHint>(
            serde_json::json!({
                "version": 0, "size": 7,
                "v2Checkpoint": {
                    "path": filename,
                    "sidecarFiles": if manifest {
                        vec![serde_json::json!({ "path": "leaf.parquet", "sizeInBytes": if invalid_size { -1 } else { 13 }, "modificationTime": 23 })]
                    } else {
                        vec![]
                    },
                    "nonFileActions": [{ "checkpointMetadata": { "version": 0, "tags": { "sidecarFileSchema": schema } } }]
                }
            })
        ).unwrap());
        let path = if checkpoint {
            filename
        } else {
            "00000000000000000000.json"
        };
        let hint = SnapshotHint::try_new(
            "memory:///checkpoint-info/",
            0,
            PublicationWatermark::PublishedThrough(0),
            vec![
                LogPath::try_new(FileMeta {
                    location: Url::parse(&format!("memory:///checkpoint-info/_delta_log/{path}"))
                        .unwrap(),
                    last_modified: 11,
                    size: 17,
                })
                .unwrap(),
            ],
            serde_json::from_value(
                serde_json::json!({ "minReaderVersion": 1, "minWriterVersion": 2 }),
            )
            .unwrap(),
            serde_json::from_value(serde_json::json!({
                "id": "checkpoint-info", "format": { "provider": "parquet", "options": {} },
                "schemaString": schema, "partitionColumns": [], "configuration": {}
            }))
            .unwrap(),
            last_checkpoint,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .unwrap();
        let snapshot = Snapshot::builder_for("memory:///checkpoint-info/")
            .with_snapshot_hint(hint)
            .build(engine.as_ref())
            .unwrap();
        let snapshot: Handle<SharedSnapshot> = snapshot.into();
        let engine = engine_to_handle(engine, TEST_ERROR_ALLOCATORS);
        (snapshot, engine)
    }
}
