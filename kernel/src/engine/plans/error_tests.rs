use std::error::Error as _;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use rstest::rstest;
use url::Url;

use super::PlanBasedEngine;
use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::engine::sync::SyncEngine;
use crate::error::{delta_errors, ErrorContext};
use crate::metrics::MeteredDeltaEngine;
use crate::object_store::memory::InMemory;
use crate::plans::{IoOperation, Operation, PlanExecutor, PlanResult};
use crate::schema::schema_ref;
use crate::{
    DeltaResult, Engine, EngineData, EngineError, EngineResult, Error, FileMeta,
    FilteredEngineData, KernelError,
};

#[derive(Clone, Copy, Debug)]
enum FailureKind {
    Missing,
    Exists,
    Cancelled,
    External,
}

#[derive(Debug)]
struct FailingExecutor {
    kind: FailureKind,
    legacy: bool,
    lazy: bool,
}

impl PlanExecutor for FailingExecutor {
    fn execute_op(&self, operation: Operation) -> DeltaResult<PlanResult> {
        let error = if self.legacy {
            let error = match self.kind {
                FailureKind::Missing => KernelError::file_not_found("test"),
                FailureKind::Exists => KernelError::FileAlreadyExists("test".into()),
                FailureKind::Cancelled => KernelError::Cancelled,
                FailureKind::External => KernelError::missing_data("test"),
            };
            error
                .with_context(ErrorContext::Operation("legacy executor"))
                .into()
        } else {
            match self.kind {
                FailureKind::Missing => EngineError::FileNotFound {
                    path: "test".into(),
                    source: Some(Box::new(io::Error::other("missing source"))),
                },
                FailureKind::Exists => EngineError::FileAlreadyExists {
                    path: "test".into(),
                    source: Some(Box::new(io::Error::other("exists source"))),
                },
                FailureKind::Cancelled => EngineError::Cancelled,
                FailureKind::External => EngineError::external(io::Error::other("test")),
            }
            .into()
        };
        if !self.lazy {
            return Err(error);
        }
        match operation {
            Operation::QueryPlan(_) => Ok(PlanResult::Data(Box::new(std::iter::once(Err(error))))),
            Operation::IoOperation(
                IoOperation::FileListing { .. } | IoOperation::HeadFile { .. },
            ) => Ok(PlanResult::FileMeta(Box::new(std::iter::once(Err(error))))),
            Operation::IoOperation(IoOperation::ReadBytes { .. }) => {
                Ok(PlanResult::Bytes(Box::new(std::iter::once(Err(error)))))
            }
            _ => Err(error),
        }
    }
}

#[derive(Debug)]
struct WrongOutputExecutor;

impl PlanExecutor for WrongOutputExecutor {
    fn execute_op(&self, operation: Operation) -> DeltaResult<PlanResult> {
        match operation {
            Operation::QueryPlan(_) => Ok(PlanResult::Unit),
            _ => Ok(PlanResult::Data(Box::new(std::iter::empty()))),
        }
    }
}

#[rstest]
fn plan_handlers_preserve_eager_and_lazy_error_classification(
    #[values(
        FailureKind::Missing,
        FailureKind::Exists,
        FailureKind::Cancelled,
        FailureKind::External
    )]
    kind: FailureKind,
    #[values(false, true)] legacy: bool,
    #[values(false, true)] lazy: bool,
    #[values("list", "read", "head", "json", "parquet", "put", "copy", "footer")] operation: &str,
) {
    let engine = PlanBasedEngine::new(None, Arc::new(FailingExecutor { kind, legacy, lazy }));
    let error = invoke_handler(&engine, operation).unwrap_err();
    match kind {
        FailureKind::Missing => {
            assert!(matches!(&error, EngineError::FileNotFound { path, .. } if path == "test"));
        }
        FailureKind::Exists => {
            assert!(
                matches!(&error, EngineError::FileAlreadyExists { path, .. } if path == "test")
            );
        }
        FailureKind::Cancelled => assert!(matches!(&error, EngineError::Cancelled)),
        FailureKind::External => {
            assert!(matches!(&error, EngineError::External { .. }));
        }
    }
    if !matches!(kind, FailureKind::Cancelled) {
        if legacy {
            assert!(error.source().unwrap().is::<KernelError>());
        } else {
            assert!(error.source().unwrap().is::<io::Error>());
        }
    }
}

#[rstest]
fn plan_handlers_classify_wrong_output_as_engine_error(
    #[values("list", "read", "head", "json", "parquet", "put", "copy", "footer")] operation: &str,
) {
    let engine = PlanBasedEngine::new(None, Arc::new(WrongOutputExecutor));
    let error = invoke_handler(&engine, operation).unwrap_err();
    assert!(matches!(&error, EngineError::External { .. }));
    assert!(matches!(
        error.source().unwrap().downcast_ref::<KernelError>(),
        Some(KernelError::PlanResultTypeMismatch { .. })
    ));
}

#[rstest]
fn writers_preserve_upstream_error_provenance(
    #[values(false, true)] plan_backed: bool,
    #[values(false, true)] metered: bool,
    #[values(false, true)] parquet: bool,
    #[values(false, true)] after_batch: bool,
    #[values("delta", "kernel", "engine")] origin: &str,
) {
    let store = Arc::new(InMemory::new());
    let mut engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(store.clone()));
    if plan_backed {
        engine = Arc::new(PlanBasedEngine::new(
            Some(engine),
            Arc::new(SyncPlanExecutor::new(Some(store))),
        ));
    }
    if metered {
        engine = Arc::new(MeteredDeltaEngine::new(engine));
    }
    let upstream = match origin {
        "delta" => delta_errors::table_not_found("test"),
        "kernel" => KernelError::missing_data("upstream").into(),
        "engine" => EngineError::external(io::Error::other("upstream")).into(),
        _ => unreachable!(),
    };
    let expected_message = upstream.to_string();
    let mut batches: Vec<DeltaResult<Box<dyn EngineData>>> = Vec::new();
    if after_batch {
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        batches.push(Ok(Box::new(ArrowEngineData::new(batch))));
    }
    batches.push(Err(upstream));
    let location = Url::parse("memory:///output").unwrap();
    let error = if parquet {
        engine
            .parquet_handler()
            .write_parquet_file(location.clone(), Box::new(batches.into_iter()))
            .unwrap_err()
    } else {
        engine
            .json_handler()
            .write_json_file(
                &location,
                Box::new(
                    batches
                        .into_iter()
                        .map(|batch| batch.map(FilteredEngineData::with_all_rows_selected)),
                ),
                false,
            )
            .unwrap_err()
    };
    assert_eq!(error.to_string(), expected_message);
    match origin {
        "delta" => assert!(matches!(error, Error::Delta(_))),
        "kernel" => assert!(matches!(error, Error::Kernel(_))),
        "engine" => {
            let Error::Engine(error) = error else {
                panic!("expected upstream engine error");
            };
            assert!(error.source().unwrap().is::<io::Error>());
        }
        _ => unreachable!(),
    }
    assert!(matches!(
        engine.storage_handler().head(&location),
        Err(EngineError::FileNotFound { .. })
    ));
}

fn invoke_handler(engine: &dyn Engine, operation: &str) -> EngineResult<()> {
    let location = Url::parse("memory:///test").unwrap();
    let file = FileMeta {
        location: location.clone(),
        last_modified: 0,
        size: 1,
    };
    let schema = schema_ref! { nullable "value": LONG };
    match operation {
        "list" => engine
            .storage_handler()
            .list_from(&location)?
            .next()
            .unwrap()
            .map(|_| ()),
        "read" => engine
            .storage_handler()
            .read_files(vec![(location, None)])?
            .next()
            .unwrap()
            .map(|_| ()),
        "head" => engine.storage_handler().head(&location).map(|_| ()),
        "json" => engine
            .json_handler()
            .read_json_files(&[file], schema, None)?
            .next()
            .unwrap()
            .map(|_| ()),
        "parquet" => engine
            .parquet_handler()
            .read_parquet_files(&[file], schema, None)?
            .next()
            .unwrap()
            .map(|_| ()),
        "put" => engine.storage_handler().put(&location, Bytes::new(), false),
        "copy" => engine.storage_handler().copy_atomic(&location, &location),
        "footer" => engine
            .parquet_handler()
            .read_parquet_footer(&file)
            .map(|_| ()),
        _ => unreachable!(),
    }
}
