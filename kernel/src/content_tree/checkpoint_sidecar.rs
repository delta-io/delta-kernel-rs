//! Reads the actions an adaptiveMetadata (AMT) `checkpoint` action spills into sidecar files.

use std::iter;

use super::action_projector::ActionProjector;
use crate::actions::Sidecar;
use crate::log_replay::ActionsBatch;
use crate::path::LogRoot;
use crate::schema::SchemaRef;
use crate::{Engine, KernelResult, KernelResultIterator};

/// Reads the `action` rows of `sidecars` as checkpoint batches (`is_log_batch = false`) matching
/// `read_schema`.
///
/// Each sidecar is a Parquet file that, like a V2 checkpoint sidecar, holds one action per row in
/// a top-level `action` column described by `source_schema`, and whose path is relative to the
/// `_delta_log/_sidecars/` directory under `log_root` (see [`Sidecar::to_filemeta`]).
pub(super) fn read_sidecar_actions(
    engine: &dyn Engine,
    log_root: &LogRoot,
    sidecars: &[Sidecar],
    action: &str,
    source_schema: SchemaRef,
    read_schema: SchemaRef,
) -> KernelResult<Box<dyn KernelResultIterator<ActionsBatch>>> {
    if sidecars.is_empty() {
        return Ok(Box::new(iter::empty()));
    }
    let files = sidecars
        .iter()
        .map(|sidecar| sidecar.to_filemeta(log_root.log_root()))
        .collect::<KernelResult<Vec<_>>>()?;
    let projector = ActionProjector::try_new(engine, action, source_schema.clone(), read_schema)?;
    let batches = engine
        .parquet_handler()
        .read_parquet_files(&files, source_schema, None)?;
    Ok(Box::new(batches.map(move |batch| {
        Ok(ActionsBatch::new(
            projector.project(batch?.as_ref())?,
            false,
        ))
    })))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::actions::{
        DomainMetadata, SetTransaction, DOMAIN_METADATA_NAME, LOG_DOMAIN_METADATA_SCHEMA,
        LOG_TXN_SCHEMA, SET_TRANSACTION_NAME,
    };
    use crate::arrow::array::{AsArray as _, RecordBatch};
    use crate::cancellation::CancellationTokenRef;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::engine::CancellableEngine;
    use crate::expressions::Scalar;
    use crate::object_store::memory::InMemory;
    use crate::schema::StructType;
    use crate::unit_test_utils::adaptive_metadata_fixtures::write_sidecar;
    use crate::unit_test_utils::{assert_result_error_with_message, TestCancellationToken};
    use crate::KernelError;

    fn table_root() -> Url {
        Url::parse("memory:///").unwrap()
    }

    fn log_root() -> LogRoot {
        LogRoot::new(table_root()).unwrap()
    }

    fn txn(app_id: &str) -> Scalar {
        SetTransaction::new(app_id.to_string(), 1, None).into()
    }

    fn domain_metadata(domain: &str) -> Scalar {
        DomainMetadata::new(domain.to_string(), "cfg".to_string()).into()
    }

    /// The `<action>.<key>` values of `batch`.
    fn keys(batch: &RecordBatch, action: &str, key: &str) -> Vec<String> {
        let action = batch.column_by_name(action).unwrap().as_struct();
        let keys = action.column_by_name(key).unwrap().as_string::<i32>();
        keys.iter().flatten().map(str::to_string).collect()
    }

    #[rstest]
    #[case::txn(
        SET_TRANSACTION_NAME,
        LOG_TXN_SCHEMA.clone(),
        "appId",
        txn as fn(&str) -> Scalar,
        LOG_DOMAIN_METADATA_SCHEMA.clone()
    )]
    #[case::domain_metadata(
        DOMAIN_METADATA_NAME,
        LOG_DOMAIN_METADATA_SCHEMA.clone(),
        "domain",
        domain_metadata as fn(&str) -> Scalar,
        LOG_TXN_SCHEMA.clone()
    )]
    fn reads_every_sidecar_onto_the_read_schema(
        #[case] action: &str,
        #[case] source_schema: SchemaRef,
        #[case] key: &str,
        #[case] row: fn(&str) -> Scalar,
        #[case] other_schema: SchemaRef,
    ) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let write =
            |name, rows| write_sidecar(&engine, &table_root(), name, source_schema.clone(), rows);
        let sidecars = [
            write("a.parquet", vec![row("x"), row("y")]),
            write("b.parquet", vec![row("z")]),
        ];
        let read_schema = Arc::new(
            StructType::try_new(source_schema.fields().chain(other_schema.fields()).cloned())
                .unwrap(),
        );

        let batches: Vec<_> = read_sidecar_actions(
            &engine,
            &log_root(),
            &sidecars,
            action,
            source_schema,
            read_schema,
        )
        .unwrap()
        .map(|batch| {
            let batch = batch.unwrap();
            assert!(!batch.is_log_batch);
            batch.actions.try_into_record_batch().unwrap()
        })
        .collect();

        let all_keys: Vec<_> = batches.iter().flat_map(|b| keys(b, action, key)).collect();
        assert_eq!(all_keys, ["x", "y", "z"]);
        for batch in &batches {
            for field in other_schema.fields() {
                let other = batch.column_by_name(field.name()).unwrap();
                assert_eq!(other.null_count(), batch.num_rows());
            }
        }
    }

    #[test]
    fn no_sidecars_reads_nothing() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let batches = read_sidecar_actions(
            &engine,
            &log_root(),
            &[],
            SET_TRANSACTION_NAME,
            LOG_TXN_SCHEMA.clone(),
            LOG_TXN_SCHEMA.clone(),
        )
        .unwrap();
        assert_eq!(batches.count(), 0);
    }

    #[test]
    fn cancelled_read_stops() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let sidecar = write_sidecar(
            &engine,
            &table_root(),
            "a.parquet",
            LOG_TXN_SCHEMA.clone(),
            vec![txn("x")],
        );
        let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
        let result = read_sidecar_actions(
            &CancellableEngine::new(&engine, token),
            &log_root(),
            &[sidecar],
            SET_TRANSACTION_NAME,
            LOG_TXN_SCHEMA.clone(),
            LOG_TXN_SCHEMA.clone(),
        );
        assert!(matches!(result, Err(KernelError::Cancelled)));
    }

    #[test]
    fn missing_sidecar_errors_with_its_path() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let sidecar = Sidecar::new("missing.parquet".to_string(), 1, 0, None);
        let result = read_sidecar_actions(
            &engine,
            &log_root(),
            &[sidecar],
            SET_TRANSACTION_NAME,
            LOG_TXN_SCHEMA.clone(),
            LOG_TXN_SCHEMA.clone(),
        )
        .and_then(|batches| batches.collect::<KernelResult<Vec<_>>>());
        assert_result_error_with_message(result, "_delta_log/_sidecars/missing.parquet");
    }
}
