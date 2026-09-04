//! Verifies that the parallel scan-metadata path (`parallel_scan_metadata`) emits
//! `ScanMetadataCompleted` events carrying the correct [`TableType`], exercising the live
//! `StateInfo` -> phase-event wiring (a different source than the full-scan path).

use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::metrics::TableType;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use test_utils::{
    insert_data_with, install_thread_local_metrics_reporter, test_table_setup_mt, CapturingReporter,
};

use super::table_type::{assert_table_types, create_simple_table, make_committer};

#[rstest]
#[case::path_based(false)]
#[case::catalog_managed(true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_parallel_scan_metadata_events_carry_table_type(
    #[case] catalog_managed: bool,
) -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let build_snapshot = |version: u64| -> DeltaResult<Arc<Snapshot>> {
        let builder = Snapshot::builder_for(table_url.clone());
        let builder = if catalog_managed {
            builder.with_max_catalog_version(version)
        } else {
            builder
        };
        builder.build(engine.as_ref())
    };

    // Given: a table with one appended row.
    create_simple_table(engine.as_ref(), &table_path, catalog_managed)?;
    insert_data_with(
        build_snapshot(0)?,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
        make_committer(catalog_managed),
        "WRITE",
        /* data_change */ true,
        /* is_blind_append */ false,
    )
    .await?
    .unwrap_committed();

    // When: the table is scanned via the parallel (sequential + optional distributed) path.
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let scan = build_snapshot(1)?.scan_builder().build()?;
    let engine_dyn: Arc<dyn Engine> = engine.clone();
    let mut sequential = scan.parallel_scan_metadata(engine_dyn.clone())?;
    for result in sequential.by_ref() {
        result?;
    }
    if let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()? {
        let state = Arc::new(*state);
        for file in files {
            let parallel =
                ParallelScanMetadata::try_new(engine_dyn.clone(), state.clone(), vec![file])?;
            for result in parallel {
                result?;
            }
        }
        state.log_metrics();
    }

    // Then: the emitted scan-metadata event(s) carry the expected table_type.
    assert_table_types(
        &reporter.events(),
        &["ScanMetadataCompleted"],
        TableType::from_catalog_managed(catalog_managed),
    );
    Ok(())
}
