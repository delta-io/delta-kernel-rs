use std::sync::Arc;

use rstest::rstest;

use super::*;
use crate::Snapshot;
use crate::engine::DelegatingEngine;
use crate::expressions::{Predicate, column_name};
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{
    CapturingReporter, install_thread_local_metrics_reporter, load_test_table,
};

#[rstest]
#[case::plan(5, false, false, ScanPlanOutcome::Plan)]
#[case::commit_only(4, false, false, ScanPlanOutcome::Plan)]
#[case::empty(5, true, false, ScanPlanOutcome::Empty)]
#[case::error(5, false, true, ScanPlanOutcome::Error)]
fn scan_plan_reports_one_completion_with_original_options(
    #[case] version: Version,
    #[case] skip: bool,
    #[case] missing_executor: bool,
    #[case] outcome: ScanPlanOutcome,
) {
    let (engine, snapshot, _tempdir) =
        load_test_table("v1-multi-part-partitioned-struct-stats-only").unwrap();
    let snapshot = Snapshot::builder_for(snapshot.table_root().clone())
        .at_version(version)
        .build(engine.as_ref())
        .unwrap();
    let snapshot = Arc::new(
        Snapshot::new(
            snapshot.log_segment().clone(),
            snapshot.table_configuration().clone(),
        )
        .unwrap(),
    );
    let stats = StatsOptions::struct_columns(vec![column_name!("id")]);
    let partitions = PartitionValuesOptions::with_struct().with_timestamp_timezone("UTC");
    let scan = snapshot
        .scan_builder()
        .with_stats(stats.clone())
        .with_partition_values(partitions.clone())
        .with_correlation_id("scan-plan-test")
        .with_predicate(Arc::new(if skip {
            Predicate::FALSE
        } else {
            Predicate::TRUE
        }))
        .build()
        .unwrap();
    let engine = if missing_executor {
        DelegatingEngine::new(engine).without_plan_executor()
    } else {
        DelegatingEngine::new(engine)
    };
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let result = scan.declarative_metadata_scan_plan(&engine);
    assert_eq!(result.is_err(), missing_executor);
    let events: Vec<_> = reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::ScanPlanGenerationCompleted(event) => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.outcome, outcome);
    assert_eq!(event.stats, stats);
    assert_eq!(event.partition_values, partitions);
    assert_eq!(event.correlation_id.as_deref(), Some("scan-plan-test"));
    assert_ne!(event.operation_id, MetricId::nil());
    assert!(event.duration_ns > 0);
    assert_eq!(
        event.replay_base,
        (!skip).then_some(if version == 4 {
            ReplayBase::Empty
        } else {
            ReplayBase::Checkpoint
        })
    );
    if let Some(checkpoint) = &event.checkpoint {
        assert_eq!(checkpoint.cache_hit, Some(false));
        assert_eq!(checkpoint.info.is_none(), missing_executor);
    }
    let bounds = if version == 4 {
        (Some(0), Some(4))
    } else {
        (None, None)
    };
    assert_eq!(
        (event.commit_start_version, event.commit_end_version),
        bounds
    );
    assert_eq!(event.commit_total_size_bytes == 0, version != 4);
}

#[test]
fn scan_plan_updates_nearest_parent_span_and_skips_disabled_reporting() {
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        ScanPlanGenerationCompleted::update(|_| panic!("disabled update evaluated"));
    });
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let span = tracing::info_span!(
        "scan.declarative_metadata_scan_plan",
        report = tracing::field::Empty
    );
    let operation_id = MetricId::new();
    {
        let _entered = span.enter();
        tracing::info_span!("child").in_scope(|| {
            ScanPlanGenerationCompleted::update(|event| event.operation_id = operation_id);
        });
    }
    drop(span);
    assert!(matches!(reporter.events().as_slice(),
        [MetricEvent::ScanPlanGenerationCompleted(event)] if event.operation_id == operation_id));
}
