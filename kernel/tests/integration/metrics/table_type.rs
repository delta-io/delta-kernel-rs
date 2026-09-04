//! Verifies that snapshot-load and scan metric events carry the correct [`TableType`] for both
//! path-based and catalog-managed tables, exercised end-to-end through public APIs
//! (`create_table`, `Transaction`, `Scan`).

use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::{Committer, FileSystemCommitter};
use delta_kernel::metrics::{MetricEvent, TableType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use test_utils::{
    add_commit, engine_store_setup, insert_data_with, install_thread_local_metrics_reporter,
    test_table_setup_mt, CapturingReporter, TestCatalogCommitter,
};

use super::simple_schema;

#[rstest]
#[case::path_based(false)]
#[case::catalog_managed(true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_metric_events_carry_table_type(#[case] catalog_managed: bool) -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let build_snapshot = |max_catalog_version: u64| -> DeltaResult<Arc<Snapshot>> {
        let builder = Snapshot::builder_for(table_url.clone());
        let builder = if catalog_managed {
            builder.with_max_catalog_version(max_catalog_version)
        } else {
            builder
        };
        builder.build(engine.as_ref())
    };

    // Given: a table (path-based or catalog-managed) with one appended row.
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

    // When: a fresh snapshot is built and scanned with a metrics reporter installed.
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snapshot = build_snapshot(1)?;
    let scan = snapshot.scan_builder().build()?;
    for batch in scan.scan_metadata(engine.as_ref())? {
        batch?;
    }

    // Then: every snapshot-load and scan event carries the expected table_type.
    let expected = TableType::from_catalog_managed(catalog_managed);
    assert_table_types(
        &reporter.events(),
        &[
            "LogSegmentLoadSuccess",
            "ProtocolMetadataLoadSuccess",
            "SnapshotBuildSuccess",
            "ScanMetadataCompleted",
        ],
        expected,
    );
    Ok(())
}

#[rstest]
#[case::path_based(false)]
#[case::catalog_managed(true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_failure_events_carry_table_type(#[case] catalog_managed: bool) {
    let (_tmp, table_path, engine) = test_table_setup_mt().unwrap();
    let table_url = delta_kernel::try_parse_uri(&table_path).unwrap();

    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let builder = Snapshot::builder_for(table_url);
    let builder = if catalog_managed {
        builder.with_max_catalog_version(0)
    } else {
        builder
    };
    assert!(builder.build(engine.as_ref()).is_err());

    let expected = TableType::from_catalog_managed(catalog_managed);
    assert_table_types(
        &reporter.events(),
        &["LogSegmentLoadFailure", "SnapshotBuildFailure"],
        expected,
    );
}

#[rstest]
#[case::path_based(false)]
#[case::catalog_managed(true)]
#[tokio::test]
async fn test_protocol_metadata_load_failure_carries_table_type(#[case] catalog_managed: bool) {
    let (store, engine, table_url) = engine_store_setup("pm_failure", None);
    // An Add-only commit (no Protocol/Metadata) makes the protocol+metadata replay fail.
    let add_only = r#"{"add":{"path":"f.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}"#;
    add_commit(table_url.as_str(), store.as_ref(), 0, add_only.to_string())
        .await
        .unwrap();

    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let builder = Snapshot::builder_for(table_url);
    let builder = if catalog_managed {
        builder.with_max_catalog_version(0)
    } else {
        builder
    };
    assert!(builder.build(&engine).is_err());

    let expected = TableType::from_catalog_managed(catalog_managed);
    assert_table_types(
        &reporter.events(),
        &["ProtocolMetadataLoadFailure", "SnapshotBuildFailure"],
        expected,
    );
}

/// Validation fails after the protocol read succeeds, so events carry the requested mode (the
/// documented best guess), not the protocol truth.
#[rstest]
#[case::catalog_version_on_path_based(false, true, TableType::CatalogManaged)]
#[case::no_catalog_version_on_catalog_managed(true, false, TableType::PathBased)]
#[tokio::test(flavor = "multi_thread")]
async fn test_build_validation_mismatch_events_carry_requested_table_type(
    #[case] create_catalog_managed: bool,
    #[case] request_catalog_version: bool,
    #[case] expected: TableType,
) -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    create_simple_table(engine.as_ref(), &table_path, create_catalog_managed)?;

    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let builder = Snapshot::builder_for(table_url);
    let builder = if request_catalog_version {
        builder.with_max_catalog_version(0)
    } else {
        builder
    };
    assert!(builder.build(engine.as_ref()).is_err());

    assert_table_types(
        &reporter.events(),
        &["ProtocolMetadataLoadSuccess", "SnapshotBuildFailure"],
        expected,
    );
    Ok(())
}

pub(super) fn make_committer(catalog_managed: bool) -> Box<dyn Committer> {
    if catalog_managed {
        Box::new(TestCatalogCommitter)
    } else {
        Box::new(FileSystemCommitter::new())
    }
}

pub(super) fn create_simple_table(
    engine: &dyn Engine,
    table_path: &str,
    catalog_managed: bool,
) -> DeltaResult<()> {
    let builder = create_table(table_path, simple_schema(), "Test/1.0");
    let builder = if catalog_managed {
        builder.with_table_properties([
            ("delta.feature.catalogManaged", "supported"),
            ("delta.feature.vacuumProtocolCheck", "supported"),
            ("io.unitycatalog.tableId", "metrics-table-type-test"),
        ])
    } else {
        builder
    };
    builder
        .build(engine, make_committer(catalog_managed))?
        .commit(engine)?
        .unwrap_committed();
    Ok(())
}

fn table_types(events: &[MetricEvent]) -> Vec<(&'static str, TableType)> {
    events
        .iter()
        .filter_map(|e| match e {
            MetricEvent::LogSegmentLoadSuccess(e) => Some(("LogSegmentLoadSuccess", e.table_type)),
            MetricEvent::LogSegmentLoadFailure(e) => Some(("LogSegmentLoadFailure", e.table_type)),
            MetricEvent::ProtocolMetadataLoadSuccess(e) => {
                Some(("ProtocolMetadataLoadSuccess", e.table_type))
            }
            MetricEvent::ProtocolMetadataLoadFailure(e) => {
                Some(("ProtocolMetadataLoadFailure", e.table_type))
            }
            MetricEvent::SnapshotBuildSuccess(e) => Some(("SnapshotBuildSuccess", e.table_type)),
            MetricEvent::SnapshotBuildFailure(e) => Some(("SnapshotBuildFailure", e.table_type)),
            MetricEvent::ScanMetadataCompleted(e) => Some(("ScanMetadataCompleted", e.table_type)),
            _ => None,
        })
        .collect()
}

#[track_caller]
pub(super) fn assert_table_types(events: &[MetricEvent], required: &[&str], expected: TableType) {
    let observed = table_types(events);
    let kinds: HashSet<&str> = observed.iter().map(|(kind, _)| *kind).collect();
    for required in required {
        assert!(
            kinds.contains(required),
            "missing {required} event; saw {kinds:?}"
        );
    }
    for (kind, table_type) in observed {
        assert_eq!(table_type, expected, "{kind} carried the wrong table_type");
    }
}
