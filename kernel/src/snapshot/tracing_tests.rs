use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use test_utils::{actions_to_string, add_commit, delta_path_for_version, TestAction, METADATA};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};
use tracing_subscriber::registry::LookupSpan;

use super::SnapshotBuilder;
use crate::engine::sync::SyncEngine;
use crate::log_path::LogPath;
use crate::metrics::events::SNAPSHOT_COMPLETED_SPAN;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
use crate::utils::try_parse_uri;
use crate::FileMeta;

const FRESH_CONSTRUCTOR_SPAN: &str = "try_new_from_log_segment";
const INCREMENTAL_CONSTRUCTOR_SPAN: &str = "try_new_from";

type TraceFields = BTreeMap<&'static str, String>;
type CapturedTraces = Vec<(&'static str, TraceFields)>;

/// Captures the parts of snapshot tracing that these regression tests distinguish.
#[derive(Clone, Default)]
struct SnapshotTraceCaptureLayer {
    /// Field names declared by each span, including fields whose values start empty.
    declared_fields: Arc<Mutex<BTreeMap<&'static str, BTreeSet<&'static str>>>>,
    /// Values attached when each span is created.
    initial_values: Arc<Mutex<CapturedTraces>>,
    /// Values supplied later through `Span::record`.
    recorded_values: Arc<Mutex<CapturedTraces>>,
    /// Fields on failure events emitted inside the constructor spans.
    error_events: Arc<Mutex<CapturedTraces>>,
}

impl SnapshotTraceCaptureLayer {
    fn capture(
        traces: &Mutex<CapturedTraces>,
        span_name: &'static str,
        record: impl FnOnce(&mut FieldValueVisitor),
    ) {
        let mut fields = FieldValueVisitor::default();
        record(&mut fields);
        traces.lock().unwrap().push((span_name, fields.0));
    }

    fn initial_values(&self) -> CapturedTraces {
        self.initial_values.lock().unwrap().clone()
    }

    fn recorded_values(&self) -> CapturedTraces {
        self.recorded_values.lock().unwrap().clone()
    }

    fn error_events(&self) -> CapturedTraces {
        self.error_events.lock().unwrap().clone()
    }
}

fn is_captured_snapshot_span(name: &str) -> bool {
    name == FRESH_CONSTRUCTOR_SPAN
        || name == INCREMENTAL_CONSTRUCTOR_SPAN
        || name == SNAPSHOT_COMPLETED_SPAN
        || name == "snap.get_ict"
}

fn is_constructor_span(name: &str) -> bool {
    name == FRESH_CONSTRUCTOR_SPAN || name == INCREMENTAL_CONSTRUCTOR_SPAN
}

fn trace_fields<'a>(traces: &'a CapturedTraces, span_name: &str) -> &'a TraceFields {
    traces
        .iter()
        .find(|(name, _)| *name == span_name)
        .map(|(_, fields)| fields)
        .unwrap_or_else(|| panic!("expected trace for {span_name}"))
}

fn install_capture() -> (SnapshotTraceCaptureLayer, tracing::subscriber::DefaultGuard) {
    let captured = SnapshotTraceCaptureLayer::default();
    // The registry provides the span scope that `event_span` uses to find an event's active span.
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    (captured, guard)
}

#[derive(Default)]
struct FieldValueVisitor(TraceFields);

impl Visit for FieldValueVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for SnapshotTraceCaptureLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        let span_name = attrs.metadata().name();
        if !is_captured_snapshot_span(span_name) {
            return;
        }

        let fields = attrs
            .metadata()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect();
        self.declared_fields
            .lock()
            .unwrap()
            .insert(span_name, fields);
        Self::capture(&self.initial_values, span_name, |values| {
            attrs.record(values)
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let span_name = span.name();
        if !is_captured_snapshot_span(span_name) {
            return;
        }

        Self::capture(&self.recorded_values, span_name, |fields| {
            values.record(fields)
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::ERROR {
            return;
        }
        let Some(span) = ctx.event_span(event) else {
            return;
        };
        if !is_constructor_span(span.name()) {
            return;
        }

        Self::capture(&self.error_events, span.name(), |fields| {
            event.record(fields)
        });
    }
}

fn setup_test() -> (Arc<SyncEngine>, Arc<DynObjectStore>, String) {
    let table_root = String::from("memory:///");
    let store = Arc::new(InMemory::new());
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    (engine, store, table_root)
}

async fn create_table(
    store: &Arc<DynObjectStore>,
    table_root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    add_commit(
        table_root,
        store.as_ref(),
        0,
        actions_to_string(vec![TestAction::Metadata]),
    )
    .await?;
    add_commit(
        table_root,
        store.as_ref(),
        1,
        actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())]),
    )
    .await?;
    Ok(())
}

fn commit_log_path(table_root: &str, version: u64) -> LogPath {
    let table_url = try_parse_uri(table_root).unwrap();
    let location = table_url
        .join(delta_path_for_version(version, "json").as_ref())
        .unwrap();
    LogPath::try_new(FileMeta {
        location,
        last_modified: 0,
        size: 1,
    })
    .unwrap()
}

#[tokio::test]
async fn snapshot_spans_avoid_heavy_context_and_constructors_capture_failure_arguments(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_root) = setup_test();
    create_table(&store, &table_root).await?;

    let (captured, _guard) = install_capture();

    let snapshot = SnapshotBuilder::new_for(table_root.clone())
        .at_version(0)
        .build(engine.as_ref())?;
    let updated = SnapshotBuilder::new_from(snapshot).build(engine.as_ref())?;
    assert_eq!(updated.version(), 1);
    assert_eq!(
        updated.get_in_commit_timestamp_with_engine(engine.as_ref())?,
        None
    );
    assert!(captured.error_events().is_empty());

    {
        let declared_fields = captured.declared_fields.lock().unwrap();
        for span_name in [
            FRESH_CONSTRUCTOR_SPAN,
            INCREMENTAL_CONSTRUCTOR_SPAN,
            SNAPSHOT_COMPLETED_SPAN,
            "snap.get_ict",
        ] {
            assert!(
                declared_fields.contains_key(span_name),
                "expected fields for {span_name}"
            );
        }

        let required_constructor_fields = BTreeSet::from([
            "path",
            "version",
            "operation_id",
            "correlation_id",
            "incremental_replay",
            "built_as_latest",
        ]);
        for span_name in [FRESH_CONSTRUCTOR_SPAN, INCREMENTAL_CONSTRUCTOR_SPAN] {
            assert!(required_constructor_fields.is_subset(&declared_fields[span_name]));
        }

        let denied_fields = BTreeSet::from([
            "self",
            "existing_snapshot",
            "log_segment",
            "log_tail",
            "snapshot",
            "engine",
        ]);
        for (span_name, fields) in declared_fields.iter() {
            let leaked_fields = fields.intersection(&denied_fields).collect::<Vec<_>>();
            assert!(
                leaked_fields.is_empty(),
                "span {span_name} declared heavy fields: {leaked_fields:?}"
            );
        }
    }

    let initial_values = captured.initial_values();
    assert!(initial_values
        .iter()
        .filter(|(span_name, _)| is_constructor_span(span_name))
        .all(|(_, fields)| {
            fields.values().all(|value| value.len() < 256)
                && fields
                    .get("path")
                    .is_some_and(|path| path.contains(&table_root))
        }));
    let fresh_values = trace_fields(&initial_values, FRESH_CONSTRUCTOR_SPAN);
    assert_eq!(fresh_values["version"], "0");

    let recorded_values = captured.recorded_values();
    assert!(recorded_values
        .iter()
        .filter(|(span_name, _)| is_constructor_span(span_name))
        .flat_map(|(_, fields)| fields.values())
        .all(|value| value.len() < 32));
    assert_eq!(
        recorded_values
            .iter()
            .filter(|(span_name, _)| *span_name == INCREMENTAL_CONSTRUCTOR_SPAN)
            .filter_map(|(_, fields)| fields.get("version").map(String::as_str))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["0", "1"])
    );

    let same_version = SnapshotBuilder::new_from(updated.clone())
        .at_version(1)
        .build(engine.as_ref())?;
    assert!(Arc::ptr_eq(&updated, &same_version));
    let unchanged = SnapshotBuilder::new_from(updated.clone()).build(engine.as_ref())?;
    assert!(Arc::ptr_eq(&updated, &unchanged));
    assert!(captured.error_events().is_empty());

    assert!(SnapshotBuilder::new_from(updated.clone())
        .at_version(0)
        .with_correlation_id("incremental-test")
        .build(engine.as_ref())
        .is_err());

    let error_events = captured.error_events();
    assert_eq!(error_events.len(), 1);
    assert_eq!(error_events[0].0, INCREMENTAL_CONSTRUCTOR_SPAN);
    let incremental_error = trace_fields(&error_events, INCREMENTAL_CONSTRUCTOR_SPAN);
    assert!(!incremental_error["error"].is_empty());
    assert!(incremental_error["existing_snapshot"].contains("Snapshot"));
    assert!(incremental_error["log_segment"].contains("LogSegment"));
    assert!(!incremental_error.contains_key("log_tail"));
    assert!(incremental_error["metric_context"].contains("incremental-test"));
    assert_eq!(incremental_error["incremental_replay"], "Disabled");
    assert_eq!(incremental_error["checkpoint_handling"], "Adopt");
    assert_eq!(incremental_error["message"], "failed to update snapshot");

    let (invalid_engine, invalid_store, invalid_root) = setup_test();
    add_commit(
        &invalid_root,
        invalid_store.as_ref(),
        0,
        actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())]),
    )
    .await?;
    assert!(SnapshotBuilder::new_for(invalid_root)
        .with_correlation_id("fresh-test")
        .build(invalid_engine.as_ref())
        .is_err());

    let error_events = captured.error_events();
    assert_eq!(error_events.len(), 2);
    let fresh_error = trace_fields(&error_events, FRESH_CONSTRUCTOR_SPAN);
    assert!(!fresh_error["error"].is_empty());
    assert!(fresh_error["location"].contains("memory"));
    assert!(fresh_error["log_segment"].contains("LogSegment"));
    assert!(fresh_error["metric_context"].contains("fresh-test"));
    assert_eq!(fresh_error["incremental_replay"], "Disabled");
    assert_eq!(
        fresh_error["message"],
        "failed to construct snapshot from log segment"
    );

    Ok(())
}

#[tokio::test]
async fn snapshot_unsupported_reader_feature_emits_constructor_error(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_root) = setup_test();
    let unsupported_table = METADATA.replace(
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
    );
    add_commit(&table_root, store.as_ref(), 0, unsupported_table).await?;

    let (captured, _guard) = install_capture();
    assert!(SnapshotBuilder::new_for(table_root)
        .with_correlation_id("unsupported-feature-test")
        .build(engine.as_ref())
        .is_err());

    let error_events = captured.error_events();
    assert_eq!(error_events.len(), 1);
    assert_eq!(error_events[0].0, FRESH_CONSTRUCTOR_SPAN);
    let error = trace_fields(&error_events, FRESH_CONSTRUCTOR_SPAN);
    assert!(error["error"].contains("futureFeature"));
    assert!(error["log_segment"].contains("LogSegment"));
    assert!(error["metric_context"].contains("unsupported-feature-test"));

    Ok(())
}

#[tokio::test]
async fn incremental_failure_captures_assembled_log_segment(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_root) = setup_test();
    add_commit(&table_root, store.as_ref(), 0, METADATA.to_owned()).await?;
    let snapshot = SnapshotBuilder::new_for(&table_root)
        .at_version(0)
        .build(engine.as_ref())?;

    let unsupported_table = METADATA.replace(
        r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["futureFeature"],"writerFeatures":["futureFeature"]}}"#,
    );
    add_commit(&table_root, store.as_ref(), 1, unsupported_table).await?;

    let (captured, _guard) = install_capture();
    assert!(SnapshotBuilder::new_from(snapshot)
        .with_log_tail(vec![
            commit_log_path(&table_root, 0),
            commit_log_path(&table_root, 1),
        ])
        .build(engine.as_ref())
        .is_err());

    let error_events = captured.error_events();
    assert_eq!(error_events.len(), 1);
    assert_eq!(error_events[0].0, INCREMENTAL_CONSTRUCTOR_SPAN);
    let fields = &error_events[0].1;
    assert!(fields["error"].contains("futureFeature"));
    assert!(fields["log_segment"].contains("end_version: 1"));
    assert!(fields["log_segment"].contains("00000000000000000001.json"));
    assert!(!fields.contains_key("log_tail"));

    Ok(())
}

#[tokio::test]
async fn snapshot_rebuild_failure_emits_inner_and_outer_constructor_errors(
) -> Result<(), Box<dyn std::error::Error>> {
    let (engine, store, table_root) = setup_test();
    create_table(&store, &table_root).await?;
    let snapshot = SnapshotBuilder::new_for(table_root)
        .at_version(0)
        .build(engine.as_ref())?;

    store
        .put(
            &Path::from("_delta_log/00000000000000000001.checkpoint.parquet"),
            b"not parquet".to_vec().into(),
        )
        .await?;

    let (captured, _guard) = install_capture();
    assert!(SnapshotBuilder::new_from(snapshot)
        .build(engine.as_ref())
        .is_err());

    let error_events = captured.error_events();
    assert_eq!(error_events.len(), 2);
    assert_eq!(
        error_events
            .iter()
            .map(|(span_name, _)| *span_name)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([FRESH_CONSTRUCTOR_SPAN, INCREMENTAL_CONSTRUCTOR_SPAN])
    );
    let outer = trace_fields(&error_events, INCREMENTAL_CONSTRUCTOR_SPAN);
    assert!(outer["log_segment"].contains("00000000000000000001.checkpoint.parquet"));

    Ok(())
}
