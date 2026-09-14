//! Tracing layer for reporting the dynamic lifetime of call-frame-enabled spans.

use std::sync::Arc;

use tracing::span::Id;
use tracing::subscriber::Interest;
use tracing::{Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Tracing field whose presence opts a span into call-frame lifecycle reporting.
pub const ENABLE_CALL_FRAME_FIELD: &str = "enable_call_frame";

/// Receives synchronous notifications when a call-frame-enabled span is entered and exited.
///
/// Calls are synchronous and may occur concurrently on multiple threads. Profile consumers should
/// capture time in these methods and maintain a separate event stack for each calling thread.
pub trait FrameReporter: Send + Sync + std::fmt::Debug {
    /// Reports that the current thread entered `name`, identified by `span_id`.
    ///
    /// The `name` comes from the span's static tracing metadata. A later [`Self::exit`] call uses
    /// the same `span_id` to identify the matching span.
    fn enter(&self, span_id: u64, name: &'static str);

    /// Reports that the current thread exited the span identified by `span_id` from
    /// [`Self::enter`].
    fn exit(&self, span_id: u64);
}

/// A tracing layer that forwards dynamic span entry and exit to a [`FrameReporter`].
///
/// A span opts in by declaring a field named [`ENABLE_CALL_FRAME_FIELD`]. The field value is
/// ignored; the frame name comes from the span's static tracing metadata.
#[derive(Debug)]
pub struct FrameReporterLayer {
    reporter: Arc<dyn FrameReporter>,
}

impl FrameReporterLayer {
    /// Creates a layer that forwards frame lifecycle notifications to the supplied `reporter`.
    pub fn new(reporter: Arc<dyn FrameReporter>) -> Self {
        Self { reporter }
    }

    fn is_call_frame_enabled(metadata: &Metadata<'_>) -> bool {
        metadata.is_span() && metadata.fields().field(ENABLE_CALL_FRAME_FIELD).is_some()
    }
}

impl<S> Layer<S> for FrameReporterLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if Self::is_call_frame_enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(metadata) = ctx.metadata(id) else {
            return;
        };
        if Self::is_call_frame_enabled(metadata) {
            self.reporter.enter(id.into_u64(), metadata.name());
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(metadata) = ctx.metadata(id) else {
            return;
        };
        if Self::is_call_frame_enabled(metadata) {
            self.reporter.exit(id.into_u64());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::Mutex;

    use tracing::field::Empty;
    use tracing::subscriber::with_default;
    use tracing::{info_span, trace_span, Span};
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::Registry;

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::events::SNAPSHOT_COMPLETED_SPAN;
    use crate::unit_test_utils::{copy_test_table, load_test_table};
    use crate::Snapshot;

    #[derive(Clone, Debug, PartialEq)]
    enum FrameEvent {
        Enter { id: u64, name: &'static str },
        Exit { id: u64 },
    }

    #[derive(Debug, Default)]
    struct CapturingFrameReporter {
        events: Mutex<Vec<FrameEvent>>,
    }

    impl FrameReporter for CapturingFrameReporter {
        fn enter(&self, span_id: u64, name: &'static str) {
            self.events
                .lock()
                .unwrap()
                .push(FrameEvent::Enter { id: span_id, name });
        }

        fn exit(&self, span_id: u64) {
            self.events
                .lock()
                .unwrap()
                .push(FrameEvent::Exit { id: span_id });
        }
    }

    fn capture(f: impl FnOnce()) -> Vec<FrameEvent> {
        test_utils::ensure_metrics_compatible_global_subscriber();
        let reporter = Arc::new(CapturingFrameReporter::default());
        let subscriber = Registry::default().with(FrameReporterLayer::new(reporter.clone()));
        with_default(subscriber, f);
        let events = reporter.events.lock().unwrap().clone();
        events
    }

    fn entered_callstacks(events: &[FrameEvent]) -> Vec<Vec<&'static str>> {
        let mut stack = Vec::new();
        let mut callstacks = Vec::new();
        for event in events {
            match *event {
                FrameEvent::Enter { id, name } => {
                    stack.push((id, name));
                    callstacks.push(stack.iter().map(|(_, name)| *name).collect());
                }
                FrameEvent::Exit { id } => {
                    let Some((entered_id, _)) = stack.pop() else {
                        panic!("exit without matching enter: {events:?}");
                    };
                    assert_eq!(id, entered_id, "frames exited out of order: {events:?}");
                }
            }
        }
        assert!(stack.is_empty(), "unclosed frames: {events:?}");
        callstacks
    }

    fn assert_contains_callstack(callstacks: &[Vec<&str>], expected: &[&str]) {
        assert!(
            callstacks.iter().any(|stack| stack == expected),
            "missing callstack {expected:?}; captured {callstacks:?}"
        );
    }

    #[test]
    fn reports_nested_dynamic_entries() {
        let events = capture(|| {
            let outer = info_span!("outer", enable_call_frame = Empty);
            let _outer_guard = outer.enter();
            let inner = info_span!("inner", enable_call_frame = Empty);
            let _inner_guard = inner.enter();
        });

        let [FrameEvent::Enter {
            id: outer_id,
            name: "outer",
        }, FrameEvent::Enter {
            id: inner_id,
            name: "inner",
        }, FrameEvent::Exit { id: inner_exit_id }, FrameEvent::Exit { id: outer_exit_id }] =
            events.as_slice()
        else {
            panic!("unexpected events: {events:?}");
        };
        assert_eq!(inner_id, inner_exit_id);
        assert_eq!(outer_id, outer_exit_id);
    }

    #[test]
    fn ignores_unmarked_spans() {
        let events = capture(|| {
            let span = info_span!("not-profiled");
            let _guard = span.enter();
        });
        assert!(events.is_empty());
    }

    #[test]
    fn registers_interest_only_in_marked_spans() {
        let reporter = Arc::new(CapturingFrameReporter::default());
        let layer = FrameReporterLayer::new(reporter);

        with_default(Registry::default(), || {
            let unmarked = trace_span!("unmarked-trace");
            let marked = trace_span!("marked-trace", enable_call_frame = Empty);
            let unmarked_metadata = unmarked.metadata().unwrap();
            let marked_metadata = marked.metadata().unwrap();

            assert!(<FrameReporterLayer as Layer<Registry>>::register_callsite(
                &layer,
                unmarked_metadata
            )
            .is_never());
            assert!(<FrameReporterLayer as Layer<Registry>>::register_callsite(
                &layer,
                marked_metadata
            )
            .is_always());
        });
    }

    #[test]
    fn ignores_the_opt_in_field_value() {
        let events = capture(|| {
            let span = info_span!("value-is-ignored", enable_call_frame = false);
            let _guard = span.enter();
        });

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0],
            FrameEvent::Enter {
                name: "value-is-ignored",
                ..
            }
        ));
    }

    #[test]
    fn reports_each_sequential_entry_of_a_reused_span() {
        let events = capture(|| {
            let span = info_span!("reused", enable_call_frame = Empty);
            for _ in 0..2 {
                let _guard = span.enter();
            }
        });

        let ids: Vec<_> = events
            .iter()
            .map(|event| match event {
                FrameEvent::Enter { id, .. } | FrameEvent::Exit { id } => *id,
            })
            .collect();
        assert_eq!(events.len(), 4);
        assert!(ids.iter().all(|id| *id == ids[0]));
        assert!(matches!(events[0], FrameEvent::Enter { .. }));
        assert!(matches!(events[1], FrameEvent::Exit { .. }));
        assert!(matches!(events[2], FrameEvent::Enter { .. }));
        assert!(matches!(events[3], FrameEvent::Exit { .. }));
    }

    #[test]
    fn reports_only_dynamically_entered_spans() {
        let events = capture(|| {
            let parent = info_span!("parent", enable_call_frame = Empty);
            let child = info_span!(parent: &parent, "child", enable_call_frame = Empty);
            let _child_guard = child.enter();
        });

        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], FrameEvent::Enter { name: "child", .. }));
    }

    #[test]
    fn error_return_reports_exit() {
        fn fail() -> Result<(), &'static str> {
            let span = info_span!("fails", enable_call_frame = Empty);
            let _guard = span.enter();
            Err("expected")
        }

        let events = capture(|| assert_eq!(fail(), Err("expected")));

        assert_eq!(events.len(), 2);
        let FrameEvent::Enter { id, name: "fails" } = events[0] else {
            panic!("unexpected enter event: {:?}", events[0]);
        };
        assert_eq!(events[1], FrameEvent::Exit { id });
    }

    #[test]
    fn panic_unwinding_reports_exit() {
        let events = capture(|| {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let span = info_span!("panics", enable_call_frame = Empty);
                let _guard = span.enter();
                panic!("expected");
            }));
            assert!(result.is_err());
            assert!(Span::current().is_none());
        });

        assert_eq!(events.len(), 2);
        let FrameEvent::Enter { id, name: "panics" } = events[0] else {
            panic!("unexpected enter event: {:?}", events[0]);
        };
        assert_eq!(events[1], FrameEvent::Exit { id });
    }

    #[test]
    fn snapshot_construction_reports_expected_callstacks() {
        let (table_url, _tempdir) = copy_test_table("table-without-dv-small").unwrap();
        let engine = SyncEngine::new();

        let events = capture(|| {
            Snapshot::builder_for(table_url).build(&engine).unwrap();
        });
        let callstacks = entered_callstacks(&events);

        assert_contains_callstack(
            &callstacks,
            &[
                SNAPSHOT_COMPLETED_SPAN,
                "log_segment.for_snapshot",
                "last_checkpoint.read",
            ],
        );
        assert_contains_callstack(
            &callstacks,
            &[
                SNAPSHOT_COMPLETED_SPAN,
                "log_segment.for_snapshot",
                "log.list",
            ],
        );
        assert_contains_callstack(
            &callstacks,
            &[
                SNAPSHOT_COMPLETED_SPAN,
                "try_new_from_log_segment",
                "log_seg.load_p_m",
            ],
        );
    }

    #[test]
    fn imperative_scan_reports_expected_callstacks() {
        let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small").unwrap();

        let events = capture(|| {
            let scan = snapshot.scan_builder().build().unwrap();
            for metadata in scan.scan_metadata(engine.as_ref()).unwrap() {
                metadata.unwrap();
            }
        });
        let callstacks = entered_callstacks(&events);

        assert_contains_callstack(&callstacks, &["scan_builder.build"]);
        assert_contains_callstack(&callstacks, &["scan_log_replay.process_actions_batch"]);
    }

    #[cfg(feature = "declarative-plans")]
    #[test]
    fn declarative_scan_reports_expected_callstacks() {
        let (engine, snapshot, _tempdir) =
            load_test_table("v2-checkpoints-parquet-with-sidecars").unwrap();

        let events = capture(|| {
            let scan = snapshot.scan_builder().build().unwrap();
            scan.declarative_metadata_scan_plan(engine.as_ref())
                .unwrap();
        });
        let callstacks = entered_callstacks(&events);

        assert_contains_callstack(
            &callstacks,
            &[
                "scan.declarative_metadata_scan_plan",
                "checkpoint_shape.try_new",
                "checkpoint_shape.from_v2_checkpoint_hint",
                "checkpoint_shape.try_new_manifest",
            ],
        );
        assert_contains_callstack(
            &callstacks,
            &[
                "scan.declarative_metadata_scan_plan",
                "scan_plan.build_metadata_scan_plan",
            ],
        );
    }
}
