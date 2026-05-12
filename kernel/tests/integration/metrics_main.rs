use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::metrics::{MetricEvent, MetricsReporter, WithMetricsReporterLayer as _};
use delta_kernel::Snapshot;
use test_utils::table_builder::{LogState, TestTableBuilder};
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Debug, Default)]
struct NoopReporter;
impl MetricsReporter for NoopReporter {
    fn report(&self, _: MetricEvent) {}
}

fn main() {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_commits(1))
        .with_data(1, 1)
        .build()
        .expect("build test table");
    let table_url = delta_kernel::try_parse_uri(table.table_root()).expect("parse table url");
    let engine = DefaultEngineBuilder::new(table.store().clone()).build();

    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .with_metrics_reporter_layer(Arc::new(NoopReporter));
    tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber)).unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let watch = Arc::clone(&done);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !watch.load(Ordering::SeqCst) {
            eprintln!("WATCHDOG: deadlock - aborting");
            std::process::abort();
        }
    });

    let _snap = Snapshot::builder_for(table_url).build(&engine);

    {
        // trigger the invalid uuid warn
        let _ = tracing::info_span!(
            "snap.build",
            operation_id = "not-a-uuid",
            version = 0u64,
            report = tracing::field::Empty,
        );
    }
    {
        // trigger invalid field warn
        let s = tracing::info_span!(
            "snap.build",
            operation_id = %uuid::Uuid::new_v4(),
            version = 0u64,
            report = tracing::field::Empty,
            invalid_field = tracing::field::Empty,
        );
        s.record("invalid_field", 42u64);
    }

    done.store(true, Ordering::SeqCst);
    println!("PASS");
}
