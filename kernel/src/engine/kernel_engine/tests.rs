//! Cancellation and ownership through real engine read paths.
//!
//! These unit tests use `SyncEngine` handlers and table fixtures. `RecordingReads` verifies token
//! identity, records handler requests, and triggers cancellation at read entry or after a JSON
//! iterator item. It delegates successful reads to the underlying handlers and can inject a
//! cancellation error without a token to check error propagation.
//!
//! The recording tests cover:
//! - Bound-token inheritance, explicit-token overrides, and unscoped listing, byte-range, JSON,
//!   Parquet, and footer reads.
//! - Checkpoint-hint and log discovery, plus full and incremental snapshot loading through CRC,
//!   JSON commits, and Parquet checkpoints, with builder-bound or caller-wrapped engines.
//! - CRC reconstruction and optional-read fallback: cancellation propagates, including after a JSON
//!   batch, while malformed CRC content can fall back to replay.
//! - SnapshotReader token forwarding for post-build metadata APIs, including timestamp fallback.
//! - Correlated failure metrics through metered handlers, without snapshot success on cancellation.
//!
//! Tests using `KernelEngine` directly cover iterator ownership after the wrapper is dropped,
//! release of the build token by completed snapshots, cached snapshot reuse, nested HEAD preflight,
//! and unchanged parsing, writes, and declarative replay.
//!
//! This suite checks Kernel token propagation and cancellation handling. Connector-managed
//! shared-work cancellation and abort latency for in-flight cloud I/O require separate integration
//! tests.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use itertools::Itertools;
use rstest::rstest;
use tempfile::TempDir;
use url::Url;

use crate::actions::LOG_METADATA_SCHEMA;
use crate::arrow::array::StringArray;
use crate::committer::FileSystemCommitter;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::engine::KernelEngine;
use crate::metrics::{LogSegmentLoadType, MeteredDeltaEngine, MetricEvent, SnapshotLoadType};
use crate::object_store::memory::InMemory;
use crate::schema::{schema_ref, ColumnName, SchemaRef};
use crate::snapshot::IncrementalReplay;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::unit_test_utils::{
    copy_test_table, install_thread_local_metrics_reporter, string_array_to_engine_data,
    CapturingReporter, TestCancellationToken,
};
use crate::utils::try_parse_uri;
use crate::{
    CancellationTokenRef, Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize,
    FileSlice, FilteredEngineData, JsonHandler, KernelError, KernelResult, KernelResultIterator,
    KernelResultIteratorStatic, ParquetFooter, ParquetHandler, PredicateRef, Snapshot,
    StorageHandler,
};

/// Read boundary observed by the recording handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Read {
    List,
    Hint,
    Crc,
    Json,
    Parquet,
    Footer,
}

/// Records scopes at the real engine's read boundaries; all data comes from the inner handlers.
struct RecordingReads {
    /// Engine supplying fixture data and delegated operations.
    inner: Arc<dyn Engine>,
    /// Token cancelled by the configured read trigger.
    token: Arc<TestCancellationToken>,
    /// Expected token identity or absence at each read boundary.
    expected_token: Option<CancellationTokenRef>,
    /// Read boundary that triggers cancellation, when configured.
    cancel_on: Option<Read>,
    /// Cancels the test token after each JSON iterator item.
    cancel_json_after_batch: bool,
    /// Read boundaries observed in call order.
    events: Mutex<Vec<Read>>,
}

impl RecordingReads {
    /// Creates a recorder that can cancel at a selected read boundary.
    fn new(cancel_on: Option<Read>) -> Arc<Self> {
        let token = Arc::new(TestCancellationToken::default());
        Arc::new(Self {
            inner: Arc::new(SyncEngine::new()),
            expected_token: Some(token.clone()),
            token,
            cancel_on,
            cancel_json_after_batch: false,
            events: Mutex::new(Vec::new()),
        })
    }

    /// Builds an engine that records reads through handler-based replay.
    fn engine(self: &Arc<Self>) -> DelegatingEngine {
        let engine = DelegatingEngine::new(self.inner.clone())
            .with_storage_handler(self.clone())
            .with_json_handler(self.clone())
            .with_parquet_handler(self.clone());
        #[cfg(feature = "declarative-plans")]
        let engine = engine.without_plan_executor();
        engine
    }

    /// Verifies token identity, records the read, and applies its cancellation trigger.
    fn record(&self, read: Read, token: &Option<CancellationTokenRef>) -> KernelResult<()> {
        match (token, &self.expected_token) {
            (Some(token), Some(expected)) => assert!(Arc::ptr_eq(token, expected)),
            (None, None) => {}
            _ => panic!("missing or replaced token on {read:?}"),
        }
        self.events.lock().unwrap().push(read);
        if self.cancel_on == Some(read) {
            self.token.cancel();
            if token.is_none() {
                return Err(KernelError::Cancelled);
            }
        }
        Ok(())
    }
}

/// Routes plain reads through the recorder without supplying a cancellation token.
macro_rules! plain_reads {
    ($(
        $(#[$doc:meta])*
        fn $plain:ident, $cancellable:ident($($arg:ident: $ty:ty),* $(,)?) -> $result:ty;
    )*) => {$(
        $(#[$doc])*
        fn $plain(&self, $($arg: $ty),*) -> $result {
            self.$cancellable($($arg,)* None)
        }
    )*};
}

impl StorageHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped listing request.
        fn list_from, list_from_with_cancellation(path: &Url)
            -> KernelResult<KernelResultIteratorStatic<FileMeta>>;

        /// Records an unscoped byte-range read.
        fn read_files, read_files_with_cancellation(files: Vec<FileSlice>)
            -> KernelResult<KernelResultIteratorStatic<Bytes>>;
    }

    fn list_from_with_cancellation(
        &self,
        path: &Url,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<KernelResultIteratorStatic<FileMeta>> {
        self.record(Read::List, &token)?;
        self.inner
            .storage_handler()
            .list_from_with_cancellation(path, token)
    }

    fn read_files_with_cancellation(
        &self,
        files: Vec<FileSlice>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<KernelResultIteratorStatic<Bytes>> {
        let read = if files.iter().any(|(url, _)| url.path().ends_with(".crc")) {
            Read::Crc
        } else {
            Read::Hint
        };
        self.record(read, &token)?;
        self.inner
            .storage_handler()
            .read_files_with_cancellation(files, token)
    }

    forward_methods! {
        /// Writes fixture bytes through the real storage handler.
        fn put(path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> => storage_handler;

        /// Retrieves fixture metadata through the real storage handler.
        fn head(path: &Url) -> KernelResult<FileMeta> => storage_handler;

        /// Deletes a fixture file through the real storage handler.
        fn delete(path: &Url) -> KernelResult<()> => storage_handler;

        /// Copies a fixture file atomically through the real storage handler.
        fn copy_atomic(src: &Url, dest: &Url) -> KernelResult<()> => storage_handler;
    }
}

impl JsonHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped JSON read.
        fn read_json_files, read_json_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;
    }

    fn read_json_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        if !files.is_empty() {
            self.record(Read::Json, &token)?;
        }
        let batches = self
            .inner
            .json_handler()
            .read_json_files_with_cancellation(files, schema, predicate, token)?;
        if self.cancel_json_after_batch {
            let token = self.token.clone();
            Ok(Box::new(batches.inspect(move |_| token.cancel())))
        } else {
            Ok(batches)
        }
    }

    forward_methods! {
        /// Parses fixture data through the real JSON handler.
        fn parse_json(data: Box<dyn EngineData>, schema: SchemaRef)
            -> KernelResult<Box<dyn EngineData>> => json_handler;

        /// Writes fixture batches through the real JSON handler.
        fn write_json_file(
            path: &Url, data: KernelResultIterator<'_, FilteredEngineData>, overwrite: bool,
        ) -> KernelResult<FileSize> => json_handler;
    }
}

impl ParquetHandler for RecordingReads {
    plain_reads! {
        /// Records an unscoped Parquet read.
        fn read_parquet_files, read_parquet_files_with_cancellation(
            files: &[FileMeta], schema: SchemaRef, predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator>;

        /// Records an unscoped Parquet footer read.
        fn read_parquet_footer, read_parquet_footer_with_cancellation(file: &FileMeta)
            -> KernelResult<ParquetFooter>;
    }

    fn read_parquet_files_with_cancellation(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.record(Read::Parquet, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_files_with_cancellation(files, schema, predicate, token)
    }

    fn read_parquet_footer_with_cancellation(
        &self,
        file: &FileMeta,
        token: Option<CancellationTokenRef>,
    ) -> KernelResult<ParquetFooter> {
        self.record(Read::Footer, &token)?;
        self.inner
            .parquet_handler()
            .read_parquet_footer_with_cancellation(file, token)
    }

    forward_methods! {
        /// Writes fixture batches through the real Parquet handler.
        fn write_parquet_file(
            location: Url, data: KernelResultIteratorStatic<Box<dyn EngineData>>,
        ) -> KernelResult<FileSize> => parquet_handler;
    }
}

/// Handler entry point selected for a read.
#[derive(Clone, Copy)]
enum ReadApi {
    Plain,
    WithCancellation,
}

/// Starts a fixture read and erases successful values for shared assertions.
fn start_read(
    engine: &dyn Engine,
    read: Read,
    api: ReadApi,
    token: Option<CancellationTokenRef>,
) -> KernelResult<KernelResultIteratorStatic<()>> {
    let log = try_parse_uri("tests/data/parquet_row_group_skipping")?.join("_delta_log/")?;
    let file_name = match read {
        Read::List => "00000000000000000000",
        Read::Hint => "_last_checkpoint",
        Read::Crc => "00000000000000000000.crc",
        Read::Json => "00000000000000000000.json",
        Read::Parquet | Read::Footer => {
            "00000000000000000001.checkpoint.0000000001.0000000005.parquet"
        }
    };
    let path = if read == Read::Crc {
        try_parse_uri("tests/data/crc-full")?.join("_delta_log/00000000000000000000.crc")?
    } else {
        log.join(file_name)?
    };
    let file = FileMeta::new(path.clone(), 0, 0);
    let schema = LOG_METADATA_SCHEMA.clone();
    match (read, api) {
        (Read::List, ReadApi::Plain) => {
            discard_read_values(engine.storage_handler().list_from(&path))
        }
        (Read::List, ReadApi::WithCancellation) => discard_read_values(
            engine
                .storage_handler()
                .list_from_with_cancellation(&path, token),
        ),
        (Read::Hint | Read::Crc, ReadApi::Plain) => {
            discard_read_values(engine.storage_handler().read_files(vec![(path, None)]))
        }
        (Read::Hint | Read::Crc, ReadApi::WithCancellation) => discard_read_values(
            engine
                .storage_handler()
                .read_files_with_cancellation(vec![(path, None)], token),
        ),
        (Read::Json, ReadApi::Plain) => {
            discard_read_values(engine.json_handler().read_json_files(&[file], schema, None))
        }
        (Read::Json, ReadApi::WithCancellation) => discard_read_values(
            engine
                .json_handler()
                .read_json_files_with_cancellation(&[file], schema, None, token),
        ),
        (Read::Parquet, ReadApi::Plain) => discard_read_values(
            engine
                .parquet_handler()
                .read_parquet_files(&[file], schema, None),
        ),
        (Read::Parquet, ReadApi::WithCancellation) => discard_read_values(
            engine
                .parquet_handler()
                .read_parquet_files_with_cancellation(&[file], schema, None, token),
        ),
        (Read::Footer, ReadApi::Plain) => {
            engine.parquet_handler().read_parquet_footer(&file)?;
            Ok(Box::new(std::iter::once(Ok(()))))
        }
        (Read::Footer, ReadApi::WithCancellation) => {
            engine
                .parquet_handler()
                .read_parquet_footer_with_cancellation(&file, token)?;
            Ok(Box::new(std::iter::once(Ok(()))))
        }
    }
}

/// Erases successful read values without consuming the iterator.
fn discard_read_values<T: 'static>(
    result: KernelResult<KernelResultIteratorStatic<T>>,
) -> KernelResult<KernelResultIteratorStatic<()>> {
    Ok(Box::new(result?.map(|item| item.map(|_| ()))))
}

/// Checks token identity for inherited reads and explicit overrides across the read handlers.
#[rstest]
fn engine_reads_inherit_scope_and_explicit_tokens_override_it(
    #[values(
        Read::List,
        Read::Hint,
        Read::Crc,
        Read::Json,
        Read::Parquet,
        Read::Footer
    )]
    read: Read,
    #[values(ReadApi::Plain, ReadApi::WithCancellation)] api: ReadApi,
    #[values(false, true)] cancelled: bool,
) {
    let reads = RecordingReads::new(None);
    if cancelled {
        reads.token.cancel();
    }
    let engine = KernelEngine::new(&reads.engine(), reads.token.clone());
    let result = start_read(&engine, read, api, None)
        .and_then(|mut iter| iter.next().expect("fixture must produce a result"));
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [read]);

    let explicit = RecordingReads::new(None);
    let engine = KernelEngine::new(
        &explicit.engine(),
        Arc::new(TestCancellationToken::cancelled()),
    );
    start_read(
        &engine,
        read,
        ReadApi::WithCancellation,
        Some(explicit.token.clone()),
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    assert_eq!(*explicit.events.lock().unwrap(), [read]);
}

/// Keeps lazy reads usable and cancellable after their engine wrapper is dropped.
#[rstest]
fn iterators_keep_scope_after_engine_is_dropped(
    #[values(Read::List, Read::Hint, Read::Crc, Read::Json, Read::Parquet)] read: Read,
    #[values(false, true)] cancel: bool,
) {
    let token = Arc::new(TestCancellationToken::default());
    let mut iter = {
        let engine = KernelEngine::new(&SyncEngine::new(), token.clone());
        start_read(&engine, read, ReadApi::Plain, None).unwrap()
    };
    if cancel {
        token.cancel();
        assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
    } else {
        iter.next().unwrap().unwrap();
    }
}

/// Verifies that reads through an unwrapped engine reach handlers without a token.
#[rstest]
fn unscoped_engine_reads_pass_none(
    #[values(
        Read::List,
        Read::Hint,
        Read::Crc,
        Read::Json,
        Read::Parquet,
        Read::Footer
    )]
    read: Read,
) {
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    start_read(&reads.engine(), read, ReadApi::Plain, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(*reads.events.lock().unwrap(), [read]);
}

/// Lets the builder replace a cancelled bound token without replacing the evaluator.
#[test]
fn builder_token_overrides_bound_read_token() {
    let reads = RecordingReads::new(None);
    let original = reads.engine();
    let engine = KernelEngine::new(&original, Arc::new(TestCancellationToken::cancelled()));
    assert!(Arc::ptr_eq(
        &engine.evaluation_handler(),
        &original.evaluation_handler()
    ));
    Snapshot::builder_for("tests/data/table-without-dv-small")
        .with_cancellation_token(reads.token.clone() as CancellationTokenRef)
        .build(&engine)
        .unwrap();
    let events = reads.events.lock().unwrap();
    assert!(events.contains(&Read::Hint));
    assert!(events.contains(&Read::List));
    assert!(events.contains(&Read::Json));
}

/// Preserves iterator cancellation when the metrics adapter wraps the token-bound engine.
#[test]
fn metered_engine_preserves_operation_token() {
    let token = Arc::new(TestCancellationToken::default());
    let engine = MeteredDeltaEngine::new(Arc::new(KernelEngine::new(
        &SyncEngine::new(),
        token.clone(),
    )));
    let mut iter = start_read(&engine, Read::Json, ReadApi::Plain, None).unwrap();
    token.cancel();
    assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
}

/// Correlates stage and snapshot failures under either ordering of the metrics and token wrappers.
#[rstest]
#[case("table-without-dv-small", Read::List)]
#[case("table-without-dv-small", Read::Json)]
#[case("crc-full", Read::Crc)]
fn cancelled_snapshot_reports_correlated_failure_with_metered_handlers(
    #[case] table: &str,
    #[case] read: Read,
    #[values(false, true)] meter_outside: bool,
) {
    let reads = RecordingReads::new(Some(read));
    let original = reads.engine();
    let engine: Box<dyn Engine> = if meter_outside {
        Box::new(MeteredDeltaEngine::new(Arc::new(KernelEngine::new(
            &original,
            reads.token.clone(),
        ))))
    } else {
        Box::new(KernelEngine::new(
            &MeteredDeltaEngine::new(Arc::new(original)),
            reads.token.clone(),
        ))
    };
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let result = Snapshot::builder_for(format!("tests/data/{table}"))
        .with_correlation_id("cancelled-snapshot")
        .build(engine.as_ref());
    assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");

    let events = reporter.events();
    let failure = events
        .iter()
        .filter_map(|event| match event {
            MetricEvent::SnapshotBuildFailure(failure) => Some(failure),
            _ => None,
        })
        .exactly_one()
        .unwrap();
    assert_eq!(
        failure.correlation_id.as_deref(),
        Some("cancelled-snapshot")
    );
    let stage_failure = events.iter().find_map(|event| match event {
        MetricEvent::LogSegmentLoadFailure(event) if read == Read::List => {
            Some((event.operation_id, event.correlation_id.as_deref()))
        }
        MetricEvent::ProtocolMetadataLoadFailure(event) if read != Read::List => {
            Some((event.operation_id, event.correlation_id.as_deref()))
        }
        _ => None,
    });
    assert_eq!(
        stage_failure,
        Some((failure.operation_id, Some("cancelled-snapshot"))),
        "{events:?}"
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, MetricEvent::SnapshotBuildSuccess(_))));
}

/// Checks nested read overrides, cumulative HEAD preflight, and unscoped storage writes.
#[test]
fn nested_reads_override_tokens_but_head_checks_each_wrapper_and_writes_stay_unscoped() {
    let engine = SyncEngine::new();
    let cancelled: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let cancelled_engine = KernelEngine::new(&engine, cancelled);
    let rebound_engine = KernelEngine::new(
        &cancelled_engine,
        Arc::new(TestCancellationToken::default()),
    );
    start_read(&rebound_engine, Read::Json, ReadApi::Plain, None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let path = Url::from_file_path(temp.path().join("data")).unwrap();
    cancelled_engine
        .storage_handler()
        .put(&path, Bytes::from_static(b"data"), false)
        .unwrap();
    let live_engine = KernelEngine::new(&engine, Arc::new(TestCancellationToken::default()));
    let metadata = live_engine.storage_handler().head(&path).unwrap();
    assert_eq!(metadata.location, path);
    assert_eq!(metadata.size, 4);
    assert!(matches!(
        cancelled_engine.storage_handler().head(&path),
        Err(KernelError::Cancelled)
    ));
    assert!(matches!(
        rebound_engine.storage_handler().head(&path),
        Err(KernelError::Cancelled)
    ));
    assert_eq!(
        engine
            .storage_handler()
            .read_files(vec![(path.clone(), None)])
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"data")
    );
    cancelled_engine.storage_handler().delete(&path).unwrap();
}

/// Round-trips parsed JSON and Parquet writes even when the read token is cancelled.
#[rstest]
fn parsing_and_writes_ignore_read_cancellation(#[values(false, true)] cancelled: bool) {
    let original = SyncEngine::new();
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let engine = KernelEngine::new(&original, token);
    let schema = schema_ref! { nullable "id": INTEGER };
    let data = || {
        engine
            .json_handler()
            .parse_json(
                string_array_to_engine_data(StringArray::from(vec![r#"{"id":7}"#])),
                schema.clone(),
            )
            .unwrap()
    };
    let temp = tempfile::tempdir().unwrap();
    let json_path = Url::from_file_path(temp.path().join("data.json")).unwrap();
    let json_size = engine
        .json_handler()
        .write_json_file(
            &json_path,
            Box::new(std::iter::once(Ok(
                FilteredEngineData::with_all_rows_selected(data()),
            ))),
            false,
        )
        .unwrap();
    let json_meta = original.storage_handler().head(&json_path).unwrap();
    assert_eq!(json_meta.size, json_size);
    assert_eq!(
        original
            .json_handler()
            .read_json_files(&[json_meta], schema.clone(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .len(),
        1,
    );

    let parquet_path = Url::from_file_path(temp.path().join("data.parquet")).unwrap();
    let parquet_size = engine
        .parquet_handler()
        .write_parquet_file(parquet_path.clone(), Box::new(std::iter::once(Ok(data()))))
        .unwrap();
    let parquet_meta = original.storage_handler().head(&parquet_path).unwrap();
    assert_eq!(parquet_meta.size, parquet_size);
    assert_eq!(
        original
            .parquet_handler()
            .read_parquet_files(&[parquet_meta], schema, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .len(),
        1,
    );
}

/// Preserves token identity and cancellation across full and incremental snapshot read paths.
#[rstest]
#[case::full_json("table-without-dv-small", Read::Json, false)]
#[case::full_crc("crc-full", Read::Crc, false)]
#[case::full_parquet("parquet_row_group_skipping", Read::Parquet, false)]
#[case::incremental_json("table-with-dv-small", Read::Json, true)]
#[case::incremental_parquet("parquet_row_group_skipping", Read::Parquet, true)]
fn snapshot_reads_keep_builder_token(
    #[case] table: &str,
    #[case] read: Read,
    #[case] incremental: bool,
    #[values(false, true)] cancel: bool,
    #[values(false, true)] bind_in_builder: bool,
) {
    let reads = RecordingReads::new(cancel.then_some(read));
    let path = format!("tests/data/{table}");
    let original = reads.engine();
    let scoped = KernelEngine::new(&original, reads.token.clone());
    let engine: &dyn Engine = if bind_in_builder { &original } else { &scoped };
    let token = bind_in_builder.then(|| reads.token.clone() as CancellationTokenRef);
    let result = if incremental {
        let base = Snapshot::builder_for(&path)
            .at_version(0)
            .build(reads.inner.as_ref())
            .unwrap();
        Snapshot::builder_from(base)
            .with_cancellation_token(token)
            .build(engine)
    } else {
        Snapshot::builder_for(&path)
            .with_cancellation_token(token)
            .build(engine)
    };
    let events = reads.events.lock().unwrap();
    assert!(events.contains(&read));
    if cancel {
        assert!(matches!(result, Err(KernelError::Cancelled)));
        assert_eq!(events.last(), Some(&read));
    } else if incremental {
        assert_eq!(result.unwrap().version(), 1);
    } else {
        let actual = result.unwrap();
        let expected = Snapshot::builder_for(&path)
            .build(reads.inner.as_ref())
            .unwrap();
        assert_eq!(actual.as_ref(), expected.as_ref());
    }
}

/// Creates a table with CRC at version zero and a newer commit to replay.
fn stale_crc_table() -> (Url, TempDir) {
    let (url, temp) = copy_test_table("crc-full").unwrap();
    std::fs::write(
        url.to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.json"),
        r#"{"commitInfo":{"operation":"WRITE"}}"#,
    )
    .unwrap();
    (url, temp)
}

/// Propagates cancellation at JSON read entry or after a batch during stale-CRC reconstruction.
#[rstest]
fn stale_crc_replay_keeps_builder_token(
    #[values(false, true)] incremental: bool,
    #[values(None, Some(false), Some(true))] cancel_after_batch: Option<bool>,
) {
    let (url, _temp) = stale_crc_table();
    let mut reads = RecordingReads::new((cancel_after_batch == Some(false)).then_some(Read::Json));
    Arc::get_mut(&mut reads).unwrap().cancel_json_after_batch = cancel_after_batch == Some(true);
    let engine = reads.engine();
    let token: CancellationTokenRef = reads.token.clone();
    let result = if incremental {
        let base = Snapshot::builder_for(&url)
            .at_version(0)
            .build(reads.inner.as_ref())
            .unwrap();
        Snapshot::builder_from(base)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .with_cancellation_token(token)
            .build(&engine)
    } else {
        Snapshot::builder_for(&url)
            .with_incremental_crc_replay(IncrementalReplay::Unlimited)
            .with_cancellation_token(token)
            .build(&engine)
    };
    if cancel_after_batch.is_some() {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        assert_eq!(result.unwrap().crc_at_version().unwrap().version, 1);
    }
    let events = reads.events.lock().unwrap();
    assert_eq!(events.iter().filter(|read| **read == Read::Json).count(), 1);
}

/// Stops on a cancelled newer CRC but replays malformed CRC content from the retained base.
#[rstest]
#[case::cancelled(true)]
#[case::malformed(false)]
fn newer_disk_crc_failure_with_retained_base(#[case] cancel_crc: bool) {
    let (url, _temp) = stale_crc_table();
    let base = Snapshot::builder_for(&url)
        .at_version(0)
        .build(&SyncEngine::new())
        .unwrap();
    assert_eq!(base.crc_at_version().unwrap().version, 0);
    std::fs::write(
        url.to_file_path()
            .unwrap()
            .join("_delta_log/00000000000000000001.crc"),
        b"malformed",
    )
    .unwrap();

    let mut reads = RecordingReads::new(cancel_crc.then_some(Read::Crc));
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let result = Snapshot::builder_from(base)
        .with_incremental_crc_replay(IncrementalReplay::Unlimited)
        .with_correlation_id("incremental-crc-failure")
        .build(&reads.engine());
    let read_events = reads.events.lock().unwrap();
    assert!(read_events.contains(&Read::Crc));
    if !cancel_crc {
        let updated = result.unwrap();
        assert_eq!(updated.version(), 1);
        assert_eq!(updated.crc_at_version().unwrap().version, 1);
        assert!(read_events.contains(&Read::Json));
        return;
    }

    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(read_events.last(), Some(&Read::Crc));
    assert!(!read_events.contains(&Read::Json));
    let events = reporter.events();
    let failures: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                MetricEvent::ProtocolMetadataLoadFailure(_) | MetricEvent::SnapshotBuildFailure(_)
            )
        })
        .collect();
    let [MetricEvent::ProtocolMetadataLoadFailure(pm), MetricEvent::SnapshotBuildFailure(build)] =
        failures.as_slice()
    else {
        panic!("expected one protocol/metadata and snapshot failure: {events:?}");
    };
    assert_eq!(pm.operation_id, build.operation_id);
    assert_eq!(
        pm.correlation_id.as_deref(),
        Some("incremental-crc-failure")
    );
    assert_eq!(pm.correlation_id, build.correlation_id);
    assert_eq!(pm.load_type, LogSegmentLoadType::Incremental);
    assert_eq!(build.load_type, SnapshotLoadType::Incremental);
    assert!(!events.iter().any(|event| matches!(
        event,
        MetricEvent::ProtocolMetadataLoadSuccess(_) | MetricEvent::SnapshotBuildSuccess(_)
    )));
}

/// Stops snapshot discovery when cancellation arrives at the checkpoint hint or listing read.
#[rstest]
#[case(Read::Hint)]
#[case(Read::List)]
fn discovery_keeps_builder_token(#[case] read: Read) {
    let reads = RecordingReads::new(Some(read));
    let token: CancellationTokenRef = reads.token.clone();
    let result = Snapshot::builder_for("tests/data/table-without-dv-small")
        .with_cancellation_token(token)
        .build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(reads.events.lock().unwrap().last(), Some(&read));
}

/// Propagates an engine-reported CRC cancellation even without a caller-supplied token.
#[test]
fn engine_reported_crc_cancellation_is_not_optional() {
    let mut reads = RecordingReads::new(Some(Read::Crc));
    Arc::get_mut(&mut reads).unwrap().expected_token = None;
    let result = Snapshot::builder_for("tests/data/crc-full").build(&reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)));
    assert_eq!(reads.events.lock().unwrap().last(), Some(&Read::Crc));
}

/// Releases the build token after success and permits cached reuse with a cancelled token.
#[test]
fn completed_snapshot_does_not_retain_build_scope() {
    let engine = SyncEngine::new();
    let cancelled: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    assert!(matches!(
        Snapshot::builder_for("tests/data/table-without-dv-small")
            .with_cancellation_token(cancelled)
            .build(&engine),
        Err(KernelError::Cancelled)
    ));
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::default());
    let weak = Arc::downgrade(&token);
    let snapshot = Snapshot::builder_for("tests/data/table-without-dv-small")
        .with_cancellation_token(token)
        .build(&engine)
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(snapshot.version(), 0);
    Snapshot::builder_from(snapshot)
        .at_version(0)
        .with_cancellation_token(
            Arc::new(TestCancellationToken::cancelled()) as CancellationTokenRef
        )
        .build(&engine)
        .unwrap();
}

/// Lazy snapshot metadata that requires log replay when no CRC is available.
#[derive(Clone, Copy, Debug)]
enum MetadataRead {
    Domain,
    Transaction,
    Clustering,
    ClusteringInfo,
    Domains,
    InternalDomain,
    UserDomains,
    RowTracking,
    Timestamp,
    InCommitTimestamp,
}

/// Forwards the reader's exact token through each metadata API and preserves unscoped reads.
#[rstest]
fn post_build_metadata_reads_inherit_operation_token(
    #[values(
        MetadataRead::Domain,
        MetadataRead::Transaction,
        MetadataRead::Clustering,
        MetadataRead::ClusteringInfo,
        MetadataRead::Domains,
        MetadataRead::InternalDomain,
        MetadataRead::UserDomains,
        MetadataRead::RowTracking,
        MetadataRead::Timestamp,
        MetadataRead::InCommitTimestamp
    )]
    read: MetadataRead,
    #[values(false, true)] cancelled: bool,
) {
    let original = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    create_table(
        "memory:///",
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .with_domain_metadata("test", "value")
    .with_transaction_id("app", 7)
    .with_table_properties([
        ("delta.enableInCommitTimestamps", "true"),
        ("delta.enableRowTracking", "true"),
    ])
    .with_data_layout(DataLayout::Clustered {
        columns: vec![ColumnName::new(["id"])],
    })
    .build(&original, Box::new(FileSystemCommitter::new()))
    .unwrap()
    .commit(&original)
    .unwrap()
    .unwrap_committed();
    let mut reads = RecordingReads::new(None);
    Arc::get_mut(&mut reads).unwrap().inner = Arc::new(original);
    let engine = reads.engine();
    let snapshot = Snapshot::builder_for("memory:///")
        .build(reads.inner.as_ref())
        .unwrap();
    assert!(snapshot.crc_at_version().is_none());
    reads.events.lock().unwrap().clear();
    if cancelled {
        reads.token.cancel();
    }
    let load = |reader: &crate::SnapshotReader<'_>, engine: &dyn Engine| -> KernelResult<()> {
        match read {
            MetadataRead::Domain => assert_eq!(
                reader.get_domain_metadata("test", engine)?,
                Some("value".into())
            ),
            MetadataRead::Transaction => {
                assert_eq!(reader.get_app_id_version("app", engine)?, Some(7));
            }
            MetadataRead::Clustering => assert_eq!(
                reader.get_physical_clustering_columns(engine)?,
                Some(vec![ColumnName::new(["id"])])
            ),
            MetadataRead::ClusteringInfo => {
                let infos = reader.get_clustering_column_infos(engine)?.unwrap();
                assert_eq!(infos[0].physical_column, ColumnName::new(["id"]));
            }
            MetadataRead::Domains => assert!(reader
                .get_domain_metadatas_internal(engine, None)?
                .contains_key("test")),
            MetadataRead::InternalDomain => assert!(reader
                .get_domain_metadata_internal("delta.clustering", engine)?
                .is_some()),
            MetadataRead::UserDomains => {
                assert_eq!(reader.get_all_domain_metadata(engine)?.len(), 1)
            }
            MetadataRead::RowTracking => assert_eq!(
                reader.get_row_tracking_high_water_mark(engine)?,
                Some(crate::ROW_TRACKING_INITIAL_HIGH_WATER_MARK)
            ),
            MetadataRead::Timestamp => assert!(reader.get_timestamp(engine)? > 0),
            MetadataRead::InCommitTimestamp => {
                assert!(reader.get_in_commit_timestamp(engine)?.is_some())
            }
        }
        Ok(())
    };
    let reader = snapshot
        .reader()
        .with_cancellation_token(reads.token.clone() as CancellationTokenRef);
    let result = load(&reader, &engine);
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [Read::Json]);
    load(&snapshot.reader(), reads.inner.as_ref()).unwrap();
}

/// Stops metadata replay at JSON or Parquet entry and on the next pull after a JSON batch.
#[rstest]
#[case::json_entry("app-txn-no-checkpoint", Read::Json, false)]
#[case::json_batch("app-txn-no-checkpoint", Read::Json, true)]
#[case::parquet_entry("app-txn-checkpoint", Read::Parquet, false)]
fn reader_cancellation_during_replay(
    #[case] table: &str,
    #[case] boundary: Read,
    #[case] after_batch: bool,
) {
    let mut reads = RecordingReads::new((!after_batch).then_some(boundary));
    Arc::get_mut(&mut reads).unwrap().cancel_json_after_batch = after_batch;
    let snapshot = Snapshot::builder_for(format!("tests/data/{table}"))
        .build(reads.inner.as_ref())
        .unwrap();
    let result = snapshot
        .reader()
        .with_cancellation_token(reads.token.clone() as CancellationTokenRef)
        .get_app_id_version("missing-app", &reads.engine());
    assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
    assert_eq!(reads.events.lock().unwrap().last(), Some(&boundary));
}

/// Keeps the reader token on tail-only metadata replay rooted in a stale complete CRC.
#[test]
fn reader_stale_crc_replay_is_cancellable() {
    let (url, _temp) = stale_crc_table();
    let reads = RecordingReads::new(Some(Read::Json));
    let snapshot = Snapshot::builder_for(&url)
        .with_incremental_crc_replay(IncrementalReplay::Disabled)
        .build(reads.inner.as_ref())
        .unwrap();
    assert!(snapshot.crc_at_version().is_none());
    let reader = snapshot
        .reader()
        .with_cancellation_token(reads.token.clone() as CancellationTokenRef);
    assert!(matches!(
        reader.get_domain_metadata("missing", &reads.engine()),
        Err(KernelError::Cancelled)
    ));
    assert_eq!(*reads.events.lock().unwrap(), [Read::Json]);
}

/// Leaves declarative protocol and metadata replay independent of the wrapper's read token.
#[cfg(feature = "declarative-plans")]
#[test]
fn declarative_replay_does_not_use_handler_cancellation() {
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for("tests/data/table-without-dv-small")
        .build(&engine)
        .unwrap();
    let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let engine = KernelEngine::new(&engine, token);
    let resolution = snapshot
        .log_segment()
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.metadata.is_some());
    assert!(resolution.protocol.is_some());
}
