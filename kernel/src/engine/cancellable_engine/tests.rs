//! Adapter token propagation, delegation, and iterator ownership contracts.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use itertools::Itertools;
use rstest::rstest;
use url::Url;

use crate::actions::LOG_METADATA_SCHEMA;
use crate::arrow::array::{Int32Array, StringArray};
use crate::committer::FileSystemCommitter;
use crate::engine::CancellableEngine;
use crate::engine::arrow_data::EngineDataArrowExt;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::metrics::{MeteredDeltaEngine, MetricEvent};
use crate::object_store::memory::InMemory;
use crate::schema::{ColumnName, schema_ref};
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::unit_test_utils::recording_reads::{Read, RecordingReads};
use crate::unit_test_utils::{
    CapturingReporter, TestCancellationToken, install_thread_local_metrics_reporter,
    string_array_to_engine_data,
};
use crate::utils::try_parse_uri;
use crate::{
    CancellationTokenRef, Engine, EngineData, FileMeta, FileSlice, FilteredEngineData, KernelError,
    KernelResult, KernelResultIterator, Snapshot, StorageHandler,
};

/// Handler entry point selected for a read.
#[derive(Clone, Copy)]
enum ReadApi {
    Default,
    WithCancellation,
}

/// Starts a fixture read and erases successful values for shared assertions.
fn start_read(
    engine: &dyn Engine,
    read: Read,
    api: ReadApi,
    token: Option<CancellationTokenRef>,
) -> KernelResult<Box<dyn KernelResultIterator<()>>> {
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
    let file = FileMeta::new(path.clone(), /* last_modified */ 0, /* size */ 0);
    let schema = LOG_METADATA_SCHEMA.clone();
    match (read, api) {
        (Read::List, ReadApi::Default) => {
            discard_read_values(engine.storage_handler().list_from(&path))
        }
        (Read::List, ReadApi::WithCancellation) => discard_read_values(
            engine
                .storage_handler()
                .list_from_with_cancellation(&path, token),
        ),
        (Read::Hint | Read::Crc, ReadApi::Default) => discard_read_values(
            engine
                .storage_handler()
                .read_files(vec![(path, /* byte_range */ None)]),
        ),
        (Read::Hint | Read::Crc, ReadApi::WithCancellation) => discard_read_values(
            engine
                .storage_handler()
                .read_files_with_cancellation(vec![(path, /* byte_range */ None)], token),
        ),
        (Read::Json, ReadApi::Default) => discard_read_values(
            engine
                .json_handler()
                .read_json_files(&[file], schema, /* predicate */ None),
        ),
        (Read::Json, ReadApi::WithCancellation) => {
            discard_read_values(engine.json_handler().read_json_files_with_cancellation(
                &[file],
                schema,
                /* predicate */ None,
                token,
            ))
        }
        (Read::Parquet, ReadApi::Default) => discard_read_values(
            engine
                .parquet_handler()
                .read_parquet_files(&[file], schema, /* predicate */ None),
        ),
        (Read::Parquet, ReadApi::WithCancellation) => discard_read_values(
            engine
                .parquet_handler()
                .read_parquet_files_with_cancellation(
                    &[file],
                    schema,
                    /* predicate */ None,
                    token,
                ),
        ),
        (Read::Footer, ReadApi::Default) => {
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
    result: KernelResult<Box<dyn KernelResultIterator<T>>>,
) -> KernelResult<Box<dyn KernelResultIterator<()>>> {
    Ok(Box::new(result?.map(|item| item.map(|_| ()))))
}

/// Checks that reads inherit the token bound to `CancellableEngine` unless explicitly overridden.
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
    #[values(ReadApi::Default, ReadApi::WithCancellation)] api: ReadApi,
    #[values(false, true)] cancelled: bool,
) {
    let reads = RecordingReads::new(None);
    if cancelled {
        reads.token.cancel();
    }
    let engine = CancellableEngine::new(&reads.engine(), reads.token.clone());
    let result = start_read(&engine, read, api, None)
        .and_then(|mut iter| iter.next().expect("fixture must produce a result"));
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)));
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [read]);

    let explicit = RecordingReads::new(None);
    let engine = CancellableEngine::new(
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
        let engine = CancellableEngine::new(&SyncEngine::new(), token.clone());
        start_read(&engine, read, ReadApi::Default, None).unwrap()
    };
    if cancel {
        token.cancel();
        assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
    } else {
        iter.next().unwrap().unwrap();
    }
}

/// Lets the builder replace a cancelled bound token without replacing the evaluator.
#[test]
fn builder_token_overrides_bound_read_token() {
    let reads = RecordingReads::new(None);
    let original = reads.engine();
    let engine = CancellableEngine::new(&original, Arc::new(TestCancellationToken::cancelled()));
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
    let engine = MeteredDeltaEngine::new(Arc::new(CancellableEngine::new(
        &SyncEngine::new(),
        token.clone(),
    )));
    let mut iter = start_read(&engine, Read::Json, ReadApi::Default, None).unwrap();
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
        Box::new(MeteredDeltaEngine::new(Arc::new(CancellableEngine::new(
            &original,
            reads.token.clone(),
        ))))
    } else {
        Box::new(CancellableEngine::new(
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
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, MetricEvent::SnapshotBuildSuccess(_)))
    );
}

/// Checks nested read overrides, cumulative HEAD preflight, and unscoped storage writes.
#[test]
fn nested_reads_override_tokens_but_head_checks_each_wrapper_and_writes_stay_unscoped() {
    let engine = SyncEngine::new();
    let cancelled: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
    let cancelled_engine = CancellableEngine::new(&engine, cancelled);
    let rebound_engine = CancellableEngine::new(
        &cancelled_engine,
        Arc::new(TestCancellationToken::default()),
    );
    start_read(&rebound_engine, Read::Json, ReadApi::Default, None)
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
    let live_engine = CancellableEngine::new(&engine, Arc::new(TestCancellationToken::default()));
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

#[rstest]
fn copy_atomic_forwards_arguments_and_result(
    #[values(false, true)] cancelled: bool,
    #[values(false, true)] fail: bool,
) {
    let storage = Arc::new(RecordingCopy {
        inner: SyncEngine::new().storage_handler(),
        calls: Mutex::new(Vec::new()),
        result: Mutex::new(Some(if fail {
            Err(KernelError::generic("copy failure"))
        } else {
            Ok(())
        })),
    });
    let original =
        DelegatingEngine::new(Arc::new(SyncEngine::new())).with_storage_handler(storage.clone());
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let engine = CancellableEngine::new(&original, token);
    let src = Url::parse("memory:///source").unwrap();
    let dest = Url::parse("memory:///destination").unwrap();
    let result = engine.storage_handler().copy_atomic(&src, &dest);
    if fail {
        assert!(matches!(result, Err(KernelError::Generic(message)) if message == "copy failure"));
    } else {
        result.unwrap();
    }
    assert_eq!(*storage.calls.lock().unwrap(), [(src, dest)]);
    assert!(storage.result.lock().unwrap().is_none());
}

/// Records atomic copies and supplies a caller-selected result.
struct RecordingCopy {
    /// Handler for storage operations other than atomic copies.
    inner: Arc<dyn StorageHandler>,
    /// Source and destination of each atomic copy.
    calls: Mutex<Vec<(Url, Url)>>,
    /// Result consumed by the single expected copy.
    result: Mutex<Option<KernelResult<()>>>,
}

impl StorageHandler for RecordingCopy {
    forward_methods! {
        fn list_from(path: &Url) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>>;
        fn read_files(files: Vec<FileSlice>) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>>;
        fn put(path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()>;
        fn head(path: &Url) -> KernelResult<FileMeta>;
        fn delete(path: &Url) -> KernelResult<()>;
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.calls.lock().unwrap().push((src.clone(), dest.clone()));
        self.result.lock().unwrap().take().unwrap()
    }
}

/// Round-trips parsed JSON and Parquet writes even when the read token is cancelled.
#[rstest]
fn parsing_and_writes_ignore_read_cancellation(#[values(false, true)] cancelled: bool) {
    let original = SyncEngine::new();
    let token = Arc::new(TestCancellationToken::default());
    if cancelled {
        token.cancel();
    }
    let engine = CancellableEngine::new(&original, token);
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
    assert_single_id(
        original
            .json_handler()
            .read_json_files(&[json_meta], schema.clone(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
    );

    let parquet_path = Url::from_file_path(temp.path().join("data.parquet")).unwrap();
    let parquet_size = engine
        .parquet_handler()
        .write_parquet_file(parquet_path.clone(), Box::new(std::iter::once(Ok(data()))))
        .unwrap();
    let parquet_meta = original.storage_handler().head(&parquet_path).unwrap();
    assert_eq!(parquet_meta.size, parquet_size);
    assert_single_id(
        original
            .parquet_handler()
            .read_parquet_files(&[parquet_meta], schema, None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap(),
    );
}

fn assert_single_id(data: Box<dyn EngineData>) {
    let batch = data.try_into_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids, &Int32Array::from(vec![7]));
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
    let scoped = CancellableEngine::new(&original, reads.token.clone());
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
}

/// Shares the operation token with lazy domain, transaction, and clustering metadata reads.
#[rstest]
fn post_build_metadata_reads_inherit_operation_token(
    #[values(
        MetadataRead::Domain,
        MetadataRead::Transaction,
        MetadataRead::Clustering
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
    let engine = CancellableEngine::new(&reads.engine(), reads.token.clone());
    let snapshot = Snapshot::builder_for("memory:///").build(&engine).unwrap();
    assert!(snapshot.crc_at_version().is_none());
    reads.events.lock().unwrap().clear();
    if cancelled {
        reads.token.cancel();
    }
    let load = |engine: &dyn Engine| -> KernelResult<()> {
        match read {
            MetadataRead::Domain => assert_eq!(
                snapshot.get_domain_metadata("test", engine)?,
                Some("value".into())
            ),
            MetadataRead::Transaction => {
                assert_eq!(snapshot.get_app_id_version("app", engine)?, Some(7));
            }
            MetadataRead::Clustering => assert_eq!(
                snapshot.get_physical_clustering_columns(engine)?,
                Some(vec![ColumnName::new(["id"])])
            ),
        }
        Ok(())
    };
    let result = load(&engine);
    if cancelled {
        assert!(matches!(result, Err(KernelError::Cancelled)), "{result:?}");
    } else {
        result.unwrap();
    }
    assert_eq!(*reads.events.lock().unwrap(), [Read::Json]);
    load(reads.inner.as_ref()).unwrap();
}

/// Checks that cancelling a JSON read after successful batches terminates its iterator.
#[rstest]
fn json_iterator_stops_after_mid_stream_cancellation(#[values(false, true)] cancel: bool) {
    let token = Arc::new(TestCancellationToken::default());
    let engine = CancellableEngine::new(&SyncEngine::new(), token.clone());
    let log = try_parse_uri("tests/data/table-with-cdf")
        .unwrap()
        .join("_delta_log/")
        .unwrap();
    let files: Vec<_> = (0..5)
        .map(|version| {
            FileMeta::new(
                log.join(&format!("{version:020}.json")).unwrap(),
                /* last_modified */ 0,
                /* size */ 0,
            )
        })
        .collect();
    let mut iter = engine
        .json_handler()
        .read_json_files(
            &files,
            LOG_METADATA_SCHEMA.clone(),
            /* predicate */ None,
        )
        .unwrap();
    for _ in 0..3 {
        iter.next()
            .expect("fixture must supply three successful batches")
            .unwrap();
    }
    if cancel {
        token.cancel();
        assert!(matches!(iter.next(), Some(Err(KernelError::Cancelled))));
    } else {
        iter.next()
            .expect("fixture must have more batches after the cancellation point")
            .unwrap();
        for batch in iter.by_ref() {
            batch.unwrap();
        }
    }
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
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
    let engine = CancellableEngine::new(&engine, token);
    let resolution = snapshot
        .log_segment()
        .read_protocol_metadata_opt(&engine, None)
        .unwrap();
    assert!(resolution.metadata.is_some());
    assert!(resolution.protocol.is_some());
}
