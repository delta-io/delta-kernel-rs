use std::future::pending;
use std::sync::Arc;

use rstest::rstest;
use tempfile::tempdir;
use tracing::field::Empty;
use tracing::info_span;

use super::*;
use crate::coroutine::engine::EngineConnector;
use crate::engine::sync::SyncEngine;
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{install_thread_local_metrics_reporter, CapturingReporter};

/// Connector action after the toy workflow's single `ReadSmallFile` request.
#[derive(Clone, Copy)]
enum ResumeOutcome {
    Ok,
    Err,
    Drop,
}

#[rstest]
#[case::ok(ResumeOutcome::Ok)]
#[case::err(ResumeOutcome::Err)]
#[case::drop(ResumeOutcome::Drop)]
fn reporting_span_tracks_single_resume_outcome(#[case] outcome: ResumeOutcome) {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    {
        // Any lifecycle span with `report` works; this is not a CRC-read test.
        let span = info_span!("crc_read_completed", report = Empty);
        let _enter = span.enter();
        let location = Url::parse("memory:///toy").unwrap();
        let Workflow::Request(Request::ReadSmallFile(_, resume)) =
            Workflow::start(async move |channel| channel.read_small_file(location, None).await)
                .unwrap()
        else {
            panic!("toy workflow should suspend once on ReadSmallFile");
        };
        match outcome {
            ResumeOutcome::Ok => {
                resume.resume(Ok(Bytes::from_static(b"ok"))).unwrap();
            }
            ResumeOutcome::Err => {
                let _ = resume.resume(Err(Error::generic("connector failed the read")));
            }
            ResumeOutcome::Drop => drop(resume),
        }
    }

    let events = reporter.events();
    let success = events
        .iter()
        .any(|e| matches!(e, MetricEvent::CrcReadSuccess(_)));
    let failure = events
        .iter()
        .any(|e| matches!(e, MetricEvent::CrcReadFailure));
    match outcome {
        ResumeOutcome::Ok => {
            assert!(success, "expected CrcReadSuccess; got: {events:?}");
            assert!(!failure, "did not expect CrcReadFailure; got: {events:?}");
        }
        ResumeOutcome::Err | ResumeOutcome::Drop => {
            assert!(failure, "expected CrcReadFailure; got: {events:?}");
            assert!(!success, "did not expect CrcReadSuccess; got: {events:?}");
        }
    }
}

#[test]
fn workflow_output_is_independent_of_request_response_type() {
    let location = Url::parse("memory:///answer").unwrap();
    let expected_location = location.clone();
    let mut workflow = Workflow::start(async move |channel| {
        let bytes = channel.read_small_file(location, None).await?;
        Ok(format!("read {} bytes", bytes.len()))
    })
    .unwrap();
    let output = loop {
        workflow = match workflow {
            Workflow::Done(output) => break output,
            Workflow::Request(Request::ReadSmallFile((location, range), resume)) => {
                assert_eq!(location, expected_location);
                assert_eq!(range, None);
                resume.resume(Ok(Bytes::from_static(b"answer"))).unwrap()
            }
            Workflow::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        };
    };

    assert_eq!(output, "read 6 bytes");
}

#[test]
fn connector_facing_generator_interleaves_requests_and_yields() {
    let location = Url::parse("memory:///item").unwrap();
    let mut generator = Generator::start(async move |channel| {
        let bytes = channel.read_small_file(location, None).await?;
        channel.yield_item(bytes.len()).await?;
        Ok("generator complete")
    })
    .unwrap();

    let mut yielded = Vec::new();
    let output = loop {
        generator = match generator {
            Generator::Done(output) => break output,
            Generator::Yield(item, resume) => {
                yielded.push(item);
                resume.resume(Ok(())).unwrap()
            }
            Generator::Request(Request::ReadSmallFile(_, resume)) => resume
                .resume(Ok(Bytes::from_static(b"generated item")))
                .unwrap(),
            Generator::Request(_) => panic!("generator requested an unexpected operation"),
        };
    };

    assert_eq!(yielded, vec![14]);
    assert_eq!(output, "generator complete");
}

#[test]
fn yield_resume_error_is_delivered_to_generator() {
    let generator = Generator::start(async |channel| {
        let err = channel.yield_item(1).await.unwrap_err();
        Ok(err.to_string())
    })
    .unwrap();
    let Generator::Yield(1, resume) = generator else {
        panic!("generator did not yield its item");
    };

    let generator = resume
        .resume(Err(Error::generic("connector rejected yield")))
        .unwrap();
    let Generator::Done(output) = generator else {
        panic!("generator did not handle the yield error");
    };

    assert!(output.contains("connector rejected yield"));
}

#[test]
fn prepare_threads_an_opaque_id_to_continue() {
    let bounds = ListingBounds {
        prefix: Url::parse("memory:///").unwrap(),
        low: Url::parse("memory:///00000000000000000000").unwrap(),
        high: Url::parse("memory:///00000000000000000002").unwrap(),
    };
    let mut workflow = Workflow::start(async move |channel| {
        let cursor = channel.prepare_forward_listing(bounds).await?;
        let page = channel.continue_forward_listing(cursor).await?;
        assert!(page.next.is_none());
        Ok(page.data.len())
    })
    .unwrap();

    let output = loop {
        workflow = match workflow {
            Workflow::Done(output) => break output,
            Workflow::Request(Request::ListForward(PageRequest::Prepare(_, resume))) => {
                resume.resume(Ok(Cursor::id(7))).unwrap()
            }
            Workflow::Request(Request::ListForward(PageRequest::Continue(
                Cursor {
                    state: CursorState::Id(id),
                    ..
                },
                resume,
            ))) => {
                assert_eq!(id, 7);
                resume
                    .resume(Ok(Page {
                        data: Vec::new(),
                        next: None,
                    }))
                    .unwrap()
            }
            Workflow::Request(Request::ListForward(PageRequest::Start(..))) => {
                panic!("workflow unexpectedly started listing eagerly")
            }
            Workflow::Request(_) => panic!("workflow requested an unexpected operation"),
        };
    };

    assert_eq!(output, 0);
}

#[test]
fn parent_intercepts_child_items_while_child_io_reaches_connector() {
    #[derive(Debug, PartialEq, Eq)]
    struct Report {
        items: Vec<Bytes>,
    }

    async fn child(channel: Yielder<Bytes>) -> DeltaResult<()> {
        let first = Url::parse("memory:///first")?;
        channel
            .yield_item(channel.read_small_file(first, None).await?)
            .await?;
        let second = Url::parse("memory:///second")?;
        channel
            .yield_item(channel.read_small_file(second, None).await?)
            .await?;
        Ok(())
    }

    async fn parent(channel: Channel) -> DeltaResult<Report> {
        let mut child = GeneratorState::Start(Generator::start(child)?);
        let mut items = Vec::new();
        while let Some(item) = child.next(&channel).await? {
            items.push(item);
        }
        Ok(Report { items })
    }

    let mut connector_inputs = Vec::new();
    let mut workflow = Workflow::start(parent).unwrap();
    let output = loop {
        workflow = match workflow {
            Workflow::Done(output) => break output,
            Workflow::Request(Request::ReadSmallFile((location, None), resume)) => {
                connector_inputs.push(location.path().to_string());
                resume
                    .resume(Ok(Bytes::copy_from_slice(location.path().as_bytes())))
                    .unwrap()
            }
            Workflow::Request(Request::ReadSmallFile(..)) => {
                panic!("workflow unexpectedly requested a ranged read")
            }
            Workflow::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        };
    };

    assert_eq!(connector_inputs, vec!["/first", "/second"]);
    assert_eq!(
        output,
        Report {
            items: vec![
                Bytes::from_static(b"/first"),
                Bytes::from_static(b"/second"),
            ],
        }
    );
}

#[test]
fn pending_without_connector_work_fails_instead_of_hanging() {
    let result = Workflow::start(async |_channel| {
        pending::<()>().await;
        Ok(())
    });

    let Err(err) = result else {
        panic!("unsupported pending future unexpectedly started");
    };
    assert!(err
        .to_string()
        .contains("Pending without a live connector request"));
}

#[test]
fn stale_weak_mailbox_entries_do_not_block_reuse() {
    let core = RequestMailbox::default();
    let abandoned = Arc::new(Exchange::new((
        Url::parse("memory:///abandoned").unwrap(),
        None,
    )));
    core.publish(PendingRequest::ReadSmallFile(Arc::downgrade(&abandoned)))
        .unwrap();
    drop(abandoned);

    let live = Arc::new(Exchange::new((Url::parse("memory:///live").unwrap(), None)));
    core.publish(PendingRequest::ReadSmallFile(Arc::downgrade(&live)))
        .unwrap();
    core.take_pending().unwrap();
}

#[test]
fn engine_connector_drives_real_storage_operations() {
    let temp_dir = tempdir().unwrap();
    let location = Url::from_file_path(temp_dir.path().join("data.bin")).unwrap();
    let expected = Bytes::from_static(b"kernel coroutine");
    let write_data = expected.clone();
    let sync_engine = SyncEngine::new();
    let connector = EngineConnector::new(&sync_engine);

    let actual = connector
        .run(async move |channel| {
            channel
                .write_bytes(location.clone(), write_data, false)
                .await?;
            channel.read_small_file(location, None).await
        })
        .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn engine_connector_pages_forward_listing() {
    let temp_dir = tempdir().unwrap();
    let root = Url::from_directory_path(temp_dir.path()).unwrap();
    let expected: Vec<_> = (1..=3)
        .map(|version| root.join(&format!("{version:020}.json")).unwrap())
        .collect();
    let sync_engine = SyncEngine::new();
    let connector = EngineConnector::new(&sync_engine).with_cancellation_token(None);

    let actual = connector
        .iterate_generator(Generator::start(async move |channel| {
            for version in 1..=3 {
                channel
                    .write_bytes(
                        root.join(&format!("{version:020}.json"))?,
                        Bytes::new(),
                        false,
                    )
                    .await?;
            }
            let mut page = channel
                .start_forward_listing(ListingBounds {
                    prefix: root.clone(),
                    low: root.join("00000000000000000000")?,
                    high: root.join("00000000000000000004")?,
                })
                .await?;
            loop {
                for entry in page.data {
                    channel.yield_item(entry?.location).await?;
                }
                let Some(next) = page.next else {
                    break;
                };
                page = channel.continue_forward_listing(next).await?;
            }
            Ok(())
        }))
        .unwrap()
        .collect::<DeltaResult<Vec<_>>>()
        .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn engine_connector_pages_backward_listing() {
    let temp_dir = tempdir().unwrap();
    let root = Url::from_directory_path(temp_dir.path()).unwrap();
    let expected: Vec<_> = (1..=3)
        .map(|version| root.join(&format!("{version:020}.json")).unwrap())
        .collect();
    let sync_engine = SyncEngine::new();
    let connector = EngineConnector::new(&sync_engine);

    let actual = connector
        .run(async move |channel| {
            for version in 1..=3 {
                channel
                    .write_bytes(
                        root.join(&format!("{version:020}.json"))?,
                        Bytes::new(),
                        false,
                    )
                    .await?;
            }
            let mut page = channel
                .start_backward_listing(ListingBounds {
                    prefix: root.clone(),
                    low: root.join("00000000000000000000")?,
                    high: root.join("00000000000000000004")?,
                })
                .await?;
            let mut files = Vec::new();
            loop {
                assert!(page.data.known_version_boundary);
                for entry in page.data.entries {
                    files.push(entry?.location);
                }
                let Some(next) = page.next else {
                    break;
                };
                page = channel.continue_backward_listing(next).await?;
            }
            Ok(files)
        })
        .unwrap();

    assert_eq!(actual, expected);
}
