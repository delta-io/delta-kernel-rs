use std::borrow::Borrow;
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use rstest::rstest;
use tempfile::tempdir;
use tracing::field::Empty;
use tracing::info_span;
use url::Url;

use super::engine::{drive_workflow, run_workflow_with_connector, EngineConnector};
use super::generator::{generator, static_generator};
use super::kernel::{workflow, Channel};
use super::listing::{BackwardListing, ForwardListing, ListingBounds};
use super::*;
use crate::engine::sync::SyncEngine;
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{
    assert_result_error_with_message, install_thread_local_metrics_reporter, CapturingReporter,
};
use crate::{DeltaResultIteratorStatic, Error};

/// Connector action after the toy workflow's single `ReadSmallFile` request.
#[derive(Clone, Copy)]
enum ReplyOutcome {
    Ok,
    Err,
    Drop,
}

#[test]
fn lazy_workflow_runs_only_after_the_caller_requests_a_step() {
    let ran = Arc::new(AtomicBool::new(false));
    let workflow_ran = Arc::clone(&ran);
    let mut workflow = workflow!(async move |_channel| {
        workflow_ran.store(true, Ordering::Relaxed);
        Ok(())
    });

    assert!(!ran.load(Ordering::Relaxed));
    assert!(matches!(
        workflow.advance().unwrap(),
        WorkflowStep::Done(())
    ));
    assert!(ran.load(Ordering::Relaxed));
}

#[test]
fn lazy_generator_runs_only_after_the_caller_starts_it() {
    let ran = Arc::new(AtomicBool::new(false));
    let generator_ran = Arc::clone(&ran);
    let mut generator = static_generator!(Request, async move |channel| {
        generator_ran.store(true, Ordering::Relaxed);
        channel.yield_item(()).await
    });

    assert!(!ran.load(Ordering::Relaxed));
    let GeneratorStep::Yield(()) = generator.advance().unwrap() else {
        panic!("generator did not yield");
    };
    assert!(ran.load(Ordering::Relaxed));
}

#[test]
fn iterator_generator_is_lazy_and_stops_at_first_error() {
    let polls = Arc::new(AtomicUsize::new(0));
    let iterator_polls = Arc::clone(&polls);
    let items: DeltaResultIteratorStatic<usize> = Box::new(std::iter::from_fn(move || {
        match iterator_polls.fetch_add(1, Ordering::Relaxed) {
            0 => Some(Ok(1)),
            1 => Some(Err(Error::generic("iterator failed"))),
            _ => panic!("generator polled past the first error"),
        }
    }));
    let mut generator = StaticGenerator::from_iterator(items);

    assert_eq!(polls.load(Ordering::Relaxed), 0);
    let GeneratorStep::Yield(item) = generator.advance().unwrap() else {
        panic!("generator did not yield the first item");
    };
    assert_eq!(item, 1);
    assert_eq!(polls.load(Ordering::Relaxed), 1);

    assert_result_error_with_message(generator.advance(), "iterator failed");
    assert_eq!(polls.load(Ordering::Relaxed), 2);
}

#[test]
fn nested_async_operation_uses_the_parent_request_channel() {
    async fn child(channel: &Channel, location: Url) -> DeltaResult<Bytes> {
        channel.read_small_file(location, None).await
    }

    let location = Url::parse("memory:///nested").unwrap();
    let mut workflow = workflow!(async move |channel| child(channel, location).await);

    let WorkflowStep::Request(Request::ReadSmallFile(_file, reply)) = workflow.advance().unwrap()
    else {
        panic!("nested workflow did not expose its read request");
    };
    reply.send(Ok(Bytes::from_static(b"response")));
    let WorkflowStep::Done(bytes) = workflow.advance().unwrap() else {
        panic!("nested workflow did not complete");
    };
    assert_eq!(bytes, Bytes::from_static(b"response"));
}

#[test]
fn workflow_retains_handle_between_advances() {
    let location = Url::parse("memory:///async-task").unwrap();
    let mut workflow =
        workflow!(async move |channel| { channel.read_small_file(location, None).await });

    let WorkflowStep::Request(Request::ReadSmallFile(_file, reply)) = workflow.advance().unwrap()
    else {
        panic!("workflow did not produce its request");
    };
    let bytes = Ok(Bytes::from_static(b"response"));
    reply.send(bytes);
    let WorkflowStep::Done(output) = workflow.advance().unwrap() else {
        panic!("workflow did not complete");
    };
    assert_eq!(output, Bytes::from_static(b"response"));
    assert_result_error_with_message(workflow.advance(), "advanced after completion");
}

#[test]
fn workflow_advance_before_reply_fails() {
    let location = Url::parse("memory:///async-task").unwrap();
    let mut workflow =
        workflow!(async move |channel| { channel.read_small_file(location, None).await });
    let WorkflowStep::Request(Request::ReadSmallFile(_file, reply)) = workflow.advance().unwrap()
    else {
        panic!("workflow did not produce its request");
    };
    assert_result_error_with_message(
        workflow.advance(),
        "coroutine suspended without posting a request",
    );
    reply.send(Ok(Bytes::new()));
    assert!(matches!(workflow.advance().unwrap(), WorkflowStep::Done(_)));
}

#[rstest]
#[case::ok(ReplyOutcome::Ok)]
#[case::err(ReplyOutcome::Err)]
#[case::drop(ReplyOutcome::Drop)]
fn reporting_span_tracks_single_reply_outcome(#[case] outcome: ReplyOutcome) {
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    {
        // Any lifecycle span with `report` works; this is not a CRC-read test.
        let span = info_span!("crc_read_completed", report = Empty);
        let _enter = span.enter();
        let location = Url::parse("memory:///toy").unwrap();
        let mut workflow =
            workflow!(async move |channel| channel.read_small_file(location, None).await);
        let WorkflowStep::Request(Request::ReadSmallFile(_file, reply)) =
            workflow.advance().unwrap()
        else {
            panic!("toy workflow should suspend once on ReadSmallFile");
        };
        match outcome {
            ReplyOutcome::Ok => {
                reply.send(Ok(Bytes::from_static(b"ok")));
                let _ = workflow.advance().unwrap();
            }
            ReplyOutcome::Err => {
                reply.send(Err(Error::generic("connector failed the read")));
                let _ = workflow.advance();
            }
            ReplyOutcome::Drop => {
                drop(reply);
                drop(workflow);
            }
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
        ReplyOutcome::Ok => {
            assert!(success, "expected CrcReadSuccess; got: {events:?}");
            assert!(!failure, "did not expect CrcReadFailure; got: {events:?}");
        }
        ReplyOutcome::Err | ReplyOutcome::Drop => {
            assert!(failure, "expected CrcReadFailure; got: {events:?}");
            assert!(!success, "did not expect CrcReadSuccess; got: {events:?}");
        }
    }
}

#[test]
fn workflow_output_is_independent_of_request_response_type() {
    let location = Url::parse("memory:///answer").unwrap();
    let expected_location = location.clone();
    let mut workflow = workflow!(async move |channel| {
        let bytes = channel.read_small_file(location, None).await?;
        Ok(format!("read {} bytes", bytes.len()))
    });
    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ReadSmallFile((location, range), reply)) => {
                assert_eq!(location, expected_location);
                assert_eq!(range, None);
                reply.send(Ok(Bytes::from_static(b"answer")));
            }
            WorkflowStep::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        }
    };

    assert_eq!(output, "read 6 bytes");
}

#[test]
fn connector_facing_generator_interleaves_requests_and_yields() {
    let location = Url::parse("memory:///item").unwrap();
    let mut generator = static_generator!(Request, async move |channel| {
        let bytes = channel.read_small_file(location, None).await?;
        channel.yield_item(bytes.len()).await?;
        Ok(())
    });

    let mut yielded = Vec::new();
    loop {
        match generator.advance().unwrap() {
            GeneratorStep::Done => break,
            GeneratorStep::Yield(item) => yielded.push(item),
            GeneratorStep::Request(Request::ReadSmallFile(_file, reply)) => {
                reply.send(Ok(Bytes::from_static(b"generated item")));
            }
            GeneratorStep::Request(_) => {
                panic!("generator requested an unexpected operation")
            }
        }
    }

    assert_eq!(yielded, vec![14]);
}

#[test]
fn prepare_threads_opaque_state_to_continue() {
    let bounds = ListingBounds {
        prefix: Url::parse("memory:///").unwrap(),
        low: Url::parse("memory:///00000000000000000000").unwrap(),
        high: Url::parse("memory:///00000000000000000002").unwrap(),
    };
    let mut workflow = workflow!(async move |channel| {
        let cursor = channel.prepare_paged(ForwardListing::new(bounds)).await?;
        let page = channel.continue_paged(cursor).await?;
        assert!(page.next.is_none());
        Ok(page.data.len())
    });

    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ListForward(PageRequest::Prepare(
                _operation,
                reply,
            ))) => {
                reply.send(Ok(Cursor::new(7_i64)));
            }
            WorkflowStep::Request(Request::ListForward(PageRequest::Continue(cursor, reply))) => {
                assert_eq!(cursor.into_inner::<i64>().unwrap(), 7);
                reply.send(Ok(Page::new(Vec::new(), None)));
            }
            WorkflowStep::Request(Request::ListForward(PageRequest::Start(..))) => {
                panic!("workflow unexpectedly started listing eagerly")
            }
            WorkflowStep::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        }
    };

    assert_eq!(output, 0);
}

#[test]
fn parent_intercepts_child_items_while_child_io_reaches_connector() {
    #[derive(Debug, PartialEq, Eq)]
    struct Report {
        items: Vec<Bytes>,
    }

    async fn parent(channel: &Channel) -> DeltaResult<Report> {
        let first = Url::parse("memory:///first")?;
        let second = Url::parse("memory:///second")?;
        let mut child = generator!(channel, async move |channel| {
            for location in [first, second] {
                let item = channel.read_small_file(location, None).await?;
                channel.yield_item(item).await?;
            }
            Ok(())
        });
        let mut items = Vec::new();
        while let Some(item) = child.next().await? {
            items.push(item);
        }
        Ok(Report { items })
    }

    let mut connector_inputs = Vec::new();
    let mut workflow = workflow!(parent);
    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ReadSmallFile((location, range), reply)) => {
                assert_eq!(range, None);
                connector_inputs.push(location.path().to_string());
                reply.send(Ok(Bytes::copy_from_slice(location.path().as_bytes())));
            }
            WorkflowStep::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        }
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
fn channel_taking_generator_supports_static_and_borrowed_state() {
    fn operation<'task>(
        channel: impl Borrow<Channel> + Send + 'task,
        prefix: impl Borrow<str> + Send + 'task,
    ) -> Generator<'task, String> {
        generator!(channel, async move |channel| {
            channel
                .yield_item(format!("{} item", prefix.borrow()))
                .await?;
            assert!(!prefix.borrow().is_empty());
            Ok(())
        })
    }

    let mut generator =
        StaticGenerator::new(move |channel| operation(channel, String::from("root")));
    let GeneratorStep::Yield(item) = generator.advance().unwrap() else {
        panic!("static generator did not yield");
    };
    assert_eq!(item, "root item");
    let GeneratorStep::Done = generator.advance().unwrap() else {
        panic!("static generator did not complete");
    };
    assert_result_error_with_message(generator.advance(), "advanced after completion");

    async fn parent(channel: &Channel) -> DeltaResult<Vec<String>> {
        let prefix = "scoped".to_string();
        let prefix = prefix.as_str();
        let mut child = operation(channel, prefix);
        let mut items = Vec::new();
        while let Some(item) = child.next().await? {
            items.push(item);
        }
        Ok(items)
    }

    let mut workflow = workflow!(parent);
    let WorkflowStep::Done(items) = workflow.advance().unwrap() else {
        panic!("parent workflow did not complete");
    };
    assert_eq!(items, ["scoped item"]);
}

#[test]
fn static_generator_advance_before_request_reply_fails() {
    let location = Url::parse("memory:///item").unwrap();
    let mut generator: StaticGenerator<()> = static_generator!(Request, async move |channel| {
        channel.read_small_file(location, None).await?;
        Ok(())
    });
    let GeneratorStep::Request(Request::ReadSmallFile(_file, reply)) = generator.advance().unwrap()
    else {
        panic!("generator did not request a read");
    };
    assert_result_error_with_message(
        generator.advance(),
        "coroutine suspended without posting a request",
    );
    reply.send(Ok(Bytes::new()));
    assert!(matches!(generator.advance().unwrap(), GeneratorStep::Done));
}

#[test]
fn engine_workflow_can_borrow_caller_state() {
    let engine = SyncEngine::new();
    let value = "scoped".to_string();
    let value = value.as_str();
    let workflow = workflow!(async move |_channel| Ok::<_, Error>(value.len()));
    let length = drive_workflow(&engine, workflow).unwrap();

    assert_eq!(length, 6);
}

#[test]
fn pending_without_connector_work_fails() {
    let mut workflow = workflow!(async |_channel| {
        pending::<()>().await;
        Ok(())
    });
    assert_result_error_with_message(
        workflow.advance(),
        "coroutine suspended without posting a request",
    );
}

#[test]
fn engine_connector_drives_real_storage_operations() {
    let temp_dir = tempdir().unwrap();
    let location = Url::from_file_path(temp_dir.path().join("data.bin")).unwrap();
    let expected = Bytes::from_static(b"kernel coroutine");
    let write_data = expected.clone();

    let workflow = workflow!(async move |channel| {
        channel
            .write_bytes(location.clone(), write_data, false)
            .await?;
        channel.read_small_file(location, None).await
    });
    let connector = EngineConnector::new(&SyncEngine::new());
    let actual = connector.drive_workflow(workflow).unwrap();
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

    let generator = static_generator!(Request, async move |channel| {
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
            .start_paged(ForwardListing::new(ListingBounds {
                prefix: root.clone(),
                low: root.join("00000000000000000000")?,
                high: root.join("00000000000000000004")?,
            }))
            .await?;
        loop {
            for entry in page.data {
                channel.yield_item(entry?.location).await?;
            }
            let Some(next) = page.next else {
                return Ok(());
            };
            page = channel.continue_paged(next).await?;
        }
    });
    let actual = connector
        .iterate_generator(generator)
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

    let actual = run_workflow_with_connector!(connector, async move |channel| {
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
            .start_paged(BackwardListing::new(ListingBounds {
                prefix: root.clone(),
                low: root.join("00000000000000000000")?,
                high: root.join("00000000000000000004")?,
            }))
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
            page = channel.continue_paged(next).await?;
        }
        Ok(files)
    })
    .unwrap();

    assert_eq!(actual, expected);
}
