use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use rstest::rstest;
use tempfile::tempdir;
use tracing::field::Empty;
use tracing::info_span;
use url::Url;

use super::core::{self, Step, Task};
use super::engine::EngineConnector;
use super::kernel::generator::{Generator as _, GeneratorImpl};
use super::kernel::workflow::{Workflow as _, WorkflowImpl};
use super::kernel::Yielder;
use super::listing::ListingBounds;
use super::*;
use crate::engine::sync::SyncEngine;
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{install_thread_local_metrics_reporter, CapturingReporter};
use crate::Error;

/// Connector action after the toy workflow's single `ReadSmallFile` request.
#[derive(Clone, Copy)]
enum ReplyOutcome {
    Ok,
    Err,
    Drop,
}

enum CustomRequest {
    Length(String, Reply<usize>),
}

struct CustomChannel(core::Channel<CustomRequest>);

impl CustomChannel {
    async fn length(&self, value: String) -> DeltaResult<usize> {
        self.0
            .request(|reply| CustomRequest::Length(value, reply))
            .await
    }
}

#[test]
fn lazy_workflow_runs_only_after_the_caller_starts_it() {
    let ran = Arc::new(AtomicBool::new(false));
    let workflow_ran = Arc::clone(&ran);
    let workflow = WorkflowImpl::new(async move |_channel| {
        workflow_ran.store(true, Ordering::Relaxed);
        Ok(())
    });

    assert!(!ran.load(Ordering::Relaxed));
    let mut task = workflow.start();
    assert!(matches!(task.advance().unwrap(), WorkflowStep::Done(())));
    assert!(ran.load(Ordering::Relaxed));
}

#[test]
fn lazy_generator_runs_only_after_the_caller_starts_it() {
    let ran = Arc::new(AtomicBool::new(false));
    let generator_ran = Arc::clone(&ran);
    let generator = GeneratorImpl::new(async move |yielder: Yielder<'_, ()>| {
        generator_ran.store(true, Ordering::Relaxed);
        yielder.yield_item(()).await
    });

    assert!(!ran.load(Ordering::Relaxed));
    let GeneratorStep::Yield((), _) = generator.start().advance().unwrap() else {
        panic!("generator did not yield");
    };
    assert!(ran.load(Ordering::Relaxed));
}

#[test]
fn nested_lazy_workflow_uses_the_parent_request_channel() {
    let location = Url::parse("memory:///nested").unwrap();
    let child =
        WorkflowImpl::new(async move |channel| channel.read_small_file(location, None).await);
    let mut task = WorkflowImpl::new(async move |channel| child.run_with(channel).await).start();

    let WorkflowStep::Request(Request::ReadSmallFile(_, reply)) = task.advance().unwrap() else {
        panic!("nested workflow did not expose its read request");
    };
    reply.send(Ok(Bytes::from_static(b"response"))).unwrap();
    let WorkflowStep::Done(bytes) = task.advance().unwrap() else {
        panic!("nested workflow did not complete");
    };
    assert_eq!(bytes, Bytes::from_static(b"response"));
}

#[test]
fn workflow_task_retains_its_continuation_between_advances() {
    let location = Url::parse("memory:///async-task").unwrap();
    let workflow =
        WorkflowImpl::new(async move |channel| channel.read_small_file(location, None).await);
    let mut task = workflow.start();

    let WorkflowStep::Request(Request::ReadSmallFile(_, reply)) = task.advance().unwrap() else {
        panic!("workflow task did not produce its request");
    };
    let bytes = Ok(Bytes::from_static(b"response"));
    let WorkflowStep::Done(output) = task.resume(reply, bytes).unwrap() else {
        panic!("workflow task did not complete");
    };
    assert_eq!(output, Bytes::from_static(b"response"));
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
        let mut task =
            WorkflowImpl::new(async move |channel| channel.read_small_file(location, None).await)
                .start();
        let WorkflowStep::Request(Request::ReadSmallFile(_, reply)) = task.advance().unwrap()
        else {
            panic!("toy workflow should suspend once on ReadSmallFile");
        };
        match outcome {
            ReplyOutcome::Ok => {
                reply.send(Ok(Bytes::from_static(b"ok"))).unwrap();
                let _ = task.advance().unwrap();
            }
            ReplyOutcome::Err => {
                reply
                    .send(Err(Error::generic("connector failed the read")))
                    .unwrap();
                let _ = task.advance();
            }
            ReplyOutcome::Drop => drop(reply),
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
    let mut workflow = WorkflowImpl::new(async move |channel| {
        let bytes = channel.read_small_file(location, None).await?;
        Ok(format!("read {} bytes", bytes.len()))
    })
    .start();
    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ReadSmallFile((location, range), reply)) => {
                assert_eq!(location, expected_location);
                assert_eq!(range, None);
                reply.send(Ok(Bytes::from_static(b"answer"))).unwrap();
            }
            WorkflowStep::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        }
    };

    assert_eq!(output, "read 6 bytes");
}

#[test]
fn custom_workflow_uses_its_own_request_vocabulary() {
    let mut task = Task::single_lane(|channel| async move {
        let channel = CustomChannel(channel);
        let length = channel.length("custom request".to_string()).await?;
        Ok(length * 2)
    });

    let Step::Request(CustomRequest::Length(value, reply)) = task.advance().unwrap() else {
        panic!("custom workflow did not request string length");
    };
    assert_eq!(value, "custom request");

    reply.send(Ok(value.len())).unwrap();
    let Step::Done(output) = task.advance().unwrap() else {
        panic!("custom workflow did not complete");
    };
    assert_eq!(output, 28);
}

#[test]
fn connector_facing_generator_interleaves_requests_and_yields() {
    let location = Url::parse("memory:///item").unwrap();
    let generator = GeneratorImpl::new(async move |yielder| {
        let bytes = yielder.read_small_file(location, None).await?;
        yielder.yield_item(bytes.len()).await?;
        Ok("generator complete")
    });
    let mut generator = generator.start();

    let mut yielded = Vec::new();
    let output = loop {
        match generator.advance().unwrap() {
            GeneratorStep::Done(output) => break output,
            GeneratorStep::Yield(item, reply) => {
                yielded.push(item);
                reply.send(Ok(())).unwrap();
            }
            GeneratorStep::Request(Request::ReadSmallFile(_, reply)) => {
                reply
                    .send(Ok(Bytes::from_static(b"generated item")))
                    .unwrap();
            }
            GeneratorStep::Request(_) => {
                panic!("generator requested an unexpected operation")
            }
        }
    };

    assert_eq!(yielded, vec![14]);
    assert_eq!(output, "generator complete");
}

#[test]
fn yield_reply_error_is_delivered_to_generator() {
    let generator = GeneratorImpl::new(async |yielder| {
        let err = yielder.yield_item(1).await.unwrap_err();
        Ok(err.to_string())
    });
    let mut generator = generator.start();
    let GeneratorStep::Yield(1, reply) = generator.advance().unwrap() else {
        panic!("generator did not yield its item");
    };

    let err = Err(Error::generic("connector rejected yield"));
    let GeneratorStep::Done(output) = generator.resume(reply, err).unwrap() else {
        panic!("generator did not handle the yield error");
    };
    assert!(output.contains("connector rejected yield"));
}

#[test]
fn prepare_threads_opaque_state_to_continue() {
    let bounds = ListingBounds {
        prefix: Url::parse("memory:///").unwrap(),
        low: Url::parse("memory:///00000000000000000000").unwrap(),
        high: Url::parse("memory:///00000000000000000002").unwrap(),
    };
    let mut workflow = WorkflowImpl::new(async move |channel| {
        let cursor = channel.prepare_forward_listing(bounds).await?;
        let page = channel.continue_forward_listing(cursor).await?;
        assert!(page.next.is_none());
        Ok(page.data.len())
    })
    .start();

    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ListForward(PageRequest::Prepare(_, reply))) => {
                reply.send(Ok(Cursor::new(7_i64))).unwrap();
            }
            WorkflowStep::Request(Request::ListForward(PageRequest::Continue(cursor, reply))) => {
                assert_eq!(cursor.into_inner::<i64>().unwrap(), 7);
                reply.send(Ok(Page::new(Vec::new(), None))).unwrap();
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
        let child = GeneratorImpl::new(async move |yielder| {
            for location in [first, second] {
                let item = yielder.read_small_file(location, None).await?;
                yielder.yield_item(item).await?;
            }
            Ok(())
        });
        let mut child = child.bind(channel);
        let mut items = Vec::new();
        while let Some(item) = child.next().await? {
            items.push(item);
        }
        Ok(Report { items })
    }

    let mut connector_inputs = Vec::new();
    let mut workflow = WorkflowImpl::new(parent).start();
    let output = loop {
        match workflow.advance().unwrap() {
            WorkflowStep::Done(output) => break output,
            WorkflowStep::Request(Request::ReadSmallFile((location, None), reply)) => {
                connector_inputs.push(location.path().to_string());
                reply
                    .send(Ok(Bytes::copy_from_slice(location.path().as_bytes())))
                    .unwrap();
            }
            WorkflowStep::Request(Request::ReadSmallFile(..)) => {
                panic!("workflow unexpectedly requested a ranged read")
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
fn pending_without_connector_work_fails_instead_of_hanging() {
    let mut task = WorkflowImpl::new(async |_channel| {
        pending::<()>().await;
        Ok(())
    })
    .start();
    let result = task.advance();

    let Err(err) = result else {
        panic!("unsupported pending future unexpectedly started");
    };
    assert!(err.to_string().contains("Pending without a live request"));
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
        .run_body(async move |channel| {
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

    let generator = GeneratorImpl::new(async move |yielder| {
        for version in 1..=3 {
            yielder
                .write_bytes(
                    root.join(&format!("{version:020}.json"))?,
                    Bytes::new(),
                    false,
                )
                .await?;
        }
        let mut page = yielder
            .start_forward_listing(ListingBounds {
                prefix: root.clone(),
                low: root.join("00000000000000000000")?,
                high: root.join("00000000000000000004")?,
            })
            .await?;
        loop {
            for entry in page.data {
                yielder.yield_item(entry?.location).await?;
            }
            let Some(next) = page.next else {
                return Ok(());
            };
            page = yielder.continue_forward_listing(next).await?;
        }
    });
    let actual = connector
        .iterate_generator(generator)
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
        .run_body(async move |channel| {
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
