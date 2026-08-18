use std::future::pending;
use std::sync::Arc;

use bytes::Bytes;
use derive_more::{Deref, From};
use rstest::rstest;
use tempfile::tempdir;
use tracing::field::Empty;
use tracing::info_span;
use url::Url;

use super::core::{
    self, DeltaFuture, GeneratorTask, IntoRequest, OutboxEntry, Pending, Step, WorkflowTask,
    YieldChannel,
};
use super::engine::EngineConnector;
use super::listing::ListingBounds;
use super::*;
use crate::engine::sync::SyncEngine;
use crate::metrics::MetricEvent;
use crate::unit_test_utils::{install_thread_local_metrics_reporter, CapturingReporter};
use crate::Error;

/// Connector action after the toy workflow's single `ReadSmallFile` request.
#[derive(Clone, Copy)]
enum ResumeOutcome {
    Ok,
    Err,
    Drop,
}

enum CustomPending {
    Length(Pending<String, usize>),
}

impl Default for CustomPending {
    fn default() -> Self {
        Self::Length(Pending::default())
    }
}

impl OutboxEntry for CustomPending {
    fn is_live(&self) -> bool {
        match self {
            Self::Length(pending) => pending.is_live(),
        }
    }
}

enum CustomRequest<N: Send + 'static> {
    Length(String, Resume<N, usize>),
}

impl<N: Send + 'static> IntoRequest<N> for CustomPending {
    type Request = CustomRequest<N>;

    fn into_request(self, step: impl Step<N>) -> DeltaResult<Self::Request> {
        match self {
            Self::Length(pending) => pending.into_request(step, CustomRequest::Length),
        }
    }
}

struct CustomChannel(core::Channel<CustomPending>);

impl CustomChannel {
    async fn length(&self, value: String) -> DeltaResult<usize> {
        self.0.exchange(value, CustomPending::Length).await
    }
}

#[derive(From)]
enum CustomWorkflow<O: Send + 'static> {
    Done(O),
    Request(CustomRequest<Self>),
}

impl<O: Send + 'static> CustomWorkflow<O> {
    fn start<Fut>(workflow: impl FnOnce(CustomChannel) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let task = WorkflowTask::new(|channel| workflow(CustomChannel(channel)));
        task.step()
    }
}

#[derive(Deref)]
struct CustomYielder<Y: Send + 'static> {
    #[deref]
    channel: CustomChannel,
    yields: YieldChannel<Y>,
}

impl<Y: Send + 'static> CustomYielder<Y> {
    async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        self.yields.yield_item(item).await
    }
}

#[derive(From)]
enum CustomGenerator<Y: Send + 'static, O: Send + 'static = ()> {
    Done(O),
    Yield(Y, YieldResume<Self>),
    Request(CustomRequest<Self>),
}

impl<Y: Send + 'static, O: Send + 'static> CustomGenerator<Y, O> {
    fn start<Fut>(generator: impl FnOnce(CustomYielder<Y>) -> Fut) -> DeltaResult<Self>
    where
        Fut: DeltaFuture<O> + 'static,
    {
        let task = GeneratorTask::new(|channel, yields| {
            generator(CustomYielder {
                channel: CustomChannel(channel),
                yields,
            })
        });
        task.step()
    }
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
                let _ = resume(Ok(Bytes::from_static(b"ok"))).unwrap();
            }
            ResumeOutcome::Err => {
                let _ = resume(Err(Error::generic("connector failed the read")));
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
                resume(Ok(Bytes::from_static(b"answer"))).unwrap()
            }
            Workflow::Request(_) => {
                panic!("workflow requested an unexpected operation")
            }
        };
    };

    assert_eq!(output, "read 6 bytes");
}

#[test]
fn custom_workflow_uses_its_own_request_vocabulary() {
    let workflow = CustomWorkflow::start(async |channel| {
        let length = channel.length("custom request".to_string()).await?;
        Ok(length * 2)
    })
    .unwrap();

    let CustomWorkflow::Request(CustomRequest::Length(value, resume)) = workflow else {
        panic!("custom workflow did not request string length");
    };
    assert_eq!(value, "custom request");

    let workflow = resume(Ok(value.len())).unwrap();
    let CustomWorkflow::Done(output) = workflow else {
        panic!("custom workflow did not complete");
    };
    assert_eq!(output, 28);
}

#[test]
fn custom_generator_interleaves_its_own_requests_and_yields() {
    let generator = CustomGenerator::start(async |channel| {
        let length = channel.length("yielded".to_string()).await?;
        channel.yield_item(length).await?;
        Ok("complete")
    })
    .unwrap();

    let CustomGenerator::Request(CustomRequest::Length(value, resume)) = generator else {
        panic!("custom generator did not request string length");
    };
    let generator = resume(Ok(value.len())).unwrap();

    let CustomGenerator::Yield(7, resume) = generator else {
        panic!("custom generator did not yield string length");
    };
    let generator = resume(Ok(())).unwrap();

    let CustomGenerator::Done(output) = generator else {
        panic!("custom generator did not complete");
    };
    assert_eq!(output, "complete");
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
                resume(Ok(())).unwrap()
            }
            Generator::Request(Request::ReadSmallFile(_, resume)) => {
                resume(Ok(Bytes::from_static(b"generated item"))).unwrap()
            }
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

    let generator = resume(Err(Error::generic("connector rejected yield"))).unwrap();
    let Generator::Done(output) = generator else {
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
                resume(Ok(Cursor::new(7_i64))).unwrap()
            }
            Workflow::Request(Request::ListForward(PageRequest::Continue(cursor, resume))) => {
                assert_eq!(cursor.into_inner::<i64>().unwrap(), 7);
                resume(Ok(Page::new(Vec::new(), None))).unwrap()
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
                resume(Ok(Bytes::copy_from_slice(location.path().as_bytes()))).unwrap()
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
