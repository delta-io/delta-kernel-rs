# Driving connector workflows

To run Kernel without synchronous `Engine` callbacks, drive its `Workflow` and `Generator` values
from your connector. These operations are **lazy**: they do no work until composed or started. This
keeps your runtime in control of I/O, scheduling, cancellation, and catalog calls.

Before reading this page, make sure you understand the
[connector's role](./overview.md).

> [!NOTE]
> The connector-driven API requires Delta Kernel's experimental `internal-api` feature.

## Choose the execution surface

Kernel exposes two execution surfaces:

- **Connector-driven execution** returns a lazy `Workflow` or `Generator`. Your connector starts
  the root task and serves its typed requests.
- **Engine compatibility execution** accepts `&dyn Engine`. Kernel's compatibility adapter drives
  the operation synchronously through Engine handlers.

Both surfaces run the same protocol logic. Choose based on who should control execution. An
asynchronous connector normally uses connector-driven execution even though it could call a
synchronous Engine method.

The default-engine crate supports both choices:

- `AsyncEngineConnector` serves workflows through native asynchronous `object_store` operations.
- `DefaultEngine` implements the synchronous `Engine` compatibility surface through a
  `TaskExecutor`.

## Run a workflow with the default async connector

When the default async connector should handle every request, pass a lazy workflow to
`AsyncEngineConnector::run`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# extern crate tokio;
# use std::sync::Arc;
# use delta_kernel::object_store::memory::InMemory;
# use delta_kernel::{DeltaResult, Snapshot};
# use delta_kernel_default_engine::AsyncEngineConnector;
# #[tokio::main]
# async fn main() -> DeltaResult<()> {
let store = Arc::new(InMemory::new());
let connector = AsyncEngineConnector::new(store);

let workflow = Snapshot::builder_for("memory:///table/").workflow();
let snapshot = connector.run(workflow).await?;
# let _ = snapshot;
# Ok(())
# }
```

Creating `workflow` does no I/O. `run` calls `start`, retains the resulting task, and alternates
between advancing Kernel and serving requests until the workflow completes.

## Own the driver loop

Own the task loop when you need to intercept requests, such as catalog commit and publish
operations:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::coroutine::{Workflow, WorkflowStep};
# use delta_kernel::DeltaResult;
# use delta_kernel_default_engine::AsyncEngineConnector;
async fn drive<W: Workflow>(
    connector: &AsyncEngineConnector,
    workflow: W,
) -> DeltaResult<W::Output> {
    let mut task = workflow.start();
    loop {
        match task.advance()? {
            WorkflowStep::Done(output) => return Ok(output),
            WorkflowStep::Request(request) => connector.reply(request).await?,
        }
    }
}
```

`advance` is synchronous. It runs Kernel until the workflow completes or posts one request; it
doesn't perform connector work. `reply` performs the request asynchronously and delivers its
result to the suspended Kernel await point.

If you handle a request yourself, complete the `Reply` carried by that request before advancing
the task again. `WorkflowTask::resume` combines reply delivery and the next advance when your match
arm has the typed reply and response available together.

## Drive generators

A started `Generator` adds yielded items to the workflow exchange:

- `GeneratorStep::Request` asks the connector to perform work.
- `GeneratorStep::Yield` transfers one item and a `Reply<()>` acknowledgment.
- `GeneratorStep::Done` returns the generator's terminal output.

Consume or retain each yielded item before acknowledging it. Sending `Ok(())` allows the generator
to continue. Sending an error returns that error at the generator's yield point. Dropping the task
abandons the generator and invalidates all outstanding replies.

## Compose inside Kernel

Use `start` only when no parent Kernel task exists. Starting creates an independent task and
request channel that someone must drive.

When code already has a borrowed Kernel `Channel`, compose finite work with `Workflow::run_with`
and streaming work with `Generator::bind`. The child then posts requests through its parent task.
Starting a child in that situation creates a second root and forces the connector to coordinate
two independent drivers.

## Handle pagination

Large reads and listings use `PageRequest`:

- `Start` initializes an operation and returns its first `Page`.
- `Prepare` initializes an operation and returns a `Cursor` without fetching the first page.
- `Continue` consumes a cursor and returns the next page.

The cursor contains connector-owned state. Kernel stores it opaquely and returns it with the next
continuation request. `Page.next == None` marks exhaustion; an empty payload with a live cursor
doesn't.

## Propagate errors and cancellation

Sending `Err(error)` through a request's reply returns that error to the exact Kernel await point
that created the request. Replies are single-use and request-specific, so one request's response
cannot resume another.

Configure `AsyncEngineConnector::with_cancellation_token` to race I/O and stream polling against a
Tokio cancellation token. Dropping a workflow or generator task is unconditional abandonment:
Kernel drops its suspended state, and later attempts to use an outstanding reply fail.

## What's next

- [The Engine trait](../concepts/engine_trait.md) explains the synchronous compatibility surface.
- [The `EngineData` trait](./engine_data.md) defines the opaque data exchanged with Kernel.
- [Implementing a catalog committer](../catalog_managed/committer.md) explains catalog-specific
  request handling.
