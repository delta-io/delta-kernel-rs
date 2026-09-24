# Driving connector workflows

To run Kernel without synchronous `Engine` callbacks, drive workflows and generators from your
connector. Neither runs kernel until the connector requests a step. This keeps your runtime in
control of I/O, scheduling, cancellation, and catalog calls.

Before reading this page, make sure you understand the
[connector's role](./overview.md).

> [!NOTE]
> The connector-driven API requires Delta Kernel's experimental `internal-api` feature.

## Choose the execution surface

Kernel exposes two execution surfaces:

- **Connector-driven execution** returns a lazy `Workflow` or passes a `Channel` to a streaming
  operation. Your connector owns the operation and serves its typed requests.
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

Creating `workflow` does no I/O. `run` retains the workflow and alternates between advancing Kernel
and serving requests until the workflow completes.

## Own the driver loop

Own the workflow loop when you need to intercept requests, such as catalog commit and publish
operations:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::coroutine::{StaticWorkflow, WorkflowStep};
# use delta_kernel::DeltaResult;
# use delta_kernel_default_engine::AsyncEngineConnector;
async fn drive<O: Send + 'static>(
    connector: &AsyncEngineConnector,
    mut workflow: StaticWorkflow<O>,
) -> DeltaResult<O> {
    loop {
        match workflow.advance()? {
            WorkflowStep::Done(output) => return Ok(output),
            WorkflowStep::Request(request) => {
                connector.reply(request).await?;
            }
        }
    }
}
```

`advance` is synchronous. It runs Kernel until the workflow completes or posts one request; it
doesn't perform connector work. `reply` performs the request asynchronously and delivers its result
to the suspended Kernel await point.

If you handle a request yourself, match its variant to obtain the operation and single-use `Reply`.
Call `reply.send(result)` after processing the operation.

## Drive generators

A caller without a parent channel constructs a `StaticGenerator` from a channel-taking operation:

```ignore
let mut generator = StaticGenerator::new(move |channel| operation(channel, owned_inputs));
```

The callback must return `Generator<'static, _>`, so borrowed state cannot escape into the static
generator:

- `GeneratorStep::Request` asks the connector to perform work.
- `GeneratorStep::Yield` transfers one item to the caller.
- `GeneratorStep::Done` indicates that the generator completed.

Kernel acknowledges a yielded item before returning it. Calling `advance` after `Done` returns an
error. Dropping the generator abandons it and expires its outstanding replies.

## Compose inside Kernel

When code already has a borrowed Kernel `Channel`, call async workflow implementations directly.
Pass the channel to streaming operations and consume the returned `Generator` with its async
`next` method. Its lifetime covers the channel and any other borrowed inputs. The child posts
requests through its parent coroutine, and each successful `next` acknowledges the yielded item
automatically.

## Handle pagination

Large reads and listings use `PageRequest`:

- `Start` initializes an operation and returns its first `Page`.
- `Prepare` initializes an operation and returns a `Cursor` without fetching the first page.
- `Continue` consumes a cursor and returns the next page.

The cursor contains connector-owned state. Kernel stores it opaquely and returns it with the next
continuation request. `Page.next == None` marks exhaustion; an empty payload with a live cursor
doesn't.

## Propagate errors and cancellation

Replying with `Err(error)` returns that error to the exact Kernel await point that created the
request. Replies are single-use and request-specific, so one request's response cannot resume
another.

Configure `AsyncEngineConnector::with_cancellation_token` to race I/O and stream polling against a
Tokio cancellation token. Dropping a workflow or static generator abandons that operation.
Dropping a generator ends its stream early. In either case, Kernel drops its suspended state, and
later replies are discarded.

## What's next

- [The Engine trait](../concepts/engine_trait.md) explains the synchronous compatibility surface.
- [The `EngineData` trait](./engine_data.md) defines the opaque data exchanged with Kernel.
- [Implementing a catalog committer](../catalog_managed/committer.md) explains catalog-specific
  request handling.
