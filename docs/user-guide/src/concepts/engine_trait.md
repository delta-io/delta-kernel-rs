# The Engine trait

The `Engine` trait is the integration point between Delta Kernel and your connector. Kernel
implements the Delta protocol and gives you scan and write APIs, but it needs help with the
mechanics: listing files, reading and writing JSON and Parquet, and evaluating expressions for
data skipping and logical-to-physical transformations. Kernel never does any of this directly.
Instead, it calls into the `Engine` trait, which your connector implements.

This matters because it lets Kernel stay format-agnostic and runtime-agnostic. You control
how I/O happens, what columnar format you use, and how expressions are evaluated.

A [DefaultEngine](#the-default-engine) is provided that you can use out of the box. If you
need better performance or want to use your own data formats, you can build a custom engine.
See [Building a Connector](../connector/overview.md) for details.

## The trait

```rust,ignore
trait Engine: AsAny {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler>;
    fn storage_handler(&self) -> Arc<dyn StorageHandler>;
    fn json_handler(&self) -> Arc<dyn JsonHandler>;
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler>;
}
```

Kernel calls these methods whenever it needs to interact with the outside world. Each returns
a handler trait object that Kernel uses for a specific category of work. The four handlers
cover storage, JSON, Parquet, and expression evaluation. For observability, see
[Observability](../observability/observability.md).

## The four handlers

### StorageHandler

File system operations. Kernel calls this to list and read files from the Delta log, and to
write commit and checkpoint files.

| Method | Purpose |
|--------|---------|
| `list_from(path)` | List files lexicographically after `path` in the same directory |
| `read_files(files)` | Read byte ranges from one or more files |
| `copy_atomic(src, dst)` | Atomically copy a file (used for publishing commits) |
| `put(path, data, overwrite)` | Write raw bytes to a path (fails if `overwrite` is false and file exists) |
| `head(path)` | Get file metadata (size, modification time) without reading content |

### JsonHandler

Reads and writes JSON. Kernel uses this for Delta log commits (the `_delta_log/*.json`
files) and the JSON checkpoint manifest that references parquet sidecars. The sidecar
files themselves are parquet and are read by the `ParquetHandler` below.

| Method | Purpose |
|--------|---------|
| `parse_json(strings, schema)` | Parse JSON strings into columnar `EngineData` |
| `read_json_files(files, schema, predicate)` | Read JSON files and return `EngineData` (predicate is an optional hint) |
| `write_json_file(path, data, overwrite)` | Atomically write a stream of `FilteredEngineData` rows as a newline-delimited JSON file (one JSON object per row, nulls omitted) |

### ParquetHandler

Reads and writes Parquet. Kernel uses this for checkpoint files (including parquet
checkpoint sidecars) and for reading data files during scans.

| Method | Purpose |
|--------|---------|
| `read_parquet_files(files, schema, predicate)` | Read Parquet files into `EngineData` |
| `write_parquet_file(url, data)` | Write a stream of `EngineData` batches as a single Parquet file at the given URL |
| `read_parquet_footer(file)` | Read file footer metadata (schema, field IDs) without reading data |

The Parquet handler also supports metadata columns (row index, file path) and field-ID-based
column matching for column mapping.

### EvaluationHandler

Expression evaluation. Kernel uses this for data skipping (evaluating predicates against
file statistics) and for applying per-file transformations (partition value injection, column
mapping).

| Method | Purpose |
|--------|---------|
| `new_expression_evaluator(schema, expr, output_type)` | Create a reusable evaluator for an expression |
| `new_predicate_evaluator(schema, predicate)` | Create a reusable evaluator for a boolean predicate |
| `null_row(output_schema)` | Create a single-row, all-null `EngineData` with the given schema |
| `create_many(schema, rows)` | Create a multi-row `EngineData` from scalar values |

The expression and predicate evaluators are reusable objects that you can call repeatedly on
different batches of `EngineData`.

## The Default Engine

The `DefaultEngine` is a batteries-included implementation that works out of the box:

- Uses **Apache Arrow** as the in-memory data format
- Uses **`object_store`** for I/O (supports local FS, S3, GCS, Azure)
- Runs async I/O on a **Tokio** thread pool
- Supports multiple Arrow versions (see [Feature Flags](./feature_flags.md))

To construct one, create an object store and pass it to the builder:

```rust,no_run
# extern crate delta_kernel;
# extern crate url;
# use delta_kernel::engine::default::DefaultEngine;
# use delta_kernel::engine::default::storage::store_from_url;
# use delta_kernel::DeltaResult;
# fn example() -> DeltaResult<()> {
let url = url::Url::parse("file:///path/to/table")?;
let store = store_from_url(&url)?;
let engine = DefaultEngine::builder(store).build();
# Ok(())
# }
```

`DefaultEngine` also provides a convenience method for writing Parquet files:

```rust,ignore
let engine_data = engine
    .write_parquet(&data, &write_context)
    .await?;
```

This is not part of the `Engine` trait itself. It's a helper on `DefaultEngine` that
orchestrates the lower-level `ParquetHandler` methods with a `WriteContext` (write directory,
schema, stats columns, partition values).

## Configuring the Default Engine

`DefaultEngine` uses a builder pattern that lets you customize the task executor and plug in a
metrics reporter. The builder starts from `DefaultEngine::builder(store)` and chains optional
configuration before calling `build()`:

```rust,ignore
let engine = DefaultEngine::builder(store)
    .with_metrics_reporter(reporter)
    .with_task_executor(executor)
    .build();
```

All builder methods are optional. Calling `DefaultEngine::builder(store).build()` gives you a
fully functional engine with sensible defaults.

### The TaskExecutor trait

`DefaultEngine` uses asynchronous I/O internally, but Kernel's public APIs are synchronous.
The `TaskExecutor` trait bridges this gap by defining how async work gets scheduled and
awaited. It has four methods:

| Method | Purpose |
|--------|---------|
| `block_on(future)` | Run an async future to completion and return the result. Must not panic when called inside an async context. |
| `spawn(future)` | Run a future in the background without waiting for its result. |
| `spawn_blocking(closure)` | Run a blocking closure on a thread where blocking is safe, returning a future of the result. |
| `enter()` | Enter the executor's runtime context, returning a guard. While the guard is held, `tokio::runtime::Handle::current()` resolves to the executor's runtime. |

You don't need to implement `TaskExecutor` yourself unless you have a non-Tokio async runtime.
Kernel ships two Tokio-based implementations behind the `tokio` feature flag.

### TokioBackgroundExecutor

`TokioBackgroundExecutor` is the default executor. It spawns a **dedicated background thread**
running a single-threaded Tokio runtime. All async work is dispatched to that thread over a
channel.

```rust,ignore
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;

let executor = TokioBackgroundExecutor::new();
```

This is the right choice when:

- You don't already have a Tokio runtime in your application.
- You want Kernel's I/O isolated from the rest of your process.
- You're building a standalone connector or CLI tool.

Because it owns its runtime, `TokioBackgroundExecutor` works even when no external Tokio
runtime is active. On drop, it shuts down the background thread cleanly.

### TokioMultiThreadExecutor

`TokioMultiThreadExecutor` runs async work on a multi-threaded Tokio runtime. It comes in two
flavors.

**Share an existing runtime.** If your application already has a Tokio runtime (for example,
a web server or a query engine), pass its handle so Kernel's I/O shares the same thread pool:

```rust,ignore
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;

let handle = tokio::runtime::Handle::current();
let executor = TokioMultiThreadExecutor::new(handle);
```

The handle must come from a multi-threaded runtime. Passing a current-thread handle causes a
panic.

**Own a dedicated runtime.** If you want a multi-threaded runtime that Kernel manages, use
`new_owned_runtime`. You can optionally set the number of worker threads and the maximum number
of blocking threads. Pass `None` for either to use Tokio's defaults:

```rust,ignore
let executor = TokioMultiThreadExecutor::new_owned_runtime(
    Some(4),   // 4 worker threads
    Some(64),  // up to 64 blocking threads
)?;
```

> [!WARNING]
> Deeply nested `block_on` calls can exhaust Tokio's blocking thread pool and deadlock. If you
> set a small `max_blocking_threads`, keep nesting depth low.

### Choosing an executor

| Scenario | Executor |
|----------|----------|
| No existing Tokio runtime | `TokioBackgroundExecutor` (the default) |
| You have an existing multi-threaded Tokio runtime you want to share | `TokioMultiThreadExecutor::new(handle)` |
| You want a dedicated multi-threaded pool with custom sizing | `TokioMultiThreadExecutor::new_owned_runtime(workers, blocking)` |
| You need to isolate Kernel I/O from other async work | `TokioBackgroundExecutor` |

To use a custom executor, pass it to the builder with `with_task_executor`:

```rust,ignore
let executor = Arc::new(TokioMultiThreadExecutor::new(handle));
let engine = DefaultEngine::builder(store)
    .with_task_executor(executor)
    .build();
```

### Entering the runtime context

Some libraries (for example, `object_store` or `reqwest`) require an active Tokio runtime
context to construct clients or resolve configuration. If you call such code outside of an
async function, `tokio::runtime::Handle::current()` will panic because no runtime is active.

`DefaultEngine::enter()` solves this by entering the executor's runtime context:

```rust,ignore
let guard = engine.enter();
// Code here can call Handle::current() safely.
// The guard must be dropped before acquiring another.
drop(guard);
```

The returned guard keeps the runtime context active until it is dropped. If you acquire
multiple guards, you must drop them in reverse order. Dropping out of order causes a panic.

## When to implement your own Engine

You should implement `Engine` if:

- You have your own columnar data format (not Arrow)
- You need custom I/O (e.g. your own distributed file system client)
- You want to use your own expression evaluation engine
- You need to control parallelism or resource usage beyond what `DefaultEngine` offers

You do **not** need a custom engine to use different storage (S3, Azure, etc.). The
`DefaultEngine` supports all `object_store` backends. See
[Configuring Storage](../storage/configuring_storage.md).

For a guide on implementing `Engine`, see
[Implementing the Engine Trait](../connector/implementing_engine.md).

## What's next

- [Building a Connector](../connector/overview.md) explains the role of a connector and how
  the `Engine` fits into the bigger picture.
- [Implementing the Engine Trait](../connector/implementing_engine.md) walks through building
  a custom `Engine` from scratch.
- [Configuring Storage](../storage/configuring_storage.md) shows how to point `DefaultEngine`
  at S3, GCS, or Azure storage.
