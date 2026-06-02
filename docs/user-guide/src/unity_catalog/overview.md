# Unity Catalog integration

<!-- Page type: Explanation -->

**Unity Catalog (UC) integration** is a set of crates that connect Delta Kernel
to [Unity Catalog](https://www.unitycatalog.io/), a multi-engine governance
layer for data and AI assets. This matters because UC-managed Delta tables
require all reads and writes to go through the catalog, and these crates handle
that coordination so your connector doesn't have to implement the UC REST
protocol from scratch.

Before reading this page, make sure you understand the general concepts in
[Catalog-Managed Tables: Overview](../catalog_managed/overview.md).

## What Unity Catalog provides

Unity Catalog acts as the source of truth for catalog-managed tables. When a
table has the `catalogManaged` table feature enabled, your connector can no
longer read or write it by accessing the
transaction log on disk alone. Instead, the
connector must:

1. **Resolve** the table name to a storage path and table ID via the UC API.
2. **Obtain credentials** from UC to access the table's cloud storage.
3. **Fetch recent commits** from UC that may not yet be published to disk.
4. **Commit through UC** rather than writing directly to `_delta_log/`.

The UC integration crates handle steps 1 through 4 while Kernel handles
everything else: log replay, data skipping, schema enforcement, and protocol
compliance.

## The three crates

The UC integration is split across three crates, each with a distinct
responsibility.

### `unity-catalog-delta-client-api`: transport-agnostic traits

This crate defines the **API contract** for communicating with Unity Catalog. It
contains no HTTP code or network dependencies. The key types are:

- **`CommitClient`** trait: commits a new version to a UC-managed table. Your
  implementation calls the UC commits API to ratify a staged commit.
- **`GetCommitsClient`** trait: retrieves the list of ratified commits for a
  table. The response includes each commit's version, file name, size, and the
  latest ratified table version.
- **`CommitsRequest`** / **`CommitsResponse`** / **`Commit`**: the request and
  response models for the commits API.
- **`CommitRequest`**: the request model for ratifying a single commit.
- **`TemporaryTableCredentials`** / **`AwsTempCredentials`** / **`Operation`**:
  credential vending models. `Operation` distinguishes `Read`, `Write`, and
  `ReadWrite` access.
- **`InMemoryCommitsClient`**: a test-only implementation (behind the
  `test-utils` feature flag) that stores commits in memory. Useful for unit
  testing your connector without a live UC server.

Because this crate is transport-agnostic, you can swap in any backend (REST,
gRPC, or in-memory) without changing the code that depends on these traits.

### `unity-catalog-delta-rest-client`: HTTP implementation

This crate provides the concrete REST-over-HTTP implementations:

- **`UCClient`**: calls the UC tables API (`get_table`) and the credentials API
  (`get_credentials`). You use it to resolve a three-part table name like
  `my_catalog.my_schema.my_table` into a `table_id` and `storage_location`,
  then obtain temporary cloud credentials scoped to that location.
- **`UCCommitsRestClient`**: implements both `CommitClient` and
  `GetCommitsClient` over HTTP. It talks to the UC commits endpoint to fetch
  ratified commits and to ratify new ones.
- **`ClientConfig`** / **`ClientConfigBuilder`**: configuration for the HTTP
  clients, including the workspace URL and authentication token.

### `delta-kernel-unity-catalog`: the Kernel integration layer

This crate connects the UC client layer to Kernel's APIs. It depends on both
`unity-catalog-delta-client-api` and `delta_kernel`. The key types are:

- **`UCKernelClient<C: GetCommitsClient>`**: the main entry point. It wraps any
  `GetCommitsClient` implementation and provides `load_snapshot()` and
  `load_snapshot_at()` methods. These methods call `get_commits`, convert the
  response into a `Vec<LogPath>` log tail, and pass it to
  `Snapshot::builder_for().with_log_tail()` so Kernel can build a
  Snapshot that includes unpublished commits.
- **`UCCommitter<C: CommitClient>`**: implements Kernel's `Committer` trait for
  UC tables. For version 0 (table creation), it writes `000.json` directly to
  the published commit path. For all subsequent versions, it writes a staged
  commit to `_delta_log/_staged_commits/`, then calls the UC commit API to
  ratify it. The `publish()` method copies ratified staged commits to
  `_delta_log/` as published commits.
- **`get_required_properties_for_disk()`**: returns the table properties you
  must include when creating a UC-managed table (the `catalogManaged` and
  `vacuumProtocolCheck` feature signals, plus the `io.unitycatalog.tableId`).
  Kernel's `create_table()` consumes these as table properties on the version 0
  commit.
- **`get_final_required_properties_for_uc()`**: extracts the full set of
  properties from the post-creation Snapshot (feature signals, protocol
  versions, in-commit timestamp, optional clustering columns) that you send to
  your UC server's table-registration endpoint to finalize the table.

See [Creating UC Tables](./creating_tables.md) for the end-to-end creation
flow and how these two utilities fit together.

> [!NOTE]
> `UCCommitter` requires a multi-threaded tokio runtime. The default Kernel
> Engine uses tokio, so this is compatible. If you use a custom Engine, ensure
> your runtime is multi-threaded.

## How the crates map to catalog-managed concepts

The [Catalog-Managed Tables: Overview](../catalog_managed/overview.md)
describes the generic architecture: a catalog client-side component that
resolves tables, fetches commits, and provides a `Committer`. Here is how the
UC crates fill those roles:

| Generic concept | UC implementation |
|-----------------|-------------------|
| Resolve table name to path + credentials | `UCClient::get_table()` + `UCClient::get_credentials()` |
| Fetch ratified commits (log tail) | `UCKernelClient::load_snapshot()` via `GetCommitsClient::get_commits()` |
| Build Snapshot with catalog commits | `UCKernelClient` calls `Snapshot::builder_for().with_log_tail().with_max_catalog_version()` |
| Commit through catalog | `UCCommitter` implements `Committer`: stages, ratifies via `CommitClient::commit()`, then publishes |
| Publish staged commits | `UCCommitter::publish()` copies staged files to `_delta_log/` |

## Architecture

The following diagram shows how data flows through the three crates when your
connector reads or writes a UC-managed table.

```text
 ┌─────────────────────────────────────────────────────────┐
 │                   Your Connector                        │
 │                                                         │
 │  1. UCClient::get_table("catalog.schema.table")         │
 │  2. UCClient::get_credentials(&table_id, Read)          │
 │  3. UCKernelClient::load_snapshot(&table_id, &uri, ..)  │
 │  4. snapshot.scan_builder().build()?.execute(engine)?    │
 └──────────┬──────────────┬───────────────────────────────┘
            │              │
            ▼              ▼
 ┌──────────────────┐  ┌───────────────────────────────────┐
 │  unity-catalog-  │  │  delta-kernel-unity-catalog        │
 │  delta-rest-     │  │                                    │
 │  client          │  │  UCKernelClient                    │
 │                  │  │    calls get_commits()              │
 │  UCClient        │  │    converts to Vec<LogPath>        │
 │  UCCommitsRest   │  │    calls Snapshot::builder_for()   │
 │  Client          │  │      .with_log_tail(commits)       │
 │                  │  │      .build(engine)                 │
 │  Implements:     │  │                                    │
 │  CommitClient    │  │  UCCommitter                       │
 │  GetCommitsClient│  │    implements Committer trait       │
 └──────┬───────────┘  └──────────┬────────────────────────┘
        │                         │
        ▼                         ▼
 ┌──────────────────┐  ┌───────────────────────────────────┐
 │  unity-catalog-  │  │  delta_kernel                      │
 │  delta-client-   │  │                                    │
 │  api             │  │  Snapshot, Scan, Transaction        │
 │                  │  │  Committer trait                    │
 │  CommitClient    │  │  LogPath, SnapshotBuilder           │
 │  GetCommitsClient│  │                                    │
 │  (traits)        │  │  Knows nothing about UC.            │
 └──────────────────┘  └───────────────────────────────────┘
```

The diagram shows the steady-state commit flow for an existing table. The
version 0 commit (table creation) takes a different path: `UCCommitter` writes
`_delta_log/00000000000000000000.json` directly and skips the UC commits API.
See [Creating UC Tables](./creating_tables.md) for the full creation flow.

## Dependencies and feature flags

To use the UC integration, add the following to your `Cargo.toml`:

```toml
[dependencies]
delta-kernel-unity-catalog = { version = "..." }
unity-catalog-delta-rest-client = { version = "..." }
```

Depend on `unity-catalog-delta-client-api` whenever you import types from it
directly (including `Operation`, `CommitClient`, and `GetCommitsClient`). The
REST client crate does not re-export these. You also need the client-api crate
when implementing a custom backend, such as a gRPC client.

The `delta-kernel-unity-catalog` crate has the following feature flags:

| Feature | Default | Description |
|---------|---------|-------------|
| `arrow` | Yes | Enables Arrow integration (currently delegates to `arrow-58`) |
| `arrow-58` | Via `arrow` | Uses Arrow version 58 |
| `arrow-57` | No | Uses Arrow version 57 |

The `unity-catalog-delta-client-api` crate has one feature flag:

| Feature | Default | Description |
|---------|---------|-------------|
| `test-utils` | No | Enables `InMemoryCommitsClient` for unit testing |

> [!TIP]
> The `unity-catalog-delta-rest-client` crate also exposes a `test-utils`
> feature that enables the `test-utils` feature on the client API crate
> transitively. Add it to your `[dev-dependencies]` to get the in-memory
> client for tests.

## Client configuration and retries

`ClientConfigBuilder` exposes the following tuning knobs. The defaults are
sensible for most workloads, but long-running reads and bursty write paths
often benefit from raising timeouts or the retry budget.

| Method | Default | Description |
|--------|---------|-------------|
| `with_timeout(Duration)` | 30 seconds | Per-request timeout for UC REST calls. |
| `with_connect_timeout(Duration)` | 10 seconds | TCP connect timeout. |
| `with_max_retries(u32)` | 3 | Maximum retry attempts for a single request. |
| `with_retry_delays(base, max)` | 500 ms base, 10 s max | Linear backoff bounds between retries. |

```rust,ignore
use std::time::Duration;
use unity_catalog_delta_rest_client::ClientConfig;

let config = ClientConfig::build(&endpoint, &token)
    .with_timeout(Duration::from_secs(60))
    .with_max_retries(5)
    .with_retry_delays(Duration::from_millis(200), Duration::from_secs(5))
    .build()?;
```

The REST client automatically retries requests that fail with server errors
(HTTP 5xx) or transient network errors, using linear backoff bounded by
`retry_base_delay` and `retry_max_delay`. Successful 2xx and client errors
(HTTP 4xx) are not retried. These retries apply to transport-level failures
only. Transaction-level conflicts (another writer won the version) must be
handled by the connector through the `CommitResult::ConflictedTransaction`
branch. See [Writing to UC Tables](./writing.md) for the full retry model.

## When not to use this

If your tables are not registered in Unity Catalog, you don't need these crates.
Standard filesystem-managed Delta tables work with Kernel directly. See
[Building a Scan](../reading/building_a_scan.md) and
[Appending Data](../writing/append.md) for the non-catalog path.

If you use a different catalog (Hive Metastore, AWS Glue, Polaris), you need a
different catalog client-side component. The generic
[catalog-managed overview](../catalog_managed/overview.md) explains the
extension points.

## What's next

- [Creating UC Tables](./creating_tables.md): how to create a new UC-managed
  table using `get_required_properties_for_disk` and
  `get_final_required_properties_for_uc`.
- [Reading UC Tables](./reading.md): how to load a Snapshot and read data from
  a UC-managed table.
- [Writing to UC Tables](./writing.md): how to commit and publish writes through
  Unity Catalog.

## See also

- [Catalog-Managed Tables: Overview](../catalog_managed/overview.md): the
  generic catalog-managed concepts that the UC crates implement.
- [Implementing a Catalog Committer](../catalog_managed/committer.md): how the
  `Committer` trait works under the hood.
