# Unity Catalog Integration

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

1. **Load** the table's metadata and recent commits from UC in one call. These
   commits may not yet be published to disk.
2. **Obtain credentials** from UC to access the table's cloud storage.
3. **Commit through UC** rather than writing directly to `_delta_log/`.

The UC integration crates handle steps 1 through 3 while Kernel handles
everything else: log replay, data skipping, schema enforcement, and protocol
compliance.

## The three crates

The UC integration is split across three crates, each with a distinct
responsibility.

### `unity-catalog-delta-client-api`: transport-agnostic traits

This crate defines the **API contract** for communicating with Unity Catalog. It
contains no HTTP code or network dependencies. The key types are:

- **`UpdateTableClient`** trait: commits a new version to a UC-managed table.
  Your implementation calls the UC commits API to ratify a staged commit.
- **`LoadTableResponse`** / **`TableMetadata`** / **`Commit`**: the response
  models for `load_table`. A single response carries the table metadata, the
  inline commits that form the log tail, and the latest ratified version.
- **`CredentialsResponse`** / **`StorageCredential`** / **`Operation`**:
  credential vending models. Each `StorageCredential` is scoped to a storage
  `prefix` and carries a `config` map of cloud-specific key-value pairs.
  `Operation` distinguishes `Read` and `ReadWrite` access.
- **`InMemoryUpdateTableClient`**: a test-only implementation (behind the
  `test-utils` feature flag) that stores commits in memory. Useful for unit
  testing your connector without a live UC server.

Because this crate is transport-agnostic, you can swap in any backend (REST,
gRPC, or in-memory) without changing the code that depends on these traits.

### `unity-catalog-delta-rest-client`: HTTP implementation

This crate provides the concrete REST-over-HTTP implementations:

- **`UCClient`**: calls the UC Delta-Tables read APIs. `load_table` returns a
  table's metadata plus its inline log tail in one call, and
  `get_table_credentials` vends temporary cloud credentials scoped to the
  table's storage.
- **`UCUpdateTableRestClient`**: implements `UpdateTableClient` over HTTP. It
  submits staged commits to the UC `update_table` endpoint, where Unity Catalog
  ratifies them.
- **`ClientConfig`** / **`ClientConfigBuilder`**: configuration for the HTTP
  clients, including the workspace URL and authentication token.

### `delta-kernel-unity-catalog`: the Kernel integration layer

This crate connects the UC client layer to Kernel's APIs. It depends on both
`unity-catalog-delta-client-api` and `delta_kernel`. The key items are:

- **`snapshot_builder_from_load_table()`**: turns a `load_table` response into a
  `SnapshotBuilder` with the log tail and catalog version already applied. You
  call `build()` on the result (or `at_version()` first, to time-travel) to get a
  Snapshot that includes unpublished commits.
- **`log_tail_from_commits()`**: converts the inline commits from a `load_table`
  response into a `Vec<LogPath>` log tail. Use it directly when you need to assemble
  the `SnapshotBuilder` yourself.
- **`UCCommitter<C: UpdateTableClient>`**: implements Kernel's `Committer` trait
  for UC tables. Version 0 (table creation) writes `00000000000000000000.json`
  directly and skips `update_table`. For version >= 1, it writes a staged commit
  to `_delta_log/_staged_commits/`, then calls the UC `update_table` API to ratify
  it. The `publish()` method copies ratified staged commits to `_delta_log/` as
  published commits.
- **`get_required_properties_for_disk()`**: returns the table properties you
  must include when creating a UC-managed table (the `catalogManaged` and
  `vacuumProtocolCheck` feature signals, plus the `io.unitycatalog.tableId`).
  Kernel's `create_table()` consumes these as table properties on the version 0
  commit.
- **`build_uc_create_table_request()`**: reads the post-creation version 0
  Snapshot and produces a typed `CreateTableRequest` whose schema, partition
  columns, protocol, domain metadata, and metadata-config properties are separate
  typed fields. You send it to your UC server's table-registration endpoint to
  finalize the table.

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
| Load table metadata + log tail | `UCClient::load_table()` |
| Obtain storage credentials | `UCClient::get_table_credentials()` |
| Build Snapshot with catalog commits | `snapshot_builder_from_load_table()`, then `build()` |
| Commit through catalog | `UCCommitter` implements `Committer`: stages a commit, submits it via `UpdateTableClient::update_table()` for UC to ratify, then publishes |
| Publish staged commits | `UCCommitter::publish()` copies staged files to `_delta_log/` |

## Architecture

The following diagram shows how data flows through the three crates when your
connector reads or writes a UC-managed table.

```text
 ┌───────────────────────────────────────────────────────────┐
 │  Your Connector                                           │
 │                                                           │
 │  1. UCClient::load_table("cat", "schema", "table")        │
 │  2. UCClient::get_table_credentials(.., ReadWrite)        │
 │  3. snapshot_builder_from_load_table(&resp)?.build(..)    │
 │  4. snapshot.transaction(UCCommitter).commit(engine)?     │
 └──────┬─────────────────────────┬──────────────────────────┘
        │                         │
        ▼                         ▼
 ┌──────────────────┐  ┌─────────────────────────────────────┐
 │  unity-catalog-  │  │  delta-kernel-unity-catalog         │
 │  delta-rest-     │  │                                     │
 │  client          │  │  snapshot_builder_from_load_table() │
 │                  │  │    load_table resp -> builder       │
 │  UCClient        │  │                                     │
 │    load_table    │  │  UCCommitter                        │
 │    get_table_    │  │    implements Committer trait       │
 │      credentials │  │    stages a commit, submits via     │
 │  UCUpdateTable   │  │    update_table() for UC to ratify  │
 │    RestClient    │  │                                     │
 │  impl UpdateTable│  │                                     │
 │    Client        │  │                                     │
 └──────┬───────────┘  └──────────┬──────────────────────────┘
        │                         │
        ▼                         ▼
 ┌──────────────────┐  ┌─────────────────────────────────────┐
 │  unity-catalog-  │  │  delta_kernel                       │
 │  delta-client-   │  │                                     │
 │  api             │  │  Snapshot, Scan, Transaction        │
 │                  │  │  Committer trait                    │
 │  UpdateTable     │  │  LogPath, SnapshotBuilder           │
 │    Client (trait)│  │                                     │
 │  wire models     │  │  Knows nothing about UC.            │
 └──────────────────┘  └─────────────────────────────────────┘
```

The diagram shows the steady-state commit flow for an existing table. Version 0
(table creation) takes a different path: `UCCommitter` writes the published commit
directly and skips `update_table`. See [Creating UC Tables](./creating_tables.md)
for the creation flow.

## Dependencies and feature flags

To use the UC integration, add the following to your `Cargo.toml`:

```toml
[dependencies]
delta-kernel-unity-catalog = { version = "..." }
unity-catalog-delta-rest-client = { version = "..." }
```

Depend on `unity-catalog-delta-client-api` whenever you import types from it
directly (including `Operation` and `UpdateTableClient`). The
REST client crate does not re-export these. You also need the client-api crate
when implementing a custom backend, such as a gRPC client.

The `delta-kernel-unity-catalog` crate has the following feature flags:

| Feature | Default | Description |
|---------|---------|-------------|
| `arrow` | Yes | Enables Arrow integration (currently delegates to `arrow-59`) |
| `arrow-59` | Via `arrow` | Uses Arrow version 59 |
| `arrow-58` | No | Uses Arrow version 58 |

The `unity-catalog-delta-client-api` crate has one feature flag:

| Feature | Default | Description |
|---------|---------|-------------|
| `test-utils` | No | Enables `InMemoryUpdateTableClient` for unit testing |

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
  `build_uc_create_table_request`.
- [Reading UC Tables](./reading.md): how to load a Snapshot and read data from
  a UC-managed table.
- [Writing to UC Tables](./writing.md): how to commit and publish writes through
  Unity Catalog.

## See also

- [Catalog-Managed Tables: Overview](../catalog_managed/overview.md): the
  generic catalog-managed concepts that the UC crates implement.
- [Implementing a Catalog Committer](../catalog_managed/committer.md): how the
  `Committer` trait works under the hood.
