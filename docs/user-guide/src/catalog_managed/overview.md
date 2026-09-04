# Catalog-managed tables

A **catalog-managed table** is a Delta table whose commits go through a catalog
instead of being written directly to the filesystem. This matters because it
shifts the source of truth from the filesystem to the catalog, enabling
centralized governance, enforceable constraints, and multi-table coordination.

Before reading this page, make sure you understand
[Architecture overview](../concepts/architecture.md) and
[The Engine trait](../concepts/engine_trait.md).

## Filesystem-managed vs. catalog-managed

By default, Delta tables are **filesystem-managed**: every commit is written
directly to `_delta_log/` as a JSON file, and readers discover table state by
listing that directory. Atomicity comes from the filesystem's PUT-if-absent
semantics.

A **catalog-managed table** changes this model. The `catalogManaged`
reader-writer table feature makes the catalog the **source of truth** for
commits:

- Writers must commit through the catalog, not directly to the filesystem.
- Readers must contact the catalog to discover recent (possibly unpublished)
  commits.
- The catalog decides whether a commit attempt succeeds, not the filesystem.
- Path-based access is not supported. Tables must be accessed through the
  catalog.

## Benefits of catalog-managed tables

In filesystem-managed tables, the filesystem is the ultimate authority on table
state. Catalog-managed tables change that by putting the catalog on the critical
path for all reads and writes:

- **Governance across engines.** Every read and write goes through the catalog,
  ensuring all engines enforce the same permissions, constraints, lineage, and
  auditing.
- **Enforceable constraints.** The catalog can reject invalid schema or
  constraint changes (for example, blocking a NOT NULL drop on a column
  referenced by a foreign key).
- **Multi-table atomic updates.** The catalog can coordinate commits spanning
  multiple tables without custom coordination services.
- **Faster query planning.** The catalog can serve table metadata directly,
  skipping cloud storage LIST operations that add 100+ ms of latency.

## Terminology: commit types

Catalog-managed tables have several kinds of commits:

| Commit type | Description |
|------------|-------------|
| **Staged commit** | Written to `_delta_log/_staged_commits/<version>.<uuid>.json`. Has the same format as a normal delta file. The catalog records which staged commit won each version. |
| **Ratified commit** | A staged commit that the catalog has accepted as the winner for a given version. May or may not be published yet. |
| **Published commit** | A ratified commit that has been copied to `_delta_log/<version>.json` as a normal delta file. Once published, it is discoverable through filesystem listing. |

Writing a staged commit file does **not** reserve a version. Multiple writers may stage
different `<version>.<uuid>.json` files for the same version; the catalog decides which
one wins. Staged commits that lose the race become orphans on disk and are cleaned up
out of band (typically by a catalog-driven sweep).

> [!NOTE]
> The Delta protocol also defines **inline commits**, where commit content is
> sent directly to the catalog rather than written to disk. Kernel does not yet
> support inline commits.

## Protocol requirements

The `catalogManaged` reader-writer table feature requires `inCommitTimestamp`
to be enabled. A spec-compliant catalog-managed table therefore carries:

- `catalogManaged` in both `readerFeatures` and `writerFeatures` in the
  `Protocol` action (it is a reader-writer feature).
- `inCommitTimestamp` enabled as a writer-only feature.

Kernel constructs this protocol automatically during the `create_table` call;
you do not hand-craft the `Protocol` or `Metadata` actions. The list above
describes what ends up in the commit, not steps you write by hand.

The Delta spec also requires every `CommitInfo` action on a catalog-managed
table to carry a unique `txnId` field. Kernel writes a fresh `txnId` (UUID) on
every `CommitInfo` it generates, so this requirement is met automatically.
Custom committers **must** write the full iterator of actions Kernel supplies
to the staged commit file. They do not pick and choose which actions to
include, and they do not synthesize replacement `CommitInfo` actions.

## How Kernel fits in

Kernel's design philosophy for catalog-managed tables is straightforward:
**Kernel doesn't know about catalogs.** It doesn't know about table names, table
IDs, catalog APIs, or catalog servers. Instead:

- The **catalog client** (a client-side library you write or import) works
  together with your connector to resolve table names to paths, fetch
  credentials, and retrieve recent ratified commits from the catalog.
- The catalog client translates the catalog's response into a `Vec<LogPath>` log
  tail and a max catalog version. Your connector passes these to `SnapshotBuilder`
  to load the table.
- For writes, the catalog client provides a **`Committer`** that knows how to
  stage, ratify, and publish commits through the catalog.
- Kernel does its job (protocol compliance, log replay, data skipping, schema
  enforcement) without knowing or caring whether a catalog exists.

```text
+-----------------------------------------------------------+
|                   Compute Engine                          |
|         (Spark, Flink, DuckDB, Polars, ...)               |
+---------------------------+-------------------------------+
                            | table name
                            v
+-----------------------------------------------------------+
|                   Catalog Client                          |
|                                                           |
|  1. Resolves table name -> path + credentials             |
|  2. Calls catalog API to get ratified commits             |
|  3. Translates commits into LogPath entries (log tail)    |
|  4. Provides a Committer for writing                      |
+---------------------------+-------------------------------+
                            | path, log tail, committer
                            v
+-----------------------------------------------------------+
|               Your Delta Connector                        |
|                                                           |
|  Uses Kernel APIs to read and write:                      |
|    Snapshot::builder_for(path)                            |
|      .with_log_tail(commits)                              |
|      .with_max_catalog_version(version)                   |
|      .build(&engine)                                      |
|    snapshot.transaction(committer, &engine)                |
+---------------------------+-------------------------------+
                            | calls Kernel APIs
                            v
+-----------------------------------------------------------+
|                   Delta Kernel                            |
|                                                           |
|  Knows nothing about catalogs, table names, or IDs.       |
|  Sees: a path, log files, and a Committer trait.          |
|  Handles: log replay, data skipping, protocol compliance  |
+-----------------------------------------------------------+
```

### Key Kernel APIs for catalog-managed tables

Four Kernel APIs form the integration surface between the catalog client and
Kernel:

- **`SnapshotBuilder::with_log_tail(Vec<LogPath>)`** accepts a contiguous run of
  commits from version `M` to version `N` inclusive (published or staged). The
  sequence must be ascending, gap-free, and duplicate-free. See
  [Reading catalog-managed tables](./reading.md#building-a-log-tail) for the exact
  rules, including what `M` and `N` must equal with and without time travel.

- **`SnapshotBuilder::with_max_catalog_version(Version)`** caps the snapshot
  version at the catalog's latest ratified version. Kernel requires this to be set
  for every catalog-managed snapshot, so readers come with an explicit
  catalog-ratified upper bound rather than silently reading whatever happens to be
  on the filesystem.

- **`Committer` trait** defines how transactions are committed. A catalog
  committer implements `commit()` to stage and ratify commits through the
  catalog API, and `publish()` to copy ratified commits to the Delta log.

- **`Snapshot::publish()`** publishes all unpublished catalog commits at the
  current snapshot version. Published commits become visible to filesystem-based
  readers and enable maintenance operations like checkpointing. If the snapshot
  has no unpublished commits, `publish()` is a no-op and returns the same
  snapshot reference without invoking the committer. If unpublished commits
  exist, both the snapshot and the committer must be catalog-managed; otherwise
  `publish()` errors.

## Example: the read flow

The following pseudocode shows how a catalog client typically integrates with
Kernel for reading. The exact API calls depend on your catalog.

```rust,ignore
// 1. Resolve table name through catalog API
//    catalog_client.get_table("catalog.schema.table") -> table_id, path, credentials

// 2. Get ratified commits from catalog
//    catalog_client.get_commits(table_id) -> commits, max_version

// 3. Convert catalog response to LogPath entries. If your catalog returns
//    (version, uuid, ...) tuples, format the filename as {version:020}.{uuid}.json
//    first. See "Building a log tail" in reading.md.
//    let log_tail: Vec<LogPath> = commits.into_iter()
//        .map(|c| LogPath::staged_commit(table_root.clone(), &c.filename, c.timestamp, c.size))
//        .collect::<Result<_, _>>()?;

// 4. Build Engine with vended credentials
//    let engine = build_engine_with_credentials(path, credentials);

// 5. Build Snapshot with log tail from catalog
//    let snapshot = Snapshot::builder_for(path)
//        .with_log_tail(log_tail)
//        .with_max_catalog_version(max_version)
//        .build(&engine)?;

// 6. Read the table using standard Kernel APIs
//    let scan = snapshot.scan_builder().build()?;
//    for data in scan.execute(Arc::new(engine))? { ... }
```

The key insight: the catalog client handles all catalog-specific logic
(authentication, API calls, response parsing). By the time Kernel sees the request,
it is a standard `Snapshot::builder_for()` call with an extra log tail and version
cap.

## Maintenance operations

Catalog-managed tables restrict which maintenance operations a client may run.
Two layers enforce the rules:

- **Kernel enforces** that checkpoints and log compaction run only on
  **published** versions. Publish commits before running either. Version
  checksum (CRC) files, by contrast, can be written for unpublished versions
  as well.
- **The managing catalog enforces** which maintenance operations a client may
  request.

## See also

- [Unity Catalog integration](../unity_catalog/overview.md) for a concrete
  implementation using Unity Catalog.

## What's next

- [Implementing a catalog committer](./committer.md) describes how to implement
  the `Committer` trait for your catalog.
- [Reading catalog-managed tables](./reading.md) covers how to provide catalog
  commits to Kernel for snapshot construction.
- [Writing: the commit and publish flow](./writing.md) walks through the full
  commit, ratify, and publish lifecycle.
