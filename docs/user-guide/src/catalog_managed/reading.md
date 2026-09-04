# Reading catalog-managed tables

To read a catalog-managed table, you provide Kernel with a **log tail**: the set of
recent ratified commits your catalog client knows about. Kernel merges these with whatever
it finds on the filesystem to build a complete [Snapshot](../concepts/architecture.md).

Before reading this page, make sure you understand
[Catalog-managed tables](./overview.md).

## Table resolution is outside Kernel

Kernel doesn't know about table names, table IDs, or catalog APIs. Before you can use
any Kernel API, the catalog client must:

1. **Resolve the table name to a storage path** (e.g. `unity.my_schema.my_table` ->
   `s3://bucket/path/to/table/`)
2. **Fetch storage credentials** (e.g. temporary AWS credentials for the table's S3
   location)
3. **Get the latest ratified commits** from the catalog (the log tail)

Each catalog has its own APIs for these steps.

## Why a log tail?

A catalog-managed table can have a mix of published commits (already in `_delta_log/`)
and unpublished ratified commits (in `_staged_commits/` or inline in the catalog).
Kernel lists `_delta_log/` to find published commits and checkpoints, but it cannot
discover unpublished commits on its own. The log tail bridges this gap.

For example, suppose versions 0-7 are published and versions 8-9 are staged:

```text
_delta_log/
  00000000000000000000.json          (published)
  ...
  00000000000000000007.json          (published)
  _staged_commits/
    00000000000000000008.<uuid>.json  (ratified, not published)
    00000000000000000009.<uuid>.json  (ratified, not published)
```

Without a log tail, Kernel would only see versions 0-7. The log tail tells Kernel about
versions 8 and 9.

## Building a log tail

The catalog client calls the catalog's commits API to get the latest ratified
commits, then translates each one into a `LogPath`:

```rust,ignore
use delta_kernel::LogPath;

let log_path = LogPath::staged_commit(
    table_url.clone(),                              // table root URL (must end with '/')
    "00000000000000000008.<uuid>.json",             // staged filename: <version>.<uuid>.json
    1234567890,                                     // last modified timestamp
    4096,                                           // file size in bytes
)?;
```

The filename follows the Delta staged-commit convention: a zero-padded 20-digit version,
the UUID assigned when the commit was staged, and a `.json` extension. Kernel mints the
UUID inside `CommitMetadata::staged_commit_path()` at write time; the writer passes the
resulting path to the catalog, and the catalog returns that same path on reads. If your
catalog returns commits as `(version, uuid, ...)` tuples, format the filename as
`{version:020}.{uuid}.json` before calling `LogPath::staged_commit`. Published commit
files use `{version:020}.json` and are loaded via `LogPath::try_new`, not
`LogPath::staged_commit`. The `table_url` argument must end with `/`, otherwise
`LogPath::staged_commit` returns an error.

Then pass the resulting `Vec<LogPath>` to `SnapshotBuilder` along with the maximum
catalog-ratified version:

```rust,ignore
use delta_kernel::Snapshot;

let snapshot = Snapshot::builder_for(table_url)
    .with_max_catalog_version(latest_version) // the latest version the catalog ratified
    .with_log_tail(log_paths)                 // the commits the catalog returned
    .build(&engine)?;
```

The log tail must be a **contiguous sequence** `M..=N`: ascending by version, no gaps,
no duplicates. The first version `M` can be any value `>= 0` (typical catalog clients
return a suffix of recent commits, not the full history). The last version `N` depends
on whether you are time-traveling:

- Without time travel, `N` must equal `max_catalog_version`.
- With `.at_version(v)`, `N` must be `>= v`, and `max_catalog_version` must still be
  set. Both constraints apply simultaneously.

The log tail can overlap with published commits already in `_delta_log/`. For any
version present in both, Kernel uses the log-tail entry, as the Delta spec requires
readers to prefer the catalog-supplied commit when the catalog provides one.

Call `.with_max_catalog_version(N)` to tell Kernel the highest version the catalog has
ratified. This prevents Kernel from loading filesystem commits beyond what the catalog
knows about.

> [!NOTE]
> To time-travel to a specific version within a catalog-managed table, call both
> `.at_version(v)` and `.with_max_catalog_version(N)` on the builder. Call order does
> not matter. The requested version must not exceed `N`.

For a complete Unity Catalog example, see
[Reading Unity Catalog tables](../unity_catalog/reading.md).

After building the snapshot, reading works the same as for a filesystem-managed table:
build a scan, apply predicates, and read data. See [Building a scan](../reading/building_a_scan.md).

## What's next

- [Writing: the commit and publish flow](./writing.md) covers how writes work for
  catalog-managed tables.
- [Implementing a catalog committer](./committer.md) describes the `Committer` trait in
  detail.
