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

let log_path = LogPath::from_commit_filename(
    table_url.clone(),                              // table root URL
    8,                                              // version the catalog reported
    "00000000000000000008.<uuid>.json",             // filename the catalog reported
    1234567890,                                     // last modified timestamp
    4096,                                           // file size in bytes
)?;
```

A staged commit filename follows the Delta staged-commit convention: a zero-padded 20-digit
version, the UUID assigned when the commit was staged, and a `.json` extension. Kernel mints the
UUID inside `CommitMetadata::staged_commit_path()` at write time; the writer passes the
resulting path to the catalog, and the catalog returns that same path on reads. If your
catalog returns commits as `(version, uuid, ...)` tuples, format the filename as
`{version:020}.{uuid}.json`. Catalogs may also report a commit that is already published by its
`{version:020}.json` filename; `LogPath::from_commit_filename` resolves either form to the
right directory. It returns an error if the filename's version does not match `version`.

Then pass the resulting `Vec<LogPath>` to `SnapshotBuilder` along with the maximum
catalog-ratified version:

```rust,ignore
use delta_kernel::Snapshot;

let snapshot = Snapshot::builder_for(table_url)
    .with_max_catalog_version(latest_version) // the latest version the catalog ratified
    .with_log_tail(log_paths)                 // the commits the catalog returned
    .build(&engine)?;
```

When nonempty, the log tail must be a **contiguous sequence** `M..=N`: ascending by version, no gaps,
no duplicates. The first version `M` can be any value `>= 0` (typical catalog clients
return a suffix of recent commits, not the full history). When you supply
`max_catalog_version`, the last version `N` must satisfy these constraints:

- Without time travel, `N` must equal `max_catalog_version`.
- With `.at_version(v)`, `N` must be `>= v`, and `v` must not exceed
  `max_catalog_version`.

If you supply staged paths through `with_log_tail`, you must also supply `max_catalog_version`.
This requirement applies even when those commits have published copies in `_delta_log/`.
A tail containing only published paths doesn't itself require a catalog bound.

The order of `.at_version()`, `.with_log_tail()`, and `.with_max_catalog_version()`
doesn't matter.

The log tail can overlap with published commits already in `_delta_log/`. For any
version present in both, Kernel uses the log-tail entry, as the Delta spec requires
readers to prefer the catalog-supplied commit when the catalog provides one.

Call `.with_max_catalog_version(N)` to tell Kernel the highest version the catalog has
ratified. This prevents Kernel from loading filesystem commits beyond what the catalog
knows about. Supply this bound even when the catalog returns an empty tail.

## Time travel and management changes

When using catalog discovery, pass its bound together with `.at_version(v)` for time
travel. The requested version must not exceed the bound, regardless of the protocol at
that version. Without an explicit target, Kernel rejects a Snapshot whose protocol is
catalog-managed unless you supply a bound.

For an explicit target, Kernel doesn't require a bound solely because that historical
version was catalog-managed. For example, after a published downgrade, you can read an
earlier catalog-managed version from published history without a bound. Your connector
must still perform catalog discovery when required by the table's current management
state. Kernel doesn't load newer protocol state to validate this obligation.

Retained staged paths in an existing Snapshot or complete snapshot hint don't themselves
require a new bound: their ratification is already part of the Snapshot or hint's contract.
Any supplied bound still applies to an explicit target.

A catalog-ratified downgrade produces a non-catalog-managed Snapshot. You can read it
with the catalog bound when its commit is supplied in the catalog tail, or already
published and omitted from that tail. An unpublished commit must remain available through
the supplied tail or a retained Snapshot or complete hint. To discover later filesystem
commits, refresh without the old bound.

## Publication evidence and snapshot reuse

Publication can advance without changing a snapshot's version. Snapshot reuse can retain older
publication evidence even when a supplied tail or filesystem listing establishes further
publication. As a result, checkpointing can return `UnpublishedVersion` after the required commits
have been published. This limitation is tracked in
[issue #3845](https://github.com/delta-io/delta-kernel-rs/issues/3845).

To refresh publication evidence, build a fresh snapshot with `Snapshot::builder_for(table_root)`
after publication, supplying the catalog bound and tail when required by the request. The fresh
build must observe the published copies to record their publication.

`skip_new_checkpoints()` lists after the cached snapshot version, so it cannot observe publication
at that version. Complete snapshot hints perform no engine I/O and preserve the publication
evidence supplied in the hint.

## Reading the snapshot

For a complete Unity Catalog example, see
[Reading Unity Catalog tables](../unity_catalog/reading.md).

After building the snapshot, reading works the same as for a filesystem-managed table:
build a scan, apply predicates, and read data. See [Building a scan](../reading/building_a_scan.md).

## What's next

- [Writing: the commit and publish flow](./writing.md) covers how writes work for
  catalog-managed tables.
- [Implementing a catalog committer](./committer.md) describes the `Committer` trait in
  detail.
