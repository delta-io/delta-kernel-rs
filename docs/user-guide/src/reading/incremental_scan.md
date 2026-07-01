# Incremental scans

To advance a cached file listing from one table version to a newer one without replaying the
whole log, you use an incremental scan. It streams only the file-level changes (added and
removed files) between a version you already know about and a newer `Snapshot`, so you can
patch your cache instead of rebuilding it from scratch.

Before reading this page, make sure you understand [Building a Scan](./building_a_scan.md).

An incremental scan is the right tool when you maintain a long-lived, cached listing of a
table's active files and poll for updates. If you only need the current file list once, use a
full scan (see [Building a Scan](./building_a_scan.md)). If you need row-level changes (the
individual inserts, updates, and deletes), use [Change Data Feed](./change_data_feed.md)
instead. An incremental scan reports file-level changes only.

## The range model

An incremental scan covers the half-open range `(base_version, target_version]`:

- `base_version` is the version you already have cached. It is **excluded**.
- `target_version` is the version of the `Snapshot` you build the scan from. It is
  **included**.

So `base_version` is the last version whose state your cache reflects, and the scan tells you
what changed to reach the target `Snapshot`. The range must be non-empty:
`base_version < target_version`, or `build()` returns an error.

## Building the scan

Call `incremental_scan_builder(base_version)` on a `Snapshot`, then `build(engine)`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// The cache already reflects version 5.
let base_version = 5;

// Build a Snapshot at the target version you want to advance to.
let target = Snapshot::builder_for(url).build(&engine)?;

// Scan the range (5, target.version()].
let maybe_stream = target
    .incremental_scan_builder(base_version)
    .build(&engine)?;
# Ok(())
# }
```

`build()` returns `Option<IncrementalScanStream>`, not the stream directly. The next section
explains why.

### Handle `None`: fall back to a full scan

`build()` returns `None` when the target `Snapshot`'s commit list can't cover the range. This
happens when `base_version` predates the oldest commit the `Snapshot` retains, usually because
a checkpoint truncated the log history before it. `None` is not an error. It's the signal that
an incremental advance isn't possible, so you rebuild your cache from a full scan:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# use delta_kernel::incremental_scan::IncrementalScanStream;
# fn consume(_: IncrementalScanStream) {}
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let base_version = 5;
# let target = Snapshot::builder_for(url).build(&engine)?;
match target.clone().incremental_scan_builder(base_version).build(&engine)? {
    Some(stream) => {
        // The range is covered. Consume the diff and patch your cache.
        consume(stream);
    }
    None => {
        // History was truncated. Rebuild the listing from a full scan.
        let _scan = target.scan_builder().build()?;
    }
}
# Ok(())
# }
```

> [!WARNING]
> Never unwrap the `Option`. `None` is a normal outcome whenever a checkpoint has truncated
> the log below your `base_version`. Treating it as an error strands your cache with no way to
> advance.

## Consuming the stream

`IncrementalScanStream` yields the live added files as `FilteredEngineData` batches, newest
commit first. A `FilteredEngineData` pairs a data batch with a selection vector, so respect
the selection vector when reading. Kernel emits one row per Add action and marks inactive the
rows an Add that later got cancelled within the range.

Each batch corresponds to a source commit's surviving Adds. A commit whose Adds were all
cancelled by a later Remove in the range produces no batch at all, and one commit's rows can
span more than one batch.

After you've read the batches you care about, call a terminal method to recover the file-key
sets that describe the diff. `into_summary()` drains any batches you didn't read and returns
the live added and removed file keys:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let base_version = 5;
# let target = Snapshot::builder_for(url).build(&engine)?;
# let Some(stream) = target.incremental_scan_builder(base_version).build(&engine)? else {
#     return Ok(());
# };
let mut added_row_count = 0;
let mut stream = stream;
for batch in stream.by_ref() {
    let batch = batch?;
    // Only the rows selected by the selection vector are live Adds.
    added_row_count += batch.selection_vector().iter().filter(|&&s| s).count();
}

// Recover the file-key sets after reading the batches.
let summary = stream.into_summary()?;
println!(
    "range ({}, {}]: {} live adds, {} removes, {} added rows",
    summary.base_version,
    summary.target_version,
    summary.live_adds.len(),
    summary.removes.len(),
    added_row_count,
);
# Ok(())
# }
```

If you'd rather buffer the whole diff at once, `into_listing()` collects every Add batch into
a `Vec` alongside the summary. Use it when the diff fits comfortably in memory.

### File keys identify files by path _and_ deletion vector

The `live_adds` and `removes` sets hold `FileActionKey` values, each a `(path, dv_unique_id)`
pair. **Delta tables use deletion vectors to mark rows as deleted without rewriting the data
file**, so the same path can appear with different deletion-vector ids. Match on the whole key,
never on the path alone.

For example, replacing a file's deletion vector adds `(P, dv=new)` and removes `(P, dv=old)`.
Those are two distinct logical files. Collapsing them to the path `P` would treat the add and
the remove as the same file and corrupt your listing.

## Advancing a cached listing

When your cache is a listing layered on top of a base version, an incremental scan gives you
exactly the delta to apply. Append the streamed Add batches to your delta layer, then mask the
base with the files that are no longer current.

The mask is the union of two sets, so use `into_summary_against_base_closure` (or its `_iter`
variant) rather than `into_summary`. That terminal method takes your base listing and returns
both `removes` and `duplicate_adds`:

- `removes` masks files that left the table.
- `duplicate_adds` masks the stale base entry of each file the range re-added with new
  metadata. Compaction (OPTIMIZE) and liquid clustering re-tag files this way: the file key is
  already in your base, and the range re-adds it, so the base's old row must be evicted in
  favor of the freshly streamed one.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::collections::HashSet;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# use delta_kernel::log_replay::FileActionKey;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let base_version = 5;
# let target = Snapshot::builder_for(url).build(&engine)?;
# let Some(stream) = target.incremental_scan_builder(base_version).build(&engine)? else {
#     return Ok(());
# };
// Your cache exposes a `contains` lookup keyed by (path, dv_unique_id).
let base: HashSet<FileActionKey> = HashSet::new();

let summary = stream.into_summary_against_base_closure(|key| base.contains(key))?;

// Evict every base entry that either left the table or was re-added with new metadata.
let mask: HashSet<&FileActionKey> =
    summary.removes.iter().chain(summary.duplicate_adds.iter()).collect();
# let _ = mask;
# Ok(())
# }
```

> [!WARNING]
> Apply both `removes` and `duplicate_adds` to the base. Dropping `duplicate_adds` leaves stale
> rows in your listing for every file that compaction or clustering re-tagged, so your cache
> keeps pointing at data files that are no longer current.

> [!TIP]
> Pick the terminal variant by your cache's shape. Use `_closure` when your cache supports a
> `contains` lookup (`HashSet`, `HashMap`, `BTreeMap`, or a sorted `Vec` with `binary_search`).
> It probes the live-Add set, which is usually much smaller than the base. Use `_iter` only
> when your cache can't do membership lookup and can only stream its keys.

## Correctness requirements

A few properties of the stream are easy to violate and produce a silently wrong listing rather
than an error.

### Errors are sticky

Once `next()` yields an error, the stream is dead. Every later call returns `None`, and the
terminal methods return an error instead of a partial summary. The dedup state is incomplete
after an error, so a partial result would be wrong. To retry, drop the stream and build a fresh
one.

### A vacuumed commit means fall back to a full scan

If consumption fails with a file-not-found error, a commit in the range was vacuumed between
`build()` and draining the stream. Build a fresh incremental scan. It will most likely return
`None` (the commits are no longer available), which is your signal to fall back to a full scan.

### Catalog-managed tables need the log tail on the `Snapshot`

For **catalog-managed tables** (tables whose commits a catalog ratifies), the incremental scan
walks the `Snapshot`'s already-validated commit list and deliberately does not re-list the
`_delta_log/` directory. Staged commits that the catalog has ratified but not yet published
appear only if you passed them to the `Snapshot` when you built it. Build the target `Snapshot`
with the catalog's log tail so the range includes those commits. A fresh storage listing would
miss them. See [Reading Catalog-Managed Tables](../catalog_managed/reading.md).

## Current limitations

The incremental scan is narrower than a full scan. Know these limits before you build on it.

### No predicate or column pushdown

Unlike `scan_builder`, the incremental scan builder takes no predicate and no projection. It
reports every added and removed file in the range, so you can't push data skipping or column
selection into it. Apply any file-level or row-level filtering in your connector after the scan
returns.

### In-range metadata changes aren't surfaced

`build()` validates reader features against the target `Snapshot` only. If the table's schema,
column-mapping mode, or reader features changed _within_ the range, the scan doesn't tell you.
Kernel decodes only the file path and deletion-vector fields, and those stay correct across
such a change. But if your connector reads pass-through fields off each row (`stats`,
`partitionValues`, `baseRowId`), interpret each row against the protocol at that row's commit
version, not against the target `Snapshot`. Surfacing in-range metadata changes is tracked in
[issue #2552](https://github.com/delta-io/delta-kernel-rs/issues/2552).

> [!WARNING]
> If the range may span a metadata change and your connector reads pass-through fields, decode
> each row against its own commit's protocol. Decoding every row against the target `Snapshot`
> silently misreads rows written under the older metadata.

## What's next

- [Time Travel and Snapshot Management](./time_travel.md): building and updating the `Snapshot`
  you scan from
- [Advanced Reads with scan_metadata()](./scan_metadata.md): full-scan file listing when an
  incremental advance isn't possible
- [Reading Change Data Feed](./change_data_feed.md): row-level changes instead of file-level
  changes

## See also

- [Reading Catalog-Managed Tables](../catalog_managed/reading.md): building a `Snapshot` with a
  log tail for staged commits
