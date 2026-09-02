# Time travel and snapshot management

To read a Delta table at a specific version or refresh an existing snapshot to
pick up new commits, you use Kernel's snapshot builder APIs. `SnapshotBuilder`
loads from a table URL, while `IncrementalSnapshotBuilder` updates an existing
`Snapshot`.

Before reading this page, make sure you understand
[Building a Scan](./building_a_scan.md).

## Reading a table at a specific version

Every Delta table maintains a versioned
transaction log. Each commit creates a new version.
By default, `Snapshot::builder_for` loads the latest version. To read a
specific historical version, chain `.at_version()` onto the builder.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// Read the table at version 5
let snapshot = Snapshot::builder_for(&url)
    .at_version(5)
    .build(&engine)?;

println!("Loaded version: {}", snapshot.version());
# Ok(())
# }
```

The `at_version` method accepts a `u64` version number. If the requested
version does not exist in the transaction log, `build` returns an error.

To read the latest version, omit `at_version`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
let snapshot = Snapshot::builder_for(&url)
    .build(&engine)?;
# Ok(())
# }
```

## Refreshing an existing snapshot

If you already hold a `Snapshot` (which is wrapped in an `Arc` as
`SnapshotRef`) and want to check for newer commits, use
`Snapshot::builder_from` instead of rebuilding from scratch. Kernel reuses the
existing Snapshot and applies only the newer table state, avoiding a full log
replay.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// Build an initial snapshot
let snapshot = Snapshot::builder_for(&url)
    .build(&engine)?;
println!("Initial version: {}", snapshot.version());

// Later, refresh to pick up new commits
let updated = Snapshot::builder_from(snapshot)
    .build(&engine)?;
println!("Updated version: {}", updated.version());
# Ok(())
# }
```

The incremental update follows these rules:

1. If you call `.at_version()` with the same version the existing snapshot
   already holds, Kernel returns the existing snapshot without doing any work.
2. If the table has not advanced since the existing snapshot, Kernel returns
   the existing snapshot.
3. If new commits exist, Kernel replays only the commits after the existing
   snapshot's version.
4. You cannot refresh backward. Requesting a version older than the existing
   snapshot produces an error.

You can also refresh to a specific newer version by combining `builder_from`
with `at_version`:

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let snapshot = Snapshot::builder_for(&url).at_version(3).build(&engine)?;
// Refresh from version 3 to exactly version 7
let updated = Snapshot::builder_from(snapshot)
    .at_version(7)
    .build(&engine)?;
# Ok(())
# }
```

### Retaining commits for conflict checks

To inspect every commit added during a refresh, call `skip_new_checkpoints()` before `build()`.
This is useful when your connector must check intervening commits before retrying a write.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel::commit_range::CommitRange;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
let snapshot = Snapshot::builder_for(&url).build(&engine)?;
let previous_version = snapshot.version();

let updated = Snapshot::builder_from(snapshot)
    .skip_new_checkpoints()
    .build(&engine)?;

if updated.version() > previous_version {
    let intervening_commits =
        CommitRange::builder_from(updated.clone(), previous_version + 1).build(&engine)?;
    // Use intervening_commits to decide whether to rebuild the Transaction.
}
# Ok(())
# }
```

The update performs one transaction-log listing for commits after the input
`Snapshot`. `CommitRange::builder_from` reuses the preserved commit-file metadata,
so it doesn't list the log again. Kernel merges catalog-provided commits into
the same range and rejects gaps.

Without `skip_new_checkpoints()`, an incremental update may adopt a newer checkpoint and discard
commit-file metadata covered by it. Skipping new checkpoints keeps that metadata, but increases
the `Snapshot`'s memory use and may make full scans replay more transaction-log commits.

## Getting the timestamp of a version

`Snapshot` exposes `get_timestamp`, which returns the timestamp of the
snapshot's version in milliseconds since the Unix epoch. When the table has
In-Commit Timestamps (ICT) enabled, the method returns the ICT value stored
inside the commit. Otherwise, it falls back to the filesystem's last-modified
time on the commit file.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use std::sync::Arc;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
let snapshot = Snapshot::builder_for(&url)
    .at_version(5)
    .build(&engine)?;

let timestamp_ms = snapshot.get_timestamp(&engine)?;
println!("Version {} was committed at {} ms", snapshot.version(), timestamp_ms);
# Ok(())
# }
```

> [!NOTE]
> `get_timestamp` requires an `&dyn Engine` because it may need to read the
> commit file from storage when In-Commit Timestamps are enabled.

## Resolving timestamps to versions

To time travel by timestamp rather than by version number, use the
`history_manager` module. It exposes three helpers that translate timestamps
(in milliseconds since the Unix epoch) into the version numbers you can pass
to `at_version`.

| Function | Returns |
|----------|---------|
| `latest_version_as_of(snapshot, engine, timestamp, resolved_commit_type)` | The `Commit` for the latest version with a timestamp at or before `timestamp`. |
| `first_version_after(snapshot, engine, timestamp, resolved_commit_type)` | The `Commit` for the first version with a timestamp at or after `timestamp`. |
| `timestamp_range_to_versions(snapshot, engine, start, end)` | A `(start_version, end_version)` pair covering the timestamp range. |

Each helper takes a `Snapshot` reference, which defines the searchable
version range. You typically pass the latest snapshot so the search covers
the table's full history. `timestamp_range_to_versions` is the most
common of the three because it translates a `[start_ts, end_ts]` window
into the version range that change data feed (CDF) queries need to read.

The helper function also take a `resolved_commit_type`, controlling which
versions are eligible. Pass `HistoryCommitType::Recreatable` to restrict
the result to versions whose table state you can actually load.
Pass `HistoryCommitType::Published` to allow any version that still has a commit file.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
# use delta_kernel::history_manager::{latest_version_as_of, HistoryCommitType};
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
// 1. Load the latest snapshot to define the search range.
let latest = Snapshot::builder_for(&url).build(&engine)?;

// 2. Resolve a timestamp (Jan 1, 2024 UTC) to a matching commit.
let timestamp_ms = 1_704_067_200_000;
let commit = latest_version_as_of(&latest, &engine, timestamp_ms, HistoryCommitType::Recreatable)?;

// 3. Time travel to the resolved version.
let snapshot = Snapshot::builder_for(&url)
    .at_version(commit.version)
    .build(&engine)?;
println!(
    "Resolved timestamp {timestamp_ms} to version {} (committed at {} ms)",
    commit.version, commit.timestamp
);
# Ok(())
# }
```

`first_version_after` is the symmetric variant. It returns a `CommitAt` for the
earliest version whose timestamp is at or after the requested timestamp, which
is useful for picking up changes that happened after a known point in time.

To resolve a timestamp range, call `timestamp_range_to_versions`. This is
the primary entry point for CDF queries, which need to translate a user's
timestamp window into the start and end versions to read. The end
timestamp is optional. Pass `None` to indicate no upper bound.

```rust,no_run
# extern crate delta_kernel;
# extern crate delta_kernel_default_engine;
# use delta_kernel_default_engine::DefaultEngine;
# use delta_kernel_default_engine::storage::store_from_url;
# use delta_kernel::{DeltaResult, Snapshot};
use delta_kernel::history_manager::timestamp_range_to_versions;
# fn example() -> DeltaResult<()> {
# let url = delta_kernel::try_parse_uri("/tmp/table")?;
# let store = store_from_url(&url)?;
# let engine = DefaultEngine::builder(store).build();
# let latest = Snapshot::builder_for(&url).build(&engine)?;
let start_ms = 1_704_067_200_000; // Jan 1, 2024 UTC
let end_ms = 1_706_745_600_000;   // Feb 1, 2024 UTC

let (start_version, end_version) =
    timestamp_range_to_versions(&latest, &engine, start_ms, Some(end_ms))?;
# Ok(())
# }
```

These helpers return errors when the timestamp falls outside the range of
commits visible from the snapshot, or when the requested range is empty.
See the `LogHistoryError` variants for the specific failure modes.

## When to use `builder_for` vs. `builder_from`

| Scenario | Method | Why |
|----------|--------|-----|
| First read of a table | `Snapshot::builder_for(url)` | You have no existing snapshot to update from. |
| Time travel to a known version | `Snapshot::builder_for(url).at_version(v)` | You want a specific historical version and have no nearby snapshot. |
| Polling for new commits | `Snapshot::builder_from(existing)` | Reuses the existing Snapshot and applies only newer table state. |
| Advancing to a specific newer version | `Snapshot::builder_from(existing).at_version(v)` | Combines incremental update with a target version. |
| Inspecting commits added during a refresh | `Snapshot::builder_from(existing).skip_new_checkpoints()` | Keeps the complete update range for `CommitRange::builder_from`. |

The key difference is cost. `builder_for` replays the transaction log from the
most recent checkpoint. `builder_from` replays only the commits after the
existing snapshot's version. For long-lived connectors that periodically check
for updates, `builder_from` avoids redundant log replay.

## What's next

- [Column Selection](./column_selection.md) to read specific columns from a
  historical snapshot.
- [Filter Pushdown and File Skipping](./filter_pushdown.md) to apply predicates
  that reduce I/O.
- [Advanced Reads with scan_metadata()](./scan_metadata.md) for distributed
  scan execution.
