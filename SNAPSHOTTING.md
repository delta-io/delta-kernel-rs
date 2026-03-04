# Snapshotting Code Flow

## Entry Point: `Snapshot::builder_for`
(`kernel/src/snapshot.rs:86`)

The entry point for creating a new snapshot is `Snapshot::builder_for(table_root)`. It creates a
new `SnapshotBuilder` for a specific table root URL. This is the recommended way to build a
snapshot from scratch given a table location.

---

## `SnapshotBuilder::build`
(`kernel/src/snapshot/builder.rs:91`)

`build` is called on the builder to actually construct the `Snapshot`. At this point, `log_tail`
is empty (a `Vec::new()`). Here's why:

**What is `log_tail`?**
`log_tail` is a list of pre-known commit log files (`ParsedLogPath`s) that have already been
fetched externally (e.g. by a catalog) and are passed in ahead of time. It represents the "tail"
of the log — recent commit files that we already know about before doing any listing ourselves.

**Why is it empty here?**
`log_tail` starts as `Vec::new()` and only gets populated if the caller explicitly calls
`with_log_tail()` on the builder — a method only available when the `catalog-managed` feature is
enabled. In normal (non-catalog) usage, no one calls `with_log_tail()`, so it stays empty when
`build` is called.

The production code path for a catalog-managed table would look like:
```rust
Snapshot::builder_for(table_root)
    .with_log_tail(ratified_commits)
    .build(engine)
```
where `ratified_commits` is the set of commits the catalog returned that aren't published yet.

**Note for the recovery/backward-scan changes:** `log_tail` does not need any special handling
in the new recovery code. It gets passed through to `list()` → `list_log_files()` which already
handles merging it with filesystem-listed files. The recovery code just needs to pass `log_tail`
along to `list()` the same way the existing `_` arm already does.

**What is the time-travel version?**
Delta Lake tables are append-only — every commit creates a new numbered version (`0000.json`,
`0001.json`, etc.). By default you get the latest version, but you can ask for a specific
historical version (e.g. "what did this table look like at version 5?") — this is time travel.
You set it by calling `.at_version(5)` on the builder before calling `build`. If you don't call
it, `version` stays `None`, meaning "give me the latest".

The time-travel version matters during log listing: if you're targeting version 5, you must not
load any commits or checkpoints newer than that. So if the `_last_checkpoint` hint points to
version 7, that hint is stale and can't be used — `for_snapshot_impl` falls back to a backward
scan to find a usable checkpoint at or before the target version.

`build` then calls `LogSegment::for_snapshot` with the (empty) log_tail, time-travel version, and
other params to construct the log segment (this is the case where we're building from a table
root, rather than incrementally updating an existing snapshot). The goal is to produce a
`LogSegment` — a validated, contiguous bundle of checkpoint + commit files that is ready to be
replayed to reconstruct table state. All the listing and checkpoint-finding logic exists to build
this structure correctly.

---

## `LogSegment::for_snapshot`
(`kernel/src/log_segment.rs:247`)

This is a public wrapper around `for_snapshot_impl`. It:
1. Tries to get the **last checkpoint hint** from `_delta_log/_last_checkpoint` — a small metadata
   file that records the version of the most recent checkpoint, so we don't have to scan from
   scratch.
2. Calls `LogSegment::for_snapshot_impl` with that hint (if it exists).
3. Reports observability metrics (number of commit/checkpoint/compaction files, duration).

---

## `LogSegment::for_snapshot_impl`
(`kernel/src/log_segment.rs:287`)

This is the core listing logic. It determines which log files to load based on whether a valid
checkpoint hint exists and whether we're doing time travel. Four match arms:

1. **`(Some(cp), None)` — valid hint, latest query** — list forward from the hint version,
   picking up newer commits on top.
2. **`(Some(cp), Some(tv)) if cp.version <= tv` — valid hint, time travel** — list from the
   hint through the specified time-travel version.
3. **`(_, Some(end_version))` — stale or absent hint, time travel** — scan backwards in
   1000-version batches to find the last complete checkpoint at or before `end_version`, then list
   forward from it. Falls back to listing from version 0 if no checkpoint exists before
   `end_version`.
4. **`(None, None)` — no hint, latest query** — no upper bound to scan backwards from; list all
   files from version 0.

Returns a `LogSegment` via `try_new()` with the discovered files.

### Java equivalent cases (`getStartCheckpointVersion`)

Java's `SnapshotManager.getStartCheckpointVersion` enumerates all the cases that determine the
starting checkpoint version. Here they are mapped to the Rust match arms:

| Case | Java condition | Java action | Rust arm |
|------|----------------|-------------|----------|
| 1 | Latest query, no `_last_checkpoint`, no `maxCatalogVersion` | List from version 0 | `(None, None)` |
| 2 | Latest query, hint present, no `maxCatalogVersion` | Use hint directly | `(Some(cp), None)` |
| 3 | Latest query, hint present, `maxCatalogVersion` present, hint ≤ catalog version | Use hint | **Excluded** — no catalog support in Rust |
| 4 | Latest query, hint present, `maxCatalogVersion` present, hint > catalog version (race condition) | Backward scan from `maxCatalogVersion` | **Excluded** — no catalog support in Rust |
| 5a | Time-travel, hint present, hint ≤ travel version | Use hint | `(Some(cp), Some(tv)) if cp.version <= tv` |
| 5b | Time-travel, hint present, hint > travel version (stale) | Backward scan from `end_version` | `(_, Some(end_version))` — guard on arm 5a fails, falls here |
| 5c | Time-travel, no hint | Backward scan from `end_version` | `(_, Some(end_version))` — `None` matches `_` |

Cases 3 and 4 (`maxCatalogVersion` race condition) are intentionally excluded — catalog-managed
table support is its own separate feature.

---

## `ListedLogFiles::list_with_checkpoint_hint`
(`kernel/src/listed_log_files.rs:492`)

Called from the `Some(cp)` arms of `for_snapshot_impl`. It calls `Self::list()` starting from
the hint's version up to `end_version`, then validates what it found against the hint in two
stages:

**Stage 1 — did we find any checkpoint at all?**
`checkpoint_parts` in the returned `ListedLogFiles` holds the parts of the single most recent
complete checkpoint found. If it's empty, no checkpoint exists on disk at or after the hint
version — the hint was pointing at something that's gone. This is a hard error today.

> **Note:** This is exactly the case that could be recovered from by falling back to the backward
> scan (listing 1000 versions at a time from the desired version) rather than erroring out.

**Stage 2 — is the checkpoint we found the one the hint claimed?**
Now that we know at least one checkpoint exists, we check whether its version matches the hint.
If it doesn't, a newer checkpoint was written to disk after the hint was last updated — the hint
was stale. This isn't an error: `list()` already found the correct latest checkpoint during
listing, so we just log a warning and return the `ListedLogFiles` as-is, with the corrected
latest checkpoint version in place.

**What the returned `ListedLogFiles` represents:**
At this point it contains the most recent complete checkpoint (whichever version that turned out
to be) plus all commit files on top of it up to `end_version`, plus any CRC/compaction files in
that range. It's the raw listing result — files bucketed by type, but not yet validated for gaps
or version consistency. That stricter validation happens when `LogSegment::try_new()` consumes it.

---

## `list_log_files` — the core listing function
(`kernel/src/listed_log_files.rs:141`)

Called internally by `Self::list()`. Lists all relevant log files from storage in the range
`[start_version, end_version]` and merges them with the `log_tail`.

**Note on catalog scope:** Catalogs only ever write commits (staged commits to
`_delta_log/_staged_commits/`). Checkpoints and CRC files are always written by the Delta client
directly to the filesystem. This means `log_tail` only ever contains commits, and `list()` fully
manages all catalog/`log_tail` merging internally. For the backward scan recovery logic, the only
job is to find the correct checkpoint `start_version` via `find_last_complete_checkpoint_before`
and pass it to `list()` — everything else is handled automatically.

The iterator chain at lines 172-187 does the following:
1. `storage.list_from(start_version)` — forward-only storage listing from the start version
2. `ParsedLogPath::try_from(meta)` — parses each raw `FileMeta` (just a path + size + timestamp)
   into a typed `ParsedLogPath` with a version number and file type
3. `filter_map_ok(should_list)` — drops unparseable files and staged commits; passes errors through
4. `take_while(version <= end_version)` — stops as soon as we pass the end version
5. `try_collect()` — collects into a `Vec`, surfacing any error

After this, `all_files` contains every relevant file in the range: checkpoints, commits,
compacted commits, CRC files, and unknowns. Staged commits are the only thing excluded. No
bucketing by file type has happened yet — that comes in the next step.
