# CRC (Version Checksum) Design — `delta-kernel-rs`

This document describes the in-memory CRC architecture in `delta-kernel-rs`: how CRCs flow
through the connector, how the kernel produces and applies CRC deltas, and which log-state
situations are handled by which code path. Focus is on **CUJs** (customer user journeys),
**flow** (data + control), and **state**, not implementation details.

For the field-level mechanics see the source: `kernel/src/crc/{mod,delta,state,file_stats,
file_size_histogram,reader,writer,lazy}.rs` and `kernel/src/log_segment/crc_replay.rs`.

---

## TL;DR

- Every `Snapshot` carries an eager `Arc<Crc>` — the in-memory representation of the table's
  version-checksum state. Always present, never lazy.
- `CrcUpdate` is the universal "delta" type. It captures the changes from one commit (or an
  accumulated range of commits).
- `Crc::apply(CrcUpdate)` is the **only** mutator. It implements the fundamental invariant
  `Crc[N] + CrcUpdate(N→N+1) = Crc[N+1]` (and more generally, `Crc[X] + CrcUpdate(X→M) = Crc[M]`
  for any range).
- All four CRC production paths (commit-time, stale-CRC catchup, no-CRC bootstrap, future
  rebase + recovery) feed the same `Crc::apply`. They differ only in *how* they produce the
  `CrcUpdate`.
- File stats use a typed state enum (`FileStatsState`: Valid / RequiresCheckpointRead /
  Indeterminate / Untrackable) so misuse is unrepresentable. Domain metadata and set
  transactions use the same Complete / Partial / Untracked completeness pattern.

---

## Customer User Journeys (CUJs)

### CUJ 1: Read a snapshot, query domain metadata

```rust
let snap = Snapshot::builder_for(table_url).build(&engine)?;
let zip = snap.get_domain_metadata("zip", &engine)?;
```

**Flow:**

1. `builder_for(...).build(engine)` triggers snapshot loading. The kernel:
   - Lists log files in `_delta_log/`.
   - Builds the `Crc` (see CUJ 5 for variants).
   - Wraps it in `Arc<Crc>` on the `Snapshot`.
2. `get_domain_metadata("zip", engine)` branches on `crc.domain_metadata`:
   - `Complete(map)`: serve from map (hit or miss is authoritative).
   - `Partial(map)`: hit → serve; miss → log replay only for the missed domain.
   - `Untracked`: log replay.

**Cost:** for the common case (CRC at target on disk OR full-replay produces Complete DM),
the second call is **zero I/O** — DM is in memory.

### CUJ 2: Query file stats (vacuum-style readers, etc.)

```rust
// No I/O. Returns Some(FileStats) only when CRC's FileStatsState is Valid.
let stats = snap.get_file_stats();
```

**Flow:** reads `crc.file_stats()`, which returns `Some(FileStats)` only when
`FileStatsState` is `Valid`. For other states (`Indeterminate`, `Untrackable`,
`RequiresCheckpointRead`), returns `None`.

**Cost:** zero I/O — file stats are precomputed during snapshot load.

**Recovery:** if `crc.file_stats_validity()` is `Indeterminate` or
`RequiresCheckpointRead`, call

```rust
let recovered: SnapshotRef = snap.load_file_stats(&engine)?;
```

to perform a full action-reconciliation pass and rebuild absolute file stats. The
recovered snapshot has `Valid` file stats (or `Untrackable` if a remove with missing
size was found during recovery, in which case byte-level stats are permanently
impossible). All other CRC fields (P/M, DM, txns, ICT) are preserved.

If `crc.file_stats_validity()` is already `Untrackable`, `load_file_stats` returns
`Error::ChecksumWriteUnsupported` — there is no recovery path.

### CUJ 3: Commit a transaction

```rust
let txn = snap.transaction(committer, &engine)?
    .with_operation("WRITE")
    .with_domain_metadata("zip", "zap1");
let committed = txn.commit(&engine)?.unwrap_committed();
let new_snap = committed.post_commit_snapshot().unwrap();
```

**Flow:**

1. Transaction stages add/remove file metadata, DM changes, set transactions, etc.
2. On `commit`:
   - Kernel writes the commit JSON via `Committer`.
   - On success, kernel builds `CrcUpdate` from staged actions:
     - File stats: `FileStatsDelta` (net counts, bytes, histogram).
     - DM map: keyed by domain name.
     - txn map: keyed by app_id.
     - ICT, P/M (only if changed), `operation_safe` (precomputed from `with_operation`).
   - `read_snapshot.crc.apply(update)` produces the post-commit CRC.
   - Wraps in a new `Snapshot` (post-commit snapshot).
3. Caller may now call `new_snap.write_checksum(&engine)` to persist `Crc[N+1]` as `N+1.crc`.

**Invariant:** `Crc[N] (read) + CrcUpdate(N→N+1) (txn) = Crc[N+1] (post-commit)`.

### CUJ 4: Write a checksum file

```rust
let (result, new_snap) = snap.write_checksum(&engine)?;
match result {
    ChecksumWriteResult::Written => /* CRC at this version persisted */,
    ChecksumWriteResult::AlreadyExists => /* another writer beat us, benign */,
}
```

**Flow:**

1. Check if `<version>.crc` already exists in the log segment → `AlreadyExists`, benign.
2. `try_write_crc_file(engine, &path, &crc)`:
   - Validates `crc.file_stats.is_writable()` — only `Valid` states are writable. Non-Valid
     states return `Error::ChecksumWriteUnsupported`.
   - Serializes via `serde_json::to_vec(&crc)` (uses `CrcRaw` intermediate to map typed enums
     to flat JSON).
   - Writes via `engine.storage_handler().put(path, data, overwrite=false)` — atomic
     put-if-absent. Concurrent writers race; one wins, others get `FileAlreadyExists`.

**Per Delta protocol:** writers MUST NOT overwrite existing CRC files.

### CUJ 5: Snapshot loading variants

The kernel handles multiple log states transparently. Caller writes the same `Snapshot::builder_for(url).build(engine)`; the kernel picks the right path.

| Log state | Path | Cost |
|---|---|---|
| CRC file at target version | Load directly from disk, no log replay. P&M comes from CRC. | One storage read. |
| CRC file at older version (stale) + commits since | Load stale CRC, reverse-replay commits in `(crc_version, end_version]`, apply once via `Crc::apply`. | One storage read + one batched read of commits. |
| No CRC file, has checkpoint + commits | Reverse-replay full segment (commits + checkpoint actions), `into_fresh_crc()`. | One batched read of commits + checkpoint. |
| No CRC file, no checkpoint | Reverse-replay all commits 0..N, `into_fresh_crc()`. | One batched read of all commits. |
| CRC file corrupt | Warn, fall back to full log replay. | One storage read (CRC, fails) + one batched read. |
| Time travel target older than CRC | Ignore the CRC, fall back to full log replay. | Same as no-CRC path. |

All paths produce an `Arc<Crc>` on the Snapshot. The eager design means the Snapshot's
contract is consistent: callers always have a `Crc` to query.

### CUJ 6: Catalog-managed table snapshot loading

Same as CUJ 5, but the log segment includes catalog-provided "log tail" commits via
`SnapshotBuilder::with_log_tail()`. The reverse-replay accumulator processes log-tail
batches the same as any other commit batches — no special casing.

---

## Architecture

### The universal mutator

```
Crc[N] + CrcUpdate(N→N+1) = Crc[N+1]
       ^                    ^
   apply() is forward       result is the new CRC at N+1
```

`Crc::apply(CrcUpdate)` is the only mutator. It uses last-write-wins semantics: `Some` fields
in the update overwrite, `None` fields don't change the base. Maps upsert per key (DM
tombstones remove the entry).

Repeated apply composes: `Crc[N].apply(δ₁); .apply(δ₂); ...` is equivalent to
`Crc[N].apply(δ₁ ⊕ δ₂ ⊕ ...)` where `⊕` is delta-merge.

### Production strategies

The four ways to produce a `CrcUpdate`:

1. **Commit-time** (`Transaction::build_crc_update`): one commit's staged actions →
   one `CrcUpdate`. Direction-agnostic (single commit).
2. **Reverse-accumulate** (`LogSegment::build_crc_from_stale`,
   `LogSegment::build_crc_from_scratch`): stream commits descending via
   `read_actions`; visitor (`CrcReplayVisitor`) does first-seen-wins per key, sums file
   stats; finalize → one `CrcUpdate`. Used for:
   - Stale-CRC catchup (apply the update onto the loaded base CRC).
   - No-CRC bootstrap (call `into_fresh_crc()` to construct a fresh `Crc`).
3. **Action reconciliation** (`LogSegment::recover_file_stats`, called by
   `Snapshot::load_file_stats`): a dedicated `ReconciliationVisitor` reads the entire
   segment with `(path, dv_unique_id)` deduplication, building absolute file stats
   from scratch. The result is a `FileStatsState` that replaces the snapshot's CRC
   `file_stats` field; other CRC fields are preserved.
4. **Forward per-commit** *(future)*: rebase / row-level concurrency. Each winning commit's
   actions → one `CrcUpdate`, applied per-commit with conflict checks interleaved. Same
   `CrcUpdate` shape, same `Crc::apply`.

The first three are implemented today. Strategies 1, 2, and 4 feed the same `Crc::apply`
mutator; strategy 3 directly produces a `FileStatsState` and patches the CRC field.
Direction (forward vs reverse) is a per-strategy implementation choice; the `CrcUpdate`
shape is direction-agnostic.

### State enums

```
FileStatsState ::= Valid { num_files, table_size_bytes, histogram }
                 | RequiresCheckpointRead { commit_delta_files, ..., commit_delta_histogram }
                 | Indeterminate
                 | Untrackable

DomainMetadataState ::= Complete(HashMap) | Partial(HashMap) | Untracked
SetTransactionState ::= Complete(HashMap) | Partial(HashMap) | Untracked
```

Each variant carries exactly the data that variant requires. You cannot read an absolute
file count from `Indeterminate` because there's no field there to read — the type system
prevents the bug.

State transitions (during `Crc::apply`):

| Current FileStats | + safe op | + unsafe op | + missing remove.size |
|-------------------|-----------|-------------|-----------------------|
| Valid | Valid | Indeterminate | Untrackable |
| RequiresCheckpointRead | RCR | Indeterminate | Untrackable |
| Indeterminate | Indeterminate | Indeterminate | Untrackable |
| Untrackable | Untrackable | Untrackable | Untrackable |

| Base DM/txn | + entries from update | + nothing |
|-------------|-----------------------|-----------|
| Complete | Complete (upserted) | Complete |
| Partial | Partial (upserted) | Partial |
| Untracked | Partial (entries from update) | Untracked |

The `Untracked → Partial` transition is important: when a stale CRC base lacks DM but newer
commits have DM, the post-replay state is `Partial(map_with_just_those_entries)`. Snapshot DM
queries can serve hits from `Partial` directly; only misses fall through to log replay.

### On-disk JSON ↔ in-memory enums

`CrcRaw` is a private serde intermediate that maps the typed in-memory `Crc` to/from the flat
JSON shape from the Delta protocol spec:

```
CrcRaw { tableSizeBytes, numFiles, numMetadata, numProtocol,
         metadata, protocol, inCommitTimestampOpt,
         setTransactions: Option<Vec>, domainMetadata: Option<Vec>,
         fileSizeHistogram: Option<...> }
```

Mediated by `#[serde(from = "CrcRaw", into = "CrcRaw")]` on `Crc`. Deserialization always
yields `Valid` file stats and `Complete` DM/txns (or `Untracked` if the JSON omits them). The
writer (`try_write_crc_file`) gates on `is_writable()` so non-Valid CRCs are never persisted.

---

## Cases covered

The following scenarios are handled by the implementation. Test coverage is in
`kernel/src/crc/{state,delta,mod,reader,writer}.rs` (unit), `kernel/src/log_segment/{crc_replay,crc_tests}.rs` (snapshot-load unit tests), and `kernel/tests/crc.rs` (integration).

### Bootstrap (snapshot loading from disk)

| # | Scenario | State after load |
|---|----------|------------------|
| B1 | CRC file at target, all fields populated | Valid + Complete + ICT |
| B2 | CRC file at target, missing DM | Valid + Untracked DM |
| B3 | CRC file at target, missing histogram | Valid (no histogram) + Complete |
| B4 | CRC file at target, missing ICT | Valid + Complete + ICT=None (falls back to commit file on query) |
| B5 | Stale CRC at X + commits X+1..M, all safe ops | Valid + Complete (or Partial DM if older CRC was Untracked) |
| B6 | Stale CRC at X, replay hits ANALYZE STATS | Indeterminate file stats (DM/txn/P/M still updated) |
| B7 | Stale CRC at X, replay hits a remove with missing size | Untrackable file stats |
| B8 | CRC at target corrupt | Warn, fall back to full replay (build_crc_from_scratch) |
| B9 | Stale CRC at X corrupt | Warn, fall back to full replay |
| B10 | No CRC, V1 single-part checkpoint | Valid + Complete from checkpoint+commits |
| B11 | No CRC, V1 multi-part checkpoint | Same as B10 (all parts read) |
| B12 | No CRC, V2 checkpoint with sidecars | Same, sidecars read via existing checkpoint stream |
| B13 | No CRC, no checkpoint, several commits | Valid + Complete from full reverse replay |
| B14 | No CRC, ANALYZE STATS in log | Indeterminate file stats |
| B15 | Time travel to v50 when CRC at v100 exists | Ignore that CRC, fall back to full replay |
| B16 | Catalog-managed log tail | Reverse-replay treats log-tail batches uniformly |

### Post-commit (in-memory chaining)

| # | Scenario | Transition |
|---|----------|------------|
| P1 | CREATE TABLE | Crc[0] built via `CrcUpdate::into_fresh_crc` |
| P2 | Blind append | Valid → Valid (sums + histogram bins inserted) |
| P3 | MERGE / UPDATE / DELETE | Valid → Valid |
| P4 | OPTIMIZE | Valid → Valid (many adds + many removes; net file count drops) |
| P5 | DV update (remove + add same path/size) | Valid → Valid (net zero) |
| P6 | Empty commit (commitInfo only) | Valid → Valid (only ICT updates) |
| P7 | Schema evolution (Metadata change) | New metadata applied |
| P8 | Protocol upgrade | New protocol applied |
| P9 | Domain metadata set | DM upsert |
| P10 | Domain metadata remove (tombstone) | DM removed from map |
| P11 | Set transaction | txn upsert |
| P12 | ICT enabled mid-table | ICT None → Some |
| P13 | ICT disabled mid-table | ICT Some → None |
| P14 | REPLACE TABLE (RTAS) | Big churn but still Valid |
| P15 | Pre-commit Crc was Indeterminate | Stays Indeterminate |
| P16 | Pre-commit Crc was Untrackable | Stays Untrackable |
| P17 | Unknown / unrecognized operation | operation_safe=false → Indeterminate |

### Read fast paths

| # | Scenario | Behavior |
|---|----------|----------|
| R1 | get_domain_metadata, CRC has Complete DM | Serve from CRC, no I/O |
| R2 | get_domain_metadata, CRC has Partial DM, hit | Serve from CRC, no I/O |
| R3 | get_domain_metadata, CRC has Partial DM, miss | Log replay for missing domain |
| R4 | get_domain_metadata, CRC has Untracked DM | Full log replay |
| R5 | get_app_id_version, txns Complete | Serve from CRC, no I/O |
| R6 | get_app_id_version, txns Partial, hit | Serve from CRC, no I/O |
| R7 | get_app_id_version, txns Partial, miss | Full log replay |
| R8 | get_app_id_version, txns Untracked | Full log replay |
| R9 | get_in_commit_timestamp, CRC has ICT | Serve from CRC, no I/O |
| R10 | get_in_commit_timestamp, CRC has ICT=None | Fall back to reading latest commit file |

### Write checksum

| # | Scenario | Result |
|---|----------|--------|
| W1 | CRC already on disk at this version | `AlreadyExists` (idempotent, benign) |
| W2 | CRC has Valid file stats | `Written` |
| W3 | CRC has RequiresCheckpointRead/Indeterminate/Untrackable | `Error::ChecksumWriteUnsupported` |
| W4 | Two writers race | One wins (`Written`), other gets `AlreadyExists` (atomic put-if-absent) |
| W5 | CRC has Complete DM (typical post-commit) | DM serialized as `[...]` array |
| W6 | CRC has Partial DM | DM omitted from JSON (only Complete is persisted) |

### Concurrency and safety

| # | Scenario | Behavior |
|---|----------|----------|
| C1 | Two writers commit at v101 | One wins (committer atomic put), other gets `ConflictedTransaction` |
| C2 | Two writers attempt v101.crc concurrently | One succeeds, other gets `AlreadyExists` (benign per protocol) |
| C3 | CRC written but `_last_checkpoint` not updated | Independent — CRC is just a hint |
| C4 | Reader loads snapshot during concurrent commit | Sees consistent state at its version (snapshots are immutable) |

### Recovery via `Snapshot::load_file_stats`

| # | Scenario | Behavior |
|---|----------|----------|
| Rec1 | Indeterminate Crc, caller calls `load_file_stats` | Full reverse-replay with `(path, dv_unique_id)` deduplication; transitions to Valid (or Untrackable if a Remove with missing size is observed). |
| Rec2 | Untrackable Crc, caller calls `load_file_stats` | Returns `Error::ChecksumWriteUnsupported` (permanently unrecoverable). |
| Rec3 | RequiresCheckpointRead, caller calls `load_file_stats` | Same full-replay pass: reads commits + checkpoint together, dedups by `(path, dv_unique_id)`, transitions to Valid. |
| Rec4 | Valid Crc, caller calls `load_file_stats` | No-op fast path; returns `Arc::clone(self)`. |

All other CRC fields (P/M, DM, txns, ICT) are preserved through recovery; only
`file_stats` is rebuilt.

---

## Connector usage patterns

### "Lazy" / read-mostly (OSS Spark connector, kernel-java)

```
Snapshot::builder_for → build → query DM/txn/ICT/file_stats from in-memory Crc.
No write_checksum unless writing.
```

Cost: one snapshot load (one batched read). Subsequent queries are zero I/O.

### "Eager" streaming writer

```
Loop:
  txn = snap.transaction(committer, engine)
  txn.add_files(...)
  committed = txn.commit(engine)?.unwrap_committed()
  let (_, new_snap) = committed.post_commit_snapshot()
      .unwrap()
      .write_checksum(engine)?;
  snap = new_snap
```

Each iteration: one commit + one CRC file written. No re-read of the log between commits
because the post-commit snapshot has `Crc[N+1]` in memory.

### Cost-aware writer (advanced)

```
let crc = snap.get_current_crc_if_loaded_for_testing();  // test API today; production API TBD
match crc.file_stats_validity() {
    Valid => { snap.write_checksum(engine) }
    Indeterminate | Untrackable => { /* skip CRC write, log warning */ }
    RequiresCheckpointRead => { snap.load_file_stats(engine)?.write_checksum(engine) }
}
```

Connectors can inspect the validity classification via `FileStatsValidity` and decide whether
to write CRC synchronously, asynchronously, or call `load_file_stats` first.

---

## Log state situations and the right path

The kernel automatically picks the right CRC build strategy based on the log state. Connectors
just call `Snapshot::builder_for(...).build(engine)` and get a `Snapshot` with `Arc<Crc>`.

```
log files → list → log segment → try_build_crc_from_file?
                                  yes (CRC at target):
                                    load → done (no log replay)
                                  yes (CRC stale, X < target):
                                    load + segment_after_crc(X).build_crc_from_stale
                                          → reverse replay X+1..target
                                          → accumulator → CrcUpdate
                                          → base.apply(update) → Crc[target]
                                  no:
                                    log_segment.build_crc_from_scratch
                                          → reverse replay full segment (commits + checkpoint)
                                          → accumulator → CrcUpdate
                                          → into_fresh_crc → Crc[target]
```

For a stale or corrupt CRC that fails to load: warn and fall back to the no-CRC path.

---

## Performance characteristics

- **One batched engine call per replay range.** `LogSegment::read_actions` calls
  `engine.json_handler().read_json_files(&all_files, schema, None)` once per range. The
  engine controls all I/O parallelism internally.
- **Eager Crc cost:** one extra parquet read per V1 parquet checkpoint at snapshot load
  (sidecar probe — V1 checkpoints written by kernel include an empty sidecar field in the
  schema; the existing checkpoint-stream code path probes for sidecars when the read schema
  includes Add/Remove). For V2 checkpoints with real sidecars, this read is necessary
  anyway. Trade-off: a small constant cost per snapshot load in exchange for free DM/txn/file
  stats queries forever after.
- **Histogram merge:** short-circuits to `None` on boundary mismatch. Connectors that pass
  custom bin boundaries should be consistent across writes.
- **Per-batch accumulator:** does not allocate per row. Visitor uses typed `GetData`
  accessors and accumulates into pre-allocated state.

---

## Known costs and trade-offs

1. **Sidecar probe on V1 parquet checkpoints.** Documented in the metric tests
   (`tests/metrics/snapshot_load.rs`). Could be elided with a smarter check
   ("classic-named single part = always V1, skip sidecar probe") but is structurally
   correct as-is.
2. **`Untracked → Partial` may surprise callers.** A snapshot loaded from a CRC that lacked
   DM, then incrementally updated, has a Partial DM map after the update. Specific-domain
   queries against Partial that hit the cache return without I/O, but the cache is not
   exhaustive — callers requesting "all domains" trigger a full log replay.
3. **Histogram dropped on degraded state.** Indeterminate and Untrackable have no histogram
   field. Recovery via `load_file_stats` rebuilds the histogram from active files (using
   default bin boundaries).
4. **Operation safety whitelist is conservative.** `INCREMENTAL_SAFE_OPS` lists
   `WRITE`, `MERGE`, `UPDATE`, `DELETE`, `OPTIMIZE`, `CREATE TABLE`, `REPLACE TABLE`,
   `CREATE TABLE AS SELECT`, `REPLACE TABLE AS SELECT`, `CREATE OR REPLACE TABLE AS SELECT`.
   Unknown ops (including new Delta protocol additions) default to unsafe → Indeterminate.
   Conservative but recoverable.
5. **Log compactions are not yet handled.** The accumulator treats compaction batches as
   regular commit batches, which over-counts. Log compaction is currently disabled (#2337);
   when re-enabled, compactions need to be filtered out of the CRC-replay segment or have
   their own visitor logic.

---

## Future work

- **Priority 4 (rebase / row-level concurrency):** implement forward per-commit apply onto a
  pre-rebase `Crc`. Adds `LogSegment::read_actions_ascending` helper. Reuses `CrcUpdate` and
  `Crc::apply`.
- **Priority 5 (file stats recovery):** real implementation of `Snapshot::load_file_stats`.
  Wide schema includes `add.path`, `add.deletionVector`, `add.size`. Forks the visitor for
  `FileActionDeduplicator` integration. Recovers `Indeterminate` to `Valid`. Handles
  `RequiresCheckpointRead` by reading checkpoint adds and merging with deltas.
- **Connector hint API:** `SnapshotBuilder::with_eager_file_stats(bool)` for callers that
  want to skip the eager CRC build (e.g. for time-travel reads where file stats aren't
  needed).
- **Log compaction support:** filter compaction batches out of CRC-replay or fork visitor.
- **Smarter sidecar probe:** classic-named single-part checkpoints don't need sidecar
  probing — a small optimization.

---

## File map

| File | Purpose |
|------|---------|
| `kernel/src/crc/mod.rs` | `Crc` struct, `CrcRaw` serde intermediate, `minimal_from_pm` |
| `kernel/src/crc/state.rs` | `FileStatsState`, `DomainMetadataState`, `SetTransactionState`, `FileStatsValidity` |
| `kernel/src/crc/delta.rs` | `CrcUpdate`, `Crc::apply`, `into_fresh_crc`, `transition_file_stats`, histogram merge |
| `kernel/src/crc/file_stats.rs` | `FileStats`, `FileStatsDelta`, `try_compute_for_txn`, `is_incremental_safe` whitelist |
| `kernel/src/crc/file_size_histogram.rs` | `FileSizeHistogram`, default bins, `try_apply_delta`, `try_add`, `try_sub` |
| `kernel/src/crc/reader.rs` | `try_read_crc_file` (storage handler) |
| `kernel/src/crc/writer.rs` | `try_write_crc_file` (storage handler, gates on `is_writable`) |
| `kernel/src/crc/lazy.rs` | `LazyCrc` helper for the P&M-replay path (used internally by incremental snapshot updates) |
| `kernel/src/log_segment/crc_replay.rs` | `CrcReplayVisitor`, `CrcReplayAccumulator`, `build_crc_from_stale`, `build_crc_from_scratch` |
| `kernel/src/snapshot/mod.rs` | `Snapshot.crc: Arc<Crc>`, `try_build_crc_from_file`, `build_crc`, `new_post_commit`, `write_checksum`, `load_file_stats` (stub) |
| `kernel/src/transaction/mod.rs` | `build_crc_update` (commit-time delta production), `into_committed` (CREATE TABLE → fresh Crc, otherwise apply onto read snapshot's Crc) |
| `kernel/tests/crc.rs` | Integration tests for CRC API on `Snapshot` |
| `kernel/src/log_segment/crc_tests.rs` | Unit tests for P&M replay + ICT reads with CRC files |

---

## Summary

The CRC system in `delta-kernel-rs` is built around **one mutator** (`Crc::apply`),
**one delta type** (`CrcUpdate`), and **typed state enums** that make invalid states
unrepresentable. Every snapshot has an eager `Arc<Crc>` available for zero-I/O DM / txn /
ICT / file-stats queries.

The kernel transparently handles every common log state: CRC at target, stale CRC, no CRC
+ checkpoint, no CRC + no checkpoint, corrupt CRC, time travel. Connectors call
`Snapshot::builder_for(...)` and get the right behavior. For writers, the post-commit
chain (`Crc[N] + CrcUpdate(N→N+1) = Crc[N+1]`) keeps the CRC up-to-date in memory and
lets `write_checksum` persist it cheaply.

Future priorities (rebase, action reconciliation) reuse the same primitives — the design is
ready for them without a refactor.
