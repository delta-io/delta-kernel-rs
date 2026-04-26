# CRC Prototype 4 — Design Decisions Log

Running log of decisions made while implementing the CRC redesign on `stack/crc_prototype_4` (branched off `main`), with code copied/adapted from the stale `stack/crc_replay_prototype_3` per `~/claude_plans/2026_04_25_crc_design_plan.md`.

## Source-of-truth pointers

- Plan: `~/claude_plans/2026_04_25_crc_design_plan.md`
- Stale prototype: `origin/stack/crc_replay_prototype_3` (10 commits, diverged at `f5b7b5af`)
- Current base: `main` (HEAD `6486bd26`, post #2387)

## Decision log

### 2026-04-25 — Starting state

Current branch already has the "prototype 3" structural foundation from the stale prototype: `Crc` exists, has serde-friendly fields, `CrcDelta` + `Crc::apply` mutator exist, `LazyCrc` is on `Snapshot`, `try_compute_for_txn` builds histograms, `try_write_crc_file` gates on validity. So this is more an evolution than a full rewrite.

### What's missing (vs. plan)

1. **State enums.** `Crc` has flat `table_size_bytes: i64` + `num_files: i64` + a *separate* `file_stats_validity` flag. Plan calls for the typed `FileStatsState` enum so misuse is impossible. Same story for `Option<HashMap>` for DM/txns: plan calls for `DomainMetadataState` / `SetTransactionState` (Complete / Partial / Untracked). Currently `Option::None` doubles as both "not tracked" and "tracked but empty" with comments papering over it.
2. **Eager `Arc<Crc>` on Snapshot.** Today still `Arc<LazyCrc>` (read-on-demand). Plan wants the CRC always materialised (free choice path: at most one batched read per replay range, no `OnceLock` ceremony in get-paths).
3. **`CrcUpdate` (renamed from `CrcDelta`).** Plan unifies "single-commit delta" and "batched-replay accumulator output" under one type with `apply` and `into_fresh_crc`. Today `CrcDelta` only handles single commits (writes path); the snapshot-load path uses `LazyCrc.load → P&M-only`.
4. **Reverse log replay accumulator.** No equivalent of `crc_replay.rs` — today the `LazyCrc` only ever loads a single CRC file from disk; stale CRCs are not "completed" via replay. The P&M replay path (`protocol_metadata_replay.rs`) is parallel infrastructure that only handles two fields.
5. **`Snapshot::load_file_stats` stub.** No public surface for priority-5 recovery yet.
6. **Partial state transitions.** Today `Crc::apply` *skips* DM/txn updates when the base is `None` (= Untracked). Plan wants `Untracked → Partial` so newer commits' DM/txns are observable even if the CRC base was missing them.
7. **Histogram already wired** via `bin_boundaries` plumbing — keep it.

### Decision A — Refactor in place, do NOT add `CrcRaw` serde intermediate

Prototype 3 introduced `CrcRaw` as a flat 1:1-with-disk-spec struct, with `#[serde(from, into)]` on `Crc` to bridge typed enums to flat JSON. Reasons against:

- `Crc`'s public API is `pub(crate)` (only escapes to `pub` under `test-utils`) — no external connectors round-trip it.
- `CrcRaw` doubles the surface area, doubles maintenance burden, every new field touches both structs.
- The typed-state enums can serialise themselves directly with custom `#[serde(with = ...)]` helpers, same as today's `de_opt_vec_to_opt_map` handles `HashMap` ↔ `Vec`.

**Pick:** add custom serde helpers on the new state-enum fields; keep `Crc` as the single source of truth for the JSON. Saves ~150 lines of conversion code and one indirection layer.

### Decision B — `FileStatsState` is the *whole* file-stats field

That is, `Crc.file_stats: FileStatsState` replaces the 3-tuple of `table_size_bytes` + `num_files` + `file_stats_validity` + `file_size_histogram`. The histogram lives *inside* `FileStatsState::Valid { histogram, .. }` and `RequiresCheckpointRead { commit_delta_histogram, .. }`. Indeterminate / Untrackable have no histogram (because what would it mean?).

This makes the variant-and-data invariant compiler-checked. You cannot read `num_files` from an `Indeterminate` state because there's no field there to read.

### Decision C — `CrcRaw` JSON spec field mapping

The on-disk `.crc` JSON spec is fixed (Delta protocol). For a Valid CRC, the relevant fields are:

```
{ "tableSizeBytes": int, "numFiles": int, "numMetadata": 1, "numProtocol": 1,
  "metadata": {...}, "protocol": {...}, "inCommitTimestampOpt": int|null,
  "setTransactions": [...]|null, "domainMetadata": [...]|null,
  "fileSizeHistogram": {...}|null }
```

When deserialising we always start from `Valid` (a CRC file on disk is by definition valid). When serialising, we require `Valid` (writer rejects others via `is_writable`). So the serde mapping is one-way easy: the typed enum and the flat JSON are *isomorphic* on the writable subset.

**Implementation:** custom `Serialize`/`Deserialize` for `Crc` directly that emits the flat JSON shape from the typed `FileStatsState::Valid` variant, and rejects on others. Or — cleaner — make `Crc` derive `Serialize`/`Deserialize` and use `#[serde(flatten)]` on the file-stats sub-struct with a manual writability gate.

After looking at the existing implementation closely, the cleanest path is:

- Keep `Crc` itself derived `Serialize, Deserialize` with the existing flat camelCase fields *visible* to serde.
- Add private `pub` methods on `FileStatsState` to project to/from the flat triple. `Crc` exposes its file-stats *only* through accessors and a `file_stats` enum field that is `#[serde(skip)]`.
- During deserialise: read flat fields into a temporary, then set the enum to `Valid {…}`.
- During serialise: extract flat fields from the `Valid` variant, error on others.

**Update after starting work:** the cleanest realisation is exactly what prototype 3 did with `CrcRaw` — except the maintenance burden complaint above is overstated. `CrcRaw` is a private-to-the-module flat mirror of disk JSON, lives next to `Crc`, and is a one-time write. Going with `CrcRaw` after all. Reverses Decision A.

### Decision D — DM/txn states: `Untracked → Partial` via apply

Per plan, when a commit's DM update lands on a CRC whose `domain_metadata` is `Untracked`, the result is `Partial(map_with_just_those_entries)`. This makes post-commit snapshots' DM cache "warm" for the most-recent domains even when the disk CRC didn't track them.

The Snapshot fast-path branches on the variant:
- `Complete(map)` → return from map (authoritative for both hits and misses)
- `Partial(map)` → check map; on miss for a *specific* domain, do a log replay; on full enumeration, must do a log replay
- `Untracked` → log replay

### Decision E — Defer connector-hint API

`SnapshotBuilder::with_eager_file_stats(bool)` is in the plan's "deferred" bucket. Skip for now. We always do the cheap path (load CRC file, replay if stale). Reyden / Untrackable recovery comes via `Snapshot::load_file_stats(engine)` (stubbed `Err`).

### Decision F — Keep the existing `LazyCrc` plumbing for P&M-only replay

The `LazyCrc` machinery in `protocol_metadata_replay.rs` is finely tuned for the case where we have a CRC file but P&M *might* still be in newer commits. Rewriting that path is out of scope. We keep `LazyCrc` as an internal helper for the P&M-only path inside the incremental snapshot updater (`Snapshot::try_new_from`), and add a new path for the full-CRC build (`try_build_crc_from_file` on the snapshot, called from `try_new_from_log_segment`).

The Snapshot's *stored* CRC moves from `Arc<LazyCrc>` to `Arc<Crc>`. The `LazyCrc` lives only as a transient local var inside the incremental updater. **This is the prototype-3 shape.** Net result: zero external API change to `LazyCrc`, the eager `Arc<Crc>` is the public contract for `Snapshot`.

### Decision G — Direction (reverse vs forward) — DEEP ANALYSIS

**The plan recommends reverse.** I challenged this by going through forward's strengths in detail. Outcome below.

#### Engine API constraint check

`JsonHandler::read_json_files` contract (kernel/src/lib.rs:626-642): "the engine data iterator must first return all the engine data from file 'a', _then_ all the engine data from file 'b'." **The engine returns data in file order.** So direction is purely a kernel-side concern (it depends on whether `find_commit_cover` returns ascending or descending).

`find_commit_cover` ends with `selected_files.reverse()`, returning descending. That's a *convention*, not a constraint. Adding a forward variant (`find_commit_cover_ascending`) would be a few lines.

#### Direction-agnostic vs direction-sensitive aggregations

| Aggregation | Reverse | Forward | Direction-agnostic? |
|---|---|---|---|
| Add `size` sum | += | += | ✓ |
| Remove `size` sum (log only) | -= | -= | ✓ |
| Histogram bins | += / -= | += / -= | ✓ |
| Protocol replace | first-seen-wins (skip if Some) | last-wins (overwrite if Some) | ✓ (symmetric) |
| Metadata replace | first-seen | last-wins | ✓ |
| DM upsert per `domain` | first-seen-wins | last-wins | ✓ |
| txn upsert per `app_id` | first-seen-wins | last-wins | ✓ |
| ICT (newest commit) | first commitInfo seen | last commitInfo seen | ✓ |
| `operation_safe` (any unsafe → Indeterminate) | OR | OR | ✓ |
| Action dedup (path, dv_id) — priority 5 | first-seen-wins | last-wins | ✓ (memory same) |

**Every CRC field is direction-agnostic.** The plan's "priority 5 requires reverse" claim is false: `FileActionDeduplicator` *was designed for* reverse, but reconciliation works in either direction with the same `O(distinct_files)` memory.

#### Forward arguments

1. **Unifies the apply path.** One mutator (`Crc::apply`); one shape (per-commit `CrcUpdate`); no separate accumulator with special `into_fresh_crc` finalization. The build-from-scratch path becomes literally `for batch in commits: crc.apply(extract(batch))`.
2. **Composes with priority 4 (rebase).** Rebase is forward by necessity (apply each winning commit, interleaved with conflict checks). If CRC build is also forward, both share `Crc::apply` AND the per-commit shape.
3. **Matches Delta protocol semantics.** "CRC at N = state of replaying commits 0..N forward." Forward replay literally implements this.
4. **No `into_fresh_crc` ambiguity.** For reverse, an accumulated update represents a *range* of commits, but its semantics (first-seen P/M, summed file stats, etc.) only make sense when applied in one shot. For forward, every per-commit update is self-contained — apply repeatedly without aggregation.
5. **Engine API is direction-agnostic** (proven above). No engine refactoring needed.
6. **Visitor is the same complexity.** `map.insert()` (last-wins) is no harder than `entry().or_insert_with()` (first-wins).

#### Reverse arguments

1. **Existing `read_actions` returns reverse.** Adding ascending requires touching `find_commit_cover` (5 lines).
2. **Current code already produces "one delta, apply once".** The in-memory commit-time path produces a single `CrcDelta` and applies once; reverse-batched-replay fits this model directly.
3. **Aligns with kernel's reverse-replay convention** (`LogReplayProcessor`, `FileActionDeduplicator`). Going forward for CRC is the odd one out.
4. **Plan explicitly recommends reverse.** Diverging from documented design needs stronger evidence than "forward is conceptually nicer."
5. **Forward's main win (rebase composition) is theoretical** — rebase is priority 4, not built yet, may evolve.
6. **For "stale CRC + replay newer commits," reverse-then-apply-once produces the cleanest 2-arg API**: `crc.apply(replay_update)` — one network read produces one mutation.

#### Histogram merge subtlety (forward only)

Forward replay starting from `Crc::default()` has `histogram = None`. The first commit's update has `Some(histogram)`. Today's merge logic drops the histogram if base is `Some` and delta is `None`, but for `(None, Some)` it returns `None` (not great — we should adopt the delta's histogram).

This is a fixable issue but it does mean forward needs a slightly different merge rule:
- `(Some, Some)` → merge
- `(Some, None)` → drop
- `(None, Some)` → adopt delta
- `(None, None)` → stay None

Reverse doesn't hit `(None, Some)` because the reverse-accumulator builds the histogram once at the end.

#### Priority 4 (rebase) compatibility

Both directions support rebase. Rebase forward-applies each winning commit's `CrcUpdate` onto the pre-rebase Crc. The `CrcUpdate` shape is the same regardless of how the original CRC was built. Reverse-built CRCs can be forward-rebased without issue.

#### My take

Forward is conceptually nicer but reverse is closer to the current code. Given:

- Plan explicitly says reverse.
- Current code already does one-delta-one-apply (reverse-style).
- Engine API supports both.
- Visitor logic is mirror-image.
- Both are correct.
- Forward's rebase win is hypothetical (priority 4 is far out).
- Forward needs a histogram merge tweak.

**Going with REVERSE.** Simpler delta from current state, matches plan, defers the rebase-shape question until it actually matters.

#### What this means for the visitor

Reverse-iterate. Visitor:

- **Protocol/Metadata**: first-seen-wins. The accumulator holds `Option<Protocol>` / `Option<Metadata>` — set once, then skip.
- **DomainMetadata**: first-seen-wins per `domain` via `entry().or_insert_with()`.
- **SetTransaction**: first-seen-wins per `app_id`.
- **Add file size**: `+= add.size` always (no dedup — `Incremental` strategy).
- **Remove file size**: `-= remove.size` *only on commit batches* (`is_log_batch=true`). Checkpoint tombstones are skipped (they're for vacuum, not active state).
- **Histogram**: insert add, remove remove, only on commit batches for removes.
- **commitInfo.operation**: any commit row whose operation is not `INCREMENTAL_SAFE_OPS` flips `operation_safe = false`. Missing commitInfo also flips it.
- **commitInfo.inCommitTimestamp**: first seen (= newest commit's) ICT, captured into `in_commit_timestamp`.
- **Missing remove.size**: any remove with null size flips `has_missing_file_size = true`.

After iteration, the accumulator yields a `CrcUpdate`. We then either:
- `base.apply(update)` for stale-CRC + replay (priorities 1).
- `update.into_fresh_crc()` for full-replay (priorities 3, no checkpoint or no CRC).

For checkpoint-bootstrap, the segment includes checkpoint + commits; the same one-pass replay handles both via `is_log_batch` gating (commits get full delta semantics, checkpoint adds get summed in, checkpoint removes are skipped). At the end, `into_fresh_crc()` produces a Valid Crc.

### Decision G.2 — Long-term direction architecture (revised)

User asked for long-term design, not just-for-this-PR thinking. Re-examining:

**Long-term right answer:**

Different consumers need different directions. The architecture should *support both*, with each consumer picking what fits its workload:

| Consumer | Best direction | Why |
|---|---|---|
| Action reconciliation (P5) | Reverse | `FileActionDeduplicator` was designed reverse; sharing primitive |
| Rebase (P4) | Forward per-commit | Interleaved with conflict checks; can't pre-batch |
| Stale-CRC catch-up (P1) | Either | Long-term: pick *forward* (matches commit-time + rebase shape) |
| No-CRC bootstrap with checkpoint (P3) | Reverse | Checkpoint is "state at C", commits are deltas after; reverse handles both uniformly via the accumulator |
| Full replay 0..N (P3) | Reverse | Accumulator + `into_fresh_crc` validates full state at end |

The shared primitives are direction-agnostic:

- `CrcUpdate` (the universal delta type)
- `Crc::apply(CrcUpdate)` (the universal mutator, last-write-wins)
- `CrcUpdate::into_fresh_crc() -> DeltaResult<Crc>` (constructor when no base CRC exists)

The direction-specific helpers are:

- `extract_crc_update_for_batch(batch) -> CrcUpdate` (single-batch extractor; direction-agnostic)
- `ReverseCrcAccumulator` (composes batches with first-seen-wins; for reverse callers)
- *Future:* `LogSegment::read_actions_ascending` for forward callers (rebase + forward stale-catchup option)

### What lands in this PR (long-term-correct subset)

For prototype 4, ship the universal pieces and the reverse accumulator:

1. `CrcUpdate` type with `apply` and `into_fresh_crc`.
2. `Crc::apply` last-write-wins mutator.
3. `ReverseCrcAccumulator` for stale-CRC catch-up AND no-CRC bootstrap.
4. Both consumers share `read_actions` (existing reverse-streaming).
5. The per-batch extractor lives inside `ReverseCrcAccumulator::absorb` for now; extracting it as a standalone function is a future refactor when forward consumers (rebase) need it.

When rebase lands later:
- Pull `extract_crc_update_for_batch(batch) -> CrcUpdate` out of `ReverseCrcAccumulator::absorb`.
- Add `LogSegment::read_actions_ascending`.
- Implement forward-per-commit apply in the rebase code path.
- *Optional:* migrate stale-CRC catch-up to forward (free choice, not blocking).

This shape:
- Doesn't lock us into reverse-only.
- Doesn't ship infrastructure we don't need yet.
- Provides a clean factoring point when rebase forces our hand.

### Decision H — Ignore log compactions in CRC replay (for now)

Per user direction and CLAUDE.md ("log compaction (disabled, #2337)"). The visitor doesn't currently distinguish compaction batches from regular commit batches. If compactions later contain pre-summed deltas, the accumulator would over-count. For now: don't go out of our way to support them. If they appear in a CRC-replay segment, the accumulator treats them as commit batches, which over-counts file stats. Real fix: filter compactions out of the CRC-replay segment, or have the visitor flip `operation_safe = false` when seeing a compaction batch. Tracked as TODO.

### Decision G.1 — Direction is per-use-case, abstractions are direction-agnostic

The user pushed back: action reconciliation needs reverse, rebase needs forward, stale-CRC could be either. The right answer is to make `Crc::apply` + `CrcUpdate` direction-agnostic so the choice for "free" cases is just a per-call decision, not an architecture commitment.

**Concretely:**

- `extract_update_for_batch(batch) -> CrcUpdate` — direction-agnostic per-batch extractor. Each commit has at most one Protocol/Metadata/commitInfo, so within a single batch there's no first/last ambiguity.
- `Crc::apply(CrcUpdate)` — last-write-wins (overwrite if `Some`, `map.insert()`). That's the natural shape for forward-per-batch.
- `ReverseCrcAccumulator` — for reverse callers. Internally does first-seen-wins as it absorbs each batch. Finalizes to a single `CrcUpdate` that's already been merged. The single `Crc::apply` then has nothing to overwrite.

Both paths produce identical Crc state. The choice is a caller knob.

**For this PR**, stale-CRC catch-up uses reverse-accumulator-then-apply, because:
- Closer to current code shape.
- Aligns with priority-5 direction (shared scaffolding).
- Engine `read_actions` already streams reverse.
- Engine API supports both, so this is not a one-way door.

**For priority 4 (rebase)**, when it lands, it'll use forward-per-commit `Crc::apply` directly — no accumulator needed. Same `CrcUpdate` type.

**For priority 5 (reconciliation)**, when it lands, it'll fork the visitor for the wide schema + dedup needs, but feed the result through the same `Crc::apply`.

#### What changes in `Crc::apply` semantics for `CrcUpdate`

The current `CrcDelta` has `domain_metadata_changes: Vec<DomainMetadata>` and `set_transaction_changes: Vec<SetTransaction>`. Replay produces *map-keyed* updates (already deduplicated by domain / app_id). To unify, change the field types to `HashMap<String, _>` matching the in-memory storage. Commit-time path (`Transaction::commit`) builds the map from its `Vec` source.

ICT semantics: today `apply` does `self.in_commit_timestamp_opt = delta.in_commit_timestamp;` unconditionally. Keep that. For commit-time deltas, this sets the new commit's ICT. For reverse-replay updates, this sets the *newest* commit's ICT (because the visitor captured first-seen-in-reverse). Both correct.

operation_safe: today `apply` checks `delta.operation` (Option<String>) against the whitelist. For reverse-replay, the visitor has *already* computed `operation_safe: bool` (any unsafe seen). So the field type changes from `operation: Option<String>` to `operation_safe: bool`. Commit-time path computes this once.

### Decision H — `Snapshot::load_file_stats` is `pub`, returns `SnapshotRef`

The plan says "`Snapshot::load_file_stats(engine) -> SnapshotRef`". Public API surface, returns a *new* snapshot (immutable upgrade) with the recovered file stats. Today: stub returns `Err(Error::FileStatsRecoveryNotImplemented)` (new error variant or generic). Connector contract is locked but recovery is post-MVP.
