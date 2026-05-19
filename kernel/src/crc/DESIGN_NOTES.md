# CRC reverse-replay: design decisions

> Branch: `stack/incremental_crc_5` (PR4 of the CRC MVP-Prime plan).
> This branch will **not** be merged. It will be split into a pre-factor PR + the
> production PR that lands `LogSegment::build_crc_delta_from_stale`. This document
> captures the decisions we made along the way that differed from the original plan
> at `~/claude_plans/2026_05_13_crc_mvp_prime_plan.md`. Read it before splitting.

## TL;DR

| Topic | Initial plan | What we did |
|---|---|---|
| Per-commit invariant | Per-batch heuristic (matches prototype) | Per-commit via `_file` metadata column |
| ICT semantics | `Option<Option<i64>>` in `CrcDelta` for "observed vs unobserved" | Flat `Option<i64>`; only the first/newest commit sets ICT; mismatch caught on the write path |
| `CrcDelta.is_incremental_safe` | Two fields (`operation: Option<String>` + `has_missing_file_size`) | Single `bool` collapsed at producer; eager `warn!` at detection site |
| `CrcDelta` DM/txn shape | `Vec<DomainMetadata>` / `Vec<SetTransaction>` ("*_changes") | `HashMap<String, _>` keyed by domain / app_id (pre-factor) |
| Accumulator shape | Bag of fields | `delta: CrcDelta` + scaffolding |
| `base_version` | Stored on `Crc` (vague) | Separate `Version` parameter; explicit precondition error |
| Compaction files | Supported | Skipped (TODO) |
| ICT/protocol consistency check | Implicit | Enforced on the write path in `try_write_crc_file` |

## Pre-factor PR (split this out before merging the production PR)

Mark everything below with `PREFACTOR-PR:` comments in code so the boundary is easy
to spot at split time. These touch shared shapes / call sites used by the existing
forward producer as well as the new reverse producer, so they make most sense as a
standalone landing.

1. **`CrcDelta` shape change** in `crc/delta.rs`:
   - `domain_metadata_changes: Vec<DomainMetadata>` → `domain_metadata: HashMap<String, DomainMetadata>`.
   - `set_transaction_changes: Vec<SetTransaction>` → `set_transactions: HashMap<String, SetTransaction>`.
   - `operation: Option<String>` + `has_missing_file_size: bool` → single `is_incremental_safe: bool`.
   - `in_commit_timestamp: Option<i64>` (already flat — no change).
   - Module doc, struct doc, and `Crc::apply` body updated to match.
2. **Forward producer** in `transaction/mod.rs::build_crc_delta`:
   - Converts caller's `Vec` shapes to `HashMap` at the boundary.
   - Computes `is_incremental_safe` once from `self.operation`.
   - Ideal end state: push the `HashMap` shape further up the chain
     (`generate_domain_metadata_actions`, `create_commit_metadata`) so the boundary
     conversion disappears. The current PR doesn't do this — the conversion is
     marked `PREFACTOR-PR:`.
3. **Apply tests** in `crc/delta.rs`: every `CrcDelta { domain_metadata_changes: vec![...] }`
   test fixture updated to `HashMap::from([...])`. Same for `set_transaction_changes`.

The production PR (reverse-replay) then lands cleanly on top of this shape.

## Decision: per-commit boundaries via `_file` metadata column

**Problem.** Prototypes (`stack/crc_prototype_4`, `stack/crc_replay_prototype_3`) and
the original plan all used a **per-batch** heuristic: "if this batch had file
actions but no commitInfo, mark unsafe." That's wrong when a single commit's
actions span multiple batches — the per-batch check spuriously fires on the
file-action batch even when commitInfo arrives in a later batch.

The CDF code path (`table_changes/log_replay.rs`) solves this by reading one file
at a time, sacrificing engine-level parallelism in `read_json_files`.

**Our solution.** Pass **all** plain commit files to `read_json_files` in a single
call (preserves the engine's `buffered(1000)` parallelism) and project the `_file`
metadata column (`MetadataColumnSpec::FilePath`). The visitor reads `_file` from
row 0 of each batch (URL is constant within a batch per JsonHandler contract); a
URL change marks a commit boundary. The per-commit flags
(`current_commit_saw_file_action`, `current_commit_saw_commit_info`) live in the
accumulator and persist across batches of the same file. The per-commit invariant
is checked in `process_commit_file_end`, called either on file transition or
explicitly at end-of-replay for the final commit.

**Why it works.**
- `JsonHandler::read_json_files` returns batches in file-input order.
- The `_file` column gives us O(1) per-batch file identity without per-row cost.
- A commit's batches are always contiguous in the stream (one file = one commit
  for plain commit files), so a URL transition cleanly demarcates commits.

**Cost.**
- Compaction files are skipped (they pack many commits into one file, breaking
  both per-commit boundaries and first-seen-in-reverse semantics). Documented as
  a TODO. Plain commits give full coverage of every version, so skipping
  compactions is only a perf optimization loss, not a correctness loss.
- One extra column in the projected schema. Cheap.

## Decision: ICT — first-commit-only capture, write-path validation

**Problem.** The Delta spec says `inCommitTimestampOpt` is present in a CRC iff
the table has the `inCommitTimestamp` writer feature enabled (PROTOCOL.md L409).
The original plan modeled this with `Option<Option<i64>>` in `CrcDelta`: outer
`Some` means "delta observed ICT," outer `None` means "leave the base's ICT
alone." That's elegant for forward producers but the reverse case has subtleties:

- What if the newest commit has commitInfo but lacks `inCommitTimestamp`? Per
  spec, that's a malformed commit on an ICT-enabled table — but our visitor
  can't tell whether ICT is enabled (it doesn't carry the effective protocol).
- What if the newest commit has no commitInfo at all? Same: we can't classify.

**Our solution (in two parts).**

1. **Visitor: first-commit-only capture.** Drop the nesting; `CrcDelta.in_commit_timestamp`
   is flat `Option<i64>`. The reverse visitor captures ICT from the FIRST commit
   (newest in reverse iteration) via the `is_first_commit` gate. After the first
   file-to-file transition, `is_first_commit = false` and subsequent (older)
   commits do NOT touch ICT. If the newest commit has no commitInfo (or has it
   but without `inCommitTimestamp`), the captured value is `None`. We **do not**
   flip `is_incremental_safe` for ICT issues — that flag is for file stats.
2. **Write path: protocol-consistency catch-net.** `try_write_crc_file` in
   `crc/writer.rs` rejects any `Crc` where the ICT feature and the ICT value
   disagree (both directions). This means: if a malformed commit produced a CRC
   with `inCommitTimestamp = None` but the protocol has the feature enabled, the
   write fails with `Error::ChecksumWriteUnsupported`. Three tests in
   `crc/writer.rs` lock this contract:
   - `test_write_rejects_ict_feature_without_value`
   - `test_write_rejects_ict_value_without_feature`
   - `test_write_accepts_neither_ict_feature_nor_value`

**Why this layering.** The visitor doesn't have the effective protocol available
(`Crc` doesn't carry the table's effective protocol at any given version — only
the `protocol` field captured from the delta itself, which may be `None` for
non-protocol commits). The write path DOES have the full `Crc` and can do the
consistency check. Pushing validation to the write boundary keeps the visitor
simple and centralizes the invariant.

**Test naming for the ICT cases** (read-path):
- `delta_in_commit_timestamp_observation` rstest covers six scenarios:
  `with_ict`, `without_ict`, `two_commits_newest_wins`,
  `newest_without_ict_clears_older_ict`, `newest_commit_has_no_commit_info_older_has_ict`,
  `no_commit_info_at_all`. The last two pin the user's literal example.

## Decision: single `is_incremental_safe: bool` (collapse two signals)

**Problem.** Original `CrcDelta` had two orthogonal-looking signals:
- `operation: Option<String>` — operation safety (`ANALYZE STATS` is unsafe).
- `has_missing_file_size: bool` — a remove row had `path` set but `size` null.

Apply OR'd them. Two fields, one effective bool.

**Our solution.** Collapse to a single `is_incremental_safe: bool` on `CrcDelta`.
Producers compute the AND of their internal signals at output time:
- Forward producer: `is_incremental_safe = is_incremental_safe(operation_string)`
  (the missing-size case can't reach `build_crc_delta` because
  `try_compute_for_txn` errors on null `size` via `get` instead of `get_opt`).
- Reverse producer: tracks `operation_safe` and `has_missing_remove_size`
  internally, combines at `into_crc_delta`. Each cause emits its own `warn!` at
  the detection site (carries the specific commit URL / remove path) instead of
  one summary warning at end-of-replay.

**Trade-off.** Producers lose the structural distinction in the output, but the
warning logs preserve diagnostic granularity. Apply's state machine is
substantially simpler (no OR clause in `transition_file_stats`).

## Decision: accumulator literally holds a `CrcDelta`

```rust
struct CrcReplayAccumulator {
    delta: CrcDelta,          // <- the thing being built; mutated batch-by-batch
    is_first_commit: bool,    // gates ICT capture (newest commit only)
    current_file_url: Option<String>,
    current_commit_saw_file_action: bool,
    current_commit_saw_commit_info: bool,
}
```

`into_crc_delta()` is `self.delta`. No field-by-field conversion. The accumulator
is honest about what it is: "a `CrcDelta` being built, plus the tracking to
build it correctly."

**Pre-condition for this clean shape.** The HashMap-keyed DM/txn pre-factor
(above). Without it, the accumulator would need separate HashMap fields plus a
`Vec` conversion at output.

## Decision: `base_version: Version` as a parameter (not stored on `Crc`)

**Problem.** `Crc` does not currently carry its own version. To build a delta
that advances `base` to `self.end_version`, we need to know `base.version`.

**Initial design.** Document the precondition; caller does
`segment_after_crc(crc_version)` first, then passes the pruned segment.

**Our solution.** Pass `base_version: Version` explicitly. The function does the
`c.version > base_version` filter internally. Returns
`Error::internal_error(...)` if `base_version >= self.end_version` (caller is
expected to short-circuit `X == N` before invoking).

**Why this is a stop-gap.** TODO comment in `crc_replay.rs:68-70` notes the
ideal end state: load the version from the `.crc` filename when constructing
`Crc`, then drop the parameter. This is a follow-up — not in scope for the
production PR. The current shape avoids the mismatch footgun via the
precondition error.

**Test coverage.** `base_version_filter_is_strictly_greater` pins the
`>` (not `>=`) boundary. `base_version_at_or_above_end_version_errors` pins the
precondition error.

## Decision: compaction files skipped (TODO)

`segment_after_crc` returns both `ascending_commit_files` and
`ascending_compaction_files`. The reverse replay iterates **only** the plain
commit files. Compaction files are intentionally skipped because:

1. They pack multiple commits' actions in **ascending** order within one file.
   This breaks first-seen-in-reverse semantics for per-key fields (P/M/DM/txn).
2. The per-commit boundary mechanism (URL transitions) would treat one
   compaction file as one commit, conflating many commitInfos.

Plain commit files exist for every version, so skipping compactions is a
**perf optimization loss**, not a correctness loss. A follow-up could iterate a
compaction's actions backwards-by-commit using commitInfo rows to demarcate
commit boundaries within the file.

## Decision: `crc_replay_schema()` projects `_file` metadata column

The schema includes seven action types plus `_file`:

```rust
fn crc_replay_schema() -> DeltaResult<SchemaRef> {
    let projected = get_commit_schema().project_as_struct(&[
        ADD_NAME, REMOVE_NAME, PROTOCOL_NAME, METADATA_NAME,
        SET_TRANSACTION_NAME, DOMAIN_METADATA_NAME, COMMIT_INFO_NAME,
    ])?;
    let with_file = projected.add_metadata_column("_file", MetadataColumnSpec::FilePath)?;
    Ok(Arc::new(with_file))
}
```

The pattern is verified by `kernel/src/engine/tests.rs::test_json_handler_file_path_contract`
(contract-tested for both `DefaultJsonHandler` and `SyncJsonHandler`).

## Decision: column-index constants in visitor

Visitor's 12 columns are indexed by `const COL_FILE: usize = 0; const COL_OP: usize = 1; ...`
rather than raw `getters[0]` / `getters[11]`. Maintenance: when columns get added or
reordered, the call sites change to match the constants.

The schema column order is: `_file`, `commitInfo.{operation, inCommitTimestamp}`,
`add.size`, `remove.{path, size}`, `domainMetadata.{domain, configuration, removed}`,
`txn.{appId, version, lastUpdated}`. CommitInfo comes right after `_file` so the
"is this a commitInfo row?" check is early in the visitor body.

## What we did NOT change (worth flagging)

- **`FileStatsDelta::is_incremental_safe` safelist.** Still a hardcoded list of
  operation strings (`WRITE`, `MERGE`, `UPDATE`, …). The spec defines an explicit
  `commitInfo.isIncrementalSafe: bool` field that we could consume instead.
  Tables written by Delta-Spark using `"INSERT"` will be force-`Indeterminate` by
  the kernel because `INSERT` is not in our safelist. **Follow-up PR**: consume
  `isIncrementalSafe` when present, fall back to the safelist when absent.
- **DV histogram fields** (`numDeletedRecordsOpt`, `numDeletionVectorsOpt`,
  `deletedRecordCountsHistogramOpt`). Optional CRC fields per spec; not modeled
  in `CrcDelta`. If the kernel ever tracks them, the reverse-replay accumulator
  will need DV-aware updates.
- **Test coverage for multi-batch-per-commit.** Direct accumulator unit tests
  exercise this (`accumulator_per_commit_invariant_across_files_and_batches`
  rstest) since the SyncEngine always emits one batch per file. End-to-end
  multi-batch coverage will come when an engine path allows forcing batch size.

## Production PR split (for future-you)

1. **Pre-factor PR**: CrcDelta shape change + forward producer boundary
   conversion + apply iteration update + test fixture updates.
   See "Pre-factor PR" section above for the file list.
2. **Production PR (this one's content minus the pre-factor)**:
   - `kernel/src/log_segment/crc_replay.rs` (new module, ~840 lines incl. tests)
   - `kernel/src/log_segment/mod.rs` (one-line `mod crc_replay;`)
   - `kernel/src/crc/writer.rs` ICT/protocol-consistency check + tests
   - This file
3. **Follow-up PRs** (separate, not blocking):
   - Snapshot wiring (PR5 of the original plan).
   - Consume `commitInfo.isIncrementalSafe`.
   - Store `version` on `Crc`; drop `base_version` parameter.
   - Compaction-file support in reverse replay.
   - DV histogram tracking when the kernel adds it.

## Plan vs reality

The original plan at `~/claude_plans/2026_05_13_crc_mvp_prime_plan.md` § PR4
predicted ~700 lines of code. Actual: ~840 lines of code in `crc_replay.rs` plus
the pre-factor changes (which the plan did not anticipate). The user's review
comments drove most of the design pivots above. Notably:

- Per-commit boundaries via `_file` (vs the prototype's per-batch heuristic) was
  not in the plan.
- HashMap-shaped `CrcDelta` (vs Vec) was not in the plan.
- Write-path validation of ICT/protocol consistency was not in the plan.

These pivots are durable correctness wins; record them as such.
