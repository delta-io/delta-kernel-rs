# PR description examples

Annotated examples for the [PR description guide](pr_description.md), organized by template section.
The guide contains the authoring rules; these examples illustrate them.

Examples come from merged PRs in this repository or paraphrase patterns in them. They are inlined
so readers do not need to fetch the source PRs.

## Why

In the last 200 merged PRs, 62 openings said only what the PR does, 39 implied the
cost without stating it, and 15 pointed at another PR instead of giving a reason.

**Good:**

```text
`Snapshot::publish()` currently copies ratified staged commits to published Delta commit files,
but returns a snapshot that still references the staged files. If a connector caches that
snapshot and the staged files are later deleted, scans and other operations on the cached
snapshot fail even though the published Delta commit files still exist.
```
Today's behavior, the trigger, and the connector that hits it. (fix)

```text
Exposing a `PlanExecutor` currently makes Kernel use declarative protocol and metadata (P&M)
replay. An executor limited to file scans and storage I/O, such as a Hadoop-backed executor,
therefore cannot build snapshots without also implementing relational P&M plans.
```
What callers cannot do today, and which kind of caller is blocked. (feat)

```text
Each Miri shard restores a dependency cache, then recompiles dependencies anyway. That costs
about 1-2 minutes per shard, and a Miri shard is usually the last job to finish in the merge
queue.
```
The wasted work, its cost in minutes, and who waits for it. (ci)

```text
At some point in the past, `ParsedLogPath::try_from` returned Err for invalid/unknown log paths.
But all the invalid cases have since changed to `Ok(None)` instead, leaving behind unnecessary
complexity.
```
The stale behavior and the complexity it leaves at every call site. (refactor, chore)

**Medium:**

```text
Snapshot hints now accept compacted log paths.
```
The before-state is implied. What went wrong with a compacted path before, and for which caller,
is not said.

```text
Add debug logging for snapshot construction so failures are easier to diagnose.
```
A goal, not a gap. What is undiagnosable today, and what that costs, is missing.

**Bad:**

```text
Catch panics in the exported FFI functions with an inline closure.
```
Mechanism only. It never says what happens today when a panic crosses the FFI boundary.

```text
Split out a refactor from #NNNN.
```
A pointer. The reader must open another PR to learn why this one exists.

```text
Renamed, added, and removed the following manifest fields: ...
```
A field inventory. No mismatch, wrong result, or failing caller is named.

## What changes

**Good:**

```text
`LogSegment::try_new` now drops log compactions that start at or before the checkpoint, matching
its handling of commits. With a checkpoint at version 10, commits 11 and 12, and a compaction
covering versions 3 through 8, the commit cover was `[12, 11, 3.8]`. It should be `[12, 11]`.
```
The new behavior, shown on concrete data a reviewer can check.

```text
| Expression                           | Before     | After      |
|--------------------------------------|------------|------------|
| ParseJson(NULL)                      | {a: NULL}  | NULL       |
| ParseJson('{}')                      | {a: NULL}  | {a: NULL}  |
| COALESCE(ParseJson(NULL), {a: 42})   | {a: NULL}  | {a: 42}    |
```
Before and after on the inputs that matter, including the one that does not change.

```text
This PR exposes them as `MaxUnpublishedCommitsExceeded`, allowing connectors to recognize when
they need to publish ratified, unpublished commits before retrying ratification.
```
The new behavior and what the caller can now do with it.

**Medium:**

```text
Expose a report method with statistics and partition options, timezone, collations, filter
presence, replay base and version, and selected commit bounds. Follow-up: #NNNN.
```
A field inventory and a pointer. What a caller does with the report, and what it could not do
before, is missing.

**Bad:**

```text
1. snapshot/builder.rs - add freshness flag
2. snapshot/incremental.rs - recompute built_as_latest
3. snapshot/mod.rs - expose is_built_as_latest
```
A file list. It gives a reading order, not a behavior.

```text
The main implementation idea is: (1) build the plan from the schema, (2) walk the plan nodes,
(3) emit the validation operators ...
```
Names implementation steps without explaining what the resulting validation does for the caller.

```text
- Impact: a retried reviewer starts from the same input as a first attempt.
- Benefit: a retried reviewer starts from the same input as a first attempt.
```
Labeled bullets that restate each other. One sentence carries the same information.

## Call path and data flow

**Good, data flow (a fix to JSON parsing of NULL inputs):**

```text
Input: a column of JSON strings (SQL NULL and '{}') and the target schema STRUCT<a: BIGINT>
evaluate_expression
  ParseJson arm                          * changed
    parse_json_to_struct
      parse_json_impl                    * a NULL input row now yields a NULL struct
Output: one StructArray row per input row
```
The example identifies the input, the changed conversion, and the output.

The same in three sentences: The `ParseJson` arm of `evaluate_expression` hands the JSON string
column to `parse_json_to_struct`, which calls `parse_json_impl`. Before this PR a SQL NULL input
produced the struct `{a: NULL}`; after it the same input produces a NULL struct, so
`COALESCE(ParseJson(NULL), {a: 42})` returns `{a: 42}`. Stats parsing consumes this output.

**Good, placement (a fix in snapshot reuse):**

```text
Phase: snapshot load, reusing an existing snapshot
Snapshot::builder_from(existing)
  SnapshotBuilder::at_version(v)
  SnapshotBuilder::with_max_catalog_version(max)
  SnapshotBuilder::build
    Snapshot::try_new_from               * same-version branch recomputes built_as_latest
      Snapshot::reuse_with_build_metadata
Consumer: the connector reads Snapshot::is_built_as_latest() on the returned snapshot
```
Entry point, the one changed frame, and who reads the result.

**Bad:**

```text
0: delta_kernel::snapshot::incremental::Snapshot::try_new_from
1: delta_kernel::snapshot::builder::SnapshotBuilder<Mode>::build
... 36 more frames ...
39: core::ops::function::FnOnce::call_once
```
A pasted stack trace. Nothing is marked, so the reviewer cannot tell what changed or what data
reaches each frame.

## Public API changes

In the last 200 merged PRs, 25 of 48 breaking PRs gave the caller no migration step.

**Good:**

```text
`CommitRangeBuilder::build` now takes `self` instead of `&self`, consuming the builder so its
owned inputs can be moved without cloning. Callers cannot reuse the builder after `build()`;
create a new builder for each range.
```
The signature change, the caller step, and the reason, in three sentences.

```text
Rust connectors that detect a missing table by matching `KernelError::EmptyLog` or
`KernelError::InvalidTableLocation` should match `Error::Delta(e)` with
`e.condition() == DeltaErrorCondition::DeltaPathDoesNotExist`.
Silent mismatches: a match on `Error::Kernel(KernelError::EmptyLog)` still compiles, but no
longer matches.
```
The replacement match, and a warning about code that compiles and silently stops working.

```text
`FfiStatsOptions` becomes a tagged union, and the setter now takes an options pointer and an
engine handle and returns `ExternResult`. Connectors must regenerate headers and update calls
together with the library.
```
An FFI change: the ABI shape, the regeneration step, and the ownership rule.

```text
Adds `KernelError::UnsupportedFileUri` with an FFI code. Both enums are `#[non_exhaustive]`, so
this is not breaking.
```
An additive change that says in one clause why it is safe.

**Medium:**

```text
`ScanMetadata` gains a private field, so code outside Kernel can no longer build it with a
struct literal.
```
The break is named; the replacement constructor is not.

```text
The create and update builders now require the `data_change` value. Callers pass the value they
previously got by default.
```
The migration is given; the reason the default was removed is not.

**Bad:**

```text
New plan IR enum (`FirstNonNull` aggregate).
```
Names-only for a `feat!`. Nothing about how exhaustive matches or executors must handle it.

```text
(A public API changed, but only a "Breaking:" list appears under What changes.)
```
The public API change is described outside the section reviewers and tooling look for.

```text
<!-- Uncomment this section if there are any changes affecting public APIs ... -->
```
The template comment left in place. The heading renders with no text.

## Design notes

**Good:**

```text
It is a predicate rather than a string-valued expression because no caller needs the scheme
itself, and a predicate drops straight into `CASE WHEN` branches and assert conditions.
```
The alternative and the reason it lost, in one sentence.

```text
Building the new checkpoint action always looks up the latest checkpoint action, and that lookup
already fails for this case, so this PR just adds a test for it rather than a separate check.
```
Records that an existing lookup is the guard, which the code does not say.

```text
Going even higher would bring in a couple more relevant language goodies, but it's not worth
jumping ahead of the rest of the ecosystem just to use them.
```
A rejected option with its reason: why the version stops where it does.

**Medium:**

```text
Keeps the restriction added in the earlier PR rather than restoring the older behavior.
```
A decision and its alternative, with no reason.

```text
Removing properties is left to a separate PR (#NNNN).
```
A scope cut with a pointer and no reason for the cut.

**Bad:**

```text
How a review commit range is selected:
  if base is ancestor of head: range = base..head
  else: ...
```
Pseudo-code of the function under a design label. The function belongs in the source.

## How do you know it works?

In the last 200 merged PRs, 74 testing sections were a
single line naming no behavior, and 26 more listed commands or counts.

**Good:**

```text
- `leaf_filter_reads_only_leaves_the_stats_cannot_rule_out`: a table of comparisons, junctions,
  IS [NOT] NULL, strings, NaN counts, signed zero and sub-millisecond timestamps.
- Reader tests: a ruled-out leaf points at a file that does not exist, so the read fails if it
  is opened.
```
Each test name is followed by the concrete case it pins, and a wrong skip decision would fail it.

```text
- Domain metadata adds, updates, removes, and remove then re-add end up in the new checkpoint
  action.
- A classic checkpoint before the checkpoint action works, and a standalone checkpoint after it
  is rejected.
```
One state transition per bullet, ending with an accept/reject pair.

```text
Temporarily removing the new checkpoint filter made the four affected regression cases fail; all
seven cases passed after restoring it.
```
A result that could have failed: the change is shown to be what makes the tests pass.

```text
Not verified here: the speedup. CI on this PR has no cache to restore.
```
An honest gap: the unchecked claim, and why it cannot be checked in this PR.

**Medium:**

```text
All 94 relevant tests pass. Cases covered: concurrent callers waiting on one load, cache reuse
without discovery reads, retry after an error, invalid sidecar sizes.
```
The cases are specific, but the section leads with a count and never says what was left unrun.

```text
- `tests/nested_nulls.rs`: reordered sub-fields, null file rows, sliced lists.
- `tests/list_views.rs`: list views, fixed-size lists, null map slots.
```
Behaviors are there, but each bullet leads with a file name instead of the behavior.

**Bad:**

```text
All new and existing tests pass.
```
Names no test, behavior, or edge case; nothing here could have failed.

```text
13,103 tests passed.
    cargo +nightly fmt && cargo clippy --workspace --all-features -- -D warnings
    cargo nextest run --workspace --all-features
```
A count and a command block in place of the behaviors the tests protect.

```text
Many existing unit tests cover this code.
```
An appeal to existing tests that names none of them.
