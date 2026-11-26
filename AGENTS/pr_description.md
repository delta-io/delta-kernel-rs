# PR description guide

How to write a pull request description for this repository. Read it before opening a PR or
generating a description.

Prepare human and AI reviewers before they open the diff: explain why the change is needed, how
the affected components and data fit together, and which design decisions matter to the review.
AI tools may draft this context under the [AI Policy](../AI_POLICY.md); the author reviews and owns
the result.

## How to read this file

The file has two parts. Part 1, "About descriptions", is about the file and about descriptions in
general: who reads them, what belongs in one, format rules, and the recipe agents follow. Part 2,
"The template, section by section", walks `.github/PULL_REQUEST_TEMPLATE.md` top to bottom. Every
heading in Part 2 quotes a template heading verbatim in backticks, for example "Section `## Why`",
and gives the rule for that section, then annotated good and bad examples, with medium examples
where useful.

All examples are text from merged PRs in this repository or paraphrases of patterns in them. They
are inlined on purpose: read them here, do not go looking for the source PRs.

## Part 1: About descriptions

### Who reads a description

Three readers ask the same questions: why does this exist, what changes in behavior, where do I
start, what was verified, what is out of scope.

- The human reviewers. Two approvals are required, and reviewers cross-check the description
  against the code.
- The [CI AI reviewer](../.github/omnigent/reviewer/README.md#pr-description-input).
  It checks claims in the title and description against the diff and flags a description that
  misstates the change.
- `git log`. Merges are squash-only and the PR body becomes the commit message. Anything in the
  body at merge time is permanent, including stack banners and leftover template comments.

Write for a reader who has not seen the issue, has no hallway context, and will read the body
months from now.

### What reviewers need before reading the diff

Give reviewers context they would otherwise have to reconstruct from the source and discussion.
Explain the problem and its cost, the behavior after the change, how data moves through the
affected components, the reasons for material design decisions, and what was verified. For a
stacked PR, explain which parent it builds on and what this PR contributes.

Use the diff to verify the description. An overview can connect behavior across several files or
explain unchanged callers and consumers that matter to the change. Keep it focused on the
relationships a reviewer needs to understand; avoid a file inventory or a line-by-line walkthrough.

### Length and format

- Put the motivation and key behavior first. Keep the description concise, with enough context for
  reviewers to assess the change.
- ASCII only; the [body validator](../.github/workflows/pr-body-validator.yml) rejects other
  characters.
- One line per paragraph and per list item. GitHub renders a single newline as a line break.
- Delete template comments you did not use. They are sent to the CI reviewer and land in the
  commit message.
- Keep the description current as review changes the PR.

### Stacked PRs

Each PR in a stack gets its own description that stands alone: its own Why, the changes relative to
the parent, and what is deliberately left to later PRs. Keep the stack banner that the stacking tool
writes at the top, and do not duplicate it. Say "Stacked on #N" in the first line of the body.

### Recipe for agents

Gather these inputs before writing anything, in this order:

1. The linked issue or the author's stated reason. This is the only source for Why. If neither
   exists, write `TODO: why this PR exists` under Why and stop; never invent a reason.
2. Decisions made while implementing: alternatives rejected and why. These are the candidate
   Design notes; the author keeps or deletes each one.
3. Relevant tests and recorded validation results from the implementation session or CI, including
   tests that were not changed. Identify the behaviors covered, what actually ran, the results,
   and material failures or gaps. Report added coverage separately from executed validation; if
   execution evidence is unavailable, say so.
4. The `cargo semver-checks` result and the `breaking-change` label, to fill or demand the public
   API section.
5. The changed functions, their callers, and the data at the changed point, to draft the call
   path and data flow for the author to verify.

The author owns the opening sentences and the confidence claims. Draft them, and do not post them
without the author's review.

Pre-submit checklist:

Check each section against [the section guidance](#part-2-the-template-section-by-section).

- Required headings present: `## Why`, `## What changes`, and `## How do you know it works?`.
- Optional sections deleted when empty.
- Public API section present for public API changes, including behavior changes.
- Breaking changes called out clearly, with impact explained and `!` in the title.
- Every test name in the testing section exists in the diff or the repository.
- Follow [Length and format](#length-and-format).
- Why states what happens today and what it costs; What changes states the behavior after.
- Testing distinguishes coverage added from validation run, and states gaps.

## Part 2: The template, section by section

### Section `## Why`

In one to three sentences, say what happens today or what callers cannot do, what that costs or
gets wrong, and who is affected. Link the issue or the protocol section. The form depends on the
kind of change:

| Kind | Why must say |
|---|---|
| fix | what the code returns or does today, the wrong result, and which callers get it |
| feat | what callers cannot do today, and what they do instead (or fail) |
| refactor | what the current shape costs or blocks, and who pays (copies, parameters, churn) |
| docs | what readers or agents get wrong or cannot find, and the cost of that |
| ci | which job fails silently or wastes how many minutes, and who waits |
| perf, memory | the operation, its measured cost (or a labeled estimate), and who sees it |
| test | which behavior is unchecked, and which regression would pass CI |
| chore | what keeping the old state costs (churn, risk, exposure), and who pays |

Not: a restatement of the title, a pointer to another PR ("split out of #N"), or a description of
the mechanism. In the last 200 merged PRs, 62 openings said only what the PR does, 39 implied the
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

### Section `## What changes`

Say what X does after this PR and what the benefit is. Describe the behavior and data flow a caller
or reader would notice, leading with the user-visible or API-visible change. Mention every material
change: a behavior change, a flag, a removed capability, a new path. For a PR that fixes several
independent problems, write one short block per problem: the problem, its cause, the fix.

Not: a file list, a walk through the diff, or a line-by-line explanation; the diff and rustdoc do
that.

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

### Section `### Call path and data flow`

Optional. Include this section when a reviewer would otherwise need to reconstruct how the change
fits the existing flow. Show where the change is reached from, what data enters and leaves at that
point, and which component or phase owns the data. Include unchanged callers and consumers when
they explain the behavior or constraints of the change.

Choose the view that makes the change easiest to check. A before/after example shows how a log
cover, action, or null mask changes. A short call tree or sequence diagram shows how components
interact. Prose can explain either. Mark what changed and keep enough surrounding context to make
the path understandable; avoid pasted function bodies and file inventories.

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

### Section `### This PR affects the following public APIs`

Optional. Keep this heading text exactly; tooling looks for it. Delete the section if the PR
changes no public API. For each change give three things: the exact type, method, or C symbol that
changes; what callers do instead (replace a match, pass the new argument, regenerate bindings); and
why the break is worth it. If old code still compiles but silently stops matching, say so. For an
additive API, say in one clause why it is not breaking. A behavior change to an existing public API
belongs here even without a signature change. Other breaking behavior may be disclosed elsewhere
in the description. A breaking change needs `!` in the title; CI adds the `breaking-change` label
from `cargo semver-checks`.

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

### Section `### Design notes`

Optional. Include this section when a design decision, constraint, invariant, or scope choice helps
a reviewer evaluate the approach. Explain why the approach fits the problem and why relevant
alternatives were rejected. Explain important invariants even when the code enforces them if the
reason for the invariant is unclear. Scale the detail to the decision; omit the section when there
is no material rationale to add, and do not invent alternatives to fill it.

Forms that work:

- A decision: "Chose X over Y because Z." Use concrete names.
- Rejected alternatives that were considered: a "Rejected:" list, each item with its reason.
- An invariant: "X holds because Y." Lasting contracts go in rustdoc; keep here only what the
  reviewer of this change needs.
- A scope cut: "Not included: X (#N), because Z."

Keep the explanation focused on reasons and constraints. Implementation steps without their
rationale and Impact/Benefit bullets that restate each other do not explain a design decision.

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

### Section `## How do you know it works?`

Say which behaviors and edge cases you verified and what the results establish. Distinguish
coverage added from validation actually run, and mention material failures or gaps. When naming a
specific test or assertion, link to its source so reviewers can inspect the coverage. Include
commands or reproduction steps when they help reproduce unusual validation or a relevant result.
For each sentence ask: would this failure mean the change is wrong? If not, cut it.

Not: routine check inventories, passing-test totals, routine command lists, lint, CI status, or
"all tests pass". "Existing tests" is acceptable only for a behavior-preserving refactor, and then
name the tests that cover the changed path. In the last 200 merged PRs, 74 testing sections were a
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
