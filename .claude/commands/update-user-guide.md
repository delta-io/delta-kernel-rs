---
description: Validate the user guide against delta-kernel-rs (forward user-guide-to-kernel + backward kernel-commits-to-user-guide), categorize findings, then apply approved updates. Records the kernel SHA last validated against in LAST_VALIDATED.md.
---

# /update-user-guide

You are auditing the delta-kernel-rs user guide against the current state of
the kernel codebase, then applying the resulting updates. Run in three phases:

1. **Collect** (read-only) — forward validation (user guide → kernel) per
   page + backward validation (kernel commits → user guide) over kernel
   commits since the last run.
2. **Categorize + checkpoint** — synthesize one ranked list with stable IDs,
   show it to the user, get approval.
3. **Apply** — dispatch edit agents to make the approved changes, then do a
   quick re-validation pass on the edited pages. Update `LAST_VALIDATED.md`.

## Output discipline

Between dispatch and approval, only progress lines and the Step 2 synthesis
are user-facing. Never dump raw agent returns — even if asked to "show the
findings." The synthesis IS the findings.

## Terminology: forward vs. backward

These two terms appear throughout this command. **Always pair them with their
direction in user-facing text** — never write "forward" or "backward" alone in
prose printed to the user, because the bare words are ambiguous on their own.

- **Forward (user guide → kernel)**: walk every guide page and verify each
  kernel-touching claim against current kernel source. Catches drift the
  guide accumulated *before* the last audit.
- **Backward (kernel commits → user guide)**: walk kernel commits since the
  last audit SHA and identify which guide page(s) each user-facing change
  affects. Catches changes the guide hasn't absorbed yet.

Acceptable phrasings: "forward (user guide → kernel)", "forward
user-guide-to-kernel validation", "backward (kernel commits → user guide)",
"backward kernel-to-guide validation". Section headings inside this command
file may use the short forms because the surrounding section makes the
direction obvious.

## Step 0: Locate the repos and read state

1. `USER_GUIDE_ROOT` is the directory containing `book.toml` and
   `src/SUMMARY.md`. Verify with `ls`.
2. `KERNEL_ROOT` is the directory containing `kernel/src/lib.rs` and a
   workspace `Cargo.toml` with a `kernel` workspace member. Walk upward
   from `$USER_GUIDE_ROOT` until found. This resolves to the
   `delta-kernel-rs/` parent of `docs/user-guide/`.
3. **Detect the kernel branch.** Run `git -C $KERNEL_ROOT branch --show-current`.
   The audit targets whatever branch is checked out — `git log <LAST_SHA>..HEAD`
   in Step 1a uses HEAD automatically. Three cases to handle in the prose
   you print to the user:
   - `main` — typical case.
   - `release/X.Y.x` (regex: `^release/\d+\.\d+\.x$`, e.g. `release/0.20.x`) —
     release branch. Audit against that branch's commits, not main. This is
     the right behavior when the user guide also lives on a release branch
     and needs to track the release line.
   - Anything else — warn the user (`Currently on branch '<name>'. Not main
     and not a release/X.Y.x branch. Continue?`) and require explicit
     confirmation before proceeding.
4. Read `$USER_GUIDE_ROOT/LAST_VALIDATED.md` and extract `<LAST_SHA>` and
   `<LAST_BRANCH>`.
   - **If `LAST_VALIDATED.md` does not exist, or exists but has no parseable
     SHA, STOP and fail.** Print a clear error: "No prior validation SHA
     found. Provide a starting kernel commit SHA before re-running." Do not
     pick a default — a wrong starting SHA either silently skips real
     changes or floods the run with irrelevant commits, and either way the
     user loses confidence in the audit.
   - **If `<LAST_BRANCH>` differs from the current kernel branch**, warn:
     `Last validated against '<LAST_BRANCH>', currently on '<current>'.
     Continue?` This catches accidental context switches (e.g. user
     validated against main, then later runs from a release branch worktree
     without realizing the SHA is from a different line of history).
   - Step 4 of every successful run MUST write a fresh SHA and branch back
     to `LAST_VALIDATED.md`. The state file's whole purpose is to drift
     forward with the kernel; if a run completes without updating it, the
     next run re-audits commits that were already audited.
5. Run `git -C $KERNEL_ROOT status --short`. If non-empty, warn the user — a
   recorded SHA is meaningless if there are uncommitted changes. Ask whether
   to proceed.

## Step 1: Collect (parallel, read-only)

### 1a — Backward validation (kernel commits → user guide): cumulative diff since last run

In `KERNEL_ROOT`, get the list of files changed since the last audit:

```
git diff <LAST_SHA>..HEAD --name-only -M
```

The `-M` flag enables rename detection so a moved file shows as one change
rather than delete + add. The cumulative diff naturally collapses
revert-pairs and rename-then-use chains that confuse commit-by-commit
walkers — what matters is the net change to the public surface, not the
intermediate history.

#### Filter to user-facing paths

Apply the **include allow-list** (any matching file is in scope):

- `kernel/src/**/*.rs` and `kernel/Cargo.toml`
- `ffi/src/**/*.rs` and `ffi/Cargo.toml`
- `derive-macros/src/**/*.rs` and `derive-macros/Cargo.toml`
- `delta-kernel-unity-catalog/src/**/*.rs` and `delta-kernel-unity-catalog/Cargo.toml`
- `unity-catalog-delta-client-api/src/**/*.rs` and `unity-catalog-delta-client-api/Cargo.toml`
- `unity-catalog-delta-rest-client/src/**/*.rs` and `unity-catalog-delta-rest-client/Cargo.toml`
- Top-level workspace `Cargo.toml` (workspace-level feature flags)

Then apply the **exclude list** (overrides any include):

- `**/tests/**`, `**/benches/**`, `**/examples/**`
- `.github/**`, `docs/**`
- `Cargo.lock`, `*.lock`

The allow-list intentionally excludes test-only crates (`test-utils`,
`benchmarks`, `mem-test`, `feature-tests`, `acceptance`) — they are not
documented in the user guide. When a new user-guide-relevant crate is
added to the workspace, update this list.

#### Bucket filtered files into themes (mirroring SUMMARY.md)

Read `$USER_GUIDE_ROOT/src/SUMMARY.md` to confirm the guide's section
layout, then map each touched file to a **theme** that mirrors a top-level
guide section. Themes are NOT raw directory prefixes — cross-cutting
features (column mapping, table features, new action types, write path)
deliberately group across directories so a single agent sees the change
as a unit, not a slice.

**Themes are the top-level sections of `$USER_GUIDE_ROOT/src/SUMMARY.md`**
— Getting Started, Core Concepts, Reading Tables, Writing Tables,
Maintenance Operations, Building a Connector, Catalog-Managed Tables,
Unity Catalog Integration, Storage Configuration, Observability, FFI —
plus a **Cross-cutting** catch-all. There is no static module → theme
mapping table; the kernel grows and renames modules regularly, and a
hardcoded table would go stale silently. Each run, the orchestrator:

1. Reads `SUMMARY.md` to enumerate the current top-level sections.
2. For each touched file, picks the theme whose guide pages most
   directly cover that file's surface — using the file path, its diff
   content, and the section's pages as joint signal.
3. Routes files with no clear theme to Cross-cutting and lists them
   explicitly in the dispatch roster — that's the signal a brand-new
   kernel module may warrant its own theme treatment in a future run.

Routing rules:

- **Multi-theme files** (e.g. `kernel/src/snapshot/` covers both reads
  and catalog-managed reads): inspect the diff content as tiebreaker.
  Route to the more specific match when the diff makes it clear,
  otherwise default to the more common case (typically Reading for
  `snapshot/`, Concepts / Engine for `engine_data.rs`, etc.).
- **No-clear-theme files**: route to Cross-cutting and surface them in
  the dispatch roster.

Theme size constraints:

- Target 5-15 files per theme.
- Theme >15 files: split sub-thematically (e.g. split Reading into
  Reading-Scan and Reading-Log-Replay), using directory prefix as the
  last-resort split key.
- Theme with 1-2 files: fold into the nearest theme; Cross-cutting is
  the natural fold-in destination.
- Skip empty themes — no agent dispatched.
- Cap at 12 themes/agents total.

Spawn one Explore subagent per non-empty theme in a single message
(parallel). Each agent receives:

- Its theme's file list.
- The page paths from `src/SUMMARY.md` corresponding to its theme (so
  the `Pages:` field on findings is grounded, not guessed).
- Both repo paths.

Each agent must:
1. For each file in its theme: `git diff <LAST_SHA>..HEAD -- <file>` and
   read the cumulative diff.
2. From the diff, identify user-facing changes — new/removed/renamed
   public types, methods, traits, fields; signature changes; new feature
   flags; semantic behavior shifts visible in the change. Skip purely
   internal refactors.
3. For each user-facing change, identify which `src/**/*.md` page(s)
   from the theme's section list it likely affects. Out-of-theme pages
   need an explicit justification line.
4. Return findings in this format, **4 sentences max each**:
   ```
   - File: kernel/src/snapshot/builder.rs
     Pages: src/reading/building_a_scan.md
     Change: SnapshotBuilder::with_log_tail now takes impl IntoIterator instead of Vec<...>.
     Confidence: verified | likely | speculative
     Citation: kernel/src/snapshot/builder.rs:88
   ```

### 1b — Forward validation (user guide → kernel): every page → kernel code

List every `*.md` file under `$USER_GUIDE_ROOT/src/` (excluding `SUMMARY.md`).
Bucket by cost — long pages get their own agent, short pages batched. Scale
the number of parallel Explore agents to the page count: cap at 12, but for
a small guide (say, 4 pages) one agent per page is fine. Don't spawn empty
agents to hit a number.

Each agent receives its page list, both repo paths, and pointers to
`$KERNEL_ROOT/CLAUDE.md`, `$KERNEL_ROOT/CLAUDE/architecture.md`, and
`$USER_GUIDE_ROOT/CLAUDE.md`.

For each page:
1. Read it in full.
2. Extract every kernel-touching claim: type names, trait names, method
   signatures, module paths, feature flags, behavior descriptions, error
   conditions.
3. Verify each claim by reading the actual kernel source. Don't trust
   memory — open the file.
4. Verify code examples: identifiers exist, signatures match, imports
   resolve, referenced `listings/` files exist.
5. Verify cross-references: every relative link resolves to a real file.
6. Return findings in this format, **4 sentences max each**:
   ```
   ### src/reading/building_a_scan.md
   - Issue: ScanBuilder::with_predicate is documented as taking Expression; signature is Predicate.
     Confidence: verified
     Citation: kernel/src/scan/builder.rs:142
   - Issue: link to ../concepts/engine.md is dead — file is engine_trait.md.
     Confidence: verified
     Citation: src/reading/building_a_scan.md:34
   ```
7. **If a page has no findings, write an explicit line:**
   `Status: ok — no findings.` Silence is ambiguous; the synthesis step needs
   to know the page was actually checked.

### Progress reporting while agents run

After dispatching parallel agents in Phase 1 (and again in Phase 3 apply +
re-validation), the orchestrator MUST keep the user informed of progress
rather than going silent until every agent finishes.

Concretely:
- Immediately after dispatch, print a one-line roster: `N agents dispatched
  — backward (kernel commits → user guide) ×K, forward (user guide → kernel)
  ×M`. Naming each agent's slice (e.g. "concepts", "writing+maintenance")
  helps the user spot which area is taking longest.
- Each time an agent completion notification arrives, print one line:
  `<slice> done (N/total complete, M still running)`.
- If no completion notification has arrived for ~60 seconds, use
  `ScheduleWakeup` (delaySeconds 60) to fire a heartbeat and print
  `still waiting on: <slice list>`. This gives the user a recurring signal
  even during a long quiet stretch — without polling tightly enough to
  invalidate the prompt cache.
- Never sleep-loop or poll task output files. The completion notifications
  and ScheduleWakeup heartbeat are the only allowed cadence sources.

The intent is "the user always knows whether the audit is making progress
without having to ask" — not strict 15-second metronome ticking.

### Rules for both 1a and 1b agents

- **Confidence is required per finding.**
  - `verified`: agent read the kernel file and the issue is unambiguous. MUST cite `path:line`.
  - `likely`: signal points to an issue but agent didn't fully confirm. MUST say what would confirm it.
  - `speculative`: worth a human glance, agent isn't sure. Use sparingly.
- **Cap each agent's total output at ~500 words.** The synthesis step
  consolidates; don't pad.
- **Do not propose rewrites.** Just identify the issue. The apply phase does
  the writing.

## Step 2: Categorize + checkpoint

Synthesize all findings into one ranked list. Group by severity, dedupe
across the forward (user guide → kernel) and backward (kernel commits → user
guide) phases (same issue from both = one entry, keep the higher-confidence
citation and note both sources).

Categories, in priority order:

- **MUST UPDATE** (`M1`, `M2`, ...) — factually wrong, broken example,
  dead link, signature mismatch, claim that contradicts current kernel
  behavior. Almost always `verified`.
- **SHOULD UPDATE** (`S1`, `S2`, ...) — stale or imprecise: deprecated
  pattern, missing required context, page doesn't reflect a recent kernel
  change. Mostly `verified` or `likely`.
- **COULD UPDATE** (`C1`, `C2`, ...) — improvement opportunities: clearer
  example, missing prerequisite link, page would benefit from a section.
- **NITS** (`N1`, `N2`, ...) — terminology drift, dataset deviation from
  the canonical Alice/Bob/Carol example, minor formatting.

IDs are stable for this run — the apply phase references them.

**Print the full categorized list directly to the user — no file output.**
Include:
- A summary table (counts per category)
- All four categories inline (MUST, SHOULD, COULD, NITS), with each finding's
  ID, target page, 1-2 sentence description, confidence tag, and citation
- A "Cross-cutting themes" section for patterns affecting many pages

Then ask:

```
Apply which? You must answer explicitly — silence or empty input is NOT approval.
Options:
  yes / apply all      apply every finding
  m+s                  apply MUST + SHOULD only
  m                    apply MUST only
  no / none            skip apply phase entirely
  skip <ids>           apply all except listed (e.g. "skip C2 N1 N3")
  only <ids>           apply only listed (e.g. "only M1 M3 S2")
```

Wait for the user's answer. If the response is empty, ambiguous, or
unparseable, ask again — do not assume any default, and do not infer
approval from anything other than an explicit affirmative.

## Step 3: Apply (parallel edit agents)

For the approved IDs, group findings by target page. Spawn one
general-purpose subagent per page in parallel, batching small pages
together. Cap at 12 agents but scale to the work — if only three pages
have approved findings, three agents.

Each edit agent receives:
- The page path(s) it owns
- ONLY the findings assigned to those pages, with their IDs
- Pointer to `$USER_GUIDE_ROOT/CLAUDE.md` for writing standards
- The constraint: **apply only the listed findings; do not freelance.**
  No surrounding cleanup. No "while I'm here" edits. Stick to the IDs.

Each edit agent must:
1. For each finding ID assigned, make the targeted edit.
2. If a finding turns out to be wrong on closer read, do NOT edit — return
   that ID as `punted` with one sentence explaining why.
3. Return a per-page summary: `Applied: M1, S2. Punted: C3 (reason).`

After all edit agents complete, run a **post-apply re-validation** — spawn
2-4 Explore agents over the edited pages only, checking for:
- New broken links
- Undefined identifiers introduced by edits
- Examples that no longer reference real kernel APIs
- **Internal consistency within each edited page.** This is the most common
  regression from targeted edits: an agent renames a method in one
  paragraph, but two paragraphs later the old name is still there. Every
  occurrence on the page of any identifier that was changed must agree.
  Same goes for terminology shifts and type renames.

Report any regressions back to the user; don't auto-fix.

## Step 4: Update state

Overwrite `$USER_GUIDE_ROOT/LAST_VALIDATED.md` with this exact content:

```markdown
<!-- State file for the /update-user-guide slash command. Records the kernel
SHA last audited so the next run only re-checks newer commits. -->

- Last validated: <ISO date, e.g. 2026-04-27>
- Last kernel SHA: <full 40-char SHA from KERNEL_ROOT HEAD>
- Last kernel branch: <branch name, e.g. main or release/0.20.x>
```

That is the entire file. The HTML comment is the only header. No history
table, no extra preamble. Its only job is to remember the SHA and branch so
the next run knows where to start.

Print to the user a one-line summary of the run (counts per category,
applied vs. total) but don't persist it. Don't auto-commit. Leave the diff
on the user guide pages for the user to review.

## Constraints

- **Phases 1 collection and post-apply re-validation are read-only.** Use
  Explore subagents (which can't edit). Spawn parallel agents in a single
  message with multiple Agent calls.
- **Phase 3 apply uses general-purpose subagents** because Explore can't
  edit. Each agent's scope is its assigned pages and IDs only.
- **No auto-commits.** Leave the diff for human review.
- **No path hard-coding** beyond Step 0's auto-detection.
- **Files written by this command:** only `LAST_VALIDATED.md` (state) and the user
  guide `.md` pages edited in the apply phase. No scratch files, no report
  files, no per-agent output files. Sub-agent findings and summaries return
  via the Agent tool result and live in the orchestrator's context — that
  is sufficient for synthesis without intermediate storage.
- **One finding = 4 sentences max.** Long findings hide the actual issue.
- **No findings = explicit "ok" line per page**, never silence.
- **Dedupe in synthesis.** If forward (user guide → kernel) and backward
  (kernel commits → user guide) agents flag the same underlying kernel
  change, that's one entry, not two.
- **Always pair "forward" and "backward" with their direction in
  user-facing prose.** See the "Terminology" section near the top.
