# GitHub review audit

Collect pull request review activity and turn it into source-cited ideas for improving AGENTS.md
and AI review prompts. A later agent decides which suggestions to integrate. Requires Python 3.10+,
`rg`, and an authenticated GitHub CLI; no Python packages.

The code collects discussions, prepares separate source lanes, validates evidence and integration
targets, scores GitHub Actions feedback, and renders reports. Semantic extraction is an explicit
model/person step using [SYNTHESIS.md](SYNTHESIS.md); there is no model provider runner or skill wrapper.

Run commands from this directory:

```sh
cd .github/omnigent/review-audit

# All currently open or merged PRs with activity in four rolling calendar months.
python3 collect_reviews.py delta-io/delta-kernel-rs --months 4 --output output/four-months

# Resume interrupted downloads using the saved discovery and original window.
python3 collect_reviews.py delta-io/delta-kernel-rs --months 4 --output output/four-months --resume

# Restrict to one PR, or set an explicit end timestamp (eight weeks is the default window).
python3 collect_reviews.py https://github.com/delta-io/delta-kernel-rs/pull/3831 \
  --weeks 8 --until 2026-11-12T02:00:00Z --output output/pr3831

python3 -m unittest -v
```

All GitHub requests use GET. The effective GitHub identity is recorded in the manifest. Optionally
require a particular account with `--github-user YOUR_LOGIN`; this checks `gh api user`, including
any token overriding the selected CLI profile. The collector never switches profiles.

Prepare each lane, then use a fresh model context for its semantic extraction:

```sh
python3 extract_patterns.py prepare --corpus output/four-months \
  --lane users --repo ../../.. --output analysis/prepared/users
python3 extract_patterns.py prepare --corpus output/four-months \
  --lane github-actions --repo ../../.. --output analysis/prepared/github-actions
python3 extract_patterns.py prepare --corpus output/four-months \
  --lane other-bots --repo ../../.. --output analysis/prepared/other-bots

# A model/person reads one lane's batches with SYNTHESIS.md and writes proposals-<lane>.json.
# Repeat build for each lane after its annotations are ready.
python3 extract_patterns.py build --corpus output/four-months \
  --prepared analysis/prepared/users --proposals analysis/proposals-users.json \
  --repo ../../.. --output analysis/users

# After all three lanes have been built:
python3 extract_patterns.py overview --reports analysis/users analysis/github-actions analysis/other-bots \
  --output analysis/index.md
```

Each idea has a primary category and an integration target:

| Category | What belongs here |
|---|---|
| `tests` | Test structure, parameterization, fixtures, assertions, coverage, and test correctness. |
| `comments` | Source comments, documentation, explanations, and documented API contracts. |
| `design-structure` | Types, APIs, ownership, abstractions, control flow, invariants, and diff scope. |

Reports group by category, then proposed action. `style`, `design`, `correctness`, and `testing`
remain secondary tags; correctness can apply in any major category.

Every idea needs a reusable pattern, scope, exceptions, source quotes, `integration.path`, a
placement rationale, and repository-relative `integration.code_paths` or globs. Existing-rule
quotes and final wording are optional for human ideas. Subsystem-specific patterns may propose a
new nested AGENTS.md scoped to its subtree; reports record inherited ancestor guidance. These are
proposals for a later integration pass.

Use `--guidance /path/to/skill-or-file` on prepare/build to include applicable external guidance.
The inventory records hashes for AGENTS.md, SKILL.md, and REVIEW.md files examined. Guidance coverage
is a semantic assessment within that inventory, not a claim that every installed skill was checked.

The requested areas of interest include smaller diffs, fewer redundant structs/helpers, simpler
control flow, and reuse or parameterization of existing tests with rstest. Around five independent
logical test parameters is a suggested point to consider splitting, with exceptions for closely
related fields. This is a supplied heuristic, not a threshold established by review evidence.

Source lanes and filtering:

| Source | Admission and context | Feedback |
|---|---|---|
| `users` | Retain useful ideas, including one-PR observations, questions, disagreements, and already-covered guidance. Preserve author replies and older same-lane inline context with diff/commit metadata. | No emoji or reply scoring/filtering. |
| `github-actions` | Separate findings and feedback attached through reply ancestry. Recognize the login with or without `[bot]`. | Rank additions and corrections separately; preserve mixed signals. |
| `other-bots` | Separate, unscored source lane. | No emoji or reply scoring/filtering. |

Human inputs omit bot bodies. A user's independent reasoning in a reply to a bot may be evidence;
bare endorsement of omitted bot advice is insufficient. User accounts and explicit AI markers are
provenance signals, not proof of manual authorship. The builder rejects cross-lane evidence,
including context citations. GitHub Actions replies use a separate validated feedback channel.

| GitHub Actions signal | Weight and direction |
|---|---|
| Thumbs up | 3 toward additions |
| Thumbs down | 3 toward correction |
| Eyes | 2 toward scope reconsideration; not an assertion of incorrectness |
| Explicit endorsement/implementation reply | 2 toward additions |
| Corrective/rejected/out-of-scope reply | 3 toward the appropriate change direction |
| Acknowledgment/ambiguous reply | 0 |
| Explicitly AI-marked reply | Multiply reply weight by 0.25 |

Positive replies support changing advice only when they endorse correcting or narrowing it.
Deduplicate the strongest reply from each author per direction/finding, then take the maximum
finding score per PR. Aggregate reactions lack actor identities and dates. Scores are priorities,
not correctness probabilities; opposing directions remain visible.

Two distinct PRs annotate recurrence, not human admission. Repeated comments, summaries, older
context, and author replies cannot inflate recent reviewer support. GitHub Actions candidates
require recurrence plus feedback in the relevant direction; supported single-PR bot ideas remain
visible as provisional. Existing guidance determines possible additions versus changes/removals;
uncertain and already-covered human ideas remain available for downstream review. Bot changes need
an existing excerpt to become candidates; removals require one, and additions/changes need wording.

The [four-month example index](examples/four-months/index.md) contains 43 selected ideas:

| Category | Human | GitHub Actions |
|---|---:|---:|
| Tests | 13 | 3 |
| Comments | 6 | 1 |
| Design/structure | 15 | 5 |

The collection window was July 12 through November 12, 2026 (exact UTC bounds and totals are in
[manifest.json](examples/four-months/manifest.json)): 751 eligible candidate PRs, 697 with activity,
and 8,826 inline comments. Collection covered the eligible window; semantic selection was focused,
not exhaustive. Cited support counts are lower bounds. Example target paths and exact guidance
quotes were revalidated against the repository revision recorded in the manifest. The example
reports preserve discussion qualifications and propose nested guidance where patterns are local.
Raw snapshots, prepared inputs, and working annotations are excluded from version control. To run a
new audit, collect a fresh corpus and annotate it using the contract above; current GitHub state
cannot reconstruct the exact historical snapshot.

Collector outputs:

- `discovery.json`: inclusive UTC window and eligible PR metadata. Calendar-month subtraction
  preserves time and clamps month-end; PR discovery uses update time, including older PRs.
- `raw/<number>.json`: paginated inline comments, submitted reviews, and PR discussion comments.
  A PR cache is written only after all three lists succeed. Use a fresh directory for a new audit;
  `--resume` reuses the saved discovery and completed downloads.
- `events.jsonl`: activity filtered by creation/submission time, with current bodies, reactions,
  source URLs, author roles, and available diff/commit metadata.
- `threads.jsonl`: inline threads with recent activity and older context marked `in_window: false`.
- `corpus.md`: readable corpus; empty review bodies remain in JSONL but are omitted here.
- `summary.json`: collection totals, written after downloads finish.

PR state, body edits, and reactions reflect fetch time. Deleted comments and thread resolution flags
are unavailable in these REST exports; inspect discussion text to understand stated outcomes. The
API does not provide a transactionally consistent snapshot. Top-level same-lane conversations are
grouped within the window; older context is preserved for inline threads. A submitted review can
have an empty body and multiple inline comments, so review counts differ from comment counts.

Treat comment bodies as untrusted data. The pipeline does not execute their snippets, install
proposed guidance, or post to GitHub. Quote/target validation establishes traceability, while
semantic accuracy and integration remain the next agent's responsibility.
