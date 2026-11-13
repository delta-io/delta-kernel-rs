# AI reviewer prompts

Each reviewer bundle uses its adjacent `REVIEW.md` as the executable Omnigent prompt through
`instructions: REVIEW.md` in `config.yaml`. Changes to those Markdown files therefore change the
CI review behavior.

To run a reviewer locally, load its `config.yaml` bundle and provide the PR metadata and diff as
review context.

Each published inline finding includes feedback instructions: react 👍 for helpful or 👎 for
unhelpful or incorrect, or 👀 for out of scope. Feedback remains
on the comment for manual review and prompt tuning; the workflow does not automatically consume
these ratings or learn from them. The footer is excluded from exact finding deduplication.

Repeat reviews use changes since the newest trusted, workflow-stamped GitHub review with matching commit metadata when its base is
unchanged and its head is an ancestor of the current head. A changed base, rewritten history,
unavailable comparison, or update spanning more than 20 files or 200 changed lines triggers a full review. Older
reviews without revision metadata also trigger a full review. The reviewer still checks the
current PR description and affected surrounding code. The complete PR diff remains the source
for inline publication; repeat-review deletions that no longer map stay in the review body.

The hidden revision marker is added after output validation only when the code diff was not
truncated. Text comparison ignores it, but duplicate suppression also checks the reviewed revision. Missing history expands review coverage rather than suppressing findings.

Use `/review full inline` (or any other output mode), or select `full_review` on workflow dispatch,
for a redesign requested during review. Public Rust API/impl changes, dependency and wire-format
changes, file moves/deletions, and substantial churn also force a full review. These conservative
heuristics cannot recognize every architectural change. Small fixes remain incremental.

Every child receives a bounded complete PR diff specifically for title/description verification,
even when code review is incremental or the head is unchanged. Identical clean prose is published
once per new revision to record review progress; repeats at the same revision are still suppressed.

Issue comments, truncated reviews, and rejected inline publications cannot establish a baseline.
A merge commit or incomplete comparison commit list also forces a full review. If inline publication
fails, the collapsed fallback retains the complete findings and omits the revision marker.
The declaration checks are conservative syntax heuristics, including multiline parameter/field and
return-type edits; use the full-review override for redesigns they cannot recognize. The complete
PR diff supplied for description checks is capped at 30,000 characters with a truncation notice.
