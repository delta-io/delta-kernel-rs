# AI reviewer prompts

Each reviewer bundle uses its adjacent `REVIEW.md` as the executable Omnigent prompt through
`instructions: REVIEW.md` in `config.yaml`. Changes to those Markdown files therefore change the
CI review behavior.

To run a reviewer locally, load its `config.yaml` bundle and provide the PR metadata and diff as
review context.

The workflow reviews each PR's own changes in a git stack, even when every PR targets `main`.
It resolves the parent from the current PR's comparison link in the `Stacked PR` description,
or from the immediate parent PR when no pinned link is present, and verifies ancestry before
fetching the diff. A merged parent's original head remains the boundary while the child still
inherits it, including after a squash merge; a rebased child uses the GitHub base instead.
Missing or stale stack boundaries stop the review; refresh the stack description and retry.
Ordinary PRs and stack roots without a pinned comparison link use their configured GitHub base.
Reviewers receive both base SHAs and treat ancestor changes as context only. Stack boundaries
are author supplied and verified for ancestry, not independently established as complete.

Inline publication validates positions against GitHub's PR diff. RIGHT-side positions use the
shared head tree; LEFT-side findings from a different scoped diff remain in the collapsed body.
If GitHub rejects an inline review, the fallback comment retains the findings intended for inline
publication.

Each published inline finding includes feedback instructions: react 👍 for helpful or 👎 for
unhelpful or incorrect, or 👀 for out of scope. Feedback remains
on the comment for manual review and prompt tuning; the workflow does not automatically consume
these ratings or learn from them. The footer is excluded from exact finding deduplication.
