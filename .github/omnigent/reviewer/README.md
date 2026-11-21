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
inherits it, including after a squash merge; a child inheriting the parent's merge result uses
the GitHub base instead.
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

## Tools

Omnigent registers its built-in tool groups on every agent unless the agent's config opts out.
Each reviewer config sets `tool_groups.projects: false`, and the workflow sets
`OMNIGENT_FEATURES: "-peer_session_messaging"`. Together they remove the tools that organize
sessions into projects and the tool that sends a message to another session.

The reviewers read untrusted PR text and do not use these tools. Two of them let one agent act
on another: `sys_session_message` puts a message into another agent's session, and
`sys_session_update` can archive another agent's session, which stops it.

A new reviewer config needs the same `tool_groups` block. The "Validate AI reviewer runtime"
step in `ai-review.yml` owns the list of removed tools and fails when one is exposed.

## Dependencies

The review workflow installs Omnigent from `../requirements.txt` and its build backend from
`../requirements-build.txt`. `uv pip compile` generates both files, and each pins every package
by version and hash. Edit only the `.in` files.

To change a version:

1. Edit `requirements.in` or `requirements-build.in`.
2. From the repository root, run the command in the header of the matching `.txt` file with the
   uv version that `ai-review.yml` installs. uv keeps every existing pin that the new input
   allows, so the diff shows only what the change requires. Add `--upgrade` to refresh every
   pin, in a pull request of its own.
3. Commit the `.in` and `.txt` files together.
4. Run a review from the branch before merging. Automatic reviews use the reviewer on `main`,
   so a pull request does not exercise its own lock. Push the branch to this repository, then
   run `gh workflow run ai-review.yml --ref <branch> -f pr=<number> -f output_mode=artifact`.
5. List the packages the lock adds and drops in the pull request description.

`../package.json` and `../package-lock.json` pin the Claude Code and Codex CLIs.
