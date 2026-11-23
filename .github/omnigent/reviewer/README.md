# AI reviewer prompts

Each reviewer bundle uses its adjacent `REVIEW.md` as the executable Omnigent prompt through
`instructions: REVIEW.md` in `config.yaml`. Changes to those Markdown files therefore change the
CI review behavior.

To run a reviewer locally, [expand the shared instruction blocks](#shared-instruction-blocks)
into a directory that does not exist yet. From that directory, load `config.yaml` for the
orchestrator or `agents/<name>/config.yaml` for a child reviewer. Supply the PR metadata and diff
as review context.

PR authors using git stack should keep the generated stack list in their PR description up to
date, including the link to the immediate parent PR. The parent and child PRs must be opened in the
same repository; their head branches can live in forks. For an unmerged parent, the child must
inherit the parent's current head, or the workflow stops with a neutral PR check explaining that
the stack needs synchronizing. This avoids guessing which changes belong to the child after a parent
advances or is rewritten.
Scope decisions are logged in the workflow run. If resolution stops, the author should follow the
neutral check's instructions to refresh the description or synchronize the stack, then retry.
See [`resolve_review_base`](../review_scope.py) for the boundary selection rules. Published scoped
reviews identify their reviewed commit range.

Inline publication validates positions against GitHub's PR diff. LEFT-side findings stay in the
collapsed review when that file's scoped and GitHub diff sections differ. If GitHub rejects an
inline review, the fallback comment retains the findings intended for inline publication.

Each published inline finding includes feedback instructions: react 👍 for helpful or 👎 for
unhelpful or incorrect, or 👀 for out of scope. Feedback remains
on the comment for manual review and prompt tuning; the workflow does not automatically consume
these ratings or learn from them. The footer is excluded from exact finding deduplication.

## Shared instruction blocks

Each `REVIEW.md` keeps its reviewer's own instructions inline and shows, in reading order,
everything that reviewer receives. Text that several reviewers share is stored once under
[`shared/`](shared), and a `REVIEW.md` names it with a placeholder line. The line
`${review_rules}` stands for `shared/review_rules.md`.

| Block | Contents | Included by |
|---|---|---|
| `${base_context}` | Project conventions and read-only source tools | Every child except `disprove-reviewer` |
| `${review_rules}` | Known issue handling and previous AI review handling | Every reviewer |
| `${pr_description}` | PR description accuracy | The orchestrator and `docs-reviewer` |
| `${maintainer}` | Maintainer expertise and review criteria | Both maintainer reviewers |
| `${ci_environment}` | CI restrictions | Every child except `disprove-reviewer` |

For example, `agents/docs-reviewer/REVIEW.md` reads:

```text
${base_context}

${review_rules}

${pr_description}

You are an elite documentation reviewer specializing in Rust codebases ...
[the docs reviewer's own mission, review process, and output format]

${ci_environment}
```

Omnigent loads `REVIEW.md` as written, so [`prepare_reviewer.py`](../prepare_reviewer.py) writes a
copy of this directory in which every placeholder line is replaced by its block:

```bash
python3 .github/omnigent/prepare_reviewer.py /tmp/reviewer
```

The "Expand reviewer instructions" step in `ai-review.yml` runs the same command and uses the copy
in place of the checked-out directory. To see exactly what a reviewer receives, read its
`REVIEW.md` in the copy.

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
