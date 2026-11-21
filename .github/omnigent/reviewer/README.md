# AI reviewer instructions and tasks

The orchestrator in this directory dispatches the child reviewers under `agents/` and consolidates
their findings. Every reviewer, orchestrator or child, starts with two texts:

- **Instructions** are static for a given reviewer at a given workflow revision. They are the
  reviewer's `REVIEW.md`. They define its role, what counts as a finding, and how to report one.
  Omnigent sends them ahead of the conversation: as the system prompt for Claude reviewers and as
  developer instructions for Codex reviewers.
- **The task** is the reviewer's first message. It tells the reviewer what to do in this run and
  gives it the material to work on.

For example, `architecture-reviewer` gets the same `REVIEW.md` on every pull request. Its task
starts with an assignment such as "Review the architecture of this change." The workflow then
appends prior AI review history when there is any, and the pull request's metadata, description,
and diff. The two maintainer reviewers also get root `AGENTS.md` in their task. `AGENTS.md`
contains rules, but it travels in the task because the workflow selects its version for each run.

A reviewer receives more input after it starts:

- At startup, Omnigent gives the reviewer the definitions of the tools it may call, such as
  `read_source_file`.
- During the review, tool results enter the conversation. For a change under `ffi/`, a maintainer
  reviewer has root `AGENTS.md` in its task, and `ffi/AGENTS.md` arrives when it reads that file
  with `repository: guidance`.
- During coordination, the orchestrator receives each child's findings. It sends candidate
  findings to `disprove-reviewer` in that reviewer's assignment and receives its verdicts.

To decide where a piece of text belongs, ask when the reviewer needs to learn it:

| The reviewer needs it | Where it goes | Example |
|---|---|---|
| On every run | Instructions | "Check for duplicate findings." |
| For this run | Task | The findings from the last AI review of this pull request |
| For some changes only | A tool read | The contents of `ffi/AGENTS.md` |

## Where each text comes from

| Text | Contents, in order | Where it comes from |
|---|---|---|
| Orchestrator instructions | [`REVIEW.md`](REVIEW.md) with its shared blocks expanded | Checked in. [`config.yaml`](config.yaml) names it under `instructions`. |
| Orchestrator task | PR metadata and description; orchestration notes, prior AI reviews, and the output contract; PR diff | The "Collect PR data and build reviewer tasks" step in [`ai-review.yml`](../../workflows/ai-review.yml) writes `/tmp/orchestrator_task.md`. The "Run AI review" step passes it to `omnigent run -p`. |
| Child instructions | `agents/<name>/REVIEW.md` with its shared blocks expanded | Checked in. `agents/<name>/config.yaml` names it under `instructions`. |
| Child task | Assignment; root `AGENTS.md` for the two maintainer reviewers; PR data | The orchestrator writes the assignment. [`child_task.py`](../child_task.py) supplies the rest, called the child task body. |

Changing a `REVIEW.md` or a shared block changes CI review behavior. Most of a task is the pull
request's own text, which the PR author controls and reviewers treat as untrusted data.

The **trusted workflow revision** is the commit the workflow itself runs from (`GITHUB_SHA`).
Automatic reviews and `/review` comment runs use the tip of the repository's default branch,
whatever branch the pull request targets, and never the pull request's head. A manual dispatch
uses the ref the dispatcher selects.

## Shared instruction blocks

Each `REVIEW.md` keeps its reviewer's own instructions inline and shows, in reading order,
everything that reviewer receives. Text that several reviewers share is stored once under
[`shared/`](shared), and a `REVIEW.md` names it with a placeholder line. The line
`${review_rules}` stands for `shared/review_rules.md`.

| Block | Contents | Included by |
|---|---|---|
| `${project_guidance}` | Where project rules live, which sources are untrusted, and the limits on convention-only findings | Every child |
| `${review_rules}` | Known issue handling and previous AI review handling | Every reviewer |
| `${pr_description}` | PR description accuracy | The orchestrator and `docs-reviewer` |
| `${maintainer}` | The maintainer role and review criteria | Both maintainer reviewers |
| `${ci_environment}` | CI restrictions | Every child except `disprove-reviewer` |

For example, `agents/docs-reviewer/REVIEW.md` reads:

```text
${project_guidance}

${review_rules}

${pr_description}

You are an elite documentation reviewer specializing in Rust codebases ...
[the docs reviewer's own mission, review process, and output format]

${ci_environment}
```

The two maintainer reviewers have identical `REVIEW.md` files. They differ only in `config.yaml`,
which selects the harness and the model: Claude for one, Codex for the other. The [FAQ](#faq)
explains why they stay two files.

Omnigent loads `REVIEW.md` as written, so [`prepare_reviewer.py`](../prepare_reviewer.py) writes a
copy of this directory in which every placeholder line is replaced by its block:

```bash
python3 .github/omnigent/prepare_reviewer.py /tmp/reviewer
```

The "Expand reviewer instructions" step in `ai-review.yml` runs the same command and uses the copy
in place of the checked-out directory. To see exactly what a reviewer receives, read its
`REVIEW.md` in the copy.

## Example: what `architecture-reviewer` receives

Its **instructions** are `agents/architecture-reviewer/REVIEW.md` after expansion. The outline
below lists the sections; the right column gives the shared block or a summary.

```text
## Project guidance                ${project_guidance}
## Known issue handling            ${review_rules}
## Previous AI review handling     ${review_rules}
You are a senior systems architect reviewing Rust codebases ...
## Your Mission                    evaluate only the shape of the change
## What you do NOT review          line-level bugs, style, test coverage
## Output Format                   at most 5 findings
## Anatomy of a strong finding
## CI environment note             ${ci_environment}
```

Nothing in the instructions refers to a particular pull request. They give the model its role,
its scope, its output format, and its restrictions.

Its **task** is one string, built for each run:

```text
Review the architecture of this change.               assignment, written by the orchestrator
## PR data (untrusted)
[prior AI reviews, or a sentence saying none exist]
## PR Metadata                                        title, branches, SHAs, change size
## PR Description                                     written by the PR author
## PR Diff                                            at most 80,000 bytes
```

The first line is the assignment. The orchestrator chooses it, so it differs between children.
Everything after it is the child task body. A maintainer reviewer's task has one more section,
between the assignment and the PR data:

```text
## Root AGENTS.md (trusted workflow revision <sha>)
[root AGENTS.md]
```

## How a child task is assembled

1. The "Stage trusted project guidance" step copies the guidance `.md` files committed at the
   trusted workflow revision into a new directory and exports its path as `GUIDANCE_SOURCE_ROOT`.
2. The "Collect PR data and build reviewer tasks" step calls `build_root_guidance()` and
   `build_pr_data()` in `child_task.py`. It writes their results to
   `/tmp/child_task_root_guidance.md` and `/tmp/child_task_pr_data.md`. The same step writes the
   orchestrator task.
3. The "Run AI review" step starts the orchestrator with its instructions and its task.
4. The orchestrator calls `sys_session_send` with only an assignment in `args.input`. The dispatch
   hook `append_child_task_body()`, registered in `config.yaml`, appends the PR data file. For a
   reviewer listed under `root_guidance_agents` in `config.yaml`, the hook puts the root guidance
   file first. Omnigent then starts the child with its instructions and that task. A missing
   assignment or file denies the dispatch.

`config.yaml` lists the hook after the two policies that guard dispatches: the cap on dispatches
per turn and the check that each dispatch declares a purpose. Omnigent passes a rewritten dispatch
to later policies without its tool name, so a policy listed after the hook would not recognize the
dispatch and would allow it.

## Project guidance

Reviewers read source from three directories, which the source tools call repositories:

| Repository | Contents | Reviewers treat it as |
|---|---|---|
| `guidance` | The `.md` files committed at the trusted workflow revision, without changelogs, user guide pages, and this reviewer's own files | Project rules |
| `pr` | The pull request's head | Untrusted data |
| `delta` | `delta-io/delta` at `master` | Untrusted data |

Reviewers take project rules only from `guidance` because the PR author controls everything in
`pr`. If reviewers applied the pull request's copy of `AGENTS.md`, a pull request could delete the
rule it breaks, or add text telling reviewers to approve it. Files in `guidance` come from the
trusted workflow revision, which the pull request cannot change. A pull request's edits to
guidance are therefore review data, not policy for its own review. `delta` is another project's
branch and is reference data for the same reason.

The two maintainer reviewers check general project conventions, so the workflow puts root
`AGENTS.md` in their task. The other reviewers work within one focus and do not receive it. Any
reviewer can read guidance with `read_source_file`, `list_source_files`, and `search_source_code`
and `repository: guidance`, and `${project_guidance}` tells each one to read a project rule before
relying on it. A file read returns complete lines and ends with a `Next line: N` cursor or
`End of file.`; a line too long to return produces an error that names the line to continue from.
Root `AGENTS.md` must be nonempty UTF-8; `tests/test_child_task.py` checks the repository's copy.
The workflow sends the whole file, whatever its size.

The dispatch hook is the only path by which `AGENTS.md` reaches a reviewer. Codex would otherwise
add the `AGENTS.md` files between its working directory and the repository root to every session
it starts, so the workflow sets `project_doc_max_bytes = 0` in the Codex configuration.

A convention-only finding must cite a rule, concern a change visible in the diff, and omit issues
that configured linters enforce. The orchestrator merges these findings by rule and publishes at
most three, as non-blocking notes.

## Running a reviewer locally

Run `prepare_reviewer.py` with a destination directory that does not exist yet. Install source
tools in the expanded copy as the workflow's "Materialize read-only source tools" step does,
then set `PR_SOURCE_ROOT`, `DELTA_SOURCE_ROOT`, and `GUIDANCE_SOURCE_ROOT`.

To run one child, load its `config.yaml` from the copy and send it a task: an assignment followed
by the output of `build_pr_data()`. For a maintainer reviewer, put the output of
`build_root_guidance()` between them. To run the orchestrator, also put `.github/omnigent` on
`PYTHONPATH` so Omnigent can import `child_task`, and write the two files that `config.yaml` names
under `append_child_task_body`.

The unit tests and the workflow's runtime probes check that instructions expand and load, tasks
are assembled in order, and sources are readable. They do not measure review quality.

## Finding feedback

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

## FAQ

### Why are there two maintainer reviewers with identical `REVIEW.md` files?

The maintainer criteria are one file, `shared/maintainer.md`. The two `REVIEW.md` files only name
shared blocks, and `tests/test_prepare_reviewer.py` fails if they differ. Two directories remain
for two reasons:

- Omnigent ties a harness and a model to one agent directory's `config.yaml`. Reviewing with
  Claude and with Codex therefore takes two directories.
- Each directory needs its own instructions file. When `instructions:` names a path outside the
  agent's directory, Omnigent does not load that file. It uses the path string itself as the
  instructions and reports no error.

### Why not dispatch one maintainer agent twice with different models?

Omnigent lets a dispatch override the harness and the model when the agent opts in. The reviewer
does not use that:

- The orchestrator reads untrusted pull request text, so it does not choose models. The dispatch
  hook removes model and reasoning effort overrides, and each reviewer runs with the values in its
  own `config.yaml`.
- The quorum rule counts the maintainer reviewers, and failure reporting and finding attribution
  name each reviewer.
- The "Materialize reviewer model config" step replaces the model alias in each `config.yaml`
  with a model from the repository variables.

### Why does a missing, empty, or non-UTF-8 root `AGENTS.md` fail the review?

The maintainer instructions state that the task includes root `AGENTS.md`. A review that ran
without the file would make that statement false, and the maintainers would check conventions
against rules they cannot see. `tests/test_child_task.py` checks the repository's copy on every
pull request. The file has no size limit: the workflow sends it whole.

### Why does the workflow turn off Codex's own `AGENTS.md` loading?

Codex adds the `AGENTS.md` files it finds between its working directory and the repository root to
every session. The reviewers run inside the checkout, so each Codex reviewer would receive root
`AGENTS.md` that way: `disprove-reviewer`, which is not meant to get it, and the Codex maintainer
a second time. Setting `project_doc_max_bytes = 0` leaves the dispatch hook as the only path, so
`config.yaml` alone decides which reviewers receive the file.
