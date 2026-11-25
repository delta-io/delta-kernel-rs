# AI review architecture and terminology

[`ai-review.yml`](../../workflows/ai-review.yml) runs an orchestrator model and specialist review
models. Python prepares their inputs and output requests; workflow steps create GitHub comments,
Checks or saved reports. This document defines those responsibilities and the text exchanged.
Contributor disclosures belong in [`AI_POLICY.md`](../../../AI_POLICY.md).

## Components

| Component | Responsibility |
| --- | --- |
| GitHub Actions workflow | Shell commands fetch PR inputs and call GitHub; Python builds task messages, validates model output and prepares requests/reports. |
| Omnigent | Start model sessions, supply tools and carry assignments/results between sessions. |
| Orchestrator | Select Reviewer subagents, assign their work, request checking and write the orchestrator report. |
| Reviewer subagent | A separate model session reviewing architecture, protocol, maintainer concerns, tests or docs. |
| Checker subagent | The separate `disprove-reviewer` model session that challenges supplied candidate findings. |

## Message glossary

Use **role instructions** for the separate standing role text, and **task message** for a
session's initial run-specific message. A task message can itself contain instructions. These
terms distinguish how text reaches the model; they do not claim that all rules live outside the
task message.

| Term | Definition | Concrete example |
| --- | --- | --- |
| Role instructions | Standing rules for a model role, loaded from its configured `REVIEW.md` with shared text expanded. | The Docs role checks documentation against code. |
| Assignment | The request the orchestrator writes for a particular subagent, before Python appends the shared PR task text and any maintainer guidance. | "Review PR #1234's documentation for accuracy." |
| Task message | The initial run-specific message delivered to a model session. | That Docs assignment plus PR metadata, reviewed SHA, description, earlier AI review text and patch excerpt. |
| Shared PR task text | Text appended to each subagent assignment: optional review-range instructions, earlier AI review text, PR metadata, description excerpt and patch excerpt. | Python writes it to `/tmp/child_task_pr_data.md`. |
| Child task body (implementation term) | Text appended after an assignment: shared PR task text, preceded by trusted root `AGENTS.md` for the two maintainer roles. | The `append_child_task_body()` hook and its error messages use this term. |
| Response format instructions | Requirements for the response's run-specific start/end markers and final-finding JSON with optional locations. | The workflow's `output_contract` variable contains these instructions. |
| Trusted project guidance | Project rules selected from the trusted workflow revision. | Root `AGENTS.md`, appended inside the two maintainer task messages. |
| Tool definition | A callable tool's name, parameters and usage description supplied to a model. | `read_source_file(repository, path, start_line=1, line_count=400)`. |
| Tool call | A model's request to execute a tool with particular arguments. | Read line 6 of `integration-tests/Cargo.toml` from `repository="pr"`. |
| Tool result | The content or error returned by executing one tool call. | A numbered source line followed by `Next line: 7`, or `Error: ...`. |
| Reviewer result | A Reviewer subagent's completed report of candidate findings or no findings. | The Docs subagent reports that a command omits the separate integration-test workspace. |
| Candidate finding | A specific problem proposed before the orchestrator's checking and final selection. | The Docs subagent's concern about that command. |
| Checker decision | The Checker's judgment of one supplied candidate finding. | `CONFIRMED`, `DISPROVED`, `NITPICK` or `CONTESTED`. |
| Checker result | The Checker subagent's completed report of decisions, evidence and assessments of proposed fixes. | For each supplied candidate finding, `disprove-reviewer` returns a decision, its supporting evidence and an assessment of the proposed fix. |
| Final finding | A problem included in the orchestrator report, with a severity and run-local ID. | The documentation correction after the orchestrator selects it. |
| Orchestrator report | The Markdown findings and summary returned by the orchestrator, before the workflow formats GitHub comments. | The final findings selected from the reviewer results and checker result, followed by a summary. |

Omnigent can deliver a Reviewer result or Checker result inside a tool-result message. A
`read_source_file` tool result contains source evidence.

## Review flow

Boxes identify components or external data providers. Text between boxes names the information
passed between them. Numbers identify operations in this explanation; they are not runtime IDs.
Omnigent is the runtime that starts model sessions, supplies their tools and carries messages
between them. Every ORCHESTRATOR box below represents the same model session at a later step.

```text
+-------------------------------------------+     +-------------------------------------------+
| TRUSTED WORKFLOW REVISION                 |     | GITHUB AND REFERENCE DATA                 |
| Workflow/Python, config.yaml, REVIEW.md,  |     | PR metadata, original description, patch, |
| shared instruction text and AGENTS.md     |     | earlier AI reviews, PR/Delta source       |
+-------------------------------------------+     +-------------------------------------------+
                      | selected code/instructions                      | review data
                      +------------------------+------------------------+
                                               v
             +-------------------------------------------------------------------+
             | (1) GITHUB ACTIONS WORKFLOW: shell and Python helpers             |
             | Select review base/head; fetch PR data; prepare message text.     |
             | Save the PR patch and task text as files on the Actions runner.   |
             | Start Omnigent with the orchestrator configuration and task.      |
             +-------------------------------------------------------------------+
                                               | role instructions + initial task message
                                               v
             +-------------------------------------------------------------------+
             | (2) ORCHESTRATOR MODEL SESSION                                    |
             | Select relevant Reviewer subagents; docs-reviewer is required.    |
             | Write an assignment for each selected role.                       |
             +-------------------------------------------------------------------+
                                               | sys_session_send: role + assignment
                                               v
             +-------------------------------------------------------------------+
             | (3) OMNIGENT DISPATCH + child_task.py                             |
             | Task message = assignment + optional guidance                     |
             |                + shared PR task text.                             |
             | Include trusted root AGENTS.md for the two maintainer roles only. |
             | Start each selected subagent with its configured instructions.    |
             +-------------------------------------------------------------------+
                                               | task message for each selected role
        (4) AVAILABLE REVIEWER SUBAGENTS: DOCS + OTHER SELECTED RELEVANT ROLES
                                               |
       +---------------+---------------+-------+-------+---------------+---------------+
       |               |               |               |               |               |
       v               v               v               v               v               v
+-------------+ +-------------+ +-------------+ +-------------+ +-------------+ +-------------+
| ARCHITECTURE| | DELTA       | | CLAUDE      | | CODEX       | | TEST        | | DOCS        |
|             | | PROTOCOL    | | MAINTAINER  | | MAINTAINER  | | COVERAGE    | | (required)  |
| Reviewer    | | Reviewer    | | Reviewer    | | Reviewer    | | Reviewer    | | Reviewer    |
| subagent    | | subagent    | | subagent    | | subagent    | | subagent    | | subagent    |
+-------------+ +-------------+ +-------------+ +-------------+ +-------------+ +-------------+
       |               |               |               |               |               |
       +---------------+---------------+-------+-------+---------------+---------------+
                                               | reviewer results: candidate findings or none
                                               v
+-----------------------------------------------------+     +---------------------------------+
| (5) ORCHESTRATOR MODEL SESSION                      |---->| CHECKER SUBAGENT                |
| Collect reviewer results; build a candidate list.   |     | disprove-reviewer               |
| Before keeping Blocker/Should Fix, request one      |     | Challenge supplied claims.      |
| checker task containing the candidate list.         |<----| Return a checker result.        |
+-----------------------------------------------------+     +---------------------------------+
                                               | candidate findings + checker result
                                               v
             +-------------------------------------------------------------------+
             | (6) ORCHESTRATOR MODEL SESSION                                    |
             | Merge duplicates; select findings; write the orchestrator report. |
             +-------------------------------------------------------------------+
                                               | orchestrator report + optional inline-location JSON
                                               v
             +-------------------------------------------------------------------+
             | (7) GITHUB ACTIONS WORKFLOW: shell and Python helpers             |
             | Python checks and extracts the orchestrator report.               |
             | finding_publication.py posts or reuses each finding comment.      |
             | review_publish.py formats the main comment and finding comments.  |
             | Workflow attempts the AI Review Summary Check Run.                |
             +-------------------------------------------------------------------+
                                               | output mode selects report location
              +--------------------------------+--------------------------------+
              v                                v                                v
+---------------------------+    +---------------------------+    +---------------------------+
| GITHUB: INLINE MODE       |    | GITHUB: COMMENT OR        |    | ACTIONS: SUMMARY OR       |
| Main Conversation comment |    | COLLAPSED MODE            |    | ARTIFACT MODE             |
| plus one comment per      |    | Main Conversation comment |    | Job summary or artifact   |
| final finding             |    | plus finding comments     |    |                           |
+---------------------------+    +---------------------------+    +---------------------------+
```

When credentials and the reviewed head are available, the workflow attempts to create the
`AI Review Summary` Check Run in the PR's Checks tab, with GitHub conclusion `neutral`.

The outgoing checker task message contains the candidate-finding list; the checker result contains
decisions, evidence and assessments of proposed fixes. The orchestrator instructions require this
checker task before publishing any Blocker or Should Fix, and prohibit separate checker calls per
reviewer or per finding. The checker runs in its own model session, separately from the Reviewer
subagents. Separate sessions can use the same underlying model.

The orchestrator must select Docs and the other relevant reviewers.
Before returning an orchestrator report for publication, it must receive completed
results from at least one maintainer and one other Reviewer subagent.
A failed reviewer gets one retry after the other in-flight reviewers finish.
The orchestrator waits for every dispatched reviewer to finish or exhaust that retry,
and for any required checker result. It lists reviewers still unavailable after
retry in the Summary. These are model instructions; workflow Python does not
independently verify reviewer completion.

### Source reads during model review

A Reviewer or Checker subagent can request more source text while reviewing. A source read
returns file text to the requesting session. That intermediate tool result is separate from the
Reviewer or Checker subagent's completed report to the orchestrator.

```text
+--------------------------------+                    +---------------------------------------+
| REVIEWER OR CHECKER SUBAGENT   | -- tool call ----> | READ-ONLY PYTHON SOURCE TOOLS         |
| Choose file and line range.    |                    | Read from selected source tree.       |
| Read returned numbered lines.  | <-- tool result -- | Return numbered text or an error.     |
+--------------------------------+                    +---------------------------------------+
                                                                          | repository selector
              +--------------------------------+--------------------------+-----+
              v                                v                                v
+---------------------------+    +---------------------------+    +---------------------------+
| repository: pr            |    | repository: delta         |    | repository: guidance      |
| Reviewed PR head          |    | Delta reference source    |    | Trusted workflow revision |
+---------------------------+    +---------------------------+    +---------------------------+
```

### Reviewer roles

| Diagram label | Omnigent configuration identifier | Review focus |
| --- | --- | --- |
| Architecture | `architecture-reviewer` | APIs, abstractions and module boundaries. |
| Delta protocol | `delta-protocol-reviewer` | Changed behavior against the Delta specification. |
| Claude maintainer | `maintainer-claude-reviewer` | Rust/Delta behavior and project conventions. |
| Codex maintainer | `maintainer-codex-reviewer` | The maintainer checklist using the configured Codex model. |
| Test coverage | `test-coverage-reviewer` | Missing cases and meaningful assertions. |
| Docs | `docs-reviewer` | PR title/description and documentation accuracy. |
| Checker | `disprove-reviewer` | Evidence for or against supplied candidate findings. |

## Where each message comes from

| Message | Contents, in order | Producer |
| --- | --- | --- |
| Orchestrator role instructions | [`REVIEW.md`](REVIEW.md) with shared blocks expanded | Checked in; [`config.yaml`](config.yaml) selects `instructions`. |
| Orchestrator task message | PR metadata/description; run-specific rules, earlier AI review text and response format instructions; patch excerpt | The "Collect PR data and build reviewer tasks" step in [`ai-review.yml`](../../workflows/ai-review.yml). The "Run AI review" step passes the text to `omnigent run -p`. |
| Subagent role instructions | `agents/<role>/REVIEW.md` with shared blocks expanded | Checked in; that role's `config.yaml` selects `instructions`. |
| Reviewer subagent task message | Orchestrator assignment; trusted root `AGENTS.md` for the two maintainer roles; shared PR task text | The orchestrator writes the assignment; [`child_task.py`](../child_task.py) appends the remaining text. |
| Checker subagent task message | Orchestrator assignment containing candidate findings; shared PR task text | The same dispatch hook appends the shared PR task text to the checker assignment. |

Changing `REVIEW.md` or a shared block changes model instructions. A task message also contains
run-specific instructions, but the PR description, patch and earlier AI review text remain
untrusted data.

The **trusted workflow revision** is the commit the workflow runs from (`GITHUB_SHA`). Automatic
reviews and `/review` comment runs use the tip of the repository's default branch, regardless of
the PR's target branch. A manual dispatch uses the ref the dispatcher selects. This revision
selects the workflow, model configuration, role instructions and project guidance. The
**reviewed PR head** separately selects the source code under review.

Omnigent sends role instructions ahead of the conversation: as a system prompt for Claude
sessions and developer instructions for Codex sessions. Tool definitions are supplied separately;
later tool results and messages join the conversation after the initial task message.

### One worked example

This is an illustrative review of a fictional PR, not a transcript or a reported defect. The
source-file excerpt and source-tool result format below are shown for `integration-tests/Cargo.toml`.

1. Fictional PR #1234 adds this sentence to documentation:

   > Run `cargo test --workspace` to run all integration tests.

2. Omnigent loads the Docs role instructions from its configured `REVIEW.md`. An actual
   instruction in that role is: "Review all documentation touched by or relevant to recent code
   changes in the PR."
3. The orchestrator writes the assignment: "Review PR #1234's documentation for accuracy."
   `child_task.py` appends the shared PR task text, producing the Docs task message. It includes
   the reviewed commit and the changed sentence. Docs does not automatically receive root
   `AGENTS.md`; it can read trusted guidance through a tool when needed.
4. The Docs subagent makes this tool call:

```python
read_source_file(
    repository="pr",
    path="integration-tests/Cargo.toml",
    start_line=6,
    line_count=1,
)
```

5. The source tool returns numbered file lines and a continuation cursor. Numbers below are
   separated from source text by a tab, matching the tool's return format:

```text
6	[workspace]
Next line: 7
```

6. The Docs subagent returns a candidate finding: "The command runs tests in the root workspace,
   but `integration-tests` declares a separate workspace. Run that workspace separately."
   The orchestrator receives this reviewer result, applies the checker policy where required,
   and decides whether to include the correction in the orchestrator report. Python prepares the
   comment request or report; the workflow's output steps create the selected GitHub objects
   or saved report.

## PR change patch and stacked PRs

The **PR change patch** is the fetched patch for the selected review base and reviewed PR head,
before shortening it for task messages. For a stacked PR, the selected base excludes inherited
parent-PR changes:

```text
main:        A
              \
parent PR:     P1 -- P2
                     \
this PR:              C1 -- C2

Review base: P2                   Reviewed PR head: C2
PR change patch: changes between P2 and C2; excludes inherited P1/P2 changes
```

The **GitHub PR patch** is the comparison against GitHub's PR base. It can include parent-PR
changes that the selected review range excludes. Inline-comment validation needs this patch
because GitHub uses it to determine valid comment locations. For ordinary PRs, the two ranges
coincide.

PR authors using git stack must keep the generated stack list and immediate-parent PR link up
to date. Parent and child PRs must belong to the same repository; the declared parent may already
be merged. Head branches can live in forks. An unmerged parent's current head must be an ancestor
of the child. Otherwise the workflow
stops model review and creates a neutral Check Run explaining the boundary problem. Follow that
check's instructions to refresh the description or synchronize the stack, then retry.
[`resolve_review_base`](../review_scope.py) selects the boundary and logs its decision.

A **patch excerpt** is the leading portion of the fetched PR change patch inserted into task
messages. Selecting the commit range happens before applying the excerpt's byte limit.
`pr_change_patch_bytes` in the workflow holds the fetched patch; `pr_change_patch_excerpt_text`
holds the decoded excerpt. Neither a fetched patch nor a model's successful return proves every
changed line was examined.

## Input limits

The workflow applies these limits while preparing the initial task messages:

| Workflow local | What it limits | Value |
| --- | --- | --- |
| `max_patch_excerpt_bytes` | Raw PR change patch bytes, before decoding; the same excerpt is included in orchestrator and subagent task messages. | 80,000 bytes |
| `max_orchestrator_initial_task_message_bytes` | The entire initial orchestrator task message, encoded as UTF-8. | 120,000 bytes |
| `pr_description_excerpt_text` | The original PR description, before adding any truncation warning. | 4,096 Python characters |

The patch excerpt omits the assignment and other PR data. Subagent task messages contain their
assignments, shared PR task text and optional maintainer guidance; the task builder does not apply a
combined 120,000-byte limit to those messages. Source-tool reads can return additional context
later in the conversation.

The 120,000-byte guard protects the single `omnigent run -p` command argument. On Linux with
4 KiB pages, one argument is limited to 131,072 bytes including its trailing NUL. Separate role
instructions, tool definitions and later conversation are outside this initial-message guard.
It is not the model's context-window limit. The patch cap is an input-size policy, without a
measured review-quality optimum established by this workflow.

The workflow takes the description's first 4,096 characters without removing generated stack
navigation. A long stack header can therefore crowd out author-written text. A patch exceeding
80,000 bytes gets a truncation warning. If the orchestrator's initial task message exceeds
120,000 UTF-8 bytes, the workflow stops before starting the orchestrator.

## GitHub comments, Checks and saved reports

| Term | GitHub object and visible location |
| --- | --- |
| Inline review comment | A file/line comment starting a discussion thread in Files changed. |
| Review body | Text submitted with a GitHub review, separate from its inline comments. The publication code creates individual comments without submitting a review body. |
| PR Conversation comment | A top-level comment in the PR's Conversation timeline, without a file/line anchor. |
| Main AI review comment | A Conversation comment with severity counts, the reviewed commit, an assessment summary and links to finding comments. |
| Confirmed comment URL | A URL returned by GitHub or read from an existing trusted bot comment. It establishes that the comment exists. |
| `AI Review Summary` Check Run | The commit-specific result in the PR's Checks tab, with GitHub conclusion `neutral`. |
| Actions artifact | Downloadable files attached to a workflow run. |

A **Blocker** is a supported correctness, protocol or safety defect, a materially inaccurate PR
explanation, or an undisclosed breaking change that needs correction before approval. **Should
Fix** is a concrete advisory improvement. A **Nit** is a smaller useful correction. The selection
rules, including limits on convention findings, are in [`REVIEW.md`](REVIEW.md#consolidation).

In PR comment modes, [`finding_publication.py`](../finding_publication.py) creates a main AI review
comment, posts or reuses each final finding's comment, then updates the main comment with confirmed
counts and links. [`review_publish.py`](../review_publish.py) formats the comment text. `inline`
mode attempts inline placement; `comment` and `collapsed` modes use Conversation finding comments.

In `summary` mode, the workflow writes the complete report to its Actions job summary. In
`artifact` mode, it uploads the report and supporting files listed below. These modes create no
PR comments.

When credentials and the reviewed PR head are available, the workflow attempts to create an
`AI Review Summary` Check Run. It includes all parsed final findings when available.
If review-range selection fails, it shows the resolver's error. Otherwise, it shows a
generic unavailable message and a workflow-run link for diagnostics. Its GitHub conclusion is
`neutral`; this does not mean the AI approved the PR.

### Files on the Actions runner

`/tmp/` paths below are local to the Actions runner. Artifact mode uploads the complete report, publication record,
PR change patch and shared PR task text with seven-day retention.

| Runner file | Writer | Consumer and purpose |
| --- | --- | --- |
| `/tmp/pr_meta.json` | GitHub fetch step | Task builder and range resolver read original metadata/description and commit identities. |
| `/tmp/pr_history.json` | History fetch step | History code prepares earlier AI review text for the orchestrator. |
| `/tmp/pr_review_comments.json` | Review-comment fetch step | History code associates earlier inline comments with diff sides. |
| `/tmp/pr_diff.txt` | Selected-range fetch | Task builder reads the PR change patch; publication code also checks inline locations against it. |
| `/tmp/github_pr_diff.txt` | Patch fetch | Publication code checks inline locations against GitHub's comparison; other modes reuse the PR change patch. |
| `/tmp/orchestrator_task.md` | Task builder | Workflow passes the initial task text to `omnigent run -p`. |
| `/tmp/child_task_pr_data.md` | Task builder | Dispatch hook appends shared PR task text to subagent assignments. |
| `/tmp/child_task_root_guidance.md` | Task builder | Dispatch hook adds trusted root guidance for the two maintainer roles. |
| `/tmp/review_raw_output.txt` | Model-run step | Validation step extracts the marked orchestrator report. |
| `/tmp/review_output.txt` | Validation step | Publication reads the extracted orchestrator report; the Check Run uses it if the prepared check summary is unavailable. |
| `/tmp/inline_findings.json` | Validation step | Publication code reads final findings and optional locations. |
| `/tmp/publication_record.json` | Publication step | Records final findings, confirmed comment URLs and posting errors. |
| `/tmp/review_output_complete.txt` | Publication step | Summary, artifact and Check Run outputs include all parsed final findings. |
| `/tmp/check-summary.txt` | Publication step or diagnostic path | Check Run reads the assessment and findings, resolver error or generic unavailable message. |

## Review input limits

[`diff_coverage.py`](../diff_coverage.py) prepares independent text limits from the exact resolved
base-to-head diff. The byte limits measure UTF-8 text, not characters or model tokens.

| Input | Limit | What the limit includes |
|---|---|---|
| Diff in each child task, including the checker | 100,000 bytes | The first diff bytes in Git's file order; instructions and assignments are separate. |
| Diff in the orchestrator task | Up to 80,000 bytes | The diff excerpt only, reduced further when the complete task has less room. |
| Complete orchestrator task | 120,000 bytes | Task-specific instructions, PR metadata/description, earlier reviews, path list and diff. |
| PR author's description | 8,192 bytes | Author text after recognized generated GitStack navigation is removed. |

`prepare_inputs()` cuts the child diff and records every changed path and its byte interval in
`/tmp/diff_coverage.json`. Each path is marked fully included, partly included or omitted.
`parent_prompt()` reserves the orchestrator's non-diff text and complete path list before adding
its separate diff excerpt. If the path list cannot fit, `list_changed_paths` reads it in pages
from `/tmp/ai_review_changed_paths.json`. A source-file read provides context and does not add
another changed line to the review's scope. Static reviewer instructions and later tool results
are separate from the initial task limit. Stack resolution reads the complete original PR body.

If the child diff is cut, the main AI review comment reports file counts between its reviewed
commit line and PR Summary. The AI Review Summary Check Run and workflow log list partly included
and omitted paths, reviewed SHAs and delivered/total bytes. An uncut diff adds no notice.
These measurements describe the supplied diff, not whether the reviewers found every defect.

### Finding comments and confirmed comment URLs

With `FINDING_PLACEMENT=inline-or-conversation`, the workflow's `inline` mode attaches each final
finding to a changed line when both the PR change patch and GitHub PR patch accept that
location. That creates an inline review comment.
Each other finding gets a separate PR Conversation comment, including PR-wide concerns and
locations GitHub rejects. LEFT-side findings use Conversation comments when the file's two diff
sections differ. The workflow does not limit the number of inline finding comments.

`finding_publication.py` checks earlier bot comments before posting another copy of a finding.
An explicitly rejected inline write can fall back to a Conversation comment. A write whose
response is lost remains unconfirmed, without an automatic read-back or retry.
The main comment counts each final finding with a confirmed new or reused comment URL once.
A comment URL establishes delivery, not correctness. Unconfirmed writes produce an explicit
posting error instead of a successful count.

The command writes finding IDs, severities, reviewed SHA, comment URLs and posting errors to
`/tmp/publication_record.json`. The workflow uses that file to build the Check Run report.
The Check Run includes all final findings. Posting errors appear in the workflow log and main
comment; the Check Run does not add a separate comment-delivery summary.
`summary` and `artifact` modes record their selected report output without posting finding comments.

### Main AI review comment format

[`format_review_summary()`](../review_publish.py) is the single renderer for the main comment.
The orchestrator supplies the assessment summary and final finding text. Python supplies counts, links,
the reviewed commit and the layout below. Angle-bracket placeholders stand for generated text;
hidden bot identifiers are omitted from this example.

```markdown
## 🤖 AI Review 🧐

**🛑 2 Blockers · 🔧 1 Should Fix · 💡 2 Nits**

<details><summary>Overall assessment</summary>

<one paragraph giving an overall assessment without repeating finding details>

</details>

<details><summary>Key findings</summary>

- **[Blocker 1]** [<one-line claim>](<finding comment URL>)
- **[Blocker 2]** [<one-line claim>](<finding comment URL>)
- **[Should Fix 1]** [<one-line claim>](<finding comment URL>)

</details>

<sub>Reviewed `<short SHA>` · [Full AI review → Checks](<PR checks URL>)
· [Workflow run](<workflow run URL>)</sub>
```

`Key findings` links every final Blocker and Should Fix finding, including reused comments.
It is absent when there are none. Nits count in the headline and have individual comments, but
no rows in that disclosure. All disclosures start closed. A posting failure can replace the
count line with `Finding counts unavailable.` and adds an explanation. The Check Run includes
all final findings, including findings supplied only in structured model output.

### Individual finding comment format

Inline and Conversation finding comments use the same format. Keep the claim, explanation and
suggested fix visible. Reviewer attribution, the reviewed commit and the main-comment link use
small text above the feedback prompt. Machine-readable IDs such as `ShouldFix1` are displayed as
labels such as `[Should Fix 1]`.

```markdown
**[Should Fix 1]** <one-paragraph claim>

**Explanation:** <evidence and reasoning supporting the claim>

**Suggested fix:** <the proposed correction>

---
<sub>Raised by: <reviewer names> · Reviewed `<short SHA>` · [AI Review](<main URL>)</sub>
<sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</sub>
```

## Shared instruction blocks

Each `REVIEW.md` contains that role's instructions in reading order, including placeholders for
shared instruction blocks. Text that several roles share is stored once under [`shared/`](shared),
and a `REVIEW.md` names it with a placeholder line. The line `${review_rules}` stands for
`shared/review_rules.md`.

| Block | Contents | Included by |
|---|---|---|
| `${project_guidance}` | Where project rules live, which sources are untrusted, and the limits on convention-only findings | Every subagent |
| `${review_rules}` | Known issue handling and previous AI review handling | Orchestrator, Reviewer subagents and Checker subagent |
| `${pr_description}` | PR description accuracy | The orchestrator and `docs-reviewer` |
| `${maintainer}` | The maintainer role and review criteria | Both maintainer reviewers |
| `${ci_environment}` | CI restrictions | Every subagent except `disprove-reviewer` |

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
in place of the checked-out directory. To inspect a role's expanded instructions, read its
`REVIEW.md` in the prepared copy.

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
## PR Metadata                                        title, branches, and commit SHAs
## PR Description                                     written by the PR author
## PR Diff                                            excerpt; see Review input limits
```

The first line is the assignment. The orchestrator chooses it, so it differs between children.
Everything after it is the child task body. When the workflow reviews only a stacked PR's
changes, the review scope precedes the PR data. A maintainer reviewer's task has one more section,
between the assignment and the PR data:

```text
## Root AGENTS.md (trusted workflow revision <sha>)
[root AGENTS.md]
```

## How a subagent task message is assembled

1. The "Stage trusted project guidance" step copies the guidance `.md` files committed at the
   trusted workflow revision into a new directory and exports its path as `GUIDANCE_SOURCE_ROOT`.
2. The "Collect PR data and build reviewer tasks" step calls `build_root_guidance()` and
   `build_pr_data()` in `child_task.py`. It writes their results to
   `/tmp/child_task_root_guidance.md` and `/tmp/child_task_pr_data.md`. The same step writes the
   orchestrator task.
3. The "Run AI review" step starts the orchestrator with its instructions and its task.
4. The orchestrator calls `sys_session_send` with only an assignment in `args.input`. The dispatch
   hook `append_child_task_body()`, registered in `config.yaml`, appends the shared PR task text. For a
   reviewer listed under `root_guidance_agents` in `config.yaml`, the hook puts the root guidance
   file first. Omnigent then starts the subagent with its instructions and that task. A missing
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

The dispatch hook controls which task messages automatically include root `AGENTS.md`. Codex
would otherwise add `AGENTS.md` files between its working directory and the repository root to
every session it starts, so the workflow sets `project_doc_max_bytes = 0` in the Codex configuration.

A convention-only finding must cite a rule, concern a change visible in the diff, and omit issues
that configured linters enforce. The orchestrator merges these findings by rule and publishes at
most three, as non-blocking notes.

## Running a reviewer locally

Run `prepare_reviewer.py` with a destination directory that does not exist yet. Install source
tools in the expanded copy as the workflow's "Materialize read-only source tools" step does,
then set `PR_SOURCE_ROOT`, `DELTA_SOURCE_ROOT`, and `GUIDANCE_SOURCE_ROOT`.

To run one subagent, load its `config.yaml` from the copy and send it a task: an assignment followed
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
a second time. Setting `project_doc_max_bytes = 0` leaves the dispatch hook as the only automatic
inclusion, so `config.yaml` decides which task messages receive the file. Reviewers can still
request guidance through source tools.
