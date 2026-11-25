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
| Orchestrator | Dispatch every configured Reviewer subagent, assign their work, request checking and write the orchestrator report. |
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
             | Dispatch every configured Reviewer subagent.                      |
             | Write an assignment for each configured Reviewer role.                       |
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
                                               | task message for each configured Reviewer role
        (4) CONFIGURED REVIEWER SUBAGENTS: DISPATCH EVERY ROLE
                                               |
       +---------------+---------------+-------+-------+---------------+---------------+
       |               |               |               |               |               |
       v               v               v               v               v               v
+-------------+ +-------------+ +-------------+ +-------------+ +-------------+ +-------------+
| ARCHITECTURE| | DELTA       | | CLAUDE      | | CODEX       | | TEST        | | DOCS        |
|             | | PROTOCOL    | | MAINTAINER  | | MAINTAINER  | | COVERAGE    | |             |
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
             | Python computes advisory status from recorded review results.     |
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

The orchestrator dispatches every configured Reviewer subagent, including Docs. Before returning
an orchestrator report for publication, it must receive completed results from at least one
maintainer and one other Reviewer subagent. A failed reviewer gets one retry after the other
in-flight reviewers finish. The orchestrator waits for every dispatched reviewer to finish or
exhaust that retry, and for any required Checker result. It lists reviewers still unavailable
after retry in the Summary.

These publication requirements are model instructions. After publication, Python independently
records whether every configured Reviewer result and required Checker decision arrived. Missing
results leave the execution record incomplete even when the report met the publication quorum.
See [Review execution record](#review-execution-record).

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
of the child. If the resolver cannot establish the review base, the workflow stops before
starting model sessions and creates a neutral Check Run explaining the range-selection problem.
For ambiguous ancestry, it also posts the repair comment described below.
[`resolve_review_base`](../review_scope.py) selects the boundary and logs its decision.

A **patch excerpt** is the leading portion of the fetched PR change patch inserted into task
messages. Selecting the commit range happens before applying the excerpt's byte limit.
`pr_change_patch_bytes` in the workflow holds the fetched patch; `subagent_patch_excerpt_text`
holds the excerpt included in subagent task messages. Neither a fetched patch nor a model's
successful return proves every changed line was examined.

### When stack ancestry is ambiguous

If the declared parent PR's ancestry cannot establish this PR's review base,
`review_scope.resolve_review_base()` raises `AmbiguousStackRange` with the parent PR number,
reason and suggested repair. The failed review-range resolution stops the workflow before it
starts model sessions. An always-run step calls [`scope_publication.py`](../scope_publication.py)
to post one repair PR Conversation comment per PR head commit. The comment names the declared
parent PR and asks the author to rebase this PR's branch onto that parent PR's current head,
push the branch and comment `/review inline`. The `AI Review Summary` Check Run shows the same
repair text with GitHub conclusion `neutral`. No orchestrator report or final findings are
produced.

Reruns reuse the existing GitHub Actions bot comment, updating it if the parent declaration or
repair instructions change. Each run separately records the PR number, PR head commit, declared
parent PR number, reason, workflow URL and comment URL when confirmed in
`/tmp/review_scope_outcome.json`. The Actions run retains that outcome as a scope artifact for
seven days. Invalid descriptions, empty review ranges and API failures keep their own
diagnostics and produce no ancestry repair comment.

## Input limits

[`diff_coverage.py`](../diff_coverage.py) prepares independent patch excerpts from the PR change
patch. Byte limits measure UTF-8 text, not characters or model tokens.

| Input | Limit | What the limit includes |
|---|---|---|
| Patch excerpt in each subagent task message, including the Checker | 100,000 bytes | Leading PR change patch bytes in Git's file order. |
| Patch excerpt in the initial orchestrator task message | Up to 80,000 bytes | Patch text, reduced further when the complete initial task message has less room. |
| Complete initial orchestrator task message | 120,000 bytes | Run-specific instructions, PR metadata, description excerpt, earlier AI review text, changed paths and patch excerpt. |
| PR description excerpt | 8,192 bytes | Author text after recognized generated GitStack navigation is removed. |

`prepare_review_inputs()` prepares the subagent patch excerpt. It records every changed path and
its byte interval in `/tmp/diff_coverage.json`, marking each file section fully included, partly
included or omitted. These measurements describe the supplied patch excerpt; they do not prove
which changed lines a model examined or whether it found every defect.

`build_orchestrator_task_message()` reserves the other task-message text and complete changed-path
list before adding the orchestrator's patch excerpt. If the path list cannot fit,
`list_changed_paths` reads it in pages from `/tmp/ai_review_changed_paths.json`. A source-file read
provides context without expanding the changed lines eligible for new findings.

Subagent task messages contain assignments, shared PR task text and optional maintainer guidance.
The task builder applies no combined 120,000-byte limit to those messages. Role instructions,
tool definitions and later tool results are outside the initial task-message limits. Review-range
selection reads the complete original PR description before excerpt preparation.

If the subagent patch excerpt is truncated, the main AI review comment reports file counts between
its reviewed-commit line and Assessment Summary. The `AI Review Summary` Check Run and workflow log list
partly included and omitted paths, selected commit SHAs and supplied/total bytes. An uncut patch
adds no truncation notice.

The 120,000-byte guard protects the single `omnigent run -p` command argument. On Linux with
4 KiB pages, one argument is limited to 131,072 bytes including its trailing NUL. Separate role
instructions, tool definitions and later conversation are outside this initial-message guard.
It is not the model's context-window limit. Patch excerpt limits are input-size policies, without a
measured review-quality optimum established by this workflow.

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
`artifact` mode, it uploads the report and supporting files listed below. These modes create no main AI review comment or finding comments.
The ancestry repair step described above runs independently of output mode.

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
| `/tmp/review_observations.jsonl` | `observe_review()` | Execution-record builder reads filtered dispatch, session and result-event metadata. |
| `/tmp/review_accounting.json` | Validation step | Execution-record builder reads the orchestrator's consolidation outcomes. |
| `/tmp/review_execution.json` | Execution-record step | Check Run displays its tables; Actions retains the record for seven days. |
| `/tmp/review_status.json` | Status computation | Actions retains the advisory status, counted findings and unmet status requirements. |
| `/tmp/review_scope_outcome.json` | Range resolver and repair-comment publisher | Actions retains the skipped-review outcome and confirmed repair-comment URL for seven days. |

## Review execution record

The orchestrator dispatches every configured Reviewer subagent. A specialist with no applicable
concern returns a Reviewer result with an explicit empty candidate-finding list. Each candidate
finding receives a reviewer-local ID such as `C1`. `docs-reviewer/C1` and
`maintainer-codex-reviewer/C1` identify different concerns. Python uses that pair to track each
candidate finding through checking and final selection.

The orchestrator sends all Blocker and Should Fix candidate findings in one Checker assignment.
Each Reviewer subagent/ID pair receives a distinct batch ID such as `check1`. The Checker result
contains a decision for every batch ID. If that candidate set is empty, the orchestrator skips
the Checker subagent. Nits bypass checking. The Checker result records each batch ID and its
`decision` in a `decisions` list. `CONFIRMED` means that the Checker supports the claim; it does not establish that a GitHub comment exists. A `CONTESTED` decision is a completed
decision. Its candidate finding can be dropped or retained only as a nonblocking final finding.

The orchestrator returns a consolidation outcome for every candidate finding, including Nits:
`retained` keeps it, `downgraded` lowers its severity, `merged` combines it with another concern,
and `dropped` removes it with a reason. The other outcomes name a final finding ID. For example,
`docs-reviewer/C1` and `maintainer-codex-reviewer/C1` can both map to `ShouldFix1`, with a reason
explaining the combination. Python then posts or reuses one comment and counts one final finding.

[`observe_review()`](../review_evidence.py) records dispatches, accepted session IDs and subagent
result events from Omnigent without modifying the tool calls. A **subagent attempt** is one
dispatch to a Reviewer or Checker session; a retry is a separate attempt. A result event can
contain a completed Reviewer result, a completed Checker result or a failure. The observer
records when it receives each event. It does not know the subagent's actual execution end time.
An empty inbox, the overall process exit or an orchestrator statement cannot establish that a
Reviewer or Checker result arrived.

After finding publication, [`build_execution_record()`](../review_evidence.py) matches result events to
accepted session IDs and configured roles. It checks each candidate finding's Checker decision
when required, consolidation outcome and final finding's confirmed comment URL or selected
summary/artifact output. Missing or conflicting events, mappings or publication results leave
the execution record incomplete. A complete record requires a valid Reviewer result from every
configured Reviewer subagent.

The orchestrator report follows a separate publication rule: at least one maintainer and one
other Reviewer subagent must return completed results. Every dispatched subagent must finish or
exhaust its retry, and every required Checker result must arrive. A report can satisfy that rule
while the execution record remains incomplete because a configured Reviewer stayed unavailable.
The execution record reports that gap after publication; it does not suppress those comments.

The execution artifact excludes full model responses, environment values and full tool payloads.
[`history_evidence()`](../review_history.py) separately records earlier final Blockers from
trusted bot comments and the commits they reviewed. Failed history requests, missing pages,
clipped text or unknown Blocker origins leave history incomplete. An earlier final finding does
not establish that the concern still applies to a different commit.

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

## Advisory status for the reviewed commit

[`compute_review_status()`](../review_status.py) reads the patch-excerpt coverage record,
Reviewer results, Checker decisions, candidate-to-final mappings and finding output records.
PR comment modes record confirmed new or reused finding-comment URLs. `summary` and `artifact`
modes record the selected report placement without comment URLs or delivery claims. Missing required
records take precedence over finding counts.

| Status in the comment/check report | Meaning |
|---|---|
| `NO VERDICT` | Required input, result, finding-mapping, earlier-review-history or finding-output records are incomplete, or a Checker decision remains contested. |
| `NOT APPROVED` | Those records are complete and the counted findings include a Blocker. |
| `APPROVED WITH FEEDBACK` | Those records are complete, with Should Fix or Nit findings but no Blocker. |
| `APPROVED` | Those records are complete and contain no counted findings. |

The counts include the current orchestrator report's final findings and any earlier run's final
Blockers for the same commit that the current report omits. A Blocker on an earlier commit
without a matching current result gives a linked `NO VERDICT` reason and stays outside current
counts. Reusing a finding's comment does not remove the finding from the count. These statuses
do not submit a GitHub review or replace human approval. The `AI Review Summary` Check Run
always has GitHub conclusion `neutral`.

The status record's `status_requirements_met` field is true when the patch-excerpt, execution,
history and publication records satisfy the status rules and no Checker decision remains
contested. A contested Checker decision is a completed decision that prevents a decisive advisory
status. Patch-excerpt coverage separately measures the PR change patch bytes supplied to subagent
task messages. These records do not prove that every changed line was examined.

`publish_review_status()` updates this run's existing main AI review comment in PR comment modes.
It returns without posting a PR comment in `summary` and `artifact` modes. The command writes
`/tmp/review_status.json`, adds status data to `/tmp/review_execution.json`, and rebuilds the
full Check Run report from the counted findings. The seven-day execution artifact includes
`review_execution.json`, `/tmp/diff_coverage.json` and the complete `/tmp/check-summary.txt`.
If the report exceeds GitHub's display limit, the Check Run links to the complete artifact.

### Main AI review comment format

[`format_review_summary()`](../review_publish.py) is the single renderer for the main comment.
The orchestrator supplies the assessment summary and final finding text. Python supplies counts, links,
the reviewed commit and the layout below. Angle-bracket placeholders stand for generated text;
Hidden bot identifiers are omitted from this example. When all status requirements are met, the
status line says `Status requirements met`. The Unmet status requirements disclosure appears
only when a requirement is unmet.

```markdown
## 🤖 AI Review 🧐 - <advisory status>

**🛑 2 Blockers · 🔧 1 Should Fix · 💡 2 Nits**

Status requirements unmet

<details><summary>Unmet status requirements</summary>

- <specific missing input, reviewer task result, checker decision or comment URL>

</details>

<details><summary>Assessment Summary</summary>

<one paragraph giving an overall assessment without repeating finding details>

</details>

<details><summary>Review Summary</summary>

- **[Blocker 1]** [<one-line claim>](<finding comment URL>)
- **[Blocker 2]** [<one-line claim>](<finding comment URL>)
- **[Should Fix 1]** [<one-line claim>](<finding comment URL>)

</details>

<sub>Reviewed `<short SHA>` · [Full AI review → Checks](<PR checks URL>)
· [Workflow run](<workflow run URL>)</sub>
```

`Review Summary` links every counted Blocker and Should Fix finding, including reused comments
and retained earlier final Blockers. It is absent when there are none. Nits count in the headline
and have individual comments, but no rows in that disclosure. All disclosures start closed.
Missing required records produce `NO VERDICT` with an Unmet status requirements disclosure.
Known Blockers still appear in the count and Review Summary. A posting failure can also show
`Finding counts unavailable.` in place of the count line. The Check Run includes all counted
findings, including final findings supplied only in structured model output.

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
<sub>Raised by: <reviewer names> · Reviewed `<short SHA>` · [AI Review](<main URL>)</sub><br>
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
| `${candidate_results}` | Explicit candidate IDs and severities, including empty returns | Every Reviewer subagent |

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

`config.yaml` checks the per-turn dispatch limit and purpose, records the original dispatch,
then appends the child task body. The observer returns only ALLOW and cannot rewrite arguments
or bypass either guard. Omnigent passes a rewritten dispatch to later policies without its tool
name, so the appending hook remains last.

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
