# PR description guide

Read this guide and the [PR template](../.github/PULL_REQUEST_TEMPLATE.md). When drafting or
rewriting a section, read its linked examples.

Prepare human and AI reviewers before they open the diff: explain why the change is needed, how
the affected components and data fit together, and which design decisions matter to the review.
AI tools may draft this context under the [AI Policy](../AI_POLICY.md); the author reviews and owns
the result.

## General guidance

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

Check each section against [the section guidance](#template-sections).

- Required headings present: `## Why`, `## What changes`, and `## How do you know it works?`.
- Optional sections deleted when empty.
- Public API section present for public API changes, including behavior changes.
- Breaking changes called out clearly, with impact explained and `!` in the title.
- Every test name in the testing section exists in the diff or the repository.
- Follow [Length and format](#length-and-format).
- Why states what happens today and what it costs; What changes states the behavior after.
- Testing distinguishes coverage added from validation run, and states gaps.

## Template sections

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
the mechanism.

[Annotated examples](pr_description_examples.md#why).

### Section `## What changes`

Say what X does after this PR and what the benefit is. Describe the behavior and data flow a caller
or reader would notice, leading with the user-visible or API-visible change. Mention every material
change: a behavior change, a flag, a removed capability, a new path. For a PR that fixes several
independent problems, write one short block per problem: the problem, its cause, the fix.

Not: a file list, a walk through the diff, or a line-by-line explanation; the diff and rustdoc do
that.

[Annotated examples](pr_description_examples.md#what-changes).

### Section `### Call path and data flow`

Optional. Include this section when a reviewer would otherwise need to reconstruct how the change
fits the existing flow. Show where the change is reached from, what data enters and leaves at that
point, and which component or phase owns the data. Include unchanged callers and consumers when
they explain the behavior or constraints of the change.

Choose the view that makes the change easiest to check. A before/after example shows how a log
cover, action, or null mask changes. A short call tree or sequence diagram shows how components
interact. Prose can explain either. Mark what changed and keep enough surrounding context to make
the path understandable; avoid pasted function bodies and file inventories.

[Annotated examples](pr_description_examples.md#call-path-and-data-flow).

### Section `### This PR affects the following public APIs`

Optional. Keep this heading text exactly; tooling looks for it. Delete the section if the PR
changes no public API. For each change give three things: the exact type, method, or C symbol that
changes; what callers do instead (replace a match, pass the new argument, regenerate bindings); and
why the break is worth it. If old code still compiles but silently stops matching, say so. For an
additive API, say in one clause why it is not breaking. A behavior change to an existing public API
belongs here even without a signature change. Other breaking behavior may be disclosed elsewhere
in the description. A breaking change needs `!` in the title; CI adds the `breaking-change` label
from `cargo semver-checks`.

[Annotated examples](pr_description_examples.md#public-api-changes).

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

[Annotated examples](pr_description_examples.md#design-notes).

### Section `## How do you know it works?`

Say which behaviors and edge cases you verified and what the results establish. Distinguish
coverage added from validation actually run, and mention material failures or gaps. When naming a
specific test or assertion, link to its source so reviewers can inspect the coverage. Include
commands or reproduction steps when they help reproduce unusual validation or a relevant result.
For each sentence ask: would this failure mean the change is wrong? If not, cut it.

Not: routine check inventories, passing-test totals, routine command lists, lint, CI status, or
"all tests pass". "Existing tests" is acceptable only for a behavior-preserving refactor, and then
name the tests that cover the changed path.

[Annotated examples](pr_description_examples.md#how-do-you-know-it-works).
