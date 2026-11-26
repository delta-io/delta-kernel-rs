<!--
Prepare human and AI reviewers: explain why the change exists, what behavior changes, how data
moves through the affected components, the relevant design decisions, and what was verified.
Authoring guide and links to examples: AGENTS/pr_description.md.
Sections marked optional can be deleted; the others are required.
-->

## Why
<!--
What happens today, or what can callers not do? What does that cost or get wrong, and who is
affected? One to three sentences. Link the issue or the protocol section. The form depends on the
kind of change (fix, feat, refactor, docs, ci, perf); see the guide. Not: a restatement of the
title or a pointer to another PR.
-->

## What changes
<!--
What does X do after this PR, and what is the benefit? Describe the behavior and data flow a caller
or reader would notice, leading with the user-visible or API-visible change.
Not: a file list or a walk through the diff.
-->

### Call path and data flow
<!--
Optional. Include when a reviewer would otherwise need to reconstruct how the change fits the
existing flow. Show the entry point, what data enters and leaves the changed step (before and
after), who owns it, and who consumes the result. Use prose, a worked example, or a short diagram;
include unchanged components when they explain the change. Avoid function bodies and file lists.
-->

### This PR affects the following public APIs
<!--
Optional. Delete if none. For each change: the exact type, method, or C symbol; what callers do
instead; why it is worth it. If old code still compiles but stops matching, say so. For an
additive API, say why it is not breaking. Breaking changes need `!` in the title; CI adds the
`breaking-change` label. Not: type names without the migration step.
-->

### Design notes
<!--
Optional. Explain material design decisions, constraints, invariants, or scope choices that help a
reviewer evaluate the approach. Say why this approach fits the problem and why relevant alternatives
were rejected. Delete if there is no material rationale to add.
-->

## How do you know it works?
<!--
Which behaviors and edge cases did you verify, and what do the results establish? Distinguish
coverage added from validation run, and mention material failures or gaps. Include test names or
reproduction steps when they help explain unusual validation or reproduce a relevant result.
Not: routine check inventories, passing-test totals, or "all tests pass".
-->
