<!--
A good description says why the change exists, what behavior changes, and how you know it works,
in that order. Full guide with examples, for people and agents: AGENTS/pr_description.md.
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
Optional. Give the reviewer the bigger picture: how this change fits the existing flow and, where it
helps, a visual of how data moves through it. Use it for changes in a shared path (e.g. snapshot
load, log replay, scan, commit). Show where the change is reached from, what data enters and leaves
at that point (before and after), and which phase owns it. An indented tree of up to 25 lines, a
short sequence diagram, or three sentences; mark what changed. Not: function bodies or a file list.
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
Optional. Delete if none. Only decisions the code does not show: why this approach, what you
rejected and why, the invariant the change relies on.
-->

## How do you know it works?
<!--
Which behaviors and edge cases did you verify, and what do the results establish? Distinguish
coverage added from validation run, and mention material failures or gaps. Include test names or
reproduction steps when they help explain unusual validation or reproduce a relevant result.
Not: routine check inventories, passing-test totals, or "all tests pass".
-->
