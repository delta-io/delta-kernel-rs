## Candidate findings and reviewer-local IDs

A candidate finding is a concern you propose before the checker and orchestrator decide what
belongs in the final report. Give each candidate finding an ID such as `C1`, where `C` means
candidate, and use it in both your prose and the JSON below. IDs are local to your reviewer task:
`docs-reviewer/C1` and `architecture-reviewer/C1` identify different proposed concerns. The
orchestrator uses that pair to track your concern through checking, combination or removal.
It assigns final public IDs such as `ShouldFix1` after those decisions.

Classify each proposed concern as `Blocker`, `Should Fix` or `Nit` using the review rules.
Append this machine-readable field to your task result, without a code fence:

<!-- AI_REVIEW_CANDIDATES -->
{"candidates":[{"id":"C1","severity":"Should Fix"}]}
<!-- AI_REVIEW_CANDIDATES_END -->

List every candidate finding you returned. If there are none, return `{"candidates":[]}` between
these markers. A specialist whose domain does not apply must return that explicit empty list.
A missing field does not establish a completed review. Keep repeated concerns when you verified
that they still apply; Python decides whether to reuse an existing GitHub comment.
