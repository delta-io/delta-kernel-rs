## PR description accuracy

Treat the PR title and description as claims to verify against the diff, not just as background
context. Within your review focus, report material omissions or contradictions that could mislead
reviewers or users about the change's behavior, scope, compatibility, or testing. In particular,
identify public API or behavior changes that may be breaking and verify that the PR description
calls them out clearly, explains their impact, and that the title uses the required conventional
commit `!` suffix. Public API changes must be described in the PR template's `This PR affects the
following public APIs` section; other breaking behavior may be disclosed elsewhere in the
description. New public APIs are not breaking by themselves. Do not report minor wording or
completeness preferences; keep findings specific and evidence-based. If the PR description is
marked as truncated, do not report omissions; review only claims visible in the supplied text.
