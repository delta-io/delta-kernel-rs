# Review ideas by category

delta-io/delta-kernel-rs: 2026-07-12T04:44:29Z through 2026-11-12T04:44:29Z.

Selected source-cited ideas for downstream integration review. Counts describe cited evidence, not an exhaustive semantic classification. Human ideas need no emoji feedback or recurrence. GitHub Actions scores stay in their own lane.

## Tests

### users

- [cross-engine-null-semantics-need-execution-tests](users/recommendations.md#cross-engine-null-semantics-need-execution-tests-correctness): When lowering Kernel predicates to another engine, execute null-valued, negated, and nested cases instead of assuming the target engine shares Kernel semantics. Target: `datafusion-executor/AGENTS.md`; 1 cited PRs; add; idea.
- [validate-backend-request-paths](users/recommendations.md#validate-backend-request-paths-testing): Test storage changes against the backend request paths they alter, including pagination continuations and fallback capability paths. Target: `default-engine/AGENTS.md`; 1 cited PRs; add; idea.
- [assert-a-distinguishing-oracle](users/recommendations.md#assert-a-distinguishing-oracle-correctness): Choose fixtures and assertions that would fail for the plausible incorrect implementation being guarded against. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 6 cited PRs; change; idea.
- [reuse-test-infrastructure](users/recommendations.md#reuse-test-infrastructure-testing): Search existing test helpers and public domain types before adding a custom test engine, enum, or fixture framework. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 6 cited PRs; change; idea.
- [bound-test-matrix-complexity](users/recommendations.md#bound-test-matrix-complexity-testing): Parameterize shared behavior while keeping independent contracts and meaningful combinations visible. Target: `AGENTS.md`; 4 cited PRs; change; idea.
- [minimize-fixtures-and-redundant-coverage](users/recommendations.md#minimize-fixtures-and-redundant-coverage-testing): Remove setup that cannot affect the asserted behavior, and use existing lower-level coverage to keep integration tests focused. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 4 cited PRs; change; idea.
- [assert-executed-values-and-schema](users/recommendations.md#assert-executed-values-and-schema-testing): For translated expressions and plans, execute representative inputs and compare readable results plus the output schema. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 3 cited PRs; change; idea.
- [put-coupled-inputs-and-expectations-in-cases](users/recommendations.md#put-coupled-inputs-and-expectations-in-cases-testing): Each named case should state the logical input and expected result together, removing dispatch that derives both from a case label. Target: `AGENTS.md`; 3 cited PRs; change; idea.
- [cover-semantic-degenerate-inputs](users/recommendations.md#cover-semantic-degenerate-inputs-correctness): Choose boundary cases from the operation semantics: empty, all-null, mixed-null, and nested inputs often differ despite sharing one nominal type. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 2 cited PRs; change; idea.
- [test-lazy-composition-and-state-transitions](users/recommendations.md#test-lazy-composition-and-state-transitions-testing): Exercise the changed public path across lazy pulls, cache states, and in-flight transitions, asserting the contract the composed API actually promises. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 2 cited PRs; change; idea.
- [test-cross-path-equivalence](users/recommendations.md#test-cross-path-equivalence-correctness): When a change promises equivalence to another public path, compare both paths on the same state and inputs instead of only checking a hand-written expected set. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 1 cited PRs; change; idea.
- [extend-existing-rstest-flows](users/recommendations.md#extend-existing-rstest-flows-testing): When new cases share setup, action, and assertions with an existing test, extend that rstest instead of creating a parallel test. Target: `AGENTS.md`; 10 cited PRs; unspecified; idea.
- [independent-rstest-axes](users/recommendations.md#independent-rstest-axes-testing): Use rstest identities for meaningful independent input axes rather than loops hidden inside a test. Target: `AGENTS.md`; 4 cited PRs; unspecified; idea.

### github-actions

- [bot-assert-semantic-results](github-actions/recommendations.md#bot-assert-semantic-results-testing): Bot reviews often request assertions on values and dispatch, beyond observing no extra I/O or a successful return. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 5 cited PRs; add; bot-feedback-candidate.
- [bot-check-existing-coverage](github-actions/recommendations.md#bot-check-existing-coverage-testing): Trace enabled CI features and caller coverage before demanding another test or CI job. Target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md`; 2 cited PRs; change; bot-feedback-candidate.
- [bot-preserve-test-type-contracts](github-actions/recommendations.md#bot-preserve-test-type-contracts-style): Retain type annotations that intentionally make a test enforce a static API contract. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 1 cited PRs; change; single-pr-bot-idea.

## Comments

### users

- [make-deferred-limitations-actionable](users/recommendations.md#make-deferred-limitations-actionable-style): When deferring a real limitation, leave a local explanation of the missing behavior and a tracking issue or clear trigger for revisiting it. Target: `AGENTS.md`; 4 cited PRs; add; idea.
- [qualify-contracts-and-test-descriptions](users/recommendations.md#qualify-contracts-and-test-descriptions-correctness): State precisely which inputs, branches, and implementations a documented guarantee covers; describe tests at the boundary they actually exercise. Target: `AGENTS.md`; 5 cited PRs; change; idea.
- [document-observable-api-not-internal-plumbing](users/recommendations.md#document-observable-api-not-internal-plumbing-style): Keep API documentation centered on observable behavior and caller decisions; place algorithm and implementation rationale beside the implementation. Target: `AGENTS.md`; 3 cited PRs; change; idea.
- [explain-nonlocal-invariants-and-policies](users/recommendations.md#explain-nonlocal-invariants-and-policies-design): Document non-obvious invariants and asymmetric policies at their owning implementation so later cleanup does not erase intentional behavior. Target: `AGENTS.md`; 3 cited PRs; change; idea.
- [show-small-input-output-examples](users/recommendations.md#show-small-input-output-examples-style): Use a small concrete input/output example when terminology or a transformation is difficult to infer from the signature. Target: `AGENTS.md`; 2 cited PRs; change; idea.
- [comments-explain-intent-once](users/recommendations.md#comments-explain-intent-once-style): Use names and one authoritative explanation instead of repeated inventories, temporal notes, or comments restating the test. Target: `AGENTS.md`; 9 cited PRs; unspecified; idea.

### github-actions

- [bot-docs-match-conditional-behavior](github-actions/recommendations.md#bot-docs-match-conditional-behavior-style): Bot reviews repeatedly note unconditional documentation that overlooks a conditional branch. Target: `AGENTS.md`; 5 cited PRs; add; bot-feedback-candidate.

## Design/structure

### users

- [feature-support-is-operation-specific](users/recommendations.md#feature-support-is-operation-specific-correctness): When declaring a table feature supported, enumerate CREATE, ALTER, and write behavior and verify schema/feature requirements in both directions. Target: `kernel/src/table_features/AGENTS.md`; 4 cited PRs; add; idea.
- [make-fallback-and-enum-coverage-explicit](users/recommendations.md#make-fallback-and-enum-coverage-explicit-correctness): Require explicit decisions where falling through an enum or type classification can silently skip validation or discard work. Target: `AGENTS.md`; 3 cited PRs; add; idea.
- [checkpoint-format-is-not-filename](users/recommendations.md#checkpoint-format-is-not-filename-correctness): Check protocol-defined checkpoint contents independently from filename shape. Target: `kernel/src/checkpoint/AGENTS.md`; 1 cited PRs; add; idea.
- [snapshot-diagnostics-without-success-overhead](users/recommendations.md#snapshot-diagnostics-without-success-overhead-design): Keep useful failure diagnostics while avoiding large captures and clones on successful snapshot construction. Target: `kernel/src/snapshot/AGENTS.md`; 1 cited PRs; add; idea.
- [remove-unhelpful-helper-layers](users/recommendations.md#remove-unhelpful-helper-layers-design): Remove wrappers and optional machinery that add navigation or configuration without a distinct responsibility. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 5 cited PRs; change; idea.
- [reuse-existing-state](users/recommendations.md#reuse-existing-state-design): Extend the existing representation before introducing overlapping types or recomputing already-owned state. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 5 cited PRs; change; idea.
- [errors-identify-the-failed-condition](users/recommendations.md#errors-identify-the-failed-condition-correctness): Use the established error category and diagnostic context for the condition that actually failed, rather than a generic or misleading explanation. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 4 cited PRs; change; idea.
- [place-configuration-with-its-owning-lifecycle](users/recommendations.md#place-configuration-with-its-owning-lifecycle-design): Attach configuration and derived views to the object or lifecycle stage that owns the decision, avoiding independent knobs that can drift apart. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 3 cited PRs; change; idea.
- [preserve-lazy-dataflow-resource-bounds](users/recommendations.md#preserve-lazy-dataflow-resource-bounds-design): Before simplifying an iterator or batch pipeline, trace which work becomes eager, duplicated, or discarded and preserve its intended memory and I/O bounds. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 3 cited PRs; change; idea.
- [validate-invariants-at-entrypoints](users/recommendations.md#validate-invariants-at-entrypoints-correctness): Make required validation part of the entry point that establishes the invariant, and share the same checks across alternate construction paths. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 3 cited PRs; change; idea.
- [engine-interfaces-hide-storage-layout](users/recommendations.md#engine-interfaces-hide-storage-layout-design): Keep logical Engine interfaces independent of Arrow-specific storage layout and terminology; localize physical details to the implementation. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 2 cited PRs; change; idea.
- [visibility-follows-real-consumer-needs](users/recommendations.md#visibility-follows-real-consumer-needs-design): Choose API visibility and accessors from actual consumer needs and exposed invariants rather than applying a blanket private-or-public rule. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 2 cited PRs; change; idea.
- [keep-pr-concerns-focused](users/recommendations.md#keep-pr-concerns-focused-design): Separate independent preparatory cleanup or FFI plumbing when it makes the substantive change easier to review. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 5 cited PRs; unspecified; idea.
- [simplify-local-control-flow](users/recommendations.md#simplify-local-control-flow-style): Use direct local control flow or construct shared values once when branches merely repeat the same operation. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 3 cited PRs; unspecified; idea.
- [ffi-representation-follows-ownership](users/recommendations.md#ffi-representation-follows-ownership-design): Choose borrowed C-compatible views or owned handles from the actual retention lifetime, rather than adding ownership and release steps to call-scoped data. Target: `ffi/AGENTS.md`; 2 cited PRs; unspecified; idea.

### github-actions

- [bot-preserve-schema-validity](github-actions/recommendations.md#bot-preserve-schema-validity-correctness): Preserve declared field metadata/nullability and parent validity when rebuilding columnar values. Target: `AGENTS.md`; 2 cited PRs; add; bot-feedback-candidate.
- [bot-duplicate-derived-state](github-actions/recommendations.md#bot-duplicate-derived-state-design): Before introducing a second state/schema decision or encoding, identify the existing owner and compare the semantic and runtime costs of reuse. Target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md`; 5 cited PRs; add; bot-feedback-candidate.
- [bot-preserve-static-constructor-contracts](github-actions/recommendations.md#bot-preserve-static-constructor-contracts-correctness): Before replacing a manual constructor with a derive, check caller-visible capabilities such as const evaluation and visibility. Target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md`; 1 cited PRs; add; single-pr-bot-idea.
- [bot-narrow-low-value-scope](github-actions/recommendations.md#bot-narrow-low-value-scope-design): Scope advice to a concrete defect or maintenance cost before asking for broader API or extreme-input support. Target: `.github/omnigent/reviewer/REVIEW.md`; 3 cited PRs; change; bot-feedback-candidate.
- [bot-validation-trust-boundaries](github-actions/recommendations.md#bot-validation-trust-boundaries-correctness): Bot reviews flag intrinsic checks that are skipped by a state gate or a direct deserialization path. Target: `AGENTS.md`; 2 cited PRs; unspecified; defer.
