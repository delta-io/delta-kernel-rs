# Review-derived ideas: github-actions

2026-07-12T04:44:29Z through 2026-11-12T04:44:29Z.

Separate focused GitHub Actions pass over the four-month export, extending earlier selected themes with positively rated older findings and explicitly interpreted thread replies. Bot claims and their feedback remain separate from user-review patterns. Source collection covers the complete eligible window; semantic annotations are selected examples, not an exhaustive labeling. Three previously unweighted themes now have direction-specific support. One new constructor theme is a single-PR provisional idea. Positive replies support correction only when they endorse narrowing or changing the original advice.

Source lane: github-actions. Only GitHub Actions receives emoji/reply scores. Other lanes retain one-PR ideas; recurrence and existing-guidance coverage are annotations, not admission gates. Every idea has a concrete integration target and code paths, including proposed nested AGENTS.md files for local patterns. PR-author replies and older comments provide context without inflating recent-reviewer recurrence. Counts are cited lower bounds, not exhaustive frequencies or proof of correctness. Add/change/remove labels are proposals for a later integration agent.

Existing guidance classifications are semantic assessments; source hashes and anchors identify any files assessed. Proposed new files have no existing contents.

## Tests

### Possible additions

#### bot-assert-semantic-results (testing)

Bot reviews often request assertions on values and dispatch, beyond observing no extra I/O or a successful return.

Scope: Cached-sidecar plans, replay tie selection, and null callback dispatch.

Exceptions: Check existing caller coverage first, and assert meaningful outcomes rather than exact incidental message text.

Cited evidence: 5 distinct PRs; 1 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:85` (existing file; new).

A positively rated diagnostic-regression finding and an implemented validation-test request supply feedback-backed bot evidence. This is a targeted addition proposal to reconcile with the existing test-quality rules.

Applicable code: `kernel/src`, `ffi/src`, `.github/omnigent`.

Bot feedback ranking: add=5, remove/change=0. Mixed feedback: False.

- `inline:3981963341`: +1=0, -1=0, eyes=0; positive replies=1, corrections=0, scope replies=0; unclassified replies=1.
- `inline:4027872776`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4531648303`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4544196650`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4544660682`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:3982269421](https://github.com/delta-io/delta-kernel-rs/pull/3299#discussion_r3982269421) @chiinlquah (implemented): Fixed in ce638294. CLI-level tests now cover malformed history fallback, mixed-type trusted logins, and all publication output files.

Proposed wording:

```markdown
For a behavior fix, identify the smallest regression assertion that would fail before the change. Observe the value, schema, dispatch, or emitted diagnostic/output that carries the contract, and check existing caller tests before requesting another test.
```

Citations:

- [inline:4531648303](https://github.com/delta-io/delta-kernel-rs/pull/3831#discussion_r4531648303) @github-actions[bot] (bot; support): **Nit3** The cached-sidecar branch is only checked indirectly (no `DynamicScan`, no extra I/O). Nothing verifies that the planned paths equal `cached.sidecars` or that each file carries the root version in `file_constants`; wrong constants would break `_version` ordering without a test failing. This call also repeats the cache lookup done in `MetadataReplayBase::try_new`, which is correct for a frozen segment but unexplained. Raised by: test-coverage-reviewer, maintainer-claude-reviewer. Suggested fix: compare the cached-sidecar plan with the plan from `checkpoint_hint_version_tagged_sidecar_scan_files` and add a one-line comment on the second lookup.
- [inline:4544660682](https://github.com/delta-io/delta-kernel-rs/pull/3607#discussion_r4544660682) @github-actions[bot] (bot; support): **Nit3** No test covers the stated non-plan tie change end to end. This test supplies tags by hand and computes `expected_content_root` with the same comparison the code uses; `assert_replay_resolution` compares only `action.version()`. Ties and non-monotonic versions are invalid under the RFC and the resolve-by-commit-order behavior is undocumented. Raised by: test-coverage-reviewer, maintainer-claude-reviewer, maintainer-codex-reviewer, delta-protocol-reviewer. Suggested fix: add a both-engine replay test with two actions that share `checkpointMetadata.version` but have distinct content-root paths and assert which one is captured. Make the expected content root a `#[case]` value and add a
- [inline:4544196650](https://github.com/delta-io/delta-kernel-rs/pull/3921#discussion_r4544196650) @github-actions[bot] (bot; support): **Nit4** The new Scalar::Null dispatch (decimal -> visit_literal_decimal_null, others -> visit_literal_null with a tag) has no test; the test visitor uses ignore_null/ignore_decimal_null, so a mis-wired callback would pass. Raised by: test-coverage-reviewer, maintainer-codex-reviewer. Suggested fix: add an rstest with recording callbacks for decimal, integer and variant nulls.
- [inline:4027872776](https://github.com/delta-io/delta-kernel-rs/pull/3337#discussion_r4027872776) @github-actions[bot] (bot; support): **Nit1** The fix converts a compiler panic into a proper diagnostic but ships with manual testing only. The refactor made this unit-testable: internal_api_impl(Item) -> TokenStream and make_public now use proc_macro2 types, so a test can assert on the stringified output. Without a guard, a future refactor could reintroduce the panic and CI would stay green; the existing test module references neither internal_api nor make_public. Raised by: maintainer-claude-reviewer, test-coverage-reviewer, maintainer-codex-reviewer. Suggested fix: add a test that parses `pub struct Foo;`, calls internal_api_impl, and asserts the output contains `compile_error` and still contains `struct Foo` (preserving the original item). Prefer asserting on compile_error presence over exact message text; an unsupported-item case (e.g. `impl Foo {}`) covers the other error arm. Explanation: The positively rated finding asks for a regression assertion on compile_error and the preserved source item.
- [inline:3981963341](https://github.com/delta-io/delta-kernel-rs/pull/3299#discussion_r3981963341) @github-actions[bot] (bot; support): **Nit2** The new trusted_bot_logins validation branch (raise ValueError on non-list or mixed-type input) has no unit test, yet this value gates which authors count as trusted history, so a silent regression is security relevant. Raised by: test-coverage-reviewer. Suggested fix: add a main() test that a non-list or mixed-type logins value raises, plus a test that the duplicate/skip output files are written. Explanation: The thread explicitly reports implementing tests for validation inputs and generated outputs.

### Possible changes/removals

#### bot-check-existing-coverage (testing)

Trace enabled CI features and caller coverage before demanding another test or CI job.

Scope: Missing-coverage findings against feature-gated implementations or helpers exercised through callers.

Exceptions: A downvote is a prioritization signal, not proof that the bot was wrong. Validate the claimed existing coverage against the reviewed revision.

Cited evidence: 2 distinct PRs; 1 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/test-coverage-reviewer/REVIEW.md:37` (existing file; strengthen).

A reply rejects a duplicate CI recommendation because all-features jobs already enable the feature. Narrow the coverage-search instruction rather than adding an unconditional new CI check.

Applicable code: `ffi/src`, `kernel/src`.

Bot feedback ranking: add=0, remove/change=9. Mixed feedback: False.

- `inline:4152974994`: +1=0, -1=1, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4471170426`: +1=0, -1=1, eyes=0; positive replies=0, corrections=1, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:4471209676](https://github.com/delta-io/delta-kernel-rs/pull/3500#discussion_r4471209676) @jtuglu1 (corrected): The existing CI already enables this feature: the test matrix runs `cargo nextest run --locked --workspace --all-features` (build.yml, all-features tests), and coverage runs `cargo llvm-cov --locked --all-features --workspace`. Both include the installed-global-allocator test and feature-on getter arms. The FFI MSRV build also uses `--all-features`. A separate duplicate step is unnecessary.

Existing wording to change:

```markdown
Read the existing tests to understand what is already covered.
```

Proposed wording:

```markdown
Read existing tests and relevant CI commands, including workspace membership and feature flags. Before claiming coverage is missing, check whether caller tests or an existing all-features job already exercise the path; cite the gap that remains.
```

Citations:

- [inline:4471170426](https://github.com/delta-io/delta-kernel-rs/pull/3500#discussion_r4471170426) @github-actions[bot] (bot; support): **Nit1** The `#[global_allocator]` static, the feature-on getter arms and the `compile_error!` guard are only built with `alloc-tracking`, and no CI job enables it, so a broken static or getter wiring would ship unnoticed. Raised by: test-coverage-reviewer. Suggested fix: add `cargo test --locked -p delta_kernel_ffi --features alloc-tracking -- alloc_stats` to CI. --- <sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</sub>
- [inline:4152974994](https://github.com/delta-io/delta-kernel-rs/pull/3471#discussion_r4152974994) @github-actions[bot] (bot; support): **Nit1** All reader tests go through convert_root_entries_to_add_actions, so nothing checks that convert_entries_with uses the caller's AddFieldSources or a non-root input_schema. Add a direct unit test (for example base_row_id: lit(777i64)). Also document that input_schema must contain the entry columns, since AddSelectionVisitor reads them by name. Raised by: test-coverage-reviewer, maintainer-claude-reviewer, architecture-reviewer, docs-reviewer. --- <sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</sub>

#### bot-preserve-test-type-contracts (style)

Retain type annotations that intentionally make a test enforce a static API contract.

Scope: A result-type annotation guarding an import-shadowing regression.

Exceptions: Most redundant annotations can still be removed; this is a single-PR correction and should not be described as recurring by itself.

Cited evidence: 1 distinct PRs; 1 reviewers; 1 discussions. Recurring: False. Opposing PRs cited: 0. State: single-pr-bot-idea.

Suggested action: change; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:157` (existing file; strengthen).

The downvote and corrective reply identify the annotation as a regression guard; qualify the existing blanket wording.

Applicable code: `default-engine/src`.

Bot feedback ranking: add=0, remove/change=6. Mixed feedback: False.

- `inline:4158908715`: +1=0, -1=1, eyes=0; positive replies=0, corrections=1, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:4160267144](https://github.com/delta-io/delta-kernel-rs/pull/3477#discussion_r4160267144) @Jameson-Crate (corrected): This annotation intentionally verifies that the helper returns `delta_kernel::Result`, guarding against the import-shadowing bug identified in the previous review. The one-line `.unwrap()` form compiles with either error type and would not catch that regression.

Existing wording to change:

```markdown
- **Remove redundant type annotations:** Flag `let x: Type = expr` when the type is already clear from the right-hand side (e.g. `TryFromStringSlice::try_from_slice` already returns a known type, fully-qualified paths like `crate::handle::Handle<crate::SharedExternEngine>` when the types are already imported). Let Rust's type inference do its job -- explicit annotations add noise without aiding readability.
```

Proposed wording:

```markdown
Before removing a type annotation from a test, check whether it intentionally asserts an API/error-type contract. Type inference may compile with the very wrong type the test is meant to reject.
```

Citations:

- [inline:4158908715](https://github.com/delta-io/delta-kernel-rs/pull/3477#discussion_r4158908715) @github-actions[bot] (bot; support): **Nit1** The `delta_kernel::Result<_>` annotation looks like a leftover from the old `object_store::Result` shadowing. The test module now imports `Result as ObjectStoreResult`, so `read_all_rows_helper` already returns the kernel `Result` through `super::*`. The annotation does nothing, and the sibling test uses the one-line form. Raised by: maintainer-claude-reviewer, test-coverage-reviewer. Suggested fix: use `let data = read_all_rows_helper(file_meta).await.unwrap();`. --- <sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</sub>

## Comments

### Possible additions

#### bot-docs-match-conditional-behavior (style)

Bot reviews repeatedly note unconditional documentation that overlooks a conditional branch.

Scope: Fast-path footer reads, publication behavior, and optional stats schemas.

Exceptions: Do not assume a bot assertion accurately describes the implementation.

Cited evidence: 5 distinct PRs; 1 reviewers; 5 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:330` (existing file; new).

The extended window supplies positively rated findings on two distinct PRs. Propose this concrete doc-review check for reconciliation with the generic keep-comments-current rule.

Applicable code: `kernel/src`, `ffi/src`, `release.sh`.

Bot feedback ranking: add=9, remove/change=0. Mixed feedback: False.

- `inline:3938023438`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:3974781049`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4099584633`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4496161849`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=1.
- `inline:4533021515`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Proposed wording:

```markdown
Check documented effects against the actual branch or lifecycle stage: staging is not committing, tagging is not publishing, and a default or fast path may have narrower behavior. Describe the visible guarantee and its conditions rather than copying a broader helper contract.
```

Citations:

- [inline:4099584633](https://github.com/delta-io/delta-kernel-rs/pull/3044#discussion_r4099584633) @github-actions[bot] (bot; support): **Nit1** The doc comment still says "one sidecar footer read", but the test is now an `#[rstest]` with a `without_leaf_schema` case that does zero footer reads, so it contradicts the parametrized behavior and the "fast path reads the sidecar footer only when its schema is needed" assertion message. Suggested fix: reword to reflect the conditional read, e.g. "Fast path on a manifest hint: no drain (`query_scans == 0`); the sidecar footer is read only when the leaf schema is requested." Raised by: docs-reviewer.
- [inline:4496161849](https://github.com/delta-io/delta-kernel-rs/pull/3837#discussion_r4496161849) @github-actions[bot] (bot; support): **Nit1** The doc says filesystem-managed tables require no publication and return the existing snapshot. That only holds when there is nothing to publish; staged commits on a filesystem-managed table still error. Raised by: docs-reviewer. Suggested fix: say 'Filesystem-managed tables with nothing to publish return the existing snapshot.'
- [inline:4533021515](https://github.com/delta-io/delta-kernel-rs/pull/3848#discussion_r4533021515) @github-actions[bot] (bot; support): **Nit1** Nit: the doc gives the reason but not the return contract (returns `None` when `synthesize_json` is off; schema covers all non-partition physical leaves). The method has no direct test, and the new JSON arm in `read_schema` (`synthesize_json` with no stats schema) is not covered. Suggested fix: document the contract, add tests for the `None` case and the non-indexed-column case, and a scan test with JSON-only stats asserting `add.stats` is still read. Raised by: docs-reviewer, test-coverage-reviewer, maintainer-claude-reviewer.
- [inline:3974781049](https://github.com/delta-io/delta-kernel-rs/pull/3216#discussion_r3974781049) @github-actions[bot] (bot; support): **Nit2** Doc comment says 'Commits file as this transaction's root manifest', but the function only stages the file; the write happens later in the separate commit FFI (the kernel method correctly says 'Stages'). Raised by: docs-reviewer, maintainer-claude-reviewer. Suggested fix: reword to 'Stages file to be committed as this transaction's root manifest.' Explanation: A positive reaction supports distinguishing staging a root manifest from actually committing it.
- [inline:3938023438](https://github.com/delta-io/delta-kernel-rs/pull/3066#discussion_r3938023438) @github-actions[bot] (bot; support): **Blocker3** This usage step does not publish and collides with the existing tag. The header lists the kernel post-merge step as ./release.sh tag delta_kernel, but publishing happens only in the no-arg main-branch flow (handle_main_branch), which also already tags via tag_release delta_kernel. Following the header skips publishing the kernel crates, and running tag delta_kernel after the real no-arg run hits the existing-tag guard and errors. Raised by: docs-reviewer. Suggested fix: document the no-arg ./release.sh publish-and-tag flow for the kernel, and reserve tag <crate> for the independently versioned crates. Explanation: A positive reaction supports documenting the command that actually publishes and tags.

## Design/structure

### Possible additions

#### bot-preserve-schema-validity (correctness)

Preserve declared field metadata/nullability and parent validity when rebuilding columnar values.

Scope: Schema-preserving Arrow list/struct transformations.

Exceptions: An explicit schema transformation may intentionally change these attributes; observed batch contents alone cannot establish the declared schema.

Cited evidence: 2 distinct PRs; 1 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:242` (existing file; new).

The selected positively rated findings expose a concrete transformation invariant absent from the root gotchas; this remains a separately sourced bot proposal.

Applicable code: `kernel/src`.

Bot feedback ranking: add=10, remove/change=0. Mixed feedback: False.

- `inline:4310740341`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4352304580`: +1=1, -1=0, eyes=0; positive replies=2, corrections=0, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:4353030332](https://github.com/delta-io/delta-kernel-rs/pull/3340#discussion_r4353030332) @rliao147 (endorsed): +1, please fix this one.

Feedback [inline:4356864799](https://github.com/delta-io/delta-kernel-rs/pull/3340#discussion_r4356864799) @mahesh-desu (implemented): Fixed in d05babcf: eorder_list_values now rebuilds the element field with with_data_type, so the declared nullability and metadata are preserved. The ListView test declares a nullable element field with metadata and no null elements, and asserts both survive; it fails on the previous implementation.

Proposed wording:

```markdown
When rebuilding Arrow lists or structs, preserve declared field metadata/nullability and the source parent validity unless the operation explicitly changes that contract. A batch containing no null values does not make a nullable field non-nullable.
```

Citations:

- [inline:4352304580](https://github.com/delta-io/delta-kernel-rs/pull/3340#discussion_r4352304580) @github-actions[bot] (bot; support): **Nit1** The element field's nullability is taken from `result_array.is_nullable()`, which reflects whether this batch has null elements, not the declared field. A nullable element field with no nulls becomes non-nullable, so the output schema depends on the data, and element-field metadata is dropped. `reorder_list` does the same, but the new helper extends it. Suggested fix: clone `values_field` and replace only its data type, e.g. `values_field.as_ref().clone().with_data_type(result_array.data_type().clone())`, and add a test with a nullable element field and no null elements. Raised by: maintainer-codex-reviewer, maintainer-claude-reviewe
- [inline:4310740341](https://github.com/delta-io/delta-kernel-rs/pull/3656#discussion_r4310740341) @github-actions[bot] (bot; support): **Blocker1** The action struct is rebuilt with struct_expr_from_schema, which produces Struct(fields, None), so the source action's null mask is dropped. A null `add` row either fails Arrow validation (required child like add.path is null under a valid parent) or becomes a present-but-empty action. Fix: use a nullability-aware struct expression taking validity from column([action]).is_not_null(), and add a mixed present/null action test; or document and validate a non-null precondition. Raised by: maintainer-claude-reviewer, maintainer-codex-reviewer. --- <sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</

#### bot-duplicate-derived-state (design)

Before introducing a second state/schema decision or encoding, identify the existing owner and compare the semantic and runtime costs of reuse.

Scope: Duplicated schema selection and representation fields.

Exceptions: Separate representations can enforce distinct invariants; reusing multiple accessors can add replay/I/O cost, so deduplication is not automatically cheaper.

Cited evidence: 5 distinct PRs; 1 reviewers; 6 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: add; final integration belongs to the downstream agent.

Thread context: The snapshot-accessor rewrite was implemented and accepted with an explicit caveat: it may perform three replays instead of one, judged acceptable for a non-sensitive path usually served by CRC. This is not an unconditional deduplication rule.

Integration target: `.github/omnigent/reviewer/agents/architecture-reviewer/REVIEW.md:26` (existing file; new).

Two positively rated older PRs and a separately endorsed implementation thread turn this into a bot-sourced addition idea for the existing architecture prompt. The downstream agent should merge or reject overlap with current rules.

Applicable code: `kernel/src`, `release.sh`.

Bot feedback ranking: add=7, remove/change=0. Mixed feedback: False.

- `inline:3938023447`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4010314626`: +1=0, -1=0, eyes=0; positive replies=2, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4010550924`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4051179095`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4489856075`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=1.
- `inline:4544676601`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=2.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:4010609316](https://github.com/delta-io/delta-kernel-rs/pull/3216#discussion_r4010609316) @amogh-jahagirdar (implemented): I went ahead and addrssed this by adding a get_app_ids to snapshot which already looked to be a todo on that anyways. It did simplify the control flow on scan_non_content_metadata, cc @kyli87 let me know what you think about this one. Since it did look like an existing TODO on snapshot I went ahead and added the function, but if you think it doesn't fit can go back to what i had before

Feedback [inline:4017816276](https://github.com/delta-io/delta-kernel-rs/pull/3216#discussion_r4017816276) @kyli87 (endorsed): I think this seems fine, the main tradeoff I see is that now we have to potentially do 3 total replays (one for each of set transaction, domain metadata, and checkpoint action) instead of getting all 3 in one. But this doesn't seem like a perf-sensitive path and most of the time it will come from CRC anyway.

Proposed wording:

```markdown
For a duplicated schema/state decision or encoding, cite the existing owner and the new copy. Check whether the owner can be reused without changing invariants or adding material replay/I/O cost; propose the smaller maintained representation with that tradeoff explicit.
```

Citations:

- [inline:4051179095](https://github.com/delta-io/delta-kernel-rs/pull/2995#discussion_r4051179095) @github-actions[bot] (bot; support): **Nit1** The `commit_read_schema` computed here re-derives a decision `Scan::commit_read_schema` / `Scan::skip_stats` already make in scan/mod.rs. This picks full `COMMIT_READ_SCHEMA` when `emit_json || state_info.physical_stats_schema.is_some()`, while `Scan` picks it when `!skip_stats()`; these are logical inverses that must stay in lockstep. The commit actions are read with `Scan::commit_read_schema()`, but the processor declares its `commit_transform` input from this recomputed value, so if the two ever diverge the `column_expr_ref!("add.stats")` branch in `get_add_transform_expr` would reference a column absent from the fed data (a runtime schema mismatch, not a compile error). Both mai
- [inline:4489856075](https://github.com/delta-io/delta-kernel-rs/pull/3831#discussion_r4489856075) @github-actions[bot] (bot; support): **Nit2** build_metadata_scan_plan rebuilds an Arc<CheckpointShape> by cloning each field, a hand-written Clone, and CheckpointShape duplicates state (leaf_checkpoint_schema equals info.leaf_schema; checkpoint_type is derivable from sidecars and info). Raised by: architecture-reviewer, maintainer-claude-reviewer. Suggested fix: take &Arc<CheckpointShape> in the planner or derive Clone, and document or remove the redundant fields.
- [inline:4544676601](https://github.com/delta-io/delta-kernel-rs/pull/3874#discussion_r4544676601) @github-actions[bot] (bot; support): **Nit4** The exact decimal widening rule exists in both can_upcast_to_decimal and decimal_stat_as, linked only by comments, so a change to the reader rule could silently drift from pruning. Raised by: architecture-reviewer, delta-protocol-reviewer. Suggested fix: add a parity test over source/target pairs asserting decimal_stat_as agrees with the Arrow cast whenever can_upcast_to_decimal accepts, or share the scale-increase helper.
- [inline:3938023447](https://github.com/delta-io/delta-kernel-rs/pull/3066#discussion_r3938023447) @github-actions[bot] (bot; support): **Nit3** The workspace-versioned vs independently-versioned distinction is encoded in several spots (the derived independent_dependents_of predicate plus hardcoded name lists in handle_crate_release, tag_release, and this tag_name_for sentinel). A future workspace-versioned crate would be classified correctly by the derived predicate but mis-tagged here and not blocked by handle_crate_release. Raised by: architecture-reviewer. Suggested fix: derive the class once (for example compare the crate's literal manifest version against version.workspace) and consult that predicate everywhere. Explanation: A positive reaction supports one source of truth for release-versioning classification.
- [inline:4010550924](https://github.com/delta-io/delta-kernel-rs/pull/3216#discussion_r4010550924) @github-actions[bot] (bot; support): **Nit1** CheckpointAction::into_engine_data rebuilds, line for line, the Scalar::Array of union-struct elements that the pre-existing try_into_scalar (line 1331, marked #[allow(unused)]) already produces, then only adds the trailing create_row lift. The checkpoint-action wire encoding now lives in two bodies that must change in lockstep, and the dead #[allow(unused)] copy is likely to drift as the still-in-dev adaptiveMetadata format evolves. Raised by: architecture-reviewer. Suggested fix: make into_engine_data a thin wrapper `create_row(engine, schema, self.try_into_scalar()?)` and drop the dead copy. Explanation: A positive reaction supports reusing the existing checkpoint action encoder.
- [inline:4010314626](https://github.com/delta-io/delta-kernel-rs/pull/3216#discussion_r4010314626) @github-actions[bot] (bot; support): **Nit1** scan_non_content_metadata reimplements 'active domain-metadata and set-transaction maps as of this snapshot' (CRC fast path plus stale-CRC tail scan) that Snapshot::get_domain_metadatas_internal and the set-transaction scanner already own; the genuinely new need is only locating the latest CheckpointAction. Cost: CRC reconstruction semantics must stay in lockstep in two places. Raised by: architecture-reviewer. Suggested fix: compose the existing snapshot accessors and reduce this scan to finding the latest checkpoint action; if a nested-set base is needed, add it next to scan_domain_metadatas on Snapshot/LogSegment. Explanation: The thread evaluates reusing existing snapshot accessors and the extra-replay tradeoff.

#### bot-preserve-static-constructor-contracts (correctness)

Before replacing a manual constructor with a derive, check caller-visible capabilities such as const evaluation and visibility.

Scope: Constructor simplifications used from static or const initializers.

Exceptions: The selected findings come from one PR and remain provisional; normal compilation may already catch this, so the next agent should assess whether durable reviewer guidance adds value.

Cited evidence: 1 distinct PRs; 1 reviewers; 2 discussions. Recurring: False. Opposing PRs cited: 0. State: single-pr-bot-idea.

Suggested action: add; final integration belongs to the downstream agent.

Integration target: `.github/omnigent/reviewer/agents/maintainer-claude-reviewer/REVIEW.md:157` (existing file; new).

A positively rated simplification regression suggests a concrete capability check for the Rust reviewer; retain it provisionally for downstream scrutiny.

Applicable code: `kernel/src`.

Bot feedback ranking: add=3, remove/change=0. Mixed feedback: False.

- `inline:4019910053`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4019910059`: +1=1, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Proposed wording:

```markdown
When recommending a derive or generated constructor in place of a manual one, inspect its callers and preserve required const evaluation, visibility, and signature behavior. Avoid filing a redundant compile failure if existing CI already makes the issue clear.
```

Citations:

- [inline:4019910053](https://github.com/delta-io/delta-kernel-rs/pull/3331#discussion_r4019910053) @github-actions[bot] (bot; support): **Blocker1** GetterColumn now derives Constructor, but GetterColumn::new is called from 16 const associated-item initializers (lines 347-368, e.g. `const ADD_PATH: GetterColumn = GetterColumn::new(0, "add.path");`). The removed manual new was `const fn`. derive_more 2.1.1 generates a non-const `new`, so each const initializer fails to compile with E0015. Suggested fix: drop Constructor from the derive and keep the manual `const fn new`. Raised by: maintainer-codex-reviewer, maintainer-claude-reviewer, architecture-reviewer, delta-protocol-reviewer. Explanation: The derived constructor loses const capability needed by existing associated constants.
- [inline:4019910059](https://github.com/delta-io/delta-kernel-rs/pull/3331#discussion_r4019910059) @github-actions[bot] (bot; support): **Blocker2** MinReaderWriterVersion now derives Constructor, but MinReaderWriterVersion::new is called from seven `static FeatureInfo` initializers (lines 306, 318, 326, 334, 344, 352, 531, e.g. `Some(MinReaderWriterVersion::new(1, 2))`). The removed new was `pub(crate) const fn`. The derived non-const `new` cannot be evaluated in a static and fails with E0015. Suggested fix: keep the manual `const fn new` and remove the Constructor derive. Raised by: maintainer-codex-reviewer, maintainer-claude-reviewer, architecture-reviewer, delta-protocol-reviewer. Explanation: A second constructor on the same PR has the same static-initializer issue; this is not independent recurrence.

### Possible changes/removals

#### bot-narrow-low-value-scope (design)

Scope advice to a concrete defect or maintenance cost before asking for broader API or extreme-input support.

Scope: Unusual numeric exponents and alternative API-layering preferences.

Exceptions: An eyes reaction signals out of scope, not incorrect code. A concrete production use case or protocol requirement can justify the same change.

Cited evidence: 3 distinct PRs; 1 reviewers; 4 discussions. Recurring: True. Opposing PRs cited: 0. State: bot-feedback-candidate.

Suggested action: change; final integration belongs to the downstream agent.

Thread context: Older replies reject additional span fields and a wrapper collapse because parent context and error-only ownership already solve the requested problems. These corrections support narrowing advice, not adding the original bot requests.

Integration target: `.github/omnigent/reviewer/REVIEW.md:97` (existing file; strengthen).

Eyes reactions and an explicit scope reply suggest narrowing the existing useful-note rule; they do not establish a correctness defect in the reviewed code.

Applicable code: `kernel/src`, `workloads/src`.

Bot feedback ranking: add=0, remove/change=10. Mixed feedback: False.

- `inline:3974312651`: +1=0, -1=0, eyes=0; positive replies=0, corrections=1, scope replies=0; unclassified replies=0.
- `inline:3974312662`: +1=0, -1=0, eyes=0; positive replies=0, corrections=1, scope replies=0; unclassified replies=0.
- `inline:4087669935`: +1=0, -1=0, eyes=1; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4139787286`: +1=0, -1=0, eyes=1; positive replies=0, corrections=0, scope replies=1; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Feedback [inline:4139798157](https://github.com/delta-io/delta-kernel-rs/pull/3450#discussion_r4139798157) @rliao147 (out-of-scope): extreme zero exponents already fail safely. Supporting absurd exponents adds complexity unrelated to DAT.

Feedback [inline:3980717997](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3980717997) @chiinlquah (corrected): Keeping the success spans fieldless is intentional. We chose to record this context only on failure; the parent `snap.build` span still carries operation/correlation IDs for the build, and the metrics layer does not read these constructor spans. The PR description now says this explicitly.

Feedback [inline:3980717860](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3980717860) @chiinlquah (rejected): I kept the wrapper split because it catches every return path once and adds context only when the result is `Err`. Folding it back into the instrumented function would lose the bounded fields or repeat `inspect_err` at each `?`. A shared helper would need nearly the same inputs plus two different field sets, so it would add indirection without removing much code.

Existing wording to change:

```markdown
2. **Non-blocking notes** -- brief, only if genuinely useful.
```

Proposed wording:

```markdown
Keep non-blocking notes tied to a concrete benefit in this PR. When current behavior fails safely and broader input/API support is not required by the contract, state the scope tradeoff and avoid requesting added machinery without a meaningful use case.
```

Citations:

- [inline:4139787286](https://github.com/delta-io/delta-kernel-rs/pull/3450#discussion_r4139787286) @github-actions[bot] (bot; support): **Nit2** The zero-mantissa early return happens after the exponent is parsed and checked_sub is evaluated, so price = 0e-2147483648 errors with 'exponent is out of range' and 0e99999999999 fails with a bare integer parse error, though both are valid zeros. The 1e-2147483648 test passes only because of this overflow. Raised by: maintainer-claude-reviewer. Suggested fix: compute shifted_scale in i64, or check for an all-zero mantissa before parsing the exponent; or add a comment that rejecting these is acceptable. --- <sub>Feedback: react 👍 if helpful, 👎 if unhelpful or incorrect, or 👀 if out of scope.</sub>
- [inline:4087669935](https://github.com/delta-io/delta-kernel-rs/pull/3381#discussion_r4087669935) @github-actions[bot] (bot; support): **Nit2** `to_selection_vec(&self, len)` takes the row-domain length as a decoder parameter and errors when a position is >= len, whereas the sibling DV helpers size to the bitmap itself and defer row-domain reconciliation to `split_vector`. A future caller wiring Mumbling into the same read path inherits two length conventions for the same Vec<bool>. Design-altitude judgment for a human maintainer, not a defect for well-formed input. Raised by: architecture-reviewer. Suggested fix: consider exposing only bitmap-intrinsic output (or query via contains) and letting the reader layer own row-domain fit, matching the existing helpers.
- [inline:3974312651](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3974312651) @github-actions[bot] (bot; support): **Nit1** `#[instrument(skip_all)]` has no `fields(...)` clause, so both constructor spans (here and incremental.rs:103) now carry zero fields on the success path. The PR description says the spans retain compact operational context (version, operation/correlation IDs, replay policy, built_as_latest), but that context now appears only on the failure error! event; successful loads lose operation_id/correlation_id span correlation. skip_all already excludes the heavy LogSegment/Snapshot/log-tail args, so the cheap scalars can be kept. Raised by: maintainer-claude-reviewer, maintainer-codex-reviewer. Suggested fix: add fields(operation_id = %metric_context.operation_id, correlation_id = metric_context.correlation_id.as_deref().unwrap_or("")) to the skip_all attribute, or update the PR description to state context is now error-event-only. Explanation: The requested lower-span context already exists at the owning parent span.
- [inline:3974312662](https://github.com/delta-io/delta-kernel-rs/pull/2839#discussion_r3974312662) @github-actions[bot] (bot; support): **Nit2** The try_new_from_impl / try_new_from_log_segment_impl split mirrors its single caller's argument list closely, keeps the too_many_arguments allow, and hand-duplicates the shared error-context field set (operation_id, correlation_id, incremental_replay, built_as_latest, path) across both error! sites. The capture bug is fixed by skip_all alone, so the wrapper layer is optional; this is a design preference, not a defect. Raised by: architecture-reviewer. Suggested fix: consider a single instrumented function using #[instrument(err, skip_all, fields(...))], or factor the shared error fields into one helper to avoid the field lists drifting. Explanation: The wrapper simplification overlooks a reason for catching all fallible exits once.

### Already covered or deferred

#### bot-validation-trust-boundaries (correctness)

Bot reviews flag intrinsic checks that are skipped by a state gate or a direct deserialization path.

Scope: CRC allFiles and deletion-vector validation.

Exceptions: The current trust model may intentionally accept these values; a bot finding is not a settled contract.

Cited evidence: 2 distinct PRs; 1 reviewers; 2 discussions. Recurring: True. Opposing PRs cited: 0. State: defer.

Suggested action: unspecified; final integration belongs to the downstream agent.

Integration target: `AGENTS.md:242` (existing file; defer).

Investigate individual trust-boundary claims before turning them into a durable rule.

Applicable code: `kernel/src`.

Bot feedback ranking: add=0, remove/change=0. Mixed feedback: False.

- `inline:4041841224`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.
- `inline:4042977001`: +1=0, -1=0, eyes=0; positive replies=0, corrections=0, scope replies=0; unclassified replies=0.

Heuristic ranking, not a correctness probability. Reactions are aggregate current counts; actor identities are unknown. Same-author replies count once per direction per finding; pattern scores take the maximum per PR.

Citations:

- [inline:4041841224](https://github.com/delta-io/delta-kernel-rs/pull/3333#discussion_r4041841224) @github-actions[bot] (bot; support): **Nit2** The allFiles duplicate-path, negative-size, numFiles, and tableSizeBytes checks are nested inside `if let FileStatsState::Complete(stats)`. When all_files is Some but file_stats_state is not Complete (reachable via the FFI non-Complete file-stats variant), these intrinsic checks are silently skipped, so a malformed enumeration with duplicate paths or negative sizes is accepted. Raised by: delta-protocol-reviewer. Suggested fix: hoist the duplicate-path and negative-size checks so they run whenever all_files is present, keeping only the numFiles/tableSizeBytes aggregate cross-checks gated on Complete file stats.
- [inline:4042977001](https://github.com/delta-io/delta-kernel-rs/pull/3353#discussion_r4042977001) @github-actions[bot] (bot; support): **Nit1** Add and DeletionVectorDescriptor now reach the CRC allFiles read path through derived serde, which constructs fields directly and does not call DeletionVectorDescriptor::try_new. The invariants try_new enforces (an inline DV must not carry an offset; offset/size_in_bytes/cardinality non-negative) are not checked for a DV arriving via allFiles, so a malformed or adversarial CRC is accepted verbatim and surfaced to callers. Consistent with the trust-the-CRC model and no current read path acts on these DVs, so non-blocking. Raised by: delta-protocol-reviewer. Suggested fix: route DV construction through try_new (or add a post-deserialize validation pass over allFiles), or document the
